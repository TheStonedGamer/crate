package preview

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// StreamError wraps streaming failures with an HTTP status.
type StreamError struct {
	Status int
	Err    error
}

func (e *StreamError) Error() string { return e.Err.Error() }
func (e *StreamError) Unwrap() error { return e.Err }

func streamErr(status int, err error) *StreamError {
	return &StreamError{Status: status, Err: err}
}

// ServeFile streams the preview session's partial file for a track with HTTP
// Range support, so <audio> elements can play while the transfer is running.
//
// The partial file grows as slskd receives bytes. Browsers request ranges as
// they decode; each request is answered from the bytes currently on disk and
// ends cleanly (short read), which makes the browser issue the next Range
// request from where it left off. The advertised Content-Length is the FULL
// transfer size, so the player's seek bar spans the whole song.
func (s *Service) ServeFile(w http.ResponseWriter, r *http.Request, trackID int64) error {
	s.mu.Lock()
	sess, ok := s.sessions[trackID]
	s.mu.Unlock()
	if !ok {
		return streamErr(http.StatusNotFound, ErrNoSession)
	}
	sess.UpdatedAt = time.Now()

	if !s.Configured() {
		return streamErr(http.StatusServiceUnavailable, errors.New("preview is not configured"))
	}

	path, err := s.PartialPath(sess.Username, sess.Filename)
	if err != nil {
		return streamErr(http.StatusInternalServerError, err)
	}

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		if s.downloadsDir == "" {
			return streamErr(http.StatusNotFound, fmt.Errorf("partial file not created yet"))
		}
		transfer, transferErr := s.slskd.GetDownload(r.Context(), sess.Username, sess.TransferID)
		if transferErr != nil || !(strings.Contains(transfer.State, "Succeeded") || strings.Contains(transfer.State, "Completed")) {
			return streamErr(http.StatusNotFound, fmt.Errorf("partial file not created yet"))
		}
		path, err = s.completedPath(sess.Username, sess.Filename)
		if err != nil {
			return streamErr(http.StatusInternalServerError, err)
		}
		f, err = os.Open(path)
		if os.IsNotExist(err) {
			return streamErr(http.StatusNotFound, fmt.Errorf("completed file not found"))
		}
	}
	if err != nil {
		return streamErr(http.StatusInternalServerError, err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return streamErr(http.StatusInternalServerError, err)
	}
	onDisk := fi.Size()
	if onDisk == 0 {
		return streamErr(http.StatusServiceUnavailable, errors.New("no bytes received yet"))
	}

	// Advertise the full transfer size so the seek bar spans the whole file;
	// what we actually serve is limited to the bytes currently on disk.
	total := sess.Size
	if total < onDisk {
		// Transfer finished and slskd already moved the file (race between
		// the move and our open). Serve what we can read now.
		total = onDisk
	}

	ext := strings.ToLower(filepath.Ext(sess.Filename))
	w.Header().Set("Content-Type", contentTypeFor(ext))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "no-store")

	// Single-part Range only; a multipart range on a growing file is not
	// worth the complexity.
	rng := r.Header.Get("Range")
	if rng != "" {
		return s.serveRange(w, f, rng, onDisk, total)
	}

	// Advertise only what we can actually serve: a Content-Length larger than
	// the body we write makes Chrome abort with ERR_CONTENT_LENGTH_MISMATCH.
	// The browser learns the full total from the first 206 Content-Range
	// (bytes x-y/total) and keeps issuing Range requests as bytes land, so the
	// seek bar still spans the whole file.
	w.Header().Set("Content-Length", strconv.FormatInt(onDisk, 10))
	w.WriteHeader(http.StatusOK)
	_, copyErr := io.Copy(w, io.LimitReader(f, onDisk))
	if copyErr != nil {
		// Client went away or the server timed out the write; both are normal
		// for growing-file streaming (the browser re-requests with a Range).
		slog.Debug("preview: stream copy ended", "track_id", trackID, "error", copyErr)
	}
	return nil
}

func (s *Service) completedPath(username, filename string) (string, error) {
	localized := strings.ReplaceAll(filename, "\\", "/")
	localized = strings.TrimLeft(localized, "/")
	parts := strings.Split(localized, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if p := sanitizeSegment(part); p != "" {
			clean = append(clean, p)
		}
	}
	if len(clean) == 0 {
		return "", fmt.Errorf("no usable path in %q", filename)
	}
	return filepath.Join(append([]string{s.downloadsDir, sanitizeSegment(username)}, clean...)...), nil
}

func (s *Service) serveRange(w http.ResponseWriter, f *os.File, rng string, onDisk, total int64) error {
	// Format: bytes=start-end (end optional). A missing end means "to EOF".
	var start, end int64
	n, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
	if err != nil && n < 1 {
		// Suffix form: bytes=-N (last N bytes). Rare from browsers; serve 416.
		return streamErr(http.StatusRequestedRangeNotSatisfiable, fmt.Errorf("unsupported range %q", rng))
	}
	if start < 0 || start >= total {
		return streamErr(http.StatusRequestedRangeNotSatisfiable, fmt.Errorf("range start out of bounds"))
	}
	if end <= 0 || end >= total {
		end = total - 1
	}
	if end < start {
		return streamErr(http.StatusRequestedRangeNotSatisfiable, fmt.Errorf("invalid range"))
	}

	// Limit what we can actually serve to the bytes on disk.
	serveEnd := end
	if serveEnd >= onDisk {
		serveEnd = onDisk - 1
	}
	if start > serveEnd {
		// The requested range starts beyond what's on disk. Tell the client
		// what's available; browsers treat this as "buffer more and retry".
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		return streamErr(http.StatusRequestedRangeNotSatisfiable, errors.New("requested range not yet downloaded"))
	}

	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return streamErr(http.StatusInternalServerError, err)
	}

	length := serveEnd - start + 1
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, serveEnd, total))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, copyErr := io.Copy(w, io.LimitReader(f, length))
	if copyErr != nil {
		slog.Debug("preview: range copy ended", "error", copyErr)
	}
	return nil
}

func contentTypeFor(ext string) string {
	switch ext {
	case ".flac", "flac":
		return "audio/flac"
	case ".mp3", "mp3":
		return "audio/mpeg"
	case ".ogg", "ogg":
		return "audio/ogg"
	case ".opus", "opus":
		return "audio/ogg"
	case ".m4a", "m4a", ".aac", "aac":
		return "audio/mp4"
	case ".wav", "wav":
		return "audio/wav"
	default:
		return "application/octet-stream"
	}
}
