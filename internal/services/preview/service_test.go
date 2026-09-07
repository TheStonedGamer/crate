package preview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TheOutdoorProgrammer/crate/internal/activity"
	"github.com/TheOutdoorProgrammer/crate/internal/db"
	"github.com/TheOutdoorProgrammer/crate/internal/models"
	"github.com/TheOutdoorProgrammer/crate/internal/services/downloader"
	"github.com/TheOutdoorProgrammer/crate/internal/services/slskd"
)

type noopOrganizer struct{}

func (o *noopOrganizer) Organize(track *models.Track) error { return nil }

// --- slskd sanitization mirror (the critical correctness surface) ---

func TestPartialPathWindowsRemoteName(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("paultjuh84", `C:\Music\TheFatRat\Ray Tracer.flac`)
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/paultjuh84/Music/TheFatRat/Ray Tracer.flac"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathUnixRemoteName(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("user", "/music/Artist/Album/01 Song.flac")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/user/music/Artist/Album/01 Song.flac"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathUNCRoot(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("user", `\\server\share\Album\02 Track.mp3`)
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/user/share/Album/02 Track.mp3"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathSoulseekQtPrefix(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("user", `@@abcde\Music\Artist\03 Song.mp3`)
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/user/Music/Artist/03 Song.mp3"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathSanitizesNulWithinSegment(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	// A NUL inside a segment must become an underscore, never a separator.
	got, err := s.PartialPath("user", "Music\\Track\x00Name.flac")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/user/Music/Track_Name.flac"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathUsernameSanitized(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("bad/user", "Music/Track.flac")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/bad_user/Music/Track.flac"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathCollapseTraversalSegments(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("user", "Music/../../etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	got = filepath.ToSlash(got)
	if strings.Contains(got, "..") {
		t.Errorf("traversal survived: %q", got)
	}
	if want := "/incomplete/user/Music/_/_/etc/passwd"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestPartialPathDotSegmentCollapsed(t *testing.T) {
	s := NewService(nil, nil, nil, "/incomplete")
	got, err := s.PartialPath("user", "Music/./Track.flac")
	if err != nil {
		t.Fatal(err)
	}
	if want := "/incomplete/user/Music/_/Track.flac"; filepath.ToSlash(got) != want {
		t.Errorf("got %q want %q", filepath.ToSlash(got), want)
	}
}

func TestPartialPathNotConfigured(t *testing.T) {
	s := NewService(nil, nil, nil, "")
	if _, err := s.PartialPath("user", "Music/Track.flac"); err == nil {
		t.Error("expected error when not configured")
	}
}

// --- Session lifecycle against a fake slskd ---

const (
	peerFlac = `C:\Music\Test Artist - Wanted Song.flac`
	peer2Mp3 = `C:\Music\Test Artist - Wanted Song.mp3`
)

// fakeSlskdStream stands in for slskd with a swappable search response and a
// transfer store whose progress the test controls. Enqueued transfers get
// sequential ids (transfer-1, transfer-2, ...).
type fakeSlskdStream struct {
	*httptest.Server
	mu        sync.Mutex
	resp      slskd.SearchResponse
	transfers map[string]*slskd.Transfer
	nextID    int
}

func (f *fakeSlskdStream) setSearchResponse(resp slskd.SearchResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resp = resp
}

func (f *fakeSlskdStream) searchResponse() slskd.SearchResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resp
}

func (f *fakeSlskdStream) setProgress(id string, bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if tr, ok := f.transfers[id]; ok {
		tr.BytesTransferred = bytes
		tr.PercentComplete = float64(bytes) / float64(tr.Size) * 100
		tr.State = "InProgress"
	}
}

func (f *fakeSlskdStream) setProgressAll(bytes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tr := range f.transfers {
		tr.BytesTransferred = bytes
		tr.PercentComplete = float64(bytes) / float64(tr.Size) * 100
		tr.State = "InProgress"
	}
}

func (f *fakeSlskdStream) transferCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.transfers)
}

func defaultSearchResponse() slskd.SearchResponse {
	return slskd.SearchResponse{
		ID: "search-1", IsComplete: true,
		Responses: []slskd.SearchResult{
			{
				Username:          "peer",
				HasFreeUploadSlot: true,
				Files: []slskd.SearchFile{
					{Filename: peerFlac, Size: 1 << 20, BitRate: 1000},
				},
			},
			{
				Username:    "peer2",
				QueueLength: 1,
				Files: []slskd.SearchFile{
					{Filename: peer2Mp3, Size: 8 << 20, BitRate: 320},
				},
			},
		},
	}
}

type streamEnv struct {
	svc     *Service
	slskd   *fakeSlskdStream
	dir     string
	queries *db.Queries
}

func newStreamEnv(t *testing.T) *streamEnv {
	t.Helper()

	database, err := db.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	queries := db.NewQueries(database)

	actLog, err := activity.NewLog(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { actLog.Close() })

	fake := &fakeSlskdStream{
		resp:      defaultSearchResponse(),
		transfers: map[string]*slskd.Transfer{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v0/searches", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(fake.searchResponse())
	})
	mux.HandleFunc("/api/v0/searches/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		json.NewEncoder(w).Encode(fake.searchResponse())
	})
	mux.HandleFunc("/api/v0/transfers/downloads", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		byUser := map[string][]slskd.Transfer{}
		for _, tr := range fake.transfers {
			byUser[tr.Username] = append(byUser[tr.Username], *tr)
		}
		fake.mu.Unlock()
		list := make([]slskd.UserDownloads, 0, len(byUser))
		for user, files := range byUser {
			list = append(list, slskd.UserDownloads{
				Username:    user,
				Directories: []slskd.DownloadDirectory{{Directory: "Music", Files: files}},
			})
		}
		json.NewEncoder(w).Encode(list)
	})
	mux.HandleFunc("/api/v0/transfers/downloads/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/v0/transfers/downloads/")
		parts := strings.Split(rest, "/")
		username := parts[0]

		switch r.Method {
		case "POST":
			var reqs []slskd.DownloadRequest
			if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil || len(reqs) == 0 {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			fake.mu.Lock()
			fake.nextID++
			id := fmt.Sprintf("transfer-%d", fake.nextID)
			tr := &slskd.Transfer{
				ID: id, Username: username, Filename: reqs[0].Filename,
				Size: reqs[0].Size, State: "Initializing",
			}
			fake.transfers[id] = tr
			fake.mu.Unlock()
			json.NewEncoder(w).Encode(slskd.EnqueueResponse{Enqueued: []slskd.Transfer{*tr}})
		case "DELETE":
			id := parts[len(parts)-1]
			fake.mu.Lock()
			delete(fake.transfers, id)
			fake.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	fake.Server = httptest.NewServer(mux)
	t.Cleanup(fake.Close)

	dir := t.TempDir()
	client := slskd.NewClient(fake.URL, "test-key")
	dl := downloader.NewService(queries, client, &noopOrganizer{}, actLog)
	svc := NewService(queries, client, dl, dir)

	// Seed a library track to preview: artist "Test Artist", track
	// "Wanted Song" — both names appear in the fake search filenames.
	artist := &models.Artist{Name: "Test Artist", Provider: "test", ProviderID: "a1", Status: models.ArtistStatusPartial}
	if err := queries.CreateArtist(artist); err != nil {
		t.Fatal(err)
	}
	album := &models.Album{ArtistID: artist.ID, Title: "Test Album", Provider: "test", ProviderID: "al1", RecordType: "album", Status: models.AlbumStatusWatched}
	if err := queries.CreateAlbum(album); err != nil {
		t.Fatal(err)
	}
	track := &models.Track{AlbumID: album.ID, Title: "Wanted Song", Provider: "test", ProviderID: "t1", Status: models.TrackStatusWanted}
	if err := queries.CreateTrack(track); err != nil {
		t.Fatal(err)
	}

	return &streamEnv{svc: svc, slskd: fake, dir: dir, queries: queries}
}

// session peeks the live session for track 1 (tests run in-package).
func (e *streamEnv) session(t *testing.T) *Session {
	t.Helper()
	e.svc.mu.Lock()
	defer e.svc.mu.Unlock()
	sess, ok := e.svc.sessions[1]
	if !ok {
		t.Fatal("no session for track 1")
	}
	return sess
}

// writePartial writes n bytes to the current session's partial file path.
func (e *streamEnv) writePartial(t *testing.T, content string) {
	t.Helper()
	sess := e.session(t)
	p, err := e.svc.PartialPath(sess.Username, sess.Filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestStartPreviewSessionLifecycle(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	st, err := env.svc.Start(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if st.Username != "peer" {
		t.Errorf("username = %q, want best-candidate peer", st.Username)
	}
	if st.State != "buffering" {
		t.Errorf("state = %q", st.State)
	}
	if st.Candidates != 1 {
		t.Errorf("candidates remaining = %d, want 1", st.Candidates)
	}

	// Status polls the transfer once bytes arrive.
	sess := env.session(t)
	env.slskd.setProgress(sess.TransferID, 1000)
	st, err = env.svc.GetStatus(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if st.BytesReceived != 1000 {
		t.Errorf("bytes = %d", st.BytesReceived)
	}
	if st.State != "downloading" {
		t.Errorf("state = %q", st.State)
	}
	if st.Percent <= 0 {
		t.Errorf("percent = %f, want > 0", st.Percent)
	}

	// Cancel cleans up.
	env.svc.Cancel(ctx, 1)
	if _, err := env.svc.GetStatus(ctx, 1); !errors.Is(err, ErrNoSession) {
		t.Errorf("expected ErrNoSession after cancel, got %v", err)
	}
}

func TestKeepAdoptsTransferAsDownloadRow(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	sess := env.session(t)
	if err := env.svc.Keep(ctx, 1); err != nil {
		t.Fatal(err)
	}

	dl, err := env.queries.FindActiveDownloadByTrack(1)
	if err != nil {
		t.Fatal(err)
	}
	if dl.Status != models.DownloadStatusDownloading {
		t.Errorf("download status = %q", dl.Status)
	}
	wantKey := "peer|" + sess.TransferID
	if dl.SlskdSearchID == nil || *dl.SlskdSearchID != wantKey {
		t.Errorf("transfer key = %v, want %q", dl.SlskdSearchID, wantKey)
	}
	track, err := env.queries.GetTrack(1)
	if err != nil {
		t.Fatal(err)
	}
	if track.Status != models.TrackStatusDownloading {
		t.Errorf("track status = %q", track.Status)
	}
	if track.DownloadedFrom == nil || *track.DownloadedFrom != "peer" {
		t.Errorf("downloaded_from = %v", track.DownloadedFrom)
	}
	if track.DownloadedFilename == nil || *track.DownloadedFilename != peerFlac {
		t.Errorf("downloaded_filename = %v", track.DownloadedFilename)
	}

	// Session is gone after keep.
	if _, err := env.svc.GetStatus(ctx, 1); !errors.Is(err, ErrNoSession) {
		t.Errorf("expected ErrNoSession after keep, got %v", err)
	}
}

func TestRejectBlacklistsAdvancesThenExhausts(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// First reject: blacklist peer's flac, advance to peer2's mp3.
	st, err := env.svc.Reject(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		t.Fatal("expected advanced status, got exhausted")
	}
	if st.Username != "peer2" {
		t.Errorf("advanced to username = %q, want peer2", st.Username)
	}
	if !env.queries.IsBlacklisted("peer", peerFlac) {
		t.Error("expected (peer, flac) blacklisted after reject")
	}
	if env.queries.IsBlacklisted("peer2", peer2Mp3) {
		t.Error("peer2 must not be blacklisted before its own reject")
	}
	if st.Candidates != 0 {
		t.Errorf("candidates remaining = %d, want 0", st.Candidates)
	}

	// Second reject: no candidates left, session ends.
	st, err = env.svc.Reject(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if st != nil {
		t.Errorf("expected nil status on exhausted candidates, got %+v", st)
	}
	if !env.queries.IsBlacklisted("peer2", peer2Mp3) {
		t.Error("expected (peer2, mp3) blacklisted after second reject")
	}
	if _, err := env.svc.GetStatus(ctx, 1); !errors.Is(err, ErrNoSession) {
		t.Errorf("expected ErrNoSession after exhaustion, got %v", err)
	}
}

func TestCancelDoesNotBlacklist(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.svc.Cancel(ctx, 1)
	if env.queries.IsBlacklisted("peer", peerFlac) {
		t.Error("cancel must not blacklist")
	}
	if env.slskd.transferCount() != 0 {
		t.Errorf("transfers after cancel = %d, want 0", env.slskd.transferCount())
	}
}

// --- Streaming with Range over a growing file ---

func TestStreamServesPartialBytes(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.writePartial(t, "0123456789")
	sess := env.session(t)
	env.slskd.setProgress(sess.TransferID, 10)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/1/preview/stream", nil)
	if err := env.svc.ServeFile(w, req, 1); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK {
		t.Errorf("code = %d", w.Code)
	}
	if got := w.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q", got)
	}
	if got := w.Header().Get("Content-Length"); got != fmt.Sprint(1<<20) {
		t.Errorf("Content-Length = %q, want full transfer size", got)
	}
	body, _ := io.ReadAll(w.Body)
	if string(body) != "0123456789" {
		t.Errorf("body = %q", body)
	}
}

func TestStreamRangeRequest(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.writePartial(t, "0123456789")
	sess := env.session(t)
	env.slskd.setProgress(sess.TransferID, 10)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/1/preview/stream", nil)
	req.Header.Set("Range", "bytes=2-5")
	if err := env.svc.ServeFile(w, req, 1); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusPartialContent {
		t.Errorf("code = %d", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	if string(body) != "2345" {
		t.Errorf("body = %q", body)
	}
	if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 2-5/%d", 1<<20) {
		t.Errorf("Content-Range = %q", got)
	}
}

func TestStreamRangeTruncatedToOnDisk(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.writePartial(t, "0123456789")
	sess := env.session(t)
	env.slskd.setProgress(sess.TransferID, 10)

	// Request to EOF; only the on-disk bytes can be served.
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/1/preview/stream", nil)
	req.Header.Set("Range", "bytes=4-")
	if err := env.svc.ServeFile(w, req, 1); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusPartialContent {
		t.Errorf("code = %d", w.Code)
	}
	body, _ := io.ReadAll(w.Body)
	if string(body) != "456789" {
		t.Errorf("body = %q", body)
	}
	if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes 4-9/%d", 1<<20) {
		t.Errorf("Content-Range = %q", got)
	}
}

func TestStreamRangeBeyondDownloadedIs416(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.writePartial(t, "0123456789")
	sess := env.session(t)
	env.slskd.setProgress(sess.TransferID, 10)

	// The requested range starts beyond what's on disk. The error carries
	// the 416 status for the handler to write, plus the available-range hint.
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/1/preview/stream", nil)
	req.Header.Set("Range", "bytes=500000-")
	err := env.svc.ServeFile(w, req, 1)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("expected StreamError, got %v", err)
	}
	if se.Status != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("status = %d", se.Status)
	}
	if got := w.Header().Get("Content-Range"); got != fmt.Sprintf("bytes */%d", 1<<20) {
		t.Errorf("Content-Range = %q", got)
	}
}

func TestStreamNoSessionIs404(t *testing.T) {
	env := newStreamEnv(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/999/preview/stream", nil)
	err := env.svc.ServeFile(w, req, 999)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("expected StreamError, got %v", err)
	}
	if se.Status != http.StatusNotFound {
		t.Errorf("status = %d", se.Status)
	}
}

func TestStreamNoBytesYetIs503(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.writePartial(t, "")

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/tracks/1/preview/stream", nil)
	err := env.svc.ServeFile(w, req, 1)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("expected StreamError, got %v", err)
	}
	if se.Status != http.StatusServiceUnavailable {
		t.Errorf("status = %d", se.Status)
	}
}

// --- Janitor ---

func TestReapExpiredCancelsTransfers(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	// Force-expire by backdating the session.
	env.svc.mu.Lock()
	sess := env.svc.sessions[1]
	sess.UpdatedAt = sess.UpdatedAt.Add(-SessionTTL - time.Minute)
	env.svc.mu.Unlock()

	env.svc.ReapExpired(ctx)
	if len(env.svc.sessions) != 0 {
		t.Errorf("sessions = %d, want 0", len(env.svc.sessions))
	}
	if n := env.slskd.transferCount(); n != 0 {
		t.Errorf("transfers after reap = %d, want 0 (cancelled)", n)
	}
}

func TestReapKeepsFreshSessions(t *testing.T) {
	env := newStreamEnv(t)
	ctx := context.Background()

	if _, err := env.svc.Start(ctx, 1); err != nil {
		t.Fatal(err)
	}
	env.svc.ReapExpired(ctx)
	if len(env.svc.sessions) != 1 {
		t.Errorf("sessions = %d, want 1", len(env.svc.sessions))
	}
	if n := env.slskd.transferCount(); n != 1 {
		t.Errorf("transfers = %d, want 1 (not cancelled)", n)
	}
}

// --- Start guard ---

func TestStartRequiresConfiguration(t *testing.T) {
	env := newStreamEnv(t)
	svc := NewService(env.queries, nil, nil, "")
	if _, err := svc.Start(context.Background(), 1); err == nil {
		t.Error("expected not-configured error")
	}
}
