// Package preview implements preview-before-download: search Soulseek via
// slskd, stream the real bytes of the file that would be downloaded while the
// transfer is in flight, and let the user keep (adopt as a download) or reject
// (blacklist + advance to the next-best source) what they hear.
package preview

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/TheOutdoorProgrammer/crate/internal/db"
	"github.com/TheOutdoorProgrammer/crate/internal/models"
	"github.com/TheOutdoorProgrammer/crate/internal/services/downloader"
	"github.com/TheOutdoorProgrammer/crate/internal/services/slskd"
)

// SessionTTL bounds how long an idle preview session lives. The janitor
// cancels the backing transfer and reaps the session after this.
const SessionTTL = 15 * time.Minute

var (
	// Mirrors of slskd's FileSafety root-stripping regexes (Linux runtime):
	// drive letters, UNC roots, and SoulseekQt @@hash share prefixes.
	driveRootRe      = regexp.MustCompile(`^[a-zA-Z]:[/\\]?`)
	uncRootRe        = regexp.MustCompile(`^[/\\]{2}[^/\\]+[/\\]?`)
	soulseekQtRootRe = regexp.MustCompile(`^@@[a-zA-Z0-9]{5,}[\/\\]?`)
)

// Candidate is one ranked source file from the preview search.
type Candidate = downloader.RankedCandidate

// Session is one in-progress preview: the ranked candidate list, the
// currently-streaming transfer, and bookkeeping for adoption/rejection.
type Session struct {
	TrackID    int64
	Username   string
	Filename   string // slskd remote filename
	Size       int64
	BitRate    int
	TransferID string
	Candidates []Candidate // ranked, best first; [0] is the live transfer
	SearchID   string      // slskd search to clean up on cancel
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Status is what the frontend polls for while a preview plays.
type Status struct {
	TrackID       int64   `json:"track_id"`
	Username      string  `json:"username"`
	Filename      string  `json:"filename"`
	Size          int64   `json:"size"`
	BitRate       int     `json:"bit_rate"`
	BytesReceived int64   `json:"bytes_received"`
	Percent       float64 `json:"percent"`
	State         string  `json:"state"` // starting|buffering|downloading|completed|failed
	Error         string  `json:"error,omitempty"`
	Candidates    int     `json:"candidates"` // remaining sources after the current one
}

// Service manages preview sessions and streams partial files.
type Service struct {
	queries       *db.Queries
	slskd         *slskd.Client
	downloader    *downloader.Service
	incompleteDir string
	downloadsDir  string

	mu       sync.Mutex
	sessions map[int64]*Session // keyed by track ID
	startMu  sync.Mutex
	starting map[int64]chan struct{}
}

// NewService returns a preview service. incompleteDir may be empty, in which
// case Start fails fast with a "not configured" error rather than reading disk.
func NewService(queries *db.Queries, client *slskd.Client, dl *downloader.Service, incompleteDir string, downloadsDir ...string) *Service {
	var completedDir string
	if len(downloadsDir) > 0 {
		completedDir = downloadsDir[0]
	}
	return &Service{
		queries:       queries,
		slskd:         client,
		downloader:    dl,
		incompleteDir: incompleteDir,
		downloadsDir:  completedDir,
		sessions:      make(map[int64]*Session),
		starting:      make(map[int64]chan struct{}),
	}
}

// Configured reports whether previewing is possible on this install.
func (s *Service) Configured() bool { return s.incompleteDir != "" }

// Run starts the session janitor that cancels expired sessions. Run in a
// goroutine alongside the downloader's Run.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ReapExpired(ctx)
		}
	}
}

// ReapExpired cancels and drops sessions idle past SessionTTL.
func (s *Service) ReapExpired(ctx context.Context) {
	var expired []*Session
	s.mu.Lock()
	for id, sess := range s.sessions {
		if time.Since(sess.UpdatedAt) > SessionTTL {
			expired = append(expired, sess)
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()
	for _, sess := range expired {
		slog.Info("preview: reaping expired session", "track_id", sess.TrackID, "username", sess.Username)
		s.cancelSessionTransfer(ctx, sess)
	}
}

// Start begins a preview for a tracked (library) track: searches slskd with
// the downloader's scoring, starts the best transfer, and stores the ranked
// candidate list so Reject can advance through the rest.
func (s *Service) Start(ctx context.Context, trackID int64) (*Status, error) {
	if !s.Configured() {
		return nil, errors.New("preview is not configured: set CRATE_SLSKD_INCOMPLETE_DIR")
	}

	track, err := s.queries.GetTrackWithMeta(trackID)
	if err != nil {
		return nil, err
	}

	// Serialize starts per track. A concurrent caller waits for the first
	// search/enqueue to finish, then receives the resulting session.
	s.startMu.Lock()
	if done, exists := s.starting[trackID]; exists {
		s.startMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		s.mu.Lock()
		sess, ok := s.sessions[trackID]
		s.mu.Unlock()
		if ok {
			return s.statusFor(sess, "buffering", nil), nil
		}
		return nil, errors.New("preview start did not produce a session")
	}
	done := make(chan struct{})
	s.starting[trackID] = done
	s.startMu.Unlock()
	defer func() { s.startMu.Lock(); delete(s.starting, trackID); close(done); s.startMu.Unlock() }()

	// Existing sessions are idempotent; do not enqueue a duplicate transfer.
	s.mu.Lock()
	sess, hadOld := s.sessions[trackID]
	s.mu.Unlock()
	if hadOld {
		return s.statusFor(sess, "buffering", nil), nil
	}

	search, err := s.slskd.StartSearch(ctx, s.downloader.SearchQuery(track))
	if err != nil {
		slog.Warn("preview: start search failed", "track_id", trackID, "error", err)
		return nil, fmt.Errorf("start search: %w", err)
	}

	candidates, err := s.collectCandidates(ctx, search.ID, track)
	if err != nil {
		slog.Warn("preview: candidate collection failed", "track_id", trackID, "error", err)
		_ = s.slskd.DeleteSearch(ctx, search.ID)
		return nil, err
	}
	if len(candidates) == 0 {
		_ = s.slskd.DeleteSearch(ctx, search.ID)
		return nil, errors.New("no suitable sources found")
	}

	return s.beginTransfer(ctx, trackID, search.ID, candidates)
}

// collectCandidates polls the search until it completes, yields enough
// candidates, or the window closes. Candidates are ranked and auto-download
// filtered (title+artist match, blacklist, cooldowns, negative keywords).
func (s *Service) collectCandidates(ctx context.Context, searchID string, track *models.Track) ([]Candidate, error) {
	const (
		pollEvery = 2 * time.Second
		maxWait   = 20 * time.Second
		goodAfter = 4 * time.Second // accept the list once results have had time to trickle in
		minCount  = 3               // ...if at least this many candidates exist
	)
	started := time.Now()
	deadline := started.Add(maxWait)
	for {
		search, err := s.slskd.GetSearch(ctx, searchID)
		if err != nil {
			return nil, fmt.Errorf("poll search: %w", err)
		}
		candidates := s.downloader.PreviewCandidates(search.Responses, track)
		if search.IsComplete || (len(candidates) >= minCount && time.Since(started) >= goodAfter) || time.Now().After(deadline) {
			return candidates, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

// beginTransfer starts the transfer for candidates[0] and stores the session.
func (s *Service) beginTransfer(ctx context.Context, trackID int64, searchID string, candidates []Candidate) (*Status, error) {
	best := candidates[0]
	transfer, err := s.slskd.StartDownload(ctx, best.Username, best.Filename, best.Size)
	if err != nil {
		slog.Warn("preview: start download failed", "track_id", trackID, "username", best.Username, "filename", best.Filename, "error", err)
		return nil, fmt.Errorf("start download: %w", err)
	}

	now := time.Now()
	sess := &Session{
		TrackID:    trackID,
		Username:   best.Username,
		Filename:   best.Filename,
		Size:       best.Size,
		BitRate:    best.BitRate,
		TransferID: transfer.ID,
		Candidates: candidates,
		SearchID:   searchID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.mu.Lock()
	s.sessions[trackID] = sess
	s.mu.Unlock()

	slog.Info("preview: transfer started", "track_id", trackID, "username", best.Username, "transfer_id", transfer.ID, "candidates", len(candidates))
	s.downloader.LogPreviewActivity("preview_started", trackID,
		fmt.Sprintf("Preview: %s from %s", filepath.Base(best.Filename), best.Username))

	return s.statusFor(sess, "buffering", nil), nil
}

// GetStatus returns the live session state for a track, polling slskd for
// transfer progress.
func (s *Service) GetStatus(ctx context.Context, trackID int64) (*Status, error) {
	s.mu.Lock()
	sess, ok := s.sessions[trackID]
	s.mu.Unlock()
	if !ok {
		return nil, ErrNoSession
	}
	sess.UpdatedAt = time.Now()

	transfer, err := s.slskd.GetDownload(ctx, sess.Username, sess.TransferID)
	if err != nil {
		st := s.statusFor(sess, "failed", err)
		return st, nil
	}
	switch {
	case strings.Contains(transfer.State, "Succeeded"), strings.Contains(transfer.State, "Completed"):
		st := s.statusFor(sess, "completed", nil)
		st.BytesReceived = transfer.BytesTransferred
		st.Percent = transfer.PercentComplete
		if st.Percent == 0 {
			st.Percent = 100
		}
		return st, nil
	case strings.Contains(transfer.State, "Cancelled"), strings.Contains(transfer.State, "Errored"), strings.Contains(transfer.State, "Rejected"):
		return s.statusFor(sess, "failed", fmt.Errorf("transfer %s", transfer.State)), nil
	case transfer.BytesTransferred > 0:
		st := s.statusFor(sess, "downloading", nil)
		st.BytesReceived = transfer.BytesTransferred
		st.Percent = transfer.PercentComplete
		return st, nil
	default:
		return s.statusFor(sess, "buffering", nil), nil
	}
}

// ErrNoSession is returned when a track has no preview session.
var ErrNoSession = errors.New("no active preview session")

func (s *Service) statusFor(sess *Session, state string, cause error) *Status {
	st := &Status{
		TrackID:    sess.TrackID,
		Username:   sess.Username,
		Filename:   sess.Filename,
		Size:       sess.Size,
		BitRate:    sess.BitRate,
		State:      state,
		Candidates: len(sess.Candidates) - 1,
	}
	if cause != nil {
		st.Error = cause.Error()
	}
	return st
}

// Keep adopts the in-flight transfer as a normal download row; the
// downloader's tick then shepherds it through organize/tag/notify.
func (s *Service) Keep(ctx context.Context, trackID int64) error {
	s.mu.Lock()
	sess, ok := s.sessions[trackID]
	s.mu.Unlock()
	if !ok {
		return ErrNoSession
	}

	if err := s.downloader.AdoptTransfer(ctx, trackID, sess.Username, sess.Filename, sess.Size, sess.BitRate, sess.TransferID, "Preview kept"); err != nil {
		return err
	}
	if sess.SearchID != "" {
		_ = s.slskd.DeleteSearch(ctx, sess.SearchID)
	}

	s.mu.Lock()
	delete(s.sessions, trackID)
	s.mu.Unlock()
	slog.Info("preview: kept, transfer adopted", "track_id", trackID, "username", sess.Username)
	return nil
}

// Reject cancels the transfer, blacklists (username, filename), deletes the
// partial file, and starts the next-best candidate. It returns the new
// session status, or nil with the session ended when no candidates remain.
func (s *Service) Reject(ctx context.Context, trackID int64) (*Status, error) {
	s.mu.Lock()
	sess, ok := s.sessions[trackID]
	s.mu.Unlock()
	if !ok {
		return nil, ErrNoSession
	}

	// Cancel first so no more bytes land, then clean disk and blacklist.
	s.cancelSessionTransfer(ctx, sess)
	_ = s.queries.BlacklistFile(sess.Username, sess.Filename, "preview rejected by user")
	slog.Info("preview: rejected and blacklisted", "track_id", trackID, "username", sess.Username, "filename", filepath.Base(sess.Filename))
	s.downloader.LogPreviewActivity("preview_rejected", trackID,
		fmt.Sprintf("Rejected preview from %s, trying next source", sess.Username))

	rest := sess.Candidates[1:]
	if len(rest) == 0 {
		if sess.SearchID != "" {
			_ = s.slskd.DeleteSearch(ctx, sess.SearchID)
		}
		s.mu.Lock()
		delete(s.sessions, trackID)
		s.mu.Unlock()
		return nil, nil
	}
	return s.beginTransfer(ctx, trackID, sess.SearchID, rest)
}

// Cancel aborts a preview without blacklisting (player closed / navigated).
func (s *Service) Cancel(ctx context.Context, trackID int64) {
	s.mu.Lock()
	sess, ok := s.sessions[trackID]
	s.mu.Unlock()
	if !ok {
		return
	}
	s.cancelSessionTransfer(ctx, sess)
	if sess.SearchID != "" {
		_ = s.slskd.DeleteSearch(ctx, sess.SearchID)
	}
	s.mu.Lock()
	delete(s.sessions, trackID)
	s.mu.Unlock()
	slog.Info("preview: cancelled", "track_id", trackID, "username", sess.Username)
}

func (s *Service) cancelSessionTransfer(ctx context.Context, sess *Session) {
	if sess.Username != "" && sess.TransferID != "" {
		_ = s.slskd.CancelDownload(ctx, sess.Username, sess.TransferID)
	}
	s.removePartial(sess)
}

// removePartial deletes the cancelled transfer's partial file so slskd won't
// try to resume it if the same file is requested again later.
func (s *Service) removePartial(sess *Session) {
	if !s.Configured() {
		return
	}
	p, err := s.PartialPath(sess.Username, sess.Filename)
	if err != nil {
		return
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		slog.Warn("preview: failed to remove partial file", "path", p, "error", err)
	}
}

// PartialPath maps a slskd remote filename to its expected on-disk
// incomplete path, replicating slskd's FileSafety sanitization for the Linux
// runtime (crate always runs in the Linux container alongside slskd).
//
// Layout (slskd DownloadService):
//
//	<incompleteDir>/<sanitized(username)>/<sanitized(remote dirs)>/<sanitized(file)>
func (s *Service) PartialPath(username, remoteFilename string) (string, error) {
	if !s.Configured() {
		return "", errors.New("preview is not configured")
	}
	localized := strings.ReplaceAll(remoteFilename, "\\", "/")
	stripped := driveRootRe.ReplaceAllLiteralString(localized, "")
	stripped = uncRootRe.ReplaceAllLiteralString(stripped, "")
	stripped = soulseekQtRootRe.ReplaceAllLiteralString(stripped, "")
	stripped = strings.TrimPrefix(stripped, "/")

	parts := strings.Split(stripped, "/")
	clean := make([]string, 0, len(parts))
	for _, seg := range parts {
		seg = sanitizeSegment(seg)
		if seg == "" {
			continue
		}
		clean = append(clean, seg)
	}
	if len(clean) == 0 {
		return "", fmt.Errorf("no usable path in %q", remoteFilename)
	}
	return filepath.Join(append([]string{s.incompleteDir, sanitizeSegment(username)}, clean...)...), nil
}

// sanitizeSegment applies slskd's Unix SanitizePathSegment: NUL and slash
// become underscore; "."/".." collapse to a single underscore.
func sanitizeSegment(seg string) string {
	seg = strings.ReplaceAll(seg, "\x00", "_")
	seg = strings.ReplaceAll(seg, "/", "_")
	seg = strings.ReplaceAll(seg, "\\", "_")
	if seg == "." || seg == ".." {
		return "_"
	}
	return seg
}
