package api

import (
	"errors"
	"net/http"

	"github.com/TheOutdoorProgrammer/crate/internal/services/preview"
)

// Preview endpoints. The flow: the frontend starts a preview (search + best
// transfer), polls status, plays the partial file via the stream endpoint
// (Range requests as bytes land), then keeps (adopt as download) or rejects
// (blacklist + next source) what it heard. Delete cancels quietly.

func (s *Server) handlePreviewStart(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	// Browse flow: the frontend watches the track first (which creates
	// artist/album/track rows and returns the numeric id), then previews.
	status, err := s.preview.Start(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePreviewStatus(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	status, err := s.preview.GetStatus(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePreviewStream(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := s.preview.ServeFile(w, r, id); err != nil {
		var se *preview.StreamError
		if errors.As(err, &se) {
			writeError(w, se.Status, se.Err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func (s *Server) handlePreviewKeep(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	if err := s.preview.Keep(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "kept"})
}

func (s *Server) handlePreviewReject(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	// Returns nil status when no more candidates remain.
	status, err := s.preview.Reject(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if status == nil {
		writeJSON(w, http.StatusOK, map[string]any{"status": "exhausted"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handlePreviewCancel(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	s.preview.Cancel(r.Context(), id)
	w.WriteHeader(http.StatusNoContent)
}
