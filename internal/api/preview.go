package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/TheOutdoorProgrammer/crate/internal/models"
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

	// Browse flow: the track may not exist in the DB yet. The request carries
	// the full watch-track payload; we create the entities first (mirroring
	// handleWatchTrack) and preview the new track ID.
	var req struct {
		Create                bool `json:"create"`
		preview.BrowseRequest `json:",inline"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	trackID := id
	if req.Create {
		track, err := s.createBrowseTrack(r, &req.BrowseRequest)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		trackID = track.ID
	}

	status, err := s.preview.Start(r.Context(), trackID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status.TrackID = trackID
	writeJSON(w, http.StatusOK, status)
}

// createBrowseTrack creates artist/album/track rows for a browse preview, the
// same flow as handleWatchTrack. The track starts "wanted"; a rejected
// preview leaves it wanted (the scheduler may still try it later) unless the
// user ignores it.
func (s *Server) createBrowseTrack(r *http.Request, req *preview.BrowseRequest) (*models.Track, error) {
	primary := req.Provider
	if primary == "" {
		primary = s.providers.Primary()
	}

	artist, err := s.queries.FindArtistByProvider(primary, req.ArtistProviderID)
	if errors.Is(err, sql.ErrNoRows) {
		artist = &models.Artist{
			Name:       req.ArtistName,
			Provider:   primary,
			ProviderID: req.ArtistProviderID,
			ImageURL:   strPtrOrNil(req.ArtistImageURL),
			Status:     models.ArtistStatusPartial,
		}
		if err := s.queries.CreateArtist(artist); err != nil {
			return nil, errors.New("failed to save artist")
		}
	} else if err != nil {
		return nil, err
	}

	album, err := s.queries.FindAlbumByProvider(primary, req.AlbumProviderID)
	if errors.Is(err, sql.ErrNoRows) {
		album = &models.Album{
			ArtistID:   artist.ID,
			Title:      req.AlbumTitle,
			Year:       req.AlbumYear,
			Provider:   primary,
			ProviderID: req.AlbumProviderID,
			RecordType: "album",
			Status:     models.AlbumStatusWatched,
		}
		if err := s.queries.CreateAlbum(album); err != nil {
			return nil, errors.New("failed to save album")
		}
	} else if err != nil {
		return nil, err
	}

	track, err := s.queries.FindTrackByProvider(req.Provider, req.ProviderID)
	if errors.Is(err, sql.ErrNoRows) {
		track = &models.Track{
			AlbumID:     album.ID,
			Title:       req.Title,
			TrackNumber: req.TrackNumber,
			DiscNumber:  req.DiscNumber,
			DurationMs:  req.DurationMs,
			Provider:    req.Provider,
			ProviderID:  req.ProviderID,
			Status:      models.TrackStatusWanted,
		}
		if err := s.queries.CreateTrack(track); err != nil {
			return nil, errors.New("failed to save track")
		}
	} else if err != nil {
		return nil, err
	}

	return track, nil
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
