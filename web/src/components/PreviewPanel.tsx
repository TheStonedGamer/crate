import { useCallback, useEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { useToast } from './Toast';
import { formatFileSize } from '../lib/format';
import type { PreviewStatus } from '../types/index';// PreviewPanel: the inline panel that plays a preview. Mounted under a track
// row once a preview session exists. It polls status, mounts the <audio>
// element once bytes are on disk (Range requests keep it fed as the transfer
// grows), and offers Keep (adopt as download) / Reject (next source) / Close
// (cancel quietly).
//
// generation: bumped whenever the underlying transfer changes (reject
// advanced to another source) so the audio element remounts and plays the
// new file instead of resuming the old byte stream.
export default function PreviewPanel({
  trackId,
  initialStatus,
  onClose,
  onKept,
}: {
  trackId: number;
  initialStatus: PreviewStatus;
  onClose: () => void;
  onKept?: () => void;
}) {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  const [status, setStatus] = useState<PreviewStatus>(initialStatus);
  const [generation, setGeneration] = useState(0);
  const [busy, setBusy] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  // Tracks the transfer we last mounted audio for, so a remount only happens
  // when the source actually changes, not on every status refresh.
  const mountedFor = useRef<string>('');

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const close = useCallback(() => {
    stopPolling();
    onClose();
  }, [stopPolling, onClose]);

  // Poll the session while it is live. Terminal states (completed, failed)
  // stop the loop; the panel stays mounted so the user can still decide.
  useEffect(() => {
    if (status.state === 'completed' || status.state === 'failed') {
      stopPolling();
      return;
    }
    const poll = async () => {
      try {
        const st = await api.previewStatus(trackId);
        setStatus(st);
      } catch {
        // Session vanished server-side (janitor or restart): stop polling.
        // Leave the panel; Keep/Reject will surface the error if tried.
        stopPolling();
      }
    };
    pollRef.current = setInterval(poll, 2000);
    return stopPolling;
  }, [status.state, trackId, stopPolling]);

  // Unmount: cancel the preview so nothing keeps downloading unheard.
  useEffect(() => {
    return () => {
      api.cancelPreview(trackId).catch(() => {});
    };
  }, [trackId]);

  const key = `${status.username}|${status.filename}`;
  const audioSrc = `/api/tracks/${trackId}/preview/stream?g=${generation}`;

  // Remount audio only when the source (username+file) changes — e.g. after
  // a reject advanced to the next candidate. The ?g= cache-buster forces the
  // element to drop its buffered position for the new file.
  useEffect(() => {
    if (key !== mountedFor.current && status.bytes_received > 0) {
      mountedFor.current = key;
      setGeneration((g) => g + 1);
    }
  }, [key, status.bytes_received]);

  const keep = async () => {
    setBusy(true);
    try {
      await api.keepPreview(trackId);
      toast('Preview kept - downloading to library', 'success');
      stopPolling();
      onKept?.();
      queryClient.invalidateQueries({ queryKey: ['downloads'] });
      queryClient.invalidateQueries({ queryKey: ['album'] });
      queryClient.invalidateQueries({ queryKey: ['status'] });
      onClose();
    } catch (err) {
      toast(err instanceof Error ? err.message : 'Keep failed', 'error');
      setBusy(false);
    }
  };

  const reject = async () => {
    setBusy(true);
    try {
      const resp = await api.rejectPreview(trackId);
      if (!('username' in resp)) {
        // Exhausted: no candidates remain, session is gone.
        toast('No more sources for this track', 'error');
        stopPolling();
        onClose();
        return;
      }
      // Advanced to the next source: reset the panel around the new transfer.
      setStatus(resp);
      mountedFor.current = '';
      setGeneration((g) => g + 1);
      setBusy(false);
    } catch (err) {
      toast(err instanceof Error ? err.message : 'Reject failed', 'error');
      setBusy(false);
    }
  };

  const pct = status.size > 0 ? Math.min(100, status.percent) : 0;

  return (
    <div className="bg-zinc-900/50 border-b border-zinc-800/50 px-3 py-2.5 animate-fade-in">
      <div className="flex items-center gap-2.5 mb-2">
        <div className="flex-1 min-w-0">
          <p className="text-[11px] text-zinc-300 truncate">
            {status.filename.split(/[/\\]/).pop()}
            {status.bit_rate > 0 && <span className="text-zinc-500"> ({status.bit_rate}k)</span>}
          </p>
          <p className="text-[10px] text-zinc-500 truncate">
            from {status.username}
            {' · '}{formatFileSize(status.bytes_received)} of {formatFileSize(status.size)}
            {status.candidates > 0 && ` · ${status.candidates} more source${status.candidates === 1 ? '' : 's'}`}
          </p>
        </div>
        <div className="flex items-center gap-1.5 shrink-0">
          <button
            onClick={keep}
            disabled={busy}
            className="px-2.5 py-1 bg-green-700 hover:bg-green-600 disabled:opacity-40 rounded text-xs font-medium text-green-100 transition-colors"
            title="Keep this version - adopt the in-flight download"
          >
            Keep
          </button>
          <button
            onClick={reject}
            disabled={busy}
            className="px-2.5 py-1 bg-zinc-700 hover:bg-zinc-600 disabled:opacity-40 rounded text-xs font-medium text-zinc-300 transition-colors"
            title="Wrong song or bad quality - blacklist this source and try the next"
          >
            Reject
          </button>
          <button
            onClick={close}
            disabled={busy}
            className="px-2 py-1 text-zinc-500 hover:text-zinc-300 disabled:opacity-40 rounded text-xs transition-colors"
            title="Stop previewing"
          >
            Close
          </button>
        </div>
      </div>

      {status.state === 'failed' ? (
        <p className="text-xs text-red-400 py-1">
          {status.error || 'Transfer failed'} — Reject to try the next source.
        </p>
      ) : status.bytes_received > 0 ? (
        <>
          {/* Key forces a fresh element when the source changes; preload
              none avoids double-fetching before the browser seeks. */}
          <audio
            key={`${key}-${generation}`}
            src={audioSrc}
            controls
            autoPlay
            preload="none"
            className="w-full h-8"
          />
          <div className="flex items-center gap-2 mt-1.5">
            <div className="flex-1 h-1 bg-zinc-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all duration-300 ${
                  status.state === 'completed' ? 'bg-green-500' : 'bg-blue-500'
                }`}
                style={{ width: `${pct}%` }}
              />
            </div>
            <span className="text-[10px] text-zinc-500 tabular-nums shrink-0">
              {status.state === 'completed' ? 'downloaded' : `${Math.round(pct)}%`}
            </span>
          </div>
        </>
      ) : (
        <div className="flex items-center gap-2 py-2">
          <div className="w-4 h-4 border-2 border-zinc-600 border-t-zinc-300 rounded-full animate-spin" />
          <span className="text-xs text-zinc-500">
            {status.state === 'buffering' ? 'Connecting to peer, waiting for first bytes...' : 'Starting...'}
          </span>
        </div>
      )}
    </div>
  );
}
