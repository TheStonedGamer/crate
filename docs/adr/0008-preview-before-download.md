# ADR-0008: Preview Before Download

## Status

Accepted (2026-09-07).

## Context

Users need to hear the actual Soulseek file before committing it to the library. slskd writes transfer bytes under its incomplete directory and moves the file immediately when complete.

## Decision

Preview sessions are in-memory and keyed by track ID. Search results use the downloader ranking. The first candidate is transferred and streamed from `CRATE_SLSKD_INCOMPLETE_DIR`; Reject blacklists it and advances, while Keep adopts the existing transfer without restarting it. Browse uses idempotent watchTrack-then-start. There is no startup orphan reconciliation; this accepted leak risk is bounded by the runtime janitor.

Streaming returns `200` with the bytes currently on disk. Range requests return `206` and reveal the transfer total through `Content-Range`; advertising the full total as `Content-Length` on a short growing response caused Chrome `ERR_CONTENT_LENGTH_MISMATCH`. When slskd has completed and moved the file, the service falls back to the configured downloads directory using the same username/remote-path layout.

## Consequences

Preview does not create a download queue row until Keep, and Cancel does not blacklist. A process restart may leave a slskd transfer behind, and the completed fallback depends on matching slskd's filesystem layout and mounts.
