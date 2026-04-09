# Crowd Requests Now-Playing and Guest Queue Sync Audit

## Current now-playing source of truth before Phase 2.7
- The sidecar stored guest now-playing in `_now_playing` inside `src/analysis/crowd_request_server.py`.
- The native app updated that payload through `/operator/now-playing` from `syncGuestNowPlaying()` in `src/ui/AncillaryScreensWidget.h`.
- The payload source was the `setNowPlayingProvider(...)` lambda in `src/ui/main.cpp`, which returned `playerTrackLabel_`, `playerArtistLabel_`, and `playerMetaLabel_`.
- That meant the guest page was trusting UI label text rather than actual deck transport state.

## Current guest queue ordering before Phase 2.7
- The active guest queue came from `CrowdRequestState.queue()` in `src/analysis/crowd_request_server.py`.
- Visible queue rows were ordered only by status, then votes, then submit time:
  - `PENDING`
  - `ACCEPTED`
  - `HANDED_OFF`
  - `HANDOFF_FAILED`
  - `REJECTED`
- The guest now-playing block was rendered separately and did not affect queue ordering.

## Drift points before Phase 2.7
- A request could be `HANDED_OFF` and actually playing, but still appear in the visible queue because there was no playback-driven queue reconciliation.
- A request could be live on Deck A or Deck B while the guest page still showed stale text from the native labels.
- Both decks loaded or both decks playing could not be represented honestly because the sidecar only held a single generic now-playing payload.
- Non-request playback could not be represented without confusing the queue because no playback/request mapping existed.

## Phase 2.7 implementation direction
- Keep the existing sidecar and queue model.
- Upgrade `/operator/now-playing` from a label dump into a deck-aware playback reconciliation input.
- Use the real native playback authority already present in `EngineBridge`:
  - `deckIsPlaying(int)`
  - `deckFilePath(int)`
  - `deckTrackLabel(int)`
- Match playback to requests by strongest identity first:
  1. exact file path
  2. fallback normalized artist/title only when exact path is unavailable
- Add the smallest additional persisted states needed:
  - `NOW_PLAYING`
  - `PLAYED`

## Phase 2.7 result
- `HANDED_OFF` now still means loaded and verified, not playing.
- `NOW_PLAYING` is only created from actual deck playback state.
- `PLAYED` is recorded when playback moves away from a request-backed live track.
- The guest page now separates actual now-playing decks from the remaining queue.
- A live request does not appear both in Now Playing and the queue.
- If both decks are active and the mapping is ambiguous, the guest page stays explicit and does not create a false request mapping.