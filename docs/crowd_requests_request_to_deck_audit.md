# Crowd Requests Request-to-Deck Audit

## Current UI action
- Operator selects a row in the Crowd Requests queue and clicks `Accept to Deck A` or `Accept to Deck B` in `src/ui/AncillaryScreensWidget.h`.
- The UI first calls the local sidecar `/operator/accept`, which moves the request to `ACCEPTED` in the SQLite queue.
- The native UI then invokes the shared `trackLoader_` callback with the resolved local file path and target deck index.
- After dispatch, the UI polls `EngineBridge::deckFilePath(deckIndex)` and `EngineBridge::isDeckFullyDecoded(deckIndex)` before it records `HANDED_OFF`.
- If dispatch fails, the deck never reports the requested path, or decode readiness never completes, the UI records `HANDOFF_FAILED` with an explicit reason through `/operator/handoff`.

## Current backend behavior
- `src/analysis/crowd_request_server.py` persists requests in SQLite and now supports the full request status set: `PENDING`, `ACCEPTED`, `HANDED_OFF`, `HANDOFF_FAILED`, `REJECTED`, and `REMOVED`.
- Every operator transition is audited in `crowd_request_audit`.
- `/operator/accept` only moves eligible requests into `ACCEPTED`.
- `/operator/handoff` is the audited bridge from native deck verification back into persisted queue state.
- Queue reads now expose handoff metadata (`handoff_deck`, `handoff_detail`, `handoff_target_path`) so guest and operator surfaces can distinguish accepted, handed-off, and failed-handoff items honestly.

## Actual deck integration points
- `src/ui/main.cpp` resolves the request file path against the native `allTracks_` library and enters DJ mode before any deck mutation.
- The real deck handoff target is `EngineBridge::loadTrackToDeck(deckIndex, filePath)`.
- `EngineBridge::loadTrackToDeck` dispatches into `EngineCore::loadFileIntoDeck`, which performs the actual decode/load path for Deck A or Deck B.
- `EngineBridge::deckFilePath(deckIndex)` and `EngineBridge::isDeckFullyDecoded(deckIndex)` are the safest existing confirmation points because they report whether the requested file actually reached the target deck and finished decode readiness.

## Pre-Phase-2.6 blockers
- The old `trackLoader_` contract returned `void`, so the queue UI could not distinguish dispatch failure from success.
- The queue accepted requests optimistically and updated operator text before the deck path was verified.
- The backend status model stopped at `ACCEPTED`, so it could not persist or audit real handoff results.
- Search quality relied mostly on `library.json` text fields and had no persisted place to surface handoff metadata or anti-spam policy.

## Implemented plan
1. Extend the SQLite schema and API so the request backend can persist `HANDED_OFF` and `HANDOFF_FAILED` transitions with audit detail.
2. Change the native `trackLoader_` contract to return an explicit dispatch result instead of relying on message boxes.
3. Keep `/operator/accept` as the queue-state gate, then verify the actual deck state before promoting to `HANDED_OFF`.
4. Fail closed: if file resolution, DJ mode entry, deck dispatch, deck path confirmation, or decode readiness fails, record `HANDOFF_FAILED` with the real reason.
5. Surface the result back into the operator queue with status counts and explicit status coloring.

## Remaining blockers / deferred items
- There is still no dedicated operator diagnostics panel that streams the deck or sidecar logs in-app.
- `data/runtime/library.json` currently has zero direct file-path overlap with `data/dj_library_core.db`, so search enrichment must fall back to normalized artist/title matching instead of exact path joins.
- Guest queue updates remain polling-based rather than push-based.
- Feedback/tips flows and live connection metrics remain deferred.