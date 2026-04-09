# Crowd Requests Phase 2.8: Event-Driven Deck Authority

Phase 2.8 keeps the Phase 2.6 and 2.7 request model intact, but moves crowd-request playback truth closer to native deck events.

## Native event path

- `EngineBridge::djSnapshotUpdated()` remains the primary deck-state event source.
- `AncillaryScreensWidget` now listens to that signal and uses it for three targeted jobs:
  - handoff verification against deck path plus decode readiness
  - now-playing payload pushes into the sidecar
  - fast queue refresh after authoritative deck changes
- The old `requestPollTimer_` remains as a 3 second fallback poll only.

## Handoff verification

- Old behavior: `verifyDeckHandoff()` retried every 250 ms until timeout.
- Phase 2.8 behavior: accepted requests enter a pending verification set.
- Each snapshot event checks the deck's reported file path and decode-ready state.
- A 400 ms fallback timer only runs while pending verifications exist and only matters if snapshot events stall.

## Cross-deck authority rules

Guest-facing now-playing is driven by the strongest trustworthy live deck signal.

1. If exactly one deck is actively playing, that deck is authoritative.
2. If both decks are active and the previous authoritative deck is still strong enough, hold that authority until a clear winner appears.
3. If one active deck has a clearly stronger live signal bucket or peak margin, that deck becomes authoritative.
4. If overlap stays too close to call, fail closed:
   - do not claim a new authoritative now-playing deck
   - do not promote a request to `NOW_PLAYING`
   - keep request rows in `HANDED_OFF` until the signal is strong enough

## Queue truth model

- `HANDED_OFF` still means loaded and verified, not live.
- `NOW_PLAYING` only tracks the authoritative live deck.
- `PLAYED` is assigned when playback authority moves away from the request-backed track.
- Non-authoritative active decks remain visible in the sidecar now-playing payload as standby or ambiguous overlap, but they do not own guest-facing truth.

## Guest page behavior

- The guest page still polls because it has no push transport.
- Native deck events now update sidecar truth immediately.
- Guest polling is tightened to 1 second so visible lag is smaller without redesigning the stack.