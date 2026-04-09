# Ancillary Screens Phase 2 Gap List

This list is intentionally explicit about what still remains across the four ancillary screens after the Crowd Requests Phase 2.6 request-to-deck integration plus UX tightening pass.

## missing_backend
- Hardware Integration: no real MIDI mapping backend, no controller-specific configuration backend, no DVS signal/control backend.
- Streaming Music Services: no external service authentication, no platform API search, no playlist/recommendation backend, no remote stream-resolution backend.
- Live Streaming Setup: no broadcaster process integration or capture-verification backend.
- Crowd Requests: live connection metrics and optional feedback/tips backend remain unimplemented. Guest-side now-playing is now available when the native operator app is online.

## missing_interaction
- Hardware Integration: Configure and LED Sync buttons are still non-operational; DVS and MIDI panes are mostly informational.
- Streaming Music Services: service connect/disconnect cards are still visual toggles rather than authenticated sessions.
- Live Streaming Setup: software choice changes instructions but does not drive software-specific integration checks.
- Crowd Requests: guest and operator queue updates use near-live polling rather than push. QR is now present in both the guest page and operator UI, and request acceptance now records explicit `HANDOFF_FAILED` versus `HANDED_OFF` states.

## missing_state_persistence
- Hardware settings are not persisted.
- Streaming service connection state is not persisted as authenticated sessions.
- Live streaming software/theme/resolution profile is not persisted as an operator preset.
- Crowd Requests queue/policy/payment handles are now persisted. Any future audience feedback/tips state is still not implemented.

## missing_navigation/polish
- Donor React routes were split across dedicated screens, while the native port still consolidates all ancillary workflows into one stacked page.
- Crowd Requests has no explicit operator diagnostics panel for bind failures, token mismatches, deck-handoff troubleshooting, or server log tailing.
- Crowd Requests search quality now consults `dj_library_core.db`, but the current runtime `library.json` has no direct file-path overlap with that DB, so enrichment still relies on normalized artist/title fallback rather than exact path joins.
- Streaming lacks setup/help surfaces comparable to donor setup/auth flows.
- Live Streaming Setup lacks saved profile recall and capture-health confirmation.

## missing_operator tools
- Hardware Integration lacks actionable controller diagnostics and mapping tools.
- Streaming Music Services lacks operator-facing auth/session diagnostics and playlist shortcuts.
- Crowd Requests lacks feedback/tips moderation, connection-count visibility, and a richer queue triage surface beyond the current accept/reject/remove plus deck A/B handoff actions.
- Live Streaming Setup lacks profile presets, scene/capture validation, and stream-readiness diagnostics.

## missing security / local-network controls
- Crowd Requests now enforces local/private-network clients, operator-token headers, input validation, duplicate-request protection, per-client request cooldowns, and vote cooldowns, but the UI does not yet expose a LAN-mode/local-bind choice explicitly.
- Crowd Requests guest and operator flows are still local-token based only; there is no richer operator-session UX or token rotation surface in the UI.
- Streaming and Live Streaming Setup do not yet expose security diagnostics because their external integrations are not wired.

## priority order
1. Streaming real service integrations.
2. Hardware MIDI/controller backend integration.
3. Crowd Requests connection metrics, richer operator diagnostics, and better runtime-library/core-DB alignment.
4. Live Streaming profile persistence and capture verification.
