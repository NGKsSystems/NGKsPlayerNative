# Phase 2.9 Identity Model

The crowd-request workflow now uses a deterministic identity bundle instead of allowing artist/title fallback to define queue truth.

## Canonical Fields

- `stable_identity_key`: primary workflow identity. Format: `PATH::<normalized_runtime_path>`.
- `file_path_normalized`: normalized runtime path used for exact comparisons and audit output.
- `authority_track_id`: reconciled authority DB track id when a deterministic bridge exists.
- `identity_confidence`: `strong`, `reconciled`, `degraded`, or `unresolved`.
- `identity_match_basis`: records how authority or playback reconciliation was reached.

## Matching Order

1. `stable_identity_key`
2. `authority_track_id`
3. `file_path_normalized`
4. Exact normalized `artist/title` only as degraded metadata, never as authoritative playback promotion for strong requests

## Reconciliation Rules

- Exact runtime path to authority DB path is `strong` with `identity_match_basis=db_path`.
- Exact unique filename reconciliation is `reconciled` with `identity_match_basis=db_filename`.
- Exact unique normalized artist/title reconciliation is `degraded` with `identity_match_basis=artist_title`.
- Ambiguous or missing matches remain unresolved and must fail closed.

## Operational Effect

- Request creation forwards the identity bundle from native search and guest search.
- Now-playing updates publish the same bundle from the live deck.
- The sidecar promotes requests to `NOW_PLAYING` only on exact identity matches.
- Artist/title-only collisions are preserved as queue state and audit detail instead of being silently promoted.