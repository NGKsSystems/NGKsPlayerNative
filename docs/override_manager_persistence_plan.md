# NGKsPlayerNative — Override Manager Persistence Plan

**Version:** 1.0
**Date:** 2026-04-03

---

## Storage Approach Decision

**Chosen approach: JSONL event log + JSON state snapshot**

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| CSV + JSON sidecar | Familiar, human-readable | No atomicity, multi-file sync issues | ❌ |
| SQLite | Fast queries, ACID | Heavy for <1000 rows, overkill | ❌ |
| **JSONL event log + JSON snapshot** | Append-only audit for free, reconstructable state, simple I/O, one snapshot file for fast load | Two files to manage | ✅ |

**Justification:** The override system operates on ~73–907 rows. JSONL gives us an
immutable, append-only audit trail that doubles as the event log. The JSON snapshot
file gives fast load times for the UI without replaying all events. On every state
change, we append to the JSONL first, then re-write the snapshot. If the snapshot is
corrupted, we replay the JSONL to reconstruct it.

---

## File Layout

```
data/
└── overrides/
    ├── override_events.jsonl         # Append-only audit log (source of truth)
    ├── override_state.json           # Current state snapshot (derived, reconstructable)
    ├── override_state.json.bak       # Previous snapshot (crash recovery)
    ├── merge_history.jsonl           # Merge batch records
    └── backups/
        ├── override_events_20260403_101500.jsonl.bak
        └── override_state_20260403_101500.json.bak
```

---

## Layer 1: Base Export (READ-ONLY)

| Attribute | Value |
|---|---|
| File | `_artifacts/exports/NGKs_final_analyzer_export.csv` |
| Rows | 907 |
| Fields | 40 |
| Access | READ-ONLY. Never modified by override system. |
| Role | Source of truth for analyzer values. |

The base export is loaded once at app startup or when explicitly refreshed.
Override system NEVER writes to this file.

---

## Layer 2: Override Events Log (APPEND-ONLY)

**File:** `data/overrides/override_events.jsonl`

Each line is a JSON object representing one `AuditEvent`:

```json
{
  "event_id": "evt-aaaa-1111",
  "override_id": "ovr-bbbb-2222",
  "row_identity_key": "DJ LOA - Don't Say Goodbye Remix.mp3",
  "timestamp": "2026-04-03T10:15:02Z",
  "state_before": "NEW",
  "state_after": "DRAFT",
  "trigger": "USER_EDIT",
  "user": "NGK",
  "reason": "Initial BPM override entry",
  "bpm_before": null,
  "bpm_after": 128.0,
  "key_before": null,
  "key_after": null,
  "merge_batch_id": null
}
```

**Write protocol:**
1. Serialize event to JSON string
2. Open file in append mode (`"a"`)
3. Write JSON + `"\n"`
4. Flush and fsync
5. Close

This file is NEVER rewritten or truncated. It is the source of truth.

---

## Layer 3: Override State Snapshot (DERIVED)

**File:** `data/overrides/override_state.json`

Contains the current state of all override entries:

```json
{
  "version": 1,
  "last_event_id": "evt-aaaa-9999",
  "last_updated": "2026-04-03T10:20:00Z",
  "overrides": {
    "ovr-bbbb-2222": {
      "override_id": "ovr-bbbb-2222",
      "revision": 1,
      "row_identity": {
        "filename": "DJ LOA - Don't Say Goodbye Remix.mp3",
        "artist": "DJ LOA",
        "title": "Don't Say Goodbye Remix",
        "row_number": 294
      },
      "state": "DRAFT",
      "override_bpm": 128.0,
      "override_key": null,
      "override_scope": "BPM",
      "bpm_reason": "Verified via Tunebat",
      "key_reason": null,
      "notes": null,
      "entered_by": "NGK",
      "validation_message": null,
      "conflict_flag": false,
      "original_bpm": 120.2,
      "original_key": "3A",
      "created_at": "2026-04-03T10:15:02Z",
      "updated_at": "2026-04-03T10:15:30Z",
      "validated_at": null,
      "approved_at": null,
      "applied_at": null,
      "disabled_at": null,
      "merge_batch_id": null,
      "superseded_by": null
    }
  }
}
```

**Write protocol (atomic rename):**
1. Serialize full state to JSON
2. Write to temp file: `override_state.json.tmp`
3. Rename existing `override_state.json` → `override_state.json.bak`
4. Rename `override_state.json.tmp` → `override_state.json`
5. Delete `.bak` only after confirming `.json` exists

If the app crashes between steps 3 and 4, recovery finds `.bak` and `.tmp`,
uses `.tmp` (more recent). If it crashes before step 3, `.json` is still
intact from the previous write.

---

## Layer 4: Merge History (APPEND-ONLY)

**File:** `data/overrides/merge_history.jsonl`

Each line is a JSON object representing one `MergeBatch`:

```json
{
  "batch_id": "MB-001",
  "timestamp": "2026-04-03T10:20:00Z",
  "user": "NGK",
  "base_export_path": "_artifacts/exports/NGKs_final_analyzer_export.csv",
  "base_export_hash": "sha256:abc123...",
  "submitted_count": 3,
  "applied_count": 3,
  "skipped_invalid_count": 0,
  "skipped_conflict_count": 0,
  "merged_export_path": "_artifacts/exports/NGKs_final_analyzer_export_OVERRIDDEN.csv",
  "merged_export_hash": "sha256:def456...",
  "override_ids_applied": ["ovr-bbbb-2222", "ovr-cccc-3333", "ovr-dddd-4444"],
  "status": "SUCCESS",
  "rollback_available": true,
  "rolled_back_at": null
}
```

---

## Layer 5: Merged Export (WRITE-ONCE PER MERGE)

| Attribute | Value |
|---|---|
| File | `_artifacts/exports/NGKs_final_analyzer_export_OVERRIDDEN.csv` |
| Rows | 907 (same as base) |
| Fields | 49 (40 base + 9 override audit fields) |
| Access | Written by merge flow. Read by Library View. |

**Write protocol (atomic rename):**
1. Open temp file: `..._OVERRIDDEN.csv.tmp`
2. Write all 907 rows (applying overrides inline)
3. Flush and fsync
4. Rename `.csv.tmp` → `.csv`

If the temp file already exists from a prior failed merge, it is deleted first.

---

## Row Identity & Matching Rules

### Primary Key
**Filename** is the authoritative identifier. It is the most stable field across
re-analysis runs (artist/title can change if metadata is corrected).

```python
identity_key = row["Filename"]  # e.g. "DJ LOA - Don't Say Goodbye Remix.mp3"
```

### Secondary Verification
Artist + Title are checked for soft consistency. A mismatch triggers CONFLICT
state rather than blocking silently.

### Matching Algorithm

```
1. Load base export (907 rows)
2. Build lookup: { filename → base_row }
3. For each override:
   a. Match by filename (exact, case-insensitive)
   b. If match found:
      - Check artist (case-insensitive, stripped): warn if different
      - Check title (case-insensitive, stripped): warn if different
      - If both artist+title match: IDENTITY_MATCH
      - If only filename matches: IDENTITY_SOFT_MATCH (warning, not blocking)
   c. If no match:
      - IDENTITY_MISSING → override becomes CONFLICT
```

### Re-Analysis Remapping

When the base export is re-generated after re-analysis:
1. Load new base export
2. For each existing override in store:
   a. Try to match by filename
   b. If filename found: update row_number if it changed
   c. If filename NOT found: mark override as CONFLICT with reason "Row no longer in base export"
3. Write reconciliation audit events

### Duplicate Handling

- Only ONE active override per filename at a time
- "Active" = state NOT IN (DISABLED, SUPERSEDED)
- Creating a new override for a filename that already has an APPLIED override:
  - Existing APPLIED override → SUPERSEDED
  - New override → NEW (revision incremented)
  - Audit events recorded for both

---

## Draft / Approval Two-Layer Model

### Draft Layer
- States: NEW, DRAFT, INVALID, CONFLICT
- Freely editable by user
- No impact on library view or merged exports
- Can be discarded without consequences

### Approval Layer
- States: VALID, APPROVED, APPLIED
- VALID = passed automated checks, awaiting human approval
- APPROVED = human-confirmed, ready for merge
- APPLIED = included in a merged export
- Transitions from approval layer back to draft layer (re-edit) create audit trail

### Revision Behavior

When an APPLIED override needs to be changed:
1. User clicks "Create New Revision" on the override
2. Existing APPLIED override → SUPERSEDED
3. New override entry created at revision N+1 in DRAFT state
4. New entry inherits: row_identity, original values, previous override values as starting point
5. New entry starts its own lifecycle: DRAFT → VALID → APPROVED → APPLIED

---

## Crash Recovery Scenarios

### Scenario 1: Crash during event append

**Symptom:** Last line of `override_events.jsonl` is truncated JSON.

**Recovery:**
1. Read file line by line
2. Attempt JSON parse on each line
3. If last line fails to parse: truncate file to remove incomplete line
4. Log recovery event
5. UI state matches last valid event

### Scenario 2: Crash during snapshot write

**Symptom:** `override_state.json.tmp` exists, or `override_state.json` is
missing but `.bak` exists.

**Recovery matrix:**

| `.json` exists | `.tmp` exists | `.bak` exists | Action |
|---|---|---|---|
| ✓ | ✗ | ✗ | Normal. Use `.json`. |
| ✓ | ✓ | ✗ | Delete `.tmp` (stale). Use `.json`. |
| ✗ | ✓ | ✓ | Rename `.tmp` → `.json`. Delete `.bak`. |
| ✗ | ✗ | ✓ | Rename `.bak` → `.json`. |
| ✗ | ✗ | ✗ | Rebuild from `override_events.jsonl`. |

### Scenario 3: Crash during merge write

**Symptom:** `..._OVERRIDDEN.csv.tmp` exists.

**Recovery:**
1. Delete the `.tmp` file
2. Override states remain APPROVED (not APPLIED — state change is done AFTER successful write)
3. User can re-run merge

### Scenario 4: Event log and snapshot disagree

**Symptom:** `last_event_id` in snapshot doesn't match last event in JSONL.

**Recovery:**
1. Replay all events from JSONL after the snapshot's `last_event_id`
2. Apply them to rebuild current state
3. Write corrected snapshot

### Scenario 5: Corrupt event log

**Symptom:** Multiple unparseable lines, or file is empty.

**Recovery:**
1. If snapshot exists and is valid: use snapshot as current state
2. Mark all overrides for re-validation
3. Write recovery audit event
4. Alert user that some audit history may be lost

---

## Backup Strategy

### Automatic Backups
- Before every merge: copy event log and snapshot to `backups/` with timestamp
- Retain last 10 backups
- Backups are timestamped: `override_events_YYYYMMDD_HHMMSS.jsonl.bak`

### Manual Export
- User can export full override state as a ZIP from Override History panel
- Contains: event log, snapshot, merge history, base export hash

---

## File Size Estimates

| File | Growth Pattern | Estimated Size (1 year) |
|---|---|---|
| `override_events.jsonl` | ~500 bytes/event, ~20 events/day assumed | ~3.7 MB |
| `override_state.json` | Rewritten each time, ~1KB/override | ~73 KB (all review rows overridden) |
| `merge_history.jsonl` | ~500 bytes/merge, ~1 merge/week | ~26 KB |
| `backups/` | ~150 KB/backup × 10 retained | ~1.5 MB |

Total storage: under 10 MB/year. No pruning or archival needed at this scale.

---

## Implementation Priority

1. **Event log writer** (Layer 2) — must exist before any state changes
2. **State snapshot reader/writer** (Layer 3) — needed for UI load
3. **Recovery module** — crash recovery checks on startup
4. **Merge writer** (Layer 5) — atomic rename merge flow
5. **Merge history** (Layer 4) — batch tracking
6. **Backup module** — pre-merge backups
