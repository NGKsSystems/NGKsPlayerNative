# Override Manager — App-Facing API Contract

**Version:** 1.0  
**Date:** 2026-04-03  

---

## Data Objects

### RowIdentity

```python
{
    "filename": str,      # PRIMARY KEY — stable across re-analysis
    "artist": str,        # secondary verification
    "title": str,         # secondary verification
    "row_number": int     # display only, not authoritative
}
```

### OverridePayload

```python
{
    "override_bpm": float | None,   # 20.0–300.0 or None
    "override_key": str | None,     # Camelot code (e.g. "8B") or Western name
    "override_scope": str,          # "BPM" | "KEY" | "BPM_AND_KEY"
    "bpm_reason": str,              # free text
    "key_reason": str,              # free text
    "notes": str,                   # free text
    "entered_by": str               # user identifier
}
```

### ValidationResult

```python
{
    "status": str,                  # "VALID" | "INVALID" | "CONFLICT"
    "messages": list[str],          # per-field messages
    "conflict_flag": bool,
    "bpm_valid": bool,
    "key_valid": bool,
    "scope_valid": bool,
    "identity_valid": bool
}
```

### MergeBatchResult

```python
{
    "batch_id": str,
    "timestamp": str,               # ISO
    "submitted_count": int,
    "applied_count": int,
    "skipped_invalid": int,
    "skipped_conflict": int,
    "merged_export_path": str,
    "status": str                   # "SUCCESS" | "PARTIAL" | "FAILED"
}
```

### EffectiveRow

```python
{
    "row_identity": RowIdentity,
    "base_values": {
        "bpm": float,
        "key": str,
        "key_name": str,
        "bpm_trust": str,
        "key_trust": str,
        "confidence_tier": str,
        "review_required": bool
    },
    "override_values": {            # None if no active override
        "bpm": float | None,
        "key": str | None,
        "scope": str,
        "state": str
    } | None,
    "effective_values": {
        "bpm": float,
        "key": str,
        "bpm_source": str,         # "BASE" | "OVERRIDE"
        "key_source": str          # "BASE" | "OVERRIDE"
    }
}
```

### OverrideError

```python
{
    "code": str,                    # machine-readable error code
    "message": str,                 # human-readable description
    "details": dict | None,         # additional context
    "recoverable": bool             # can user fix and retry?
}
```

Error codes:

| Code | Meaning | Recoverable |
|---|---|---|
| `ROW_NOT_FOUND` | Filename not in base export | Yes |
| `DUPLICATE_ACTIVE_DRAFT` | Active override already exists for this row | Yes |
| `INVALID_PAYLOAD` | Missing/malformed override fields | Yes |
| `INVALID_TRANSITION` | State machine forbids this transition | No |
| `CONFLICT_DETECTED` | Identity mismatch vs base export | Yes |
| `VALIDATION_FAILED` | Override values failed validation | Yes |
| `MERGE_FAILED` | Merge write failed | Yes |
| `STALE_IDENTITY` | Row no longer in current base export | Yes |
| `OVERRIDE_NOT_FOUND` | No override with given ID | No |
| `BASE_EXPORT_MISSING` | Base export file not found | No |

---

## Operations

### 1. `load_review_queue()`

Loads all base export rows needing review.

- **Inputs:** none
- **Outputs:** `list[dict]` — base rows where `ReviewRequired == "True"`
- **Validation:** base export must exist
- **Errors:** `BASE_EXPORT_MISSING`
- **Side effects:** none
- **Persistence:** none (read-only)
- **Audit:** none

### 2. `list_overrides(filter_state=None)`

Lists all override entries, optionally filtered by state.

- **Inputs:** `filter_state: str | None` — OverrideState value or None for all
- **Outputs:** `list[OverrideEntry]` as dicts
- **Validation:** if filter_state given, must be valid OverrideState
- **Errors:** `INVALID_PAYLOAD` if bad filter_state
- **Side effects:** none
- **Persistence:** reads override snapshot
- **Audit:** none

### 3. `get_override(row_identity)`

Gets the active override for a specific row.

- **Inputs:** `row_identity: RowIdentity` (filename is primary lookup key)
- **Outputs:** `OverrideEntry` as dict, or None
- **Validation:** filename must be non-empty
- **Errors:** `INVALID_PAYLOAD`, `OVERRIDE_NOT_FOUND`
- **Side effects:** none
- **Persistence:** reads override snapshot
- **Audit:** none

### 4. `create_override_draft(row_identity, payload)`

Creates a new override entry in DRAFT state.

- **Inputs:** `row_identity: RowIdentity`, `payload: OverridePayload`
- **Outputs:** `OverrideEntry` as dict
- **Validation:**
  - row must exist in base export
  - no active override for this row (not DISABLED/SUPERSEDED)
  - payload fields valid (BPM range, key format, scope)
- **Errors:** `ROW_NOT_FOUND`, `DUPLICATE_ACTIVE_DRAFT`, `INVALID_PAYLOAD`
- **Side effects:** creates override entry
- **Persistence:** appends event, writes snapshot
- **Audit:** emits `NEW → DRAFT` event (two events: creation + edit)

### 5. `update_override_draft(override_id, payload)`

Updates an existing DRAFT override.

- **Inputs:** `override_id: str`, `payload: OverridePayload`
- **Outputs:** `OverrideEntry` as dict (updated)
- **Validation:**
  - override must exist
  - override must be in DRAFT state
  - payload fields valid
- **Errors:** `OVERRIDE_NOT_FOUND`, `INVALID_TRANSITION`, `INVALID_PAYLOAD`
- **Side effects:** updates override values
- **Persistence:** appends event, writes snapshot
- **Audit:** emits `DRAFT → DRAFT` event

### 6. `validate_override(override_id)`

Runs validation on a DRAFT override.

- **Inputs:** `override_id: str`
- **Outputs:** `ValidationResult`
- **Validation:**
  - override must exist
  - override must be in DRAFT state
- **Errors:** `OVERRIDE_NOT_FOUND`, `INVALID_TRANSITION`
- **Side effects:** transitions state to VALID/INVALID/CONFLICT
- **Persistence:** appends event(s), writes snapshot
- **Audit:** emits `DRAFT → PENDING_VALIDATION` then `PENDING_VALIDATION → VALID/INVALID/CONFLICT`

### 7. `approve_override(override_id, approver)`

Approves a VALID override for merge.

- **Inputs:** `override_id: str`, `approver: str`
- **Outputs:** `OverrideEntry` as dict
- **Validation:**
  - override must exist
  - override must be in VALID state
- **Errors:** `OVERRIDE_NOT_FOUND`, `INVALID_TRANSITION`
- **Side effects:** transitions state to APPROVED
- **Persistence:** appends event, writes snapshot
- **Audit:** emits `VALID → APPROVED` event

### 8. `apply_approved_overrides(batch_label, user)`

Merges all APPROVED overrides into a new export.

- **Inputs:** `batch_label: str`, `user: str`
- **Outputs:** `MergeBatchResult`
- **Validation:**
  - at least 1 APPROVED override exists
  - base export must exist
  - re-validates all APPROVED overrides before applying
- **Errors:** `MERGE_FAILED`, `BASE_EXPORT_MISSING`
- **Side effects:**
  - writes merged export CSV
  - transitions each applied override to APPLIED
  - records merge batch
- **Persistence:** appends events, writes snapshot, writes merge history, writes merged CSV
- **Audit:** emits `APPROVED → APPLIED` for each override + merge batch record

### 9. `disable_override(override_id, reason)`

Disables an override (soft delete).

- **Inputs:** `override_id: str`, `reason: str`
- **Outputs:** `OverrideEntry` as dict
- **Validation:**
  - override must exist
  - override must be in a state allowing DISABLED transition
- **Errors:** `OVERRIDE_NOT_FOUND`, `INVALID_TRANSITION`
- **Side effects:** transitions to DISABLED
- **Persistence:** appends event, writes snapshot
- **Audit:** emits `* → DISABLED` event

### 10. `get_override_history(row_identity)`

Gets all audit events for a given row.

- **Inputs:** `row_identity: RowIdentity`
- **Outputs:** `list[AuditEvent]` as dicts, sorted by timestamp
- **Validation:** filename must be non-empty
- **Errors:** `INVALID_PAYLOAD`
- **Side effects:** none
- **Persistence:** reads event log
- **Audit:** none

### 11. `get_merge_history()`

Gets all merge batch records.

- **Inputs:** none
- **Outputs:** `list[MergeBatchResult]`
- **Validation:** none
- **Errors:** none
- **Side effects:** none
- **Persistence:** reads merge history
- **Audit:** none

### 12. `get_effective_row(row_identity)`

Returns the effective values for a row (base vs overridden).

- **Inputs:** `row_identity: RowIdentity`
- **Outputs:** `EffectiveRow`
- **Validation:** row must exist in base export
- **Errors:** `ROW_NOT_FOUND`
- **Side effects:** none
- **Persistence:** reads base export + override snapshot
- **Audit:** none
