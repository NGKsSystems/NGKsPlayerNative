# NGKsPlayerNative — Manual Override Schema

**Version:** 1.0
**Date:** 2026-04-02

---

## Overview

The override system provides a safe, auditable, non-destructive way for
humans to correct BPM and/or Key values in the final analyzer export.
Overrides are stored in a separate CSV, validated before merge, and
applied deterministically to produce a merged export.

---

## Override File Format

### Identity Fields (matching base export)

| Field | Type | Required | Description |
|---|---|---|---|
| Row | int | YES | Row index from base export (1-based) |
| Artist | str | YES | Must match base export for identity verification |
| Title | str | YES | Must match base export for identity verification |
| Filename | str | YES | Must match base export for identity verification |

### Override Input Fields

| Field | Type | Required | Description |
|---|---|---|---|
| OverrideFinalBPM | float | NO | Corrected BPM value. Blank = no override. |
| OverrideFinalKey | str | NO | Corrected key in Camelot (e.g. "10B") or Western (e.g. "D major"). Blank = no override. |
| OverrideBPMReason | str | NO | Why BPM was overridden (e.g. "Tunebat says 128") |
| OverrideKeyReason | str | NO | Why Key was overridden (e.g. "Verified D major via Tunebat") |
| OverrideEnteredBy | str | NO | Who entered the override (e.g. "NGK", "DJ_Review") |
| OverrideDate | str | NO | ISO date of override entry |
| OverrideNotes | str | NO | Free-text notes |

### Validation / Merge Control Fields

| Field | Type | Required | Description |
|---|---|---|---|
| OverrideEnabled | bool | YES | TRUE to apply, FALSE to skip |
| OverrideScope | str | YES | BPM / KEY / BPM_AND_KEY |
| OverrideStatus | str | AUTO | Set by validation: PENDING / VALID / INVALID / APPLIED / CONFLICT |
| OverrideConflictFlag | bool | AUTO | TRUE if conflict detected |
| OverrideValidationMessage | str | AUTO | Human-readable validation result |

---

## Override Rules

### Scope Rules
- `BPM` — Only OverrideFinalBPM is applied. OverrideFinalKey ignored even if present.
- `KEY` — Only OverrideFinalKey is applied. OverrideFinalBPM ignored even if present.
- `BPM_AND_KEY` — Both fields applied if present and valid.

### Blank Field Behavior
- Blank OverrideFinalBPM with scope BPM → INVALID (scope says BPM but no value)
- Blank OverrideFinalKey with scope KEY → INVALID (scope says KEY but no value)
- Blank OverrideFinalBPM with scope BPM_AND_KEY → only Key applied (partial)
- Blank OverrideFinalKey with scope BPM_AND_KEY → only BPM applied (partial)

### OverrideEnabled Behavior
- TRUE / 1 / yes / Y → enabled
- FALSE / 0 / no / N → disabled, row skipped entirely
- Blank → treated as FALSE

---

## Validation Rules

### Identity Validation
- Row must exist in base export (1-based index)
- Artist must match base export row (case-insensitive trim)
- Title must match base export row (case-insensitive trim)
- Filename should match base export row
- Identity mismatch → CONFLICT

### BPM Validation
- Must be numeric
- Must be in range (20, 300) exclusive
- Decimals allowed (one decimal place)

### Key Validation
- Must resolve to a valid Camelot code (1A-12A, 1B-12B)
- Accepted input formats:
  - Camelot: "10B", "7A"
  - Western: "D major", "A minor"
  - Flat notation auto-converted: "Bb major" → "6B"

### Duplicate Detection
- Multiple override rows for the same Row number → CONFLICT
- Last row wins only if explicitly flagged

---

## Merged Export Appended Fields

The merged export keeps ALL base export fields and appends:

| Field | Type | Description |
|---|---|---|
| OverrideApplied | bool | TRUE if any override was applied to this row |
| OverrideTypeApplied | str | NONE / BPM / KEY / BPM_AND_KEY |
| OverrideBPMApplied | float | The BPM override value that was applied (blank if none) |
| OverrideKeyApplied | str | The Key override value that was applied (blank if none) |
| OverrideReasonSummary | str | Combined reason text |
| OverrideSource | str | Who entered the override |
| OverrideAuditStatus | str | NOT_OVERRIDDEN / OVERRIDE_APPLIED / OVERRIDE_SKIPPED_INVALID / OVERRIDE_SKIPPED_CONFLICT |
| FinalBPM_Original | float | Pre-override FinalBPM (for audit trail) |
| FinalKey_Original | str | Pre-override FinalKey (for audit trail) |

When an override is applied:
- `FinalBPM` is replaced with `OverrideFinalBPM`
- `FinalKey` is replaced with `OverrideFinalKey` (converted to Camelot)
- `FinalKeyName` is updated to match
- `FinalBPMDecisionSource` / `FinalKeyDecisionSource` set to `MANUAL_OVERRIDE`
- `FinalBPMTrustLevel` / `FinalKeyTrustLevel` set to `HIGH` (human-verified)
- `FinalBPMReviewFlag` / `FinalKeyReviewFlag` set to `False`
- Original values preserved in `FinalBPM_Original` / `FinalKey_Original`

---

## Workflow

```
1. Run final_export_builder.py → base export + review queue
2. Run override_template_builder.py → pre-seeded override template
3. Human edits template (fills OverrideFinalBPM, OverrideFinalKey, etc.)
4. Run override_validation.py → validates, writes status
5. Run override_merge_flow.py → produces merged export
6. Merged export ready for app integration
```

---

## File Locations

| File | Path |
|---|---|
| Override template | `_proof/manual_override_system/NGKs_override_template.csv` |
| Override input (edited) | `_proof/manual_override_system/NGKs_overrides.csv` |
| Merged export | `_proof/manual_override_system/NGKs_final_analyzer_export_OVERRIDDEN.csv` |
| Base export (READ-ONLY) | `_proof/final_export_schema/NGKs_final_analyzer_export.csv` |
