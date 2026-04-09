# NGKsPlayerNative — Override Manager UI Flow

**Version:** 1.0
**Date:** 2026-04-03

---

## Navigation Entry Point

The Override Manager is a top-level section in the app sidebar, positioned after
Library and before Settings.

```
[Sidebar]
  Library
  ▶ Override Manager        ← entry point
    ├── Review Queue
    ├── Pending Approvals
    ├── Merge / Apply
    └── Override History
  Settings
```

Alternatively, rows in the main Library View that have `ReviewRequired=True` or
`ManualOverrideEligible=True` show a badge/icon. Clicking the badge opens the
Override Detail panel for that row.

---

## Screen 1: Review Queue View

### Purpose
Show all rows from the base export that need human attention. Serves as the
primary entry point into the override workflow.

### Data Source
- Base export: `NGKs_final_analyzer_export.csv`
- Override store: `overrides.jsonl` (for status overlay)

### Columns Displayed

| Column | Source | Notes |
|---|---|---|
| Row | base export | Numeric index |
| Artist | base export | |
| Title | base export | |
| FinalBPM | base export | Current analyzer BPM |
| FinalKey | base export | Current analyzer Key (Camelot) |
| BPM Trust | base export | HIGH / MEDIUM / LOW |
| Key Trust | base export | HIGH / MEDIUM / LOW |
| Confidence Tier | base export | PRODUCTION / USABLE_WITH_CAUTION / REVIEW_REQUIRED |
| Review Reason | base export | Why this row needs review |
| Override Status | override store | NEW / DRAFT / VALID / APPROVED / APPLIED / — |

### Filters
- **Show:** All / Needs Review / Has Override / Has Applied Override
- **Sort:** by Row, by Artist, by Confidence Tier, by Override Status
- **Search:** free-text search on Artist, Title, Filename

### Actions
- **Click row** → Opens Override Detail / Editor Panel (Screen 2)
- **Bulk Select + "Create Overrides"** → Creates NEW override entries for selected rows
- **"Refresh from Export"** → Reloads base export, reconciles with override store

### State Transitions Triggered
- Viewing does not change state.
- "Create Override" on a row with no existing override: creates entry in NEW state.

---

## Screen 2: Override Detail / Editor Panel

### Purpose
View current analyzer values and enter/edit override values for a single row.

### Layout

```
┌─────────────────────────────────────────────────────┐
│  OVERRIDE EDITOR — Row 294                          │
│  Artist: DJ LOA                                     │
│  Title:  Don't Say Goodbye Remix                    │
│  File:   DJ LOA - Don't Say Goodbye Remix.mp3       │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌── Current Values (read-only) ──────────────────┐ │
│  │  FinalBPM:   120.2   Trust: HIGH               │ │
│  │  FinalKey:   3A      Trust: LOW     ← flagged  │ │
│  │  Confidence: REVIEW_REQUIRED                    │ │
│  │  Reason:     review: Key                        │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│  ┌── Override Values (editable) ──────────────────┐ │
│  │  Override BPM:  [________]                      │ │
│  │  BPM Reason:    [____________________]          │ │
│  │  Override Key:  [________]                      │ │
│  │  Key Reason:    [____________________]          │ │
│  │  Scope:         ( ) BPM  ( ) KEY  (•) BOTH     │ │
│  │  Notes:         [____________________]          │ │
│  │  Entered By:    [NGK_______________]            │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│  Status: DRAFT                                      │
│  Validation: (not yet validated)                    │
│                                                     │
│  [ Save Draft ]  [ Validate ]  [ Discard ]          │
│                                                     │
│  ┌── Validation Result (shown after validate) ────┐ │
│  │  (see Screen 3 inline)                          │ │
│  └────────────────────────────────────────────────┘ │
│                                                     │
│  ┌── Override History (collapsed) ────────────────┐ │
│  │  Rev 1: 2026-04-02  BPM: 128.0  APPLIED        │ │
│  │  Rev 2: 2026-04-03  Key: 8B     DRAFT ← current│ │
│  └────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

### Inputs
- Override BPM (float, range 20–300)
- Override Key (Camelot code or Western name)
- BPM Reason (free text)
- Key Reason (free text)
- Scope selector: BPM / KEY / BPM_AND_KEY
- Notes (free text)
- Entered By (auto-filled, editable)

### Outputs
- Updated override entry in override store
- Audit event logged

### Actions

| Action | Precondition | Effect |
|---|---|---|
| Save Draft | State is NEW or DRAFT | Saves values, transitions to DRAFT |
| Validate | State is DRAFT | Runs validation, transitions to VALID/INVALID/CONFLICT |
| Approve | State is VALID | Transitions to APPROVED |
| Discard | State is NEW, DRAFT, VALID, INVALID, CONFLICT | Transitions to DISABLED |
| Re-edit | State is VALID | Transitions back to DRAFT |
| Fix & Re-edit | State is INVALID or CONFLICT | Transitions back to DRAFT |

### State Transitions
```
NEW ──[Save Draft]──> DRAFT
DRAFT ──[Save Draft]──> DRAFT (re-save)
DRAFT ──[Validate]──> PENDING_VALIDATION ──> VALID / INVALID / CONFLICT
VALID ──[Approve]──> APPROVED
VALID ──[Re-edit]──> DRAFT
INVALID ──[Fix & Re-edit]──> DRAFT
CONFLICT ──[Fix Identity]──> DRAFT
Any editable state ──[Discard]──> DISABLED
```

### Inline Validation Messages
After clicking "Validate", the validation result panel (Screen 3) appears
inline below the editor, showing field-level errors.

---

## Screen 3: Validation Results Panel

### Purpose
Show validation outcome for the current override.

### Display Format

**Success (VALID):**
```
✓ VALID — Override passed all checks.
  BPM: 128.0 ✓ (in range 20–300)
  Key: 8B ✓ (valid Camelot code, C major)
  Scope: BPM_AND_KEY ✓ (matches provided fields)
  Identity: ✓ (Artist, Title, Filename match base export)
  [ Approve ]  [ Edit Again ]
```

**Failure (INVALID):**
```
✗ INVALID — 1 error found.
  BPM: 999 ✗ (out of range: must be 20–300)        ← BLOCKING
  Key: 8B ✓ (valid Camelot code)
  Scope: BPM ✗ (scope says BPM but value is invalid) ← BLOCKING
  Identity: ✓
  → Fix the BPM value and re-validate.
  [ Fix & Re-edit ]  [ Discard ]
```

**Conflict (CONFLICT):**
```
⚠ CONFLICT — Identity mismatch detected.
  Artist: override='WRONG_NAME' ≠ base='Bad Wolves' ← BLOCKING
  Title: ✓
  Filename: ✓
  → The override row does not match the base export row.
     Fix the identity fields or verify the correct row number.
  [ Fix Identity ]  [ Discard ]
```

### Error Semantics

| Severity | Meaning | Color | Blocks? |
|---|---|---|---|
| BLOCKING | Must be fixed before approval | Red (✗) | Yes |
| WARNING | Identity soft-mismatch (filename ok, artist/title differ) | Yellow (⚠) | No |
| OK | Field passed validation | Green (✓) | No |

### State Transitions
- VALID → user can Approve or Edit Again
- INVALID → user must Fix & Re-edit or Discard
- CONFLICT → user must Fix Identity or Discard

---

## Screen 4: Conflict Resolution Panel

### Purpose
Dedicated view for overrides in CONFLICT state. Shows side-by-side comparison
of override values vs base export values.

### Layout
```
┌─────────────────────────────────────────────────┐
│  CONFLICT RESOLUTION — Row 66                   │
├────────────────────┬────────────────────────────┤
│  Field             │  Override    │  Base Export │
├────────────────────┼─────────────┼─────────────┤
│  Artist            │  WRONG_NAME │  Bad Wolves  │ ← mismatch
│  Title             │  Zombie     │  Zombie      │ ← match
│  Filename          │  (match)    │  (match)     │
│  Row               │  66         │  66          │
├────────────────────┴─────────────┴─────────────┤
│                                                 │
│  Resolution options:                            │
│  ( ) Accept base export identity values         │
│  ( ) Manually correct override identity         │
│  ( ) This override targets a different row      │
│                                                 │
│  [ Apply Resolution ]  [ Discard Override ]     │
└─────────────────────────────────────────────────┘
```

### Actions
- **Accept base identity** → copies Artist/Title/Filename from base export into override, transitions to DRAFT for re-validation
- **Manually correct** → opens editor with identity fields editable, transitions to DRAFT
- **Different row** → opens row picker, transitions to DRAFT with new row identity
- **Discard** → transitions to DISABLED

---

## Screen 5: Merge / Apply Overrides Panel

### Purpose
Batch-apply approved overrides to produce a new merged export.

### Pre-Merge View
```
┌──────────────────────────────────────────────────────┐
│  MERGE OVERRIDES                                     │
│                                                      │
│  Base export: NGKs_final_analyzer_export.csv          │
│  Base rows: 907                                      │
│  Last modified: 2026-04-02 14:30:00                  │
│                                                      │
│  Approved overrides ready to merge: 3                │
│                                                      │
│  ┌────┬──────────────┬───────────┬────────┬────────┐ │
│  │Row │ Artist       │ Scope     │ BPM    │ Key    │ │
│  ├────┼──────────────┼───────────┼────────┼────────┤ │
│  │  1 │ Unknown      │ BPM       │ 128.0  │ —      │ │
│  │ 30 │ Adam Calhoun │ KEY       │ —      │ 8B     │ │
│  │ 34 │ Airbourne    │ BPM_AND_KEY│ 140.0 │ 10B    │ │
│  └────┴──────────────┴───────────┴────────┴────────┘ │
│                                                      │
│  [ Preview Merge ]                                   │
└──────────────────────────────────────────────────────┘
```

### Merge Flow (Step by Step)

1. **User clicks "Preview Merge"**
   - System re-runs validation on all APPROVED overrides
   - If any fail re-validation → shown with errors, merge blocked
   - If all pass → preview table shown

2. **Merge Preview Table**
   ```
   Row  Field      Current    → Override    Change
   ──────────────────────────────────────────────────
    1   FinalBPM   120.2      → 128.0       +7.8
    30  FinalKey    2A         → 8B          changed
    34  FinalBPM   95.7       → 140.0       +44.3
    34  FinalKey    10B        → 10B         (same)
   ```

3. **User clicks "Confirm & Apply"**
   - Merged export written: `NGKs_final_analyzer_export_OVERRIDDEN.csv`
   - Applied overrides marked APPLIED in store
   - Apply log written: `override_applied_log.csv`
   - Merge batch record created
   - Audit events recorded for each override

4. **UI refreshes** to show:
   - Applied override count
   - Link to merged export file
   - Timestamps
   - "View Applied Overrides" link → History panel

### Rollback Path
- "Undo Last Merge" button available immediately after merge
- Rollback restores overrides to APPROVED state
- Removes the merged export file
- Writes rollback event to audit log

### Failed Merge Behavior
- If merge encounters an error mid-write:
  - Temp file is deleted (atomic rename strategy)
  - Override states remain APPROVED (not changed to APPLIED)
  - Error displayed to user with specific failure details
  - Audit event records the failure

---

## Screen 6: Override History / Audit View

### Purpose
Show complete audit trail for all overrides.

### Views

**Per-Row History** (accessible from Override Editor):
```
OVERRIDE HISTORY — Row 294 (DJ LOA - Don't Say Goodbye Remix)
─────────────────────────────────────────────────────────────
Rev  Date         Action          BPM     Key   By    Status
─────────────────────────────────────────────────────────────
 1   2026-04-02   Created         —       —     NGK   NEW
 1   2026-04-02   Edited          128.0   —     NGK   DRAFT
 1   2026-04-02   Validated       128.0   —     sys   VALID
 1   2026-04-02   Approved        128.0   —     NGK   APPROVED
 1   2026-04-02   Applied         128.0   —     sys   APPLIED
 2   2026-04-03   New Revision    128.0   8B    NGK   DRAFT
     (Rev 1 → SUPERSEDED)
```

**Global Audit Log** (accessible from Override Manager root):
```
AUDIT LOG — All Override Events
────────────────────────────────────────────────────────────────
Timestamp            Row  Artist         Action         By
────────────────────────────────────────────────────────────────
2026-04-03 10:15:02   1   Unknown        CREATED        NGK
2026-04-03 10:15:30   1   Unknown        EDITED         NGK
2026-04-03 10:15:45   1   Unknown        VALIDATED      sys
2026-04-03 10:16:00   1   Unknown        APPROVED       NGK
2026-04-03 10:20:00  30   Adam Calhoun   CREATED        NGK
...
```

**Merge History** (accessible from Merge panel):
```
MERGE HISTORY
──────────────────────────────────────────────────
Batch     Date         Overrides  Status  Export
──────────────────────────────────────────────────
MB-001    2026-04-02   3 applied  SUCCESS  OVERRIDDEN_v1.csv
MB-002    2026-04-03   2 applied  SUCCESS  OVERRIDDEN_v2.csv
```

### Actions
- **Filter** by row, date range, action type, user
- **Export audit log** as CSV

---

## App Integration: Library View Behavior

### How Merged Values Are Shown

After overrides are applied, the Library View reflects the merged data:
- Rows with applied overrides show the **overridden** BPM/Key values
- An override badge (🔧 or similar icon) appears next to overridden fields
- Tooltip on badge shows: "Manual Override: BPM changed from 120.2 to 128.0 by NGK (2026-04-02)"

### How Trust/Review Fields React After Override

When an override is applied to a row:
- `FinalBPMTrustLevel` / `FinalKeyTrustLevel` → set to `HIGH`
- `FinalBPMReviewFlag` / `FinalKeyReviewFlag` → set to `False`
- `ReviewRequired` → re-evaluated (may become False if both review flags are now False)
- `ConfidenceTier` → re-evaluated (may upgrade to PRODUCTION)
- `ManualOverrideEligible` → set to `False` (already overridden)
- `DecisionSource` → set to `MANUAL_OVERRIDE`

In the Library View:
- Row moves from "Needs Review" filter to "Production Ready"
- Confidence tier badge updates
- Original values available via Override History

### Override Manager in Navigation

```
Library View
  └── Row Context Menu
        ├── View Details
        ├── Override → opens Override Editor (Screen 2)
        └── View Override History → opens History (Screen 6)

Override Manager (sidebar)
  ├── Review Queue (Screen 1)
  │     Default filter: ReviewRequired=True
  ├── Pending Approvals
  │     Filter: state IN (VALID, APPROVED)
  ├── Merge / Apply (Screen 5)
  │     Shows: APPROVED count, merge button
  └── Override History (Screen 6)
        Shows: all audit events
```

---

## Complete User Journey Example

1. User opens **Override Manager → Review Queue**
2. Sees 73 rows with `ReviewRequired=True`
3. Clicks row 294 (DJ LOA — Don't Say Goodbye Remix)
4. **Override Editor** opens. Current: BPM=120.2 (HIGH), Key=3A (LOW, flagged)
5. User enters: Override Key = "8B", Scope = KEY, Reason = "Verified via Tunebat"
6. Clicks **Save Draft** → state: DRAFT
7. Clicks **Validate** → system runs validation → state: VALID
8. Clicks **Approve** → state: APPROVED
9. Navigates to **Merge / Apply** panel
10. Sees 1 approved override ready
11. Clicks **Preview Merge** → sees: "Row 294: FinalKey 3A → 8B"
12. Clicks **Confirm & Apply** → merged export written
13. Row 294 now shows Key=8B (HIGH) in Library View
14. Override History shows the full trail

---

## State-to-Screen Mapping

| State | Primary Screen | Available Actions |
|---|---|---|
| NEW | Review Queue | Edit Override |
| DRAFT | Override Editor | Save Draft, Validate, Discard |
| PENDING_VALIDATION | Override Editor (spinner) | (waiting) |
| VALID | Override Editor | Approve, Edit Again, Disable |
| INVALID | Override Editor + Validation Panel | Fix & Re-edit, Discard |
| CONFLICT | Conflict Resolution Panel | Fix Identity, Discard |
| APPROVED | Pending Approvals / Merge Panel | Apply Merge, Revoke, Disable |
| APPLIED | Override History | Supersede (create new rev), Disable |
| DISABLED | Override History (dimmed) | Re-enable & Edit |
| SUPERSEDED | Override History (archived) | (view only) |
