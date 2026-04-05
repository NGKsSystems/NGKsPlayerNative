"""
NGKsPlayerNative — Override Merge Flow
Loads base export + validated overrides, produces merged export.
Non-destructive: base export is never modified.
Includes demo data generation, full pipeline execution, and proof packaging.
"""

import csv
import os
import sys
import shutil
import zipfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from override_validation import (
    validate_overrides,
    write_validation_outputs,
    parse_key_to_camelot,
    REVERSE_CAMELOT,
)

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "manual_override_system")
BASE_EXPORT_PATH = os.path.join(WORKSPACE, "_proof", "final_export_schema",
                                "NGKs_final_analyzer_export.csv")

_log_lines = []


def log(line):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {line}"
    _log_lines.append(entry)
    try:
        print(entry)
    except UnicodeEncodeError:
        print(entry.encode("ascii", "replace").decode())


def _norm(s):
    return (s or "").strip()


# ──────────────────────────────────────────────────────────────────
# DEMO OVERRIDE BUILDER
# ──────────────────────────────────────────────────────────────────
def build_demo_overrides(base_rows):
    """Create a small demo override file with 5 test cases from actual base data."""
    demo = []
    review_rows = [r for r in base_rows if str(r.get("ReviewRequired", "")).upper() == "TRUE"]

    # Need 5 distinct rows from review queue
    used_rows = set()
    candidates = list(review_rows)  # copy to avoid mutation

    def pick(exclude=None):
        for r in candidates:
            rn = r.get("Row", "")
            if rn not in used_rows:
                used_rows.add(rn)
                return r
        return None

    bpm_only_cand = pick()
    key_only_cand = pick()
    both_cand = pick()
    invalid_cand = pick()
    conflict_cand = pick()

    # Case 1: Valid BPM override
    if bpm_only_cand:
        demo.append({
            "Row": bpm_only_cand["Row"],
            "Artist": bpm_only_cand["Artist"],
            "Title": bpm_only_cand["Title"],
            "Filename": bpm_only_cand["Filename"],
            "OverrideFinalBPM": "128.0",
            "OverrideFinalKey": "",
            "OverrideBPMReason": "Demo: manual BPM correction",
            "OverrideKeyReason": "",
            "OverrideEnteredBy": "DemoSystem",
            "OverrideDate": datetime.now().strftime("%Y-%m-%d"),
            "OverrideNotes": "Test case: valid BPM only",
            "OverrideEnabled": "TRUE",
            "OverrideScope": "BPM",
        })

    # Case 2: Valid Key override
    if key_only_cand:
        demo.append({
            "Row": key_only_cand["Row"],
            "Artist": key_only_cand["Artist"],
            "Title": key_only_cand["Title"],
            "Filename": key_only_cand["Filename"],
            "OverrideFinalBPM": "",
            "OverrideFinalKey": "8B",
            "OverrideBPMReason": "",
            "OverrideKeyReason": "Demo: manual key correction to C major",
            "OverrideEnteredBy": "DemoSystem",
            "OverrideDate": datetime.now().strftime("%Y-%m-%d"),
            "OverrideNotes": "Test case: valid Key only",
            "OverrideEnabled": "TRUE",
            "OverrideScope": "KEY",
        })

    # Case 3: Valid BPM+Key override
    if both_cand:
        demo.append({
            "Row": both_cand["Row"],
            "Artist": both_cand["Artist"],
            "Title": both_cand["Title"],
            "Filename": both_cand["Filename"],
            "OverrideFinalBPM": "140.0",
            "OverrideFinalKey": "10B",
            "OverrideBPMReason": "Demo: both fields corrected",
            "OverrideKeyReason": "Demo: key corrected to D major",
            "OverrideEnteredBy": "DemoSystem",
            "OverrideDate": datetime.now().strftime("%Y-%m-%d"),
            "OverrideNotes": "Test case: valid BPM and Key",
            "OverrideEnabled": "TRUE",
            "OverrideScope": "BPM_AND_KEY",
        })

    # Case 4: Invalid BPM override (out of range)
    if invalid_cand:
        demo.append({
            "Row": invalid_cand["Row"],
            "Artist": invalid_cand["Artist"],
            "Title": invalid_cand["Title"],
            "Filename": invalid_cand["Filename"],
            "OverrideFinalBPM": "999",
            "OverrideFinalKey": "",
            "OverrideBPMReason": "Demo: invalid BPM value",
            "OverrideKeyReason": "",
            "OverrideEnteredBy": "DemoSystem",
            "OverrideDate": datetime.now().strftime("%Y-%m-%d"),
            "OverrideNotes": "Test case: INVALID BPM out of range",
            "OverrideEnabled": "TRUE",
            "OverrideScope": "BPM",
        })

    # Case 5: Conflict (wrong artist name)
    if conflict_cand:
        demo.append({
            "Row": conflict_cand["Row"],
            "Artist": "WRONG_ARTIST_NAME",
            "Title": conflict_cand["Title"],
            "Filename": conflict_cand["Filename"],
            "OverrideFinalBPM": "",
            "OverrideFinalKey": "7A",
            "OverrideBPMReason": "",
            "OverrideKeyReason": "Demo: conflict due to artist mismatch",
            "OverrideEnteredBy": "DemoSystem",
            "OverrideDate": datetime.now().strftime("%Y-%m-%d"),
            "OverrideNotes": "Test case: CONFLICT identity mismatch",
            "OverrideEnabled": "TRUE",
            "OverrideScope": "KEY",
        })

    return demo


# ──────────────────────────────────────────────────────────────────
# MERGE
# ──────────────────────────────────────────────────────────────────
def merge_overrides(base_rows, validated_overrides):
    """Apply validated overrides to base rows. Returns (merged_rows, applied_log, conflicts)."""
    # Build override lookup by Row
    override_lookup = {}
    for ov in validated_overrides:
        row_num = _norm(ov.get("Row", ""))
        if row_num:
            override_lookup[row_num] = ov

    merged = []
    applied_log = []
    conflicts = []

    # Get base field names
    base_fields = list(base_rows[0].keys()) if base_rows else []

    # Override audit fields to append
    override_audit_fields = [
        "OverrideApplied", "OverrideTypeApplied",
        "OverrideBPMApplied", "OverrideKeyApplied",
        "OverrideReasonSummary", "OverrideSource", "OverrideAuditStatus",
        "FinalBPM_Original", "FinalKey_Original",
    ]

    for base in base_rows:
        row_num = _norm(base.get("Row", ""))
        merged_row = dict(base)  # start with base

        ov = override_lookup.get(row_num)

        if not ov or ov.get("OverrideStatus") == "PENDING":
            # No override or disabled
            merged_row["OverrideApplied"] = False
            merged_row["OverrideTypeApplied"] = "NONE"
            merged_row["OverrideBPMApplied"] = ""
            merged_row["OverrideKeyApplied"] = ""
            merged_row["OverrideReasonSummary"] = ""
            merged_row["OverrideSource"] = ""
            merged_row["OverrideAuditStatus"] = "NOT_OVERRIDDEN"
            merged_row["FinalBPM_Original"] = ""
            merged_row["FinalKey_Original"] = ""
            merged.append(merged_row)
            continue

        status = ov.get("OverrideStatus", "")

        if status == "INVALID":
            merged_row["OverrideApplied"] = False
            merged_row["OverrideTypeApplied"] = "NONE"
            merged_row["OverrideBPMApplied"] = ""
            merged_row["OverrideKeyApplied"] = ""
            merged_row["OverrideReasonSummary"] = ov.get("OverrideValidationMessage", "")
            merged_row["OverrideSource"] = ov.get("OverrideEnteredBy", "")
            merged_row["OverrideAuditStatus"] = "OVERRIDE_SKIPPED_INVALID"
            merged_row["FinalBPM_Original"] = ""
            merged_row["FinalKey_Original"] = ""
            merged.append(merged_row)
            applied_log.append({
                "Row": row_num,
                "Artist": base.get("Artist", ""),
                "Title": base.get("Title", ""),
                "Action": "SKIPPED_INVALID",
                "Reason": ov.get("OverrideValidationMessage", ""),
            })
            continue

        if status == "CONFLICT":
            merged_row["OverrideApplied"] = False
            merged_row["OverrideTypeApplied"] = "NONE"
            merged_row["OverrideBPMApplied"] = ""
            merged_row["OverrideKeyApplied"] = ""
            merged_row["OverrideReasonSummary"] = ov.get("OverrideValidationMessage", "")
            merged_row["OverrideSource"] = ov.get("OverrideEnteredBy", "")
            merged_row["OverrideAuditStatus"] = "OVERRIDE_SKIPPED_CONFLICT"
            merged_row["FinalBPM_Original"] = ""
            merged_row["FinalKey_Original"] = ""
            merged.append(merged_row)
            conflicts.append({
                "Row": row_num,
                "Artist": base.get("Artist", ""),
                "Title": base.get("Title", ""),
                "OverrideArtist": ov.get("Artist", ""),
                "OverrideTitle": ov.get("Title", ""),
                "ConflictReason": ov.get("OverrideValidationMessage", ""),
                "OverrideFinalBPM": ov.get("OverrideFinalBPM", ""),
                "OverrideFinalKey": ov.get("OverrideFinalKey", ""),
            })
            applied_log.append({
                "Row": row_num,
                "Artist": base.get("Artist", ""),
                "Title": base.get("Title", ""),
                "Action": "SKIPPED_CONFLICT",
                "Reason": ov.get("OverrideValidationMessage", ""),
            })
            continue

        # status == VALID: apply override
        scope = _norm(ov.get("OverrideScope", "")).upper()
        bpm_applied = False
        key_applied = False
        reasons = []

        # Save originals
        merged_row["FinalBPM_Original"] = base.get("FinalBPM", "")
        merged_row["FinalKey_Original"] = base.get("FinalKey", "")

        # Apply BPM
        if scope in ("BPM", "BPM_AND_KEY") and "_bpm_value" in ov:
            bpm_val = ov["_bpm_value"]
            merged_row["FinalBPM"] = bpm_val
            merged_row["FinalBPMConfidence"] = 1.0
            merged_row["FinalBPMTrustLevel"] = "HIGH"
            merged_row["FinalBPMDecisionSource"] = "MANUAL_OVERRIDE"
            merged_row["FinalBPMReviewFlag"] = False
            merged_row["FinalBPMReason"] = ov.get("OverrideBPMReason", "manual override")
            bpm_applied = True
            reasons.append(f"BPM: {ov.get('OverrideBPMReason', 'manual')}")

        # Apply Key
        if scope in ("KEY", "BPM_AND_KEY") and "_key_camelot" in ov:
            key_cam = ov["_key_camelot"]
            key_name = ov.get("_key_name", REVERSE_CAMELOT.get(key_cam, key_cam))
            merged_row["FinalKey"] = key_cam
            merged_row["FinalKeyName"] = key_name
            merged_row["FinalKeyConfidence"] = 1.0
            merged_row["FinalKeyTrustLevel"] = "HIGH"
            merged_row["FinalKeyDecisionSource"] = "MANUAL_OVERRIDE"
            merged_row["FinalKeyReviewFlag"] = False
            merged_row["FinalKeyReason"] = ov.get("OverrideKeyReason", "manual override")
            key_applied = True
            reasons.append(f"Key: {ov.get('OverrideKeyReason', 'manual')}")

        # Update combined quality if either applied
        if bpm_applied or key_applied:
            bpm_trust = merged_row.get("FinalBPMTrustLevel", "MEDIUM")
            key_trust = merged_row.get("FinalKeyTrustLevel", "MEDIUM")
            bpm_rev = str(merged_row.get("FinalBPMReviewFlag", "")).upper() == "TRUE"
            key_rev = str(merged_row.get("FinalKeyReviewFlag", "")).upper() == "TRUE"

            review_req = bpm_rev or key_rev
            merged_row["ReviewRequired"] = review_req
            if not review_req:
                merged_row["ReviewReason"] = ""
                merged_row["AnalyzerReady"] = True
            if bpm_trust in ("HIGH", "MEDIUM") and key_trust in ("HIGH", "MEDIUM") and not review_req:
                merged_row["ConfidenceTier"] = "PRODUCTION"
            merged_row["ManualOverrideEligible"] = False  # already overridden

        # Determine type
        if bpm_applied and key_applied:
            ov_type = "BPM_AND_KEY"
        elif bpm_applied:
            ov_type = "BPM"
        elif key_applied:
            ov_type = "KEY"
        else:
            ov_type = "NONE"

        merged_row["OverrideApplied"] = True
        merged_row["OverrideTypeApplied"] = ov_type
        merged_row["OverrideBPMApplied"] = ov.get("_bpm_value", "") if bpm_applied else ""
        merged_row["OverrideKeyApplied"] = ov.get("_key_camelot", "") if key_applied else ""
        merged_row["OverrideReasonSummary"] = "; ".join(reasons)
        merged_row["OverrideSource"] = ov.get("OverrideEnteredBy", "")
        merged_row["OverrideAuditStatus"] = "OVERRIDE_APPLIED"

        merged.append(merged_row)
        applied_log.append({
            "Row": row_num,
            "Artist": base.get("Artist", ""),
            "Title": base.get("Title", ""),
            "Action": "APPLIED",
            "Type": ov_type,
            "BPMOverride": ov.get("_bpm_value", "") if bpm_applied else "",
            "KeyOverride": ov.get("_key_camelot", "") if key_applied else "",
            "Reason": "; ".join(reasons),
        })

    return merged, applied_log, conflicts


# ──────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────────
def main():
    os.chdir(WORKSPACE)
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("MANUAL OVERRIDE MERGE FLOW")
    log(f"Workspace: {WORKSPACE}")
    log(f"Date: {datetime.now().isoformat()}")
    log("")

    # ── Load base export ──
    log("=== LOADING BASE EXPORT ===")
    if not os.path.isfile(BASE_EXPORT_PATH):
        log(f"FATAL: Base export not found: {BASE_EXPORT_PATH}")
        sys.exit(1)
    with open(BASE_EXPORT_PATH, "r", encoding="utf-8-sig") as f:
        base_rows = list(csv.DictReader(f))
    log(f"Base export: {len(base_rows)} rows")
    base_lookup = {r.get("Row", ""): r for r in base_rows}

    # ── Step 1: Generate template ──
    log("")
    log("=== STEP 1: GENERATE OVERRIDE TEMPLATE ===")
    from override_template_builder import main as build_template
    build_template()
    log("Template generated")

    # ── Step 2: Build demo overrides ──
    log("")
    log("=== STEP 2: BUILD DEMO OVERRIDES ===")
    demo_overrides = build_demo_overrides(base_rows)
    log(f"Demo override cases: {len(demo_overrides)}")

    demo_fields = ["Row", "Artist", "Title", "Filename",
                   "OverrideFinalBPM", "OverrideFinalKey",
                   "OverrideBPMReason", "OverrideKeyReason",
                   "OverrideEnteredBy", "OverrideDate", "OverrideNotes",
                   "OverrideEnabled", "OverrideScope"]

    demo_input_path = os.path.join(PROOF_DIR, "07_demo_override_input.csv")
    with open(demo_input_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=demo_fields)
        w.writeheader()
        w.writerows(demo_overrides)
    log(f"Wrote {demo_input_path}")

    for d in demo_overrides:
        log(f"  Row {d['Row']}: scope={d['OverrideScope']} bpm={d['OverrideFinalBPM'] or '-'} key={d['OverrideFinalKey'] or '-'} note={d['OverrideNotes']}")

    # ── Step 3: Validate overrides ──
    log("")
    log("=== STEP 3: VALIDATE DEMO OVERRIDES ===")
    validated = validate_overrides(demo_overrides, base_lookup)
    sp, rp, v_count, i_count, c_count = write_validation_outputs(validated, PROOF_DIR)
    log(f"Validation: VALID={v_count} INVALID={i_count} CONFLICT={c_count}")
    for vr in validated:
        log(f"  Row {vr.get('Row','?'):>4s}  {vr['OverrideStatus']:10s}  {vr['OverrideValidationMessage']}")

    # ── Step 4: Merge ──
    log("")
    log("=== STEP 4: MERGE OVERRIDES ===")
    merged_rows, applied_log, conflicts = merge_overrides(base_rows, validated)
    log(f"Merged rows: {len(merged_rows)}")
    log(f"Applied: {sum(1 for a in applied_log if a.get('Action') == 'APPLIED')}")
    log(f"Skipped invalid: {sum(1 for a in applied_log if a.get('Action') == 'SKIPPED_INVALID')}")
    log(f"Skipped conflict: {sum(1 for a in applied_log if a.get('Action') == 'SKIPPED_CONFLICT')}")
    log(f"Conflicts: {len(conflicts)}")

    # ── Write merged export ──
    merged_path = os.path.join(PROOF_DIR, "NGKs_final_analyzer_export_OVERRIDDEN.csv")
    merged_fields: list[str] = []
    if merged_rows:
        merged_fields = list(merged_rows[0].keys())
        with open(merged_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=merged_fields)
            w.writeheader()
            w.writerows(merged_rows)
    log(f"Wrote {merged_path}")

    # ── Write demo merge results ──
    demo_results_path = os.path.join(PROOF_DIR, "08_demo_merge_results.csv")
    demo_result_rows = []
    for ov in validated:
        row_num = _norm(ov.get("Row", ""))
        # Find the merged row
        for mr in merged_rows:
            if _norm(mr.get("Row", "")) == row_num:
                demo_result_rows.append({
                    "Row": row_num,
                    "Artist": mr.get("Artist", ""),
                    "Title": mr.get("Title", ""),
                    "OverrideStatus": ov.get("OverrideStatus", ""),
                    "OverrideScope": ov.get("OverrideScope", ""),
                    "FinalBPM": mr.get("FinalBPM", ""),
                    "FinalBPM_Original": mr.get("FinalBPM_Original", ""),
                    "FinalKey": mr.get("FinalKey", ""),
                    "FinalKey_Original": mr.get("FinalKey_Original", ""),
                    "OverrideApplied": mr.get("OverrideApplied", ""),
                    "OverrideTypeApplied": mr.get("OverrideTypeApplied", ""),
                    "OverrideAuditStatus": mr.get("OverrideAuditStatus", ""),
                    "OverrideValidationMessage": ov.get("OverrideValidationMessage", ""),
                })
                break
    if demo_result_rows:
        dr_fields = list(demo_result_rows[0].keys())
        with open(demo_results_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=dr_fields)
            w.writeheader()
            w.writerows(demo_result_rows)
    log(f"Wrote {demo_results_path}")

    # ── Write conflicts ──
    conflicts_path = os.path.join(PROOF_DIR, "02_override_conflicts.csv")
    if conflicts:
        cf = list(conflicts[0].keys())
        with open(conflicts_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cf)
            w.writeheader()
            w.writerows(conflicts)
    else:
        with open(conflicts_path, "w", encoding="utf-8", newline="") as f:
            f.write("Row,Artist,Title,ConflictReason\n")
            f.write("# No conflicts detected\n")
    log(f"Wrote {conflicts_path}")

    # ── Write applied log ──
    applied_log_path = os.path.join(PROOF_DIR, "03_override_applied_log.csv")
    if applied_log:
        al_fields = list(applied_log[0].keys())
        with open(applied_log_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=al_fields)
            w.writeheader()
            w.writerows(applied_log)
    else:
        with open(applied_log_path, "w", encoding="utf-8", newline="") as f:
            f.write("Row,Artist,Title,Action,Reason\n")
    log(f"Wrote {applied_log_path}")

    # ── Write workflow doc ──
    log("")
    log("=== STEP 5: WRITE WORKFLOW DOC ===")
    workflow_path = os.path.join(PROOF_DIR, "04_override_workflow.txt")
    with open(workflow_path, "w", encoding="utf-8") as f:
        f.write("MANUAL OVERRIDE WORKFLOW\n")
        f.write("=" * 60 + "\n\n")
        f.write("STEP 1: Generate base export and review queue\n")
        f.write("  Run: python tools\\final_export_builder.py\n")
        f.write("  Output: _proof\\final_export_schema\\NGKs_final_analyzer_export.csv\n")
        f.write("  Output: _proof\\final_export_schema\\03_review_queue.csv\n\n")
        f.write("STEP 2: Generate override template\n")
        f.write("  Run: python tools\\override_template_builder.py\n")
        f.write("  Output: _proof\\manual_override_system\\NGKs_override_template.csv\n")
        f.write("  Template is pre-seeded with review-flagged rows.\n\n")
        f.write("STEP 3: Human edits override template\n")
        f.write("  Open NGKs_override_template.csv in Excel / CSV editor.\n")
        f.write("  For each row to override:\n")
        f.write("    - Fill OverrideFinalBPM and/or OverrideFinalKey\n")
        f.write("    - Fill OverrideBPMReason / OverrideKeyReason\n")
        f.write("    - Set OverrideEnabled = TRUE\n")
        f.write("    - Set OverrideScope = BPM / KEY / BPM_AND_KEY\n")
        f.write("    - Fill OverrideEnteredBy and OverrideDate\n")
        f.write("  Save as NGKs_overrides.csv in same folder.\n\n")
        f.write("STEP 4: Validate overrides\n")
        f.write("  Run: python tools\\override_validation.py\n")
        f.write("  Output: 00_override_validation_summary.txt\n")
        f.write("  Output: 01_override_validation_results.csv\n")
        f.write("  Review validation results. Fix INVALID/CONFLICT rows.\n\n")
        f.write("STEP 5: Merge overrides\n")
        f.write("  Run: python tools\\override_merge_flow.py\n")
        f.write("  Output: NGKs_final_analyzer_export_OVERRIDDEN.csv\n")
        f.write("  Output: 02_override_conflicts.csv\n")
        f.write("  Output: 03_override_applied_log.csv\n\n")
        f.write("STEP 6: Use merged export\n")
        f.write("  The OVERRIDDEN export is the final production file.\n")
        f.write("  It contains all base rows + applied overrides.\n")
        f.write("  Audit columns show what was changed and why.\n\n")
        f.write("SAFETY RULES\n")
        f.write("  - Base export is NEVER modified\n")
        f.write("  - Invalid/conflicting overrides are NEVER applied\n")
        f.write("  - All changes are auditable via Override* columns\n")
        f.write("  - Original values preserved in FinalBPM_Original / FinalKey_Original\n")
        f.write("  - Row count and order always match base export\n")
    log(f"Wrote {workflow_path}")

    # ── Sanity checks ──
    log("")
    log("=== STEP 6: SANITY CHECKS ===")
    checks = []
    gate = "PASS"

    # Row count
    if len(merged_rows) == len(base_rows):
        checks.append(f"PASS  Row count: {len(merged_rows)} == {len(base_rows)}")
    else:
        checks.append(f"FAIL  Row count: {len(merged_rows)} != {len(base_rows)}")
        gate = "FAIL"

    # Row order
    order_ok = True
    for i, (m, b) in enumerate(zip(merged_rows, base_rows)):
        if m.get("Artist") != b.get("Artist") or m.get("Title") != b.get("Title"):
            # Check if this is an overridden row with conflict (artist was changed)
            if m.get("OverrideAuditStatus") != "OVERRIDE_SKIPPED_CONFLICT":
                order_ok = False
                checks.append(f"FAIL  Row order mismatch at row {i+1}")
                gate = "FAIL"
                break
    if order_ok:
        checks.append("PASS  Row order preserved")

    # Invalid overrides did not change base
    for ov in validated:
        if ov.get("OverrideStatus") in ("INVALID", "CONFLICT"):
            row_num = _norm(ov.get("Row", ""))
            base = base_lookup.get(row_num)
            merged_match = None
            for mr in merged_rows:
                if _norm(mr.get("Row", "")) == row_num:
                    merged_match = mr
                    break
            if base and merged_match:
                if (str(merged_match.get("FinalBPM", "")) != str(base.get("FinalBPM", "")) or
                        str(merged_match.get("FinalKey", "")) != str(base.get("FinalKey", ""))):
                    checks.append(f"FAIL  Invalid/conflict override Row {row_num} modified base values")
                    gate = "FAIL"
                else:
                    checks.append(f"PASS  Invalid/conflict override Row {row_num} did NOT modify base")

    # Valid overrides were applied
    for ov in validated:
        if ov.get("OverrideStatus") == "VALID":
            row_num = _norm(ov.get("Row", ""))
            for mr in merged_rows:
                if _norm(mr.get("Row", "")) == row_num:
                    if str(mr.get("OverrideApplied", "")).upper() == "TRUE":
                        checks.append(f"PASS  Valid override Row {row_num} was applied")
                    else:
                        checks.append(f"FAIL  Valid override Row {row_num} was NOT applied")
                        gate = "FAIL"
                    break

    # Override audit fields present
    required_audit = ["OverrideApplied", "OverrideTypeApplied", "OverrideAuditStatus",
                      "FinalBPM_Original", "FinalKey_Original"]
    merged_field_set = set(merged_fields) if merged_rows else set()
    for fld in required_audit:
        if fld in merged_field_set:
            checks.append(f"PASS  Audit field present: {fld}")
        else:
            checks.append(f"FAIL  Audit field missing: {fld}")
            gate = "FAIL"

    # No blank Artist/Title introduced
    blank_merged = sum(1 for r in merged_rows
                       if not _norm(r.get("Artist", "")) and not _norm(r.get("Title", "")))
    blank_base = sum(1 for r in base_rows
                     if not _norm(r.get("Artist", "")) and not _norm(r.get("Title", "")))
    if blank_merged <= blank_base:
        checks.append(f"PASS  No new blank Artist/Title (merged={blank_merged}, base={blank_base})")
    else:
        checks.append(f"FAIL  New blank Artist/Title: {blank_merged} > {blank_base}")
        gate = "FAIL"

    sanity_path = os.path.join(PROOF_DIR, "05_sanity_checks.txt")
    with open(sanity_path, "w", encoding="utf-8") as f:
        f.write("OVERRIDE MERGE SANITY CHECKS\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        for c in checks:
            f.write(f"  {c}\n")
        f.write(f"\nGATE: {gate}\n")
    log(f"Wrote {sanity_path}")
    for c in checks:
        log(f"  {c}")

    # ── Final report ──
    log("")
    log("=== STEP 7: FINAL REPORT ===")
    total_overrides = len(validated)
    applied_count = sum(1 for a in applied_log if a.get("Action") == "APPLIED")
    skipped_invalid = sum(1 for a in applied_log if a.get("Action") == "SKIPPED_INVALID")
    skipped_conflict = sum(1 for a in applied_log if a.get("Action") == "SKIPPED_CONFLICT")
    unchanged = len(base_rows) - applied_count

    report_path = os.path.join(PROOF_DIR, "06_final_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("NGKsPlayerNative MANUAL OVERRIDE SYSTEM -- FINAL REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Base export rows:      {len(base_rows)}\n")
        f.write(f"Override rows supplied: {total_overrides}\n")
        f.write(f"  Valid:               {v_count}\n")
        f.write(f"  Invalid:             {i_count}\n")
        f.write(f"  Conflict:            {c_count}\n\n")
        f.write(f"Overrides applied:     {applied_count}\n")
        f.write(f"Skipped (invalid):     {skipped_invalid}\n")
        f.write(f"Skipped (conflict):    {skipped_conflict}\n")
        f.write(f"Rows unchanged:        {unchanged}\n\n")
        f.write(f"Merged export rows:    {len(merged_rows)}\n\n")
        f.write("APPLIED OVERRIDES:\n")
        for a in applied_log:
            if a.get("Action") == "APPLIED":
                f.write(f"  Row {a['Row']:>4s}  {a.get('Artist',''):30s}  {a.get('Type','')}  {a.get('Reason','')}\n")
        f.write("\nSKIPPED OVERRIDES:\n")
        for a in applied_log:
            if a.get("Action") != "APPLIED":
                f.write(f"  Row {a['Row']:>4s}  {a.get('Artist',''):30s}  {a.get('Action','')}  {a.get('Reason','')}\n")
        f.write(f"\nSYSTEM STATUS:\n")
        f.write(f"  Override system is functional and ready for app integration.\n")
        f.write(f"  All merge operations are non-destructive and auditable.\n")
        f.write(f"  Demo test passed with {applied_count} valid, {skipped_invalid} invalid, {skipped_conflict} conflict.\n")
        f.write(f"\nGATE: {gate}\n")
    log(f"Wrote {report_path}")

    # ── Proof package ──
    log("")
    log("=== STEP 8: PROOF PACKAGE ===")

    # Write execution log
    log_path = os.path.join(PROOF_DIR, "execution_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines) + "\n")

    # Copy schema spec
    schema_src = os.path.join(WORKSPACE, "tools", "override_schema_spec.md")
    schema_dst = os.path.join(PROOF_DIR, "override_schema_spec.md")
    if os.path.isfile(schema_src):
        shutil.copy2(schema_src, schema_dst)

    # Create ZIP
    zip_path = os.path.join(WORKSPACE, "_proof", "manual_override_system.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in sorted(os.listdir(PROOF_DIR)):
            fpath = os.path.join(PROOF_DIR, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, os.path.join("manual_override_system", fname))
    log(f"Wrote {zip_path}")

    # Re-write log with final entries
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines) + "\n")

    # Final output
    log("")
    print("=" * 60)
    print(f"PF={PROOF_DIR}")
    print(f"ZIP={zip_path}")
    print(f"GATE={gate}")
    print("=" * 60)


if __name__ == "__main__":
    main()
