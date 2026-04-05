"""
NGKsPlayerNative — Override Template Builder
Generates a pre-seeded override CSV from the review queue.
Gives the human a safe worksheet with identity + current values + empty override columns.
"""

import csv
import os
import sys
from datetime import datetime

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "manual_override_system")

REVIEW_QUEUE_PATH = os.path.join(WORKSPACE, "_proof", "final_export_schema", "03_review_queue.csv")
BASE_EXPORT_PATH = os.path.join(WORKSPACE, "_proof", "final_export_schema",
                                "NGKs_final_analyzer_export.csv")

TEMPLATE_FIELDS = [
    # Identity
    "Row", "Artist", "Title", "Filename",
    # Current values (read-only reference)
    "Current_FinalBPM", "Current_FinalBPMTrustLevel", "Current_FinalBPMReviewFlag",
    "Current_FinalBPMReason",
    "Current_FinalKey", "Current_FinalKeyName", "Current_FinalKeyTrustLevel",
    "Current_FinalKeyReviewFlag", "Current_FinalKeyReason",
    "Current_ConfidenceTier", "Current_ReviewReason",
    # Override inputs (human fills these)
    "OverrideFinalBPM", "OverrideFinalKey",
    "OverrideBPMReason", "OverrideKeyReason",
    "OverrideEnteredBy", "OverrideDate", "OverrideNotes",
    # Merge control
    "OverrideEnabled", "OverrideScope",
]

_log_lines = []


def log(line):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {line}"
    _log_lines.append(entry)
    try:
        print(entry)
    except UnicodeEncodeError:
        print(entry.encode("ascii", "replace").decode())


def main():
    os.chdir(WORKSPACE)
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("OVERRIDE TEMPLATE BUILDER")
    log(f"Date: {datetime.now().isoformat()}")

    # Load base export for full field access
    if not os.path.isfile(BASE_EXPORT_PATH):
        log(f"FATAL: Base export not found: {BASE_EXPORT_PATH}")
        sys.exit(1)
    with open(BASE_EXPORT_PATH, "r", encoding="utf-8-sig") as f:
        base_rows = list(csv.DictReader(f))
    log(f"Base export: {len(base_rows)} rows")

    # Build lookup by Row number
    base_lookup = {}
    for r in base_rows:
        row_num = r.get("Row", "")
        if row_num:
            base_lookup[row_num] = r

    # Load review queue for seeding
    review_rows = []
    if os.path.isfile(REVIEW_QUEUE_PATH):
        with open(REVIEW_QUEUE_PATH, "r", encoding="utf-8-sig") as f:
            review_rows = list(csv.DictReader(f))
        log(f"Review queue: {len(review_rows)} rows")
    else:
        log(f"Review queue not found, seeding from all review-flagged rows in base")
        review_rows = [r for r in base_rows if str(r.get("ReviewRequired", "")).upper() == "TRUE"]
        log(f"Review-flagged rows: {len(review_rows)}")

    # Build template rows
    template_rows = []
    for rq in review_rows:
        row_num = rq.get("Row", "")
        base = base_lookup.get(row_num, rq)

        tr = {
            "Row": row_num,
            "Artist": base.get("Artist", ""),
            "Title": base.get("Title", ""),
            "Filename": base.get("Filename", ""),
            "Current_FinalBPM": base.get("FinalBPM", ""),
            "Current_FinalBPMTrustLevel": base.get("FinalBPMTrustLevel", ""),
            "Current_FinalBPMReviewFlag": base.get("FinalBPMReviewFlag", ""),
            "Current_FinalBPMReason": base.get("FinalBPMReason", ""),
            "Current_FinalKey": base.get("FinalKey", ""),
            "Current_FinalKeyName": base.get("FinalKeyName", ""),
            "Current_FinalKeyTrustLevel": base.get("FinalKeyTrustLevel", ""),
            "Current_FinalKeyReviewFlag": base.get("FinalKeyReviewFlag", ""),
            "Current_FinalKeyReason": base.get("FinalKeyReason", ""),
            "Current_ConfidenceTier": base.get("ConfidenceTier", ""),
            "Current_ReviewReason": base.get("ReviewReason", ""),
            # Empty override columns for human to fill
            "OverrideFinalBPM": "",
            "OverrideFinalKey": "",
            "OverrideBPMReason": "",
            "OverrideKeyReason": "",
            "OverrideEnteredBy": "",
            "OverrideDate": "",
            "OverrideNotes": "",
            "OverrideEnabled": "",
            "OverrideScope": "",
        }
        template_rows.append(tr)

    # Write template
    template_path = os.path.join(PROOF_DIR, "NGKs_override_template.csv")
    with open(template_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=TEMPLATE_FIELDS)
        w.writeheader()
        w.writerows(template_rows)
    log(f"Wrote {template_path} ({len(template_rows)} rows)")

    # Write log
    log_path = os.path.join(PROOF_DIR, "template_build_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines) + "\n")

    print(f"Template: {template_path}")
    print(f"Rows: {len(template_rows)}")


if __name__ == "__main__":
    main()
