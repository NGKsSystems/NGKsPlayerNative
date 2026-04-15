"""
NGKsPlayerNative — BPM Tuning Evaluation
Steps 1-3: Load, baseline eval, failure analysis on validated Tunebat BPM rows.
All output to _proof/bpm_tuning/
"""

import csv
import os
import sys
import math
import json
from pathlib import Path
from datetime import datetime

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
VALIDATED_CSV = os.path.join(WORKSPACE, "Validated 02_analysis_results.csv")
EVIDENCE_CSV = os.path.join(WORKSPACE, "_proof", "analyzer_upgrade", "03_analysis_with_evidence.csv")
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "bpm_tuning")

REQUIRED_COLS = [
    "Artist", "Title", "ResolvedBPM", "Tunebat BPM",
    "BPMCandidate1", "BPMCandidate2", "BPMCandidate3",
    "BPMCandidateScore1", "BPMCandidateScore2", "BPMCandidateScore3",
    "SelectedBPM", "SelectedBPMConfidence",
    "TempoPeak1", "TempoPeak2", "TempoPeak3",
    "TempoPeakStrength1", "TempoPeakStrength2", "TempoPeakStrength3",
    "BeatIntervalStdDev", "OnsetDensity", "HFPercussiveScore",
    "BPMSelectionReason",
]

# BeatGridConfidence is not a column in the evidence CSV — it's part of the
# scoring computation inside bpm_key_resolver, not stored. We'll note that.

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_LINES.append(line)

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def classify_bpm(selected, tunebat):
    err = abs(selected - tunebat)
    if err <= 1:
        return "EXACT"
    elif err <= 2:
        return "GOOD"
    elif err <= 5:
        return "CLOSE"
    else:
        return "BAD"

def candidate_covers(cand_bpm, tunebat, tol=3.0):
    if cand_bpm is None or cand_bpm == 0:
        return False
    return abs(cand_bpm - tunebat) <= tol

def main():
    os.makedirs(PROOF_DIR, exist_ok=True)

    # ─── STEP 1: Load & Validate ───────────────────────────────────────
    log("STEP 1 — LOAD & VALIDATE INPUTS")

    if not os.path.isfile(VALIDATED_CSV):
        log(f"FAIL: Validated CSV not found: {VALIDATED_CSV}")
        sys.exit(1)
    if not os.path.isfile(EVIDENCE_CSV):
        log(f"FAIL: Evidence CSV not found: {EVIDENCE_CSV}")
        sys.exit(1)

    # Load validated CSV
    with open(VALIDATED_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        validated_rows = list(reader)
    log(f"Validated CSV rows: {len(validated_rows)}")
    log(f"Validated CSV columns: {list(validated_rows[0].keys()) if validated_rows else 'EMPTY'}")

    # Load evidence CSV
    with open(EVIDENCE_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        evidence_cols = reader.fieldnames
        evidence_rows = list(reader)
    log(f"Evidence CSV rows: {len(evidence_rows)}")

    # Check required columns
    missing_cols = [c for c in REQUIRED_COLS if c not in (evidence_cols or [])]
    if missing_cols:
        log(f"WARNING: Missing columns in evidence CSV: {missing_cols}")
        log("Will proceed with available columns")
        # Check if BeatGridConfidence is a column
    has_bgc = "BeatGridConfidence" in (evidence_cols or [])
    log(f"BeatGridConfidence column present: {has_bgc}")

    # Build calibration subset: rows with valid numeric Tunebat BPM
    # Use evidence CSV as the primary source (it has both original and evidence fields)
    calibration = []
    excluded_no_tunebat = 0
    for row in evidence_rows:
        tb = safe_float(row.get("Tunebat BPM", ""))
        if tb is not None and tb > 0:
            calibration.append(row)
        else:
            excluded_no_tunebat += 1

    log(f"Calibration rows (valid Tunebat BPM): {len(calibration)}")
    log(f"Excluded (no Tunebat BPM): {excluded_no_tunebat}")

    # Write 00_load_summary.txt
    with open(os.path.join(PROOF_DIR, "00_load_summary.txt"), "w", encoding="utf-8") as f:
        f.write("BPM TUNING — LOAD SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Source (Validated CSV) rows: {len(validated_rows)}\n")
        f.write(f"Evidence CSV rows: {len(evidence_rows)}\n")
        f.write(f"Calibration rows (valid Tunebat BPM): {len(calibration)}\n")
        f.write(f"Excluded (no Tunebat BPM): {excluded_no_tunebat}\n")
        f.write(f"\nMissing columns: {missing_cols if missing_cols else 'NONE'}\n")
        f.write(f"BeatGridConfidence column: {has_bgc}\n")
        f.write(f"\nCalibration track list:\n")
        for r in calibration:
            f.write(f"  {r.get('Artist','?')} — {r.get('Title','?')} | Tunebat={r.get('Tunebat BPM','')} | Selected={r.get('SelectedBPM','')}\n")

    log("Wrote 00_load_summary.txt")

    # ─── STEP 2: Baseline Evaluation ──────────────────────────────────
    log("\nSTEP 2 — BASELINE EVALUATION")

    baseline_rows = []
    classes = {"EXACT": 0, "GOOD": 0, "CLOSE": 0, "BAD": 0}
    coverage = {"cand1": 0, "cand2": 0, "cand3": 0, "none": 0}

    for row in calibration:
        tunebat = safe_float(row.get("Tunebat BPM", ""))
        selected = safe_float(row.get("SelectedBPM", ""))
        cand1 = safe_float(row.get("BPMCandidate1", ""))
        cand2 = safe_float(row.get("BPMCandidate2", ""))
        cand3 = safe_float(row.get("BPMCandidate3", ""))
        sc1 = safe_float(row.get("BPMCandidateScore1", ""))
        sc2 = safe_float(row.get("BPMCandidateScore2", ""))
        sc3 = safe_float(row.get("BPMCandidateScore3", ""))

        if tunebat is None or selected is None:
            continue

        cls = classify_bpm(selected, tunebat)
        classes[cls] += 1

        # Candidate coverage
        covered_by = "none"
        if candidate_covers(cand1, tunebat):
            covered_by = "cand1"
            coverage["cand1"] += 1
        elif candidate_covers(cand2, tunebat):
            covered_by = "cand2"
            coverage["cand2"] += 1
        elif candidate_covers(cand3, tunebat):
            covered_by = "cand3"
            coverage["cand3"] += 1
        else:
            coverage["none"] += 1

        baseline_rows.append({
            "Artist": row.get("Artist", ""),
            "Title": row.get("Title", ""),
            "Tunebat_BPM": tunebat,
            "SelectedBPM": selected,
            "Error": round(abs(selected - tunebat), 1),
            "Class": cls,
            "Cand1": cand1 or 0,
            "Cand1Score": sc1 or 0,
            "Cand2": cand2 or 0,
            "Cand2Score": sc2 or 0,
            "Cand3": cand3 or 0,
            "Cand3Score": sc3 or 0,
            "CoveredBy": covered_by,
            "TempoPeak1": safe_float(row.get("TempoPeak1", "")) or 0,
            "TempoPeak2": safe_float(row.get("TempoPeak2", "")) or 0,
            "TempoPeak3": safe_float(row.get("TempoPeak3", "")) or 0,
            "TempoPeakStr1": safe_float(row.get("TempoPeakStrength1", "")) or 0,
            "TempoPeakStr2": safe_float(row.get("TempoPeakStrength2", "")) or 0,
            "TempoPeakStr3": safe_float(row.get("TempoPeakStrength3", "")) or 0,
            "BeatIntervalStdDev": safe_float(row.get("BeatIntervalStdDev", "")) or 0,
            "OnsetDensity": safe_float(row.get("OnsetDensity", "")) or 0,
            "HFPercussiveScore": safe_float(row.get("HFPercussiveScore", "")) or 0,
            "BPMSelectionReason": row.get("BPMSelectionReason", ""),
        })

    total_cal = len(baseline_rows)
    log(f"Baseline evaluation: {total_cal} rows")
    for cls, cnt in classes.items():
        pct = (cnt / total_cal * 100) if total_cal > 0 else 0
        log(f"  {cls}: {cnt} ({pct:.1f}%)")

    log(f"Coverage: cand1={coverage['cand1']}, cand2={coverage['cand2']}, cand3={coverage['cand3']}, none={coverage['none']}")

    # Write baseline CSV
    baseline_fields = list(baseline_rows[0].keys()) if baseline_rows else []
    with open(os.path.join(PROOF_DIR, "01_baseline_eval.csv"), "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=baseline_fields)
        writer.writeheader()
        writer.writerows(baseline_rows)

    # BAD rows where correct BPM was in candidates vs absent
    bad_scoring = [r for r in baseline_rows if r["Class"] == "BAD" and r["CoveredBy"] != "none"]
    bad_detection = [r for r in baseline_rows if r["Class"] == "BAD" and r["CoveredBy"] == "none"]

    with open(os.path.join(PROOF_DIR, "01_baseline_summary.txt"), "w", encoding="utf-8") as f:
        f.write("BPM TUNING — BASELINE SUMMARY\n\n")
        f.write(f"Total calibration rows: {total_cal}\n\n")
        f.write("Classification:\n")
        for cls in ["EXACT", "GOOD", "CLOSE", "BAD"]:
            cnt = classes[cls]
            pct = (cnt / total_cal * 100) if total_cal > 0 else 0
            f.write(f"  {cls}: {cnt} ({pct:.1f}%)\n")
        good_close = classes["EXACT"] + classes["GOOD"] + classes["CLOSE"]
        f.write(f"\n  GOOD+CLOSE (<=5 BPM): {good_close} ({good_close / total_cal * 100:.1f}%)\n")

        f.write(f"\nCandidate Coverage (within ±3 BPM of Tunebat):\n")
        f.write(f"  Candidate1 covers: {coverage['cand1']}\n")
        f.write(f"  Candidate2 covers: {coverage['cand2']}\n")
        f.write(f"  Candidate3 covers: {coverage['cand3']}\n")
        f.write(f"  Not covered: {coverage['none']}\n")

        f.write(f"\nBAD rows — SCORING FAILURES (correct BPM in candidates): {len(bad_scoring)}\n")
        for r in bad_scoring:
            f.write(f"  {r['Artist']} — {r['Title']}: Selected={r['SelectedBPM']}, Tunebat={r['Tunebat_BPM']}, CoveredBy={r['CoveredBy']}\n")

        f.write(f"\nBAD rows — DETECTION FAILURES (correct BPM NOT in candidates): {len(bad_detection)}\n")
        for r in bad_detection:
            f.write(f"  {r['Artist']} — {r['Title']}: Selected={r['SelectedBPM']}, Tunebat={r['Tunebat_BPM']}\n")
            f.write(f"    Candidates: [{r['Cand1']}, {r['Cand2']}, {r['Cand3']}]\n")

    log("Wrote 01_baseline_eval.csv and 01_baseline_summary.txt")

    # ─── STEP 3: Failure Analysis ─────────────────────────────────────
    log("\nSTEP 3 — FAILURE ANALYSIS")

    failure_rows = []
    scoring_failures = []
    detection_failures = []

    for r in baseline_rows:
        if r["Class"] != "BAD":
            continue

        tunebat = r["Tunebat_BPM"]
        selected = r["SelectedBPM"]

        # Which candidate covers the correct BPM?
        correct_cand = None
        correct_cand_score = None
        correct_cand_rank = None
        if candidate_covers(r["Cand1"], tunebat):
            correct_cand = r["Cand1"]
            correct_cand_score = r["Cand1Score"]
            correct_cand_rank = 1
        elif candidate_covers(r["Cand2"], tunebat):
            correct_cand = r["Cand2"]
            correct_cand_score = r["Cand2Score"]
            correct_cand_rank = 2
        elif candidate_covers(r["Cand3"], tunebat):
            correct_cand = r["Cand3"]
            correct_cand_score = r["Cand3Score"]
            correct_cand_rank = 3

        ratio = selected / tunebat if tunebat > 0 else 0
        # Classify the ratio
        ratio_class = "OTHER"
        for label, target in [("HALF", 0.5), ("DOUBLE", 2.0), ("THIRD", 2/3), ("FOUR_THIRD", 4/3), ("THREE_HALF", 3/2), ("TWO_THIRD", 2/3)]:
            if abs(ratio - target) < 0.08:
                ratio_class = label
                break

        failure_type = "SCORING" if correct_cand is not None else "DETECTION"

        entry = {
            "Artist": r["Artist"],
            "Title": r["Title"],
            "Tunebat_BPM": tunebat,
            "SelectedBPM": selected,
            "Error": r["Error"],
            "Ratio": round(ratio, 3),
            "RatioClass": ratio_class,
            "FailureType": failure_type,
            "CorrectCandidateRank": correct_cand_rank or "N/A",
            "CorrectCandidateScore": correct_cand_score or "N/A",
            "WinnerScore": r["Cand1Score"],
            "Cand1": r["Cand1"],
            "Cand2": r["Cand2"],
            "Cand3": r["Cand3"],
            "Cand1Score": r["Cand1Score"],
            "Cand2Score": r["Cand2Score"],
            "Cand3Score": r["Cand3Score"],
            "TempoPeak1": r["TempoPeak1"],
            "TempoPeak2": r["TempoPeak2"],
            "TempoPeak3": r["TempoPeak3"],
            "TempoPeakStr1": r["TempoPeakStr1"],
            "TempoPeakStr2": r["TempoPeakStr2"],
            "TempoPeakStr3": r["TempoPeakStr3"],
            "OnsetDensity": r["OnsetDensity"],
            "HFPercussiveScore": r["HFPercussiveScore"],
            "BeatIntervalStdDev": r["BeatIntervalStdDev"],
            "BPMSelectionReason": r["BPMSelectionReason"],
        }
        failure_rows.append(entry)
        if failure_type == "SCORING":
            scoring_failures.append(entry)
        else:
            detection_failures.append(entry)

    # Write failure analysis CSV
    if failure_rows:
        with open(os.path.join(PROOF_DIR, "02_failure_analysis.csv"), "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(failure_rows[0].keys()))
            writer.writeheader()
            writer.writerows(failure_rows)
    else:
        with open(os.path.join(PROOF_DIR, "02_failure_analysis.csv"), "w", encoding="utf-8") as f:
            f.write("No BAD rows found\n")

    with open(os.path.join(PROOF_DIR, "02_failure_summary.txt"), "w", encoding="utf-8") as f:
        f.write("BPM TUNING — FAILURE ANALYSIS\n\n")
        f.write(f"Total BAD rows: {len(failure_rows)}\n")
        f.write(f"  SCORING failures (correct in candidates): {len(scoring_failures)}\n")
        f.write(f"  DETECTION failures (correct NOT in candidates): {len(detection_failures)}\n\n")

        f.write("─── SCORING FAILURES ───\n\n")
        for entry in scoring_failures:
            f.write(f"{entry['Artist']} — {entry['Title']}\n")
            f.write(f"  Tunebat={entry['Tunebat_BPM']}  Selected={entry['SelectedBPM']}  Error={entry['Error']}  Ratio={entry['Ratio']} ({entry['RatioClass']})\n")
            f.write(f"  Correct candidate: rank={entry['CorrectCandidateRank']} score={entry['CorrectCandidateScore']}\n")
            f.write(f"  Winner score: {entry['WinnerScore']}\n")
            f.write(f"  Candidates: [{entry['Cand1']} (s={entry['Cand1Score']}), {entry['Cand2']} (s={entry['Cand2Score']}), {entry['Cand3']} (s={entry['Cand3Score']})]\n")
            f.write(f"  Peaks: [{entry['TempoPeak1']} (s={entry['TempoPeakStr1']}), {entry['TempoPeak2']} (s={entry['TempoPeakStr2']}), {entry['TempoPeak3']} (s={entry['TempoPeakStr3']})]\n")
            f.write(f"  OnsetDensity={entry['OnsetDensity']}  HFPerc={entry['HFPercussiveScore']}  BeatIBI_std={entry['BeatIntervalStdDev']}\n")
            f.write(f"  Reason: {entry['BPMSelectionReason']}\n\n")

        f.write("─── DETECTION FAILURES ───\n\n")
        for entry in detection_failures:
            f.write(f"{entry['Artist']} — {entry['Title']}\n")
            f.write(f"  Tunebat={entry['Tunebat_BPM']}  Selected={entry['SelectedBPM']}  Error={entry['Error']}  Ratio={entry['Ratio']} ({entry['RatioClass']})\n")
            f.write(f"  Candidates: [{entry['Cand1']} (s={entry['Cand1Score']}), {entry['Cand2']} (s={entry['Cand2Score']}), {entry['Cand3']} (s={entry['Cand3Score']})]\n")
            f.write(f"  Peaks: [{entry['TempoPeak1']} (s={entry['TempoPeakStr1']}), {entry['TempoPeak2']} (s={entry['TempoPeakStr2']}), {entry['TempoPeak3']} (s={entry['TempoPeakStr3']})]\n")
            f.write(f"  OnsetDensity={entry['OnsetDensity']}  HFPerc={entry['HFPercussiveScore']}  BeatIBI_std={entry['BeatIntervalStdDev']}\n")
            f.write(f"  Reason: {entry['BPMSelectionReason']}\n\n")

        # Pattern analysis
        f.write("─── PATTERN ANALYSIS ───\n\n")
        ratio_counts = {}
        for entry in failure_rows:
            rc = entry["RatioClass"]
            ratio_counts[rc] = ratio_counts.get(rc, 0) + 1
        for rc, cnt in sorted(ratio_counts.items(), key=lambda x: -x[1]):
            f.write(f"  {rc}: {cnt} ({cnt/len(failure_rows)*100:.1f}%)\n")

        # Pattern: onset density vs BPM error direction
        f.write("\nOnset patterns in failures:\n")
        for entry in failure_rows:
            direction = "over" if entry["SelectedBPM"] > entry["Tunebat_BPM"] else "under"
            f.write(f"  {entry['Artist']}: onset={entry['OnsetDensity']}, hf_perc={entry['HFPercussiveScore']}, direction={direction}\n")

    log(f"Wrote 02_failure_analysis.csv and 02_failure_summary.txt")
    log(f"  Scoring failures: {len(scoring_failures)}")
    log(f"  Detection failures: {len(detection_failures)}")

    # Write execution log
    with open(os.path.join(PROOF_DIR, "execution_log.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))

    log("\nSTEPS 1-3 COMPLETE")
    return baseline_rows, calibration, scoring_failures, detection_failures


if __name__ == "__main__":
    main()
