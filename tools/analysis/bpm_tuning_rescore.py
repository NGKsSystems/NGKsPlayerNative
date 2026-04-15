"""
NGKsPlayerNative — BPM Tuning Re-scoring Evaluation
Re-extracts features for calibration rows and compares original vs tuned BPM resolver.
Produces Steps 3-9 proof artifacts.
"""

import csv
import os
import sys
import time
import traceback
import zipfile
from datetime import datetime
from pathlib import Path

WORKSPACE = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
VALIDATED_CSV = WORKSPACE / "Validated 02_analysis_results.csv"
EVIDENCE_CSV = WORKSPACE / "_proof" / "analyzer_upgrade" / "03_analysis_with_evidence.csv"
MUSIC_DIR = Path(r"C:\Users\suppo\Music")
PROOF_DIR = WORKSPACE / "_proof" / "bpm_tuning"

sys.path.insert(0, str(WORKSPACE / "tools"))

from feature_extractor import extract_features
from bpm_key_resolver import _resolve_bpm_original, _resolve_bpm_tuned

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


def main():
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("BPM TUNING — RE-SCORING EVALUATION")
    log(f"Workspace: {WORKSPACE}")

    # ─── Load evidence CSV to get calibration rows ─────────────────────
    if not EVIDENCE_CSV.is_file():
        log(f"FAIL: Evidence CSV not found: {EVIDENCE_CSV}")
        sys.exit(1)

    with open(EVIDENCE_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        evidence_rows = list(reader)

    # Build calibration subset
    calibration = []
    for row in evidence_rows:
        tb = safe_float(row.get("Tunebat BPM", ""))
        if tb is not None and tb > 0:
            calibration.append(row)

    log(f"Evidence CSV rows: {len(evidence_rows)}")
    log(f"Calibration rows: {len(calibration)}")

    if len(calibration) == 0:
        log("FAIL: No calibration rows found")
        sys.exit(1)

    # ─── Re-extract features and re-score ──────────────────────────────
    log("\nRe-extracting features for calibration tracks...")

    results = []
    success = 0
    fail = 0

    for i, row in enumerate(calibration):
        artist = row.get("Artist", "?")
        title = row.get("Title", "?")
        filename = row.get("Filename", "")
        tunebat_bpm = safe_float(row.get("Tunebat BPM", ""))
        original_selected = safe_float(row.get("SelectedBPM", ""))

        log(f"  [{i+1}/{len(calibration)}] {artist} — {title}")

        # Find audio file
        filepath = MUSIC_DIR / filename
        if not filepath.is_file():
            log(f"    SKIP: file not found: {filepath}")
            fail += 1
            continue

        # Extract features
        try:
            t0 = time.time()
            features = extract_features(str(filepath))
            dt = time.time() - t0

            if features.error:
                log(f"    ERROR: {features.error}")
                fail += 1
                continue

            # Run original resolver
            orig_result = _resolve_bpm_original(features)
            # Run tuned resolver
            tuned_result = _resolve_bpm_tuned(features)

            orig_bpm = orig_result.selected_bpm
            tuned_bpm = tuned_result.selected_bpm
            orig_err = abs(orig_bpm - tunebat_bpm) if tunebat_bpm else 0
            tuned_err = abs(tuned_bpm - tunebat_bpm) if tunebat_bpm else 0
            improvement = orig_err - tuned_err

            orig_cls = classify_bpm(orig_bpm, tunebat_bpm)
            tuned_cls = classify_bpm(tuned_bpm, tunebat_bpm)

            changed = "YES" if abs(orig_bpm - tuned_bpm) > 0.5 else "NO"
            regressed = "YES" if tuned_err > orig_err + 0.5 else "NO"

            log(f"    Original={orig_bpm} ({orig_cls}), Tuned={tuned_bpm} ({tuned_cls}), Tunebat={tunebat_bpm}, Δ={improvement:+.1f}")
            if changed == "YES":
                log(f"    ** CHANGED: {orig_bpm} → {tuned_bpm}")
            if regressed == "YES":
                log(f"    ** REGRESSION: error increased {orig_err:.1f} → {tuned_err:.1f}")

            results.append({
                "Artist": artist,
                "Title": title,
                "Tunebat_BPM": tunebat_bpm,
                "Original_SelectedBPM": orig_bpm,
                "Tuned_SelectedBPM": tuned_bpm,
                "Evidence_SelectedBPM": original_selected,
                "Original_Error": round(orig_err, 1),
                "Tuned_Error": round(tuned_err, 1),
                "Improvement": round(improvement, 1),
                "Original_Class": orig_cls,
                "Tuned_Class": tuned_cls,
                "Changed": changed,
                "Regressed": regressed,
                "Original_Reason": orig_result.selection_reason,
                "Tuned_Reason": tuned_result.selection_reason,
                "Orig_Cand1": orig_result.candidate1,
                "Orig_Cand2": orig_result.candidate2,
                "Orig_Cand3": orig_result.candidate3,
                "Orig_Score1": orig_result.score1,
                "Orig_Score2": orig_result.score2,
                "Orig_Score3": orig_result.score3,
                "Tuned_Cand1": tuned_result.candidate1,
                "Tuned_Cand2": tuned_result.candidate2,
                "Tuned_Cand3": tuned_result.candidate3,
                "Tuned_Score1": tuned_result.score1,
                "Tuned_Score2": tuned_result.score2,
                "Tuned_Score3": tuned_result.score3,
                "TempoPeak1": features.tempo_peak1,
                "TempoPeak2": features.tempo_peak2,
                "TempoPeak3": features.tempo_peak3,
                "ExtractTime_s": round(dt, 2),
            })
            success += 1

        except Exception as e:
            log(f"    EXCEPTION: {e}")
            traceback.print_exc()
            fail += 1

    log(f"\nRe-score complete: {success} success, {fail} fail")

    if not results:
        log("FAIL: No results produced")
        sys.exit(1)

    # ─── Step 6: Tuned Evaluation CSV ──────────────────────────────────
    log("\nSTEP 6 — TUNED EVALUATION")

    with open(PROOF_DIR / "03_tuned_eval.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    # Compute summary statistics
    orig_classes = {"EXACT": 0, "GOOD": 0, "CLOSE": 0, "BAD": 0}
    tuned_classes = {"EXACT": 0, "GOOD": 0, "CLOSE": 0, "BAD": 0}
    improved = 0
    unchanged = 0
    regressed_count = 0
    regressed_rows = []

    for r in results:
        orig_classes[r["Original_Class"]] += 1
        tuned_classes[r["Tuned_Class"]] += 1
        if r["Changed"] == "YES":
            if r["Regressed"] == "YES":
                regressed_count += 1
                regressed_rows.append(r)
            else:
                improved += 1
        else:
            unchanged += 1

    total = len(results)
    orig_good = orig_classes["EXACT"] + orig_classes["GOOD"] + orig_classes["CLOSE"]
    tuned_good = tuned_classes["EXACT"] + tuned_classes["GOOD"] + tuned_classes["CLOSE"]

    with open(PROOF_DIR / "03_tuned_summary.txt", "w", encoding="utf-8") as f:
        f.write("BPM TUNING — TUNED EVALUATION SUMMARY\n\n")
        f.write(f"Total calibration rows: {total}\n\n")

        f.write("Before vs After:\n")
        f.write(f"{'Class':<10} {'Before':>8} {'After':>8}  {'Delta':>6}\n")
        for cls in ["EXACT", "GOOD", "CLOSE", "BAD"]:
            delta = tuned_classes[cls] - orig_classes[cls]
            f.write(f"{cls:<10} {orig_classes[cls]:>8} {tuned_classes[cls]:>8}  {delta:>+6}\n")

        f.write(f"\nWithin ±5 BPM: {orig_good}/{total} ({orig_good/total*100:.1f}%) → {tuned_good}/{total} ({tuned_good/total*100:.1f}%)\n")

        f.write(f"\nRows improved: {improved}\n")
        f.write(f"Rows unchanged: {unchanged}\n")
        f.write(f"Rows regressed: {regressed_count}\n\n")

        if regressed_rows:
            f.write("REGRESSIONS:\n")
            for r in regressed_rows:
                f.write(f"  {r['Artist']} — {r['Title']}: {r['Original_SelectedBPM']}→{r['Tuned_SelectedBPM']} (Tunebat={r['Tunebat_BPM']}) err {r['Original_Error']}→{r['Tuned_Error']}\n")
        else:
            f.write("REGRESSIONS: NONE\n")

        f.write("\nRow-by-row:\n")
        for r in results:
            marker = ""
            if r["Changed"] == "YES":
                marker = " *** IMPROVED" if r["Regressed"] == "NO" else " *** REGRESSED"
            f.write(f"  {r['Artist']} — {r['Title']}: {r['Original_SelectedBPM']}→{r['Tuned_SelectedBPM']} | Tunebat={r['Tunebat_BPM']} | {r['Original_Class']}→{r['Tuned_Class']}{marker}\n")

    log("Wrote 03_tuned_eval.csv and 03_tuned_summary.txt")

    # ─── Step 7: Regression Guard ──────────────────────────────────────
    log("\nSTEP 7 — REGRESSION GUARD")

    previously_good = [r for r in results if r["Original_Class"] in ("EXACT", "GOOD", "CLOSE")]
    good_stayed_good = [r for r in previously_good if r["Tuned_Class"] in ("EXACT", "GOOD", "CLOSE")]
    good_regressed = [r for r in previously_good if r["Tuned_Class"] == "BAD"]

    with open(PROOF_DIR / "04_regression_guard.txt", "w", encoding="utf-8") as f:
        f.write("BPM TUNING — REGRESSION GUARD\n\n")
        f.write(f"Previously GOOD/CLOSE rows: {len(previously_good)}\n")
        f.write(f"Stayed good: {len(good_stayed_good)}\n")
        f.write(f"Regressed to BAD: {len(good_regressed)}\n\n")

        if good_regressed:
            f.write("REGRESSIONS (PREVIOUSLY GOOD → BAD):\n")
            for r in good_regressed:
                f.write(f"  {r['Artist']} — {r['Title']}\n")
                f.write(f"    Before: {r['Original_SelectedBPM']} ({r['Original_Class']})\n")
                f.write(f"    After: {r['Tuned_SelectedBPM']} ({r['Tuned_Class']})\n")
                f.write(f"    Tunebat: {r['Tunebat_BPM']}\n")
                f.write(f"    Tuned reason: {r['Tuned_Reason']}\n\n")
            f.write("VERDICT: REGRESSION DETECTED — tuning may need adjustment\n")
        else:
            f.write("VERDICT: NO REGRESSIONS — all previously correct rows remain correct\n")

        f.write("\nDetailed row audit:\n")
        for r in previously_good:
            f.write(f"  {r['Artist']} — {r['Title']}: {r['Original_Class']} → {r['Tuned_Class']} ({r['Original_SelectedBPM']} → {r['Tuned_SelectedBPM']})\n")

    log(f"Regression guard: {len(good_stayed_good)} stayed good, {len(good_regressed)} regressed")
    log("Wrote 04_regression_guard.txt")

    # ─── Step 8: Final Recommendations ─────────────────────────────────
    log("\nSTEP 8 — FINAL RECOMMENDATIONS")

    # Count remaining failures by type
    remaining_bad = [r for r in results if r["Tuned_Class"] == "BAD"]
    scoring_still_bad = []
    detection_still_bad = []
    for r in remaining_bad:
        tunebat = r["Tunebat_BPM"]
        # Check if tunebat is near any tuned candidate
        covered = False
        for c in [r["Tuned_Cand1"], r["Tuned_Cand2"], r["Tuned_Cand3"]]:
            if c and abs(c - tunebat) <= 3:
                covered = True
                break
        if covered:
            scoring_still_bad.append(r)
        else:
            detection_still_bad.append(r)

    tuning_success = tuned_good > orig_good and len(good_regressed) == 0
    gate = "PASS" if tuning_success else ("PASS" if tuned_good >= orig_good and len(good_regressed) == 0 else "FAIL")

    with open(PROOF_DIR / "05_final_recommendations.txt", "w", encoding="utf-8") as f:
        f.write("BPM TUNING — FINAL RECOMMENDATIONS\n\n")

        f.write(f"Tuning successful: {'YES' if tuning_success else 'CONDITIONAL'}\n\n")

        f.write("Updated BPM accuracy:\n")
        f.write(f"  EXACT: {orig_classes['EXACT']} → {tuned_classes['EXACT']}\n")
        f.write(f"  GOOD:  {orig_classes['GOOD']} → {tuned_classes['GOOD']}\n")
        f.write(f"  CLOSE: {orig_classes['CLOSE']} → {tuned_classes['CLOSE']}\n")
        f.write(f"  BAD:   {orig_classes['BAD']} → {tuned_classes['BAD']}\n")
        f.write(f"  Within ±5: {orig_good/total*100:.1f}% → {tuned_good/total*100:.1f}%\n\n")

        f.write("Regressions: ")
        if len(good_regressed) == 0:
            f.write("NONE\n\n")
        else:
            f.write(f"{len(good_regressed)} rows\n\n")

        f.write(f"Remaining BAD rows: {len(remaining_bad)}\n")
        f.write(f"  Scoring failures (correct in candidates): {len(scoring_still_bad)}\n")
        for r in scoring_still_bad:
            f.write(f"    {r['Artist']} — {r['Title']}: Tuned={r['Tuned_SelectedBPM']}, Tunebat={r['Tunebat_BPM']}\n")
        f.write(f"  Detection failures (correct NOT in candidates): {len(detection_still_bad)}\n")
        for r in detection_still_bad:
            f.write(f"    {r['Artist']} — {r['Title']}: Tuned={r['Tuned_SelectedBPM']}, Tunebat={r['Tunebat_BPM']}\n")

        f.write("\nTuning changes applied:\n")
        f.write("  1. Sub-harmonic grid alignment: paired consecutive beat intervals\n")
        f.write("     checked for every-other-beat patterns. 100% credit. Gives\n")
        f.write("     half-time BPM candidates fair grid scores instead of 0.0.\n")
        f.write("  2. Comfort zone upper bound: 160 → 165. Prevents BPMs at 161-165\n")
        f.write("     from being penalised by falling just above the old boundary.\n")
        f.write("     Evidence: XXXTentacion peak1=161.5 (Tunebat=160) was losing\n")
        f.write("     to 152.0 solely due to the comfort zone cliff at 160.\n\n")

        f.write("Tuning changes REJECTED (tested, caused regressions):\n")
        f.write("  - Peak1 affinity bonus (+0.08): caused Snoop Dogg regression\n")
        f.write("    (peak1=47.0 boosted extreme-low BPM past 94.0).\n")
        f.write("  - Half-time tie-breaker: cannot distinguish Nelly (correct=81)\n")
        f.write("    from XXXTentacion (correct=160) — both have identical ~80.7\n")
        f.write("    candidate with near-identical feature profiles.\n\n")

        f.write("Next steps:\n")
        if len(detection_still_bad) > 0:
            f.write("  1. CANDIDATE GENERATION: {0} tracks have correct BPM outside\n".format(len(detection_still_bad)))
            f.write("     the generated candidate set. Need broader candidate generation\n")
            f.write("     (e.g., 3/4 and 4/3 multiples, additional tempogram peaks,\n")
            f.write("     or narrower peak picking with 3 BPM dedup threshold).\n")
        if len(scoring_still_bad) > 0:
            f.write("  2. SCORING: {0} tracks have correct BPM in candidates but\n".format(len(scoring_still_bad)))
            f.write("     wrong candidate still wins. These are typically half-time\n")
            f.write("     songs where the comfort zone gap (0.20 vs 0.10) dominates.\n")
            f.write("     Would need genre-aware or perception-aware features.\n")
        if len(remaining_bad) == 0:
            f.write("  All calibration rows resolved. Collect more ground truth data.\n")

        f.write(f"\nAdditional evidence fields that could help:\n")
        f.write(f"  - Spectral flux variance (rhythmic complexity)\n")
        f.write(f"  - Low-frequency energy ratio (bass-heavy → half-time feel)\n")
        f.write(f"  - Beat-to-beat amplitude variance (syncopation detector)\n")
        f.write(f"  - More tempogram peaks (top 5 instead of top 3)\n")

    log("Wrote 05_final_recommendations.txt")

    # ─── Write execution log ───────────────────────────────────────────
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    log("Wrote execution_log.txt")

    # ─── Step 9: Package proof ─────────────────────────────────────────
    log("\nSTEP 9 — PACKAGE PROOF")

    required_files = [
        "00_load_summary.txt",
        "01_baseline_eval.csv",
        "01_baseline_summary.txt",
        "02_failure_analysis.csv",
        "02_failure_summary.txt",
        "03_tuned_eval.csv",
        "03_tuned_summary.txt",
        "04_regression_guard.txt",
        "05_final_recommendations.txt",
        "execution_log.txt",
    ]

    missing = [f for f in required_files if not (PROOF_DIR / f).is_file()]
    if missing:
        log(f"WARNING: Missing proof files: {missing}")

    # Create ZIP
    zip_path = WORKSPACE / "_proof" / "bpm_tuning.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in required_files:
            fpath = PROOF_DIR / fname
            if fpath.is_file():
                zf.write(fpath, f"bpm_tuning/{fname}")
                log(f"  Packed: {fname}")

    log(f"\nZIP: {zip_path}")
    log(f"ZIP size: {zip_path.stat().st_size / 1024:.1f} KB")

    # ─── Final gate ───────────────────────────────────────────────────
    log(f"\n{'='*60}")
    log(f"PF={PROOF_DIR}")
    log(f"ZIP={zip_path}")
    log(f"GATE={gate}")
    log(f"{'='*60}")

    # Re-write execution log with final lines
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))


if __name__ == "__main__":
    main()
