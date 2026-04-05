"""
NGKsPlayerNative — BPM Phase 4: Perceptual BPM Resolution Evaluation
Re-extracts features and applies octave resolution (post-scoring perception layer).
Compares against Phase 2 tuned baseline. Scoring logic unchanged from Phase 2.
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
EVIDENCE_CSV = WORKSPACE / "_proof" / "analyzer_upgrade" / "03_analysis_with_evidence.csv"
PHASE2_CSV = WORKSPACE / "_proof" / "bpm_tuning" / "03_tuned_eval.csv"
MUSIC_DIR = Path(r"C:\Users\suppo\Music")
PROOF_DIR = WORKSPACE / "_proof" / "bpm_phase4"

sys.path.insert(0, str(WORKSPACE / "tools"))

from feature_extractor import extract_features
from bpm_key_resolver import _resolve_bpm_tuned

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

    log("BPM PHASE 4 — PERCEPTUAL BPM RESOLUTION EVALUATION")
    log(f"Workspace: {WORKSPACE}")

    # ─── Load evidence CSV for calibration rows + file paths ───────────
    if not EVIDENCE_CSV.is_file():
        log(f"FAIL: Evidence CSV not found: {EVIDENCE_CSV}")
        sys.exit(1)

    with open(EVIDENCE_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        evidence_rows = list(reader)

    calibration = []
    for row in evidence_rows:
        tb = safe_float(row.get("Tunebat BPM", ""))
        if tb is not None and tb > 0:
            calibration.append(row)

    log(f"Evidence CSV rows: {len(evidence_rows)}")
    log(f"Calibration rows: {len(calibration)}")

    # ─── Load Phase 2 baseline for comparison ─────────────────────────
    phase2_baseline = {}
    if PHASE2_CSV.is_file():
        with open(PHASE2_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = f"{row.get('Artist', '')}|{row.get('Title', '')}"
                phase2_baseline[key] = row
        log(f"Phase 2 baseline rows loaded: {len(phase2_baseline)}")
    else:
        log("WARNING: Phase 2 baseline CSV not found — will compare against self only")

    # ─── Re-extract and re-score ──────────────────────────────────────
    log("\nRe-extracting features (Phase 4: 8 peaks + median-IBI + octave resolution)...")

    results = []
    success = 0
    fail = 0

    for i, row in enumerate(calibration):
        artist = row.get("Artist", "?")
        title = row.get("Title", "?")
        filename = row.get("Filename", "")
        tunebat_bpm = safe_float(row.get("Tunebat BPM", ""))

        log(f"  [{i+1}/{len(calibration)}] {artist} — {title}")

        filepath = MUSIC_DIR / filename
        if not filepath.is_file():
            log(f"    SKIP: file not found: {filepath}")
            fail += 1
            continue

        try:
            t0 = time.time()
            features = extract_features(str(filepath))
            dt = time.time() - t0

            if features.error:
                log(f"    ERROR: {features.error}")
                fail += 1
                continue

            # Run tuned resolver (scoring unchanged, candidates expanded)
            bpm_result = _resolve_bpm_tuned(features)
            selected_bpm = bpm_result.selected_bpm
            _tunebat = tunebat_bpm or 0.0
            selected_err = abs(selected_bpm - _tunebat)
            selected_cls = classify_bpm(selected_bpm, _tunebat)

            # Phase 2 comparison
            p2key = f"{artist}|{title}"
            p2row = phase2_baseline.get(p2key, {})
            p2_bpm = safe_float(p2row.get("Tuned_SelectedBPM", "")) or 0
            p2_err = abs(p2_bpm - _tunebat) if p2_bpm > 0 else 0
            p2_cls = p2row.get("Tuned_Class", "?")

            changed = "YES" if abs(selected_bpm - p2_bpm) > 0.5 else "NO"
            regressed = "YES" if selected_err > p2_err + 0.5 else "NO"
            improvement = p2_err - selected_err

            # Collect all peaks for reporting
            all_peaks = []
            for j in range(1, 9):
                pk = getattr(features, f"tempo_peak{j}", 0.0)
                ps = getattr(features, f"tempo_peak_strength{j}", 0.0)
                if pk > 0:
                    all_peaks.append(f"{pk:.1f}(s={ps:.2f})")

            # Collect all candidates from the resolver
            all_cands = []
            if bpm_result.candidate1 > 0:
                all_cands.append(f"{bpm_result.candidate1:.1f}")
            if bpm_result.candidate2 > 0:
                all_cands.append(f"{bpm_result.candidate2:.1f}")
            if bpm_result.candidate3 > 0:
                all_cands.append(f"{bpm_result.candidate3:.1f}")

            status_marker = ""
            if changed == "YES" and regressed == "NO":
                status_marker = " ** IMPROVED" if improvement > 0.5 else " ** CHANGED"
            elif regressed == "YES":
                status_marker = " ** REGRESSION"

            octave_tag = ""
            if bpm_result.octave_ambiguous:
                if "OCTAVE_RESOLVED" in bpm_result.selection_reason:
                    octave_tag = f" [OCTAVE_RESOLVED alt={bpm_result.alternate_bpm}]"
                else:
                    octave_tag = f" [OCTAVE_AMBIGUOUS alt={bpm_result.alternate_bpm}]"

            log(f"    Phase2={p2_bpm}({p2_cls}) \u2192 Phase4={selected_bpm}({selected_cls}), Tunebat={tunebat_bpm}, \u0394={improvement:+.1f}{status_marker}{octave_tag}")
            log(f"    MedianIBI={features.median_ibi_bpm:.1f}, BeatTrackTempo={features.beat_track_tempo:.1f}, PercBPM={features.percussive_median_ibi_bpm:.1f}")
            alt_peaks_str = ", ".join(f"{getattr(features, f'alt_tempo_peak{j}', 0.0):.1f}(s={getattr(features, f'alt_tempo_peak_strength{j}', 0.0):.2f})" for j in range(1,4) if getattr(features, f'alt_tempo_peak{j}', 0.0) > 0)
            log(f"    Peaks=[{', '.join(all_peaks)}]")
            log(f"    AltPeaks(hop=1024)=[{alt_peaks_str}]")

            results.append({
                "Artist": artist,
                "Title": title,
                "Tunebat_BPM": tunebat_bpm,
                "Phase2_BPM": p2_bpm,
                "Phase4_BPM": selected_bpm,
                "Phase2_Error": round(p2_err, 1),
                "Phase4_Error": round(selected_err, 1),
                "Improvement": round(improvement, 1),
                "Phase2_Class": p2_cls,
                "Phase4_Class": selected_cls,
                "Changed": changed,
                "Regressed": regressed,
                "Octave_Ambiguous": bpm_result.octave_ambiguous,
                "Alternate_BPM": bpm_result.alternate_bpm,
                "MedianIBI_BPM": features.median_ibi_bpm,
                "BeatTrackTempo": features.beat_track_tempo,
                "PercussiveBPM": features.percussive_median_ibi_bpm,
                "HFPS": features.hf_percussive_score,
                "BISD": features.beat_interval_std,
                "OnsetDensity": features.onset_density,
                "Phase4_Reason": bpm_result.selection_reason,
                "Cand1": bpm_result.candidate1,
                "Cand2": bpm_result.candidate2,
                "Cand3": bpm_result.candidate3,
                "Score1": bpm_result.score1,
                "Score2": bpm_result.score2,
                "Score3": bpm_result.score3,
                "TempoPeak1": features.tempo_peak1,
                "TempoPeak2": features.tempo_peak2,
                "TempoPeak3": features.tempo_peak3,
                "TempoPeak4": features.tempo_peak4,
                "TempoPeak5": features.tempo_peak5,
                "TempoPeak6": features.tempo_peak6,
                "TempoPeak7": features.tempo_peak7,
                "TempoPeak8": features.tempo_peak8,
                "AltPeak1": features.alt_tempo_peak1,
                "AltPeak2": features.alt_tempo_peak2,
                "AltPeak3": features.alt_tempo_peak3,
                "ExtractTime_s": round(dt, 2),
            })
            success += 1

        except Exception as e:
            log(f"    EXCEPTION: {e}")
            traceback.print_exc()
            fail += 1

    log(f"\nPhase 4 re-score: {success} success, {fail} fail")

    if not results:
        log("FAIL: No results produced")
        sys.exit(1)

    # ─── Write results CSV ─────────────────────────────────────────────
    csv_path = PROOF_DIR / "01_phase4_eval.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    log(f"Wrote {csv_path}")

    # ─── Summary ──────────────────────────────────────────────────────
    p2_classes = {"EXACT": 0, "GOOD": 0, "CLOSE": 0, "BAD": 0}
    p4_classes = {"EXACT": 0, "GOOD": 0, "CLOSE": 0, "BAD": 0}
    improved_rows = []
    regressed_rows = []
    unchanged_count = 0

    for r in results:
        p2c = r["Phase2_Class"]
        p4c = r["Phase4_Class"]
        if p2c in p2_classes:
            p2_classes[p2c] += 1
        p4_classes[p4c] += 1

        if r["Changed"] == "YES":
            if r["Regressed"] == "YES":
                regressed_rows.append(r)
            elif r["Improvement"] > 0.5:
                improved_rows.append(r)
        else:
            unchanged_count += 1

    total = len(results)
    gate = "PASS" if len(regressed_rows) == 0 else "FAIL"

    p2_good_close = p2_classes["EXACT"] + p2_classes["GOOD"] + p2_classes["CLOSE"]
    p4_good_close = p4_classes["EXACT"] + p4_classes["GOOD"] + p4_classes["CLOSE"]

    log(f"\n{'='*60}")
    log(f"PHASE 4 SUMMARY — REGRESSION GATE: {gate}")
    log(f"{'='*60}")
    log(f"Phase 2 baseline: EXACT={p2_classes['EXACT']} GOOD={p2_classes['GOOD']} CLOSE={p2_classes['CLOSE']} BAD={p2_classes['BAD']} (≤5BPM: {p2_good_close}/{total} = {p2_good_close/total*100:.1f}%)")
    log(f"Phase 4 result:   EXACT={p4_classes['EXACT']} GOOD={p4_classes['GOOD']} CLOSE={p4_classes['CLOSE']} BAD={p4_classes['BAD']} (≤5BPM: {p4_good_close}/{total} = {p4_good_close/total*100:.1f}%)")
    log(f"Changed: {len(improved_rows)} improved, {len(regressed_rows)} regressed, {unchanged_count} unchanged")

    if improved_rows:
        log(f"\nIMPROVED ROWS:")
        for r in improved_rows:
            log(f"  {r['Artist']} — {r['Title']}: {r['Phase2_BPM']}({r['Phase2_Class']}) → {r['Phase4_BPM']}({r['Phase4_Class']}), Tunebat={r['Tunebat_BPM']}")

    if regressed_rows:
        log(f"\nREGRESSED ROWS:")
        for r in regressed_rows:
            log(f"  {r['Artist']} — {r['Title']}: {r['Phase2_BPM']}({r['Phase2_Class']}) → {r['Phase4_BPM']}({r['Phase4_Class']}), Tunebat={r['Tunebat_BPM']}")

    # ─── Write summary text ───────────────────────────────────────────
    summary_path = PROOF_DIR / "02_phase4_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("BPM PHASE 4 — PERCEPTUAL BPM RESOLUTION SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Regression Gate: {gate}\n\n")

        f.write("Changes from Phase 2/3:\n")
        f.write("  - Phase 3 candidate expansion (8 tempogram peaks, median-IBI, HPSS, multi-res)\n")
        f.write("  - Phase 4 post-scoring octave resolution layer\n")
        f.write("  - Feature gate: HFPS < 0.025 OR (HFPS < 0.035 AND BISD > 0.02)\n")
        f.write("  - When selected BPM > 125 and strong half-time candidate in 55-90 range,\n")
        f.write("    override to half-time if feature gate indicates sparse/sustained signal\n")
        f.write("  - Scoring logic UNCHANGED from Phase 2 (sub-harmonic grid + comfort 165)\n\n")

        f.write(f"Phase 2 baseline: EXACT={p2_classes['EXACT']} GOOD={p2_classes['GOOD']} CLOSE={p2_classes['CLOSE']} BAD={p2_classes['BAD']} (≤5BPM: {p2_good_close}/{total} = {p2_good_close/total*100:.1f}%)\n")
        f.write(f"Phase 4 result:   EXACT={p4_classes['EXACT']} GOOD={p4_classes['GOOD']} CLOSE={p4_classes['CLOSE']} BAD={p4_classes['BAD']} (≤5BPM: {p4_good_close}/{total} = {p4_good_close/total*100:.1f}%)\n\n")

        f.write(f"Improved: {len(improved_rows)}\n")
        for r in improved_rows:
            f.write(f"  {r['Artist']} — {r['Title']}: {r['Phase2_BPM']}({r['Phase2_Class']}) → {r['Phase4_BPM']}({r['Phase4_Class']}), Tunebat={r['Tunebat_BPM']}\n")
            f.write(f"    HFPS={r.get('HFPS','?')}, BISD={r.get('BISD','?')}, OctaveAmbig={r.get('Octave_Ambiguous','?')}, Reason={r['Phase4_Reason']}\n")

        f.write(f"\nRegressed: {len(regressed_rows)}\n")
        for r in regressed_rows:
            f.write(f"  {r['Artist']} — {r['Title']}: {r['Phase2_BPM']}({r['Phase2_Class']}) → {r['Phase4_BPM']}({r['Phase4_Class']}), Tunebat={r['Tunebat_BPM']}\n")

        f.write(f"\nUnchanged: {unchanged_count}\n\n")

        # Detection failure analysis
        still_bad = [r for r in results if r["Phase4_Class"] == "BAD"]
        if still_bad:
            f.write("REMAINING BAD TRACKS — ANALYSIS:\n")
            f.write("These tracks remain BAD after octave resolution.\n")
            f.write("Root cause varies: detection failure (correct BPM not in any signal)\n")
            f.write("or insufficient feature discrimination (gate cannot safely resolve).\n\n")
            for r in still_bad:
                f.write(f"  {r['Artist']} — {r['Title']}:\n")
                f.write(f"    Tunebat={r['Tunebat_BPM']}, Selected={r['Phase4_BPM']}, Error={r['Phase4_Error']}\n")
                f.write(f"    HFPS={r.get('HFPS','?')}, BISD={r.get('BISD','?')}, OctaveAmbig={r.get('Octave_Ambiguous','?')}\n")
                if r.get('Octave_Ambiguous'):
                    f.write(f"    Octave ambiguity detected but gate blocked (HFPS/BISD out of range).\n")
                else:
                    f.write(f"    No half-time candidate qualified — fundamental detection failure.\n")
                f.write(f"\n")
            f.write("  Remaining options for these tracks:\n")
            f.write("  1. ML-based tempo detection (madmom, TempoCNN)\n")
            f.write("  2. Manual BPM override for known-difficult tracks\n\n")

        f.write("Per-track detail:\n")
        for r in results:
            marker = ""
            if r["Changed"] == "YES":
                marker = " [IMPROVED]" if r["Regressed"] == "NO" and r["Improvement"] > 0.5 else " [CHANGED]"
                if r["Regressed"] == "YES":
                    marker = " [REGRESSED]"
            f.write(f"  {r['Artist']} — {r['Title']}: P2={r['Phase2_BPM']}({r['Phase2_Class']}) → P4={r['Phase4_BPM']}({r['Phase4_Class']}) Tunebat={r['Tunebat_BPM']} HFPS={r.get('HFPS','?')} OctaveAmbig={r.get('Octave_Ambiguous','?')}{marker}\n")

    log(f"Wrote {summary_path}")

    # ─── Write log ────────────────────────────────────────────────────
    log_path = PROOF_DIR / "03_phase4_log.txt"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    log(f"Wrote {log_path}")

    # ─── Create ZIP ───────────────────────────────────────────────────
    zip_path = WORKSPACE / "_proof" / "bpm_phase4.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(PROOF_DIR.iterdir()):
            if p.is_file():
                zf.write(p, f"bpm_phase4/{p.name}")
    log(f"Wrote {zip_path}")

    print(f"\n{'='*60}")
    print(f"PHASE 4 GATE: {gate}")
    print(f"{'='*60}")

    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
