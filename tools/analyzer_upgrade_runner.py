"""
NGKsPlayerNative — Analyzer Upgrade Runner
Loads existing analysis CSV, runs Python-based feature extraction + BPM/Key resolver,
appends evidence columns, writes enriched CSV and proof artifacts.
"""

import csv
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────
ROOT = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
MUSIC_DIR = Path(r"C:\Users\suppo\Music")
SOURCE_CSV = ROOT / "_proof" / "library_full_analysis_20260402_111026" / "02_analysis_results.csv"
PROOF_DIR = ROOT / "_proof" / "analyzer_upgrade"

# Add tools dir for imports
sys.path.insert(0, str(ROOT / "tools"))

from feature_extractor import extract_features  # noqa: E402
from bpm_key_resolver import resolve_bpm, resolve_key, PITCH_CLASSES, CAMELOT_MAP  # noqa: E402

# ── New columns to append ─────────────────────────────────────────────
NEW_COLUMNS = [
    "TempoPeak1", "TempoPeak2", "TempoPeak3",
    "TempoPeakStrength1", "TempoPeakStrength2", "TempoPeakStrength3",
    "BeatIntervalStdDev", "DownbeatConfidence", "EstimatedMeter",
    "BPMCandidate1", "BPMCandidate2", "BPMCandidate3",
    "BPMCandidateScore1", "BPMCandidateScore2", "BPMCandidateScore3",
    "SelectedBPM", "SelectedBPMConfidence", "BPMSelectionReason",
    "Chroma_C", "Chroma_Cs", "Chroma_D", "Chroma_Ds", "Chroma_E", "Chroma_F",
    "Chroma_Fs", "Chroma_G", "Chroma_Gs", "Chroma_A", "Chroma_As", "Chroma_B",
    "KeyCandidate1", "KeyCandidate2", "KeyCandidate3",
    "KeyCandidateScore1", "KeyCandidateScore2", "KeyCandidateScore3",
    "TonalClarity", "KeyChangeDetected",
    "SelectedKey", "SelectedKeyConfidence", "KeySelectionReason",
]

CHROMA_KEYS = [
    "Chroma_C", "Chroma_Cs", "Chroma_D", "Chroma_Ds", "Chroma_E", "Chroma_F",
    "Chroma_Fs", "Chroma_G", "Chroma_Gs", "Chroma_A", "Chroma_As", "Chroma_B",
]


def main():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_file = datetime.now().strftime("%Y%m%d_%H%M%S")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    log_path = PROOF_DIR / "execution_log.txt"
    log_file = open(log_path, "w", encoding="utf-8")

    def log(msg: str):
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)
        log_file.write(line + "\n")
        log_file.flush()

    log(f"=== ANALYZER UPGRADE RUNNER ===")
    log(f"timestamp={timestamp}")
    log(f"source={SOURCE_CSV}")
    log(f"proof_dir={PROOF_DIR}")

    # ── Load source CSV ──
    if not SOURCE_CSV.exists():
        log(f"FAIL-CLOSED: Source CSV not found: {SOURCE_CSV}")
        log_file.close()
        sys.exit(1)

    with open(SOURCE_CSV, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        original_columns = list(reader.fieldnames or [])
        rows = list(reader)

    log(f"Loaded {len(rows)} rows, {len(original_columns)} columns")

    # ── Prepare output columns ──
    output_columns = original_columns + NEW_COLUMNS

    # ── BPM debug CSV ──
    bpm_debug_path = PROOF_DIR / "02_bpm_candidates_debug.csv"
    bpm_debug_file = open(bpm_debug_path, "w", encoding="utf-8", newline="")
    bpm_debug_writer = csv.writer(bpm_debug_file)
    bpm_debug_writer.writerow([
        "Filename", "TempoPeak1", "TempoPeak2", "TempoPeak3",
        "Strength1", "Strength2", "Strength3",
        "BeatIntervalStdDev", "EstimatedMeter", "DownbeatConfidence",
        "Cand1", "Score1", "Cand2", "Score2", "Cand3", "Score3",
        "SelectedBPM", "Confidence", "OriginalResolvedBPM", "Reason",
    ])

    # ── Key debug CSV ──
    key_debug_path = PROOF_DIR / "03_key_candidates_debug.csv"
    key_debug_file = open(key_debug_path, "w", encoding="utf-8", newline="")
    key_debug_writer = csv.writer(key_debug_file)
    key_debug_writer.writerow([
        "Filename", "TonalClarity",
        "Cand1", "Score1", "Cand2", "Score2", "Cand3", "Score3",
        "KeyChangeDetected", "SelectedKey", "Confidence",
        "OriginalKey", "Reason",
    ] + CHROMA_KEYS)

    # ── Process tracks ──
    enriched_rows = []
    success_count = 0
    fail_count = 0
    skip_count = 0
    start_time = time.time()

    for idx, row in enumerate(rows):
        filename = row.get("Filename", "")
        filepath = MUSIC_DIR / filename

        # Progress
        if (idx + 1) % 25 == 0 or idx == 0:
            elapsed = time.time() - start_time
            rate = (idx + 1) / elapsed if elapsed > 0 else 0
            eta_s = (len(rows) - idx - 1) / rate if rate > 0 else 0
            log(f"[{idx+1}/{len(rows)}] rate={rate:.1f} trk/s ETA={eta_s/60:.0f}m | {filename}")

        # Initialize new columns with empty values
        new_vals = {col: "" for col in NEW_COLUMNS}

        if not filepath.exists():
            new_vals["BPMSelectionReason"] = "FILE_NOT_FOUND"
            new_vals["KeySelectionReason"] = "FILE_NOT_FOUND"
            skip_count += 1
            row.update(new_vals)
            enriched_rows.append(row)
            continue

        # Check if original analysis was valid
        if row.get("AnalysisValid", "").strip().upper() not in ("TRUE", "1"):
            new_vals["BPMSelectionReason"] = "ORIGINAL_ANALYSIS_INVALID"
            new_vals["KeySelectionReason"] = "ORIGINAL_ANALYSIS_INVALID"
            skip_count += 1
            row.update(new_vals)
            enriched_rows.append(row)
            continue

        try:
            # ── Feature extraction ──
            features = extract_features(str(filepath))

            if features.error:
                new_vals["BPMSelectionReason"] = f"EXTRACT_ERROR: {features.error}"
                new_vals["KeySelectionReason"] = f"EXTRACT_ERROR: {features.error}"
                fail_count += 1
                row.update(new_vals)
                enriched_rows.append(row)
                log(f"  EXTRACT_ERROR [{filename}]: {features.error}")
                continue

            # ── BPM resolution ──
            bpm_result = resolve_bpm(features)

            new_vals["TempoPeak1"] = f"{features.tempo_peak1:.1f}"
            new_vals["TempoPeak2"] = f"{features.tempo_peak2:.1f}"
            new_vals["TempoPeak3"] = f"{features.tempo_peak3:.1f}"
            new_vals["TempoPeakStrength1"] = f"{features.tempo_peak_strength1:.4f}"
            new_vals["TempoPeakStrength2"] = f"{features.tempo_peak_strength2:.4f}"
            new_vals["TempoPeakStrength3"] = f"{features.tempo_peak_strength3:.4f}"
            new_vals["BeatIntervalStdDev"] = f"{features.beat_interval_std:.6f}"
            new_vals["DownbeatConfidence"] = f"{features.downbeat_confidence:.4f}"
            new_vals["EstimatedMeter"] = str(features.estimated_meter)
            new_vals["BPMCandidate1"] = f"{bpm_result.candidate1:.1f}"
            new_vals["BPMCandidate2"] = f"{bpm_result.candidate2:.1f}"
            new_vals["BPMCandidate3"] = f"{bpm_result.candidate3:.1f}"
            new_vals["BPMCandidateScore1"] = f"{bpm_result.score1:.4f}"
            new_vals["BPMCandidateScore2"] = f"{bpm_result.score2:.4f}"
            new_vals["BPMCandidateScore3"] = f"{bpm_result.score3:.4f}"
            new_vals["SelectedBPM"] = f"{bpm_result.selected_bpm:.1f}"
            new_vals["SelectedBPMConfidence"] = f"{bpm_result.selected_confidence:.4f}"
            new_vals["BPMSelectionReason"] = bpm_result.selection_reason

            # BPM debug row
            bpm_debug_writer.writerow([
                filename,
                f"{features.tempo_peak1:.1f}", f"{features.tempo_peak2:.1f}", f"{features.tempo_peak3:.1f}",
                f"{features.tempo_peak_strength1:.4f}", f"{features.tempo_peak_strength2:.4f}", f"{features.tempo_peak_strength3:.4f}",
                f"{features.beat_interval_std:.6f}", features.estimated_meter, f"{features.downbeat_confidence:.4f}",
                f"{bpm_result.candidate1:.1f}", f"{bpm_result.score1:.4f}",
                f"{bpm_result.candidate2:.1f}", f"{bpm_result.score2:.4f}",
                f"{bpm_result.candidate3:.1f}", f"{bpm_result.score3:.4f}",
                f"{bpm_result.selected_bpm:.1f}", f"{bpm_result.selected_confidence:.4f}",
                row.get("ResolvedBPM", ""),
                bpm_result.selection_reason,
            ])

            # ── Key resolution ──
            key_result = resolve_key(features)

            # Chroma values
            for i, ck in enumerate(CHROMA_KEYS):
                new_vals[ck] = f"{features.chroma[i]:.4f}" if i < len(features.chroma) else ""

            new_vals["KeyCandidate1"] = key_result.candidate1
            new_vals["KeyCandidate2"] = key_result.candidate2
            new_vals["KeyCandidate3"] = key_result.candidate3
            new_vals["KeyCandidateScore1"] = f"{key_result.score1:.4f}"
            new_vals["KeyCandidateScore2"] = f"{key_result.score2:.4f}"
            new_vals["KeyCandidateScore3"] = f"{key_result.score3:.4f}"
            new_vals["TonalClarity"] = f"{key_result.tonal_clarity:.4f}"
            new_vals["KeyChangeDetected"] = str(key_result.key_change_detected)
            new_vals["SelectedKey"] = key_result.selected_key
            new_vals["SelectedKeyConfidence"] = f"{key_result.selected_confidence:.4f}"
            new_vals["KeySelectionReason"] = key_result.selection_reason

            # Key debug row
            chroma_vals = [f"{features.chroma[i]:.4f}" for i in range(12)]
            key_debug_writer.writerow([
                filename, f"{key_result.tonal_clarity:.4f}",
                key_result.candidate1, f"{key_result.score1:.4f}",
                key_result.candidate2, f"{key_result.score2:.4f}",
                key_result.candidate3, f"{key_result.score3:.4f}",
                str(key_result.key_change_detected),
                key_result.selected_key, f"{key_result.selected_confidence:.4f}",
                row.get("Key", ""),
                key_result.selection_reason,
            ] + chroma_vals)

            success_count += 1

        except Exception as e:
            new_vals["BPMSelectionReason"] = f"EXCEPTION: {e}"
            new_vals["KeySelectionReason"] = f"EXCEPTION: {e}"
            fail_count += 1
            log(f"  EXCEPTION [{filename}]: {e}")
            traceback.print_exc()

        row.update(new_vals)
        enriched_rows.append(row)

    bpm_debug_file.close()
    key_debug_file.close()

    elapsed_total = time.time() - start_time
    log(f"\nProcessing complete: {success_count} success, {fail_count} fail, {skip_count} skip")
    log(f"Runtime: {elapsed_total:.0f}s ({elapsed_total/60:.1f}m)")

    # ── Write enriched CSV ──
    output_csv = PROOF_DIR / "03_analysis_with_evidence.csv"
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(enriched_rows)

    log(f"Wrote {len(enriched_rows)} rows to {output_csv}")

    # ── Sample rows ──
    sample_csv = PROOF_DIR / "01_sample_rows.csv"
    with open(sample_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_columns, extrasaction="ignore")
        writer.writeheader()
        # Pick first 10 successful rows
        written = 0
        for r in enriched_rows:
            if r.get("SelectedBPM", "") and r.get("SelectedKey", ""):
                writer.writerow(r)
                written += 1
                if written >= 10:
                    break

    log(f"Wrote {written} sample rows to {sample_csv}")

    # ── Run summary ──
    summary_lines = [
        "=== ANALYZER UPGRADE RUN SUMMARY ===",
        f"timestamp={timestamp}",
        f"source_csv={SOURCE_CSV}",
        f"output_csv={output_csv}",
        f"total_rows={len(rows)}",
        f"success={success_count}",
        f"fail={fail_count}",
        f"skip={skip_count}",
        f"new_columns_added={len(NEW_COLUMNS)}",
        f"runtime_seconds={elapsed_total:.0f}",
        f"runtime_human={elapsed_total/60:.1f}m",
        "",
        "--- Output Files ---",
        f"00_run_summary.txt (this file)",
        f"01_sample_rows.csv ({written} rows)",
        f"02_bpm_candidates_debug.csv",
        f"03_key_candidates_debug.csv",
        f"03_analysis_with_evidence.csv ({len(enriched_rows)} rows, {len(output_columns)} columns)",
        f"execution_log.txt",
        "",
    ]

    # Row count verification
    rows_out = len(enriched_rows)
    gate = "PASS" if rows_out == len(rows) and success_count > 0 else "FAIL"
    summary_lines.append(f"ROWS_IN={len(rows)}")
    summary_lines.append(f"ROWS_OUT={rows_out}")
    summary_lines.append(f"ROW_MATCH={'TRUE' if rows_out == len(rows) else 'FALSE'}")
    summary_lines.append(f"GATE={gate}")

    (PROOF_DIR / "00_run_summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")
    log(f"\nGATE={gate}")

    log_file.close()

    # Print contract
    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"CSV={output_csv}")
    print(f"ROWS_IN={len(rows)}")
    print(f"ROWS_OUT={rows_out}")
    print(f"SUCCESS={success_count}")
    print(f"FAIL={fail_count}")
    print(f"SKIP={skip_count}")
    print(f"ZIP=<pending>")
    print(f"GATE={gate}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
