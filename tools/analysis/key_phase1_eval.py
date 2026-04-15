"""
NGKsPlayerNative — KEY Phase 1: Key Detection Calibration
Comprehensive evaluation implementing Steps 1-13.
Re-extracts features, evaluates baseline, tunes selection, produces proof.
"""

import csv
import os
import sys
import time
import traceback
import zipfile
from datetime import datetime
from pathlib import Path
from collections import Counter

import numpy as np

WORKSPACE = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
EVIDENCE_CSV = WORKSPACE / "_proof" / "analyzer_upgrade" / "03_analysis_with_evidence.csv"
MUSIC_DIR = Path(r"C:\Users\suppo\Music")
PROOF_DIR = WORKSPACE / "_proof" / "key_phase1"

sys.path.insert(0, str(WORKSPACE / "tools"))

from feature_extractor import extract_features
from bpm_key_resolver import (
    resolve_key, CAMELOT_MAP, MAJOR_PROFILE, MINOR_PROFILE, PITCH_CLASSES,
    _to_camelot, _are_relative,
)

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


# ══════════════════════════════════════════════════════════════════════
#  CAMELOT UTILITIES
# ══════════════════════════════════════════════════════════════════════

# Reverse map: Camelot → key name (prefer sharps to match PITCH_CLASSES)
CAMELOT_REVERSE = {}
for k, v in CAMELOT_MAP.items():
    if v not in CAMELOT_REVERSE:
        CAMELOT_REVERSE[v] = k

# Tunebat Key string → Camelot notation
TUNEBAT_KEY_MAP = {}
for root in PITCH_CLASSES:
    for mode in ["major", "minor"]:
        key_name = f"{root} {mode}"
        cam = CAMELOT_MAP.get(key_name, "")
        if cam:
            TUNEBAT_KEY_MAP[key_name.lower()] = cam
# Add flat-based equivalents that Tunebat might use
_flat_map = {
    "db": "C#", "eb": "D#", "fb": "E", "gb": "F#",
    "ab": "G#", "bb": "A#", "cb": "B",
}
for flat, sharp in _flat_map.items():
    for mode in ["major", "minor"]:
        flat_key = f"{flat} {mode}"
        sharp_key = f"{sharp} {mode}"
        cam = CAMELOT_MAP.get(sharp_key, "")
        if not cam:
            # Try with proper capitalization
            sharp_key2 = f"{sharp} {mode}"
            cam = CAMELOT_MAP.get(sharp_key2, "")
        if cam:
            TUNEBAT_KEY_MAP[flat_key.lower()] = cam


def parse_tunebat_key(tunebat_str: str) -> list:
    """
    Parse Tunebat Key string to list of Camelot codes.
    Handles: "D Major", "B Minor", "E Major or B minor", "Ab major",
             "B Mminor" (typo).
    Returns list of Camelot codes (usually 1, sometimes 2 for dual keys).
    """
    if not tunebat_str or not tunebat_str.strip():
        return []

    s = tunebat_str.strip()
    # Fix common typos
    s = s.replace("Mminor", "minor").replace("mMinor", "minor")
    s = s.replace("Mmajor", "major").replace("mMajor", "major")

    results = []

    # Handle "X or Y" dual keys
    if " or " in s.lower():
        parts = s.lower().split(" or ")
    else:
        parts = [s.lower()]

    for part in parts:
        part = part.strip()
        # Try direct lookup
        cam = TUNEBAT_KEY_MAP.get(part, "")
        if cam:
            results.append(cam)
            continue

        # Try with normalized capitalization: "f# major" → "F# major"
        tokens = part.split()
        if len(tokens) == 2:
            root = tokens[0].capitalize()
            if len(root) > 1 and root[1] in ('b', '#'):
                root = root[0].upper() + root[1]
            mode = tokens[1].lower()
            key_name = f"{root} {mode}"
            cam = CAMELOT_MAP.get(key_name, "")
            if cam:
                results.append(cam)
                continue

            # Handle flat roots → sharp equivalents
            if root.endswith('b') and len(root) == 2:
                flat_letter = root.lower()
                sharp_equiv = _flat_map.get(flat_letter, "")
                if sharp_equiv:
                    key_name2 = f"{sharp_equiv} {mode}"
                    cam = CAMELOT_MAP.get(key_name2, "")
                    if cam:
                        results.append(cam)
                        continue

    return results


def classify_key_relation(detected_cam: str, ground_truth_cams: list) -> str:
    """
    Classify relationship between detected Camelot and ground truth.
    Returns: EXACT, NEIGHBOR, RELATIVE, WRONG
    """
    if not detected_cam or not ground_truth_cams:
        return "WRONG"

    for gt in ground_truth_cams:
        if detected_cam == gt:
            return "EXACT"

    for gt in ground_truth_cams:
        # RELATIVE: same number, different letter (A↔B)
        d_num, d_let = detected_cam[:-1], detected_cam[-1]
        g_num, g_let = gt[:-1], gt[-1]
        if d_num == g_num and d_let != g_let:
            return "RELATIVE"

    for gt in ground_truth_cams:
        # NEIGHBOR: ±1 on Camelot wheel, same letter
        d_num, d_let = int(detected_cam[:-1]), detected_cam[-1]
        g_num, g_let = int(gt[:-1]), gt[-1]
        if d_let == g_let:
            diff = abs(d_num - g_num)
            if diff == 1 or diff == 11:  # wraps 12→1
                return "NEIGHBOR"

    return "WRONG"


def camelot_to_key_name(cam: str) -> str:
    """Convert Camelot to readable key name."""
    return CAMELOT_REVERSE.get(cam, cam)


# ══════════════════════════════════════════════════════════════════════
#  TUNED KEY RESOLVER (Step 7)
# ══════════════════════════════════════════════════════════════════════

def resolve_key_tuned(features):
    """
    Tuned key resolver — extends base resolve_key logic with evidence-based
    adjustments. No hardcoded key swaps, no per-song overrides.

    Tuning changes:
    1. BASS NOTE ALIGNMENT: When top-2 candidates are within margin < 0.05,
       boost candidate whose root aligns with bass chroma peak.
       Rationale: Bass instruments reinforce the tonic of the perceived key.
       Evidence: Nelly D minor(0.584) vs D major(0.541) — bass profile
       can disambiguate whether tonic is major or minor.

    2. HARMONIC STABILITY PENALTY: When harmonic_stability is low (< 0.7),
       lower confidence and flag as ambiguous rather than forcing a pick.

    3. SECTION VARIANCE PENALTY: When section_chroma_variance is high (> 0.05),
       reduce confidence — the key may shift across the track.

    4. RELATIVE PAIR BOOST: When top-2 are a relative major/minor pair
       (same Camelot number, different letter) and margin < 0.08, use bass
       alignment to pick the more likely root.
    """
    chroma = features.chroma
    if np.sum(chroma) == 0:
        return {
            'all_scores': [],
            'candidate1': '', 'candidate2': '', 'candidate3': '',
            'score1': 0.0, 'score2': 0.0, 'score3': 0.0,
            'tonal_clarity': features.tonal_clarity,
            'key_change_detected': False,
            'selected_key': '',
            'selected_cam': '',
            'selected_confidence': 0.0,
            'selection_reason': 'NO_CHROMA',
            'decision_source': 'BASE_SCORER',
            'bass_chroma': features.bass_chroma,
            'harmonic_stability': features.harmonic_stability,
            'section_chroma_variance': features.section_chroma_variance,
        }

    # ── Score all 24 keys ──
    key_scores = []
    for root_idx in range(12):
        major_rotated = np.roll(MAJOR_PROFILE, root_idx)
        minor_rotated = np.roll(MINOR_PROFILE, root_idx)

        major_corr = float(np.corrcoef(chroma, major_rotated)[0, 1])
        minor_corr = float(np.corrcoef(chroma, minor_rotated)[0, 1])

        root_name = PITCH_CLASSES[root_idx]
        key_scores.append((f"{root_name} major", major_corr))
        key_scores.append((f"{root_name} minor", minor_corr))

    key_scores.sort(key=lambda x: -x[1])

    # ── Modulation detection ──
    key_change = False
    if len(features.chroma_segments) >= 2:
        first_half = features.chroma_segments[0]
        last_half = features.chroma_segments[-1]
        if np.sum(first_half) > 0 and np.sum(last_half) > 0:
            seg_corr = float(np.corrcoef(first_half, last_half)[0, 1])
            key_change = seg_corr < 0.85

    # ── Base selection (same as resolve_key) ──
    top1_name, top1_score = key_scores[0]
    top2_name, top2_score = key_scores[1]
    top3_name, top3_score = key_scores[2] if len(key_scores) >= 3 else ("", 0.0)

    margin = top1_score - top2_score
    decision_source = "BASE_SCORER"
    reason_parts = [f"key={top1_name} score={top1_score:.4f} margin={margin:.4f}"]

    selected_name = top1_name
    selected_score = top1_score

    # ── TUNING: Bass note alignment for close calls ──
    bass_chroma = features.bass_chroma
    bass_applied = False

    if margin < 0.08 and np.sum(bass_chroma) > 0:
        # Extract root note index for both candidates
        top1_root = PITCH_CLASSES.index(top1_name.split()[0]) if top1_name.split()[0] in PITCH_CLASSES else -1
        top2_root = PITCH_CLASSES.index(top2_name.split()[0]) if top2_name.split()[0] in PITCH_CLASSES else -1

        if top1_root >= 0 and top2_root >= 0:
            bass1 = bass_chroma[top1_root]
            bass2 = bass_chroma[top2_root]

            # Check if top-2 are a relative pair (same root, major vs minor)
            top1_cam = CAMELOT_MAP.get(top1_name, "")
            top2_cam = CAMELOT_MAP.get(top2_name, "")
            is_relative = False
            if top1_cam and top2_cam:
                is_relative = (top1_cam[:-1] == top2_cam[:-1] and top1_cam[-1] != top2_cam[-1])

            # Check if same root note but different mode
            same_root = (top1_root == top2_root)

            if same_root:
                # Same root, different mode (e.g. D minor vs D major).
                # Use 3rd-degree energy in bass chroma to disambiguate:
                #   major 3rd = root + 4 semitones
                #   minor 3rd = root + 3 semitones
                major_3rd_idx = (top1_root + 4) % 12
                minor_3rd_idx = (top1_root + 3) % 12
                bass_major_3rd = bass_chroma[major_3rd_idx]
                bass_minor_3rd = bass_chroma[minor_3rd_idx]

                top1_is_major = "major" in top1_name
                top2_is_major = "major" in top2_name

                if top1_is_major != top2_is_major:
                    # One major, one minor — can disambiguate via 3rd degree
                    third_diff = bass_major_3rd - bass_minor_3rd
                    reason_parts.append(
                        f"SAME_ROOT_3RD: bass_maj3={bass_major_3rd:.3f} "
                        f"bass_min3={bass_minor_3rd:.3f} Δ3={third_diff:.3f}"
                    )
                    # Swap only if bass strongly favors the mode of top2
                    if top2_is_major and third_diff > 0.10:
                        selected_name = top2_name
                        selected_score = top2_score
                        decision_source = "TUNED_MODE_DISAMBIG"
                        bass_applied = True
                        reason_parts.append(
                            f"MODE_SWAP_TO_MAJOR: bass major_3rd dominates"
                        )
                    elif not top2_is_major and third_diff < -0.10:
                        selected_name = top2_name
                        selected_score = top2_score
                        decision_source = "TUNED_MODE_DISAMBIG"
                        bass_applied = True
                        reason_parts.append(
                            f"MODE_SWAP_TO_MINOR: bass minor_3rd dominates"
                        )
                else:
                    reason_parts.append(f"SAME_ROOT_SAME_MODE({top1_name}/{top2_name})")
            elif is_relative:
                # Camelot relative pair (same number, A↔B): correlation
                # difference IS the signal. Don't override with bass.
                reason_parts.append(f"RELATIVE_PAIR({top1_name}/{top2_name})")
            elif top1_score > 0.75:
                # HIGH CONFIDENCE GUARD: When the base score is very strong,
                # correlation-based ranking is reliable — don't override.
                reason_parts.append(
                    f"HIGH_CONF_GUARD: score={top1_score:.4f}>0.75, skip bass"
                )
            else:
                # Different root notes with close scores: bass can disambiguate
                bass_diff = bass2 - bass1
                if bass_diff > 0.15:  # top2 root has substantially stronger bass
                    selected_name = top2_name
                    selected_score = top2_score
                    decision_source = "TUNED_BASS_ALIGNMENT"
                    bass_applied = True
                    reason_parts.append(
                        f"BASS_SWAP: bass[{top2_name.split()[0]}]={bass2:.3f} > "
                        f"bass[{top1_name.split()[0]}]={bass1:.3f} Δ={bass_diff:.3f}"
                    )
                elif bass_diff < -0.15:  # top1 confirmed by bass
                    reason_parts.append(
                        f"BASS_CONFIRM: bass[{top1_name.split()[0]}]={bass1:.3f} > "
                        f"bass[{top2_name.split()[0]}]={bass2:.3f}"
                    )

    # ── TUNING: Harmonic stability confidence adjustment ──
    h_stab = features.harmonic_stability
    if h_stab < 0.7:
        selected_score *= 0.90
        reason_parts.append(f"STABILITY_PENALTY: h_stab={h_stab:.3f}")

    # ── TUNING: Section variance confidence adjustment ──
    s_var = features.section_chroma_variance
    if s_var > 0.05:
        selected_score *= 0.90
        reason_parts.append(f"VARIANCE_PENALTY: s_var={s_var:.4f}")

    if key_change:
        reason_parts.append("MODULATION_DETECTED")

    # ── Confidence classification ──
    if margin < 0.02 and not bass_applied:
        decision_source = "AMBIGUOUS_KEEP_BASE"
        reason_parts.append("VERY_LOW_MARGIN")
    elif features.tonal_clarity < 0.003:
        decision_source = "MANUAL_REVIEW_RECOMMENDED"
        reason_parts.append(f"VERY_LOW_CLARITY={features.tonal_clarity:.4f}")

    selected_cam = CAMELOT_MAP.get(selected_name, selected_name)

    return {
        'all_scores': key_scores,
        'candidate1': top1_name,
        'candidate2': top2_name,
        'candidate3': top3_name,
        'score1': round(top1_score, 4),
        'score2': round(top2_score, 4),
        'score3': round(top3_score, 4),
        'tonal_clarity': features.tonal_clarity,
        'key_change_detected': key_change,
        'selected_key': selected_name,
        'selected_cam': selected_cam,
        'selected_confidence': round(selected_score, 4),
        'selection_reason': '; '.join(reason_parts),
        'decision_source': decision_source,
        'bass_chroma': features.bass_chroma,
        'harmonic_stability': features.harmonic_stability,
        'section_chroma_variance': features.section_chroma_variance,
    }


# ══════════════════════════════════════════════════════════════════════
#  MAIN EVALUATION PIPELINE
# ══════════════════════════════════════════════════════════════════════

def main():
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("KEY PHASE 1 — KEY DETECTION CALIBRATION")
    log(f"Workspace: {WORKSPACE}")
    log(f"Date: {datetime.now().isoformat()}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 1: LOAD + NORMALIZE
    # ─────────────────────────────────────────────────────────────────

    if not EVIDENCE_CSV.is_file():
        log(f"FAIL-CLOSED: Evidence CSV not found: {EVIDENCE_CSV}")
        sys.exit(1)

    with open(EVIDENCE_CSV, "r", encoding="utf-8") as f:
        all_rows = list(csv.DictReader(f))

    # Find rows with Tunebat Key
    calibration = []
    parse_ok = 0
    parse_fail = 0

    for row in all_rows:
        tk = row.get("Tunebat Key", "").strip()
        if not tk:
            continue
        cams = parse_tunebat_key(tk)
        if cams:
            row["_tunebat_cams"] = cams
            calibration.append(row)
            parse_ok += 1
        else:
            parse_fail += 1
            log(f"  PARSE FAIL: '{tk}' for {row.get('Artist','?')} — {row.get('Title','?')}")

    log(f"Total evidence rows: {len(all_rows)}")
    log(f"Rows with Tunebat Key: {len(calibration) + parse_fail}")
    log(f"  Parse success: {parse_ok}")
    log(f"  Parse fail: {parse_fail}")

    # Write 00_load_summary.txt
    with open(PROOF_DIR / "00_load_summary.txt", "w", encoding="utf-8") as f:
        f.write("KEY PHASE 1 — LOAD SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Total evidence CSV rows: {len(all_rows)}\n")
        f.write(f"Rows with Tunebat Key: {len(calibration) + parse_fail}\n")
        f.write(f"  Parse success: {parse_ok}\n")
        f.write(f"  Parse fail: {parse_fail}\n\n")
        f.write("Calibration rows:\n")
        for row in calibration:
            a = row.get("Artist", "?")
            t = row.get("Title", "?")
            tk = row.get("Tunebat Key", "")
            cams = row["_tunebat_cams"]
            f.write(f"  {a} — {t}: Tunebat='{tk}' → Camelot={cams}\n")
    log(f"Wrote {PROOF_DIR / '00_load_summary.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 2: BASELINE EVALUATION (using SelectedKey from evidence CSV)
    # ─────────────────────────────────────────────────────────────────

    log("\n=== STEP 2: BASELINE EVALUATION ===")

    baseline_results = []
    baseline_counts = Counter()

    for row in calibration:
        artist = row.get("Artist", "?")
        title = row.get("Title", "?")
        sel_key = row.get("SelectedKey", "").strip()
        gt_cams = row["_tunebat_cams"]
        tunebat_str = row.get("Tunebat Key", "")

        relation = classify_key_relation(sel_key, gt_cams)
        baseline_counts[relation] += 1

        # Check if runner-up matches ground truth
        runner_up = row.get("KeyRunnerUp", "").strip()
        runner_cam = ""
        if runner_up:
            # Convert runner-up to Camelot
            rup_parsed = parse_tunebat_key(runner_up)
            if rup_parsed:
                runner_cam = rup_parsed[0]
            else:
                runner_cam = CAMELOT_MAP.get(runner_up, "")
        runner_matches_gt = any(runner_cam == gt for gt in gt_cams) if runner_cam else False

        baseline_results.append({
            "Artist": artist,
            "Title": title,
            "Tunebat_Key": tunebat_str,
            "Tunebat_Camelot": "|".join(gt_cams),
            "SelectedKey": sel_key,
            "Relation": relation,
            "RunnerUp": runner_up,
            "RunnerUp_Camelot": runner_cam,
            "RunnerUp_Matches_GT": runner_matches_gt,
            "KeyCandidate1": row.get("KeyCandidate1", ""),
            "KeyCandidateScore1": row.get("KeyCandidateScore1", ""),
            "KeyCandidate2": row.get("KeyCandidate2", ""),
            "KeyCandidateScore2": row.get("KeyCandidateScore2", ""),
            "KeyCandidate3": row.get("KeyCandidate3", ""),
            "KeyCandidateScore3": row.get("KeyCandidateScore3", ""),
            "TonalClarity": row.get("TonalClarity", ""),
        })

    total = len(baseline_results)
    compatible = baseline_counts["EXACT"] + baseline_counts["NEIGHBOR"] + baseline_counts["RELATIVE"]

    log(f"\nBaseline results (n={total}):")
    log(f"  EXACT:    {baseline_counts['EXACT']}/{total} ({baseline_counts['EXACT']/total*100:.1f}%)")
    log(f"  NEIGHBOR: {baseline_counts['NEIGHBOR']}/{total} ({baseline_counts['NEIGHBOR']/total*100:.1f}%)")
    log(f"  RELATIVE: {baseline_counts['RELATIVE']}/{total} ({baseline_counts['RELATIVE']/total*100:.1f}%)")
    log(f"  WRONG:    {baseline_counts['WRONG']}/{total} ({baseline_counts['WRONG']/total*100:.1f}%)")
    log(f"  COMPATIBLE (E+N+R): {compatible}/{total} ({compatible/total*100:.1f}%)")

    log("\nWRONG rows:")
    for r in baseline_results:
        if r["Relation"] == "WRONG":
            rm = " ← RunnerUp=GT!" if r["RunnerUp_Matches_GT"] else ""
            log(f"  {r['Artist']} — {r['Title']}: Detected={r['SelectedKey']}, Tunebat={r['Tunebat_Camelot']}{rm}")

    log("\nRows where RunnerUp matches ground truth:")
    for r in baseline_results:
        if r["RunnerUp_Matches_GT"]:
            log(f"  {r['Artist']} — {r['Title']}: RunnerUp={r['RunnerUp']}({r['RunnerUp_Camelot']}) = Tunebat={r['Tunebat_Camelot']}")

    # Write baseline CSV
    with open(PROOF_DIR / "01_key_baseline_eval.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(baseline_results[0].keys()))
        writer.writeheader()
        writer.writerows(baseline_results)

    # Write baseline summary
    with open(PROOF_DIR / "01_key_baseline_summary.txt", "w", encoding="utf-8") as f:
        f.write("KEY PHASE 1 — BASELINE EVALUATION\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Total calibration rows: {total}\n\n")
        f.write(f"EXACT:    {baseline_counts['EXACT']}/{total} ({baseline_counts['EXACT']/total*100:.1f}%)\n")
        f.write(f"NEIGHBOR: {baseline_counts['NEIGHBOR']}/{total} ({baseline_counts['NEIGHBOR']/total*100:.1f}%)\n")
        f.write(f"RELATIVE: {baseline_counts['RELATIVE']}/{total} ({baseline_counts['RELATIVE']/total*100:.1f}%)\n")
        f.write(f"WRONG:    {baseline_counts['WRONG']}/{total} ({baseline_counts['WRONG']/total*100:.1f}%)\n")
        f.write(f"COMPATIBLE (E+N+R): {compatible}/{total} ({compatible/total*100:.1f}%)\n\n")
        f.write("WRONG rows:\n")
        for r in baseline_results:
            if r["Relation"] == "WRONG":
                rm = " ← RunnerUp=GT!" if r["RunnerUp_Matches_GT"] else ""
                f.write(f"  {r['Artist']} — {r['Title']}: Det={r['SelectedKey']}, Tunebat={r['Tunebat_Camelot']}{rm}\n")
                f.write(f"    C1={r['KeyCandidate1']}({r['KeyCandidateScore1']}) C2={r['KeyCandidate2']}({r['KeyCandidateScore2']}) C3={r['KeyCandidate3']}({r['KeyCandidateScore3']})\n")
        f.write("\nRows where RunnerUp matches ground truth:\n")
        for r in baseline_results:
            if r["RunnerUp_Matches_GT"]:
                f.write(f"  {r['Artist']} — {r['Title']}: RunUp={r['RunnerUp']}({r['RunnerUp_Camelot']})\n")

    log(f"Wrote {PROOF_DIR / '01_key_baseline_eval.csv'}")
    log(f"Wrote {PROOF_DIR / '01_key_baseline_summary.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEPS 3-9: RE-EXTRACT, RESOLVE (BASE+TUNED), EVALUATE
    # ─────────────────────────────────────────────────────────────────

    log("\n=== STEPS 3-9: RE-EXTRACT + TUNED KEY RESOLVER ===")
    log("Re-extracting features from audio with new evidence features...")

    tuned_results = []
    failure_analysis = []
    success = 0
    fail = 0

    for i, row in enumerate(calibration):
        artist = row.get("Artist", "?")
        title = row.get("Title", "?")
        filename = row.get("Filename", "")
        tunebat_str = row.get("Tunebat Key", "")
        gt_cams = row["_tunebat_cams"]

        log(f"\n  [{i+1}/{len(calibration)}] {artist} — {title}")

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

            # ── BASE RESOLVER (Step 5) ──
            base_result = resolve_key(features)
            base_cam = base_result.selected_key  # already Camelot from _to_camelot
            base_relation = classify_key_relation(base_cam, gt_cams)

            # ── TUNED RESOLVER (Steps 4+7) ──
            tuned = resolve_key_tuned(features)
            tuned_cam = tuned['selected_cam']
            tuned_relation = classify_key_relation(tuned_cam, gt_cams)

            # ── Check baseline from CSV ──
            csv_sel_key = row.get("SelectedKey", "").strip()
            csv_relation = classify_key_relation(csv_sel_key, gt_cams)

            # ── Detect changes ──
            changed = "YES" if tuned_cam != csv_sel_key else "NO"
            regressed = "NO"
            if csv_relation in ("EXACT", "NEIGHBOR", "RELATIVE") and tuned_relation == "WRONG":
                regressed = "YES"
            elif csv_relation == "EXACT" and tuned_relation in ("NEIGHBOR", "RELATIVE", "WRONG"):
                regressed = "YES"

            improvement = ""
            if changed == "YES":
                if tuned_relation in ("EXACT", "NEIGHBOR", "RELATIVE") and csv_relation == "WRONG":
                    improvement = "IMPROVED"
                elif tuned_relation == "EXACT" and csv_relation in ("NEIGHBOR", "RELATIVE"):
                    improvement = "IMPROVED"
                elif regressed == "YES":
                    improvement = "REGRESSION"
                else:
                    improvement = "CHANGED"

            # ── Check if correct key is in full 24-key scoring ──
            correct_in_top3 = False
            correct_rank = -1
            correct_score = 0.0
            all_scores = tuned['all_scores']  # list of (name, score)
            for rank, (kname, kscore) in enumerate(all_scores):
                kcam = CAMELOT_MAP.get(kname, "")
                if kcam in gt_cams:
                    if correct_rank < 0:
                        correct_rank = rank + 1  # 1-based
                        correct_score = kscore
                    if rank < 3:
                        correct_in_top3 = True
                    break

            log(f"    Base={base_cam}({base_relation}) → Tuned={tuned_cam}({tuned_relation}), GT={gt_cams}")
            log(f"    Decision={tuned['decision_source']}, TC={features.tonal_clarity:.4f}, HS={features.harmonic_stability:.3f}")
            log(f"    C1={tuned['candidate1']}({tuned['score1']}) C2={tuned['candidate2']}({tuned['score2']}) C3={tuned['candidate3']}({tuned['score3']})")
            if tuned_cam != csv_sel_key:
                log(f"    CSV_Key={csv_sel_key}({csv_relation}) → Tuned={tuned_cam}({tuned_relation}) [{improvement}]")
            if correct_rank > 0:
                log(f"    CorrectKey rank={correct_rank} score={correct_score:.4f}")
            else:
                log(f"    CorrectKey NOT IN any candidate (detection failure)")

            bass_peak_idx = int(np.argmax(features.bass_chroma)) if np.sum(features.bass_chroma) > 0 else -1
            bass_peak_note = PITCH_CLASSES[bass_peak_idx] if bass_peak_idx >= 0 else "?"

            tuned_results.append({
                "Artist": artist,
                "Title": title,
                "Tunebat_Key": tunebat_str,
                "Tunebat_Camelot": "|".join(gt_cams),
                "CSV_SelectedKey": csv_sel_key,
                "CSV_Relation": csv_relation,
                "Base_Key": base_cam,
                "Base_Relation": base_relation,
                "Tuned_Key": tuned_cam,
                "Tuned_KeyName": tuned['selected_key'],
                "Tuned_Relation": tuned_relation,
                "Tuned_Confidence": tuned['selected_confidence'],
                "Changed": changed,
                "Regressed": regressed,
                "Improvement": improvement,
                "DecisionSource": tuned['decision_source'],
                "SelectionReason": tuned['selection_reason'],
                "Candidate1": tuned['candidate1'],
                "Score1": tuned['score1'],
                "Candidate2": tuned['candidate2'],
                "Score2": tuned['score2'],
                "Candidate3": tuned['candidate3'],
                "Score3": tuned['score3'],
                "TonalClarity": round(features.tonal_clarity, 4),
                "HarmonicStability": round(features.harmonic_stability, 3),
                "SectionChromaVariance": round(features.section_chroma_variance, 6),
                "BassChromaPeak": bass_peak_note,
                "KeyChangeDetected": tuned['key_change_detected'],
                "CorrectKey_Rank": correct_rank,
                "CorrectKey_Score": round(correct_score, 4) if correct_score else 0.0,
                "CorrectKey_InTop3": correct_in_top3,
                "ExtractTime_s": round(dt, 2),
            })

            # ── Step 6: Failure analysis for WRONG rows ──
            if csv_relation == "WRONG":
                if correct_in_top3:
                    category = "A_SCORING_FAILURE"
                    explanation = f"Correct key at rank {correct_rank} (score={correct_score:.4f}) but not selected"
                elif correct_rank > 0:
                    category = "A_SCORING_FAILURE"
                    explanation = f"Correct key at rank {correct_rank} (score={correct_score:.4f}), outside top 3"
                else:
                    category = "B_DETECTION_FAILURE"
                    explanation = f"Correct key not found in any candidate"

                # Check if it's actually a relative/neighbor ambiguity
                if base_relation in ("NEIGHBOR", "RELATIVE"):
                    category = "C_AMBIGUOUS"
                    explanation = f"Base resolver gets {base_relation} — ambiguous"

                failure_analysis.append({
                    "Artist": artist,
                    "Title": title,
                    "Tunebat_Key": tunebat_str,
                    "Tunebat_Camelot": "|".join(gt_cams),
                    "CSV_SelectedKey": csv_sel_key,
                    "Tuned_Key": tuned_cam,
                    "Tuned_Relation": tuned_relation,
                    "Category": category,
                    "Explanation": explanation,
                    "CorrectKey_Rank": correct_rank,
                    "CorrectKey_Score": round(correct_score, 4) if correct_score else 0.0,
                    "Candidate1": tuned['candidate1'],
                    "Score1": tuned['score1'],
                    "Candidate2": tuned['candidate2'],
                    "Score2": tuned['score2'],
                    "Candidate3": tuned['candidate3'],
                    "Score3": tuned['score3'],
                    "TonalClarity": round(features.tonal_clarity, 4),
                    "Margin": round(tuned['score1'] - tuned['score2'], 4),
                })

            success += 1

        except Exception as e:
            log(f"    EXCEPTION: {e}")
            traceback.print_exc()
            fail += 1

    log(f"\nExtraction complete: {success} success, {fail} fail")

    if not tuned_results:
        log("FAIL: No results produced")
        sys.exit(1)

    # ─────────────────────────────────────────────────────────────────
    # Write Step 6 outputs: failure analysis
    # ─────────────────────────────────────────────────────────────────

    if failure_analysis:
        with open(PROOF_DIR / "02_key_failure_analysis.csv", "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(failure_analysis[0].keys()))
            writer.writeheader()
            writer.writerows(failure_analysis)

        cat_counts = Counter(r["Category"] for r in failure_analysis)
        with open(PROOF_DIR / "02_key_failure_summary.txt", "w", encoding="utf-8") as f:
            f.write("KEY PHASE 1 — FAILURE ANALYSIS (CSV WRONG rows)\n")
            f.write(f"Date: {datetime.now().isoformat()}\n\n")
            f.write(f"Total CSV-WRONG rows analyzed: {len(failure_analysis)}\n\n")
            for cat in sorted(cat_counts.keys()):
                f.write(f"  {cat}: {cat_counts[cat]}\n")
            f.write("\nDetails:\n")
            for r in failure_analysis:
                f.write(f"\n  {r['Artist']} — {r['Title']}\n")
                f.write(f"    Category: {r['Category']}\n")
                f.write(f"    {r['Explanation']}\n")
                f.write(f"    CSV={r['CSV_SelectedKey']} → Tuned={r['Tuned_Key']}({r['Tuned_Relation']})\n")
                f.write(f"    GT={r['Tunebat_Camelot']}, CorrectRank={r['CorrectKey_Rank']}, CorrectScore={r['CorrectKey_Score']}\n")
                f.write(f"    C1={r['Candidate1']}({r['Score1']}) C2={r['Candidate2']}({r['Score2']}) C3={r['Candidate3']}({r['Score3']})\n")
    else:
        with open(PROOF_DIR / "02_key_failure_analysis.csv", "w", encoding="utf-8") as f:
            f.write("No WRONG rows in baseline\n")
        with open(PROOF_DIR / "02_key_failure_summary.txt", "w", encoding="utf-8") as f:
            f.write("No WRONG rows in baseline\n")

    log(f"Wrote {PROOF_DIR / '02_key_failure_analysis.csv'}")
    log(f"Wrote {PROOF_DIR / '02_key_failure_summary.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 9: EVALUATE TUNED RESULTS
    # ─────────────────────────────────────────────────────────────────

    log("\n=== STEP 9: TUNED EVALUATION ===")

    tuned_counts = Counter()
    for r in tuned_results:
        tuned_counts[r["Tuned_Relation"]] += 1

    tuned_compatible = tuned_counts["EXACT"] + tuned_counts["NEIGHBOR"] + tuned_counts["RELATIVE"]

    log(f"\nTuned results (n={len(tuned_results)}):")
    log(f"  EXACT:    {tuned_counts['EXACT']}/{total} ({tuned_counts['EXACT']/total*100:.1f}%)")
    log(f"  NEIGHBOR: {tuned_counts['NEIGHBOR']}/{total} ({tuned_counts['NEIGHBOR']/total*100:.1f}%)")
    log(f"  RELATIVE: {tuned_counts['RELATIVE']}/{total} ({tuned_counts['RELATIVE']/total*100:.1f}%)")
    log(f"  WRONG:    {tuned_counts['WRONG']}/{total} ({tuned_counts['WRONG']/total*100:.1f}%)")
    log(f"  COMPATIBLE: {tuned_compatible}/{total} ({tuned_compatible/total*100:.1f}%)")

    log(f"\nComparison:")
    log(f"  Baseline EXACT:      {baseline_counts['EXACT']} → Tuned EXACT:      {tuned_counts['EXACT']}")
    log(f"  Baseline COMPATIBLE: {compatible} → Tuned COMPATIBLE: {tuned_compatible}")
    log(f"  Baseline WRONG:      {baseline_counts['WRONG']} → Tuned WRONG:      {tuned_counts['WRONG']}")

    # Improvements & regressions
    improvements = [r for r in tuned_results if r["Improvement"] == "IMPROVED"]
    regressions = [r for r in tuned_results if r["Improvement"] == "REGRESSION"]

    if improvements:
        log(f"\nImprovements ({len(improvements)}):")
        for r in improvements:
            log(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})")
    if regressions:
        log(f"\nREGRESSIONS ({len(regressions)}):")
        for r in regressions:
            log(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})")

    # Write tuned eval CSV
    with open(PROOF_DIR / "03_key_tuned_eval.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(tuned_results[0].keys()))
        writer.writeheader()
        writer.writerows(tuned_results)

    # Write tuned summary
    with open(PROOF_DIR / "03_key_tuned_summary.txt", "w", encoding="utf-8") as f:
        f.write("KEY PHASE 1 — TUNED EVALUATION\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Total: {total}\n\n")
        f.write("TUNED RESULTS:\n")
        f.write(f"  EXACT:      {tuned_counts['EXACT']}/{total} ({tuned_counts['EXACT']/total*100:.1f}%)\n")
        f.write(f"  NEIGHBOR:   {tuned_counts['NEIGHBOR']}/{total} ({tuned_counts['NEIGHBOR']/total*100:.1f}%)\n")
        f.write(f"  RELATIVE:   {tuned_counts['RELATIVE']}/{total} ({tuned_counts['RELATIVE']/total*100:.1f}%)\n")
        f.write(f"  WRONG:      {tuned_counts['WRONG']}/{total} ({tuned_counts['WRONG']/total*100:.1f}%)\n")
        f.write(f"  COMPATIBLE: {tuned_compatible}/{total} ({tuned_compatible/total*100:.1f}%)\n\n")
        f.write("COMPARISON WITH BASELINE:\n")
        f.write(f"  Baseline EXACT:      {baseline_counts['EXACT']} → Tuned: {tuned_counts['EXACT']}\n")
        f.write(f"  Baseline COMPATIBLE: {compatible} → Tuned: {tuned_compatible}\n")
        f.write(f"  Baseline WRONG:      {baseline_counts['WRONG']} → Tuned: {tuned_counts['WRONG']}\n\n")
        if improvements:
            f.write(f"Improvements ({len(improvements)}):\n")
            for r in improvements:
                f.write(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})\n")
        if regressions:
            f.write(f"\nREGRESSIONS ({len(regressions)}):\n")
            for r in regressions:
                f.write(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})\n")
        f.write("\nPer-track details:\n")
        for r in tuned_results:
            f.write(f"\n  {r['Artist']} — {r['Title']}:\n")
            f.write(f"    GT={r['Tunebat_Camelot']}, CSV={r['CSV_SelectedKey']}({r['CSV_Relation']}), Tuned={r['Tuned_Key']}({r['Tuned_Relation']})\n")
            f.write(f"    Conf={r['Tuned_Confidence']}, Decision={r['DecisionSource']}\n")
            f.write(f"    {r['SelectionReason']}\n")

    log(f"Wrote {PROOF_DIR / '03_key_tuned_eval.csv'}")
    log(f"Wrote {PROOF_DIR / '03_key_tuned_summary.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 10: REGRESSION GUARD
    # ─────────────────────────────────────────────────────────────────

    log("\n=== STEP 10: REGRESSION GUARD ===")

    protected_rows = [r for r in tuned_results if r["CSV_Relation"] in ("EXACT", "NEIGHBOR", "RELATIVE")]
    protected_regressions = [r for r in protected_rows if r["Regressed"] == "YES"]

    gate_pass = len(protected_regressions) == 0 and len(regressions) == 0

    with open(PROOF_DIR / "04_key_regression_guard.txt", "w", encoding="utf-8") as f:
        f.write("KEY PHASE 1 — REGRESSION GUARD\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Protected rows (CSV EXACT/NEIGHBOR/RELATIVE): {len(protected_rows)}\n\n")
        if not protected_regressions:
            f.write("No protected row regressions. ✓\n\n")
        else:
            f.write(f"PROTECTED ROW REGRESSIONS ({len(protected_regressions)}):\n")
            for r in protected_regressions:
                f.write(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})\n")

        f.write(f"Total regressions: {len(regressions)}\n")
        if not regressions:
            f.write("  None. ✓\n\n")
        else:
            for r in regressions:
                f.write(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})\n")

        f.write(f"\nTotal improvements: {len(improvements)}\n")
        for r in improvements:
            f.write(f"  {r['Artist']} — {r['Title']}: {r['CSV_SelectedKey']}({r['CSV_Relation']}) → {r['Tuned_Key']}({r['Tuned_Relation']})\n")

        f.write(f"\nGATE: {'PASS' if gate_pass else 'FAIL'}\n")

    log(f"Protected rows: {len(protected_rows)}, regressions: {len(protected_regressions)}")
    log(f"GATE: {'PASS' if gate_pass else 'FAIL'}")
    log(f"Wrote {PROOF_DIR / '04_key_regression_guard.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 11: MANUAL TARGETS
    # ─────────────────────────────────────────────────────────────────

    wrong_rows = [r for r in tuned_results if r["Tuned_Relation"] == "WRONG"]

    with open(PROOF_DIR / "05_key_manual_targets.csv", "w", encoding="utf-8", newline="") as f:
        fields = [
            "Artist", "Title", "Tuned_Key", "Tuned_KeyName", "Tunebat_Key", "Tunebat_Camelot",
            "Candidate1", "Score1", "Candidate2", "Score2", "Candidate3", "Score3",
            "TonalClarity", "HarmonicStability", "CorrectKey_Rank", "CorrectKey_Score",
            "Reason",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in wrong_rows:
            reason = "DETECTION_FAILURE" if r["CorrectKey_Rank"] < 0 else f"SCORING_FAILURE(rank={r['CorrectKey_Rank']})"
            writer.writerow({
                "Artist": r["Artist"],
                "Title": r["Title"],
                "Tuned_Key": r["Tuned_Key"],
                "Tuned_KeyName": r["Tuned_KeyName"],
                "Tunebat_Key": r["Tunebat_Key"],
                "Tunebat_Camelot": r["Tunebat_Camelot"],
                "Candidate1": r["Candidate1"],
                "Score1": r["Score1"],
                "Candidate2": r["Candidate2"],
                "Score2": r["Score2"],
                "Candidate3": r["Candidate3"],
                "Score3": r["Score3"],
                "TonalClarity": r["TonalClarity"],
                "HarmonicStability": r["HarmonicStability"],
                "CorrectKey_Rank": r["CorrectKey_Rank"],
                "CorrectKey_Score": r["CorrectKey_Score"],
                "Reason": reason,
            })

    log(f"Wrote {PROOF_DIR / '05_key_manual_targets.csv'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 12: FINAL REPORT
    # ─────────────────────────────────────────────────────────────────

    with open(PROOF_DIR / "06_key_final_report.txt", "w", encoding="utf-8") as f:
        f.write("KEY PHASE 1 — FINAL REPORT\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write("=" * 60 + "\n\n")

        f.write("FINAL KEY ACCURACY:\n")
        f.write(f"  EXACT:      {tuned_counts['EXACT']}/{total} ({tuned_counts['EXACT']/total*100:.1f}%)\n")
        f.write(f"  NEIGHBOR:   {tuned_counts['NEIGHBOR']}/{total} ({tuned_counts['NEIGHBOR']/total*100:.1f}%)\n")
        f.write(f"  RELATIVE:   {tuned_counts['RELATIVE']}/{total} ({tuned_counts['RELATIVE']/total*100:.1f}%)\n")
        f.write(f"  WRONG:      {tuned_counts['WRONG']}/{total} ({tuned_counts['WRONG']/total*100:.1f}%)\n")
        f.write(f"  COMPATIBLE: {tuned_compatible}/{total} ({tuned_compatible/total*100:.1f}%)\n\n")

        f.write("GAIN FROM THIS PHASE:\n")
        f.write(f"  Baseline EXACT:  {baseline_counts['EXACT']} → {tuned_counts['EXACT']}\n")
        f.write(f"  Baseline COMPAT: {compatible} → {tuned_compatible}\n")
        exact_gain = tuned_counts['EXACT'] - baseline_counts['EXACT']
        comp_gain = tuned_compatible - compatible
        f.write(f"  EXACT gain: {exact_gain:+d}\n")
        f.write(f"  COMPATIBLE gain: {comp_gain:+d}\n")
        f.write(f"  Regressions: {len(regressions)}\n\n")

        # Remaining WRONG analysis
        det_fail = sum(1 for r in wrong_rows if r["CorrectKey_Rank"] < 0)
        score_fail = sum(1 for r in wrong_rows if r["CorrectKey_Rank"] > 0)
        f.write(f"REMAINING WRONG: {len(wrong_rows)}\n")
        f.write(f"  Detection failures (correct key not in candidates): {det_fail}\n")
        f.write(f"  Scoring failures (correct key in candidates, not selected): {score_fail}\n\n")

        for r in wrong_rows:
            f.write(f"  {r['Artist']} — {r['Title']}:\n")
            f.write(f"    Tuned={r['Tuned_Key']}({r['Tuned_KeyName']}), GT={r['Tunebat_Camelot']}\n")
            reason = "detection_failure" if r["CorrectKey_Rank"] < 0 else f"scoring_failure(rank={r['CorrectKey_Rank']}, score={r['CorrectKey_Score']})"
            f.write(f"    Category: {reason}\n")
            f.write(f"    TC={r['TonalClarity']}, HS={r['HarmonicStability']}\n\n")

        # Recommendation
        if tuned_compatible >= total * 0.80:
            rec = "PRODUCTION-READY. ≥80% compatible accuracy is acceptable for DJ use."
        elif tuned_compatible >= total * 0.70:
            rec = "ACCEPTABLE with confidence filtering. Consider continuing tuning."
        else:
            rec = "CONTINUE tuning. Key detection needs more work."

        f.write(f"RECOMMENDATION: {rec}\n\n")

        if det_fail > 0:
            f.write("DETECTION FAILURES are irreducible with STFT chroma + Krumhansl profiles.\n")
            f.write("Options: CQT chroma, harmonic-percussive separation before chroma, ML-based key detection.\n\n")

        # Is the remaining error irreducible?
        if det_fail == len(wrong_rows):
            f.write("ALL remaining WRONG rows are DETECTION FAILURES.\n")
            f.write("The correct key never appears as a viable candidate.\n")
            f.write("This is an irreducible limitation of the current DSP pipeline.\n")
            f.write("RECOMMENDATION: STOP key tuning after this phase.\n")

    log(f"Wrote {PROOF_DIR / '06_key_final_report.txt'}")

    # ─────────────────────────────────────────────────────────────────
    # STEP 13: PROOF PACKAGE
    # ─────────────────────────────────────────────────────────────────

    # Write execution log
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    log(f"Wrote {PROOF_DIR / 'execution_log.txt'}")

    # Create ZIP
    zip_path = WORKSPACE / "_proof" / "key_phase1.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in sorted(PROOF_DIR.iterdir()):
            if file.is_file():
                zf.write(file, f"key_phase1/{file.name}")
    log(f"Wrote {zip_path}")

    # ── Output contract ──
    gate = "PASS" if gate_pass else "FAIL"
    print("\n" + "=" * 60)
    print(f"PF={PROOF_DIR}")
    print(f"ZIP={zip_path}")
    print(f"GATE={gate}")
    print("=" * 60)
    log(f"\nPF={PROOF_DIR}")
    log(f"ZIP={zip_path}")
    log(f"GATE={gate}")


if __name__ == "__main__":
    main()
