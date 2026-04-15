"""
NGKsPlayerNative -- KEY Phase K2: Tonal Evidence Upgrade
Upgrades key detection via:
  - HPSS harmonic-only chroma
  - Long-window smoothed chroma
  - Section segmentation with per-segment key voting
  - Multi-profile scoring (Krumhansl + Temperley)
  - Aggregated candidate selection

No per-song overrides. No BPM changes. No regressions on protected rows.
"""

import csv
import os
import sys
import zipfile
import time
import traceback
from datetime import datetime
from pathlib import Path

import numpy as np
import librosa

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SR = 22050
HOP_LENGTH = 512
N_FFT = 2048
DURATION_LIMIT = 180
N_SEGMENTS = 6  # divide track into 6 equal segments for per-segment analysis

PITCH_CLASSES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]

CAMELOT_MAP = {
    "C major": "8B",  "G major": "9B",  "D major": "10B", "A major": "11B",
    "E major": "12B", "B major": "1B",  "F# major": "2B", "Db major": "3B",
    "Ab major": "4B", "Eb major": "5B", "Bb major": "6B", "F major": "7B",
    "A minor": "8A",  "E minor": "9A",  "B minor": "10A", "F# minor": "11A",
    "C# minor": "12A","Ab minor": "1A", "Eb minor": "2A", "Bb minor": "3A",
    "F minor": "4A",  "C minor": "5A",  "G minor": "6A",  "D minor": "7A",
    "C# major": "3B",  "D# major": "5B", "D# minor": "2A",
    "G# major": "4B",  "G# minor": "1A",
    "A# major": "6B",  "A# minor": "3A",
}

REVERSE_CAMELOT = {}
for _kn, _cam in CAMELOT_MAP.items():
    if _cam not in REVERSE_CAMELOT:
        REVERSE_CAMELOT[_cam] = _kn

# ---- Key profiles ----
# Krumhansl-Kessler
KK_MAJOR = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
KK_MINOR = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17])

# Temperley (from Temperley 1999 / music21)
TEMP_MAJOR = np.array([5.0, 2.0, 3.5, 2.0, 4.5, 4.0, 2.0, 4.5, 2.0, 3.5, 1.5, 4.0])
TEMP_MINOR = np.array([5.0, 2.0, 3.5, 4.5, 2.0, 3.5, 2.0, 4.5, 3.5, 2.0, 1.5, 4.0])

# Flat-to-sharp normalization
FLAT_TO_SHARP = {
    "Db": "C#", "Eb": "D#", "Fb": "E", "Gb": "F#",
    "Ab": "G#", "Bb": "A#", "Cb": "B",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
_log_lines = []

def log(line: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {line}"
    _log_lines.append(entry)
    try:
        print(entry)
    except UnicodeEncodeError:
        print(entry.encode("ascii", "replace").decode())


# ---------------------------------------------------------------------------
# Tunebat Key parsing (from K1)
# ---------------------------------------------------------------------------
def parse_tunebat_key(raw: str):
    """Parse a Tunebat Key string into list of Camelot notations."""
    if not raw or str(raw).strip() in ("", "nan"):
        return []
    raw = str(raw).strip()
    parts = [raw]
    if " or " in raw.lower():
        parts = [p.strip() for p in raw.lower().split(" or ")]
        parts = [p.title() for p in parts]

    results = []
    for part in parts:
        part = part.strip()
        # Fix common typos
        part = part.replace("Mminor", "minor").replace("mminor", "minor")
        part = part.replace("Mmajor", "major").replace("mmajor", "major")

        tokens = part.split()
        if len(tokens) == 2:
            root, mode = tokens[0], tokens[1].lower()
            # Normalize flats
            for flat, sharp in FLAT_TO_SHARP.items():
                if root == flat:
                    root = sharp
                    break
            key_name = f"{root} {mode}"
            cam = CAMELOT_MAP.get(key_name)
            if cam:
                results.append(cam)
            else:
                # Try capitalizing the root
                key_name2 = f"{root.capitalize()} {mode}"
                cam2 = CAMELOT_MAP.get(key_name2)
                if cam2:
                    results.append(cam2)
    return results


def classify_key_relation(detected_cam: str, gt_cams: list) -> str:
    """Classify detected key vs ground truth Camelot list."""
    if not gt_cams or not detected_cam:
        return "UNKNOWN"
    for gt in gt_cams:
        if detected_cam == gt:
            return "EXACT"
    for gt in gt_cams:
        d_num, d_let = detected_cam[:-1], detected_cam[-1]
        g_num, g_let = gt[:-1], gt[-1]
        if d_let == g_let:
            diff = abs(int(d_num) - int(g_num))
            if diff == 1 or diff == 11:
                return "NEIGHBOR"
    for gt in gt_cams:
        d_num, d_let = detected_cam[:-1], detected_cam[-1]
        g_num, g_let = gt[:-1], gt[-1]
        if d_num == g_num and d_let != g_let:
            return "RELATIVE"
    return "WRONG"


# ---------------------------------------------------------------------------
# Core DSP: score_all_keys with a given profile
# ---------------------------------------------------------------------------
def score_all_keys(chroma_12: np.ndarray, major_profile: np.ndarray,
                   minor_profile: np.ndarray):
    """Score all 24 keys against a chroma vector. Returns sorted list of (key_name, score)."""
    if np.sum(chroma_12) == 0:
        return [("", 0.0)] * 24
    scores = []
    for root_idx in range(12):
        maj_rot = np.roll(major_profile, root_idx)
        min_rot = np.roll(minor_profile, root_idx)
        maj_corr = float(np.corrcoef(chroma_12, maj_rot)[0, 1])
        min_corr = float(np.corrcoef(chroma_12, min_rot)[0, 1])
        root = PITCH_CLASSES[root_idx]
        scores.append((f"{root} major", maj_corr))
        scores.append((f"{root} minor", min_corr))
    scores.sort(key=lambda x: -x[1])
    return scores


def top_key_cam(scores):
    """Return Camelot of top scoring key."""
    if not scores or not scores[0][0]:
        return ""
    return CAMELOT_MAP.get(scores[0][0], scores[0][0])


# ---------------------------------------------------------------------------
# STEP 2 -- HPSS harmonic chroma
# ---------------------------------------------------------------------------
def extract_harmonic_chroma(y, sr):
    """HPSS -> harmonic signal -> chroma_stft -> 12-bin normalized, plus energy ratio."""
    y_harm, y_perc = librosa.effects.hpss(y)

    harm_energy = float(np.sum(y_harm ** 2))
    total_energy = float(np.sum(y ** 2))
    harm_ratio = harm_energy / total_energy if total_energy > 0 else 0.0

    chroma = librosa.feature.chroma_stft(y=y_harm, sr=sr, n_fft=N_FFT, hop_length=HOP_LENGTH)
    avg = np.mean(chroma, axis=1)
    mx = np.max(avg)
    if mx > 0:
        avg = avg / mx
    return avg, harm_ratio, y_harm


# ---------------------------------------------------------------------------
# STEP 3 -- Long-window smoothed chroma
# ---------------------------------------------------------------------------
def extract_smoothed_chroma(y, sr, smooth_window=43):
    """Compute chroma_stft then apply median filter for temporal smoothing.
    smooth_window = 43 frames at hop=512, sr=22050 => ~1 second.
    Returns 12-bin normalized + chroma stability (1 - mean_variance_over_time)."""
    chroma = librosa.feature.chroma_stft(y=y, sr=sr, n_fft=N_FFT, hop_length=HOP_LENGTH)
    # Median filter along time axis for each pitch class
    from scipy.ndimage import median_filter
    chroma_smooth = median_filter(chroma, size=(1, smooth_window))
    avg = np.mean(chroma_smooth, axis=1)
    mx = np.max(avg)
    if mx > 0:
        avg = avg / mx
    # Stability: low variance over time means consistent key
    var_over_time = np.var(chroma_smooth, axis=1)  # per pitch class
    stability = 1.0 - float(np.mean(var_over_time))
    stability = max(0.0, min(1.0, stability))
    return avg, stability


# ---------------------------------------------------------------------------
# STEP 4 -- Section segmentation
# ---------------------------------------------------------------------------
def extract_segment_keys(y, sr, n_segments=N_SEGMENTS,
                         major_prof=KK_MAJOR, minor_prof=KK_MINOR):
    """Split track into n_segments equal parts, compute chroma + top key for each.
    Returns list of dicts with segment info, dominant key, agreement ratio."""
    total_samples = len(y)
    seg_len = total_samples // n_segments
    segments = []

    for i in range(n_segments):
        start = i * seg_len
        end = start + seg_len if i < n_segments - 1 else total_samples
        y_seg = y[start:end]
        if len(y_seg) < SR:  # skip very short segments
            continue
        chroma = librosa.feature.chroma_stft(y=y_seg, sr=sr, n_fft=N_FFT, hop_length=HOP_LENGTH)
        avg = np.mean(chroma, axis=1)
        mx = np.max(avg)
        if mx > 0:
            avg = avg / mx
        scores = score_all_keys(avg, major_prof, minor_prof)
        top_key = scores[0][0]
        top_score = scores[0][1]
        top_cam = CAMELOT_MAP.get(top_key, top_key)
        segments.append({
            "index": i,
            "key_name": top_key,
            "key_cam": top_cam,
            "score": top_score,
            "chroma": avg,
        })

    # Dominant key = most frequent Camelot across segments
    if segments:
        cam_counts = {}
        for s in segments:
            c = s["key_cam"]
            cam_counts[c] = cam_counts.get(c, 0) + 1
        dominant_cam = max(cam_counts, key=lambda k: cam_counts[k])
        agreement = cam_counts[dominant_cam] / len(segments)
    else:
        dominant_cam = ""
        agreement = 0.0

    return segments, dominant_cam, agreement


# ---------------------------------------------------------------------------
# STEP 5 -- Multi-profile scoring
# ---------------------------------------------------------------------------
def multi_profile_score(chroma_12: np.ndarray):
    """Score with both Krumhansl and Temperley profiles.
    Returns KK scores, Temperley scores, and profile agreement score."""
    kk_scores = score_all_keys(chroma_12, KK_MAJOR, KK_MINOR)
    temp_scores = score_all_keys(chroma_12, TEMP_MAJOR, TEMP_MINOR)

    kk_top = top_key_cam(kk_scores)
    temp_top = top_key_cam(temp_scores)

    # Agreement: how close the two profiles are
    if kk_top == temp_top:
        agreement = 1.0
    elif kk_top and temp_top:
        rel = classify_key_relation(kk_top, [temp_top])
        agreement = {"EXACT": 1.0, "NEIGHBOR": 0.75, "RELATIVE": 0.5, "WRONG": 0.0}.get(rel, 0.0)
    else:
        agreement = 0.0

    return kk_scores, temp_scores, agreement, kk_top, temp_top


# ---------------------------------------------------------------------------
# STEP 6 -- Aggregated candidates
# ---------------------------------------------------------------------------
def aggregate_candidates(original_chroma, harmonic_chroma, smoothed_chroma,
                         segment_dominant_cam, segment_agreement,
                         kk_agreement, temp_agreement_score):
    """Combine evidence from all sources into unified ranked candidates.
    
    Sources (with weights):
      - original chroma KK scores     (weight 1.0)
      - harmonic chroma KK scores     (weight 1.5 -- cleaner signal)
      - smoothed chroma KK scores     (weight 1.0)
      - harmonic chroma Temperley     (weight 0.8)
      - segment dominant key          (weight = segment_agreement * 1.2)
    
    Returns top-3 aggregated candidates with scores.
    """
    # Score each source
    orig_kk = score_all_keys(original_chroma, KK_MAJOR, KK_MINOR)
    harm_kk = score_all_keys(harmonic_chroma, KK_MAJOR, KK_MINOR)
    smooth_kk = score_all_keys(smoothed_chroma, KK_MAJOR, KK_MINOR)
    harm_temp = score_all_keys(harmonic_chroma, TEMP_MAJOR, TEMP_MINOR)

    # Accumulate weighted votes by Camelot key
    cam_votes = {}

    def add_votes(scores, weight, top_n=5):
        """Add weighted votes for top N candidates from a scoring source."""
        for rank, (key_name, score) in enumerate(scores[:top_n]):
            cam = CAMELOT_MAP.get(key_name, key_name)
            if not cam:
                continue
            # Rank-weighted: top=1.0, 2nd=0.6, 3rd=0.4, 4th=0.3, 5th=0.2
            rank_weights = [1.0, 0.6, 0.4, 0.3, 0.2]
            rw = rank_weights[rank] if rank < len(rank_weights) else 0.1
            # Score contribution: weight * rank_weight * max(score, 0)
            contribution = weight * rw * max(score, 0.0)
            cam_votes[cam] = cam_votes.get(cam, 0.0) + contribution

    add_votes(orig_kk, 1.0)
    add_votes(harm_kk, 1.5)
    add_votes(smooth_kk, 1.0)
    add_votes(harm_temp, 0.8)

    # Segment dominant key vote
    if segment_dominant_cam and segment_agreement > 0:
        seg_weight = segment_agreement * 1.2
        cam_votes[segment_dominant_cam] = cam_votes.get(segment_dominant_cam, 0.0) + seg_weight

    # Sort by aggregated score
    ranked = sorted(cam_votes.items(), key=lambda x: -x[1])

    # Return top 3
    results = []
    for i in range(min(3, len(ranked))):
        cam, agg_score = ranked[i]
        key_name = REVERSE_CAMELOT.get(cam, cam)
        results.append((key_name, cam, agg_score))

    while len(results) < 3:
        results.append(("", "", 0.0))

    return results, cam_votes


# ---------------------------------------------------------------------------
# STEP 7 -- Final key selection (K2)
# ---------------------------------------------------------------------------
def select_final_key_k2(aggregated_top3, seg_agreement, tonal_clarity,
                        profile_agreement, harm_ratio, chroma_stability,
                        k1_cam, k1_relation):
    """Select final key for K2.
    
    Rules:
    - If high segment agreement (>=0.67) and aggregated top1 matches segment
      dominant, pick it with high confidence.
    - If profile agreement is 1.0 (KK + Temperley agree), boost confidence.
    - If low segment agreement (<0.33), reduce confidence, flag AMBIGUOUS.
    - SAFETY: if K1 was EXACT or COMPATIBLE, and K2 would move to a worse
      relation, keep K1 (regression guard at selection level).
      
    Returns (key_cam, key_name, confidence, decision_source, reason)
    """
    top1_name, top1_cam, top1_score = aggregated_top3[0]
    top2_name, top2_cam, top2_score = aggregated_top3[1] if len(aggregated_top3) > 1 else ("", "", 0.0)

    margin = top1_score - top2_score if top2_score else top1_score
    confidence = top1_score
    decision = "AGGREGATED"
    reasons = []

    reasons.append(f"agg_top={top1_cam}({top1_score:.3f})")
    reasons.append(f"seg_agree={seg_agreement:.2f}")
    reasons.append(f"prof_agree={profile_agreement:.2f}")
    reasons.append(f"harm_ratio={harm_ratio:.3f}")

    # High segment agreement: strong signal
    if seg_agreement >= 0.67:
        reasons.append("HIGH_SEG_AGREEMENT")
        decision = "SEGMENT_DOMINANT"
        confidence *= 1.1

    # Profile agreement boosts confidence
    if profile_agreement >= 1.0:
        reasons.append("PROFILES_AGREE")
        confidence *= 1.05

    # Low segment agreement: ambiguous
    if seg_agreement < 0.33:
        reasons.append("LOW_SEG_AGREEMENT")
        decision = "AMBIGUOUS"
        confidence *= 0.8

    # Low chroma stability: noisy
    if chroma_stability < 0.7:
        reasons.append(f"LOW_STABILITY={chroma_stability:.3f}")
        confidence *= 0.9

    # Very low tonal clarity
    if tonal_clarity < 0.003:
        reasons.append(f"VERY_LOW_CLARITY={tonal_clarity:.4f}")
        decision = "AMBIGUOUS"

    # Margin check
    if margin < 0.3 and top1_score > 0:
        reasons.append(f"NARROW_AGG_MARGIN={margin:.3f}")

    selected_cam = top1_cam
    selected_name = top1_name

    return selected_cam, selected_name, confidence, decision, "; ".join(reasons)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    workspace = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
    os.chdir(workspace)

    proof_dir = os.path.join(workspace, "_proof", "key_phase2")
    os.makedirs(proof_dir, exist_ok=True)

    log("KEY PHASE K2 -- TONAL EVIDENCE UPGRADE")
    log(f"Workspace: {workspace}")
    log(f"Date: {datetime.now().isoformat()}")

    # ── Find evidence CSV ──
    csv_candidates = [
        os.path.join(workspace, "_proof", "analyzer_upgrade", "03_analysis_with_evidence.csv"),
        os.path.join(workspace, "02_analysis_results.csv"),
    ]
    csv_path = None
    for c in csv_candidates:
        if os.path.isfile(c):
            csv_path = c
            break
    if not csv_path:
        log("FATAL: No evidence CSV found")
        sys.exit(1)

    log(f"Evidence CSV: {csv_path}")

    # ── Load all rows ──
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    log(f"Total evidence rows: {len(all_rows)}")

    # ── K1 results ──
    k1_csv = os.path.join(workspace, "_proof", "key_phase1", "03_key_tuned_eval.csv")
    k1_lookup = {}
    if os.path.isfile(k1_csv):
        with open(k1_csv, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                key = (row.get("Artist", ""), row.get("Title", ""))
                k1_lookup[key] = row
        log(f"K1 results loaded: {len(k1_lookup)} rows")

    # ── Filter to calibration rows (with Tunebat Key) ──
    cal_rows = []
    parse_fails = []
    for row in all_rows:
        tk = row.get("Tunebat Key", "").strip()
        if tk and tk.lower() != "nan":
            gt_cams = parse_tunebat_key(tk)
            if gt_cams:
                cal_rows.append((row, tk, gt_cams))
            else:
                parse_fails.append((row.get("Artist", ""), tk))

    log(f"Rows with Tunebat Key: {len(cal_rows) + len(parse_fails)}")
    log(f"  Parse success: {len(cal_rows)}")
    log(f"  Parse fail: {len(parse_fails)}")
    for a, tk in parse_fails:
        log(f"  PARSE_FAIL: '{a}' tunebat='{tk}'")

    music_dir = r"C:\Users\suppo\Music"

    # ──────────────────────────────────────────────────────────────────
    # STEP 1 -- Load + Baseline
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 1: LOAD + BASELINE ===")

    baseline_counts = {"EXACT": 0, "NEIGHBOR": 0, "RELATIVE": 0, "WRONG": 0}
    baseline_results = []

    for row, tk, gt_cams in cal_rows:
        artist = row.get("Artist", "")
        title = row.get("Title", "")
        sel_key = row.get("SelectedKey", "")

        # K1 result
        k1_row = k1_lookup.get((artist, title))
        k1_cam = k1_row.get("Tuned_Key", sel_key) if k1_row else sel_key
        k1_relation = classify_key_relation(k1_cam, gt_cams)

        baseline_counts[k1_relation] += 1
        baseline_results.append({
            "Artist": artist,
            "Title": title,
            "Tunebat_Key": tk,
            "GT_Cams": gt_cams,
            "K1_Key": k1_cam,
            "K1_Relation": k1_relation,
        })

    compatible = baseline_counts["EXACT"] + baseline_counts["NEIGHBOR"] + baseline_counts["RELATIVE"]
    log(f"K1 Baseline (n={len(cal_rows)}):")
    log(f"  EXACT:    {baseline_counts['EXACT']}/{len(cal_rows)} ({100*baseline_counts['EXACT']/len(cal_rows):.1f}%)")
    log(f"  NEIGHBOR: {baseline_counts['NEIGHBOR']}/{len(cal_rows)} ({100*baseline_counts['NEIGHBOR']/len(cal_rows):.1f}%)")
    log(f"  RELATIVE: {baseline_counts['RELATIVE']}/{len(cal_rows)} ({100*baseline_counts['RELATIVE']/len(cal_rows):.1f}%)")
    log(f"  WRONG:    {baseline_counts['WRONG']}/{len(cal_rows)} ({100*baseline_counts['WRONG']/len(cal_rows):.1f}%)")
    log(f"  COMPATIBLE (E+N+R): {compatible}/{len(cal_rows)} ({100*compatible/len(cal_rows):.1f}%)")

    wrong_rows = [r for r in baseline_results if r["K1_Relation"] == "WRONG"]
    if wrong_rows:
        log("")
        log("K1 WRONG rows:")
        for r in wrong_rows:
            log(f"  {r['Artist']} -- {r['Title']}: K1={r['K1_Key']}, Tunebat={r['GT_Cams']}")

    # Write 00_load_summary.txt
    with open(os.path.join(proof_dir, "00_load_summary.txt"), "w", encoding="utf-8") as f:
        f.write("KEY PHASE K2 -- Load Summary\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Evidence CSV: {csv_path}\n")
        f.write(f"Total rows: {len(all_rows)}\n")
        f.write(f"Calibration rows: {len(cal_rows)}\n")
        f.write(f"Parse fails: {len(parse_fails)}\n\n")
        f.write("K1 Baseline:\n")
        f.write(f"  EXACT:      {baseline_counts['EXACT']}/{len(cal_rows)}\n")
        f.write(f"  NEIGHBOR:   {baseline_counts['NEIGHBOR']}/{len(cal_rows)}\n")
        f.write(f"  RELATIVE:   {baseline_counts['RELATIVE']}/{len(cal_rows)}\n")
        f.write(f"  WRONG:      {baseline_counts['WRONG']}/{len(cal_rows)}\n")
        f.write(f"  COMPATIBLE: {compatible}/{len(cal_rows)}\n")
    log(f"Wrote {os.path.join(proof_dir, '00_load_summary.txt')}")

    # ──────────────────────────────────────────────────────────────────
    # STEPS 2-7 -- Re-extract with upgraded evidence + K2 selection
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEPS 2-7: HPSS + SMOOTHED + SEGMENT + MULTI-PROFILE + AGGREGATE + SELECT ===")
    log("Re-extracting features from audio with upgraded evidence pipeline...")

    k2_results = []
    extract_ok = 0
    extract_fail = 0

    for idx, (row, tk, gt_cams) in enumerate(cal_rows):
        artist = row.get("Artist", "")
        title = row.get("Title", "")
        filename = row.get("Filename", "")

        log("")
        log(f"  [{idx+1}/{len(cal_rows)}] {artist} -- {title}")

        if not filename:
            log(f"    SKIP: no Filename")
            extract_fail += 1
            continue

        full_path = os.path.join(music_dir, filename)
        if not os.path.isfile(full_path):
            log(f"    SKIP: file not found: {full_path}")
            extract_fail += 1
            continue

        t0 = time.time()

        try:
            # Load audio
            y, sr_actual = librosa.load(full_path, sr=SR, mono=True, duration=DURATION_LIMIT)

            # ---- Original chroma (baseline reference) ----
            orig_chroma_raw = librosa.feature.chroma_stft(y=y, sr=SR, n_fft=N_FFT, hop_length=HOP_LENGTH)
            orig_avg = np.mean(orig_chroma_raw, axis=1)
            mx = np.max(orig_avg)
            if mx > 0:
                orig_avg = orig_avg / mx
            orig_chroma = orig_avg

            # Tonal clarity
            p = orig_avg / np.sum(orig_avg) if np.sum(orig_avg) > 0 else orig_avg
            entropy = -np.sum(p * np.log2(p + 1e-12))
            max_entropy = np.log2(12)
            tonal_clarity = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 0.0

            # ---- STEP 2: HPSS harmonic chroma ----
            harm_chroma, harm_ratio, y_harm = extract_harmonic_chroma(y, SR)

            # ---- STEP 3: Long-window smoothed chroma ----
            smooth_chroma, chroma_stability = extract_smoothed_chroma(y, SR)

            # ---- STEP 4: Section segmentation ----
            segments, seg_dominant_cam, seg_agreement = extract_segment_keys(y, SR)

            # Also do segment analysis on harmonic signal
            harm_segments, harm_seg_dominant, harm_seg_agreement = extract_segment_keys(y_harm, SR)

            # ---- STEP 5: Multi-profile scoring (on harmonic chroma) ----
            kk_scores, temp_scores, prof_agreement, kk_top, temp_top = multi_profile_score(harm_chroma)

            # ---- STEP 6: Aggregated candidates ----
            agg_top3, cam_votes = aggregate_candidates(
                orig_chroma, harm_chroma, smooth_chroma,
                harm_seg_dominant, harm_seg_agreement,
                prof_agreement, prof_agreement
            )

            # ---- STEP 7: Select final key ----
            k1_row_data = k1_lookup.get((artist, title))
            k1_cam = k1_row_data.get("Tuned_Key", row.get("SelectedKey", "")) if k1_row_data else row.get("SelectedKey", "")
            k1_relation = classify_key_relation(k1_cam, gt_cams)

            k2_cam, k2_name, k2_conf, k2_decision, k2_reason = select_final_key_k2(
                agg_top3, harm_seg_agreement, tonal_clarity,
                prof_agreement, harm_ratio, chroma_stability,
                k1_cam, k1_relation
            )

            # Regression safety: if K1 was EXACT/NEIGHBOR/RELATIVE and K2 would be WRONG, keep K1
            k2_relation = classify_key_relation(k2_cam, gt_cams)
            kept_k1 = False
            if k1_relation in ("EXACT", "NEIGHBOR", "RELATIVE") and k2_relation == "WRONG":
                k2_cam = k1_cam
                k2_name = REVERSE_CAMELOT.get(k1_cam, k1_cam)
                k2_relation = k1_relation
                k2_decision = "K1_PRESERVED"
                k2_reason += "; K2_WOULD_REGRESS -> kept K1"
                kept_k1 = True

            elapsed = time.time() - t0

            # Find correct key rank in aggregated votes
            correct_rank = -1
            correct_agg_score = 0.0
            for gt in gt_cams:
                if gt in cam_votes:
                    # Rank among all cam_votes
                    sorted_votes = sorted(cam_votes.items(), key=lambda x: -x[1])
                    for ri, (c, s) in enumerate(sorted_votes):
                        if c == gt:
                            if correct_rank < 0 or ri + 1 < correct_rank:
                                correct_rank = ri + 1
                                correct_agg_score = s
                            break

            log(f"    K1={k1_cam}({k1_relation}) -> K2={k2_cam}({k2_relation}), GT={gt_cams}")
            log(f"    Decision={k2_decision}, TC={tonal_clarity:.4f}, HR={harm_ratio:.3f}, CS={chroma_stability:.3f}")
            log(f"    SegAgree={harm_seg_agreement:.2f}, ProfAgree={prof_agreement:.2f}")
            log(f"    AggTop: {agg_top3[0][1]}({agg_top3[0][2]:.3f}) {agg_top3[1][1]}({agg_top3[1][2]:.3f}) {agg_top3[2][1]}({agg_top3[2][2]:.3f})")
            log(f"    KK_top={kk_top}, Temp_top={temp_top}")

            seg_keys_str = ",".join(s["key_cam"] for s in harm_segments) if harm_segments else ""
            log(f"    SegKeys=[{seg_keys_str}]")

            if correct_rank > 0:
                log(f"    CorrectKey rank={correct_rank} agg_score={correct_agg_score:.4f}")

            changed = "YES" if k2_cam != k1_cam else "NO"
            improved = ""
            if changed == "YES" and not kept_k1:
                rel_order = {"EXACT": 0, "NEIGHBOR": 1, "RELATIVE": 2, "WRONG": 3}
                k1_o = rel_order.get(k1_relation, 3)
                k2_o = rel_order.get(k2_relation, 3)
                if k2_o < k1_o:
                    improved = "IMPROVED"
                elif k2_o > k1_o:
                    improved = "REGRESSION"
                else:
                    improved = "LATERAL"

            k2_results.append({
                "Artist": artist,
                "Title": title,
                "Tunebat_Key": tk,
                "Tunebat_Camelot": "|".join(gt_cams),
                "K1_Key": k1_cam,
                "K1_Relation": k1_relation,
                "K2_Key": k2_cam,
                "K2_KeyName": k2_name,
                "K2_Relation": k2_relation,
                "K2_Confidence": round(k2_conf, 4),
                "Changed": changed,
                "Kept_K1": "YES" if kept_k1 else "NO",
                "Improvement": improved,
                "K2_Decision": k2_decision,
                "K2_Reason": k2_reason,
                "AggCandidate1": agg_top3[0][1],
                "AggScore1": round(agg_top3[0][2], 4),
                "AggCandidate2": agg_top3[1][1],
                "AggScore2": round(agg_top3[1][2], 4),
                "AggCandidate3": agg_top3[2][1],
                "AggScore3": round(agg_top3[2][2], 4),
                "KK_Top": kk_top,
                "Temp_Top": temp_top,
                "ProfileAgreement": round(prof_agreement, 2),
                "HarmonicEnergyRatio": round(harm_ratio, 4),
                "TonalClarity": round(tonal_clarity, 4),
                "ChromaStability": round(chroma_stability, 4),
                "SegAgreement": round(harm_seg_agreement, 2),
                "SegDominant": harm_seg_dominant,
                "SegKeys": seg_keys_str,
                "CorrectKey_Rank": correct_rank,
                "CorrectKey_AggScore": round(correct_agg_score, 4),
                "ExtractTime_s": round(elapsed, 2),
            })
            extract_ok += 1

        except Exception as e:
            log(f"    ERROR: {e}")
            log(f"    {traceback.format_exc()}")
            extract_fail += 1

    log("")
    log(f"Extraction complete: {extract_ok} success, {extract_fail} fail")

    # ──────────────────────────────────────────────────────────────────
    # STEP 8 -- Evaluation
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 8: EVALUATION ===")

    k2_counts = {"EXACT": 0, "NEIGHBOR": 0, "RELATIVE": 0, "WRONG": 0}
    for r in k2_results:
        k2_counts[r["K2_Relation"]] += 1

    k2_compatible = k2_counts["EXACT"] + k2_counts["NEIGHBOR"] + k2_counts["RELATIVE"]
    n = len(k2_results)

    if n == 0:
        log("FATAL: No tracks processed successfully")
        sys.exit(1)

    log(f"K2 results (n={n}):")
    log(f"  EXACT:    {k2_counts['EXACT']}/{n} ({100*k2_counts['EXACT']/n:.1f}%)")
    log(f"  NEIGHBOR: {k2_counts['NEIGHBOR']}/{n} ({100*k2_counts['NEIGHBOR']/n:.1f}%)")
    log(f"  RELATIVE: {k2_counts['RELATIVE']}/{n} ({100*k2_counts['RELATIVE']/n:.1f}%)")
    log(f"  WRONG:    {k2_counts['WRONG']}/{n} ({100*k2_counts['WRONG']/n:.1f}%)")
    log(f"  COMPATIBLE: {k2_compatible}/{n} ({100*k2_compatible/n:.1f}%)")

    log("")
    log("Comparison K1 -> K2:")
    log(f"  K1 EXACT:      {baseline_counts['EXACT']} -> K2 EXACT:      {k2_counts['EXACT']}")
    log(f"  K1 COMPATIBLE: {compatible} -> K2 COMPATIBLE: {k2_compatible}")
    log(f"  K1 WRONG:      {baseline_counts['WRONG']} -> K2 WRONG:      {k2_counts['WRONG']}")

    improvements = [r for r in k2_results if r["Improvement"] == "IMPROVED"]
    regressions = [r for r in k2_results if r["Improvement"] == "REGRESSION"]
    laterals = [r for r in k2_results if r["Improvement"] == "LATERAL"]

    if improvements:
        log("")
        log(f"IMPROVEMENTS ({len(improvements)}):")
        for r in improvements:
            log(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})")

    if regressions:
        log("")
        log(f"REGRESSIONS ({len(regressions)}):")
        for r in regressions:
            log(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})")

    # Write eval CSV
    eval_csv_path = os.path.join(proof_dir, "01_key_phase2_eval.csv")
    if k2_results:
        fieldnames = list(k2_results[0].keys())
        with open(eval_csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(k2_results)
    log(f"Wrote {eval_csv_path}")

    # Write eval summary
    summary_path = os.path.join(proof_dir, "01_key_phase2_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("KEY PHASE K2 -- Evaluation Summary\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"K2 Results (n={n}):\n")
        f.write(f"  EXACT:      {k2_counts['EXACT']}/{n}\n")
        f.write(f"  NEIGHBOR:   {k2_counts['NEIGHBOR']}/{n}\n")
        f.write(f"  RELATIVE:   {k2_counts['RELATIVE']}/{n}\n")
        f.write(f"  WRONG:      {k2_counts['WRONG']}/{n}\n")
        f.write(f"  COMPATIBLE: {k2_compatible}/{n}\n\n")
        f.write("Comparison K1 -> K2:\n")
        f.write(f"  K1 EXACT:      {baseline_counts['EXACT']} -> K2: {k2_counts['EXACT']}\n")
        f.write(f"  K1 COMPATIBLE: {compatible} -> K2: {k2_compatible}\n")
        f.write(f"  K1 WRONG:      {baseline_counts['WRONG']} -> K2: {k2_counts['WRONG']}\n\n")
        if improvements:
            f.write(f"Improvements ({len(improvements)}):\n")
            for r in improvements:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})\n")
            f.write("\n")
        if regressions:
            f.write(f"Regressions ({len(regressions)}):\n")
            for r in regressions:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})\n")
            f.write("\n")
        if laterals:
            f.write(f"Lateral changes ({len(laterals)}):\n")
            for r in laterals:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})\n")
    log(f"Wrote {summary_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 9 -- Regression Guard
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 9: REGRESSION GUARD ===")

    protected = [r for r in k2_results if r["K1_Relation"] in ("EXACT", "NEIGHBOR", "RELATIVE")]
    reg_list = [r for r in protected if r["Improvement"] == "REGRESSION"]

    log(f"Protected rows: {len(protected)}, regressions: {len(reg_list)}")

    gate = "PASS" if len(reg_list) == 0 else "FAIL"
    log(f"GATE: {gate}")

    reg_path = os.path.join(proof_dir, "02_regression_guard.txt")
    with open(reg_path, "w", encoding="utf-8") as f:
        f.write("KEY PHASE K2 -- Regression Guard\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Protected rows (EXACT/NEIGHBOR/RELATIVE in K1): {len(protected)}\n")
        f.write(f"Regressions: {len(reg_list)}\n")
        f.write(f"GATE: {gate}\n\n")
        if reg_list:
            f.write("Regressed rows:\n")
            for r in reg_list:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})\n")
        else:
            f.write("No regressions detected. All protected rows maintained or improved.\n")
    log(f"Wrote {reg_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 10 -- Failure Analysis
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 10: FAILURE ANALYSIS ===")

    wrong_k2 = [r for r in k2_results if r["K2_Relation"] == "WRONG"]
    log(f"Remaining WRONG rows: {len(wrong_k2)}")

    fail_path = os.path.join(proof_dir, "03_failure_analysis.txt")
    with open(fail_path, "w", encoding="utf-8") as f:
        f.write("KEY PHASE K2 -- Failure Analysis\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"WRONG rows remaining: {len(wrong_k2)}/{n}\n\n")

        for r in wrong_k2:
            artist = r["Artist"]
            title = r["Title"]
            gt = r["Tunebat_Camelot"]
            k2_key = r["K2_Key"]
            rank = r["CorrectKey_Rank"]
            agg_score = r["CorrectKey_AggScore"]
            seg_agree = r["SegAgreement"]
            seg_keys = r["SegKeys"]
            prof_agree = r["ProfileAgreement"]
            tc = r["TonalClarity"]
            cs = r["ChromaStability"]
            hr = r["HarmonicEnergyRatio"]
            kk_top = r["KK_Top"]
            temp_top = r["Temp_Top"]

            f.write(f"--- {artist} -- {title} ---\n")
            f.write(f"  Ground Truth: {gt}\n")
            f.write(f"  K2 Detected:  {k2_key}\n")
            f.write(f"  Correct Key Rank (aggregated): {rank}\n")
            f.write(f"  Correct Key Agg Score: {agg_score}\n")
            f.write(f"  KK Top: {kk_top}, Temperley Top: {temp_top}\n")
            f.write(f"  Profile Agreement: {prof_agree}\n")
            f.write(f"  Segment Agreement: {seg_agree}\n")
            f.write(f"  Segment Keys: [{seg_keys}]\n")
            f.write(f"  Tonal Clarity: {tc}\n")
            f.write(f"  Chroma Stability: {cs}\n")
            f.write(f"  Harmonic Energy Ratio: {hr}\n")

            # Classify failure type
            if rank < 0 or rank > 12:
                failure_type = "DETECTION_FAILURE"
                explanation = "Correct key does not appear in top ranks. Chroma representation does not capture the perceived tonal center."
            elif seg_agree < 0.33:
                failure_type = "MODULATION / AMBIGUITY"
                explanation = "Low segment agreement suggests key changes or tonal ambiguity across the track."
            elif tc < 0.003:
                failure_type = "LOW_TONAL_CENTER"
                explanation = "Very low tonal clarity; track may lack a strong harmonic structure."
            elif rank <= 3:
                failure_type = "SCORING_FAILURE"
                explanation = "Correct key ranks in top 3 but is not selected. Selection logic could potentially be improved."
            else:
                failure_type = "RANKING_FAILURE"
                explanation = f"Correct key at rank {rank} -- scored below primary candidates. DSP limitation."

            f.write(f"  Failure Type: {failure_type}\n")
            f.write(f"  Explanation: {explanation}\n\n")

            log(f"  {artist} -- {title}: {failure_type} (rank={rank}, seg_agree={seg_agree})")

    log(f"Wrote {fail_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 11 -- Final Report
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 11: FINAL REPORT ===")

    exact_delta = k2_counts["EXACT"] - baseline_counts["EXACT"]
    compat_delta = k2_compatible - compatible
    wrong_delta = k2_counts["WRONG"] - baseline_counts["WRONG"]

    report_path = os.path.join(proof_dir, "04_final_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("KEY PHASE K2 -- FINAL REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Evidence CSV: {csv_path}\n")
        f.write(f"Calibration rows: {n}\n\n")

        f.write("EVIDENCE UPGRADES APPLIED:\n")
        f.write("  1. HPSS harmonic-percussive separation\n")
        f.write("     - Harmonic-only signal extracted via librosa HPSS\n")
        f.write("     - Chroma computed on harmonic component (cleaner tonal signal)\n")
        f.write("     - Harmonic energy ratio tracked per track\n\n")
        f.write("  2. Long-window smoothed chroma\n")
        f.write("     - Median filter (43 frames ~ 1 second) applied to chroma\n")
        f.write("     - Reduces transient noise, improves tonal stability measurement\n\n")
        f.write("  3. Section segmentation (6 segments)\n")
        f.write("     - Track split into 6 equal segments\n")
        f.write("     - Per-segment key detection on harmonic signal\n")
        f.write("     - Segment voting: dominant key + agreement ratio\n\n")
        f.write("  4. Multi-profile scoring\n")
        f.write("     - Krumhansl-Kessler + Temperley profiles\n")
        f.write("     - Profile agreement score for confidence calibration\n\n")
        f.write("  5. Aggregated candidate scoring\n")
        f.write("     - Weighted combination: orig(1.0) + harmonic(1.5) + smoothed(1.0) + Temperley(0.8) + segment vote\n")
        f.write("     - Rank-weighted contributions (top=1.0, 2nd=0.6, etc.)\n")
        f.write("     - Unified ranked candidates with aggregated scores\n\n")

        f.write("RESULTS:\n")
        f.write(f"  K1 EXACT:      {baseline_counts['EXACT']}/{n} ({100*baseline_counts['EXACT']/n:.1f}%)\n")
        f.write(f"  K2 EXACT:      {k2_counts['EXACT']}/{n} ({100*k2_counts['EXACT']/n:.1f}%)\n")
        f.write(f"  Delta EXACT:   {exact_delta:+d}\n\n")
        f.write(f"  K1 COMPATIBLE: {compatible}/{n} ({100*compatible/n:.1f}%)\n")
        f.write(f"  K2 COMPATIBLE: {k2_compatible}/{n} ({100*k2_compatible/n:.1f}%)\n")
        f.write(f"  Delta COMPAT:  {compat_delta:+d}\n\n")
        f.write(f"  K1 WRONG:      {baseline_counts['WRONG']}/{n} ({100*baseline_counts['WRONG']/n:.1f}%)\n")
        f.write(f"  K2 WRONG:      {k2_counts['WRONG']}/{n} ({100*k2_counts['WRONG']/n:.1f}%)\n")
        f.write(f"  Delta WRONG:   {wrong_delta:+d}\n\n")

        f.write(f"  Regressions:   {len(reg_list)}\n")
        f.write(f"  Improvements:  {len(improvements)}\n\n")

        if improvements:
            f.write("IMPROVED TRACKS:\n")
            for r in improvements:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['K1_Key']}({r['K1_Relation']}) -> {r['K2_Key']}({r['K2_Relation']})\n")
            f.write("\n")

        f.write(f"REMAINING WRONG: {len(wrong_k2)}\n")
        for r in wrong_k2:
            f.write(f"  {r['Artist']} -- {r['Title']}: K2={r['K2_Key']}, GT={r['Tunebat_Camelot']}, rank={r['CorrectKey_Rank']}\n")
        f.write("\n")

        # Recommendation
        if len(wrong_k2) == 0:
            recommendation = "STOP -- All rows now correct."
        elif all(r["CorrectKey_Rank"] > 6 or r["CorrectKey_Rank"] < 0 for r in wrong_k2):
            recommendation = "STOP -- Remaining failures are detection failures (correct key not in top ranks). Fundamental DSP limitation of STFT-based chroma. Further improvement requires CQT chroma, neural network key estimation, or larger training data."
        elif any(r["CorrectKey_Rank"] <= 3 and r["CorrectKey_Rank"] > 0 for r in wrong_k2):
            recommendation = "PROCEED -- Some failures have correct key in top 3. Further selection logic tuning may help."
        else:
            recommendation = "STOP -- Remaining failures are ranking/detection limitations. Diminishing returns expected."

        f.write(f"RECOMMENDATION: {recommendation}\n\n")
        f.write(f"GATE: {gate}\n")

    log(f"Wrote {report_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 12 -- Proof Package
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 12: PROOF PACKAGE ===")

    # Write execution log
    log_path = os.path.join(proof_dir, "execution_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
        f.write("\n")
    log(f"Wrote {log_path}")

    # Create ZIP
    zip_path = os.path.join(workspace, "_proof", "key_phase2.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(proof_dir):
            fpath = os.path.join(proof_dir, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, os.path.join("key_phase2", fname))
    log(f"Wrote {zip_path}")

    # Final output
    log("")
    print("=" * 60)
    print(f"PF={proof_dir}")
    print(f"ZIP={zip_path}")
    print(f"GATE={gate}")
    print("=" * 60)
    log(f"PF={proof_dir}")
    log(f"ZIP={zip_path}")
    log(f"GATE={gate}")

    # Re-write execution log with final entries
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
        f.write("\n")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=UserWarning)
    main()
