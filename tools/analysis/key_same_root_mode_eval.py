"""
NGKsPlayerNative -- KEY Same-Root Mode Disambiguation
Narrow, safe refinement targeting ONLY rows where top-2 candidates share
the same root note and differ only in mode (major vs minor).

No BPM changes. No broad key pipeline changes. No per-song overrides.
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
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SR = 22050
HOP_LENGTH = 512
N_FFT = 2048
DURATION_LIMIT = 180
N_SEGMENTS = 6

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

# Krumhansl-Kessler key profiles
KK_MAJOR = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
KK_MINOR = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17])

FLAT_TO_SHARP = {
    "Db": "C#", "Eb": "D#", "Fb": "E", "Gb": "F#",
    "Ab": "G#", "Bb": "A#", "Cb": "B",
}

MUSIC_DIR = Path(r"C:\Users\suppo\Music")

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
# Tunebat Key parsing / classification (from K1)
# ---------------------------------------------------------------------------
def parse_tunebat_key(raw: str):
    if not raw or str(raw).strip() in ("", "nan"):
        return []
    raw = str(raw).strip()
    parts = [raw]
    if " or " in raw.lower():
        parts = [p.strip().title() for p in raw.lower().split(" or ")]
    results = []
    for part in parts:
        part = part.strip().replace("Mminor", "minor").replace("mminor", "minor")
        part = part.replace("Mmajor", "major").replace("mmajor", "major")
        tokens = part.split()
        if len(tokens) == 2:
            root, mode = tokens[0], tokens[1].lower()
            for flat, sharp in FLAT_TO_SHARP.items():
                if root == flat:
                    root = sharp
                    break
            key_name = f"{root} {mode}"
            cam = CAMELOT_MAP.get(key_name)
            if not cam:
                cam = CAMELOT_MAP.get(f"{root.capitalize()} {mode}")
            if cam:
                results.append(cam)
    return results


def classify_key_relation(detected_cam: str, gt_cams: list) -> str:
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
# Mode-sensitive evidence extraction (STEP 3)
# ---------------------------------------------------------------------------
def extract_mode_evidence(y, sr, root_idx):
    """Extract mode-sensitive evidence for a given root note.
    
    Returns dict with:
      - RootTonicStrength: chroma energy at root
      - MajorThirdStrength: chroma energy at root+4 semitones
      - MinorThirdStrength: chroma energy at root+3 semitones
      - ThirdDelta: major_3rd - minor_3rd (positive = major bias)
      - DominantStrength: chroma at root+7 (perfect 5th)
      - SubdominantStrength: chroma at root+5 (perfect 4th) 
      - BassRootStrength: bass chroma at root
      - BassMajor3rdStrength: bass chroma at root+4
      - BassMinor3rdStrength: bass chroma at root+3
      - BassThirdDelta: bass major_3rd - bass minor_3rd
      - HarmonicMajor3rd: harmonic-only chroma at root+4
      - HarmonicMinor3rd: harmonic-only chroma at root+3
      - HarmonicThirdDelta: harm major_3rd - harm minor_3rd
      - SegmentModeAgreement: fraction of segments favoring majority mode
      - ModeEvidenceScore: combined weighted mode score (positive = major)
      - SameRootModeConfidence: confidence in mode determination (0-1)
    """
    evidence = {}

    # ---- Full-band chroma ----
    chroma_raw = librosa.feature.chroma_stft(y=y, sr=sr, n_fft=N_FFT, hop_length=HOP_LENGTH)
    chroma_avg = np.mean(chroma_raw, axis=1)
    cmax = np.max(chroma_avg)
    if cmax > 0:
        chroma_avg = chroma_avg / cmax

    major_3rd_idx = (root_idx + 4) % 12
    minor_3rd_idx = (root_idx + 3) % 12
    dominant_idx = (root_idx + 7) % 12
    subdominant_idx = (root_idx + 5) % 12

    evidence["RootTonicStrength"] = round(float(chroma_avg[root_idx]), 4)
    evidence["MajorThirdStrength"] = round(float(chroma_avg[major_3rd_idx]), 4)
    evidence["MinorThirdStrength"] = round(float(chroma_avg[minor_3rd_idx]), 4)
    evidence["ThirdDelta"] = round(evidence["MajorThirdStrength"] - evidence["MinorThirdStrength"], 4)
    evidence["DominantStrength"] = round(float(chroma_avg[dominant_idx]), 4)
    evidence["SubdominantStrength"] = round(float(chroma_avg[subdominant_idx]), 4)

    # ---- Bass chroma (<=300 Hz) ----
    try:
        freq_bins = librosa.fft_frequencies(sr=sr, n_fft=N_FFT)
        S_full = np.abs(librosa.stft(y, n_fft=N_FFT, hop_length=HOP_LENGTH))
        bass_mask = freq_bins <= 300
        S_bass = np.zeros_like(S_full)
        S_bass[bass_mask, :] = S_full[bass_mask, :]
        bass_chroma = librosa.feature.chroma_stft(S=S_bass ** 2, sr=sr,
                                                   hop_length=HOP_LENGTH, n_fft=N_FFT)
        bass_avg = np.mean(bass_chroma, axis=1)
        bmax = np.max(bass_avg)
        if bmax > 0:
            bass_avg = bass_avg / bmax
    except Exception:
        bass_avg = np.zeros(12)

    evidence["BassRootStrength"] = round(float(bass_avg[root_idx]), 4)
    evidence["BassMajor3rdStrength"] = round(float(bass_avg[major_3rd_idx]), 4)
    evidence["BassMinor3rdStrength"] = round(float(bass_avg[minor_3rd_idx]), 4)
    evidence["BassThirdDelta"] = round(evidence["BassMajor3rdStrength"] - evidence["BassMinor3rdStrength"], 4)

    # ---- Harmonic-only chroma (HPSS) ----
    try:
        y_harm, _ = librosa.effects.hpss(y)
        harm_chroma_raw = librosa.feature.chroma_stft(y=y_harm, sr=sr,
                                                       n_fft=N_FFT, hop_length=HOP_LENGTH)
        harm_avg = np.mean(harm_chroma_raw, axis=1)
        hmax = np.max(harm_avg)
        if hmax > 0:
            harm_avg = harm_avg / hmax
    except Exception:
        harm_avg = np.zeros(12)
        y_harm = y  # fallback

    evidence["HarmonicMajor3rd"] = round(float(harm_avg[major_3rd_idx]), 4)
    evidence["HarmonicMinor3rd"] = round(float(harm_avg[minor_3rd_idx]), 4)
    evidence["HarmonicThirdDelta"] = round(evidence["HarmonicMajor3rd"] - evidence["HarmonicMinor3rd"], 4)

    # ---- Harmonic-only mode scoring via Pearson correlation ----
    # Score the harmonic chroma against rotated KK profiles for just this root
    major_rot = np.roll(KK_MAJOR, root_idx)
    minor_rot = np.roll(KK_MINOR, root_idx)
    harm_major_corr = float(np.corrcoef(harm_avg, major_rot)[0, 1])
    harm_minor_corr = float(np.corrcoef(harm_avg, minor_rot)[0, 1])
    evidence["HarmonicMajorCorr"] = round(harm_major_corr, 4)
    evidence["HarmonicMinorCorr"] = round(harm_minor_corr, 4)
    evidence["HarmonicCorrDelta"] = round(harm_major_corr - harm_minor_corr, 4)

    # ---- Segment mode voting ----
    total_samples = len(y_harm)
    seg_len = total_samples // N_SEGMENTS
    major_votes = 0
    minor_votes = 0

    for i in range(N_SEGMENTS):
        start = i * seg_len
        end = start + seg_len if i < N_SEGMENTS - 1 else total_samples
        y_seg = y_harm[start:end]
        if len(y_seg) < SR:
            continue
        seg_chroma = librosa.feature.chroma_stft(y=y_seg, sr=sr,
                                                  n_fft=N_FFT, hop_length=HOP_LENGTH)
        seg_avg = np.mean(seg_chroma, axis=1)
        smax = np.max(seg_avg)
        if smax > 0:
            seg_avg = seg_avg / smax
        # Score this segment for major vs minor of the given root
        seg_major_corr = float(np.corrcoef(seg_avg, major_rot)[0, 1])
        seg_minor_corr = float(np.corrcoef(seg_avg, minor_rot)[0, 1])
        if seg_major_corr > seg_minor_corr:
            major_votes += 1
        else:
            minor_votes += 1

    total_votes = major_votes + minor_votes
    if total_votes > 0:
        dominant_mode = "major" if major_votes >= minor_votes else "minor"
        agreement = max(major_votes, minor_votes) / total_votes
    else:
        dominant_mode = "unknown"
        agreement = 0.0

    evidence["SegmentMajorVotes"] = major_votes
    evidence["SegmentMinorVotes"] = minor_votes
    evidence["SegmentDominantMode"] = dominant_mode
    evidence["SegmentModeAgreement"] = round(agreement, 4)

    # ---- Combined ModeEvidenceScore ----
    # Weighted combination of evidence channels. Positive = major, negative = minor.
    #   full-band third delta:    weight 1.0
    #   bass third delta:         weight 1.5 (bass is strong mode indicator)
    #   harmonic third delta:     weight 1.5
    #   harmonic corr delta:      weight 2.0 (strongest theory-based signal)
    #   segment vote bias:        weight 1.0
    seg_bias = (major_votes - minor_votes) / max(total_votes, 1)

    mode_score = (
        1.0 * evidence["ThirdDelta"] +
        1.5 * evidence["BassThirdDelta"] +
        1.5 * evidence["HarmonicThirdDelta"] +
        2.0 * evidence["HarmonicCorrDelta"] +
        1.0 * seg_bias
    )
    evidence["ModeEvidenceScore"] = round(mode_score, 4)

    # Confidence: how consistent are the signals?
    signals = [
        1 if evidence["ThirdDelta"] > 0 else -1 if evidence["ThirdDelta"] < 0 else 0,
        1 if evidence["BassThirdDelta"] > 0 else -1 if evidence["BassThirdDelta"] < 0 else 0,
        1 if evidence["HarmonicThirdDelta"] > 0 else -1 if evidence["HarmonicThirdDelta"] < 0 else 0,
        1 if evidence["HarmonicCorrDelta"] > 0 else -1 if evidence["HarmonicCorrDelta"] < 0 else 0,
        1 if seg_bias > 0 else -1 if seg_bias < 0 else 0,
    ]
    nonzero = [s for s in signals if s != 0]
    if nonzero:
        agreement_ratio = abs(sum(nonzero)) / len(nonzero)
    else:
        agreement_ratio = 0.0
    evidence["SameRootModeConfidence"] = round(agreement_ratio, 4)

    return evidence


# ---------------------------------------------------------------------------
# STEP 4: Safe same-root mode resolver
# ---------------------------------------------------------------------------
def resolve_same_root_mode(candidate1_name, score1, candidate2_name, score2,
                           current_cam, current_relation, evidence):
    """Narrow resolver that ONLY fires when:
    1. candidate1 and candidate2 share same root
    2. One is major, one is minor
    3. Margin is within ambiguity band (< 0.15)
    4. Score1 is not already very high (< 0.85)
    5. Mode evidence is clear
    
    Returns (final_cam, final_name, confidence, decision, reason, applied)
    """
    # Parse root and mode
    parts1 = candidate1_name.split()
    parts2 = candidate2_name.split()
    if len(parts1) != 2 or len(parts2) != 2:
        return current_cam, candidate1_name, score1, "BASE_KEEP", "not_parseable", False

    root1, mode1 = parts1[0], parts1[1]
    root2, mode2 = parts2[0], parts2[1]

    # Gate: must be same root
    if root1 != root2:
        return current_cam, candidate1_name, score1, "BASE_KEEP", "different_roots", False

    # Gate: must be different modes
    if mode1 == mode2:
        return current_cam, candidate1_name, score1, "BASE_KEEP", "same_mode", False

    margin = score1 - score2
    reasons = []
    reasons.append(f"root={root1}")
    reasons.append(f"c1={candidate1_name}({score1:.4f})")
    reasons.append(f"c2={candidate2_name}({score2:.4f})")
    reasons.append(f"margin={margin:.4f}")

    # Gate: margin must be within ambiguity band
    if margin > 0.15:
        reasons.append("MARGIN_TOO_LARGE")
        return current_cam, candidate1_name, score1, "BASE_KEEP", "; ".join(reasons), False

    # Gate: score1 must not be extremely high confidence
    if score1 > 0.85:
        reasons.append("HIGH_CONFIDENCE_KEEP")
        return current_cam, candidate1_name, score1, "BASE_KEEP", "; ".join(reasons), False

    # ---- Evaluate mode evidence ----
    mode_score = evidence["ModeEvidenceScore"]
    confidence = evidence["SameRootModeConfidence"]
    harm_corr_delta = evidence["HarmonicCorrDelta"]
    seg_agreement = evidence["SegmentModeAgreement"]
    seg_dominant = evidence["SegmentDominantMode"]

    reasons.append(f"mode_score={mode_score:.4f}")
    reasons.append(f"confidence={confidence:.4f}")
    reasons.append(f"harm_corr_delta={harm_corr_delta:.4f}")
    reasons.append(f"seg_mode={seg_dominant}(agree={seg_agreement:.2f})")

    # Determine which mode the evidence favors
    evidence_favors_major = mode_score > 0
    evidence_favors_minor = mode_score < 0

    # Strong evidence threshold: |mode_score| > 0.3 AND confidence > 0.5
    evidence_strong = abs(mode_score) > 0.3 and confidence >= 0.6

    # Very strong: |mode_score| > 0.5 AND confidence >= 0.8 AND harmonic corr agrees
    evidence_very_strong = (abs(mode_score) > 0.5 and confidence >= 0.8 and
                            ((evidence_favors_major and harm_corr_delta > 0) or
                             (evidence_favors_minor and harm_corr_delta < 0)))

    if not evidence_strong:
        # Evidence is weak or contradictory: keep existing, flag ambiguous
        reasons.append("EVIDENCE_WEAK_OR_CONTRADICTORY")
        adj_confidence = score1 * 0.9  # lower confidence to flag uncertainty
        return current_cam, candidate1_name, adj_confidence, "SAME_ROOT_AMBIGUOUS_KEEP", "; ".join(reasons), True

    # Evidence is strong enough to consider a swap
    if evidence_favors_major and mode1 == "minor":
        # Current top is minor, evidence says major => swap to candidate2
        new_name = candidate2_name
        new_cam = CAMELOT_MAP.get(new_name, new_name)
        new_confidence = score2  # Use the actual score
        if evidence_very_strong:
            new_confidence *= 1.05  # slight boost for very strong evidence
        reasons.append("SWAP_TO_MAJOR")
        return new_cam, new_name, new_confidence, "SAME_ROOT_MODE_MAJOR", "; ".join(reasons), True

    elif evidence_favors_minor and mode1 == "major":
        # Current top is major, evidence says minor => swap to candidate2
        new_name = candidate2_name
        new_cam = CAMELOT_MAP.get(new_name, new_name)
        new_confidence = score2
        if evidence_very_strong:
            new_confidence *= 1.05
        reasons.append("SWAP_TO_MINOR")
        return new_cam, new_name, new_confidence, "SAME_ROOT_MODE_MINOR", "; ".join(reasons), True

    else:
        # Evidence confirms current mode selection
        reasons.append("EVIDENCE_CONFIRMS_CURRENT")
        return current_cam, candidate1_name, score1, "BASE_KEEP", "; ".join(reasons), True


# ---------------------------------------------------------------------------
# Score all 24 keys (needed for baseline context)
# ---------------------------------------------------------------------------
def score_all_keys(chroma_12, major_prof, minor_prof):
    if np.sum(chroma_12) == 0:
        return []
    scores = []
    for root_idx in range(12):
        maj_rot = np.roll(major_prof, root_idx)
        min_rot = np.roll(minor_prof, root_idx)
        maj_corr = float(np.corrcoef(chroma_12, maj_rot)[0, 1])
        min_corr = float(np.corrcoef(chroma_12, min_rot)[0, 1])
        root = PITCH_CLASSES[root_idx]
        scores.append((f"{root} major", maj_corr))
        scores.append((f"{root} minor", min_corr))
    scores.sort(key=lambda x: -x[1])
    return scores


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def main():
    workspace = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
    os.chdir(workspace)

    proof_dir = os.path.join(workspace, "_proof", "key_same_root_mode")
    os.makedirs(proof_dir, exist_ok=True)

    log("KEY SAME-ROOT MODE DISAMBIGUATION")
    log(f"Workspace: {workspace}")
    log(f"Date: {datetime.now().isoformat()}")

    # ── Load evidence CSV ──
    csv_candidates = [
        os.path.join(workspace, "_proof", "analyzer_upgrade", "03_analysis_with_evidence.csv"),
        os.path.join(workspace, "Validated 02_analysis_results.csv"),
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

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        all_rows = list(csv.DictReader(f))
    log(f"Total evidence rows: {len(all_rows)}")

    # ── Load K1 eval (for tuned key names + relation) ──
    k1_csv = os.path.join(workspace, "_proof", "key_phase1", "03_key_tuned_eval.csv")
    k1_lookup = {}
    if os.path.isfile(k1_csv):
        with open(k1_csv, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                key = (row.get("Artist", ""), row.get("Title", ""))
                k1_lookup[key] = row
        log(f"K1 results loaded: {len(k1_lookup)} rows")
    else:
        log("FATAL: K1 eval CSV not found")
        sys.exit(1)

    # ── Filter calibration rows ──
    cal_rows = []
    for row in all_rows:
        tk = row.get("Tunebat Key", "").strip()
        if tk and tk.lower() != "nan":
            gt_cams = parse_tunebat_key(tk)
            if gt_cams:
                cal_rows.append((row, tk, gt_cams))
    log(f"Calibration rows: {len(cal_rows)}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 1 -- Identify same-root mode ambiguity rows
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 1: LOAD + FILTER ===")

    same_root_rows = []
    all_cal_data = []

    for row, tk, gt_cams in cal_rows:
        artist = row.get("Artist", "")
        title = row.get("Title", "")
        k1_row = k1_lookup.get((artist, title))
        if not k1_row:
            continue

        c1 = k1_row.get("Candidate1", "")
        c2 = k1_row.get("Candidate2", "")
        s1 = float(k1_row.get("Score1", "0"))
        s2 = float(k1_row.get("Score2", "0"))
        tuned_cam = k1_row.get("Tuned_Key", "")
        tuned_rel = k1_row.get("Tuned_Relation", "")

        cal_entry = {
            "Artist": artist,
            "Title": title,
            "Filename": row.get("Filename", ""),
            "Tunebat_Key": tk,
            "GT_Cams": gt_cams,
            "Candidate1": c1,
            "Score1": s1,
            "Candidate2": c2,
            "Score2": s2,
            "Current_Cam": tuned_cam,
            "Current_Relation": tuned_rel,
        }
        all_cal_data.append(cal_entry)

        # Check same-root condition
        parts1 = c1.split()
        parts2 = c2.split()
        if len(parts1) == 2 and len(parts2) == 2:
            root1, mode1 = parts1[0], parts1[1]
            root2, mode2 = parts2[0], parts2[1]
            if root1 == root2 and mode1 != mode2:
                margin = s1 - s2
                cal_entry["is_same_root"] = True
                cal_entry["root"] = root1
                cal_entry["margin"] = margin
                same_root_rows.append(cal_entry)

    log(f"Total calibration rows loaded: {len(all_cal_data)}")
    log(f"Same-root mode ambiguity rows: {len(same_root_rows)}")
    log("")
    for r in same_root_rows:
        log(f"  {r['Artist']} -- {r['Title']}: {r['Candidate1']}({r['Score1']:.4f}) vs {r['Candidate2']}({r['Score2']:.4f}) margin={r['margin']:.4f} current={r['Current_Cam']}({r['Current_Relation']}) GT={r['GT_Cams']}")

    # Write 00_load_summary.txt
    with open(os.path.join(proof_dir, "00_load_summary.txt"), "w", encoding="utf-8") as f:
        f.write("KEY SAME-ROOT MODE DISAMBIGUATION -- Load Summary\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Evidence CSV: {csv_path}\n")
        f.write(f"Total evidence rows: {len(all_rows)}\n")
        f.write(f"Total calibration rows: {len(all_cal_data)}\n")
        f.write(f"Same-root mode ambiguity rows: {len(same_root_rows)}\n\n")
        f.write("Same-root rows:\n")
        for r in same_root_rows:
            f.write(f"  {r['Artist']} -- {r['Title']}\n")
            f.write(f"    c1={r['Candidate1']}({r['Score1']:.4f}) c2={r['Candidate2']}({r['Score2']:.4f}) margin={r['margin']:.4f}\n")
            f.write(f"    current={r['Current_Cam']}({r['Current_Relation']}) GT={r['GT_Cams']}\n")
    log(f"Wrote {os.path.join(proof_dir, '00_load_summary.txt')}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 2 -- Baseline for same-root cases
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 2: SAME-ROOT BASELINE ===")

    baseline_csv_path = os.path.join(proof_dir, "01_same_root_baseline.csv")
    baseline_fields = ["Artist", "Title", "Tunebat_Key", "GT_Cams",
                        "Candidate1", "Score1", "Candidate2", "Score2",
                        "Margin", "Root", "Current_Cam", "Current_Relation",
                        "ModeConfusionOnly"]
    baseline_rows_out = []

    for r in same_root_rows:
        # Is it WRONG only because of mode confusion?
        # i.e., would swapping to the other mode match GT?
        c1_cam = CAMELOT_MAP.get(r["Candidate1"], "")
        c2_cam = CAMELOT_MAP.get(r["Candidate2"], "")
        swap_relation = classify_key_relation(c2_cam, r["GT_Cams"])
        mode_confusion = swap_relation in ("EXACT", "NEIGHBOR", "RELATIVE") and r["Current_Relation"] == "WRONG"

        br = {
            "Artist": r["Artist"],
            "Title": r["Title"],
            "Tunebat_Key": r["Tunebat_Key"],
            "GT_Cams": "|".join(r["GT_Cams"]),
            "Candidate1": r["Candidate1"],
            "Score1": round(r["Score1"], 4),
            "Candidate2": r["Candidate2"],
            "Score2": round(r["Score2"], 4),
            "Margin": round(r["margin"], 4),
            "Root": r["root"],
            "Current_Cam": r["Current_Cam"],
            "Current_Relation": r["Current_Relation"],
            "ModeConfusionOnly": "YES" if mode_confusion else "NO",
        }
        baseline_rows_out.append(br)
        log(f"  {r['Artist']} -- {r['Title']}: mode_confusion={br['ModeConfusionOnly']} current={r['Current_Cam']}({r['Current_Relation']})")

    with open(baseline_csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=baseline_fields)
        w.writeheader()
        w.writerows(baseline_rows_out)
    log(f"Wrote {baseline_csv_path}")

    # Baseline summary
    summary_path = os.path.join(proof_dir, "01_same_root_baseline_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("KEY SAME-ROOT MODE -- Baseline Summary\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Same-root ambiguity rows: {len(same_root_rows)}\n")
        mode_confusion_count = sum(1 for br in baseline_rows_out if br["ModeConfusionOnly"] == "YES")
        f.write(f"Pure mode confusion (swap would fix): {mode_confusion_count}\n")
        f.write(f"Not fixable by mode swap: {len(same_root_rows) - mode_confusion_count}\n\n")
        for br in baseline_rows_out:
            f.write(f"  {br['Artist']} -- {br['Title']}: mode_confusion={br['ModeConfusionOnly']}\n")
            f.write(f"    {br['Candidate1']}({br['Score1']}) vs {br['Candidate2']}({br['Score2']}) margin={br['Margin']}\n")
            f.write(f"    current={br['Current_Cam']}({br['Current_Relation']}) GT={br['GT_Cams']}\n\n")
    log(f"Wrote {summary_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEPS 3-4 -- Extract mode evidence + apply resolver on ALL cal rows
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEPS 3-4: MODE EVIDENCE EXTRACTION + SAME-ROOT RESOLVER ===")

    eval_results = []

    for idx, cal in enumerate(all_cal_data):
        artist = cal["Artist"]
        title = cal["Title"]
        filename = cal.get("Filename", "")
        c1 = cal["Candidate1"]
        c2 = cal["Candidate2"]
        s1 = cal["Score1"]
        s2 = cal["Score2"]
        current_cam = cal["Current_Cam"]
        current_rel = cal["Current_Relation"]
        gt_cams = cal["GT_Cams"]

        # Check if this is a same-root case
        parts1 = c1.split()
        parts2 = c2.split()
        is_sr = (len(parts1) == 2 and len(parts2) == 2 and
                 parts1[0] == parts2[0] and parts1[1] != parts2[1])

        log("")
        log(f"  [{idx+1}/{len(all_cal_data)}] {artist} -- {title}")

        result = {
            "Artist": artist,
            "Title": title,
            "Tunebat_Key": cal["Tunebat_Key"],
            "Tunebat_Camelot": "|".join(gt_cams),
            "Candidate1": c1,
            "Score1": s1,
            "Candidate2": c2,
            "Score2": s2,
            "Current_Cam": current_cam,
            "Current_Relation": current_rel,
            "IsSameRoot": "YES" if is_sr else "NO",
        }

        if not is_sr:
            # Not a same-root case: keep current, no change
            result.update({
                "FinalKey_SR": current_cam,
                "FinalKeyName_SR": REVERSE_CAMELOT.get(current_cam, current_cam),
                "FinalKeyConfidence_SR": s1,
                "FinalKeyDecisionSource_SR": "BASE_KEEP",
                "SameRootModeApplied": "NO",
                "SameRootModeReason": "not_same_root",
                "SameRootModeEvidence": "",
                "ModeEvidenceScore": "",
                "SameRootModeConfidence": "",
                "ThirdDelta": "",
                "BassThirdDelta": "",
                "HarmonicThirdDelta": "",
                "HarmonicCorrDelta": "",
                "SegmentModeAgreement": "",
            })
            sr_rel = current_rel
            log(f"    NOT same-root -> BASE_KEEP ({current_cam})")
        else:
            # Same-root case: extract evidence from audio
            root_name = parts1[0]
            root_idx = PITCH_CLASSES.index(root_name) if root_name in PITCH_CLASSES else -1

            filepath = MUSIC_DIR / filename if filename else None
            evidence = None

            if filepath and filepath.is_file() and root_idx >= 0:
                t0 = time.time()
                try:
                    y, sr_actual = librosa.load(str(filepath), sr=SR, mono=True,
                                                duration=DURATION_LIMIT)
                    evidence = extract_mode_evidence(y, sr_actual, root_idx)
                    elapsed = time.time() - t0
                    log(f"    Evidence extracted in {elapsed:.1f}s")
                except Exception as e:
                    log(f"    ERROR extracting: {e}")

            if evidence is None:
                # Can't extract -- keep current
                result.update({
                    "FinalKey_SR": current_cam,
                    "FinalKeyName_SR": REVERSE_CAMELOT.get(current_cam, current_cam),
                    "FinalKeyConfidence_SR": s1,
                    "FinalKeyDecisionSource_SR": "BASE_KEEP",
                    "SameRootModeApplied": "NO",
                    "SameRootModeReason": "extraction_failed",
                    "SameRootModeEvidence": "",
                    "ModeEvidenceScore": "",
                    "SameRootModeConfidence": "",
                    "ThirdDelta": "",
                    "BassThirdDelta": "",
                    "HarmonicThirdDelta": "",
                    "HarmonicCorrDelta": "",
                    "SegmentModeAgreement": "",
                })
                sr_rel = current_rel
                log(f"    Extraction failed -> BASE_KEEP ({current_cam})")
            else:
                # Apply the narrow resolver
                final_cam, final_name, final_conf, decision, reason, applied = \
                    resolve_same_root_mode(c1, s1, c2, s2, current_cam, current_rel, evidence)

                result.update({
                    "FinalKey_SR": final_cam,
                    "FinalKeyName_SR": final_name,
                    "FinalKeyConfidence_SR": round(final_conf, 4),
                    "FinalKeyDecisionSource_SR": decision,
                    "SameRootModeApplied": "YES" if applied else "NO",
                    "SameRootModeReason": reason,
                    "SameRootModeEvidence": f"3rdD={evidence['ThirdDelta']:.4f} bass3rdD={evidence['BassThirdDelta']:.4f} harm3rdD={evidence['HarmonicThirdDelta']:.4f} harmCorrD={evidence['HarmonicCorrDelta']:.4f} segMode={evidence['SegmentDominantMode']}({evidence['SegmentModeAgreement']:.2f})",
                    "ModeEvidenceScore": evidence["ModeEvidenceScore"],
                    "SameRootModeConfidence": evidence["SameRootModeConfidence"],
                    "ThirdDelta": evidence["ThirdDelta"],
                    "BassThirdDelta": evidence["BassThirdDelta"],
                    "HarmonicThirdDelta": evidence["HarmonicThirdDelta"],
                    "HarmonicCorrDelta": evidence["HarmonicCorrDelta"],
                    "SegmentModeAgreement": evidence["SegmentModeAgreement"],
                })
                sr_rel = classify_key_relation(final_cam or "", gt_cams)

                log(f"    Root={root_name}, mode_score={evidence['ModeEvidenceScore']:.4f}, confidence={evidence['SameRootModeConfidence']:.4f}")
                log(f"    3rdDelta={evidence['ThirdDelta']:.4f} bass3rdD={evidence['BassThirdDelta']:.4f} harm3rdD={evidence['HarmonicThirdDelta']:.4f} harmCorrD={evidence['HarmonicCorrDelta']:.4f}")
                log(f"    SegMode={evidence['SegmentDominantMode']}({evidence['SegmentModeAgreement']:.2f})")
                log(f"    Decision={decision}: {current_cam}({current_rel}) -> {final_cam}({sr_rel})")

        result["FinalRelation_SR"] = sr_rel

        # Check for regression
        changed = result["FinalKey_SR"] != current_cam
        regressed = False
        improved = ""
        if changed:
            rel_order = {"EXACT": 0, "NEIGHBOR": 1, "RELATIVE": 2, "WRONG": 3}
            old_o = rel_order.get(current_rel, 3)
            new_o = rel_order.get(sr_rel, 3)
            if new_o > old_o:
                regressed = True
                improved = "REGRESSION"
            elif new_o < old_o:
                improved = "IMPROVED"
            else:
                improved = "LATERAL"

        result["Changed"] = "YES" if changed else "NO"
        result["Regressed"] = "YES" if regressed else "NO"
        result["Improvement"] = improved

        eval_results.append(result)

    log("")
    log(f"Processing complete: {len(eval_results)} rows")

    # ──────────────────────────────────────────────────────────────────
    # STEP 5 -- Evaluate
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 5: EVALUATE ===")

    # Overall metrics
    before_counts = {"EXACT": 0, "NEIGHBOR": 0, "RELATIVE": 0, "WRONG": 0}
    after_counts = {"EXACT": 0, "NEIGHBOR": 0, "RELATIVE": 0, "WRONG": 0}
    for r in eval_results:
        before_counts[r["Current_Relation"]] += 1
        after_counts[r["FinalRelation_SR"]] += 1

    n = len(eval_results)
    before_compat = before_counts["EXACT"] + before_counts["NEIGHBOR"] + before_counts["RELATIVE"]
    after_compat = after_counts["EXACT"] + after_counts["NEIGHBOR"] + after_counts["RELATIVE"]

    log(f"Overall results (n={n}):")
    log(f"  Before EXACT:      {before_counts['EXACT']}/{n}")
    log(f"  After  EXACT:      {after_counts['EXACT']}/{n}")
    log(f"  Before COMPATIBLE: {before_compat}/{n}")
    log(f"  After  COMPATIBLE: {after_compat}/{n}")
    log(f"  Before WRONG:      {before_counts['WRONG']}/{n}")
    log(f"  After  WRONG:      {after_counts['WRONG']}/{n}")

    # Same-root specific metrics
    sr_results = [r for r in eval_results if r["IsSameRoot"] == "YES"]
    sr_improved = [r for r in sr_results if r["Improvement"] == "IMPROVED"]
    sr_regressed = [r for r in sr_results if r["Improvement"] == "REGRESSION"]
    sr_unchanged = [r for r in sr_results if r["Changed"] == "NO"]
    sr_lateral = [r for r in sr_results if r["Improvement"] == "LATERAL"]

    log("")
    log(f"Same-root rows: {len(sr_results)}")
    log(f"  Improved:  {len(sr_improved)}")
    log(f"  Unchanged: {len(sr_unchanged)}")
    log(f"  Lateral:   {len(sr_lateral)}")
    log(f"  Regressed: {len(sr_regressed)}")

    if sr_improved:
        log("")
        log("IMPROVED same-root rows:")
        for r in sr_improved:
            log(f"  {r['Artist']} -- {r['Title']}: {r['Current_Cam']}({r['Current_Relation']}) -> {r['FinalKey_SR']}({r['FinalRelation_SR']})")

    if sr_regressed:
        log("")
        log("REGRESSED same-root rows:")
        for r in sr_regressed:
            log(f"  {r['Artist']} -- {r['Title']}: {r['Current_Cam']}({r['Current_Relation']}) -> {r['FinalKey_SR']}({r['FinalRelation_SR']})")

    # Write eval CSV
    eval_csv_path = os.path.join(proof_dir, "02_same_root_eval.csv")
    if eval_results:
        fields = list(eval_results[0].keys())
        with open(eval_csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(eval_results)
    log(f"Wrote {eval_csv_path}")

    # Write eval summary
    eval_summary_path = os.path.join(proof_dir, "02_same_root_summary.txt")
    with open(eval_summary_path, "w", encoding="utf-8") as f:
        f.write("KEY SAME-ROOT MODE -- Evaluation Summary\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Overall (n={n}):\n")
        f.write(f"  Before EXACT:      {before_counts['EXACT']}\n")
        f.write(f"  After  EXACT:      {after_counts['EXACT']}\n")
        f.write(f"  Before COMPATIBLE: {before_compat}\n")
        f.write(f"  After  COMPATIBLE: {after_compat}\n")
        f.write(f"  Before WRONG:      {before_counts['WRONG']}\n")
        f.write(f"  After  WRONG:      {after_counts['WRONG']}\n\n")
        f.write(f"Same-root rows: {len(sr_results)}\n")
        f.write(f"  Improved:  {len(sr_improved)}\n")
        f.write(f"  Unchanged: {len(sr_unchanged)}\n")
        f.write(f"  Lateral:   {len(sr_lateral)}\n")
        f.write(f"  Regressed: {len(sr_regressed)}\n\n")
        for r in sr_results:
            f.write(f"  {r['Artist']} -- {r['Title']}:\n")
            f.write(f"    {r['Current_Cam']}({r['Current_Relation']}) -> {r['FinalKey_SR']}({r['FinalRelation_SR']}) [{r['Improvement'] or 'NO_CHANGE'}]\n")
            f.write(f"    Decision={r['FinalKeyDecisionSource_SR']}\n")
            if r['SameRootModeEvidence']:
                f.write(f"    Evidence={r['SameRootModeEvidence']}\n")
            f.write("\n")
    log(f"Wrote {eval_summary_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 6 -- Regression Guard
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 6: REGRESSION GUARD ===")

    protected = [r for r in eval_results if r["Current_Relation"] in ("EXACT", "NEIGHBOR", "RELATIVE")]
    reg_list = [r for r in protected if r["Regressed"] == "YES"]

    log(f"Protected rows: {len(protected)}, regressions: {len(reg_list)}")

    gate = "PASS" if len(reg_list) == 0 else "FAIL"
    log(f"GATE: {gate}")

    reg_path = os.path.join(proof_dir, "03_regression_guard.txt")
    with open(reg_path, "w", encoding="utf-8") as f:
        f.write("KEY SAME-ROOT MODE -- Regression Guard\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Protected rows (EXACT/NEIGHBOR/RELATIVE before): {len(protected)}\n")
        f.write(f"Regressions: {len(reg_list)}\n")
        f.write(f"GATE: {gate}\n\n")
        if reg_list:
            f.write("Regressed rows:\n")
            for r in reg_list:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['Current_Cam']}({r['Current_Relation']}) -> {r['FinalKey_SR']}({r['FinalRelation_SR']})\n")
                f.write(f"    Reason: {r['SameRootModeReason']}\n")
        else:
            f.write("No regressions detected. All protected rows maintained.\n")
    log(f"Wrote {reg_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 7 -- Final Report
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 7: FINAL REPORT ===")

    exact_delta = after_counts["EXACT"] - before_counts["EXACT"]
    compat_delta = after_compat - before_compat
    wrong_delta = after_counts["WRONG"] - before_counts["WRONG"]

    # Nelly-type case analysis
    nelly_cases = [r for r in sr_results if r["Current_Relation"] == "WRONG"]
    nelly_fixed = [r for r in nelly_cases if r["Improvement"] == "IMPROVED"]

    report_path = os.path.join(proof_dir, "04_final_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("KEY SAME-ROOT MODE DISAMBIGUATION -- FINAL REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")

        f.write("SCOPE:\n")
        f.write("  Narrow refinement targeting ONLY same-root major/minor ambiguity.\n")
        f.write("  Uses mode-sensitive evidence: third-degree chroma energy,\n")
        f.write("  bass chroma profile, harmonic-only chroma, Pearson correlation\n")
        f.write("  against mode-specific KK profiles, per-segment mode voting.\n\n")

        f.write(f"SAME-ROOT ROWS FOUND: {len(sr_results)}\n")
        for r in sr_results:
            f.write(f"  {r['Artist']} -- {r['Title']}: {r['Candidate1']} vs {r['Candidate2']} (margin={r['Score1']-r['Score2']:.4f})\n")
        f.write("\n")

        f.write("RESULTS:\n")
        f.write(f"  Before EXACT:      {before_counts['EXACT']}/{n}\n")
        f.write(f"  After  EXACT:      {after_counts['EXACT']}/{n}\n")
        f.write(f"  Delta EXACT:       {exact_delta:+d}\n\n")
        f.write(f"  Before COMPATIBLE: {before_compat}/{n}\n")
        f.write(f"  After  COMPATIBLE: {after_compat}/{n}\n")
        f.write(f"  Delta COMPATIBLE:  {compat_delta:+d}\n\n")
        f.write(f"  Before WRONG:      {before_counts['WRONG']}/{n}\n")
        f.write(f"  After  WRONG:      {after_counts['WRONG']}/{n}\n")
        f.write(f"  Delta WRONG:       {wrong_delta:+d}\n\n")

        f.write(f"  Regressions:       {len(reg_list)}\n")
        f.write(f"  Improvements:      {len(sr_improved)}\n\n")

        f.write("SAME-ROOT OUTCOMES:\n")
        for r in sr_results:
            f.write(f"  {r['Artist']} -- {r['Title']}:\n")
            f.write(f"    Before: {r['Current_Cam']}({r['Current_Relation']})\n")
            f.write(f"    After:  {r['FinalKey_SR']}({r['FinalRelation_SR']})\n")
            f.write(f"    Decision: {r['FinalKeyDecisionSource_SR']}\n")
            if r["SameRootModeEvidence"]:
                f.write(f"    Evidence: {r['SameRootModeEvidence']}\n")
            f.write(f"    Result: {r['Improvement'] or 'NO_CHANGE'}\n\n")

        f.write("NELLY-TYPE CASES (WRONG + same-root):\n")
        f.write(f"  Total: {len(nelly_cases)}\n")
        f.write(f"  Fixed: {len(nelly_fixed)}\n")
        if nelly_cases:
            for r in nelly_cases:
                f.write(f"  {r['Artist']} -- {r['Title']}: {r['Current_Cam']}({r['Current_Relation']}) -> {r['FinalKey_SR']}({r['FinalRelation_SR']}) [{r['Improvement'] or 'NO_CHANGE'}]\n")
        f.write("\n")

        # Verdict
        f.write("VERDICT:\n")
        if len(sr_improved) > 0 and len(reg_list) == 0:
            f.write("  Same-root mode disambiguation IMPROVED accuracy with no regressions.\n")
            f.write("  This refinement is safe to integrate.\n")
        elif len(sr_improved) == 0 and len(reg_list) == 0:
            f.write("  Same-root mode disambiguation did not change any outcomes.\n")
            if len(nelly_cases) > 0 and len(nelly_fixed) == 0:
                f.write("  The remaining same-root WRONG cases have evidence that is\n")
                f.write("  either weak, contradictory, or the margin is too large for safe override.\n")
                f.write("  The ambiguity is not safely resolvable with current evidence.\n")
            f.write("  No regressions. GATE=PASS by safety (no harm done).\n")
        else:
            f.write("  Regressions detected. GATE=FAIL.\n")

        f.write("\n")
        f.write("RECOMMENDATION:\n")
        if after_counts["WRONG"] > 0:
            remaining_wrong = [r for r in eval_results if r["FinalRelation_SR"] == "WRONG"]
            sr_wrong = [r for r in remaining_wrong if r["IsSameRoot"] == "YES"]
            non_sr_wrong = [r for r in remaining_wrong if r["IsSameRoot"] == "NO"]
            if len(sr_wrong) > 0:
                f.write(f"  {len(sr_wrong)} same-root WRONG rows remain but are not safely fixable.\n")
            if len(non_sr_wrong) > 0:
                f.write(f"  {len(non_sr_wrong)} non-same-root WRONG rows remain (outside this phase's scope).\n")
            f.write("  Further key work on same-root mode is NOT justified -- diminishing returns.\n")
            f.write("  Remaining WRONG rows are detection failures or fundamental DSP limitations.\n")
        else:
            f.write("  All rows are now EXACT or COMPATIBLE. Key detection is complete.\n")

        f.write(f"\nGATE: {gate}\n")
    log(f"Wrote {report_path}")

    # ──────────────────────────────────────────────────────────────────
    # STEP 9 -- Proof Package
    # ──────────────────────────────────────────────────────────────────
    log("")
    log("=== STEP 9: PROOF PACKAGE ===")

    # Write execution log
    log_path = os.path.join(proof_dir, "execution_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
        f.write("\n")
    log(f"Wrote {log_path}")

    # Create ZIP
    zip_path = os.path.join(workspace, "_proof", "key_same_root_mode.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(proof_dir):
            fpath = os.path.join(proof_dir, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, os.path.join("key_same_root_mode", fname))
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
    main()
