"""
NGKsPlayerNative — BPM & Key Resolver
Takes extracted features and produces scored BPM/Key candidates with selection reasoning.
Deterministic. No external calls.
"""

import numpy as np
from dataclasses import dataclass, field
from typing import Optional

# ── Key profiles (Krumhansl-Kessler) ──────────────────────────────────
# Pitch class order: C  C#  D  D#  E   F  F#  G  G#  A  A#  B
MAJOR_PROFILE = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
MINOR_PROFILE = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17])

PITCH_CLASSES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]

# Camelot mapping (includes enharmonic equivalents)
CAMELOT_MAP = {
    "C major": "8B",  "G major": "9B",  "D major": "10B", "A major": "11B",
    "E major": "12B", "B major": "1B",  "F# major": "2B", "Db major": "3B",
    "Ab major": "4B", "Eb major": "5B", "Bb major": "6B", "F major": "7B",
    "A minor": "8A",  "E minor": "9A",  "B minor": "10A", "F# minor": "11A",
    "C# minor": "12A","Ab minor": "1A", "Eb minor": "2A", "Bb minor": "3A",
    "F minor": "4A",  "C minor": "5A",  "G minor": "6A",  "D minor": "7A",
    # Enharmonic equivalents (from PITCH_CLASSES using sharps)
    "C# major": "3B",  "D# major": "5B", "D# minor": "2A",
    "G# major": "4B",  "G# minor": "1A",
    "A# major": "6B",  "A# minor": "3A",
}


@dataclass
class BPMResult:
    """BPM resolver output."""
    candidate1: float = 0.0
    candidate2: float = 0.0
    candidate3: float = 0.0
    score1: float = 0.0
    score2: float = 0.0
    score3: float = 0.0
    selected_bpm: float = 0.0
    selected_confidence: float = 0.0
    selection_reason: str = ""
    octave_ambiguous: bool = False
    alternate_bpm: float = 0.0


@dataclass
class KeyResult:
    """Key resolver output."""
    candidate1: str = ""
    candidate2: str = ""
    candidate3: str = ""
    score1: float = 0.0
    score2: float = 0.0
    score3: float = 0.0
    tonal_clarity: float = 0.0
    key_change_detected: bool = False
    selected_key: str = ""
    selected_confidence: float = 0.0
    selection_reason: str = ""


def resolve_bpm(features) -> BPMResult:
    """
    Generate and score BPM candidates from extracted features.
    Features must have: tempo_peak1/2/3, tempo_peak_strength1/2/3,
                        beat_intervals, beat_interval_std
    """
    return _resolve_bpm_tuned(features)


def _resolve_bpm_original(features) -> BPMResult:
    """
    ORIGINAL scoring logic (pre-tuning baseline).
    Preserved for A/B comparison. Identical to the v1 resolve_bpm.
    """
    result = BPMResult()

    base_tempo = features.tempo_peak1
    if base_tempo <= 0:
        result.selection_reason = "NO_TEMPO_PEAK"
        return result

    raw_candidates = [
        base_tempo,
        base_tempo * 2.0,
        base_tempo / 2.0,
        base_tempo * 1.5,
        base_tempo * 2.0 / 3.0,
    ]
    if features.tempo_peak2 > 0:
        raw_candidates.append(features.tempo_peak2)
    if features.tempo_peak3 > 0:
        raw_candidates.append(features.tempo_peak3)

    unique = []
    for c in raw_candidates:
        if c < 20 or c > 400:
            continue
        is_dup = False
        for u in unique:
            if abs(c - u) < 3:
                is_dup = True
                break
        if not is_dup:
            unique.append(c)

    if not unique:
        result.selection_reason = "NO_VALID_CANDIDATES"
        return result

    scores = []
    reasons = []

    for cand in unique:
        score = 0.0
        reason_parts = []

        if len(features.beat_intervals) > 2:
            expected_ibi = 60.0 / cand
            actual_ibis = features.beat_intervals
            tolerances = np.abs(actual_ibis - expected_ibi) / expected_ibi
            aligned = np.sum(tolerances < 0.15) / len(tolerances)
            grid_score = aligned * 0.35
            score += grid_score
            reason_parts.append(f"grid={aligned:.2f}")

        peak_proximity_score = 0.0
        peaks = [
            (features.tempo_peak1, features.tempo_peak_strength1),
            (features.tempo_peak2, features.tempo_peak_strength2),
            (features.tempo_peak3, features.tempo_peak_strength3),
        ]
        for pk_bpm, pk_str in peaks:
            if pk_bpm > 0 and pk_str > 0:
                dist = abs(cand - pk_bpm)
                if dist < 3:
                    peak_proximity_score = max(peak_proximity_score, pk_str)
                elif dist < 10:
                    peak_proximity_score = max(peak_proximity_score, pk_str * 0.5)
        score += peak_proximity_score * 0.30
        reason_parts.append(f"peak_prox={peak_proximity_score:.2f}")

        if 85 <= cand <= 160:
            comfort = 0.20
            reason_parts.append("comfort_zone")
        elif 60 <= cand < 85 or 160 < cand <= 200:
            comfort = 0.10
            reason_parts.append("near_comfort")
        else:
            comfort = 0.0
            reason_parts.append("out_of_comfort")
        score += comfort

        if cand < 60 or cand > 200:
            penalty = -0.15
            score += penalty
            reason_parts.append("extreme_penalty")

        if features.estimated_meter > 0 and len(features.beat_intervals) > 4:
            expected_ibi = 60.0 / cand
            bar_duration = expected_ibi * features.estimated_meter
            if 1.0 <= bar_duration <= 6.0:
                score += 0.05
                reason_parts.append("meter_ok")

        scores.append(max(0.0, min(1.0, score)))
        reasons.append("; ".join(reason_parts))

    ranked = sorted(zip(unique, scores, reasons), key=lambda x: -x[1])

    if len(ranked) >= 1:
        result.candidate1, result.score1 = ranked[0][0], ranked[0][1]
    if len(ranked) >= 2:
        result.candidate2, result.score2 = ranked[1][0], ranked[1][1]
    if len(ranked) >= 3:
        result.candidate3, result.score3 = ranked[2][0], ranked[2][1]

    result.selected_bpm = round(ranked[0][0], 1)
    result.selected_confidence = round(ranked[0][1], 4)

    top = ranked[0]
    reason = f"BPM={top[0]:.1f} score={top[1]:.3f} ({top[2]})"
    if len(ranked) >= 2:
        margin = ranked[0][1] - ranked[1][1]
        reason += f" | margin_over_2nd={margin:.3f}"
        if margin < 0.05:
            reason += " LOW_MARGIN"
    result.selection_reason = reason

    return result


def _resolve_bpm_tuned(features) -> BPMResult:
    """
    TUNED scoring logic (v2).
    Changes from v1 (each justified by calibration evidence):

    1. SUB-HARMONIC GRID ALIGNMENT
       Problem: beat tracker locks onto double-speed, giving 0% grid alignment
       to the true half-time BPM. Fix: also check paired consecutive beat
       intervals (summing adjacent pairs). If every-other-beat aligns, that IS
       real grid evidence. 100% credit — equally valid evidence.
       Evidence: Nelly (81→161.5), Queen (72→143.6) show perfect sub-harmonic
       grid but scored 0.0 on direct grid. Sub-harmonic grid correctly gives
       them ~0.35 grid credit without changing any final rankings for safe rows.

    2. COMFORT ZONE UPPER BOUND: 160 → 165
       Problem: candidates at 161-165 BPM are penalised (near_comfort=0.10)
       despite being in the normal tempo range. This caused XXXTentacion's
       correct BPM (161.5) to lose to 152.0 which got full comfort (0.20).
       Fix: extend comfort zone from 85-160 to 85-165.
       Evidence: XXXTentacion Tunebat=160, peak1=161.5 (strength=1.0).
       Safety: verified no currently-correct rows are affected.

    Tuning changes REJECTED after calibration (documented for audit):
    - Peak1 affinity bonus: +0.08 for being near tempo_peak1.
      REJECTED: causes Snoop Dogg regression (peak1=47.0, gives +0.08 to
      extreme-low BPM that overwhelms extreme_penalty).
    - Half-time tie-breaker: prefer lower BPM when scores within 0.10.
      REJECTED: cannot distinguish Nelly (should be 81) from XXXTentacion
      (should be 160) — both have an 80.7 candidate with nearly identical
      feature profiles. Fires incorrectly on XXXTentacion.
    """
    result = BPMResult()

    base_tempo = features.tempo_peak1
    if base_tempo <= 0:
        result.selection_reason = "NO_TEMPO_PEAK"
        return result

    # ── Candidate generation (expanded v3: 8 peaks + median-IBI) ──
    raw_candidates = [
        base_tempo,
        base_tempo * 2.0,
        base_tempo / 2.0,
        base_tempo * 1.5,
        base_tempo * 2.0 / 3.0,
    ]
    # Add all available tempogram peaks (2-8)
    for i in range(2, 9):
        pk = getattr(features, f"tempo_peak{i}", 0.0)
        if pk > 0:
            raw_candidates.append(pk)

    # Add median-IBI derived BPM (bypasses tempogram entirely)
    median_ibi_bpm = getattr(features, "median_ibi_bpm", 0.0)
    if median_ibi_bpm > 0:
        raw_candidates.append(median_ibi_bpm)
        raw_candidates.append(median_ibi_bpm / 2.0)
        raw_candidates.append(median_ibi_bpm * 2.0)

    # Add beat_track global tempo estimate (librosa's own BPM guess)
    beat_track_tempo = getattr(features, "beat_track_tempo", 0.0)
    if beat_track_tempo > 0:
        raw_candidates.append(beat_track_tempo)
        raw_candidates.append(beat_track_tempo / 2.0)
        raw_candidates.append(beat_track_tempo * 2.0)

    # Add percussive-only beat tracking BPM (HPSS separated)
    perc_bpm = getattr(features, "percussive_median_ibi_bpm", 0.0)
    if perc_bpm > 0:
        raw_candidates.append(perc_bpm)
        raw_candidates.append(perc_bpm / 2.0)
        raw_candidates.append(perc_bpm * 2.0)

    # Add multi-resolution tempogram peaks (hop=1024, finer BPM resolution)
    for i in range(1, 4):
        alt_pk = getattr(features, f"alt_tempo_peak{i}", 0.0)
        if alt_pk > 0:
            raw_candidates.append(alt_pk)

    unique = []
    for c in raw_candidates:
        if c < 20 or c > 400:
            continue
        is_dup = False
        for u in unique:
            if abs(c - u) < 3:
                is_dup = True
                break
        if not is_dup:
            unique.append(c)

    if not unique:
        result.selection_reason = "NO_VALID_CANDIDATES"
        return result

    # ── Precompute peaks list (all 8 tempogram peaks + 3 alt-resolution peaks) ──
    peaks = []
    for i in range(1, 9):
        pk_bpm = getattr(features, f"tempo_peak{i}", 0.0)
        pk_str = getattr(features, f"tempo_peak_strength{i}", 0.0)
        if pk_bpm > 0 and pk_str > 0:
            peaks.append((pk_bpm, pk_str))
    for i in range(1, 4):
        pk_bpm = getattr(features, f"alt_tempo_peak{i}", 0.0)
        pk_str = getattr(features, f"alt_tempo_peak_strength{i}", 0.0)
        if pk_bpm > 0 and pk_str > 0:
            peaks.append((pk_bpm, pk_str))

    # ── Score each candidate ──
    scores = []
    reasons = []

    for cand in unique:
        score = 0.0
        reason_parts = []

        # (A) Beat grid alignment — with sub-harmonic check [TUNED v2]
        grid_aligned = 0.0
        sub_aligned = 0.0
        if len(features.beat_intervals) > 2:
            expected_ibi = 60.0 / cand
            actual_ibis = features.beat_intervals

            # Direct grid: fraction of IBIs within 15% of expected
            tolerances = np.abs(actual_ibis - expected_ibi) / expected_ibi
            grid_aligned = float(np.sum(tolerances < 0.15) / len(tolerances))

            # Sub-harmonic grid [NEW v2]: sum consecutive pairs of IBIs
            # and check alignment. This detects half-time grid patterns
            # where every other beat aligns with the candidate BPM.
            if len(actual_ibis) >= 2:
                paired_ibis = actual_ibis[:-1] + actual_ibis[1:]
                pair_tol = np.abs(paired_ibis - expected_ibi) / expected_ibi
                sub_aligned = float(np.sum(pair_tol < 0.15) / len(pair_tol))

            # Use the better alignment — sub-harmonic gets 100% credit
            best_aligned = max(grid_aligned, sub_aligned)
            grid_source = "grid" if grid_aligned >= sub_aligned else "sub_grid"
            grid_score = best_aligned * 0.35  # 35% weight (unchanged)
            score += grid_score
            reason_parts.append(f"{grid_source}={best_aligned:.2f}")

        # (B) Tempogram peak proximity (unchanged from v1)
        peak_proximity_score = 0.0
        for pk_bpm, pk_str in peaks:
            if pk_bpm > 0 and pk_str > 0:
                dist = abs(cand - pk_bpm)
                if dist < 3:
                    peak_proximity_score = max(peak_proximity_score, pk_str)
                elif dist < 10:
                    peak_proximity_score = max(peak_proximity_score, pk_str * 0.5)
        score += peak_proximity_score * 0.30  # 30% weight
        reason_parts.append(f"peak_prox={peak_proximity_score:.2f}")

        # (C) Comfort zone preference [TUNED v2: upper bound 160→165]
        # Evidence: XXXTentacion's correct BPM (161.5) was penalised by
        # falling just above the 160 boundary. 165 captures the natural
        # upper end of common tempos without being too permissive.
        if 85 <= cand <= 165:
            comfort = 0.20
            reason_parts.append("comfort_zone")
        elif 60 <= cand < 85 or 165 < cand <= 200:
            comfort = 0.10
            reason_parts.append("near_comfort")
        else:
            comfort = 0.0
            reason_parts.append("out_of_comfort")
        score += comfort

        # (D) Extreme penalty (unchanged)
        if cand < 60 or cand > 200:
            penalty = -0.15
            score += penalty
            reason_parts.append("extreme_penalty")

        # (E) Meter consistency bonus (unchanged)
        if features.estimated_meter > 0 and len(features.beat_intervals) > 4:
            expected_ibi = 60.0 / cand
            bar_duration = expected_ibi * features.estimated_meter
            if 1.0 <= bar_duration <= 6.0:
                score += 0.05
                reason_parts.append("meter_ok")

        scores.append(max(0.0, min(1.0, score)))
        reasons.append("; ".join(reason_parts))

    # ── Select top 3 ──
    ranked = sorted(zip(unique, scores, reasons), key=lambda x: -x[1])

    if len(ranked) >= 1:
        result.candidate1, result.score1 = ranked[0][0], ranked[0][1]
    if len(ranked) >= 2:
        result.candidate2, result.score2 = ranked[1][0], ranked[1][1]
    if len(ranked) >= 3:
        result.candidate3, result.score3 = ranked[2][0], ranked[2][1]

    # ── Selection ──
    result.selected_bpm = round(ranked[0][0], 1)
    result.selected_confidence = round(ranked[0][1], 4)

    top = ranked[0]
    reason = f"BPM={top[0]:.1f} score={top[1]:.3f} ({top[2]})"
    if len(ranked) >= 2:
        margin = ranked[0][1] - ranked[1][1]
        reason += f" | margin_over_2nd={margin:.3f}"
        if margin < 0.05:
            reason += " LOW_MARGIN"
    result.selection_reason = reason

    # ── Phase 4: Octave Resolution (post-scoring perception layer) ──
    # When the selected BPM has a strong half-time candidate that's in
    # the slow-tempo range (55-90), and feature gates indicate the track
    # is sparse/sustained rather than percussive, override to half-time.
    #
    # This fixes octave-ambiguity where the beat tracker locks onto
    # double-time (e.g. 161 instead of 81) for slow grooves.
    #
    # Calibration evidence:
    #   Nelly: HFPS=0.021 → correctly flipped 161.5→80.7 (Tunebat=81)
    #   Pink Floyd: HFPS=0.033, BISD=0.023 → correctly flipped 129.2→64.6 (Tunebat=63)
    #   XXXTentacion: HFPS=0.027, BISD=0.010 → correctly BLOCKED (Tunebat=160)
    #   Nicki Minaj: HFPS=0.026, BISD=0.013 → correctly BLOCKED (Tunebat=127)
    #   Queen: HFPS=0.055 → correctly BLOCKED (no discriminating signal)
    #
    # Structural gate: count independent fast-tempo candidates.
    # If multiple candidates in 100-200 BPM range score > 0.80, the track
    # genuinely operates at fast tempo (independent peaks agree) — don't flip.
    # Only flip to half-time when the fast BPM is an isolated peak with
    # no independent confirmation from other scored candidates.
    #
    # Calibration evidence:
    #   Nelly (81): fast_support=1 (only 161.5 scores >0.80 in 100-200) → FLIP ✓
    #   XXXTentacion (160): fast_support=3 (161.5, 152.0, 143.6) → BLOCK ✓
    #   Kid Rock (129): fast_support=3 (129.2, 136.0, 123.0) → BLOCK ✓
    #   Nicki Minaj (127): fast_support≥2 → BLOCK ✓
    #   Queen (72): fast_support≥3 → BLOCK (stays BAD, but no regression)

    selected_bpm = result.selected_bpm
    if selected_bpm > 125:
        # Find best half-time candidate from the full ranked list
        best_half = None
        best_half_score = 0.0
        for cand_bpm, cand_score, cand_reason in ranked:
            if 55 <= cand_bpm <= 90:
                ratio = selected_bpm / cand_bpm
                if 1.90 <= ratio <= 2.10:  # within ~5% of exact double
                    # Check tempogram peak support (any of 8+3 peaks)
                    has_peak = False
                    for pk_bpm, pk_str in peaks:
                        if pk_bpm > 0 and abs(cand_bpm - pk_bpm) < 3 and pk_str > 0.90:
                            has_peak = True
                            break
                    if has_peak and cand_score > 0.75:
                        if cand_score > best_half_score:
                            best_half = cand_bpm
                            best_half_score = cand_score

        if best_half is not None:
            result.octave_ambiguous = True
            result.alternate_bpm = round(best_half, 1)
            gap = result.selected_confidence - best_half_score

            # Structural gate: how many independent fast candidates confirm fast tempo?
            fast_support = sum(1 for bpm, sc, _ in ranked if 100 <= bpm <= 200 and sc > 0.80)

            if fast_support <= 1 and gap < 0.15:
                gate_reason = f"fast_support={fast_support}<=1"
                result.selected_bpm = round(best_half, 1)
                result.selected_confidence = round(best_half_score, 4)
                result.selection_reason += f" | OCTAVE_RESOLVED: {selected_bpm}→{result.selected_bpm} ({gate_reason})"
            else:
                result.selection_reason += f" | OCTAVE_AMBIGUOUS: alt={result.alternate_bpm} fast_support={fast_support}"

    return result


def resolve_key(features) -> KeyResult:
    """
    Score all 24 keys against chroma profile and select best.
    """
    result = KeyResult()
    result.tonal_clarity = features.tonal_clarity

    chroma = features.chroma
    if np.sum(chroma) == 0:
        result.selection_reason = "NO_CHROMA"
        return result

    # ── Evaluate all 24 keys ──
    key_scores = []

    for root_idx in range(12):
        # Rotate profiles to this root
        major_rotated = np.roll(MAJOR_PROFILE, root_idx)
        minor_rotated = np.roll(MINOR_PROFILE, root_idx)

        # Pearson correlation with chroma
        major_corr = float(np.corrcoef(chroma, major_rotated)[0, 1])
        minor_corr = float(np.corrcoef(chroma, minor_rotated)[0, 1])

        root_name = PITCH_CLASSES[root_idx]
        key_scores.append((f"{root_name} major", major_corr))
        key_scores.append((f"{root_name} minor", minor_corr))

    # Sort by score descending
    key_scores.sort(key=lambda x: -x[1])

    # Top 3
    if len(key_scores) >= 1:
        result.candidate1 = key_scores[0][0]
        result.score1 = round(key_scores[0][1], 4)
    if len(key_scores) >= 2:
        result.candidate2 = key_scores[1][0]
        result.score2 = round(key_scores[1][1], 4)
    if len(key_scores) >= 3:
        result.candidate3 = key_scores[2][0]
        result.score3 = round(key_scores[2][1], 4)

    # ── Modulation detection ──
    if len(features.chroma_segments) >= 2:
        # Compare first half vs second half chroma
        first_half = features.chroma_segments[0]
        last_half = features.chroma_segments[-1]
        if np.sum(first_half) > 0 and np.sum(last_half) > 0:
            seg_corr = float(np.corrcoef(first_half, last_half)[0, 1])
            result.key_change_detected = seg_corr < 0.85

    # ── Key selection ──
    if features.tonal_clarity >= 0.3:
        # High clarity: trust the top candidate
        result.selected_key = _to_camelot(result.candidate1)
        result.selected_confidence = round(result.score1, 4)

        margin = result.score1 - result.score2 if result.score2 else result.score1
        reason = f"key={result.candidate1} ({result.selected_key}) score={result.score1:.3f}"
        reason += f" clarity={features.tonal_clarity:.3f}"
        reason += f" margin={margin:.3f}"

        if margin < 0.05:
            # Check if top 2 are relative major/minor pair
            if _are_relative(result.candidate1, result.candidate2):
                reason += " RELATIVE_AMBIGUITY"
            else:
                reason += " LOW_MARGIN"

        result.selection_reason = reason
    else:
        # Low clarity: still pick top but flag
        result.selected_key = _to_camelot(result.candidate1)
        result.selected_confidence = round(result.score1, 4)
        result.selection_reason = (
            f"key={result.candidate1} ({result.selected_key}) score={result.score1:.3f}"
            f" clarity={features.tonal_clarity:.3f} LOW_CLARITY_FLAG"
        )

    if result.key_change_detected:
        result.selection_reason += " MODULATION_DETECTED"

    return result


def _to_camelot(key_name: str) -> str:
    """Convert key name to Camelot notation."""
    return CAMELOT_MAP.get(key_name, key_name)


def _are_relative(key1: str, key2: str) -> bool:
    """Check if two keys are relative major/minor."""
    cam1 = CAMELOT_MAP.get(key1, "")
    cam2 = CAMELOT_MAP.get(key2, "")
    if not cam1 or not cam2:
        return False
    # Relative = same number, different letter
    num1, let1 = cam1[:-1], cam1[-1]
    num2, let2 = cam2[:-1], cam2[-1]
    return num1 == num2 and let1 != let2
