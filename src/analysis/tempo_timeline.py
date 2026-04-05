"""
NGKsPlayerNative — Tempo Timeline Builder
Builds a time-indexed BPM timeline from accumulated frame features,
then resolves global BPM, confidence, and BPM family.
"""

import numpy as np
from analysis_contracts import TimelinePoint


# ── BPM family ranges ──
BPM_FAMILIES = [
    (60, 90, "SLOW"),
    (90, 115, "MID"),
    (115, 145, "NORMAL"),
    (145, 180, "FAST"),
    (180, 300, "VERY_FAST"),
]


def classify_bpm_family(bpm: float) -> str:
    for lo, hi, label in BPM_FAMILIES:
        if lo <= bpm < hi:
            return label
    if bpm < 60:
        return "VERY_SLOW"
    return "VERY_FAST"


def build_tempo_timeline(
    frame_features: list[dict],
    global_beat_intervals: list[float],
) -> dict:
    """Build tempo timeline and resolve global BPM.

    Args:
        frame_features: List of FrameFeature.to_dict() dicts.
        global_beat_intervals: All beat intervals across the full track (seconds).

    Returns:
        dict with keys:
            tempo_timeline:  list of TimelinePoint dicts
            final_bpm:       float
            bpm_confidence:  float
            bpm_family:      str
            bpm_candidates:  list of {bpm, confidence, source}
    """
    timeline: list[dict] = []
    local_bpms: list[float] = []

    for ff in frame_features:
        local_tempo = ff.get("local_tempo", 0.0)
        if local_tempo > 20:
            mid_t = (ff["start_s"] + ff["end_s"]) / 2.0
            local_bpms.append(local_tempo)

            # Confidence based on beat count — more beats = more confident
            beat_count = ff.get("beat_count", 0)
            conf = min(1.0, beat_count / 8.0)

            tp = TimelinePoint(
                time_s=mid_t,
                value=local_tempo,
                confidence=conf,
                label=f"{local_tempo:.1f} BPM",
            )
            timeline.append(tp.to_dict())

    # ── Resolve global BPM ──
    candidates: list[dict] = []

    # Method 1: median of local tempos (weighted by confidence)
    if local_bpms:
        median_local = float(np.median(local_bpms))
        candidates.append({
            "bpm": round(median_local, 2),
            "confidence": 0.6,
            "source": "median_local_tempos",
        })

    # Method 2: global median IBI
    if global_beat_intervals:
        ibi_arr = np.array(global_beat_intervals)
        ibi_arr = ibi_arr[(ibi_arr > 0.2) & (ibi_arr < 2.0)]  # 30–300 BPM range
        if len(ibi_arr) > 4:
            median_ibi = float(np.median(ibi_arr))
            ibi_bpm = 60.0 / median_ibi if median_ibi > 0 else 0.0
            # Confidence from IBI consistency
            ibi_std = float(np.std(ibi_arr))
            ibi_conf = max(0.0, min(1.0, 1.0 - (ibi_std / median_ibi) * 2))
            candidates.append({
                "bpm": round(ibi_bpm, 2),
                "confidence": round(ibi_conf, 3),
                "source": "global_median_ibi",
            })

    # Method 3: mode (histogram peak) of local tempos
    if len(local_bpms) > 3:
        # Bucket into 1-BPM bins
        bins = np.arange(20, 301, 1.0)
        hist, edges = np.histogram(local_bpms, bins=bins)
        peak_idx = int(np.argmax(hist))
        mode_bpm = float((edges[peak_idx] + edges[peak_idx + 1]) / 2.0)
        mode_count = int(hist[peak_idx])
        mode_conf = min(1.0, mode_count / max(1, len(local_bpms)))
        candidates.append({
            "bpm": round(mode_bpm, 2),
            "confidence": round(mode_conf, 3),
            "source": "mode_histogram",
        })

    # Select best candidate
    if candidates:
        # Prefer global IBI if confident, else use highest confidence
        candidates.sort(key=lambda c: c["confidence"], reverse=True)
        best = candidates[0]
        final_bpm = best["bpm"]
        bpm_confidence = best["confidence"]
    else:
        final_bpm = 0.0
        bpm_confidence = 0.0

    # Normalise BPM to DJ range (60–180)
    normalised_bpm = final_bpm
    while normalised_bpm > 0 and normalised_bpm > 180:
        normalised_bpm /= 2.0
    while 0 < normalised_bpm < 60:
        normalised_bpm *= 2.0

    bpm_family = classify_bpm_family(normalised_bpm) if normalised_bpm > 0 else ""

    return {
        "tempo_timeline": timeline,
        "final_bpm": round(normalised_bpm, 2),
        "bpm_confidence": round(bpm_confidence, 3),
        "bpm_family": bpm_family,
        "bpm_candidates": candidates,
    }
