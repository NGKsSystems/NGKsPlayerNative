"""
NGKsPlayerNative — Key Timeline Builder
Builds a time-indexed key timeline from accumulated chroma features,
resolves global key via weighted voting, and detects key changes.
"""

import numpy as np
from analysis_contracts import TimelinePoint

# ── Krumhansl-Kessler key profiles ──
MAJOR_PROFILE = np.array([6.35, 2.23, 3.48, 2.33, 4.38, 4.09,
                           2.52, 5.19, 2.39, 3.66, 2.29, 2.88])
MINOR_PROFILE = np.array([6.33, 2.68, 3.52, 5.38, 2.60, 3.53,
                           2.54, 4.75, 3.98, 2.69, 3.34, 3.17])

NOTE_NAMES = ["C", "C#", "D", "D#", "E", "F",
              "F#", "G", "G#", "A", "A#", "B"]

# Camelot wheel mapping: (root_index, is_minor) -> Camelot code
CAMELOT_MAP = {
    (0, False): "8B", (0, True): "5A",    # C major / C minor
    (1, False): "3B", (1, True): "12A",   # C# / Db
    (2, False): "10B", (2, True): "7A",   # D
    (3, False): "5B", (3, True): "2A",    # D# / Eb
    (4, False): "12B", (4, True): "9A",   # E
    (5, False): "7B", (5, True): "4A",    # F
    (6, False): "2B", (6, True): "11A",   # F# / Gb
    (7, False): "9B", (7, True): "6A",    # G
    (8, False): "4B", (8, True): "1A",    # G# / Ab
    (9, False): "11B", (9, True): "8A",   # A
    (10, False): "6B", (10, True): "3A",  # A# / Bb
    (11, False): "1B", (11, True): "10A", # B
}


def _correlate_key(chroma_vec: np.ndarray) -> tuple[int, bool, float]:
    """Find best-matching key using Krumhansl-Kessler correlation.

    Returns: (root_index, is_minor, correlation)
    """
    best_corr = -1.0
    best_root = 0
    best_minor = False

    for shift in range(12):
        rotated = np.roll(chroma_vec, -shift)
        corr_major = float(np.corrcoef(rotated, MAJOR_PROFILE)[0, 1])
        corr_minor = float(np.corrcoef(rotated, MINOR_PROFILE)[0, 1])

        if corr_major > best_corr:
            best_corr = corr_major
            best_root = shift
            best_minor = False
        if corr_minor > best_corr:
            best_corr = corr_minor
            best_root = shift
            best_minor = True

    return best_root, best_minor, best_corr


def _key_label(root: int, is_minor: bool) -> str:
    mode = "minor" if is_minor else "major"
    return f"{NOTE_NAMES[root]} {mode}"


def _camelot_code(root: int, is_minor: bool) -> str:
    return CAMELOT_MAP.get((root, is_minor), "?")


def build_key_timeline(frame_features: list[dict]) -> dict:
    """Build key timeline and resolve global key.

    Args:
        frame_features: List of FrameFeature.to_dict() dicts.

    Returns:
        dict with keys:
            key_timeline:        list of TimelinePoint dicts
            final_key:           str (Camelot code)
            final_key_name:      str (e.g. "C major")
            key_confidence:      float
            key_change_detected: bool
    """
    timeline: list[dict] = []
    votes: dict[tuple[int, bool], float] = {}  # (root, is_minor) -> weighted score

    for ff in frame_features:
        chroma = ff.get("chroma", [0.0] * 12)
        if len(chroma) != 12:
            continue

        chroma_arr = np.array(chroma, dtype=float)
        if np.sum(chroma_arr) < 0.01:
            continue

        root, is_minor, correlation = _correlate_key(chroma_arr)
        cam = _camelot_code(root, is_minor)
        name = _key_label(root, is_minor)

        mid_t = (ff["start_s"] + ff["end_s"]) / 2.0
        conf = max(0.0, min(1.0, (correlation + 1.0) / 2.0))  # map [-1,1] to [0,1]

        tp = TimelinePoint(
            time_s=mid_t,
            value=float(root * 2 + (1 if is_minor else 0)),  # numeric encoding
            confidence=conf,
            label=f"{cam} ({name})",
        )
        timeline.append(tp.to_dict())

        # Weighted vote
        key_tuple = (root, is_minor)
        votes[key_tuple] = votes.get(key_tuple, 0.0) + conf

    # ── Resolve global key ──
    if votes:
        sorted_votes = sorted(votes.items(), key=lambda kv: kv[1], reverse=True)
        best_key, best_score = sorted_votes[0]
        total_score = sum(v for _, v in sorted_votes)
        key_confidence = best_score / total_score if total_score > 0 else 0.0

        final_key = _camelot_code(*best_key)
        final_key_name = _key_label(*best_key)
    else:
        final_key = ""
        final_key_name = ""
        key_confidence = 0.0
        sorted_votes = []

    # ── Detect key changes ──
    key_change_detected = False
    if len(timeline) >= 4:
        # Check if second-most-voted key has significant weight
        if len(sorted_votes) >= 2:
            _, second_score = sorted_votes[1]
            if total_score > 0 and (second_score / total_score) > 0.3:
                key_change_detected = True

    return {
        "key_timeline": timeline,
        "final_key": final_key,
        "final_key_name": final_key_name,
        "key_confidence": round(key_confidence, 3),
        "key_change_detected": key_change_detected,
    }
