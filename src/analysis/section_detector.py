"""
NGKsPlayerNative — Section Detector
Detects structural sections (intro, verse, chorus, bridge, outro)
using energy contour, spectral novelty, and repetition analysis.
"""

import numpy as np
from analysis_contracts import SectionRecord


# ── Section label heuristics ──
def _label_section(
    idx: int,
    total: int,
    energy: float,
    mean_energy: float,
    prev_energy: float,
) -> str:
    """Heuristic section labeling based on position and energy."""
    if idx == 0:
        return "intro"
    if idx == total - 1:
        return "outro"

    ratio = energy / mean_energy if mean_energy > 0 else 1.0

    if ratio > 1.3:
        return "chorus"
    if ratio < 0.7:
        return "bridge"
    if prev_energy > 0 and energy > prev_energy * 1.2:
        return "buildup"
    return "verse"


def detect_sections(
    frame_features: list[dict],
    track_duration_s: float,
    min_sections: int = 4,
    max_sections: int = 12,
) -> list[dict]:
    """Detect structural sections from accumulated frame features.

    Uses energy contour + novelty to find section boundaries,
    then labels sections heuristically.

    Args:
        frame_features:   List of FrameFeature.to_dict() dicts.
        track_duration_s: Total track duration in seconds.
        min_sections:     Minimum sections to produce.
        max_sections:     Maximum sections to produce.

    Returns:
        List of SectionRecord.to_dict() dicts.
    """
    if not frame_features or track_duration_s <= 0:
        return []

    n = len(frame_features)

    # ── Build energy and novelty curves ──
    energies = np.array([ff.get("rms", 0.0) for ff in frame_features])
    chromas = np.array([ff.get("chroma", [0.0] * 12) for ff in frame_features])
    times_start = np.array([ff.get("start_s", 0.0) for ff in frame_features])
    times_end = np.array([ff.get("end_s", 0.0) for ff in frame_features])

    # Spectral novelty: cosine distance between consecutive chroma frames
    novelty = np.zeros(n)
    for i in range(1, n):
        a = chromas[i - 1]
        b = chromas[i]
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a > 0 and norm_b > 0:
            cos_sim = np.dot(a, b) / (norm_a * norm_b)
            novelty[i] = 1.0 - cos_sim

    # ── Find boundaries ──
    # Combine energy gradient + novelty
    if n > 2:
        energy_grad = np.abs(np.gradient(energies))
        energy_grad = energy_grad / (np.max(energy_grad) + 1e-10)
    else:
        energy_grad = np.zeros(n)

    if np.max(novelty) > 0:
        novelty_norm = novelty / np.max(novelty)
    else:
        novelty_norm = novelty

    combined = 0.5 * energy_grad + 0.5 * novelty_norm

    # Adaptive threshold for boundary detection
    target_sections = max(min_sections, min(max_sections, n // 3))
    target_boundaries = target_sections - 1

    if n <= target_sections:
        # Not enough chunks — make each chunk a section
        boundary_indices = list(range(n))
    else:
        # Find top-N peaks in combined signal (skip first and last)
        if n > 2:
            interior = combined[1:-1]
            sorted_idx = np.argsort(interior)[::-1]
            # Pick top peaks with minimum spacing
            min_spacing = max(1, n // (max_sections + 1))
            picked: list[int] = []
            for idx in sorted_idx:
                real_idx = idx + 1  # offset for interior slice
                too_close = any(abs(real_idx - p) < min_spacing for p in picked)
                if not too_close:
                    picked.append(real_idx)
                if len(picked) >= target_boundaries:
                    break

            picked.sort()
            boundary_indices = [0] + picked
        else:
            boundary_indices = [0]

    # ── Build section records ──
    sections: list[dict] = []
    mean_energy = float(np.mean(energies)) if len(energies) > 0 else 0.0
    prev_energy = 0.0

    for i, start_idx in enumerate(boundary_indices):
        if i + 1 < len(boundary_indices):
            end_idx = boundary_indices[i + 1] - 1
        else:
            end_idx = n - 1

        if end_idx < start_idx:
            end_idx = start_idx

        sec_start = float(times_start[start_idx])
        sec_end = float(times_end[end_idx])
        sec_duration = sec_end - sec_start

        # Section energy
        sec_energy = float(np.mean(energies[start_idx:end_idx + 1]))

        # Section local BPM (median of local tempos)
        sec_tempos = [
            ff.get("local_tempo", 0.0)
            for ff in frame_features[start_idx:end_idx + 1]
            if ff.get("local_tempo", 0.0) > 20
        ]
        sec_bpm = float(np.median(sec_tempos)) if sec_tempos else 0.0

        # Section key (mode of chroma-derived keys)
        sec_chromas = chromas[start_idx:end_idx + 1]
        sec_key = _resolve_section_key(sec_chromas)

        # Novelty at boundary
        sec_novelty = float(novelty[start_idx]) if start_idx > 0 else 0.0

        label = _label_section(i, len(boundary_indices), sec_energy, mean_energy, prev_energy)
        prev_energy = sec_energy

        rec = SectionRecord(
            index=i,
            start_s=sec_start,
            end_s=sec_end,
            duration_s=round(sec_duration, 3),
            label=label,
            energy=sec_energy,
            bpm=sec_bpm,
            key=sec_key,
            novelty=sec_novelty,
        )
        sections.append(rec.to_dict())

    return sections


def _resolve_section_key(chromas: np.ndarray) -> str:
    """Resolve key for a section from its chroma vectors."""
    if len(chromas) == 0:
        return ""

    mean_chroma = np.mean(chromas, axis=0)
    if np.sum(mean_chroma) < 0.01:
        return ""

    # Import key correlation from key_timeline (avoid circular)
    from key_timeline import _correlate_key, _camelot_code

    root, is_minor, _ = _correlate_key(mean_chroma)
    return _camelot_code(root, is_minor)
