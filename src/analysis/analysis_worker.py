"""
NGKsPlayerNative — Analysis Worker
Processes an audio file in chunks, extracts features, builds timelines,
detects sections, and produces a FullTrackAnalysisResult.

Runs in a background thread. Supports cancellation via threading.Event.
Never blocks the playback thread.
"""

import time
import threading
import warnings
from datetime import datetime
from typing import Callable, Optional

import librosa
import numpy as np

from analysis_contracts import AnalysisStatus, FullTrackAnalysisResult
from feature_extractor_frames import extract_frame_features
from tempo_timeline import build_tempo_timeline
from key_timeline import build_key_timeline
from section_detector import detect_sections
from analysis_store import AnalysisStore

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Defaults ──
DEFAULT_SR = 22050
DEFAULT_CHUNK_S = 5.0  # seconds per analysis window
CHECKPOINT_INTERVAL = 5  # checkpoint every N chunks


def run_analysis(
    filepath: str,
    result: FullTrackAnalysisResult,
    cancel_event: threading.Event,
    log_fn: Optional[Callable[[str], None]] = None,
    chunk_duration_s: float = DEFAULT_CHUNK_S,
    sample_rate: int = DEFAULT_SR,
    store: Optional[AnalysisStore] = None,
) -> None:
    """Run full-track analysis on an audio file.

    Modifies `result` in-place with progress, partial, and final data.
    Checks `cancel_event` between chunks for cooperative cancellation.

    Args:
        filepath:         Path to audio file.
        result:           FullTrackAnalysisResult to populate (shared with manager).
        cancel_event:     threading.Event — set to request cancellation.
        log_fn:           Optional logging callback.
        chunk_duration_s: Duration of each analysis chunk in seconds.
        sample_rate:      Sample rate for analysis.
        store:            Optional AnalysisStore for checkpointing.
    """

    def _log(msg: str) -> None:
        if log_fn:
            log_fn(msg)

    track_id = result.track_id
    t0 = time.perf_counter()

    _log(f"WORKER_START: {track_id} file={filepath}")
    result.status = AnalysisStatus.RUNNING.value
    result.phase = "loading"
    result.sample_rate = sample_rate

    # ── Phase 1: Load audio ──
    try:
        y, sr = librosa.load(filepath, sr=sample_rate, mono=True)
    except Exception as exc:
        result.status = AnalysisStatus.FAILED.value
        result.error = f"LOAD_FAIL: {exc}"
        _log(f"WORKER_FAIL: {track_id} — {result.error}")
        return

    duration_s = len(y) / sr
    result.duration_s = duration_s

    if duration_s < 2.0:
        result.status = AnalysisStatus.FAILED.value
        result.error = "TOO_SHORT: track is under 2 seconds"
        _log(f"WORKER_FAIL: {track_id} — {result.error}")
        return

    _log(f"LOADED: {track_id} duration={duration_s:.1f}s samples={len(y)}")

    # ── Phase 2: Chunk and extract features ──
    result.phase = "features"
    chunk_samples = int(chunk_duration_s * sr)
    total_chunks = max(1, int(np.ceil(len(y) / chunk_samples)))
    result.chunk_count = total_chunks
    result.chunks_completed = 0

    frame_features: list[dict] = []
    all_beat_intervals: list[float] = []

    for i in range(total_chunks):
        # ── Cancellation check ──
        if cancel_event.is_set():
            _log(f"CANCELLED at chunk {i}/{total_chunks}: {track_id}")
            result.status = AnalysisStatus.CANCELLED.value
            return

        # Extract chunk
        start_sample = i * chunk_samples
        end_sample = min(start_sample + chunk_samples, len(y))
        y_chunk = y[start_sample:end_sample]
        start_s = start_sample / sr

        # Extract features
        ff = extract_frame_features(y_chunk, sr, i, start_s)
        ff_dict = ff.to_dict()
        frame_features.append(ff_dict)
        all_beat_intervals.extend(ff.beat_intervals)

        # Update progress
        result.chunks_completed = i + 1
        result.progress = round((i + 1) / total_chunks * 100.0, 1)
        result.frame_features = frame_features

        # Checkpoint periodically
        if store and (i + 1) % CHECKPOINT_INTERVAL == 0:
            store.save_checkpoint(result)
            _log(f"CHECKPOINT: {track_id} chunk={i + 1}/{total_chunks} progress={result.progress}%")

    _log(f"FEATURES_DONE: {track_id} chunks={total_chunks}")

    # ── Phase 3: Build tempo timeline ──
    result.phase = "tempo"
    if cancel_event.is_set():
        result.status = AnalysisStatus.CANCELLED.value
        return

    tempo_result = build_tempo_timeline(frame_features, all_beat_intervals)
    result.tempo_timeline = tempo_result["tempo_timeline"]
    result.final_bpm = tempo_result["final_bpm"]
    result.bpm_confidence = tempo_result["bpm_confidence"]
    result.bpm_family = tempo_result["bpm_family"]
    result.bpm_candidates = tempo_result["bpm_candidates"]
    _log(f"TEMPO_DONE: {track_id} bpm={result.final_bpm} conf={result.bpm_confidence}")

    # ── Phase 4: Build key timeline ──
    result.phase = "key"
    if cancel_event.is_set():
        result.status = AnalysisStatus.CANCELLED.value
        return

    key_result = build_key_timeline(frame_features)
    result.key_timeline = key_result["key_timeline"]
    result.final_key = key_result["final_key"]
    result.final_key_name = key_result["final_key_name"]
    result.key_confidence = key_result["key_confidence"]
    result.key_change_detected = key_result["key_change_detected"]
    _log(f"KEY_DONE: {track_id} key={result.final_key} ({result.final_key_name}) conf={result.key_confidence}")

    # ── Phase 5: Detect sections ──
    result.phase = "sections"
    if cancel_event.is_set():
        result.status = AnalysisStatus.CANCELLED.value
        return

    sections = detect_sections(frame_features, duration_s)
    result.sections = sections
    _log(f"SECTIONS_DONE: {track_id} count={len(sections)}")

    # ── Phase 6: Generate cues ──
    result.phase = "cues"
    cues: list[dict] = []
    if sections:
        # Mark energy peaks as potential drops
        energies = [s.get("energy", 0.0) for s in sections]
        if energies:
            max_e = max(energies)
            for s in sections:
                if max_e > 0 and s.get("energy", 0.0) > max_e * 0.9:
                    cues.append({
                        "time_s": round(s["start_s"], 3),
                        "type": "drop",
                        "label": f"High energy @ {s['start_s']:.1f}s",
                    })
                if s.get("label") == "bridge":
                    cues.append({
                        "time_s": round(s["start_s"], 3),
                        "type": "breakdown",
                        "label": f"Bridge @ {s['start_s']:.1f}s",
                    })
    result.cues = cues

    # ── Phase 7: Finalize ──
    result.phase = "done"
    result.progress = 100.0
    elapsed = time.perf_counter() - t0
    result.processing_time_s = elapsed

    # Determine readiness
    reasons: list[str] = []
    if result.bpm_confidence < 0.5:
        reasons.append(f"Low BPM confidence ({result.bpm_confidence:.2f})")
    if result.key_confidence < 0.4:
        reasons.append(f"Low key confidence ({result.key_confidence:.2f})")
    if len(sections) < 2:
        reasons.append(f"Few sections detected ({len(sections)})")
    if result.key_change_detected:
        reasons.append("Key change detected")

    result.review_required = len(reasons) > 0
    result.review_reason = "; ".join(reasons) if reasons else ""
    result.analyzer_ready = not result.review_required or result.bpm_confidence >= 0.3

    # Save final result
    if store:
        store.save_result(result)

    _log(
        f"WORKER_DONE: {track_id} bpm={result.final_bpm} key={result.final_key} "
        f"sections={len(sections)} time={elapsed:.2f}s "
        f"ready={result.analyzer_ready} review={result.review_required}"
    )
