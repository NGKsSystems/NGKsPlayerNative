"""
NGKsPlayerNative — Frame Feature Extractor
Extracts per-chunk DSP features from an audio window.
Uses librosa. Stateless — each call processes a single numpy chunk.
"""

import numpy as np
import librosa
import warnings

from analysis_contracts import FrameFeature

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Constants ──
DEFAULT_SR = 22050
HOP = 512
N_FFT = 2048

# Camelot wheel for numeric key codes
CAMELOT_KEYS = [
    "C", "C#", "D", "D#", "E", "F",
    "F#", "G", "G#", "A", "A#", "B",
]


def extract_frame_features(
    y_chunk: np.ndarray,
    sr: int,
    chunk_index: int,
    start_s: float,
) -> FrameFeature:
    """Extract all features from a single audio chunk.

    Args:
        y_chunk:     Audio samples (1D float32/64).
        sr:          Sample rate.
        chunk_index: Ordinal of this chunk.
        start_s:     Start time offset of this chunk in the full track.

    Returns:
        FrameFeature with all fields populated.
    """
    duration_s = len(y_chunk) / sr
    end_s = start_s + duration_s
    ff = FrameFeature(chunk_index=chunk_index, start_s=start_s, end_s=end_s)

    if len(y_chunk) < sr:  # less than 1 second — skip
        return ff

    # ── RMS energy ──
    rms = librosa.feature.rms(y=y_chunk, hop_length=HOP)[0]
    ff.rms = float(np.mean(rms))

    # ── Onset strength ──
    onset_env = librosa.onset.onset_strength(y=y_chunk, sr=sr, hop_length=HOP)
    ff.onset_strength = float(np.mean(onset_env))

    # ── Spectral flux ──
    S = np.abs(librosa.stft(y_chunk, n_fft=N_FFT, hop_length=HOP))
    flux = np.sqrt(np.mean(np.diff(S, axis=1) ** 2, axis=0))
    ff.spectral_flux = float(np.mean(flux)) if len(flux) > 0 else 0.0

    # ── Chroma ──
    chroma = librosa.feature.chroma_cqt(y=y_chunk, sr=sr, hop_length=HOP)
    chroma_mean = np.mean(chroma, axis=1)
    cmax = np.max(chroma_mean)
    if cmax > 0:
        chroma_mean = chroma_mean / cmax
    ff.chroma = chroma_mean.tolist()

    # ── Local tempo estimate ──
    if len(y_chunk) >= sr * 2:  # need at least 2s for tempo
        try:
            tempo_arr, beat_frames = librosa.beat.beat_track(
                y=y_chunk, sr=sr, hop_length=HOP
            )
            t = np.asarray(tempo_arr)
            ff.local_tempo = float(t.flat[0]) if t.size > 0 else 0.0

            beat_times = librosa.frames_to_time(beat_frames, sr=sr, hop_length=HOP)
            ff.beat_count = len(beat_times)
            if len(beat_times) > 1:
                ibi = np.diff(beat_times)
                ff.beat_intervals = ibi[ibi > 0.15].tolist()
        except Exception:
            pass

    return ff
