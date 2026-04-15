"""
NGKsPlayerNative — Feature Extractor
Extracts onset, beat, tempo histogram, and chroma features from audio files.
Uses librosa. Deterministic (fixed random seed where applicable).
"""

import numpy as np
import librosa
import warnings
from dataclasses import dataclass, field

# Suppress librosa deprecation / UserWarning noise
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── Constants ──────────────────────────────────────────────────────────
SR = 22050          # Sample rate for analysis
HOP_LENGTH = 512    # Hop length for STFT / onset
N_FFT = 2048        # FFT window
DURATION_LIMIT = 180  # Analyse first 3 min max to keep runtime sane


@dataclass
class FeatureResult:
    """All extracted features for a single track."""
    # Beat / tempo
    beat_times: np.ndarray = field(default_factory=lambda: np.array([]))
    beat_intervals: np.ndarray = field(default_factory=lambda: np.array([]))
    beat_interval_std: float = 0.0

    # Tempo histogram peaks (BPM values)
    tempo_peak1: float = 0.0
    tempo_peak2: float = 0.0
    tempo_peak3: float = 0.0
    tempo_peak4: float = 0.0
    tempo_peak5: float = 0.0
    tempo_peak6: float = 0.0
    tempo_peak7: float = 0.0
    tempo_peak8: float = 0.0
    tempo_peak_strength1: float = 0.0
    tempo_peak_strength2: float = 0.0
    tempo_peak_strength3: float = 0.0
    tempo_peak_strength4: float = 0.0
    tempo_peak_strength5: float = 0.0
    tempo_peak_strength6: float = 0.0
    tempo_peak_strength7: float = 0.0
    tempo_peak_strength8: float = 0.0

    # Median-IBI derived BPM (bypass tempogram, use beat tracker directly)
    median_ibi_bpm: float = 0.0

    # Beat-track global tempo estimate (librosa's own BPM guess)
    beat_track_tempo: float = 0.0

    # Percussive-only beat tracking (HPSS separated, different from main beat tracker)
    percussive_median_ibi_bpm: float = 0.0

    # Multi-resolution tempogram peaks (hop=1024 for finer BPM resolution at low tempos)
    alt_tempo_peak1: float = 0.0
    alt_tempo_peak2: float = 0.0
    alt_tempo_peak3: float = 0.0
    alt_tempo_peak_strength1: float = 0.0
    alt_tempo_peak_strength2: float = 0.0
    alt_tempo_peak_strength3: float = 0.0

    # Meter / downbeat
    estimated_meter: int = 4
    downbeat_confidence: float = 0.0

    # Onset
    onset_density: float = 0.0
    hf_percussive_score: float = 0.0

    # Chroma (12 bins: C, C#, D, D#, E, F, F#, G, G#, A, A#, B)
    chroma: np.ndarray = field(default_factory=lambda: np.zeros(12))
    tonal_clarity: float = 0.0

    # Chroma per segment (for modulation detection)
    chroma_segments: list = field(default_factory=list)

    # Key evidence features (Phase 1 key calibration)
    bass_chroma: np.ndarray = field(default_factory=lambda: np.zeros(12))
    harmonic_stability: float = 0.0
    section_chroma_variance: float = 0.0

    # Error info
    error: str = ""

    # Tempogram (for debug)
    tempogram_bpms: np.ndarray = field(default_factory=lambda: np.array([]))
    tempogram_strengths: np.ndarray = field(default_factory=lambda: np.array([]))


def extract_features(filepath: str) -> FeatureResult:
    """Extract all features from an audio file. Fail-closed: returns result with error field set."""
    result = FeatureResult()

    try:
        y, sr = librosa.load(filepath, sr=SR, mono=True, duration=DURATION_LIMIT)
    except Exception as e:
        result.error = f"LOAD_FAIL: {e}"
        return result

    if len(y) < SR * 2:  # less than 2 seconds
        result.error = "TOO_SHORT"
        return result

    try:
        _extract_beats_tempo(y, int(sr), result)
        _extract_chroma(y, int(sr), result)
        _extract_onset_percussive(y, int(sr), result)
    except Exception as e:
        result.error = f"EXTRACT_FAIL: {e}"

    return result


def _extract_beats_tempo(y: np.ndarray, sr: int, result: FeatureResult):
    """Beat tracking, inter-beat intervals, tempo histogram, meter estimation."""
    # ── Onset envelope ──
    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=HOP_LENGTH)

    # ── Beat tracking ──
    tempo_estimate, beat_frames = librosa.beat.beat_track(
        onset_envelope=onset_env, sr=sr, hop_length=HOP_LENGTH
    )
    beat_times = librosa.frames_to_time(beat_frames, sr=sr, hop_length=HOP_LENGTH)
    result.beat_times = beat_times

    # Store the librosa global tempo estimate
    te = np.asarray(tempo_estimate)
    if te.ndim > 0 and te.size > 0:
        result.beat_track_tempo = float(te.flat[0])
    elif te.ndim == 0:
        result.beat_track_tempo = float(te)
    else:
        result.beat_track_tempo = 0.0

    # ── Inter-beat intervals ──
    if len(beat_times) > 1:
        ibi = np.diff(beat_times)
        ibi = ibi[ibi > 0.15]  # discard intervals < 150ms (> 400 BPM) as noise
        if len(ibi) > 0:
            result.beat_intervals = ibi
            result.beat_interval_std = float(np.std(ibi))
            # Median-IBI derived BPM: bypasses tempogram, uses beat tracker directly
            median_ibi = float(np.median(ibi))
            if median_ibi > 0:
                result.median_ibi_bpm = round(60.0 / median_ibi, 2)

    # ── Tempogram (autocorrelation-based) ──
    tempogram = librosa.feature.tempogram(
        onset_envelope=onset_env, sr=sr, hop_length=HOP_LENGTH
    )
    # Average across time
    avg_tempogram = np.mean(tempogram, axis=1)

    # Convert lag indices to BPM
    n_lags = len(avg_tempogram)
    bpms = np.zeros(n_lags)
    for i in range(1, n_lags):
        lag_seconds = i * HOP_LENGTH / sr
        bpms[i] = 60.0 / lag_seconds if lag_seconds > 0 else 0

    # Only consider 30–300 BPM range
    valid_mask = (bpms >= 30) & (bpms <= 300)
    valid_bpms = bpms[valid_mask]
    valid_strengths = avg_tempogram[valid_mask]

    result.tempogram_bpms = valid_bpms
    result.tempogram_strengths = valid_strengths

    if len(valid_strengths) > 0:
        # Normalize strengths
        max_s = np.max(valid_strengths)
        if max_s > 0:
            norm_strengths = valid_strengths / max_s
        else:
            norm_strengths = valid_strengths

        # Find peaks
        sorted_idx = np.argsort(norm_strengths)[::-1]

        # Pick top 8 unique peaks (at least 5 BPM apart)
        peaks_bpm = []
        peaks_str = []
        for idx in sorted_idx:
            candidate_bpm = valid_bpms[idx]
            candidate_str = float(norm_strengths[idx])
            if candidate_bpm < 30:
                continue
            # Check distance from already-picked peaks
            too_close = False
            for pb in peaks_bpm:
                if abs(candidate_bpm - pb) < 5:
                    too_close = True
                    break
            if not too_close:
                peaks_bpm.append(float(candidate_bpm))
                peaks_str.append(candidate_str)
                if len(peaks_bpm) >= 8:
                    break

        for i, (bpm, strength) in enumerate(zip(peaks_bpm, peaks_str)):
            setattr(result, f"tempo_peak{i+1}", bpm)
            setattr(result, f"tempo_peak_strength{i+1}", strength)

    # ── Multi-resolution tempogram (hop=1024 for finer BPM resolution) ──
    # Higher hop length gives finer temporal resolution → different BPM lags
    # that can resolve tempos missed by the default hop (e.g., 90.7 vs 76/152)
    try:
        alt_hop = 1024
        alt_onset = librosa.onset.onset_strength(y=y, sr=sr, hop_length=alt_hop)
        alt_tg = librosa.feature.tempogram(onset_envelope=alt_onset, sr=sr, hop_length=alt_hop)
        alt_avg = np.mean(alt_tg, axis=1)
        n_alt = len(alt_avg)
        alt_bpms = np.zeros(n_alt)
        for i in range(1, n_alt):
            alt_bpms[i] = 60.0 * sr / (i * alt_hop)
        alt_mask = (alt_bpms >= 30) & (alt_bpms <= 300)
        alt_v_bpms = alt_bpms[alt_mask]
        alt_v_str = alt_avg[alt_mask]
        if len(alt_v_str) > 0:
            alt_max = np.max(alt_v_str)
            if alt_max > 0:
                alt_norm = alt_v_str / alt_max
            else:
                alt_norm = alt_v_str
            alt_sorted = np.argsort(alt_norm)[::-1]
            alt_peaks_bpm = []
            alt_peaks_str = []
            for idx in alt_sorted:
                cbpm = alt_v_bpms[idx]
                cstr = float(alt_norm[idx])
                if cbpm < 30:
                    continue
                too_close = False
                for pb in alt_peaks_bpm:
                    if abs(cbpm - pb) < 5:
                        too_close = True
                        break
                if not too_close:
                    alt_peaks_bpm.append(float(cbpm))
                    alt_peaks_str.append(cstr)
                    if len(alt_peaks_bpm) >= 3:
                        break
            for i, (bpm, strength) in enumerate(zip(alt_peaks_bpm, alt_peaks_str)):
                setattr(result, f"alt_tempo_peak{i+1}", bpm)
                setattr(result, f"alt_tempo_peak_strength{i+1}", strength)
    except Exception:
        pass  # Non-critical

    # ── Meter estimation ──
    if len(result.beat_intervals) >= 8:
        # Group beats and test 2, 3, 4 grouping hypotheses
        ibi = result.beat_intervals
        median_ibi = np.median(ibi)

        best_meter = 4
        best_score = 0.0

        for meter in [2, 3, 4]:
            # Expected strong-beat interval
            expected_strong = median_ibi * meter
            # Check if onset envelope has energy peaks at meter intervals
            if len(onset_env) > 0:
                # Sample onset envelope at expected strong-beat positions
                strong_times = np.arange(0, beat_times[-1], expected_strong)
                if len(strong_times) > 1:
                    strong_frames = librosa.time_to_frames(
                        strong_times, sr=sr, hop_length=HOP_LENGTH
                    )
                    strong_frames = strong_frames[strong_frames < len(onset_env)]
                    if len(strong_frames) > 0:
                        score = float(np.mean(onset_env[strong_frames]))
                        if score > best_score:
                            best_score = score
                            best_meter = meter

        result.estimated_meter = best_meter

        # Downbeat confidence: ratio of strong-beat energy to average
        avg_onset = float(np.mean(onset_env)) if len(onset_env) > 0 else 1.0
        result.downbeat_confidence = min(1.0, best_score / avg_onset) if avg_onset > 0 else 0.0
    else:
        result.estimated_meter = 4
        result.downbeat_confidence = 0.0

    # ── Percussive-only beat tracking (HPSS) ──
    # Separates harmonic/percussive components and runs beat tracking on
    # percussion only. Can surface different tempo for melodic-heavy tracks
    # (trip-hop, ballads) where harmonic content confuses the main tracker.
    try:
        y_harmonic, y_percussive = librosa.effects.hpss(y)
        perc_onset = librosa.onset.onset_strength(y=y_percussive, sr=sr, hop_length=HOP_LENGTH)
        _, perc_beat_frames = librosa.beat.beat_track(
            onset_envelope=perc_onset, sr=sr, hop_length=HOP_LENGTH
        )
        perc_beat_times = librosa.frames_to_time(perc_beat_frames, sr=sr, hop_length=HOP_LENGTH)
        if len(perc_beat_times) > 1:
            perc_ibi = np.diff(perc_beat_times)
            perc_ibi = perc_ibi[perc_ibi > 0.15]
            if len(perc_ibi) > 0:
                result.percussive_median_ibi_bpm = round(60.0 / float(np.median(perc_ibi)), 2)
    except Exception:
        pass  # Non-critical — if HPSS fails, skip gracefully


def _extract_chroma(y: np.ndarray, sr: int, result: FeatureResult):
    """Chroma feature extraction (12 pitch classes) + segmented chroma for modulation."""
    # Full-track chroma (STFT-based — much faster than CQT)
    chroma = librosa.feature.chroma_stft(y=y, sr=sr, hop_length=HOP_LENGTH, n_fft=N_FFT)
    avg_chroma = np.mean(chroma, axis=1)  # shape (12,)

    # Normalize so max = 1
    max_c = np.max(avg_chroma)
    if max_c > 0:
        avg_chroma = avg_chroma / max_c

    result.chroma = avg_chroma

    # Tonal clarity: how peaked is the chroma distribution?
    # High clarity = one dominant pitch class.  Low = flat.
    if np.sum(avg_chroma) > 0:
        # Use normalized entropy: 0 = perfectly peaked, 1 = flat
        p = avg_chroma / np.sum(avg_chroma)
        entropy = -np.sum(p * np.log2(p + 1e-12))
        max_entropy = np.log2(12)
        result.tonal_clarity = float(1.0 - entropy / max_entropy)
    else:
        result.tonal_clarity = 0.0

    # Segmented chroma (4 equal segments) for modulation detection
    n_frames = chroma.shape[1]
    n_segments = 4
    seg_size = n_frames // n_segments
    if seg_size > 0:
        for i in range(n_segments):
            seg = chroma[:, i * seg_size:(i + 1) * seg_size]
            seg_avg = np.mean(seg, axis=1)
            seg_max = np.max(seg_avg)
            if seg_max > 0:
                seg_avg = seg_avg / seg_max
            result.chroma_segments.append(seg_avg)

    # ── Key evidence features (Phase 1 key calibration) ──

    # Bass chroma: pitch class energy from low frequencies only (<300 Hz)
    # Bass notes strongly indicate the tonic, helping disambiguate relative
    # major/minor pairs and parallel keys.
    try:
        freq_bins = librosa.fft_frequencies(sr=sr, n_fft=N_FFT)
        S_full = np.abs(librosa.stft(y, n_fft=N_FFT, hop_length=HOP_LENGTH))
        # Zero out everything above 300 Hz
        bass_mask = freq_bins <= 300
        S_bass = np.zeros_like(S_full)
        S_bass[bass_mask, :] = S_full[bass_mask, :]
        # Compute chroma from this bass-only spectrogram
        bass_chroma = librosa.feature.chroma_stft(
            S=S_bass ** 2, sr=sr, hop_length=HOP_LENGTH, n_fft=N_FFT
        )
        avg_bass = np.mean(bass_chroma, axis=1)
        bmax = np.max(avg_bass)
        if bmax > 0:
            avg_bass = avg_bass / bmax
        result.bass_chroma = avg_bass
    except Exception:
        pass  # Non-critical

    # Harmonic stability: consistency of chroma profile across time windows
    # Low stability = key changes or tonal ambiguity across the track
    if len(result.chroma_segments) >= 2:
        seg_stack = np.array(result.chroma_segments)  # shape (n_seg, 12)
        # Mean std across pitch classes and segments
        result.harmonic_stability = 1.0 - float(np.mean(np.std(seg_stack, axis=0)))
    else:
        result.harmonic_stability = 0.5  # unknown

    # Section chroma variance: how much the tonal content shifts between segments
    # High variance = possible modulation or tonal instability
    if len(result.chroma_segments) >= 2:
        seg_stack = np.array(result.chroma_segments)
        result.section_chroma_variance = float(np.mean(np.var(seg_stack, axis=0)))


def _extract_onset_percussive(y: np.ndarray, sr: int, result: FeatureResult):
    """Onset density and HF percussive score (lightweight — no HPSS)."""
    onset_frames = librosa.onset.onset_detect(y=y, sr=sr, hop_length=HOP_LENGTH)
    duration = len(y) / sr
    result.onset_density = float(len(onset_frames) / duration) if duration > 0 else 0.0

    # HF percussive score: ratio of high-frequency energy (>4kHz) to total
    # Much faster than HPSS decomposition, good enough for scoring
    S = np.abs(librosa.stft(y, n_fft=N_FFT, hop_length=HOP_LENGTH))
    freq_bins = librosa.fft_frequencies(sr=sr, n_fft=N_FFT)
    hf_mask = freq_bins >= 4000
    hf_energy = float(np.sum(S[hf_mask, :] ** 2))
    total_energy = float(np.sum(S ** 2))
    result.hf_percussive_score = hf_energy / total_energy if total_energy > 0 else 0.0
