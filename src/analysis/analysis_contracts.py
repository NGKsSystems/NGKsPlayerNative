"""
NGKsPlayerNative — Full-Track Analysis Contracts
Dataclasses defining all data structures for the background analysis module.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AnalysisStatus(Enum):
    """Lifecycle states for a full-track analysis job."""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class TimelinePoint:
    """A single time-indexed measurement in a timeline.

    Attributes:
        time_s:     Offset in seconds from track start.
        value:      The measured value (BPM or key code).
        confidence: Confidence in [0.0, 1.0].
        label:      Human-readable label (e.g. "8B" or "128.0 BPM").
    """
    time_s: float
    value: float
    confidence: float = 0.0
    label: str = ""

    def to_dict(self) -> dict:
        return {
            "time_s": round(self.time_s, 3),
            "value": round(self.value, 3) if isinstance(self.value, float) else self.value,
            "confidence": round(self.confidence, 3),
            "label": self.label,
        }


@dataclass
class SectionRecord:
    """A detected structural section of the track.

    Attributes:
        index:       Section ordinal (0-based).
        start_s:     Start time in seconds.
        end_s:       End time in seconds.
        duration_s:  Duration in seconds.
        label:       Section type label (intro, verse, chorus, bridge, outro, etc.).
        energy:      Mean RMS energy for this section (normalised 0–1).
        bpm:         Local BPM within this section.
        key:         Local key within this section (Camelot code).
        novelty:     Novelty score at section boundary.
    """
    index: int = 0
    start_s: float = 0.0
    end_s: float = 0.0
    duration_s: float = 0.0
    label: str = ""
    energy: float = 0.0
    bpm: float = 0.0
    key: str = ""
    novelty: float = 0.0

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "start_s": round(self.start_s, 3),
            "end_s": round(self.end_s, 3),
            "duration_s": round(self.duration_s, 3),
            "label": self.label,
            "energy": round(self.energy, 4),
            "bpm": round(self.bpm, 2),
            "key": self.key,
            "novelty": round(self.novelty, 4),
        }


@dataclass
class FrameFeature:
    """Per-chunk feature block extracted from a single analysis window.

    Attributes:
        chunk_index:     Ordinal of this chunk.
        start_s:         Start time in seconds.
        end_s:           End time in seconds.
        rms:             Root-mean-square energy.
        onset_strength:  Mean onset strength envelope value.
        spectral_flux:   Mean spectral flux.
        chroma:          12-bin chroma vector (C..B), normalised.
        local_tempo:     Local BPM estimate from this chunk.
        beat_count:      Number of beats detected in this chunk.
        beat_intervals:  Inter-beat intervals in seconds.
    """
    chunk_index: int = 0
    start_s: float = 0.0
    end_s: float = 0.0
    rms: float = 0.0
    onset_strength: float = 0.0
    spectral_flux: float = 0.0
    chroma: list[float] = field(default_factory=lambda: [0.0] * 12)
    local_tempo: float = 0.0
    beat_count: int = 0
    beat_intervals: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "chunk_index": self.chunk_index,
            "start_s": round(self.start_s, 3),
            "end_s": round(self.end_s, 3),
            "rms": round(self.rms, 6),
            "onset_strength": round(self.onset_strength, 6),
            "spectral_flux": round(self.spectral_flux, 6),
            "chroma": [round(c, 4) for c in self.chroma],
            "local_tempo": round(self.local_tempo, 2),
            "beat_count": self.beat_count,
            "beat_intervals": [round(b, 4) for b in self.beat_intervals],
        }


@dataclass
class FullTrackAnalysisResult:
    """Complete analysis result for a single track.

    Attributes:
        track_id:           Unique identifier (usually filename stem).
        filepath:           Absolute path to audio file.
        duration_s:         Total track duration in seconds.
        sample_rate:        Analysis sample rate.
        chunk_count:        Number of chunks processed.
        chunks_completed:   Number of chunks finished so far.
        progress:           Completion percentage 0–100.
        status:             Current analysis status.
        error:              Error message if failed.

        final_bpm:          Resolved global BPM.
        bpm_confidence:     Confidence in final BPM [0.0, 1.0].
        bpm_family:         BPM family label (e.g. "HALF", "NORMAL", "DOUBLE").
        bpm_candidates:     Top BPM candidates considered.

        final_key:          Resolved global key (Camelot code, e.g. "8B").
        final_key_name:     Musical key name (e.g. "C major").
        key_confidence:     Confidence in final key [0.0, 1.0].
        key_change_detected: Whether a key change was detected.

        tempo_timeline:     Time-indexed BPM measurements.
        key_timeline:       Time-indexed key measurements.
        sections:           Detected structural sections.
        cues:               Notable time points (drops, builds, breakdowns).

        frame_features:     Per-chunk feature data.

        analyzer_ready:     True if result is usable for export/display.
        review_required:    True if confidence is low or anomalies detected.
        review_reason:      Why review is needed.

        processing_time_s:  Wall-clock seconds spent analysing.
        started_at:         ISO timestamp of analysis start.
        completed_at:       ISO timestamp of analysis completion.
    """
    track_id: str = ""
    filepath: str = ""
    duration_s: float = 0.0
    sample_rate: int = 22050
    chunk_count: int = 0
    chunks_completed: int = 0
    progress: float = 0.0
    phase: str = ""  # e.g. "loading", "features", "tempo", "key", "sections"
    status: str = AnalysisStatus.QUEUED.value
    error: str = ""

    final_bpm: float = 0.0
    bpm_confidence: float = 0.0
    bpm_family: str = ""
    bpm_candidates: list[dict] = field(default_factory=list)

    final_key: str = ""
    final_key_name: str = ""
    key_confidence: float = 0.0
    key_change_detected: bool = False

    tempo_timeline: list[dict] = field(default_factory=list)
    key_timeline: list[dict] = field(default_factory=list)
    sections: list[dict] = field(default_factory=list)
    cues: list[dict] = field(default_factory=list)

    frame_features: list[dict] = field(default_factory=list)

    analyzer_ready: bool = False
    review_required: bool = False
    review_reason: str = ""

    processing_time_s: float = 0.0
    started_at: str = ""
    completed_at: str = ""

    def to_dict(self) -> dict:
        return {
            "track_id": self.track_id,
            "filepath": self.filepath,
            "duration_s": round(self.duration_s, 3),
            "sample_rate": self.sample_rate,
            "chunk_count": self.chunk_count,
            "chunks_completed": self.chunks_completed,
            "progress": round(self.progress, 1),
            "phase": self.phase,
            "status": self.status,
            "error": self.error,
            "final_bpm": round(self.final_bpm, 2),
            "bpm_confidence": round(self.bpm_confidence, 3),
            "bpm_family": self.bpm_family,
            "bpm_candidates": self.bpm_candidates,
            "final_key": self.final_key,
            "final_key_name": self.final_key_name,
            "key_confidence": round(self.key_confidence, 3),
            "key_change_detected": self.key_change_detected,
            "tempo_timeline": self.tempo_timeline,
            "key_timeline": self.key_timeline,
            "sections": self.sections,
            "cues": self.cues,
            "frame_features": self.frame_features,
            "analyzer_ready": self.analyzer_ready,
            "review_required": self.review_required,
            "review_reason": self.review_reason,
            "processing_time_s": round(self.processing_time_s, 3),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }
