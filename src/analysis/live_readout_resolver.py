"""
NGKsPlayerNative — Live Readout Resolver
Resolves current BPM / Key / Section from analysis timelines
at a given playback position. Pure lookup — no DSP.

Thread-safe. O(log n) per resolve. Safe for 8+ Hz polling.
"""

import bisect
import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger("analysis.live_readout")

LOW_CONFIDENCE_THRESHOLD = 0.5


class ReadoutState(Enum):
    NO_TRACK = "NO_TRACK"
    NO_ANALYSIS = "NO_ANALYSIS"
    ANALYSIS_RUNNING_NO_TIMELINE = "ANALYSIS_RUNNING_NO_TIMELINE"
    ANALYSIS_RUNNING_WITH_PARTIAL_TIMELINE = "ANALYSIS_RUNNING_WITH_PARTIAL_TIMELINE"
    LIVE_READOUT_AVAILABLE = "LIVE_READOUT_AVAILABLE"
    LIVE_READOUT_LOW_CONFIDENCE = "LIVE_READOUT_LOW_CONFIDENCE"
    ANALYSIS_FAILED = "ANALYSIS_FAILED"
    ANALYSIS_CANCELED = "ANALYSIS_CANCELED"


@dataclass
class LiveReadoutSnapshot:
    """Frozen snapshot of current playback-linked readout values."""
    playback_time_s: float = 0.0
    state: str = ReadoutState.NO_TRACK.value
    reason: str = "No track loaded"

    # Current values (playhead-resolved)
    current_bpm: float = 0.0
    current_bpm_confidence: float = 0.0
    current_bpm_label: str = ""
    current_key: str = ""
    current_key_name: str = ""
    current_key_confidence: float = 0.0
    current_section_label: str = ""
    current_section_index: int = -1
    current_section_start_s: float = 0.0
    current_section_end_s: float = 0.0

    # Global values (song-level, for comparison display)
    global_bpm: float = 0.0
    global_bpm_confidence: float = 0.0
    global_key: str = ""
    global_key_name: str = ""
    global_key_confidence: float = 0.0
    section_count: int = 0
    duration_s: float = 0.0

    # Flags
    bpm_is_fallback: bool = False
    key_is_fallback: bool = False

    def to_dict(self) -> dict:
        return {
            "playback_time_s": round(self.playback_time_s, 3),
            "state": self.state,
            "reason": self.reason,
            "current_bpm": round(self.current_bpm, 2),
            "current_bpm_confidence": round(self.current_bpm_confidence, 3),
            "current_bpm_label": self.current_bpm_label,
            "current_key": self.current_key,
            "current_key_name": self.current_key_name,
            "current_key_confidence": round(self.current_key_confidence, 3),
            "current_section_label": self.current_section_label,
            "current_section_index": self.current_section_index,
            "current_section_start_s": round(self.current_section_start_s, 3),
            "current_section_end_s": round(self.current_section_end_s, 3),
            "global_bpm": round(self.global_bpm, 2),
            "global_bpm_confidence": round(self.global_bpm_confidence, 3),
            "global_key": self.global_key,
            "global_key_name": self.global_key_name,
            "global_key_confidence": round(self.global_key_confidence, 3),
            "section_count": self.section_count,
            "duration_s": round(self.duration_s, 3),
            "bpm_is_fallback": self.bpm_is_fallback,
            "key_is_fallback": self.key_is_fallback,
        }


class LiveReadoutResolver:
    """Resolves current values from analysis timelines at a given playback time.

    Usage:
        resolver = LiveReadoutResolver()
        resolver.bind_result(analysis_result_dict)
        snap = resolver.resolve(playback_time_s=45.2)

    Thread-safe. All public methods acquire _lock.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._track_id: Optional[str] = None
        self._generation: int = 0

        # Sorted timeline caches (pre-built on bind)
        self._tempo_times: list[float] = []
        self._tempo_points: list[dict] = []
        self._key_times: list[float] = []
        self._key_points: list[dict] = []
        self._sections: list[dict] = []

        # Global values
        self._global_bpm: float = 0.0
        self._global_bpm_confidence: float = 0.0
        self._global_key: str = ""
        self._global_key_name: str = ""
        self._global_key_confidence: float = 0.0
        self._duration_s: float = 0.0

        # Analysis state
        self._analysis_status: Optional[str] = None
        self._has_tempo_timeline: bool = False
        self._has_key_timeline: bool = False
        self._has_sections: bool = False

    # ──────────────────────────────────────────────
    #  BINDING
    # ──────────────────────────────────────────────

    def bind_result(self, result: dict, track_id: str, generation: int) -> None:
        """Bind an analysis result for timeline lookups.

        Pre-sorts timelines for O(log n) bisect lookups.
        """
        with self._lock:
            self._track_id = track_id
            self._generation = generation
            self._analysis_status = result.get("status")

            # Global values
            self._global_bpm = result.get("final_bpm", 0.0)
            self._global_bpm_confidence = result.get("bpm_confidence", 0.0)
            self._global_key = result.get("final_key", "")
            self._global_key_name = result.get("final_key_name", "")
            self._global_key_confidence = result.get("key_confidence", 0.0)
            self._duration_s = result.get("duration_s", 0.0)

            # Build sorted tempo timeline
            raw_tempo = result.get("tempo_timeline", [])
            if raw_tempo:
                sorted_t = sorted(raw_tempo, key=lambda p: p.get("time_s", 0.0))
                self._tempo_times = [p.get("time_s", 0.0) for p in sorted_t]
                self._tempo_points = sorted_t
                self._has_tempo_timeline = True
            else:
                self._tempo_times = []
                self._tempo_points = []
                self._has_tempo_timeline = False

            # Build sorted key timeline
            raw_key = result.get("key_timeline", [])
            if raw_key:
                sorted_k = sorted(raw_key, key=lambda p: p.get("time_s", 0.0))
                self._key_times = [p.get("time_s", 0.0) for p in sorted_k]
                self._key_points = sorted_k
                self._has_key_timeline = True
            else:
                self._key_times = []
                self._key_points = []
                self._has_key_timeline = False

            # Sections (already sorted by start_s typically)
            raw_sections = result.get("sections", [])
            if raw_sections:
                self._sections = sorted(raw_sections, key=lambda s: s.get("start_s", 0.0))
                self._has_sections = True
            else:
                self._sections = []
                self._has_sections = False

        logger.info(
            "BIND track_id=%s gen=%d tempo=%d key=%d sections=%d",
            track_id, generation,
            len(self._tempo_points), len(self._key_points), len(self._sections),
        )

    def unbind(self) -> None:
        """Unbind — reset all state."""
        with self._lock:
            self._track_id = None
            self._generation = 0
            self._tempo_times = []
            self._tempo_points = []
            self._key_times = []
            self._key_points = []
            self._sections = []
            self._global_bpm = 0.0
            self._global_bpm_confidence = 0.0
            self._global_key = ""
            self._global_key_name = ""
            self._global_key_confidence = 0.0
            self._duration_s = 0.0
            self._analysis_status = None
            self._has_tempo_timeline = False
            self._has_key_timeline = False
            self._has_sections = False

    # ──────────────────────────────────────────────
    #  RESOLVE
    # ──────────────────────────────────────────────

    def resolve(self, playback_time_s: float, generation: int = 0) -> LiveReadoutSnapshot:
        """Resolve current values at a given playback time.

        Returns a frozen LiveReadoutSnapshot.
        O(log n) — safe for frequent polling.
        """
        with self._lock:
            # Guard: generation mismatch
            if generation > 0 and generation != self._generation:
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.NO_TRACK.value,
                    reason="Stale generation",
                )

            if self._track_id is None:
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.NO_TRACK.value,
                    reason="No track loaded",
                )

            # Check analysis status
            if self._analysis_status in ("FAILED",):
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.ANALYSIS_FAILED.value,
                    reason="Analysis failed",
                    global_bpm=self._global_bpm,
                    global_key=self._global_key,
                )

            if self._analysis_status in ("CANCELLED",):
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.ANALYSIS_CANCELED.value,
                    reason="Analysis canceled",
                )

            # Running with no data yet?
            if self._analysis_status in ("QUEUED", "RUNNING"):
                if not self._has_tempo_timeline and not self._has_key_timeline:
                    return LiveReadoutSnapshot(
                        playback_time_s=playback_time_s,
                        state=ReadoutState.ANALYSIS_RUNNING_NO_TIMELINE.value,
                        reason="Analysis in progress — no timeline data yet",
                    )
                # Has partial data
                snap = self._resolve_at(playback_time_s)
                snap.state = ReadoutState.ANALYSIS_RUNNING_WITH_PARTIAL_TIMELINE.value
                snap.reason = "Analysis in progress — partial timeline available"
                return snap

            # No analysis at all
            if self._analysis_status is None or self._analysis_status == "NOT_FOUND":
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.NO_ANALYSIS.value,
                    reason="No analysis available for this track",
                )

            # COMPLETED — full readout
            if not self._has_tempo_timeline and not self._has_key_timeline:
                return LiveReadoutSnapshot(
                    playback_time_s=playback_time_s,
                    state=ReadoutState.NO_ANALYSIS.value,
                    reason="Analysis completed but no timeline data",
                    global_bpm=self._global_bpm,
                    global_bpm_confidence=self._global_bpm_confidence,
                    global_key=self._global_key,
                    global_key_name=self._global_key_name,
                    global_key_confidence=self._global_key_confidence,
                    duration_s=self._duration_s,
                    section_count=len(self._sections),
                )

            snap = self._resolve_at(playback_time_s)

            # Determine confidence state
            bpm_ok = snap.current_bpm_confidence >= LOW_CONFIDENCE_THRESHOLD
            key_ok = snap.current_key_confidence >= LOW_CONFIDENCE_THRESHOLD
            if bpm_ok or key_ok:
                snap.state = ReadoutState.LIVE_READOUT_AVAILABLE.value
                snap.reason = "Live readout active"
            else:
                snap.state = ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value
                snap.reason = "Live readout active — low confidence"

            return snap

    def _resolve_at(self, t: float) -> LiveReadoutSnapshot:
        """Internal: resolve BPM/key/section at time t.

        Must be called with _lock held.
        """
        snap = LiveReadoutSnapshot(playback_time_s=t)

        # Global values
        snap.global_bpm = self._global_bpm
        snap.global_bpm_confidence = self._global_bpm_confidence
        snap.global_key = self._global_key
        snap.global_key_name = self._global_key_name
        snap.global_key_confidence = self._global_key_confidence
        snap.duration_s = self._duration_s
        snap.section_count = len(self._sections)

        # ── Resolve current BPM ──
        if self._has_tempo_timeline:
            idx = self._bisect_timeline(self._tempo_times, t)
            if idx >= 0:
                pt = self._tempo_points[idx]
                snap.current_bpm = pt.get("value", 0.0)
                snap.current_bpm_confidence = pt.get("confidence", 0.0)
                snap.current_bpm_label = pt.get("label", "")
                snap.bpm_is_fallback = False
            else:
                # Fallback to global
                snap.current_bpm = self._global_bpm
                snap.current_bpm_confidence = self._global_bpm_confidence
                snap.current_bpm_label = f"{self._global_bpm:.1f} BPM (global)"
                snap.bpm_is_fallback = True
        else:
            # No tempo timeline — use global as fallback
            if self._global_bpm > 0:
                snap.current_bpm = self._global_bpm
                snap.current_bpm_confidence = self._global_bpm_confidence
                snap.current_bpm_label = f"{self._global_bpm:.1f} BPM (global fallback)"
                snap.bpm_is_fallback = True

        # ── Resolve current Key ──
        if self._has_key_timeline:
            idx = self._bisect_timeline(self._key_times, t)
            if idx >= 0:
                pt = self._key_points[idx]
                snap.current_key = pt.get("label", "")
                snap.current_key_confidence = pt.get("confidence", 0.0)
                # Try to derive key name from label
                snap.current_key_name = ""  # timeline points don't carry names
                snap.key_is_fallback = False
            else:
                snap.current_key = self._global_key
                snap.current_key_name = self._global_key_name
                snap.current_key_confidence = self._global_key_confidence
                snap.key_is_fallback = True
        else:
            if self._global_key:
                snap.current_key = self._global_key
                snap.current_key_name = self._global_key_name
                snap.current_key_confidence = self._global_key_confidence
                snap.key_is_fallback = True

        # ── Resolve current Section ──
        if self._has_sections:
            sec = self._find_section(t)
            if sec is not None:
                snap.current_section_label = sec.get("label", "")
                snap.current_section_index = sec.get("index", -1)
                snap.current_section_start_s = sec.get("start_s", 0.0)
                snap.current_section_end_s = sec.get("end_s", 0.0)
            else:
                snap.current_section_label = ""
                snap.current_section_index = -1

        return snap

    @staticmethod
    def _bisect_timeline(times: list[float], t: float) -> int:
        """Find the index of the timeline point at or just before time t.

        Returns -1 if t is before the first point.
        Uses bisect_right for O(log n).
        """
        if not times:
            return -1
        idx = bisect.bisect_right(times, t) - 1
        if idx < 0:
            return -1
        return idx

    def _find_section(self, t: float) -> Optional[dict]:
        """Find the section containing time t.

        Uses linear scan — sections are typically <20 items.
        """
        for sec in self._sections:
            start = sec.get("start_s", 0.0)
            end = sec.get("end_s", 0.0)
            if start <= t < end:
                return sec
        # If past last section, return last
        if self._sections and t >= self._sections[-1].get("start_s", 0.0):
            return self._sections[-1]
        return None

    # ──────────────────────────────────────────────
    #  PROPERTIES
    # ──────────────────────────────────────────────

    @property
    def track_id(self) -> Optional[str]:
        with self._lock:
            return self._track_id

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    @property
    def has_tempo_timeline(self) -> bool:
        with self._lock:
            return self._has_tempo_timeline

    @property
    def has_key_timeline(self) -> bool:
        with self._lock:
            return self._has_key_timeline

    @property
    def has_sections(self) -> bool:
        with self._lock:
            return self._has_sections
