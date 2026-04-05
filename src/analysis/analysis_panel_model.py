"""
NGKsPlayerNative — Analysis Panel Model
Read-only view model for the Analysis Panel UI.
Maps raw adapter state to formatted, display-ready data.
"""

from enum import Enum
from typing import Optional


class PanelState(Enum):
    """UI states for the Analysis Panel."""
    NO_TRACK = "NO_TRACK"
    NO_ANALYSIS = "NO_ANALYSIS"
    ANALYSIS_QUEUED = "ANALYSIS_QUEUED"
    ANALYSIS_RUNNING = "ANALYSIS_RUNNING"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    ANALYSIS_FAILED = "ANALYSIS_FAILED"
    ANALYSIS_CANCELED = "ANALYSIS_CANCELED"


class AnalysisPanelModel:
    """Read-only view model populated from AnalysisAppAdapter.get_panel_state().

    All fields are read-only. No mutations allowed.
    Safe to bind directly to UI labels, progress bars, and list views.
    """

    def __init__(self):
        self._state = PanelState.NO_TRACK
        self._generation: int = 0
        self._track_id: Optional[str] = None

        # Progress
        self._status_text: str = "No track loaded"
        self._progress: float = 0.0
        self._progress_text: str = ""

        # Primary fields
        self._bpm_text: str = ""
        self._key_text: str = ""
        self._duration_text: str = ""
        self._confidence_text: str = ""

        # Section list
        self._sections: list[dict] = []
        self._section_count: int = 0

        # Cues
        self._cues: list[dict] = []

        # Flags
        self._analyzer_ready: bool = False
        self._review_required: bool = False
        self._review_reason: str = ""
        self._error_text: str = ""
        self._processing_time_text: str = ""

        # ── Live readout (playhead-resolved) ──
        self._live_readout_state: str = "NO_TRACK"
        self._live_readout_reason: str = ""
        self._live_playback_time_s: float = 0.0
        self._live_bpm_text: str = ""
        self._live_bpm_confidence: float = 0.0
        self._live_bpm_is_fallback: bool = False
        self._live_key_text: str = ""
        self._live_key_confidence: float = 0.0
        self._live_key_is_fallback: bool = False
        self._live_section_label: str = ""
        self._live_section_index: int = -1
        self._live_section_time_range: str = ""

    # ──────────────────────────────────────────────────────────
    #  UPDATE FROM ADAPTER STATE
    # ──────────────────────────────────────────────────────────

    def update_from_adapter(self, panel_state: dict) -> None:
        """Refresh all fields from an adapter panel_state dict."""
        raw_state = panel_state.get("panel_state", "NO_TRACK")
        try:
            self._state = PanelState(raw_state)
        except ValueError:
            self._state = PanelState.NO_TRACK

        self._generation = panel_state.get("generation", 0)
        self._track_id = panel_state.get("track_id")
        self._progress = panel_state.get("progress", 0.0)

        data = panel_state.get("data") or {}

        # Status text
        self._status_text = _STATUS_TEXT.get(self._state, "Unknown")

        # Progress text
        if self._state == PanelState.ANALYSIS_RUNNING:
            phase = data.get("phase", "")
            chunks_done = data.get("chunks_completed", 0)
            chunks_total = data.get("chunk_count", 0)
            if phase == "loading":
                self._progress_text = "Loading audio..."
                self._status_text = "Loading audio..."
            elif chunks_total > 0:
                self._progress_text = f"{self._progress:.0f}% ({chunks_done}/{chunks_total} chunks)"
            elif phase == "tempo":
                self._progress_text = "Building tempo timeline..."
            elif phase == "key":
                self._progress_text = "Detecting key..."
            elif phase == "sections":
                self._progress_text = "Detecting sections..."
            elif phase == "cues":
                self._progress_text = "Generating cues..."
            else:
                self._progress_text = f"{self._progress:.0f}%"
        elif self._state == PanelState.ANALYSIS_COMPLETE:
            self._progress_text = "100%"
        else:
            self._progress_text = ""

        # Primary fields
        bpm = data.get("final_bpm", 0.0)
        bpm_conf = data.get("bpm_confidence", 0.0)
        self._bpm_text = f"{bpm:.1f} BPM" if bpm > 0 else ""

        key = data.get("final_key", "")
        key_name = data.get("final_key_name", "")
        key_conf = data.get("key_confidence", 0.0)
        if key and key_name:
            self._key_text = f"{key} ({key_name})"
        elif key:
            self._key_text = key
        else:
            self._key_text = ""

        dur = data.get("duration_s", 0.0)
        if dur > 0:
            mins = int(dur // 60)
            secs = dur % 60
            self._duration_text = f"{mins}:{secs:05.2f}"
        else:
            self._duration_text = ""

        # Confidence line
        conf_parts = []
        if bpm > 0:
            conf_parts.append(f"BPM conf: {bpm_conf:.1%}")
        if key:
            conf_parts.append(f"Key conf: {key_conf:.1%}")
        self._confidence_text = "  |  ".join(conf_parts)

        # Sections
        self._sections = data.get("sections", [])
        self._section_count = data.get("section_count", len(self._sections))

        # Cues
        self._cues = data.get("cues", [])

        # Flags
        self._analyzer_ready = data.get("analyzer_ready", False)
        self._review_required = data.get("review_required", False)
        self._review_reason = data.get("review_reason", "")

        # Error
        self._error_text = data.get("error", "")

        # Processing time
        pt = data.get("processing_time_s", 0.0)
        self._processing_time_text = f"{pt:.2f}s" if pt > 0 else ""

    def update_live_readout(self, readout_snapshot: dict) -> None:
        """Refresh live readout fields from a LiveReadoutSnapshot dict."""
        self._live_readout_state = readout_snapshot.get("state", "NO_TRACK")
        self._live_readout_reason = readout_snapshot.get("reason", "")
        self._live_playback_time_s = readout_snapshot.get("playback_time_s", 0.0)

        bpm = readout_snapshot.get("current_bpm", 0.0)
        self._live_bpm_confidence = readout_snapshot.get("current_bpm_confidence", 0.0)
        self._live_bpm_is_fallback = readout_snapshot.get("bpm_is_fallback", False)
        if bpm > 0:
            suffix = " (global)" if self._live_bpm_is_fallback else ""
            self._live_bpm_text = f"{bpm:.1f} BPM{suffix}"
        else:
            self._live_bpm_text = ""

        key = readout_snapshot.get("current_key", "")
        key_name = readout_snapshot.get("current_key_name", "")
        self._live_key_confidence = readout_snapshot.get("current_key_confidence", 0.0)
        self._live_key_is_fallback = readout_snapshot.get("key_is_fallback", False)
        if key and key_name:
            suffix = " (global)" if self._live_key_is_fallback else ""
            self._live_key_text = f"{key} ({key_name}){suffix}"
        elif key:
            suffix = " (global)" if self._live_key_is_fallback else ""
            self._live_key_text = f"{key}{suffix}"
        else:
            self._live_key_text = ""

        self._live_section_label = readout_snapshot.get("current_section_label", "")
        self._live_section_index = readout_snapshot.get("current_section_index", -1)
        s_start = readout_snapshot.get("current_section_start_s", 0.0)
        s_end = readout_snapshot.get("current_section_end_s", 0.0)
        if self._live_section_label:
            self._live_section_time_range = f"{s_start:.1f}s – {s_end:.1f}s"
        else:
            self._live_section_time_range = ""

    # ──────────────────────────────────────────────────────────
    #  READ-ONLY PROPERTIES
    # ──────────────────────────────────────────────────────────

    @property
    def state(self) -> PanelState:
        return self._state

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def track_id(self) -> Optional[str]:
        return self._track_id

    @property
    def status_text(self) -> str:
        return self._status_text

    @property
    def progress(self) -> float:
        return self._progress

    @property
    def progress_text(self) -> str:
        return self._progress_text

    @property
    def bpm_text(self) -> str:
        return self._bpm_text

    @property
    def key_text(self) -> str:
        return self._key_text

    @property
    def duration_text(self) -> str:
        return self._duration_text

    @property
    def confidence_text(self) -> str:
        return self._confidence_text

    @property
    def sections(self) -> list[dict]:
        return list(self._sections)

    @property
    def section_count(self) -> int:
        return self._section_count

    @property
    def cues(self) -> list[dict]:
        return list(self._cues)

    @property
    def analyzer_ready(self) -> bool:
        return self._analyzer_ready

    @property
    def review_required(self) -> bool:
        return self._review_required

    @property
    def review_reason(self) -> str:
        return self._review_reason

    @property
    def error_text(self) -> str:
        return self._error_text

    @property
    def processing_time_text(self) -> str:
        return self._processing_time_text

    # ──────────────────────────────────────────────────────────
    #  LIVE READOUT PROPERTIES (playhead-resolved, read-only)
    # ──────────────────────────────────────────────────────────

    @property
    def live_readout_state(self) -> str:
        return self._live_readout_state

    @property
    def live_readout_reason(self) -> str:
        return self._live_readout_reason

    @property
    def live_playback_time_s(self) -> float:
        return self._live_playback_time_s

    @property
    def live_bpm_text(self) -> str:
        return self._live_bpm_text

    @property
    def live_bpm_confidence(self) -> float:
        return self._live_bpm_confidence

    @property
    def live_bpm_is_fallback(self) -> bool:
        return self._live_bpm_is_fallback

    @property
    def live_key_text(self) -> str:
        return self._live_key_text

    @property
    def live_key_confidence(self) -> float:
        return self._live_key_confidence

    @property
    def live_key_is_fallback(self) -> bool:
        return self._live_key_is_fallback

    @property
    def live_section_label(self) -> str:
        return self._live_section_label

    @property
    def live_section_index(self) -> int:
        return self._live_section_index

    @property
    def live_section_time_range(self) -> str:
        return self._live_section_time_range

    # ──────────────────────────────────────────────────────────
    #  SNAPSHOT (for proof/logging)
    # ──────────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        """Return a plain-dict snapshot of all display fields."""
        return {
            "state": self._state.value,
            "generation": self._generation,
            "track_id": self._track_id,
            "status_text": self._status_text,
            "progress": self._progress,
            "progress_text": self._progress_text,
            "bpm_text": self._bpm_text,
            "key_text": self._key_text,
            "duration_text": self._duration_text,
            "confidence_text": self._confidence_text,
            "section_count": self._section_count,
            "cue_count": len(self._cues),
            "analyzer_ready": self._analyzer_ready,
            "review_required": self._review_required,
            "review_reason": self._review_reason,
            "error_text": self._error_text,
            "processing_time_text": self._processing_time_text,
            # Live readout (playhead-resolved)
            "live_readout_state": self._live_readout_state,
            "live_readout_reason": self._live_readout_reason,
            "live_playback_time_s": round(self._live_playback_time_s, 3),
            "live_bpm_text": self._live_bpm_text,
            "live_bpm_confidence": round(self._live_bpm_confidence, 3),
            "live_bpm_is_fallback": self._live_bpm_is_fallback,
            "live_key_text": self._live_key_text,
            "live_key_confidence": round(self._live_key_confidence, 3),
            "live_key_is_fallback": self._live_key_is_fallback,
            "live_section_label": self._live_section_label,
            "live_section_index": self._live_section_index,
            "live_section_time_range": self._live_section_time_range,
        }


# ──────────────────────────────────────────────────────────
#  CONSTANTS
# ──────────────────────────────────────────────────────────

_STATUS_TEXT = {
    PanelState.NO_TRACK: "No track loaded",
    PanelState.NO_ANALYSIS: "No analysis available",
    PanelState.ANALYSIS_QUEUED: "Analysis queued…",
    PanelState.ANALYSIS_RUNNING: "Analyzing…",
    PanelState.ANALYSIS_COMPLETE: "Analysis complete",
    PanelState.ANALYSIS_FAILED: "Analysis failed",
    PanelState.ANALYSIS_CANCELED: "Analysis canceled",
}
