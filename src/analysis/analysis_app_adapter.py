"""
NGKsPlayerNative — Analysis App Adapter
Thin bridge between the desktop app and FullTrackAnalysisManager + AnalysisStore.
Manages active-track binding, generation counters, and read-only panel state.
Thread-safe. Never blocks the playback thread.
"""

import logging
import os
import threading
import time
from typing import Optional

from analysis_contracts import AnalysisStatus, FullTrackAnalysisResult
from analysis_store import AnalysisStore
from full_track_analysis_manager import FullTrackAnalysisManager

logger = logging.getLogger("analysis.app_adapter")


class AnalysisAppAdapter:
    """Coordinates analysis lifecycle from the app's perspective.

    Responsibilities:
    - on_track_selected(): bind panel to a track, start analysis if needed
    - get_panel_state(): return a read-only snapshot for the UI panel
    - Generation counter prevents cross-track data bleed
    - Cached results served instantly (no re-analysis)
    - Playback thread is NEVER blocked
    """

    def __init__(
        self,
        manager: FullTrackAnalysisManager,
        store: AnalysisStore,
    ):
        self._manager = manager
        self._store = store
        self._lock = threading.Lock()

        # Active track binding
        self._active_track_id: Optional[str] = None
        self._active_filepath: Optional[str] = None
        self._generation: int = 0

        # Cached panel snapshots
        self._panel_cache: dict[str, dict] = {}

        self._log_lines: list[str] = []

    # ──────────────────────────────────────────────────────────
    #  APP EVENTS
    # ──────────────────────────────────────────────────────────

    def on_track_selected(self, filepath: str) -> dict:
        """Called when the user selects / loads a track.

        - Derives track_id from filepath stem
        - Increments generation counter
        - Checks cache/store for existing result
        - Starts analysis if no cached result
        - Returns immediate panel state
        """
        track_id = self._derive_track_id(filepath)

        with self._lock:
            self._generation += 1
            gen = self._generation
            self._active_track_id = track_id
            self._active_filepath = filepath

        self._log(f"TRACK_SELECTED gen={gen} track_id={track_id}")

        # Check store for cached completed result
        cached = self._store.load_result(track_id)
        if cached and cached.get("status") == AnalysisStatus.COMPLETED.value:
            self._log(f"CACHE_HIT track_id={track_id}")
            with self._lock:
                self._panel_cache[track_id] = cached
            return self._build_panel_state(track_id, gen, source="cache")

        # Check if manager already has this job active
        status = self._manager.get_status(track_id)
        if status["status"] in ("QUEUED", "RUNNING"):
            self._log(f"ALREADY_ACTIVE track_id={track_id} status={status['status']}")
            return self._build_panel_state(track_id, gen, source="active")

        # Start new analysis
        result = self._manager.start_analysis(track_id, filepath)
        self._log(f"ANALYSIS_STARTED track_id={track_id} result={result['status']}")
        return self._build_panel_state(track_id, gen, source="new")

    def on_track_unselected(self) -> None:
        """Called when no track is selected (e.g. playlist cleared)."""
        with self._lock:
            self._generation += 1
            self._active_track_id = None
            self._active_filepath = None
        self._log("TRACK_UNSELECTED")

    # ──────────────────────────────────────────────────────────
    #  PANEL STATE (read-only query)
    # ──────────────────────────────────────────────────────────

    def get_panel_state(self) -> dict:
        """Return a read-only snapshot for the Analysis Panel.

        Safe to call from any thread at any frequency (e.g. 500ms timer).
        Returns a dict suitable for direct UI binding.
        """
        with self._lock:
            track_id = self._active_track_id
            gen = self._generation

        if track_id is None:
            return {
                "panel_state": "NO_TRACK",
                "generation": gen,
                "track_id": None,
                "status": None,
                "progress": 0.0,
                "data": None,
            }

        return self._build_panel_state(track_id, gen, source="poll")

    def _build_panel_state(self, track_id: str, gen: int, source: str) -> dict:
        """Build a panel state dict from available data."""
        # Check generation — prevent stale delivery
        with self._lock:
            if self._generation != gen:
                return {
                    "panel_state": "STALE",
                    "generation": gen,
                    "track_id": track_id,
                    "status": None,
                    "progress": 0.0,
                    "data": None,
                }

        # Try cache first
        with self._lock:
            cached = self._panel_cache.get(track_id)
        if cached and cached.get("status") == AnalysisStatus.COMPLETED.value:
            return {
                "panel_state": "ANALYSIS_COMPLETE",
                "generation": gen,
                "track_id": track_id,
                "status": "COMPLETED",
                "progress": 100.0,
                "data": self._extract_display_data(cached),
            }

        # Query manager for live status
        mgr_status = self._manager.get_status(track_id)
        status_str = mgr_status.get("status", "NOT_FOUND")

        if status_str == "NOT_FOUND":
            # Check store one more time
            stored = self._store.load_result(track_id)
            if stored and stored.get("status") == AnalysisStatus.COMPLETED.value:
                with self._lock:
                    self._panel_cache[track_id] = stored
                return {
                    "panel_state": "ANALYSIS_COMPLETE",
                    "generation": gen,
                    "track_id": track_id,
                    "status": "COMPLETED",
                    "progress": 100.0,
                    "data": self._extract_display_data(stored),
                }
            return {
                "panel_state": "NO_ANALYSIS",
                "generation": gen,
                "track_id": track_id,
                "status": "NOT_FOUND",
                "progress": 0.0,
                "data": None,
            }

        if status_str == "QUEUED":
            return {
                "panel_state": "ANALYSIS_QUEUED",
                "generation": gen,
                "track_id": track_id,
                "status": "QUEUED",
                "progress": 0.0,
                "data": None,
            }

        if status_str == "RUNNING":
            partial = self._manager.get_partial_result(track_id)
            progress = mgr_status.get("progress", 0.0)
            return {
                "panel_state": "ANALYSIS_RUNNING",
                "generation": gen,
                "track_id": track_id,
                "status": "RUNNING",
                "progress": progress,
                "data": self._extract_display_data(partial) if partial else None,
            }

        if status_str == AnalysisStatus.COMPLETED.value:
            final = self._manager.get_final_result(track_id)
            if final:
                with self._lock:
                    self._panel_cache[track_id] = final
                # Persist to store
                self._store.save_result(
                    FullTrackAnalysisResult(**{
                        k: v for k, v in final.items()
                        if k in FullTrackAnalysisResult.__dataclass_fields__
                    })
                )
            return {
                "panel_state": "ANALYSIS_COMPLETE",
                "generation": gen,
                "track_id": track_id,
                "status": "COMPLETED",
                "progress": 100.0,
                "data": self._extract_display_data(final) if final else None,
            }

        if status_str == AnalysisStatus.FAILED.value:
            return {
                "panel_state": "ANALYSIS_FAILED",
                "generation": gen,
                "track_id": track_id,
                "status": "FAILED",
                "progress": 0.0,
                "data": {"error": mgr_status.get("error", "Unknown error")},
            }

        if status_str == AnalysisStatus.CANCELLED.value:
            return {
                "panel_state": "ANALYSIS_CANCELED",
                "generation": gen,
                "track_id": track_id,
                "status": "CANCELLED",
                "progress": 0.0,
                "data": None,
            }

        # Fallback
        return {
            "panel_state": "UNKNOWN",
            "generation": gen,
            "track_id": track_id,
            "status": status_str,
            "progress": 0.0,
            "data": None,
        }

    # ──────────────────────────────────────────────────────────
    #  DISPLAY DATA EXTRACTION
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _extract_display_data(result: dict) -> dict:
        """Extract panel-relevant fields from a full analysis result dict."""
        if result is None:
            return {}
        return {
            "final_bpm": result.get("final_bpm", 0.0),
            "bpm_confidence": result.get("bpm_confidence", 0.0),
            "bpm_family": result.get("bpm_family", ""),
            "final_key": result.get("final_key", ""),
            "final_key_name": result.get("final_key_name", ""),
            "key_confidence": result.get("key_confidence", 0.0),
            "key_change_detected": result.get("key_change_detected", False),
            "duration_s": result.get("duration_s", 0.0),
            "section_count": len(result.get("sections", [])),
            "sections": result.get("sections", []),
            "cues": result.get("cues", []),
            "tempo_timeline_count": len(result.get("tempo_timeline", [])),
            "key_timeline_count": len(result.get("key_timeline", [])),
            "analyzer_ready": result.get("analyzer_ready", False),
            "review_required": result.get("review_required", False),
            "review_reason": result.get("review_reason", ""),
            "processing_time_s": result.get("processing_time_s", 0.0),
            "chunk_count": result.get("chunk_count", 0),
            "chunks_completed": result.get("chunks_completed", 0),
            "progress": result.get("progress", 0.0),
            "phase": result.get("phase", ""),
        }

    # ──────────────────────────────────────────────────────────
    #  LIFECYCLE
    # ──────────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """Gracefully shut down the manager. Call on app exit."""
        self._log("SHUTDOWN requested")
        self._manager.shutdown(wait=True)
        self._log("SHUTDOWN complete")

    # ──────────────────────────────────────────────────────────
    #  HELPERS
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _derive_track_id(filepath: str) -> str:
        """Derive track_id from filepath (filename stem without extension)."""
        return os.path.splitext(os.path.basename(filepath))[0]

    def _log(self, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        line = f"[{ts}] APP_ADAPTER: {msg}"
        logger.info(line)
        self._log_lines.append(line)

    def get_log(self) -> list[str]:
        return list(self._log_lines)

    @property
    def active_track_id(self) -> Optional[str]:
        with self._lock:
            return self._active_track_id

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation
