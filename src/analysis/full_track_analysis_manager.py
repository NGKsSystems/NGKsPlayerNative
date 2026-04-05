"""
NGKsPlayerNative — Full-Track Analysis Manager
Queues, starts, cancels, and tracks background analysis jobs.
Thread-safe. Never blocks the playback thread.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, Optional

from analysis_contracts import AnalysisStatus, FullTrackAnalysisResult

logger = logging.getLogger("analysis.manager")


class FullTrackAnalysisManager:
    """Coordinates background analysis jobs.

    - Prevents duplicate jobs for the same track_id.
    - Runs analysis workers in a bounded thread pool.
    - Exposes progress / partial / final result queries.
    - All public methods are thread-safe.
    """

    def __init__(
        self,
        max_workers: int = 2,
        worker_factory: Optional[Callable] = None,
    ):
        self._lock = threading.Lock()
        self._jobs: dict[str, _JobHandle] = {}
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="analysis",
        )
        self._worker_factory = worker_factory  # injected at integration time
        self._log_callback: Optional[Callable[[str], None]] = None

    # ──────────────────────────────────────────────────────────
    #  LOGGING
    # ──────────────────────────────────────────────────────────

    def set_log_callback(self, cb: Callable[[str], None]) -> None:
        self._log_callback = cb

    def _log(self, msg: str) -> None:
        logger.info(msg)
        if self._log_callback:
            self._log_callback(msg)

    # ──────────────────────────────────────────────────────────
    #  PUBLIC API
    # ──────────────────────────────────────────────────────────

    def start_analysis(self, track_id: str, filepath: str) -> dict:
        """Queue and start a background analysis job.

        Returns: dict with track_id, status, message.
        Raises nothing — returns error status on failure.
        """
        with self._lock:
            if track_id in self._jobs:
                existing = self._jobs[track_id]
                if existing.result.status in (
                    AnalysisStatus.QUEUED.value,
                    AnalysisStatus.RUNNING.value,
                ):
                    self._log(f"DUPLICATE: {track_id} already active")
                    return {
                        "track_id": track_id,
                        "status": "DUPLICATE",
                        "message": f"Analysis already active for {track_id}",
                    }

            # Create job handle
            result = FullTrackAnalysisResult(
                track_id=track_id,
                filepath=filepath,
                status=AnalysisStatus.QUEUED.value,
                started_at=datetime.now().isoformat(),
            )
            cancel_event = threading.Event()
            handle = _JobHandle(
                track_id=track_id,
                filepath=filepath,
                result=result,
                cancel_event=cancel_event,
            )
            self._jobs[track_id] = handle

        self._log(f"QUEUED: {track_id}")

        # Submit to thread pool
        future = self._pool.submit(self._run_job, handle)
        handle.future = future

        return {
            "track_id": track_id,
            "status": "QUEUED",
            "message": f"Analysis queued for {track_id}",
        }

    def cancel_analysis(self, track_id: str) -> dict:
        """Cancel a running or queued analysis job."""
        with self._lock:
            handle = self._jobs.get(track_id)
            if handle is None:
                return {
                    "track_id": track_id,
                    "status": "NOT_FOUND",
                    "message": f"No job found for {track_id}",
                }

            if handle.result.status in (
                AnalysisStatus.COMPLETED.value,
                AnalysisStatus.FAILED.value,
                AnalysisStatus.CANCELLED.value,
            ):
                return {
                    "track_id": track_id,
                    "status": handle.result.status,
                    "message": "Job already finished",
                }

        handle.cancel_event.set()
        self._log(f"CANCEL_REQUESTED: {track_id}")

        return {
            "track_id": track_id,
            "status": "CANCEL_REQUESTED",
            "message": f"Cancellation requested for {track_id}",
        }

    def get_status(self, track_id: str) -> dict:
        """Get current status and progress for a job."""
        with self._lock:
            handle = self._jobs.get(track_id)
            if handle is None:
                return {"track_id": track_id, "status": "NOT_FOUND"}

        r = handle.result
        return {
            "track_id": track_id,
            "status": r.status,
            "progress": r.progress,
            "chunks_completed": r.chunks_completed,
            "chunk_count": r.chunk_count,
            "error": r.error,
        }

    def get_partial_result(self, track_id: str) -> Optional[dict]:
        """Get current (possibly incomplete) analysis result."""
        with self._lock:
            handle = self._jobs.get(track_id)
            if handle is None:
                return None

        return handle.result.to_dict()

    def get_final_result(self, track_id: str) -> Optional[dict]:
        """Get final result if analysis is complete, else None."""
        with self._lock:
            handle = self._jobs.get(track_id)
            if handle is None:
                return None

        if handle.result.status != AnalysisStatus.COMPLETED.value:
            return None

        return handle.result.to_dict()

    def list_jobs(self) -> list[dict]:
        """List all jobs with their status."""
        with self._lock:
            handles = list(self._jobs.values())

        return [
            {
                "track_id": h.track_id,
                "status": h.result.status,
                "progress": h.result.progress,
            }
            for h in handles
        ]

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the thread pool. Cancel all active jobs first."""
        with self._lock:
            for handle in self._jobs.values():
                handle.cancel_event.set()

        self._pool.shutdown(wait=wait)
        self._log("SHUTDOWN complete")

    # ──────────────────────────────────────────────────────────
    #  INTERNAL
    # ──────────────────────────────────────────────────────────

    def _run_job(self, handle: "_JobHandle") -> None:
        """Execute an analysis job in a worker thread."""
        self._log(f"RUNNING: {handle.track_id}")
        handle.result.status = AnalysisStatus.RUNNING.value

        try:
            if self._worker_factory is None:
                from analysis_worker import run_analysis
            else:
                run_analysis = self._worker_factory

            run_analysis(
                filepath=handle.filepath,
                result=handle.result,
                cancel_event=handle.cancel_event,
                log_fn=self._log,
            )

            if handle.cancel_event.is_set():
                handle.result.status = AnalysisStatus.CANCELLED.value
                self._log(f"CANCELLED: {handle.track_id}")
            elif handle.result.status != AnalysisStatus.FAILED.value:
                handle.result.status = AnalysisStatus.COMPLETED.value
                handle.result.completed_at = datetime.now().isoformat()
                self._log(f"COMPLETED: {handle.track_id}")

        except Exception as exc:
            handle.result.status = AnalysisStatus.FAILED.value
            handle.result.error = f"WORKER_CRASH: {exc}"
            self._log(f"FAILED: {handle.track_id} — {exc}")


class _JobHandle:
    """Internal tracking object for a single analysis job."""
    __slots__ = ("track_id", "filepath", "result", "cancel_event", "future")

    def __init__(
        self,
        track_id: str,
        filepath: str,
        result: FullTrackAnalysisResult,
        cancel_event: threading.Event,
    ):
        self.track_id = track_id
        self.filepath = filepath
        self.result = result
        self.cancel_event = cancel_event
        self.future = None
