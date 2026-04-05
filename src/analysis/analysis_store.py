"""
NGKsPlayerNative — Analysis Store
Saves/loads analysis results to/from JSON.
Supports partial checkpoints and atomic writes.
"""

import json
import os
from typing import Optional

from analysis_contracts import FullTrackAnalysisResult

DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "analysis_cache",
)


class AnalysisStore:
    """Persists analysis results as JSON files.

    Path convention: <cache_dir>/<track_id>.analysis.json
    Uses atomic rename writes to prevent corruption.
    """

    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _path_for(self, track_id: str) -> str:
        safe = track_id.replace("/", "_").replace("\\", "_").replace(":", "_")
        return os.path.join(self.cache_dir, f"{safe}.analysis.json")

    def save_result(self, result: FullTrackAnalysisResult) -> str:
        """Save a result (partial or final) atomically. Returns path."""
        path = self._path_for(result.track_id)
        tmp = path + ".tmp"

        data = result.to_dict()
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            f.flush()
            os.fsync(f.fileno())

        if os.path.isfile(path):
            os.remove(path)
        os.rename(tmp, path)
        return path

    def save_checkpoint(self, result: FullTrackAnalysisResult) -> str:
        """Save a partial checkpoint. Same as save_result but named for clarity."""
        return self.save_result(result)

    def load_result(self, track_id: str) -> Optional[dict]:
        """Load a previously saved result. Returns dict or None."""
        path = self._path_for(track_id)
        if not os.path.isfile(path):
            # Check for .tmp fallback (crash recovery)
            tmp = path + ".tmp"
            if os.path.isfile(tmp):
                try:
                    os.rename(tmp, path)
                except OSError:
                    return None
            else:
                return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def has_result(self, track_id: str) -> bool:
        """Check if a result exists for the given track ID."""
        return os.path.isfile(self._path_for(track_id))

    def delete_result(self, track_id: str) -> bool:
        """Delete a stored result. Returns True if deleted."""
        path = self._path_for(track_id)
        if os.path.isfile(path):
            os.remove(path)
            return True
        return False

    def list_results(self) -> list[str]:
        """List all stored track IDs."""
        ids = []
        for f in os.listdir(self.cache_dir):
            if f.endswith(".analysis.json"):
                ids.append(f[: -len(".analysis.json")])
        return ids
