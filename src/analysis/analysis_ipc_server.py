"""
NGKsPlayerNative — Analysis IPC Server
JSON-line stdin/stdout protocol for C++ ↔ Python analysis bridge.

Protocol:
  - C++ writes one JSON object per line to stdin
  - Python writes one JSON object per line to stdout
  - stderr is reserved for logging (not parsed by C++)

Commands:
  {"cmd": "track_selected", "filepath": "C:/Music/song.mp3"}
  {"cmd": "track_unselected"}
  {"cmd": "poll"}
  {"cmd": "resolve", "time_s": 45.0}
  {"cmd": "shutdown"}

Responses always include:
  {"ok": true, ...}  or  {"ok": false, "error": "..."}
"""

import json
import logging
import os
import sys
import threading
import time
import traceback

# ── Bootstrap paths ──────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_WORKSPACE = os.path.dirname(os.path.dirname(_SCRIPT_DIR))  # two levels up
_SRC_ANALYSIS = os.path.join(_WORKSPACE, "src", "analysis")

if _SRC_ANALYSIS not in sys.path:
    sys.path.insert(0, _SRC_ANALYSIS)

# ── Logging to stderr only ───────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(name)s %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("analysis.ipc_server")

# ── Imports ──────────────────────────────────────────────────
from analysis_app_adapter import AnalysisAppAdapter
from analysis_store import AnalysisStore
from full_track_analysis_manager import FullTrackAnalysisManager
from live_readout_resolver import LiveReadoutResolver
from analysis_panel_model import AnalysisPanelModel

# ── Globals ──────────────────────────────────────────────────
_store: AnalysisStore
_manager: FullTrackAnalysisManager
_adapter: AnalysisAppAdapter
_resolver: LiveReadoutResolver
_panel_model: AnalysisPanelModel
_bound_track_id: str | None = None
_bound_generation: int = 0


def _init():
    """Initialize all analysis components."""
    global _store, _manager, _adapter, _resolver, _panel_model
    _store = AnalysisStore()
    _manager = FullTrackAnalysisManager(max_workers=1)
    _adapter = AnalysisAppAdapter(manager=_manager, store=_store)
    _resolver = LiveReadoutResolver()
    _panel_model = AnalysisPanelModel()
    logger.info("IPC server initialized. cache_dir=%s", _store.cache_dir)


def _respond(obj: dict) -> None:
    """Write one JSON line to stdout."""
    line = json.dumps(obj, ensure_ascii=False, default=str)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _handle_track_selected(msg: dict) -> dict:
    """Handle track_selected command."""
    global _bound_track_id, _bound_generation

    filepath = msg.get("filepath", "")
    if not filepath:
        return {"ok": False, "error": "missing filepath"}

    panel_state = _adapter.on_track_selected(filepath)
    _panel_model.update_from_adapter(panel_state)

    # Bind resolver if analysis is already complete (cache hit)
    gen = panel_state.get("generation", 0)
    track_id = panel_state.get("track_id", "")
    _bound_generation = gen

    if panel_state.get("panel_state") == "ANALYSIS_COMPLETE":
        cached = _store.load_result(track_id)
        if cached:
            _resolver.unbind()
            _resolver.bind_result(cached, track_id, gen)
            _bound_track_id = track_id

    snap = _panel_model.snapshot()
    return {"ok": True, "panel": snap}


def _handle_track_unselected() -> dict:
    """Handle track_unselected command."""
    global _bound_track_id, _bound_generation
    _adapter.on_track_unselected()
    _resolver.unbind()
    _bound_track_id = None
    _bound_generation = 0
    _panel_model.update_from_adapter({
        "panel_state": "NO_TRACK",
        "generation": 0,
        "track_id": None,
        "progress": 0,
        "data": None,
    })
    return {"ok": True, "panel": _panel_model.snapshot()}


def _handle_poll() -> dict:
    """Handle poll command — returns current panel state."""
    global _bound_track_id, _bound_generation

    panel_state = _adapter.get_panel_state()
    _panel_model.update_from_adapter(panel_state)

    # Auto-bind resolver when analysis completes
    gen = panel_state.get("generation", 0)
    track_id = panel_state.get("track_id")
    if (panel_state.get("panel_state") == "ANALYSIS_COMPLETE"
            and track_id and track_id != _bound_track_id):
        cached = _store.load_result(track_id)
        if cached:
            _resolver.unbind()
            _resolver.bind_result(cached, track_id, gen)
            _bound_track_id = track_id
            _bound_generation = gen

    return {"ok": True, "panel": _panel_model.snapshot()}


def _handle_resolve(msg: dict) -> dict:
    """Handle resolve command — returns live readout at playback position."""
    time_s = msg.get("time_s", 0.0)

    if not _bound_track_id:
        return {"ok": True, "readout": None, "reason": "no_binding"}

    snap = _resolver.resolve(time_s, generation=_bound_generation)
    readout_dict = snap.to_dict()
    _panel_model.update_live_readout(readout_dict)

    return {"ok": True, "panel": _panel_model.snapshot()}


def _dispatch(msg: dict) -> dict:
    """Route a command to its handler."""
    cmd = msg.get("cmd", "")
    if cmd == "track_selected":
        return _handle_track_selected(msg)
    elif cmd == "track_unselected":
        return _handle_track_unselected()
    elif cmd == "poll":
        return _handle_poll()
    elif cmd == "resolve":
        return _handle_resolve(msg)
    elif cmd == "ping":
        return {"ok": True, "pong": True}
    elif cmd == "shutdown":
        return {"ok": True, "shutdown": True}
    else:
        return {"ok": False, "error": f"unknown command: {cmd}"}


def main():
    """Main IPC loop — read JSON lines from stdin, write responses to stdout."""
    _init()
    _respond({"ok": True, "ready": True, "pid": os.getpid()})

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            msg = json.loads(line)
        except json.JSONDecodeError as e:
            _respond({"ok": False, "error": f"JSON parse error: {e}"})
            continue

        try:
            result = _dispatch(msg)
        except Exception as e:
            logger.error("Command failed: %s\n%s", e, traceback.format_exc())
            _respond({"ok": False, "error": str(e)})
            continue

        _respond(result)

        # Shutdown requested
        if result.get("shutdown"):
            logger.info("Shutdown requested, exiting.")
            break

    # Cleanup
    try:
        _manager.shutdown()
    except Exception:
        pass
    logger.info("IPC server exiting.")


if __name__ == "__main__":
    main()
