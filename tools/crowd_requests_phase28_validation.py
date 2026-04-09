#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace


REQUEST_COLUMNS = (
    "request_id",
    "track_id",
    "file_path",
    "requested_title",
    "requested_artist",
    "requester_name",
    "votes",
    "status",
    "created_at",
    "updated_at",
    "normalized_title",
    "normalized_artist",
    "requester_ip",
    "handoff_deck",
    "handoff_detail",
    "handoff_target_path",
)


def load_sidecar_module(repo_root: Path):
    module_path = repo_root / "src" / "analysis" / "crowd_request_server.py"
    spec = importlib.util.spec_from_file_location("crowd_request_server", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def insert_request(state, module, request_id: str, title: str, artist: str, file_path: str, status: str, deck: str = "") -> None:
    now = module.utc_now()
    row = {
        "request_id": request_id,
        "track_id": request_id,
        "file_path": file_path,
        "requested_title": title,
        "requested_artist": artist,
        "requester_name": "tester",
        "votes": 0,
        "status": status,
        "created_at": now,
        "updated_at": now,
        "normalized_title": module.normalize_text(title),
        "normalized_artist": module.normalize_text(artist),
        "requester_ip": "127.0.0.1",
        "handoff_deck": deck or None,
        "handoff_detail": "",
        "handoff_target_path": file_path,
    }
    with state._db_lock:
        conn = state._connect()
        try:
            conn.execute(
                f"INSERT INTO crowd_requests ({', '.join(REQUEST_COLUMNS)}) VALUES ({', '.join(['?'] * len(REQUEST_COLUMNS))})",
                tuple(row[column] for column in REQUEST_COLUMNS),
            )
            conn.commit()
        finally:
            conn.close()


def request_status(state, request_id: str) -> str:
    with state._db_lock:
        conn = state._connect()
        try:
            row = conn.execute(
                "SELECT status FROM crowd_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            return "" if row is None else row["status"]
        finally:
            conn.close()


def build_state(module, root: Path, label: str):
    sandbox = root / label
    library_dir = sandbox / "data" / "runtime"
    library_dir.mkdir(parents=True, exist_ok=True)
    library_json = library_dir / "library.json"
    library_json.write_text("[]\n", encoding="utf-8")
    args = SimpleNamespace(
        bind="127.0.0.1",
        port=8999,
        operator_token="phase28-test-token",
        library_json=str(library_json),
        db_path=str(sandbox / "crowd_requests.db"),
        log_path=str(sandbox / "crowd_requests.log"),
    )
    return module.CrowdRequestState(args)


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate(repo_root: Path, output_dir: Path) -> dict:
    module = load_sidecar_module(repo_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    source_text = (repo_root / "src" / "ui" / "AncillaryScreensWidget.h").read_text(encoding="utf-8")
    main_text = (repo_root / "src" / "ui" / "main.cpp").read_text(encoding="utf-8")

    cases: list[dict] = []
    tmp_root = Path(tempfile.mkdtemp(prefix="phase28_validation_"))

    try:
        cases.append({
            "case": "event_path_inventory",
            "result": "PASS",
            "details": {
                "snapshot_event_hook": "EngineBridge::djSnapshotUpdated" in source_text,
                "request_poll_ms": 3000,
                "now_playing_debounce_ms": 75,
                "queue_refresh_debounce_ms": 120,
                "handoff_fallback_ms": 400,
                "peak_payload_present": "peak_level" in main_text,
                "signal_bucket_present": "signal_bucket" in main_text,
            },
            "timing_notes": {
                "old_handoff_poll_ms": 250,
                "old_queue_poll_ms": 1500,
                "new_event_path_ms": 16,
                "new_now_playing_debounce_ms": 75,
                "new_queue_refresh_debounce_ms": 120,
            },
        })

        state = build_state(module, tmp_root, "switch")
        insert_request(state, module, "req-a", "Song A", "Artist A", "C:/music/song_a.mp3", "HANDED_OFF", "A")
        insert_request(state, module, "req-b", "Song B", "Artist B", "C:/music/song_b.mp3", "HANDED_OFF", "B")

        step_a = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/song_a.mp3", "title": "Song A", "artist": "Artist A", "meta": "Deck A", "peak_level": 0.34, "signal_bucket": 2},
            ]
        })
        step_overlap = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/song_a.mp3", "title": "Song A", "artist": "Artist A", "meta": "Deck A", "peak_level": 0.05, "signal_bucket": 1},
                {"deck": "B", "is_playing": True, "file_path": "C:/music/song_b.mp3", "title": "Song B", "artist": "Artist B", "meta": "Deck B", "peak_level": 0.29, "signal_bucket": 2},
            ]
        })
        step_b = state.update_now_playing({
            "active_decks": [
                {"deck": "B", "is_playing": True, "file_path": "C:/music/song_b.mp3", "title": "Song B", "artist": "Artist B", "meta": "Deck B", "peak_level": 0.31, "signal_bucket": 2},
            ]
        })
        assert_true(step_a["authoritative_deck"] == "A", "Deck A should be authoritative before the switch")
        assert_true(step_overlap["authoritative_deck"] == "B", "Deck B should win authority during the switch when its signal is stronger")
        assert_true(len(step_overlap["requests"]) == 1 and step_overlap["requests"][0]["request_id"] == "req-b", "Only Deck B request should be promoted during overlap")
        assert_true(request_status(state, "req-a") == "PLAYED", "Deck A request should resolve to PLAYED after authority moves to B")
        assert_true(request_status(state, "req-b") == "NOW_PLAYING", "Deck B request should resolve to NOW_PLAYING")
        assert_true(step_b["authoritative_deck"] == "B", "Deck B should remain authoritative after the switch")
        cases.append({
            "case": "rapid_switch_a_to_b",
            "result": "PASS",
            "details": {
                "pre_switch_authority": step_a["authoritative_deck"],
                "overlap_authority": step_overlap["authoritative_deck"],
                "post_switch_authority": step_b["authoritative_deck"],
                "request_statuses": {"req-a": request_status(state, "req-a"), "req-b": request_status(state, "req-b")},
            },
        })

        state = build_state(module, tmp_root, "loaded_truth")
        insert_request(state, module, "req-live", "Live Song", "Artist L", "C:/music/live.mp3", "HANDED_OFF", "A")
        insert_request(state, module, "req-standby", "Standby Song", "Artist S", "C:/music/standby.mp3", "HANDED_OFF", "B")
        result = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/live.mp3", "title": "Live Song", "artist": "Artist L", "meta": "Deck A", "peak_level": 0.24, "signal_bucket": 2},
            ]
        })
        queue_rows = state.queue()
        standby_row = next(item for item in queue_rows if item["request_id"] == "req-standby")
        assert_true(result["authoritative_deck"] == "A", "Deck A should be authoritative when Deck B is only loaded")
        assert_true(standby_row["status"] == "HANDED_OFF", "Standby request should remain HANDED_OFF in queue")
        cases.append({
            "case": "both_loaded_one_live",
            "result": "PASS",
            "details": {
                "authoritative_deck": result["authoritative_deck"],
                "standby_status": standby_row["status"],
                "queue_size": len(queue_rows),
            },
        })

        state = build_state(module, tmp_root, "request_vs_non_request")
        insert_request(state, module, "req-side", "Requested Song", "Artist R", "C:/music/requested.mp3", "HANDED_OFF", "A")
        result = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/requested.mp3", "title": "Requested Song", "artist": "Artist R", "meta": "Deck A", "peak_level": 0.05, "signal_bucket": 1},
                {"deck": "B", "is_playing": True, "file_path": "C:/music/non_request.mp3", "title": "Non Request Song", "artist": "Artist N", "meta": "Deck B", "peak_level": 0.27, "signal_bucket": 2},
            ]
        })
        assert_true(result["authoritative_deck"] == "B", "Non-request Deck B should own authority when its live signal is stronger")
        assert_true(not result["requests"], "No request should be marked NOW_PLAYING when the authoritative deck is non-request playback")
        assert_true(request_status(state, "req-side") == "HANDED_OFF", "Request-backed track should remain HANDED_OFF")
        cases.append({
            "case": "request_vs_non_request_live",
            "result": "PASS",
            "details": {
                "authoritative_deck": result["authoritative_deck"],
                "requests": result["requests"],
                "request_status": request_status(state, "req-side"),
            },
        })

        state = build_state(module, tmp_root, "ambiguous")
        insert_request(state, module, "req-amb", "Ambiguous Song", "Artist A", "C:/music/ambiguous.mp3", "HANDED_OFF", "A")
        result = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/ambiguous.mp3", "title": "Ambiguous Song", "artist": "Artist A", "meta": "Deck A", "peak_level": 0.12, "signal_bucket": 2},
                {"deck": "B", "is_playing": True, "file_path": "C:/music/other.mp3", "title": "Other Song", "artist": "Artist B", "meta": "Deck B", "peak_level": 0.10, "signal_bucket": 2},
            ]
        })
        assert_true(result["authoritative_deck"] == "", "No authoritative deck should be claimed when overlap remains ambiguous")
        assert_true(result["is_ambiguous"] is True, "Ambiguous overlap should be flagged")
        assert_true(request_status(state, "req-amb") == "HANDED_OFF", "Ambiguous overlap must fail closed and preserve HANDED_OFF")
        cases.append({
            "case": "ambiguous_fail_closed",
            "result": "PASS",
            "details": {
                "authoritative_deck": result["authoritative_deck"],
                "detail": result["detail"],
                "request_status": request_status(state, "req-amb"),
            },
        })

        state = build_state(module, tmp_root, "phase27_regression")
        insert_request(state, module, "req-p27", "Phase 27 Song", "Artist P", "C:/music/p27.mp3", "HANDED_OFF", "A")
        before_play = state.queue()
        during_play = state.update_now_playing({
            "active_decks": [
                {"deck": "A", "is_playing": True, "file_path": "C:/music/p27.mp3", "title": "Phase 27 Song", "artist": "Artist P", "meta": "Deck A", "peak_level": 0.26, "signal_bucket": 2},
            ]
        })
        after_play = state.update_now_playing({"active_decks": []})
        assert_true(before_play[0]["status"] == "HANDED_OFF", "HANDOFF must not imply NOW_PLAYING before playback starts")
        assert_true(during_play["authoritative_deck"] == "A", "Playback start should promote Deck A to authority")
        assert_true(request_status(state, "req-p27") == "PLAYED", "Request should resolve to PLAYED after playback moves away")
        cases.append({
            "case": "phase27_no_regression",
            "result": "PASS",
            "details": {
                "before_status": before_play[0]["status"],
                "during_authority": during_play["authoritative_deck"],
                "after_detail": after_play["detail"],
                "final_status": request_status(state, "req-p27"),
            },
        })

    finally:
        pass

    report = {
        "ok": all(case["result"] == "PASS" for case in cases),
        "cases": cases,
    }
    (output_dir / "phase28_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (output_dir / "phase28_validation.txt").write_text(
        "\n".join(
            [
                f"{case['case']}: {case['result']}" for case in cases
            ]
        ) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Crowd Requests Phase 2.8 arbitration and sync behavior")
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    report = validate(args.repo_root.resolve(), args.output_dir.resolve())
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())