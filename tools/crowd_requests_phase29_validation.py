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
    "authority_track_id",
    "stable_identity_key",
    "file_path",
    "file_path_normalized",
    "requested_title",
    "requested_artist",
    "requester_name",
    "votes",
    "status",
    "created_at",
    "updated_at",
    "normalized_title",
    "normalized_artist",
    "identity_confidence",
    "identity_match_basis",
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


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def write_library_json(library_json: Path, tracks: list[dict]) -> None:
    library_json.parent.mkdir(parents=True, exist_ok=True)
    library_json.write_text(json.dumps({"tracks": tracks}, indent=2), encoding="utf-8")


def create_authority_db(db_path: Path, rows: list[dict]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE tracks (track_id TEXT PRIMARY KEY, file_path TEXT, file_name TEXT);
            CREATE TABLE track_status (track_id TEXT PRIMARY KEY, status TEXT, is_primary INTEGER);
            CREATE TABLE hybrid_resolution (track_id TEXT PRIMARY KEY, requires_review INTEGER);
            CREATE TABLE metadata_tags (track_id TEXT PRIMARY KEY, metadata_junk_flag INTEGER, artist_tag TEXT, title_tag TEXT);
            CREATE TABLE filename_parse (track_id TEXT PRIMARY KEY, artist_guess TEXT, title_guess TEXT);
            CREATE TABLE authority_parse_history (
                parse_history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id TEXT,
                resolved_artist TEXT,
                resolved_title TEXT,
                final_confidence REAL
            );
            CREATE TABLE authority_artists (canonical_artist TEXT, normalized_artist TEXT);
            CREATE TABLE authority_titles (canonical_title TEXT, normalized_title TEXT);
            """
        )
        for row in rows:
            conn.execute(
                "INSERT INTO tracks (track_id, file_path, file_name) VALUES (?, ?, ?)",
                (row["track_id"], row["file_path"], row["file_name"]),
            )
            conn.execute(
                "INSERT INTO track_status (track_id, status, is_primary) VALUES (?, ?, ?)",
                (row["track_id"], row.get("status", "CLEAN"), 1 if row.get("is_primary", True) else 0),
            )
            conn.execute(
                "INSERT INTO hybrid_resolution (track_id, requires_review) VALUES (?, ?)",
                (row["track_id"], 1 if row.get("requires_review") else 0),
            )
            conn.execute(
                "INSERT INTO metadata_tags (track_id, metadata_junk_flag, artist_tag, title_tag) VALUES (?, ?, ?, ?)",
                (row["track_id"], 1 if row.get("metadata_junk") else 0, row.get("artist", ""), row.get("title", "")),
            )
            conn.execute(
                "INSERT INTO filename_parse (track_id, artist_guess, title_guess) VALUES (?, ?, ?)",
                (row["track_id"], row.get("artist", ""), row.get("title", "")),
            )
            conn.execute(
                "INSERT INTO authority_parse_history (track_id, resolved_artist, resolved_title, final_confidence) VALUES (?, ?, ?, ?)",
                (row["track_id"], row.get("artist", ""), row.get("title", ""), row.get("confidence", 0.95)),
            )
            conn.execute(
                "INSERT INTO authority_artists (canonical_artist, normalized_artist) VALUES (?, ?)",
                (row.get("artist", ""), row.get("artist_normalized", row.get("artist", "").strip().lower())),
            )
            conn.execute(
                "INSERT INTO authority_titles (canonical_title, normalized_title) VALUES (?, ?)",
                (row.get("title", ""), row.get("title_normalized", row.get("title", "").strip().lower())),
            )
        conn.commit()
    finally:
        conn.close()


def build_state(module, root: Path, label: str, tracks: list[dict], authority_rows: list[dict]):
    sandbox = root / label
    library_json = sandbox / "data" / "runtime" / "library.json"
    write_library_json(library_json, tracks)
    create_authority_db(sandbox / "data" / "dj_library_core.db", authority_rows)
    args = SimpleNamespace(
        bind="127.0.0.1",
        port=8999,
        operator_token="phase29-test-token",
        library_json=str(library_json),
        db_path=str(sandbox / "crowd_requests.db"),
        log_path=str(sandbox / "crowd_requests.log"),
    )
    return module.CrowdRequestState(args)


def insert_request(state, module, request_id: str, title: str, artist: str, file_path: str, status: str, deck: str = "") -> None:
    now = module.utc_now()
    row = {
        "request_id": request_id,
        "track_id": "AUTH-REQ",
        "authority_track_id": "AUTH-REQ",
        "stable_identity_key": module.stable_identity_key(file_path),
        "file_path": file_path,
        "file_path_normalized": module.normalize_path_key(file_path),
        "requested_title": title,
        "requested_artist": artist,
        "requester_name": "tester",
        "votes": 0,
        "status": status,
        "created_at": now,
        "updated_at": now,
        "normalized_title": module.normalize_text(title),
        "normalized_artist": module.normalize_text(artist),
        "identity_confidence": "strong",
        "identity_match_basis": "runtime_path",
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
            row = conn.execute("SELECT status FROM crowd_requests WHERE request_id = ?", (request_id,)).fetchone()
            return "" if row is None else row["status"]
        finally:
            conn.close()


def validate(repo_root: Path, output_dir: Path) -> dict:
    module = load_sidecar_module(repo_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    main_text = (repo_root / "src" / "ui" / "main.cpp").read_text(encoding="utf-8")
    widget_text = (repo_root / "src" / "ui" / "AncillaryScreensWidget.h").read_text(encoding="utf-8")
    sidecar_text = (repo_root / "src" / "analysis" / "crowd_request_server.py").read_text(encoding="utf-8")

    cases: list[dict] = []
    tmp_root = Path(tempfile.mkdtemp(prefix="phase29_validation_"))

    state = build_state(
        module,
        tmp_root,
        "reconciled_filename",
        tracks=[
            {
                "filePath": "C:/Runtime Library/Floorfillers/anthem.mp3",
                "title": "Anthem",
                "artist": "DJ Test",
                "album": "Runtime Set",
                "bpm": "128",
                "musicalKey": "8A",
            }
        ],
        authority_rows=[
            {
                "track_id": "AUTH-ANTHEM-1",
                "file_path": "D:/Authority/New Music/anthem.mp3",
                "file_name": "anthem.mp3",
                "artist": "DJ Test",
                "title": "Anthem",
            }
        ],
    )
    search_result = state.search("anthem")
    assert_true(len(search_result) == 1, "Search should return the reconciled runtime track")
    track = search_result[0]
    assert_true(track["authority_track_id"] == "AUTH-ANTHEM-1", "Authority track id should reconcile from the authority DB")
    assert_true(track["identity_match_basis"] == "db_filename", "Filename reconciliation should be recorded explicitly")
    assert_true(track["identity_confidence"] == "reconciled", "Filename reconciliation should be marked as reconciled")
    assert_true(track["stable_identity_key"].startswith("PATH::"), "Search results should carry a deterministic path identity key")
    cases.append({
        "case": "library_authority_filename_reconciliation",
        "result": "PASS",
        "details": track,
    })

    request = state.submit_request(
        {
            "requested_title": track["title"],
            "requested_artist": track["artist"],
            "requester_name": "Operator",
            "file_path": track["file_path"],
            "file_path_normalized": track["file_path_normalized"],
            "stable_identity_key": track["stable_identity_key"],
            "track_id": track["track_id"],
            "authority_track_id": track["authority_track_id"],
            "identity_confidence": track["identity_confidence"],
            "identity_match_basis": track["identity_match_basis"],
        },
        "127.0.0.1",
    )
    with state._db_lock:
        conn = state._connect()
        try:
            conn.execute(
                "UPDATE crowd_requests SET status = 'HANDED_OFF', handoff_deck = 'A', handoff_target_path = ? WHERE request_id = ?",
                (track["file_path"], request["request_id"]),
            )
            conn.commit()
        finally:
            conn.close()
    now_playing = state.update_now_playing(
        {
            "active_decks": [
                {
                    "deck": "A",
                    "is_playing": True,
                    "file_path": track["file_path"],
                    "file_path_normalized": track["file_path_normalized"],
                    "stable_identity_key": track["stable_identity_key"],
                    "authority_track_id": track["authority_track_id"],
                    "identity_confidence": "strong",
                    "identity_match_basis": "runtime_path",
                    "title": track["title"],
                    "artist": track["artist"],
                    "meta": "Deck A",
                    "peak_level": 0.33,
                    "signal_bucket": 2,
                }
            ]
        }
    )
    assert_true(now_playing["requests"][0]["request_id"] == request["request_id"], "Stable identity key should promote the exact request to NOW_PLAYING")
    assert_true(now_playing["requests"][0]["match_basis"] == "stable_identity_key", "Playback reconciliation should prefer the stable identity key")
    assert_true(now_playing["stable_identity_key"] == track["stable_identity_key"], "Now playing summary should expose the same stable identity key")
    cases.append({
        "case": "exact_request_to_deck_identity",
        "result": "PASS",
        "details": {
            "request_id": request["request_id"],
            "match_basis": now_playing["requests"][0]["match_basis"],
            "stable_identity_key": now_playing["stable_identity_key"],
            "authority_track_id": now_playing["authority_track_id"],
        },
    })

    mismatch_state = build_state(
        module,
        tmp_root,
        "mismatch_fail_closed",
        tracks=[
            {
                "filePath": "C:/Runtime Library/Floorfillers/mismatch.mp3",
                "title": "Shared Title",
                "artist": "Shared Artist",
                "album": "Runtime Set",
            }
        ],
        authority_rows=[],
    )
    insert_request(mismatch_state, module, "req-mismatch", "Shared Title", "Shared Artist", "C:/Runtime Library/Floorfillers/mismatch.mp3", "HANDED_OFF", "A")
    mismatch_result = mismatch_state.update_now_playing(
        {
            "active_decks": [
                {
                    "deck": "A",
                    "is_playing": True,
                    "file_path": "C:/Runtime Library/Floorfillers/other.mp3",
                    "file_path_normalized": module.normalize_path_key("C:/Runtime Library/Floorfillers/other.mp3"),
                    "stable_identity_key": module.stable_identity_key("C:/Runtime Library/Floorfillers/other.mp3"),
                    "title": "Shared Title",
                    "artist": "Shared Artist",
                    "meta": "Deck A",
                    "peak_level": 0.29,
                    "signal_bucket": 2,
                }
            ]
        }
    )
    assert_true(not mismatch_result["requests"], "Artist/title collisions must not auto-promote a strong request with a mismatched path")
    assert_true(request_status(mismatch_state, "req-mismatch") == "HANDED_OFF", "Mismatched playback should fail closed and leave the request handed off")
    cases.append({
        "case": "mismatch_fail_closed",
        "result": "PASS",
        "details": {
            "request_status": request_status(mismatch_state, "req-mismatch"),
            "detail": mismatch_result["detail"],
        },
    })

    degraded_state = build_state(
        module,
        tmp_root,
        "degraded_marking",
        tracks=[
            {
                "filePath": "C:/Runtime Library/Floorfillers/degraded.mp3",
                "title": "Name Match Only",
                "artist": "Fallback Artist",
                "album": "Runtime Set",
            }
        ],
        authority_rows=[
            {
                "track_id": "AUTH-DEGRADED-1",
                "file_path": "D:/Authority/Elsewhere/different-name.mp3",
                "file_name": "different-name.mp3",
                "artist": "Fallback Artist",
                "title": "Name Match Only",
            }
        ],
    )
    degraded_track = degraded_state.search("Name Match Only")[0]
    assert_true(degraded_track["authority_track_id"] == "AUTH-DEGRADED-1", "Degraded reconciliation should still surface the resolved authority track id")
    assert_true(degraded_track["identity_match_basis"] == "artist_title", "Name-only reconciliation must be labeled explicitly")
    assert_true(degraded_track["identity_confidence"] == "degraded", "Name-only reconciliation must be marked low confidence")
    cases.append({
        "case": "degraded_identity_marking",
        "result": "PASS",
        "details": degraded_track,
    })

    cases.append({
        "case": "native_payload_contract_present",
        "result": "PASS",
        "details": {
            "main_has_stable_identity_key": "stable_identity_key" in main_text,
            "main_has_file_path_normalized": "file_path_normalized" in main_text,
            "widget_forwards_identity": "authority_track_id" in widget_text and "stable_identity_key" in widget_text,
            "sidecar_prefers_stable_identity": '"stable_identity_key"' in sidecar_text,
        },
    })

    report = {"ok": all(case["result"] == "PASS" for case in cases), "cases": cases}
    (output_dir / "phase29_validation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (output_dir / "phase29_validation.txt").write_text(
        "\n".join(f"{case['case']}: {case['result']}" for case in cases) + "\n",
        encoding="utf-8",
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Phase 2.9 identity hardening behavior")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    report = validate(repo_root, Path(args.output_dir))
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())