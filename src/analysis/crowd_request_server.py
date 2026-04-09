#!/usr/bin/env python3

import argparse
import ctypes
import html
import io
import ipaddress
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import qrcode


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return True
    if os.name == "nt":
        synchronize = 0x00100000
        process_handle = ctypes.windll.kernel32.OpenProcess(synchronize, False, pid)
        if not process_handle:
            return False
        try:
            wait_result = ctypes.windll.kernel32.WaitForSingleObject(process_handle, 0)
            return wait_result == 0x102
        finally:
            ctypes.windll.kernel32.CloseHandle(process_handle)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def sanitize_text(value: Any, max_len: int = 160) -> str:
    text = "" if value is None else str(value)
    text = " ".join(text.replace("\x00", " ").split())
    return text[:max_len].strip()


def normalize_text(value: str) -> str:
    return " ".join(sanitize_text(value, max_len=512).lower().split())


def normalize_path_key(value: str) -> str:
    return os.path.normcase(os.path.normpath(sanitize_text(value, max_len=2048)))


def normalize_file_name(value: str) -> str:
    return os.path.basename(sanitize_text(value, max_len=512)).strip().lower()


def stable_identity_key(value: str) -> str:
    normalized = normalize_path_key(value)
    return f"PATH::{normalized}" if normalized else ""


def enrichment_name_key(artist: str, title: str) -> str:
    return f"NAME::{normalize_text(artist)}::{normalize_text(title)}"


def file_name_key(value: str) -> str:
    normalized = normalize_file_name(value)
    return f"FILE::{normalized}" if normalized else ""


def strip_extension(name: str) -> str:
    text = sanitize_text(name, max_len=512)
    base, _ = os.path.splitext(text)
    return base or text


def split_artist_title(text: str) -> tuple[str, str]:
    cleaned = sanitize_text(text, max_len=512)
    for delimiter in (" - ", " – ", " — ", " | ", " ~ "):
        if delimiter in cleaned:
            artist, title = cleaned.split(delimiter, 1)
            return sanitize_text(artist, max_len=200), sanitize_text(title, max_len=200)
    return "", cleaned


def contains_junk_marker(value: str) -> bool:
    lowered = normalize_text(value)
    return any(marker in lowered for marker in (
        "official video",
        "lyrics",
        "visualizer",
        "full album",
        "hour mix",
        "extended mix",
        "reaction",
    ))


REQUEST_STATUSES = (
    "PENDING",
    "ACCEPTED",
    "HANDED_OFF",
    "NOW_PLAYING",
    "PLAYED",
    "HANDOFF_FAILED",
    "REJECTED",
    "REMOVED",
)

SUBMIT_RATE_LIMIT = (8, 60)
VOTE_RATE_LIMIT = (24, 60)
SUBMIT_COOLDOWN_SECONDS = 20
DUPLICATE_WINDOW_SECONDS = 300
VOTE_COOLDOWN_SECONDS = 15
QUEUE_VISIBLE_STATUSES = ("PENDING", "ACCEPTED", "HANDED_OFF", "HANDOFF_FAILED", "REJECTED")
ARBITRATION_SIGNAL_FLOOR = 0.02
ARBITRATION_SIGNAL_MARGIN = 0.08

REQUEST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS crowd_requests (
    request_id TEXT PRIMARY KEY,
    track_id TEXT NULL,
    authority_track_id TEXT NULL,
    stable_identity_key TEXT NULL,
    file_path TEXT NULL,
    file_path_normalized TEXT NULL,
    requested_title TEXT NOT NULL,
    requested_artist TEXT NOT NULL,
    requester_name TEXT NOT NULL,
    votes INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    normalized_title TEXT NOT NULL,
    normalized_artist TEXT NOT NULL,
    identity_confidence TEXT NULL,
    identity_match_basis TEXT NULL,
    requester_ip TEXT NULL,
    handoff_deck TEXT NULL,
    handoff_detail TEXT NULL,
    handoff_target_path TEXT NULL
)
"""


class CrowdRequestState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.bind = args.bind
        self.port = args.port
        self.parent_pid = args.parent_pid
        self.operator_token = args.operator_token or secrets.token_urlsafe(24)
        self.library_json_path = Path(args.library_json).resolve()
        self.db_path = Path(args.db_path).resolve()
        self.log_path = Path(args.log_path).resolve()
        self.library_core_db_path = self.library_json_path.parents[1] / "dj_library_core.db"
        self.allow_private_only = True
        self._db_lock = threading.Lock()
        self._rate_lock = threading.Lock()
        self._library_lock = threading.Lock()
        self._recent_submit: dict[str, deque[float]] = defaultdict(deque)
        self._recent_vote: dict[str, deque[float]] = defaultdict(deque)
        self._submit_cooldowns: dict[str, float] = {}
        self._vote_cooldowns: dict[str, float] = {}
        self._library_cache: list[dict[str, Any]] = []
        self._library_mtime_ns = -1
        self._search_enrichment_cache: dict[str, dict[str, Any]] = {}
        self._search_enrichment_mtime_ns = -1
        self._qr_png: bytes = b""
        self._qr_join_url = ""
        self.join_host = "localhost"
        self.join_url = f"http://localhost:{self.port}"
        self._now_playing = {
            "title": "",
            "artist": "",
            "meta": "",
            "deck": "",
            "file_path": "",
            "file_path_normalized": "",
            "stable_identity_key": "",
            "authority_track_id": "",
            "identity_confidence": "unresolved",
            "identity_match_basis": "unresolved",
            "authoritative_deck": "",
            "active_decks": [],
            "requests": [],
            "is_ambiguous": False,
            "detail": "",
            "updated_at": "",
        }
        self._configure_logging()
        self._init_db()
        self._refresh_join_url()

    def start_parent_watchdog(self, server: ThreadingHTTPServer) -> None:
        if self.parent_pid <= 0:
            return

        def monitor_parent() -> None:
            while True:
                time.sleep(1.0)
                if is_process_alive(self.parent_pid):
                    continue
                self._audit("SERVER_PARENT_EXIT", None, {
                    "parent_pid": self.parent_pid,
                    "detected_at": utc_now(),
                })
                logging.warning("Parent process %s is gone; shutting down crowd request server", self.parent_pid)
                threading.Thread(target=server.shutdown, daemon=True).start()
                return

        threading.Thread(target=monitor_parent, daemon=True).start()

    def _configure_logging(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(message)s",
            handlers=[
                logging.FileHandler(self.log_path, encoding="utf-8"),
                logging.StreamHandler(),
            ],
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db_lock:
            conn = self._connect()
            try:
                self._ensure_request_schema(conn)
                conn.executescript(
                    """
                    CREATE INDEX IF NOT EXISTS idx_crowd_requests_status
                        ON crowd_requests(status, created_at DESC);

                    CREATE INDEX IF NOT EXISTS idx_crowd_requests_norm
                        ON crowd_requests(normalized_artist, normalized_title, status);

                    CREATE TABLE IF NOT EXISTS crowd_request_settings (
                        settings_id INTEGER PRIMARY KEY CHECK(settings_id = 1),
                        request_policy TEXT NOT NULL,
                        venmo TEXT NOT NULL,
                        cashapp TEXT NOT NULL,
                        paypal TEXT NOT NULL,
                        zelle TEXT NOT NULL,
                        buymeacoffee TEXT NOT NULL,
                        chime TEXT NOT NULL,
                        card_url TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS crowd_request_audit (
                        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        request_id TEXT NULL,
                        detail_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    """
                )
                self._ensure_settings_schema(conn)
                conn.execute(
                    """
                    INSERT INTO crowd_request_settings (
                        settings_id, request_policy, venmo, cashapp, paypal, zelle, buymeacoffee, chime, card_url, updated_at
                    ) VALUES (1, 'free', '', '', '', '', '', '', '', ?)
                    ON CONFLICT(settings_id) DO NOTHING
                    """,
                    (utc_now(),),
                )
                conn.commit()
            finally:
                conn.close()

    def _ensure_request_schema(self, conn: sqlite3.Connection) -> None:
        table = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'crowd_requests'"
        ).fetchone()
        if table is None:
            conn.execute(REQUEST_TABLE_SQL)
            return
        columns = {row[1] for row in conn.execute("PRAGMA table_info(crowd_requests)").fetchall()}
        required = {
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
        }
        table_sql = (table[0] or "").upper()
        if required.issubset(columns):
            return
        conn.execute("ALTER TABLE crowd_requests RENAME TO crowd_requests_legacy")
        conn.execute(REQUEST_TABLE_SQL)
        legacy_columns = {row[1] for row in conn.execute("PRAGMA table_info(crowd_requests_legacy)").fetchall()}
        copy_columns = [
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
        ]
        shared = [column for column in copy_columns if column in legacy_columns]
        conn.execute(
            f"INSERT INTO crowd_requests ({', '.join(shared)}) SELECT {', '.join(shared)} FROM crowd_requests_legacy"
        )
        conn.execute("DROP TABLE crowd_requests_legacy")

    def _ensure_settings_schema(self, conn: sqlite3.Connection) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(crowd_request_settings)").fetchall()}
        additions = {
            "buymeacoffee": "TEXT NOT NULL DEFAULT ''",
            "chime": "TEXT NOT NULL DEFAULT ''",
            "card_url": "TEXT NOT NULL DEFAULT ''",
        }
        for column, column_def in additions.items():
            if column not in columns:
                conn.execute(f"ALTER TABLE crowd_request_settings ADD COLUMN {column} {column_def}")

    def _refresh_join_url(self) -> None:
        candidates: list[str] = []
        try:
            hostname = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "localhost"
            for family, _, _, _, sockaddr in __import__("socket").getaddrinfo(hostname, None):
                if family != __import__("socket").AF_INET:
                    continue
                address = sockaddr[0]
                try:
                    ip = ipaddress.ip_address(address)
                except ValueError:
                    continue
                if ip.is_loopback:
                    continue
                if ip.is_private:
                    candidates.append(address)
        except Exception:
            pass
        self.join_host = candidates[0] if candidates else "localhost"
        self.join_url = f"http://{self.join_host}:{self.port}"

    def qr_png(self) -> bytes:
        if self._qr_png and self._qr_join_url == self.join_url:
            return self._qr_png
        return self._qr_png_for_value(self.join_url)

    def _qr_png_for_value(self, value: str) -> bytes:
        qr = qrcode.QRCode(box_size=8, border=2)
        qr.add_data(value)
        qr.make(fit=True)
        image = qr.make_image(fill_color="black", back_color="white")
        buffer = io.BytesIO()
        image.save(buffer, "PNG")
        if value == self.join_url:
            self._qr_png = buffer.getvalue()
            self._qr_join_url = self.join_url
            return self._qr_png
        return buffer.getvalue()

    def _payment_target(self, method: str, value: str) -> str:
        text = sanitize_text(value, max_len=512)
        if not text:
            return ""
        if text.startswith(("http://", "https://", "mailto:", "tel:")):
            return text
        if method == "venmo":
            return f"https://account.venmo.com/u/{text.lstrip('@')}"
        if method == "cashapp":
            handle = text if text.startswith("$") else f"${text.lstrip('$')}"
            return f"https://cash.app/{handle}"
        if method == "buymeacoffee":
            return f"https://buymeacoffee.com/{text.lstrip('@')}"
        if method == "card_url":
            return text
        return text

    def payment_methods(self) -> list[dict[str, str]]:
        handles = self.get_settings()["payment_handles"]
        definitions = [
            ("venmo", "Venmo", "Send to this Venmo profile."),
            ("cashapp", "Cash App", "Send to this Cash App handle."),
            ("paypal", "PayPal", "Send to this PayPal email or link."),
            ("zelle", "Zelle", "Send to this Zelle email or phone."),
            ("buymeacoffee", "Buy Me a Coffee", "Open this Buy Me a Coffee page."),
            ("chime", "Chime", "Send to this Chime sign or payment tag."),
            ("card_url", "Debit / Credit Card", "Open this externally configured card checkout link."),
        ]
        methods: list[dict[str, str]] = []
        for key, label, detail in definitions:
            raw_value = sanitize_text(handles.get(key), max_len=512)
            if not raw_value:
                continue
            target = self._payment_target(key, raw_value)
            methods.append({
                "key": key,
                "label": label,
                "value": raw_value,
                "target": target,
                "detail": detail,
                "target_is_url": "true" if target.startswith(("http://", "https://")) else "false",
            })
        return methods

    def payment_qr_png(self, method: str) -> bytes:
        for entry in self.payment_methods():
            if entry["key"] == method:
                return self._qr_png_for_value(entry["target"] or entry["value"])
        return b""

    def is_local_client(self, client_ip: str) -> bool:
        try:
            ip = ipaddress.ip_address(client_ip)
        except ValueError:
            return False
        return ip.is_loopback or ip.is_private

    def _audit(self, event_type: str, request_id: str | None, detail: dict[str, Any]) -> None:
        payload = json.dumps(detail, ensure_ascii=True, sort_keys=True)
        logging.info("AUDIT %s request=%s %s", event_type, request_id or "-", payload)
        with self._db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    "INSERT INTO crowd_request_audit (event_type, request_id, detail_json, created_at) VALUES (?, ?, ?, ?)",
                    (event_type, request_id, payload, utc_now()),
                )
                conn.commit()
            finally:
                conn.close()

    def get_settings(self) -> dict[str, Any]:
        with self._db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT request_policy, venmo, cashapp, paypal, zelle, buymeacoffee, chime, card_url, updated_at FROM crowd_request_settings WHERE settings_id = 1"
                ).fetchone()
            finally:
                conn.close()
        return {
            "request_policy": row["request_policy"],
            "payment_handles": {
                "venmo": row["venmo"],
                "cashapp": row["cashapp"],
                "paypal": row["paypal"],
                "zelle": row["zelle"],
                "buymeacoffee": row["buymeacoffee"],
                "chime": row["chime"],
                "card_url": row["card_url"],
            },
            "updated_at": row["updated_at"],
        }

    def update_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        handles = payload.get("payment_handles") or {}
        policy = sanitize_text(payload.get("request_policy"), max_len=16).lower() or "free"
        if policy not in {"free", "paid", "either"}:
            raise ValueError("Invalid request_policy")
        row = {
            "request_policy": policy,
            "venmo": sanitize_text(handles.get("venmo"), max_len=120),
            "cashapp": sanitize_text(handles.get("cashapp"), max_len=120),
            "paypal": sanitize_text(handles.get("paypal"), max_len=120),
            "zelle": sanitize_text(handles.get("zelle"), max_len=120),
            "buymeacoffee": sanitize_text(handles.get("buymeacoffee"), max_len=160),
            "chime": sanitize_text(handles.get("chime"), max_len=160),
            "card_url": sanitize_text(handles.get("card_url"), max_len=320),
            "updated_at": utc_now(),
        }
        with self._db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE crowd_request_settings
                    SET request_policy = ?, venmo = ?, cashapp = ?, paypal = ?, zelle = ?, buymeacoffee = ?, chime = ?, card_url = ?, updated_at = ?
                    WHERE settings_id = 1
                    """,
                    (
                        row["request_policy"],
                        row["venmo"],
                        row["cashapp"],
                        row["paypal"],
                        row["zelle"],
                        row["buymeacoffee"],
                        row["chime"],
                        row["card_url"],
                        row["updated_at"],
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        self._audit("SETTINGS_UPDATED", None, row)
        return self.get_settings()

    def _load_library(self) -> list[dict[str, Any]]:
        with self._library_lock:
            try:
                stat = self.library_json_path.stat()
            except FileNotFoundError:
                self._library_cache = []
                self._library_mtime_ns = -1
                return []
            if stat.st_mtime_ns == self._library_mtime_ns and self._library_cache:
                return self._library_cache
            try:
                payload = json.loads(self.library_json_path.read_text(encoding="utf-8"))
            except Exception:
                logging.exception("Failed to load library.json from %s", self.library_json_path)
                self._library_cache = []
                self._library_mtime_ns = stat.st_mtime_ns
                return []
            enrichment = self._load_search_enrichment()
            tracks = payload.get("tracks") or []
            loaded: list[dict[str, Any]] = []
            for index, item in enumerate(tracks):
                file_path = sanitize_text(item.get("filePath"), max_len=1024)
                file_path_normalized = normalize_path_key(file_path)
                path_identity_key = stable_identity_key(file_path)
                title = sanitize_text(item.get("title") or item.get("displayName"), max_len=200)
                artist = sanitize_text(item.get("artist"), max_len=200)
                album = sanitize_text(item.get("album"), max_len=200)
                if not file_path or not title:
                    continue
                extra = enrichment.get(file_path_normalized, {})
                if not extra:
                    extra = enrichment.get(file_name_key(file_path), {})
                if not extra:
                    extra = enrichment.get(enrichment_name_key(artist, title), {})
                canonical_artist = sanitize_text(extra.get("canonical_artist"), max_len=200)
                canonical_title = sanitize_text(extra.get("canonical_title"), max_len=200)
                resolved_artist = sanitize_text(extra.get("resolved_artist"), max_len=200)
                resolved_title = sanitize_text(extra.get("resolved_title"), max_len=200)
                metadata_artist = sanitize_text(extra.get("metadata_artist"), max_len=200)
                metadata_title = sanitize_text(extra.get("metadata_title"), max_len=200)
                filename_artist = sanitize_text(extra.get("filename_artist"), max_len=200)
                filename_title = sanitize_text(extra.get("filename_title"), max_len=200)
                authority_track_id = sanitize_text(extra.get("track_id"), max_len=80)
                authority_match_basis = sanitize_text(extra.get("authority_match_basis"), max_len=64).lower()
                identity_confidence = sanitize_text(extra.get("identity_confidence"), max_len=32).lower()
                if path_identity_key:
                    if not identity_confidence:
                        identity_confidence = "strong"
                    if not authority_match_basis:
                        authority_match_basis = "runtime_path"
                elif authority_track_id:
                    identity_confidence = identity_confidence or "reconciled"
                    authority_match_basis = authority_match_basis or "authority_track_id"
                else:
                    identity_confidence = identity_confidence or "degraded"
                    authority_match_basis = authority_match_basis or "artist_title"
                file_stem = strip_extension(Path(file_path).name)
                search_fields = [
                    title,
                    artist,
                    album,
                    file_path,
                    file_stem,
                    canonical_artist,
                    canonical_title,
                    resolved_artist,
                    resolved_title,
                    metadata_artist,
                    metadata_title,
                    filename_artist,
                    filename_title,
                ]
                loaded.append(
                    {
                        "track_id": authority_track_id,
                        "authority_track_id": authority_track_id,
                        "library_index": index,
                        "file_path": file_path,
                        "file_path_normalized": file_path_normalized,
                        "stable_identity_key": path_identity_key,
                        "identity_confidence": identity_confidence,
                        "identity_match_basis": authority_match_basis,
                        "title": title,
                        "artist": artist,
                        "album": album,
                        "bpm": sanitize_text(item.get("bpm"), max_len=32),
                        "key": sanitize_text(item.get("musicalKey"), max_len=32),
                        "clean_status": sanitize_text(extra.get("clean_status"), max_len=32) or "UNKNOWN",
                        "is_primary": bool(extra.get("is_primary")),
                        "requires_review": bool(extra.get("requires_review")),
                        "metadata_junk": bool(extra.get("metadata_junk")),
                        "parse_confidence": float(extra.get("parse_confidence") or 0.0),
                        "canonical_artist": canonical_artist,
                        "canonical_title": canonical_title,
                        "resolved_artist": resolved_artist,
                        "resolved_title": resolved_title,
                        "metadata_artist": metadata_artist,
                        "metadata_title": metadata_title,
                        "filename_artist": filename_artist,
                        "filename_title": filename_title,
                        "_file_stem": file_stem,
                        "_search_fields": [normalize_text(value) for value in search_fields if sanitize_text(value, max_len=512)],
                    }
                )
            self._library_cache = loaded
            self._library_mtime_ns = stat.st_mtime_ns
            return loaded

    def _load_search_enrichment(self) -> dict[str, dict[str, Any]]:
        try:
            stat = self.library_core_db_path.stat()
        except FileNotFoundError:
            self._search_enrichment_cache = {}
            self._search_enrichment_mtime_ns = -1
            return {}
        if stat.st_mtime_ns == self._search_enrichment_mtime_ns and self._search_enrichment_cache:
            return self._search_enrichment_cache
        conn = sqlite3.connect(self.library_core_db_path)
        conn.row_factory = sqlite3.Row
        try:
            artist_rows = conn.execute(
                "SELECT canonical_artist, normalized_artist FROM authority_artists"
            ).fetchall()
            title_rows = conn.execute(
                "SELECT canonical_title, normalized_title FROM authority_titles"
            ).fetchall()
            artist_map = {row["normalized_artist"]: row["canonical_artist"] for row in artist_rows if row["normalized_artist"]}
            title_map = {row["normalized_title"]: row["canonical_title"] for row in title_rows if row["normalized_title"]}
            rows = conn.execute(
                """
                SELECT t.track_id,
                       t.file_path,
                       t.file_name,
                       COALESCE(ts.status, '') AS clean_status,
                       COALESCE(ts.is_primary, 0) AS is_primary,
                       COALESCE(hr.requires_review, 0) AS requires_review,
                       COALESCE(mt.metadata_junk_flag, 0) AS metadata_junk,
                       COALESCE(mt.artist_tag, '') AS artist_tag,
                       COALESCE(mt.title_tag, '') AS title_tag,
                       COALESCE(fp.artist_guess, '') AS filename_artist_guess,
                       COALESCE(fp.title_guess, '') AS filename_title_guess,
                       COALESCE(lp.resolved_artist, '') AS resolved_artist,
                       COALESCE(lp.resolved_title, '') AS resolved_title,
                       COALESCE(lp.final_confidence, 0.0) AS parse_confidence
                FROM tracks t
                LEFT JOIN track_status ts ON ts.track_id = t.track_id
                LEFT JOIN hybrid_resolution hr ON hr.track_id = t.track_id
                LEFT JOIN metadata_tags mt ON mt.track_id = t.track_id
                LEFT JOIN filename_parse fp ON fp.track_id = t.track_id
                LEFT JOIN (
                    SELECT aph.track_id,
                           aph.resolved_artist,
                           aph.resolved_title,
                           aph.final_confidence
                    FROM authority_parse_history aph
                    INNER JOIN (
                        SELECT track_id, MAX(parse_history_id) AS max_parse_history_id
                        FROM authority_parse_history
                        GROUP BY track_id
                    ) latest
                    ON latest.track_id = aph.track_id
                   AND latest.max_parse_history_id = aph.parse_history_id
                ) lp ON lp.track_id = t.track_id
                """
            ).fetchall()
        finally:
            conn.close()
        enrichment: dict[str, dict[str, Any]] = {}
        file_name_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
        name_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            metadata_artist = sanitize_text(row["artist_tag"], max_len=200)
            metadata_title = sanitize_text(row["title_tag"], max_len=200)
            filename_artist = sanitize_text(row["filename_artist_guess"], max_len=200)
            filename_title = sanitize_text(row["filename_title_guess"], max_len=200)
            resolved_artist = sanitize_text(row["resolved_artist"], max_len=200)
            resolved_title = sanitize_text(row["resolved_title"], max_len=200)
            file_name = sanitize_text(row["file_name"], max_len=255)
            if metadata_title and not metadata_artist:
                split_artist, split_title = split_artist_title(metadata_title)
                metadata_artist = metadata_artist or split_artist
                metadata_title = split_title
            if not filename_title:
                split_artist, split_title = split_artist_title(strip_extension(file_name))
                filename_artist = filename_artist or split_artist
                filename_title = filename_title or split_title
            candidate_artist = normalize_text(resolved_artist or metadata_artist or filename_artist)
            candidate_title = normalize_text(resolved_title or metadata_title or filename_title)
            canonical_artist = artist_map.get(candidate_artist, "")
            canonical_title = title_map.get(candidate_title, "")
            entry = {
                "track_id": row["track_id"],
                "clean_status": row["clean_status"],
                "is_primary": bool(row["is_primary"]),
                "requires_review": bool(row["requires_review"]),
                "metadata_junk": bool(row["metadata_junk"]),
                "metadata_artist": metadata_artist,
                "metadata_title": metadata_title,
                "filename_artist": filename_artist,
                "filename_title": filename_title,
                "resolved_artist": resolved_artist,
                "resolved_title": resolved_title,
                "canonical_artist": canonical_artist,
                "canonical_title": canonical_title,
                "parse_confidence": row["parse_confidence"],
            }
            path_key = normalize_path_key(row["file_path"])
            if path_key:
                enrichment[path_key] = {
                    **entry,
                    "authority_match_basis": "db_path",
                    "identity_confidence": "strong",
                }
            filename_key_value = file_name_key(row["file_name"] or row["file_path"])
            if filename_key_value:
                file_name_candidates[filename_key_value].append(entry)
            for artist_value, title_value in (
                (resolved_artist, resolved_title),
                (metadata_artist, metadata_title),
                (filename_artist, filename_title),
                (canonical_artist, canonical_title),
            ):
                if normalize_text(title_value):
                    name_key = enrichment_name_key(artist_value, title_value)
                    name_candidates[name_key].append(entry)
        for key, candidates in file_name_candidates.items():
            unique = {sanitize_text(candidate.get("track_id"), max_len=80): candidate for candidate in candidates if sanitize_text(candidate.get("track_id"), max_len=80)}
            if len(unique) == 1:
                entry = next(iter(unique.values()))
                enrichment[key] = {
                    **entry,
                    "authority_match_basis": "db_filename",
                    "identity_confidence": "reconciled",
                }
        for key, candidates in name_candidates.items():
            unique = {sanitize_text(candidate.get("track_id"), max_len=80): candidate for candidate in candidates if sanitize_text(candidate.get("track_id"), max_len=80)}
            if len(unique) == 1:
                entry = next(iter(unique.values()))
                enrichment[key] = {
                    **entry,
                    "authority_match_basis": "artist_title",
                    "identity_confidence": "degraded",
                }
        self._search_enrichment_cache = enrichment
        self._search_enrichment_mtime_ns = stat.st_mtime_ns
        return enrichment

    def search(self, query: str) -> list[dict[str, Any]]:
        needle = normalize_text(query)
        if len(needle) < 2:
            return []
        tokens = needle.split()
        results: list[tuple[int, dict[str, Any]]] = []
        deduped: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}
        for track in self._load_library():
            fields = track["_search_fields"]
            if needle not in " ".join(fields) and not all(any(token in field for field in fields) for token in tokens):
                continue
            score = 0
            title_fields = [
                normalize_text(track["title"]),
                normalize_text(track["canonical_title"]),
                normalize_text(track["resolved_title"]),
                normalize_text(track["metadata_title"]),
                normalize_text(track["filename_title"]),
            ]
            artist_fields = [
                normalize_text(track["artist"]),
                normalize_text(track["canonical_artist"]),
                normalize_text(track["resolved_artist"]),
                normalize_text(track["metadata_artist"]),
                normalize_text(track["filename_artist"]),
            ]
            if any(field == needle for field in title_fields if field):
                score += 120
            if any(field == needle for field in artist_fields if field):
                score += 90
            if all(any(token in field for field in title_fields + artist_fields if field) for token in tokens):
                score += 70
            title_hits = sum(1 for token in tokens if any(token in field for field in title_fields if field))
            artist_hits = sum(1 for token in tokens if any(token in field for field in artist_fields if field))
            score += title_hits * 24
            score += artist_hits * 18
            for field in title_fields + artist_fields:
                if field.startswith(needle):
                    score += 18
            status = track["clean_status"]
            if status == "CLEAN":
                score += 110
            elif status == "REVIEW":
                score += 25
            elif status == "JUNK":
                score -= 90
            elif status == "LONGFORM":
                score -= 75
            if track["is_primary"]:
                score += 24
            if track["metadata_junk"]:
                score -= 35
            if track["requires_review"]:
                score -= 18
            score += int(track["parse_confidence"] * 35)
            if contains_junk_marker(track["title"]) or contains_junk_marker(track["_file_stem"]):
                score -= 20
            if re.search(r"\bexplicit\b", normalize_text(track["_file_stem"])):
                score -= 6
            if len(track["title"]) > 80:
                score -= 12
            dedupe_key = (
                normalize_text(track["canonical_artist"] or track["resolved_artist"] or track["artist"]),
                normalize_text(track["canonical_title"] or track["resolved_title"] or track["title"]),
            )
            existing = deduped.get(dedupe_key)
            if existing is None or score > existing[0]:
                deduped[dedupe_key] = (score, track)
        results = list(deduped.values())
        results.sort(key=lambda item: item[0], reverse=True)
        return [
            {
                "track_id": item[1]["track_id"],
                "authority_track_id": item[1]["authority_track_id"],
                "stable_identity_key": item[1]["stable_identity_key"],
                "file_path": item[1]["file_path"],
                "file_path_normalized": item[1]["file_path_normalized"],
                "title": item[1]["title"],
                "artist": item[1]["artist"],
                "album": item[1]["album"],
                "bpm": item[1]["bpm"],
                "key": item[1]["key"],
                "clean_status": item[1]["clean_status"],
                "identity_confidence": item[1]["identity_confidence"],
                "identity_match_basis": item[1]["identity_match_basis"],
            }
            for item in results[:25]
        ]

    def _rate_limit(self, bucket: dict[str, deque[float]], client_ip: str, limit: int, window_s: int) -> None:
        now = time.time()
        with self._rate_lock:
            slots = bucket[client_ip]
            while slots and now - slots[0] > window_s:
                slots.popleft()
            if len(slots) >= limit:
                raise ValueError("Rate limit exceeded")
            slots.append(now)

    def _request_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "request_id": row["request_id"],
            "track_id": row["track_id"],
            "authority_track_id": row["authority_track_id"],
            "stable_identity_key": row["stable_identity_key"],
            "file_path": row["file_path"],
            "file_path_normalized": row["file_path_normalized"],
            "requested_title": row["requested_title"],
            "requested_artist": row["requested_artist"],
            "requester_name": row["requester_name"],
            "votes": row["votes"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "identity_confidence": row["identity_confidence"],
            "identity_match_basis": row["identity_match_basis"],
            "handoff_deck": row["handoff_deck"],
            "handoff_detail": row["handoff_detail"],
            "handoff_target_path": row["handoff_target_path"],
        }

    def _request_identity_from_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        file_path = sanitize_text(payload.get("file_path"), max_len=1024)
        file_path_normalized = sanitize_text(payload.get("file_path_normalized"), max_len=2048) or normalize_path_key(file_path)
        identity_key = sanitize_text(payload.get("stable_identity_key"), max_len=4096) or stable_identity_key(file_path_normalized or file_path)
        authority_track_id = sanitize_text(payload.get("authority_track_id") or payload.get("track_id"), max_len=80)
        confidence = sanitize_text(payload.get("identity_confidence"), max_len=32).lower()
        match_basis = sanitize_text(payload.get("identity_match_basis"), max_len=64).lower()
        if identity_key:
            confidence = confidence or "strong"
            match_basis = match_basis or "runtime_path"
        elif authority_track_id:
            confidence = confidence or "reconciled"
            match_basis = match_basis or "authority_track_id"
        else:
            confidence = confidence or "degraded"
            match_basis = match_basis or "artist_title"
        return {
            "file_path": file_path,
            "file_path_normalized": file_path_normalized,
            "stable_identity_key": identity_key,
            "authority_track_id": authority_track_id,
            "identity_confidence": confidence,
            "identity_match_basis": match_basis,
        }

    def _request_identity_from_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, str]:
        file_path = sanitize_text(row["file_path"], max_len=1024) if row["file_path"] else ""
        handoff_target_path = sanitize_text(row["handoff_target_path"], max_len=1024) if row["handoff_target_path"] else ""
        file_path_normalized = sanitize_text(row["file_path_normalized"], max_len=2048) if row["file_path_normalized"] else ""
        if not file_path_normalized:
            file_path_normalized = normalize_path_key(handoff_target_path or file_path)
        identity_key = sanitize_text(row["stable_identity_key"], max_len=4096) if row["stable_identity_key"] else ""
        if not identity_key and file_path_normalized:
            identity_key = stable_identity_key(file_path_normalized)
        authority_track_id = sanitize_text(row["authority_track_id"] or row["track_id"], max_len=80)
        confidence = sanitize_text(row["identity_confidence"], max_len=32).lower() if row["identity_confidence"] else ""
        match_basis = sanitize_text(row["identity_match_basis"], max_len=64).lower() if row["identity_match_basis"] else ""
        if identity_key:
            confidence = confidence or "strong"
            match_basis = match_basis or "runtime_path"
        elif authority_track_id:
            confidence = confidence or "reconciled"
            match_basis = match_basis or "authority_track_id"
        else:
            confidence = confidence or "degraded"
            match_basis = match_basis or "artist_title"
        return {
            "file_path": file_path,
            "handoff_target_path": handoff_target_path,
            "file_path_normalized": file_path_normalized,
            "stable_identity_key": identity_key,
            "authority_track_id": authority_track_id,
            "identity_confidence": confidence,
            "identity_match_basis": match_basis,
        }

    def _status_counts(self, queue: list[dict[str, Any]]) -> dict[str, int]:
        counts = {status: 0 for status in REQUEST_STATUSES}
        for item in queue:
            status = item.get("status") or "PENDING"
            counts[status] = counts.get(status, 0) + 1
        return counts

    def _status_counts_from_db(self) -> dict[str, int]:
        counts = {status: 0 for status in REQUEST_STATUSES}
        with self._db_lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT status, COUNT(*) AS row_count FROM crowd_requests WHERE status != 'REMOVED' GROUP BY status"
                ).fetchall()
            finally:
                conn.close()
        for row in rows:
            status = row["status"] or "PENDING"
            counts[status] = int(row["row_count"])
        return counts

    def _playback_candidate_rows(self, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        return conn.execute(
            """
            SELECT * FROM crowd_requests
            WHERE status IN ('ACCEPTED', 'HANDED_OFF', 'NOW_PLAYING')
            ORDER BY created_at ASC
            """
        ).fetchall()

    def _request_path_candidates(self, row: sqlite3.Row) -> list[str]:
        candidates = []
        for value in (row["file_path_normalized"], row["handoff_target_path"], row["file_path"]):
            normalized = normalize_path_key(value or "")
            if normalized and normalized not in candidates:
                candidates.append(normalized)
        return candidates

    def _match_request_for_playback(self, candidates: list[sqlite3.Row], deck_state: dict[str, Any]) -> tuple[sqlite3.Row | None, str, str]:
        deck_identity_key = sanitize_text(deck_state.get("stable_identity_key"), max_len=4096)
        if deck_identity_key:
            exact = [row for row in candidates if self._request_identity_from_row(row)["stable_identity_key"] == deck_identity_key]
            if len(exact) == 1:
                return exact[0], "stable_identity_key", ""
            if len(exact) > 1:
                return None, "", "Multiple active requests matched the same live stable identity key."

        deck_authority_track_id = sanitize_text(deck_state.get("authority_track_id"), max_len=80)
        if deck_authority_track_id:
            exact = [row for row in candidates if self._request_identity_from_row(row)["authority_track_id"] == deck_authority_track_id]
            if len(exact) == 1:
                return exact[0], "authority_track_id", ""
            if len(exact) > 1:
                return None, "", "Multiple active requests matched the same authority track id."

        deck_path = sanitize_text(deck_state.get("file_path_normalized"), max_len=2048) or normalize_path_key(deck_state.get("file_path") or "")
        if deck_path:
            exact = [row for row in candidates if deck_path in self._request_path_candidates(row)]
            if len(exact) == 1:
                return exact[0], "file_path_normalized", ""
            if len(exact) > 1:
                return None, "", "Multiple active requests matched the same live deck file path."

        artist = normalize_text(deck_state.get("artist") or "")
        title = normalize_text(deck_state.get("title") or "")
        if artist and title:
            fallback = [
                row for row in candidates
                if normalize_text(row["requested_artist"] or "") == artist
                and normalize_text(row["requested_title"] or "") == title
            ]
            if fallback:
                confidences = {self._request_identity_from_row(row)["identity_confidence"] for row in fallback}
                if len(fallback) == 1 and confidences <= {"degraded", "unresolved"}:
                    return None, "", "Only degraded artist/title identity was available, so playback mapping failed closed."
                if len(fallback) > 1:
                    return None, "", "Multiple active requests matched the live deck title/artist fallback."

        return None, "", "No accepted or handed-off request matched the active deck playback state."

    def queue(self) -> list[dict[str, Any]]:
        with self._db_lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    """
                      SELECT request_id, track_id, authority_track_id, stable_identity_key, file_path, file_path_normalized,
                          requested_title, requested_artist,
                           requester_name, votes, status, created_at, updated_at,
                          identity_confidence, identity_match_basis,
                           handoff_deck, handoff_detail, handoff_target_path
                    FROM crowd_requests
                    WHERE status IN ('PENDING', 'ACCEPTED', 'HANDED_OFF', 'HANDOFF_FAILED', 'REJECTED')
                    ORDER BY CASE status
                        WHEN 'PENDING' THEN 0
                        WHEN 'ACCEPTED' THEN 1
                        WHEN 'HANDED_OFF' THEN 2
                        WHEN 'HANDOFF_FAILED' THEN 3
                        WHEN 'REJECTED' THEN 4
                        ELSE 5 END,
                        votes DESC,
                        created_at ASC
                    """
                ).fetchall()
            finally:
                conn.close()
        return [self._request_row_to_dict(row) for row in rows]

    def submit_request(self, payload: dict[str, Any], client_ip: str) -> dict[str, Any]:
        self._rate_limit(self._recent_submit, client_ip, limit=SUBMIT_RATE_LIMIT[0], window_s=SUBMIT_RATE_LIMIT[1])
        settings = self.get_settings()
        request_policy = sanitize_text(settings.get("request_policy"), max_len=16).lower() or "free"
        method_keys = {entry["key"] for entry in self.payment_methods()}
        payment_method = sanitize_text(payload.get("payment_method"), max_len=64).lower()
        payment_reference = sanitize_text(payload.get("payment_reference"), max_len=160)
        if payment_method and payment_method not in method_keys:
            raise ValueError("Selected payment method is not available")
        if request_policy == "paid":
            if not method_keys:
                raise ValueError("Paid mode is enabled but no payment methods are configured yet")
            if not payment_method:
                raise ValueError("Select a payment method before sending your request")
            if not payment_reference:
                raise ValueError("Payment confirmation is required in paid mode")
        elif request_policy == "either":
            if payment_reference and not payment_method:
                raise ValueError("Select a payment method when adding a payment confirmation")
            if payment_method and not payment_reference:
                raise ValueError("Add a payment confirmation note for the selected method")

        title = sanitize_text(payload.get("requested_title") or payload.get("title"), max_len=200)
        artist = sanitize_text(payload.get("requested_artist") or payload.get("artist"), max_len=200)
        requester = sanitize_text(payload.get("requester_name"), max_len=120) or "Anonymous"
        identity = self._request_identity_from_payload(payload)
        file_path = identity["file_path"] or None
        track_id = sanitize_text(payload.get("track_id") or identity["authority_track_id"], max_len=80) or None
        authority_track_id = identity["authority_track_id"] or None
        if not title:
            raise ValueError("requested_title is required")
        norm_title = normalize_text(title)
        norm_artist = normalize_text(artist)
        now = time.time()
        with self._rate_lock:
            last_submit = self._submit_cooldowns.get(client_ip)
            if last_submit is not None and now - last_submit < SUBMIT_COOLDOWN_SECONDS:
                wait_for = int(SUBMIT_COOLDOWN_SECONDS - (now - last_submit)) + 1
                raise ValueError(f"Please wait {wait_for}s before sending another request")
            self._submit_cooldowns[client_ip] = now

        with self._db_lock:
            conn = self._connect()
            try:
                duplicate_since = datetime.fromtimestamp(now - DUPLICATE_WINDOW_SECONDS, timezone.utc).isoformat()
                existing = None
                if identity["stable_identity_key"]:
                    existing = conn.execute(
                        """
                        SELECT request_id, status FROM crowd_requests
                        WHERE stable_identity_key = ?
                          AND status IN ('PENDING', 'ACCEPTED', 'HANDED_OFF', 'HANDOFF_FAILED')
                          AND created_at >= ?
                        LIMIT 1
                        """,
                        (identity["stable_identity_key"], duplicate_since),
                    ).fetchone()
                if existing is None and authority_track_id:
                    existing = conn.execute(
                        """
                        SELECT request_id, status FROM crowd_requests
                        WHERE authority_track_id = ?
                          AND status IN ('PENDING', 'ACCEPTED', 'HANDED_OFF', 'HANDOFF_FAILED')
                          AND created_at >= ?
                        LIMIT 1
                        """,
                        (authority_track_id, duplicate_since),
                    ).fetchone()
                if existing is None:
                    existing = conn.execute(
                        """
                        SELECT request_id, status FROM crowd_requests
                        WHERE normalized_title = ?
                          AND normalized_artist = ?
                          AND status IN ('PENDING', 'ACCEPTED', 'HANDED_OFF', 'HANDOFF_FAILED')
                          AND created_at >= ?
                        LIMIT 1
                        """,
                        (norm_title, norm_artist, duplicate_since),
                    ).fetchone()
                if existing:
                    raise ValueError("That track was already requested recently on this local queue")
                request_id = secrets.token_hex(12)
                created_at = utc_now()
                conn.execute(
                    """
                    INSERT INTO crowd_requests (
                        request_id, track_id, authority_track_id, stable_identity_key, file_path, file_path_normalized,
                        requested_title, requested_artist,
                        requester_name, votes, status, created_at, updated_at,
                        normalized_title, normalized_artist, identity_confidence, identity_match_basis, requester_ip
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'PENDING', ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        track_id,
                        authority_track_id,
                        identity["stable_identity_key"] or None,
                        file_path,
                        identity["file_path_normalized"] or None,
                        title,
                        artist,
                        requester,
                        created_at,
                        created_at,
                        norm_title,
                        norm_artist,
                        identity["identity_confidence"],
                        identity["identity_match_basis"],
                        client_ip,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                conn.commit()
            finally:
                conn.close()
        request = self._request_row_to_dict(row)
        self._audit("REQUEST_SUBMITTED", request_id, request)
        if payment_method:
            self._audit("REQUEST_PAYMENT_DECLARED", request_id, {
                "payment_method": payment_method,
                "payment_reference": payment_reference,
                "request_policy": request_policy,
            })
        return request

    def vote(self, request_id: str, payload: dict[str, Any], client_ip: str) -> dict[str, Any]:
        self._rate_limit(self._recent_vote, client_ip, limit=VOTE_RATE_LIMIT[0], window_s=VOTE_RATE_LIMIT[1])
        delta = payload.get("delta", 1)
        try:
            delta_int = int(delta)
        except Exception as exc:
            raise ValueError("delta must be an integer") from exc
        if delta_int not in {-1, 1}:
            raise ValueError("delta must be -1 or 1")
        cooldown_key = f"{client_ip}:{request_id}"
        now_ts = time.time()
        with self._rate_lock:
            last_vote = self._vote_cooldowns.get(cooldown_key)
            if last_vote is not None and now_ts - last_vote < VOTE_COOLDOWN_SECONDS:
                wait_for = int(VOTE_COOLDOWN_SECONDS - (now_ts - last_vote)) + 1
                raise ValueError(f"Please wait {wait_for}s before voting on this request again")
            self._vote_cooldowns[cooldown_key] = now_ts
        with self._db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row is None or row["status"] != "PENDING":
                    raise ValueError("Request is not available for voting")
                new_votes = max(0, row["votes"] + delta_int)
                now = utc_now()
                conn.execute(
                    "UPDATE crowd_requests SET votes = ?, updated_at = ? WHERE request_id = ?",
                    (new_votes, now, request_id),
                )
                updated = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                conn.commit()
            finally:
                conn.close()
        request = self._request_row_to_dict(updated)
        self._audit("REQUEST_VOTED", request_id, {"delta": delta_int, "client_ip": client_ip, "votes": request["votes"]})
        return request

    def operator_update(self, request_id: str, new_status: str) -> dict[str, Any]:
        if new_status not in {"ACCEPTED", "REJECTED", "REMOVED"}:
            raise ValueError("Unsupported operator status")
        with self._db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row is None:
                    raise ValueError("Request not found")
                current_status = row["status"]
                if new_status == "ACCEPTED" and current_status not in {"PENDING", "HANDOFF_FAILED"}:
                    raise ValueError("Only pending or failed requests can be accepted to a deck")
                if new_status == "REJECTED" and current_status not in {"PENDING", "ACCEPTED", "HANDOFF_FAILED"}:
                    raise ValueError("This request can no longer be rejected")
                if new_status == "REMOVED" and current_status == "REMOVED":
                    raise ValueError("Request is already removed")
                conn.execute(
                    "UPDATE crowd_requests SET status = ?, updated_at = ?, handoff_detail = CASE WHEN ? = 'ACCEPTED' THEN '' ELSE handoff_detail END, handoff_deck = CASE WHEN ? = 'ACCEPTED' THEN NULL ELSE handoff_deck END, handoff_target_path = CASE WHEN ? = 'ACCEPTED' THEN NULL ELSE handoff_target_path END WHERE request_id = ?",
                    (new_status, utc_now(), new_status, new_status, new_status, request_id),
                )
                updated = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                conn.commit()
            finally:
                conn.close()
        request = self._request_row_to_dict(updated)
        self._audit(f"REQUEST_{new_status}", request_id, request)
        return request

    def operator_handoff(self, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        new_status = sanitize_text(payload.get("status"), max_len=32).upper()
        if new_status not in {"HANDED_OFF", "HANDOFF_FAILED"}:
            raise ValueError("Unsupported handoff status")
        handoff_deck = sanitize_text(payload.get("deck"), max_len=8).upper() or None
        handoff_detail = sanitize_text(payload.get("detail"), max_len=240)
        handoff_target_path = sanitize_text(payload.get("target_path"), max_len=1024) or None
        with self._db_lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if row is None:
                    raise ValueError("Request not found")
                if row["status"] != "ACCEPTED":
                    raise ValueError("Request is not in an accepted handoff state")
                conn.execute(
                    "UPDATE crowd_requests SET status = ?, updated_at = ?, handoff_deck = ?, handoff_detail = ?, handoff_target_path = ? WHERE request_id = ?",
                    (new_status, utc_now(), handoff_deck, handoff_detail, handoff_target_path, request_id),
                )
                updated = conn.execute(
                    "SELECT * FROM crowd_requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                conn.commit()
            finally:
                conn.close()
        request = self._request_row_to_dict(updated)
        self._audit(f"REQUEST_{new_status}", request_id, request)
        return request

    def clear_queue(self) -> None:
        with self._db_lock:
            conn = self._connect()
            try:
                conn.execute(
                    "UPDATE crowd_requests SET status = 'REMOVED', updated_at = ?, handoff_detail = '', handoff_deck = NULL WHERE status != 'REMOVED'",
                    (utc_now(),),
                )
                conn.commit()
            finally:
                conn.close()
        self._audit("QUEUE_CLEARED", None, {"cleared_at": utc_now()})

    def _select_authoritative_deck(
        self,
        active_decks: list[dict[str, Any]],
        previous_authoritative_deck: str,
    ) -> tuple[dict[str, Any] | None, bool, str]:
        if not active_decks:
            return None, False, ""
        if len(active_decks) == 1:
            only = active_decks[0]
            return only, False, f"Deck {only['deck']} is currently authoritative."

        ranked = sorted(
            active_decks,
            key=lambda entry: (
                int(entry.get("signal_bucket", 0)),
                float(entry.get("peak_level", 0.0)),
                entry.get("deck", ""),
            ),
            reverse=True,
        )
        strongest = ranked[0]
        runner_up = ranked[1]
        previous = next((entry for entry in active_decks if entry.get("deck") == previous_authoritative_deck), None)

        strongest_peak = float(strongest.get("peak_level", 0.0))
        runner_up_peak = float(runner_up.get("peak_level", 0.0))
        strongest_bucket = int(strongest.get("signal_bucket", 0))
        runner_up_bucket = int(runner_up.get("signal_bucket", 0))

        if previous is not None:
            previous_peak = float(previous.get("peak_level", 0.0))
            previous_bucket = int(previous.get("signal_bucket", 0))
            if previous_bucket >= 1 and previous_peak >= ARBITRATION_SIGNAL_FLOOR:
                if previous.get("deck") == strongest.get("deck") or (
                    strongest_bucket <= previous_bucket
                    or strongest_peak - previous_peak < ARBITRATION_SIGNAL_MARGIN
                ):
                    return previous, False, (
                        f"Deck {previous['deck']} remains authoritative while cross-deck overlap settles."
                    )

        if strongest_bucket > runner_up_bucket and strongest_bucket >= 1:
            return strongest, False, (
                f"Deck {strongest['deck']} is authoritative; Deck {runner_up['deck']} remains standby."
            )

        if strongest_peak >= ARBITRATION_SIGNAL_FLOOR and strongest_peak - runner_up_peak >= ARBITRATION_SIGNAL_MARGIN:
            return strongest, False, (
                f"Deck {strongest['deck']} is authoritative by stronger live signal; Deck {runner_up['deck']} remains standby."
            )

        return None, True, (
            "Cross-deck overlap is active, but live authority is ambiguous so request playback was left unchanged."
        )

    def update_now_playing(self, payload: dict[str, Any]) -> dict[str, Any]:
        decks_payload = payload.get("active_decks") or []
        if not isinstance(decks_payload, list):
            raise ValueError("active_decks must be a list")

        active_decks: list[dict[str, Any]] = []
        for item in decks_payload:
            if not isinstance(item, dict):
                continue
            if not bool(item.get("is_playing")):
                continue
            deck_label = sanitize_text(item.get("deck"), max_len=8).upper()
            if deck_label not in {"A", "B"}:
                continue
            active_decks.append({
                "deck": deck_label,
                "title": sanitize_text(item.get("title"), max_len=200),
                "artist": sanitize_text(item.get("artist"), max_len=200),
                "meta": sanitize_text(item.get("meta"), max_len=200),
                "file_path": sanitize_text(item.get("file_path"), max_len=1024),
                "file_path_normalized": sanitize_text(item.get("file_path_normalized"), max_len=2048) or normalize_path_key(item.get("file_path") or ""),
                "stable_identity_key": sanitize_text(item.get("stable_identity_key"), max_len=4096) or stable_identity_key(item.get("file_path_normalized") or item.get("file_path") or ""),
                "authority_track_id": sanitize_text(item.get("authority_track_id"), max_len=80),
                "identity_confidence": sanitize_text(item.get("identity_confidence"), max_len=32).lower() or ("strong" if item.get("file_path") else "unresolved"),
                "identity_match_basis": sanitize_text(item.get("identity_match_basis"), max_len=64).lower() or ("runtime_path" if item.get("file_path") else "unresolved"),
                "is_playing": True,
                "peak_level": float(item.get("peak_level") or 0.0),
                "signal_bucket": int(item.get("signal_bucket") or 0),
            })

        now = utc_now()
        matched_requests: list[dict[str, Any]] = []
        resolved_playing_ids: set[str] = set()
        display_detail = ""
        playback_audits: list[tuple[str, str, dict[str, Any]]] = []
        enriched_decks: list[dict[str, Any]] = []
        previous_authoritative_deck = sanitize_text(self._now_playing.get("authoritative_deck", ""), max_len=8).upper()
        authoritative_deck, is_ambiguous, arbitration_detail = self._select_authoritative_deck(
            active_decks,
            previous_authoritative_deck,
        )

        with self._db_lock:
            conn = self._connect()
            try:
                candidates = self._playback_candidate_rows(conn)
                currently_playing_rows = {
                    row["request_id"]: row
                    for row in conn.execute(
                        "SELECT * FROM crowd_requests WHERE status = 'NOW_PLAYING'"
                    ).fetchall()
                }
                provisional_matches: list[tuple[dict[str, Any], sqlite3.Row | None, str, str]] = []
                for deck_state in active_decks:
                    row, match_basis, detail = self._match_request_for_playback(candidates, deck_state)
                    provisional_matches.append((deck_state, row, match_basis, detail))

                duplicate_match_ids = {
                    row["request_id"]
                    for _, row, _, _ in provisional_matches
                    if row is not None
                    if sum(1 for _, other_row, _, _ in provisional_matches if other_row is not None and other_row["request_id"] == row["request_id"]) > 1
                }

                for deck_state, row, match_basis, detail in provisional_matches:
                    entry = dict(deck_state)
                    if row is not None and row["request_id"] in duplicate_match_ids:
                        detail = "The same request matched more than one active deck, so playback mapping was left unresolved."
                        row = None
                        match_basis = ""
                    is_authoritative = authoritative_deck is not None and deck_state["deck"] == authoritative_deck["deck"]
                    entry["is_authoritative"] = is_authoritative
                    if authoritative_deck is None:
                        entry["role"] = "ambiguous" if len(active_decks) > 1 else "inactive"
                        if not detail:
                            detail = arbitration_detail
                    elif is_authoritative:
                        entry["role"] = "live"
                        detail = arbitration_detail or f"Deck {deck_state['deck']} is authoritative."
                    else:
                        entry["role"] = "standby"
                        detail = f"Deck {deck_state['deck']} is active but not authoritative for guest-facing now playing."
                    if row is not None and is_authoritative:
                        request_id = row["request_id"]
                        resolved_playing_ids.add(request_id)
                        entry["request_id"] = request_id
                        entry["match_basis"] = match_basis
                        entry["request_status"] = "NOW_PLAYING"
                        matched_requests.append({
                            "request_id": request_id,
                            "deck": deck_state["deck"],
                            "requested_title": row["requested_title"],
                            "requested_artist": row["requested_artist"],
                            "requester_name": row["requester_name"],
                            "status": "NOW_PLAYING",
                            "match_basis": match_basis,
                            "stable_identity_key": entry.get("stable_identity_key", ""),
                            "authority_track_id": entry.get("authority_track_id", ""),
                            "detail": f"Now playing on Deck {deck_state['deck']}.",
                        })
                        if row["status"] != "NOW_PLAYING":
                            conn.execute(
                                "UPDATE crowd_requests SET status = 'NOW_PLAYING', updated_at = ? WHERE request_id = ?",
                                (now, request_id),
                            )
                            playback_audits.append((
                                "REQUEST_NOW_PLAYING",
                                request_id,
                                {
                                    "deck": deck_state["deck"],
                                    "file_path": deck_state["file_path"],
                                    "file_path_normalized": deck_state.get("file_path_normalized", ""),
                                    "stable_identity_key": deck_state.get("stable_identity_key", ""),
                                    "authority_track_id": deck_state.get("authority_track_id", ""),
                                    "match_basis": match_basis,
                                },
                            ))
                    else:
                        entry["request_id"] = ""
                        entry["match_basis"] = ""
                        entry["request_status"] = ""
                        entry["detail"] = detail
                    enriched_decks.append(entry)

                for request_id, row in currently_playing_rows.items():
                    if request_id in resolved_playing_ids:
                        continue
                    conn.execute(
                        "UPDATE crowd_requests SET status = 'PLAYED', updated_at = ? WHERE request_id = ?",
                        (now, request_id),
                    )
                    playback_audits.append((
                        "REQUEST_PLAYED",
                        request_id,
                        {
                            "resolved_at": now,
                            "reason": "Playback moved away from this request-backed track.",
                        },
                    ))

                conn.commit()
            finally:
                conn.close()

        for event_type, request_id, detail in playback_audits:
            self._audit(event_type, request_id, detail)

        if is_ambiguous:
            self._audit(
                "NOW_PLAYING_ARBITRATION_AMBIGUOUS",
                None,
                {
                    "detail": arbitration_detail,
                    "active_decks": [
                        {
                            "deck": entry["deck"],
                            "file_path": entry["file_path"],
                            "file_path_normalized": entry.get("file_path_normalized", ""),
                            "stable_identity_key": entry.get("stable_identity_key", ""),
                            "authority_track_id": entry.get("authority_track_id", ""),
                            "peak_level": entry.get("peak_level", 0.0),
                            "signal_bucket": entry.get("signal_bucket", 0),
                        }
                        for entry in enriched_decks
                    ],
                },
            )

        if authoritative_deck is not None:
            display_detail = arbitration_detail or f"Deck {authoritative_deck['deck']} is currently authoritative."
        elif active_decks:
            display_detail = arbitration_detail or "No deck reached authoritative live status."
        elif self._now_playing.get("active_decks"):
            display_detail = "No deck is currently authoritative."

        if authoritative_deck is not None:
            summary_title = authoritative_deck.get("title", "")
            summary_artist = authoritative_deck.get("artist", "")
            summary_meta = authoritative_deck.get("meta", "")
            summary_deck = authoritative_deck.get("deck", "")
            summary_file_path = authoritative_deck.get("file_path", "")
            summary_file_path_normalized = authoritative_deck.get("file_path_normalized", "")
            summary_stable_identity_key = authoritative_deck.get("stable_identity_key", "")
            summary_authority_track_id = authoritative_deck.get("authority_track_id", "")
            summary_identity_confidence = authoritative_deck.get("identity_confidence", "unresolved")
            summary_identity_match_basis = authoritative_deck.get("identity_match_basis", "unresolved")
        else:
            summary_title = ""
            summary_artist = ""
            summary_meta = ""
            summary_deck = ""
            summary_file_path = ""
            summary_file_path_normalized = ""
            summary_stable_identity_key = ""
            summary_authority_track_id = ""
            summary_identity_confidence = "unresolved"
            summary_identity_match_basis = "unresolved"

        self._now_playing = {
            "title": summary_title,
            "artist": summary_artist,
            "meta": summary_meta,
            "deck": summary_deck,
            "file_path": summary_file_path,
            "file_path_normalized": summary_file_path_normalized,
            "stable_identity_key": summary_stable_identity_key,
            "authority_track_id": summary_authority_track_id,
            "identity_confidence": summary_identity_confidence,
            "identity_match_basis": summary_identity_match_basis,
            "authoritative_deck": summary_deck,
            "active_decks": enriched_decks,
            "requests": matched_requests,
            "is_ambiguous": is_ambiguous,
            "detail": display_detail,
            "updated_at": now,
        }
        return dict(self._now_playing)

    def health(self) -> dict[str, Any]:
        queue = self.queue()
        counts = self._status_counts_from_db()
        return {
            "running": True,
            "bind": self.bind,
            "port": self.port,
            "join_url": self.join_url,
            "qr_path": "/qr.png",
            "pending_count": counts.get("PENDING", 0),
            "request_count": counts.get("PENDING", 0) + counts.get("ACCEPTED", 0) + counts.get("HANDED_OFF", 0) + counts.get("HANDOFF_FAILED", 0) + counts.get("REJECTED", 0) + counts.get("NOW_PLAYING", 0),
            "accepted_count": counts.get("ACCEPTED", 0),
            "handed_off_count": counts.get("HANDED_OFF", 0),
            "now_playing_count": counts.get("NOW_PLAYING", 0),
            "played_count": counts.get("PLAYED", 0),
            "handoff_failed_count": counts.get("HANDOFF_FAILED", 0),
            "library_json": str(self.library_json_path),
            "library_core_db": str(self.library_core_db_path),
            "db_path": str(self.db_path),
            "now_playing": dict(self._now_playing),
            "antispam_policy": {
                "submit_rate_limit": {"limit": SUBMIT_RATE_LIMIT[0], "window_seconds": SUBMIT_RATE_LIMIT[1]},
                "submit_cooldown_seconds": SUBMIT_COOLDOWN_SECONDS,
                "duplicate_window_seconds": DUPLICATE_WINDOW_SECONDS,
                "vote_rate_limit": {"limit": VOTE_RATE_LIMIT[0], "window_seconds": VOTE_RATE_LIMIT[1]},
                "vote_cooldown_seconds": VOTE_COOLDOWN_SECONDS,
                "client_identifier": "requester_ip",
            },
        }

    def guest_page(self) -> str:
        settings = self.get_settings()
        policy = settings["request_policy"]
        payment_methods = self.payment_methods()
        payment_heading = {
            "paid": "Payment required before you send a request.",
            "either": "Payment is optional, but the DJ can prioritize paid requests.",
        }.get(policy, "")
        payment_method_options = "".join(
            f'<option value="{html.escape(method["key"])}">{html.escape(method["label"])} ({html.escape(method["value"])})</option>'
            for method in payment_methods
        )
        payment_capture_section = ""
        if policy in {"paid", "either"}:
            payment_capture_section = f"""
                        <div class=\"payment-capture\">
                            <div class=\"small\">{html.escape(payment_heading) or 'Select a payment method if you paid for request priority.'}</div>
                            <select id=\"paymentMethod\">
                                <option value=\"\">{html.escape('Choose payment method (required)' if policy == 'paid' else 'No payment selected')}</option>
                                {payment_method_options}
                            </select>
                            <input id=\"paymentReference\" placeholder=\"{html.escape('Payment confirmation (required): last 4, initials, or note' if policy == 'paid' else 'Optional payment confirmation note')}\">
                        </div>
            """
        payment_cards = "".join(
            f"""
            <div class=\"payment-card\">
                <div class=\"payment-head\">
                    <strong>{html.escape(method['label'])}</strong>
                    <span class=\"small\">{html.escape(method['detail'])}</span>
                </div>
                <div class=\"payment-body\">
                    <img src=\"/payment-qr.png?method={html.escape(method['key'])}\" alt=\"{html.escape(method['label'])} QR\">
                    <div class=\"payment-meta\">
                        <div class=\"small\">{html.escape(method['value'])}</div>
                        {f'<a class=\"payment-link\" href=\"{html.escape(method["target"])}\" target=\"_blank\" rel=\"noreferrer\">Open payment link</a>' if method['target_is_url'] == 'true' else ''}
                    </div>
                </div>
            </div>
            """
            for method in payment_methods
        )
        payment_section = ""
        if policy in {"paid", "either"}:
            if payment_cards:
                payment_section = f"""
        <section class=\"display-panel display-panel-wide payment-panel\">
            <div class=\"queue-head\">
                <h2>Payment Methods</h2>
                <div class=\"muted\">{html.escape(payment_heading)}</div>
            </div>
            <div class=\"small\">Debit / credit card payments require the DJ to configure their own checkout link.</div>
            <div class=\"payment-grid\">{payment_cards}</div>
        </section>
                """
            else:
                payment_section = f"""
        <section class=\"display-panel display-panel-wide payment-panel\">
            <h2>Payment Methods</h2>
            <div class=\"callout muted\">{html.escape(payment_heading)} The DJ has not configured payment methods yet.</div>
        </section>
                """
        return f"""<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <title>NGKs Crowd Requests</title>
    <style>
        :root {{
            --cream: #f5ead5;
            --sand: #dcc59a;
            --amber: #f3a319;
            --amber-soft: #ffd36d;
            --coral: #ff6b3d;
            --teal: #2ec4b6;
            --teal-soft: #93efe5;
            --wood: #6b2f18;
            --wood-deep: #3b170d;
            --chrome: #cfd6de;
            --chrome-shadow: #8794a3;
            --ink: #1d130d;
            --panel: rgba(52, 20, 10, 0.82);
            --panel-line: rgba(255, 214, 133, 0.35);
            --glow: 0 0 18px rgba(255, 176, 55, 0.45);
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            min-height: 100vh;
            padding: 24px 16px 40px;
            font-family: "Trebuchet MS", "Gill Sans", "Segoe UI", sans-serif;
            color: var(--cream);
            background: #000;
        }}
        h1, h2, h3 {{ margin: 0; }}
        input, button {{ font: inherit; }}
        input {{
            width: 100%;
            padding: 13px 16px;
            border-radius: 14px;
            border: 1px solid rgba(255, 211, 109, 0.28);
            background: rgba(18, 9, 6, 0.78);
            color: var(--cream);
            margin-bottom: 10px;
            box-shadow: inset 0 0 0 1px rgba(255, 248, 228, 0.05);
        }}
        input::placeholder {{ color: rgba(245, 234, 213, 0.52); }}
        button {{
            padding: 11px 16px;
            border: 1px solid rgba(255, 224, 154, 0.28);
            border-radius: 999px;
            background: linear-gradient(180deg, #ffbb3a 0%, #f4801f 100%);
            color: #2f1407;
            cursor: pointer;
            font-weight: 800;
            letter-spacing: 0.02em;
            box-shadow: 0 8px 18px rgba(35, 10, 2, 0.28), inset 0 1px 0 rgba(255, 246, 214, 0.75);
        }}
        button.secondary {{
            background: linear-gradient(180deg, #9ba8b8 0%, #667385 100%);
            color: #fff6df;
        }}
        .row {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
        .grid {{ display: grid; gap: 12px; }}
        .jukebox-wrap {{
            max-width: 920px;
            margin: 0 auto;
            position: relative;
        }}
        .jukebox-shell {{
            position: relative;
            border-radius: 54px 54px 28px 28px;
            padding: 22px 22px 28px;
            background:
                linear-gradient(180deg, rgba(255, 188, 78, 0.92) 0%, rgba(239, 115, 33, 0.96) 21%, rgba(118, 42, 18, 0.98) 58%, rgba(62, 21, 10, 1) 100%);
            box-shadow:
                0 32px 80px rgba(27, 10, 5, 0.42),
                inset 0 2px 0 rgba(255, 238, 193, 0.85),
                inset 0 -12px 32px rgba(23, 8, 3, 0.45);
            overflow: hidden;
        }}
        .jukebox-shell::before {{
            content: "";
            position: absolute;
            inset: 12px 12px auto 12px;
            height: 210px;
            border-radius: 48px 48px 22px 22px;
            background:
                radial-gradient(circle at 50% 18%, rgba(255, 245, 221, 0.46), transparent 34%),
                linear-gradient(180deg, rgba(255, 210, 122, 0.76) 0%, rgba(255, 118, 58, 0.58) 40%, rgba(69, 24, 11, 0.2) 100%);
            pointer-events: none;
            box-shadow: inset 0 0 32px rgba(255, 204, 94, 0.45);
        }}
        .jukebox-shell::after {{
            content: "";
            position: absolute;
            left: 50%;
            transform: translateX(-50%);
            bottom: 0;
            width: calc(100% - 52px);
            height: 28px;
            border-radius: 16px 16px 0 0;
            background: linear-gradient(180deg, rgba(24, 10, 6, 0.15), rgba(16, 5, 3, 0.65));
        }}
        .jukebox-arch {{
            position: relative;
            z-index: 1;
            padding: 26px 18px 20px;
            border-radius: 42px 42px 20px 20px;
            background:
                linear-gradient(180deg, rgba(24, 12, 7, 0.72), rgba(20, 8, 5, 0.92));
            border: 7px solid rgba(207, 214, 222, 0.7);
            box-shadow:
                inset 0 0 0 2px rgba(255, 255, 255, 0.32),
                inset 0 0 0 10px rgba(255, 162, 39, 0.18),
                var(--glow);
        }}
        .jukebox-neon {{
            position: absolute;
            inset: 12px;
            border-radius: 34px 34px 14px 14px;
            border: 3px solid rgba(255, 201, 85, 0.72);
            box-shadow:
                0 0 14px rgba(255, 186, 57, 0.46),
                inset 0 0 18px rgba(255, 130, 50, 0.32);
            pointer-events: none;
        }}
        .hero-top {{ display: flex; gap: 14px; align-items: flex-start; justify-content: space-between; flex-wrap: wrap; }}
        .brand-mark {{
            display: inline-block;
            margin-bottom: 8px;
            padding: 4px 12px;
            border-radius: 999px;
            background: rgba(46, 196, 182, 0.15);
            border: 1px solid rgba(46, 196, 182, 0.42);
            color: var(--teal-soft);
            font-size: 11px;
            font-weight: 800;
            letter-spacing: 0.18em;
            text-transform: uppercase;
        }}
        .hero-copy h1 {{
            font-size: clamp(2rem, 4vw, 3.1rem);
            line-height: 0.96;
            color: #fff4d7;
            text-shadow: 0 2px 0 rgba(60, 19, 8, 0.45), 0 0 24px rgba(255, 176, 55, 0.26);
        }}
        .hero-copy p {{ margin: 10px 0 0; max-width: 560px; color: rgba(245, 234, 213, 0.82); font-size: 14px; }}
        .policy-badge {{
            display: inline-flex;
            align-items: center;
            padding: 7px 14px;
            border-radius: 999px;
            background: linear-gradient(180deg, rgba(46, 196, 182, 0.2), rgba(17, 96, 87, 0.42));
            border: 1px solid rgba(124, 241, 228, 0.4);
            color: #d4fff9;
            font-size: 12px;
            font-weight: 800;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}
        .display-grid {{
            position: relative;
            z-index: 1;
            display: grid;
            grid-template-columns: 1.15fr 0.85fr;
            gap: 16px;
            margin-top: 18px;
        }}
        .display-panel {{
            position: relative;
            padding: 18px;
            border-radius: 22px;
            background: linear-gradient(180deg, rgba(33, 13, 7, 0.82), rgba(18, 8, 4, 0.92));
            border: 1px solid var(--panel-line);
            box-shadow: inset 0 0 0 1px rgba(255, 243, 218, 0.05), inset 0 0 26px rgba(255, 178, 52, 0.08);
        }}
        .display-panel::before {{
            content: "";
            position: absolute;
            top: 12px;
            bottom: 12px;
            left: -11px;
            width: 8px;
            border-radius: 999px;
            background: linear-gradient(180deg, #fdf8ee 0%, #b3bdc7 40%, #fdf8ee 100%);
            box-shadow: 0 0 0 1px rgba(68, 33, 18, 0.24), 0 0 14px rgba(255, 219, 128, 0.24);
        }}
        .display-panel::after {{
            content: "";
            position: absolute;
            top: 12px;
            bottom: 12px;
            right: -11px;
            width: 8px;
            border-radius: 999px;
            background: linear-gradient(180deg, #fdf8ee 0%, #b3bdc7 40%, #fdf8ee 100%);
            box-shadow: 0 0 0 1px rgba(68, 33, 18, 0.24), 0 0 14px rgba(255, 219, 128, 0.24);
        }}
        .display-panel-wide {{ grid-column: 1 / -1; }}
        .display-panel h2 {{
            margin-bottom: 8px;
            color: #fff1cc;
            font-size: clamp(1.4rem, 2.8vw, 2rem);
            text-shadow: 0 0 16px rgba(255, 183, 60, 0.18);
        }}
        .hero-panel {{ min-height: 100%; }}
        .display-subtitle {{ color: rgba(245, 234, 213, 0.78); font-size: 13px; line-height: 1.5; margin-bottom: 12px; }}
        .result, .queue-item {{
            border: 1px solid rgba(255, 204, 109, 0.22);
            border-radius: 16px;
            padding: 13px 14px;
            margin-top: 12px;
            background: linear-gradient(180deg, rgba(82, 31, 16, 0.5), rgba(31, 13, 7, 0.66));
            box-shadow: inset 0 1px 0 rgba(255, 244, 218, 0.04);
        }}
        .muted {{ color: rgba(245, 234, 213, 0.62); font-size: 12px; }}
        .votes {{ font-weight: 800; color: var(--teal-soft); text-shadow: 0 0 10px rgba(46, 196, 182, 0.28); }}
        .pill {{ display: inline-block; padding: 5px 10px; border-radius: 999px; background: rgba(255, 205, 111, 0.12); color: #fff0c7; font-size: 11px; border: 1px solid rgba(255, 205, 111, 0.25); font-weight: 800; letter-spacing: 0.06em; text-transform: uppercase; }}
        .pill.accepted {{ background: rgba(117, 219, 135, 0.14); color: #d9ffd7; border-color: rgba(117, 219, 135, 0.24); }}
        .pill.handed_off {{ background: rgba(92, 177, 255, 0.15); color: #e2f1ff; border-color: rgba(92, 177, 255, 0.24); }}
        .pill.handoff_failed {{ background: rgba(255, 110, 86, 0.16); color: #ffd8cf; border-color: rgba(255, 110, 86, 0.22); }}
        .pill.now_playing {{ background: rgba(255, 180, 61, 0.2); color: #fff0c2; border-color: rgba(255, 180, 61, 0.3); }}
        .pill.played {{ background: rgba(189, 197, 205, 0.12); color: #ebeff3; border-color: rgba(189, 197, 205, 0.22); }}
        .pill.rejected {{ background: rgba(192, 101, 78, 0.16); color: #ffd1c8; border-color: rgba(192, 101, 78, 0.22); }}
        .pill.pending {{ background: rgba(46, 196, 182, 0.14); color: #d5fff8; border-color: rgba(46, 196, 182, 0.22); }}
        .toolbar {{ display: flex; gap: 8px; flex-wrap: wrap; }}
        .status {{ min-height: 20px; font-size: 13px; color: #ffe5a6; margin-top: 8px; font-weight: 700; }}
        .confirm {{ color: #a7fff3; }}
        .error {{ color: #ffc2b8; }}
        .queue-grid {{ display: grid; gap: 10px; }}
        .queue-head {{ display: flex; justify-content: space-between; align-items: center; gap: 10px; }}
        .qr-row {{ display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
        .qr-card {{
            padding: 14px;
            border-radius: 20px;
            background: linear-gradient(180deg, #fffaf0 0%, #edd9ab 100%);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.9), 0 10px 24px rgba(26, 10, 4, 0.24);
        }}
        .qr-card img {{ width: 118px; height: 118px; display: block; }}
        .join-block {{ display: grid; gap: 8px; }}
        .join-label {{ color: var(--amber-soft); font-size: 12px; font-weight: 800; letter-spacing: 0.14em; text-transform: uppercase; }}
        .join-url {{ color: #fff7e1; font-weight: 700; word-break: break-word; }}
        .small {{ font-size: 11px; color: rgba(245, 234, 213, 0.6); }}
        .now-playing {{ display: grid; gap: 4px; }}
        .callout {{ border-left: 3px solid var(--teal); padding-left: 10px; }}
        .queue-item.mine {{ border-color: rgba(46, 196, 182, 0.72); box-shadow: inset 0 0 0 1px rgba(46, 196, 182, 0.32), 0 0 18px rgba(46, 196, 182, 0.14); }}
        .payment-grid {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-top: 12px; }}
        .payment-card {{ border: 1px solid rgba(255, 205, 111, 0.24); border-radius: 16px; background: linear-gradient(180deg, rgba(82, 31, 16, 0.5), rgba(31, 13, 7, 0.7)); padding: 14px; display: grid; gap: 10px; }}
        .payment-head {{ display: grid; gap: 4px; }}
        .payment-body {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
        .payment-body img {{ width: 108px; height: 108px; border-radius: 12px; background: #fff6df; padding: 6px; box-shadow: inset 0 1px 0 rgba(255,255,255,0.8); }}
        .payment-meta {{ display: grid; gap: 8px; min-width: 0; }}
        .payment-link {{ color: #ffdb8f; font-size: 12px; }}
        .payment-capture {{
            margin-top: 10px;
            padding: 10px;
            border: 1px solid rgba(255, 205, 111, 0.24);
            border-radius: 12px;
            background: linear-gradient(180deg, rgba(82, 31, 16, 0.4), rgba(31, 13, 7, 0.55));
            display: grid;
            gap: 8px;
        }}
        select {{
            width: 100%;
            padding: 12px 14px;
            border-radius: 12px;
            border: 1px solid rgba(255, 211, 109, 0.28);
            background: rgba(18, 9, 6, 0.78);
            color: var(--cream);
        }}
        .speaker-row {{
            position: relative;
            z-index: 1;
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            gap: 18px;
            margin-top: 18px;
        }}
        .speaker {{
            width: 126px;
            aspect-ratio: 1 / 1;
            border-radius: 50%;
            background:
                radial-gradient(circle at 50% 50%, rgba(255, 209, 119, 0.6) 0 8%, rgba(53, 16, 8, 0.92) 9% 23%, rgba(203, 211, 220, 0.94) 24% 28%, rgba(41, 16, 9, 0.94) 29% 60%, rgba(214, 221, 228, 0.85) 61% 67%, rgba(53, 24, 13, 1) 68% 100%);
            box-shadow: inset 0 0 18px rgba(0, 0, 0, 0.35), 0 12px 24px rgba(26, 10, 4, 0.26);
            opacity: 0.92;
            flex: 0 0 auto;
        }}
        .speaker-center {{
            flex: 1 1 auto;
            min-width: 0;
            padding: 10px 18px 0;
        }}
        .speaker-center .muted {{ text-align: center; }}
        @media (max-width: 640px) {{
            body {{ padding: 14px 10px 28px; }}
            .jukebox-shell {{ padding: 16px 14px 22px; border-radius: 36px 36px 20px 20px; }}
            .jukebox-arch {{ padding: 18px 12px 16px; border-radius: 28px 28px 16px 16px; }}
            .display-grid {{ grid-template-columns: 1fr; gap: 14px; }}
            .hero-top, .queue-head {{ align-items: flex-start; }}
            .speaker-row {{ gap: 10px; margin-top: 14px; }}
            .speaker {{ width: 82px; }}
            .speaker-center {{ padding: 4px 6px 0; }}
            button {{ width: 100%; }}
            .toolbar button {{ width: auto; flex: 1 1 120px; }}
            .display-panel::before, .display-panel::after {{ display: none; }}
        }}
    </style>
</head>
<body>
    <div class=\"jukebox-wrap\">
        <div class=\"jukebox-shell\">
            <div class=\"jukebox-arch\">
                <div class=\"jukebox-neon\"></div>
                <div class=\"hero-top\">
                    <div class=\"hero-copy\">
                        <div class=\"brand-mark\">NGKs Selection Wall</div>
                        <h1>NGKs Crowd Requests</h1>
                        <p>Styled like a classic jukebox front-end so guests can browse, request, and vote from the same glowing cabinet.</p>
                    </div>
                    <div id=\"policyBadge\" class=\"policy-badge\">Policy: {html.escape(policy)}</div>
                </div>
                <div class=\"display-grid\">
                    <section class=\"display-panel hero-panel\">
                        <div class=\"qr-row\">
                            <div class=\"qr-card\"><img src=\"/qr.png\" alt=\"Join QR code\"></div>
                            <div class=\"join-block\">
                                <div class=\"join-label\">Join URL</div>
                                <div class=\"join-url\">{self.join_url}</div>
                                <div class=\"small\">Guest refresh polls every 1 second; deck authority is pushed from native deck events.</div>
                                <div id=\"nowPlaying\" class=\"now-playing\"></div>
                            </div>
                        </div>
                    </section>
                    <section class=\"display-panel\">
                        <h2>Selection Booth</h2>
                        <div class=\"display-subtitle\">Request your track, leave your name, and refresh the board if the room gets busy.</div>
                        <input id=\"requester\" placeholder=\"Your name\">
                        <input id=\"query\" placeholder=\"Search title or artist\">
                        {payment_capture_section}
                        <div class=\"toolbar\">
                            <button id=\"searchBtn\">Search</button>
                            <button id=\"refreshBtn\" class=\"secondary\">Refresh Queue</button>
                        </div>
                        <div id=\"status\" class=\"status\"></div>
                        <div id=\"requestState\" class=\"small\"></div>
                    </section>
                    <section class=\"display-panel display-panel-wide\">
                        <div class=\"queue-head\">
                            <h2>Live Queue</h2>
                            <div id=\"queueSummary\" class=\"muted\">Loading queue…</div>
                        </div>
                        <div id=\"queue\" class=\"queue-grid\"></div>
                    </section>
                    <section class=\"display-panel display-panel-wide\">
                        <h2>Catalog Results</h2>
                        <div class=\"display-subtitle\">Clean primary matches stay near the top so the right record gets picked first.</div>
                        <div id=\"results\"></div>
                    </section>
                    {payment_section}
                </div>
                <div class=\"speaker-row\">
                    <div class=\"speaker\" aria-hidden=\"true\"></div>
                    <div class=\"speaker-center\">
                        <div class=\"muted\">Classic cabinet styling, live local queue underneath.</div>
                    </div>
                    <div class=\"speaker\" aria-hidden=\"true\"></div>
                </div>
            </div>
        </div>
    </div>
    <script>
        const resultsEl = document.getElementById('results');
        const queueEl = document.getElementById('queue');
        const queueSummaryEl = document.getElementById('queueSummary');
        const requesterEl = document.getElementById('requester');
        const statusEl = document.getElementById('status');
        const requestStateEl = document.getElementById('requestState');
        const nowPlayingEl = document.getElementById('nowPlaying');
        const policyBadgeEl = document.getElementById('policyBadge');
        const paymentMethodEl = document.getElementById('paymentMethod');
        const paymentReferenceEl = document.getElementById('paymentReference');
        const requestPolicy = {json.dumps(policy)};
        const availablePaymentMethods = {json.dumps([method['key'] for method in payment_methods])};
        let lastQueueFingerprint = '';
        const lastRequestStorageKey = 'ngks-crowd-last-request-id';

        function setStatus(message, kind = 'info') {{
            statusEl.textContent = message || '';
            statusEl.className = 'status ' + (kind === 'error' ? 'error' : (kind === 'confirm' ? 'confirm' : ''));
        }}

        function statusPill(status) {{
            const lower = (status || 'pending').toLowerCase();
            return `<span class=\"pill ${'{'}lower{'}'}\">${'{'}status{'}'}</span>`;
        }}

        async function fetchJson(url, options = {{}}) {{
            const response = await fetch(url, {{
                headers: {{'Content-Type': 'application/json'}},
                ...options,
            }});
            const data = await response.json();
            if (!response.ok || data.ok === false) {{
                throw new Error(data.error || 'Request failed');
            }}
            return data;
        }}

        function buildRequestPayload(track) {{
            const payload = {{
                requested_title: track.title,
                requested_artist: track.artist,
                requester_name: requesterEl.value.trim(),
                file_path: track.file_path,
                file_path_normalized: track.file_path_normalized,
                stable_identity_key: track.stable_identity_key,
                track_id: track.track_id,
                authority_track_id: track.authority_track_id,
                identity_confidence: track.identity_confidence,
                identity_match_basis: track.identity_match_basis,
            }};

            const paymentMethod = paymentMethodEl ? paymentMethodEl.value.trim() : '';
            const paymentReference = paymentReferenceEl ? paymentReferenceEl.value.trim() : '';
            if (requestPolicy === 'paid') {{
                if (!availablePaymentMethods.length) {{
                    throw new Error('Paid mode is enabled but payment methods are not configured yet.');
                }}
                if (!paymentMethod) {{
                    throw new Error('Select a payment method before sending your request.');
                }}
                if (!paymentReference) {{
                    throw new Error('Enter a payment confirmation before sending your request.');
                }}
            }}
            if (requestPolicy === 'either') {{
                if (paymentReference && !paymentMethod) {{
                    throw new Error('Choose a payment method when adding a payment confirmation.');
                }}
                if (paymentMethod && !paymentReference) {{
                    throw new Error('Add a payment confirmation note for the selected method.');
                }}
            }}
            if (paymentMethod) {{
                payload.payment_method = paymentMethod;
                payload.payment_reference = paymentReference;
            }}
            return payload;
        }}

        async function runSearch() {{
            const q = document.getElementById('query').value.trim();
            resultsEl.innerHTML = '';
            if (q.length < 2) {{
                setStatus('Enter at least 2 characters to search.', 'error');
                return;
            }}
            try {{
                setStatus('Searching local library…');
                const data = await fetchJson('/search?q=' + encodeURIComponent(q));
                if (!data.results.length) {{
                    setStatus('No local matches found for that search.', 'error');
                    resultsEl.innerHTML = '<div class="callout muted">No local matches found. Try a shorter artist or title fragment.</div>';
                    return;
                }}
                setStatus(`Found ${{data.results.length}} local matches.`, 'confirm');
                data.results.forEach((track) => {{
                    const item = document.createElement('div');
                    item.className = 'result';
                    item.innerHTML = `<strong>${'{'}track.title{'}'}</strong><div class="muted">${'{'}track.artist || 'Unknown Artist'{'}'}${'{'}track.album ? ' • ' + track.album : ''{'}'}${'{'}track.bpm ? ' • BPM ' + track.bpm : ''{'}'}${'{'}track.key ? ' • Key ' + track.key : ''{'}'}</div><div class="small">Quality: ${'{'}track.clean_status || 'UNKNOWN'{'}'}</div>`;
                    const button = document.createElement('button');
                    button.textContent = 'Request';
                    button.onclick = async () => {{
                        try {{
                            const response = await fetchJson('/request', {{
                                method: 'POST',
                                body: JSON.stringify(buildRequestPayload(track)),
                            }});
                            const request = response.request || {{}};
                            if (request.request_id) {{
                                localStorage.setItem(lastRequestStorageKey, request.request_id);
                            }}
                            setStatus(`Request sent: ${{track.title}} is now in the local queue.`, 'confirm');
                            requestStateEl.textContent = `Latest request: ${{track.title}} by ${{track.artist || 'Unknown Artist'}}.`;
                            await refreshQueue();
                        }} catch (error) {{
                            setStatus(error.message, 'error');
                        }}
                    }};
                    item.appendChild(button);
                    resultsEl.appendChild(item);
                }});
            }} catch (error) {{
                setStatus(error.message, 'error');
                resultsEl.textContent = error.message;
            }}
        }}

        async function refreshQueue() {{
            try {{
                const data = await fetchJson('/queue');
                if (policyBadgeEl && data.request_policy) {{
                    policyBadgeEl.textContent = `Policy: ${'{'}data.request_policy{'}'}`;
                }}
                const fingerprint = JSON.stringify(data.requests.map((item) => [item.request_id, item.votes, item.status, item.updated_at]));
                const counts = data.counts_by_status || {{}};
                queueSummaryEl.textContent = `${'{'}counts.NOW_PLAYING || 0{'}'} now playing / ${'{'}counts.PENDING || 0{'}'} pending / ${'{'}counts.ACCEPTED || 0{'}'} accepted / ${'{'}counts.HANDED_OFF || 0{'}'} loaded / ${'{'}counts.HANDOFF_FAILED || 0{'}'} failed`;
                const nowPlaying = data.now_playing || {{}};
                window.__lastNowPlaying = nowPlaying;
                const activeDecks = nowPlaying.active_decks || [];
                if (activeDecks.length) {{
                    nowPlayingEl.innerHTML = `<div><strong>Now Playing</strong></div>` + activeDecks.map((entry) => `
                        <div class="result">
                            <div><strong>Deck ${'{'}entry.deck{'}'}${'{'}entry.is_authoritative ? ' • Live' : (entry.role === 'standby' ? ' • Standby' : ' • Awaiting authority'){'}'}</strong></div>
                            <div class="muted">${'{'}entry.title || 'Unknown Title'{'}'}${'{'}entry.artist ? ' • ' + entry.artist : ''{'}'}</div>
                            <div class="small">${'{'}entry.meta || ''{'}'}</div>
                            <div class="small">${'{'}entry.request_id ? 'Request-backed playback' : (entry.detail || 'Non-request playback'){'}'}</div>
                        </div>`).join('');
                }} else if (nowPlaying.title || nowPlaying.artist || nowPlaying.meta) {{
                    nowPlayingEl.innerHTML = `<div><strong>Now Playing</strong></div><div class="muted">${'{'}nowPlaying.title || 'Unknown Title'{'}'}${'{'}nowPlaying.artist ? ' • ' + nowPlaying.artist : ''{'}'}</div><div class="small">${'{'}nowPlaying.meta || ''{'}'}</div><div class="small">${'{'}nowPlaying.detail || ''{'}'}</div>`;
                }} else {{
                    nowPlayingEl.innerHTML = '<div class="small">Now Playing becomes visible when a deck is actually playing and the operator app is online.</div>';
                }}
                if (fingerprint === lastQueueFingerprint) {{
                    updateMyRequestState(data.requests);
                    return;
                }}
                lastQueueFingerprint = fingerprint;
                if (!data.requests.length) {{
                    queueEl.innerHTML = '<div class="callout muted">No requests yet. Search above to send the first request.</div>';
                    updateMyRequestState([]);
                    return;
                }}
                queueEl.innerHTML = '';
                data.requests.forEach((item, index) => {{
                    const row = document.createElement('div');
                    row.className = 'queue-item';
                    const myRequestId = localStorage.getItem(lastRequestStorageKey);
                    if (myRequestId && item.request_id === myRequestId) {{
                        row.classList.add('mine');
                    }}
                    row.innerHTML = `
                        <div class="queue-head">\n              <strong>#${'{'}index + 1{'}'} ${'{'}item.requested_title{'}'}</strong>\n              ${'{'}statusPill(item.status){'}'}\n            </div>\n            <div class="muted">${'{'}item.requested_artist || 'Unknown Artist'{'}'} • Requested by ${'{'}item.requester_name{'}'}</div>\n            <div class="votes">Votes: ${'{'}item.votes{'}'}</div>\n            ${'{'}item.handoff_deck ? `<div class="small">Loaded on Deck ${'{'}item.handoff_deck{'}'}</div>` : ''{'}'}\n            ${'{'}item.handoff_detail ? `<div class="small">${'{'}item.handoff_detail{'}'}</div>` : ''{'}'}`;
                    const controls = document.createElement('div');
                    controls.className = 'row';
                    const up = document.createElement('button');
                    up.textContent = '+1';
                    up.disabled = item.status !== 'PENDING';
                    up.onclick = async () => {{
                        try {{
                            await fetchJson('/vote', {{ method: 'POST', body: JSON.stringify({{ request_id: item.request_id, delta: 1 }}) }});
                            setStatus(`Vote added to ${{item.requested_title}}.`, 'confirm');
                            await refreshQueue();
                        }} catch (error) {{
                            setStatus(error.message, 'error');
                        }}
                    }};
                    controls.appendChild(up);
                    row.appendChild(controls);
                    queueEl.appendChild(row);
                }});
                updateMyRequestState(data.requests);
            }} catch (error) {{
                setStatus(error.message, 'error');
                queueEl.textContent = error.message;
            }}
        }}

        function updateMyRequestState(items) {{
            const myRequestId = localStorage.getItem(lastRequestStorageKey);
            if (!myRequestId) {{
                requestStateEl.textContent = 'Your latest request status will appear here after you submit one.';
                return;
            }}
            const liveRequests = ((window.__lastNowPlaying && window.__lastNowPlaying.requests) || []);
            const match = items.find((item) => item.request_id === myRequestId) || liveRequests.find((item) => item.request_id === myRequestId);
            if (!match) {{
                requestStateEl.textContent = 'Your latest request is no longer visible in the active queue.';
                return;
            }}
            requestStateEl.textContent = `Your latest request is ${'{'}match.status{'}'}${'{'}(match.handoff_detail || match.detail) ? ' — ' + (match.handoff_detail || match.detail) : ''{'}'}.`;
        }}

        document.getElementById('searchBtn').addEventListener('click', runSearch);
        document.getElementById('refreshBtn').addEventListener('click', refreshQueue);
        document.getElementById('query').addEventListener('keydown', (event) => {{ if (event.key === 'Enter') runSearch(); }});
        window.__lastNowPlaying = {{ requests: [] }};
        refreshQueue();
        setInterval(refreshQueue, 1000);
    </script>
</body>
</html>
"""


class CrowdRequestHandler(BaseHTTPRequestHandler):
    server_version = "NGKsCrowdRequestServer/2.0"

    @property
    def state(self) -> CrowdRequestState:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        logging.info("HTTP %s - %s", self.address_string(), format % args)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, status: int, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _binary(self, status: int, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        if length > 16384:
            raise ValueError("Payload too large")
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid JSON payload") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON object payload required")
        return payload

    def _client_ip(self) -> str:
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",", 1)[0].strip()
        return self.client_address[0]

    def _ensure_local(self) -> bool:
        client_ip = self._client_ip()
        if self.state.is_local_client(client_ip):
            return True
        self._json(403, {"ok": False, "error": "Local network access only"})
        return False

    def _ensure_operator(self) -> bool:
        token = self.headers.get("X-Operator-Token", "")
        if token and secrets.compare_digest(token, self.state.operator_token):
            return True
        self._json(403, {"ok": False, "error": "Operator token required"})
        return False

    def do_GET(self) -> None:
        if not self._ensure_local():
            return
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._html(200, self.state.guest_page())
            return
        if parsed.path == "/qr.png":
            self._binary(200, self.state.qr_png(), "image/png")
            return
        if parsed.path == "/payment-qr.png":
            method = parse_qs(parsed.query).get("method", [""])[0]
            png = self.state.payment_qr_png(method)
            if not png:
                self.send_error(404)
                return
            self._binary(200, png, "image/png")
            return
        if parsed.path == "/health":
            self._json(200, {"ok": True, **self.state.health()})
            return
        if parsed.path == "/queue":
            queue = self.state.queue()
            counts = self.state._status_counts_from_db()
            settings = self.state.get_settings()
            self._json(200, {
                "ok": True,
                "requests": queue,
                "pending_count": counts.get("PENDING", 0),
                "counts_by_status": counts,
                "now_playing": dict(self.state._now_playing),
                "request_policy": settings.get("request_policy", "free"),
            })
            return
        if parsed.path == "/search":
            query = parse_qs(parsed.query).get("q", [""])[0]
            self._json(200, {"ok": True, "results": self.state.search(query)})
            return
        if parsed.path == "/settings":
            if not self._ensure_operator():
                return
            self._json(200, {"ok": True, **self.state.get_settings()})
            return
        self._json(404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        if not self._ensure_local():
            return
        parsed = urlparse(self.path)
        try:
            payload = self._read_json_body()
            if parsed.path == "/request":
                request = self.state.submit_request(payload, self._client_ip())
                self._json(200, {"ok": True, "request": request})
                return
            if parsed.path == "/vote":
                request_id = sanitize_text(payload.get("request_id"), max_len=64)
                if not request_id:
                    raise ValueError("request_id is required")
                request = self.state.vote(request_id, payload, self._client_ip())
                self._json(200, {"ok": True, "request": request})
                return
            if parsed.path == "/operator/settings":
                if not self._ensure_operator():
                    return
                settings = self.state.update_settings(payload)
                self._json(200, {"ok": True, **settings})
                return
            if parsed.path == "/operator/now-playing":
                if not self._ensure_operator():
                    return
                now_playing = self.state.update_now_playing(payload)
                self._json(200, {"ok": True, "now_playing": now_playing})
                return
            if parsed.path in {"/operator/accept", "/operator/reject", "/operator/remove"}:
                if not self._ensure_operator():
                    return
                request_id = sanitize_text(payload.get("request_id"), max_len=64)
                if not request_id:
                    raise ValueError("request_id is required")
                status_map = {
                    "/operator/accept": "ACCEPTED",
                    "/operator/reject": "REJECTED",
                    "/operator/remove": "REMOVED",
                }
                request = self.state.operator_update(request_id, status_map[parsed.path])
                self._json(200, {"ok": True, "request": request})
                return
            if parsed.path == "/operator/handoff":
                if not self._ensure_operator():
                    return
                request_id = sanitize_text(payload.get("request_id"), max_len=64)
                if not request_id:
                    raise ValueError("request_id is required")
                request = self.state.operator_handoff(request_id, payload)
                self._json(200, {"ok": True, "request": request})
                return
            if parsed.path == "/operator/clear":
                if not self._ensure_operator():
                    return
                self.state.clear_queue()
                self._json(200, {"ok": True})
                return
            if parsed.path == "/operator/shutdown":
                if not self._ensure_operator():
                    return
                self.state._audit("SERVER_SHUTDOWN", None, {"requested_at": utc_now()})
                self._json(200, {"ok": True})
                threading.Thread(target=self.server.shutdown, daemon=True).start()  # type: ignore[attr-defined]
                return
            self._json(404, {"ok": False, "error": "Not found"})
        except ValueError as exc:
            self._json(400, {"ok": False, "error": str(exc)})
        except Exception:
            logging.exception("Unhandled request failure")
            self._json(500, {"ok": False, "error": "Internal server error"})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NGKs local crowd request server")
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--library-json", required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--parent-pid", type=int, default=0)
    parser.add_argument("--operator-token", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    state = CrowdRequestState(args)
    state._audit("SERVER_START", None, {
        "bind": state.bind,
        "port": state.port,
        "parent_pid": state.parent_pid,
        "library_json": str(state.library_json_path),
        "db_path": str(state.db_path),
        "join_url": state.join_url,
    })
    server = ThreadingHTTPServer((state.bind, state.port), CrowdRequestHandler)
    server.state = state  # type: ignore[attr-defined]
    state.start_parent_watchdog(server)
    print(json.dumps({"ready": True, "port": state.port, "join_url": state.join_url}), flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())