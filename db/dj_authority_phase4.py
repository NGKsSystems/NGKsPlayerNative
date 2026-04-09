#!/usr/bin/env python3
"""
DJ Library Core — Phase 4: Music Authority Layer Foundation
=============================================================
Builds persistent authority tables for canonical artist/title truth,
seeds from trusted sources, and provides lookup/scoring helpers.

READ-ONLY on the filesystem. Only writes to SQLite DB and data/*.csv.
"""

import csv
import os
import re
import sqlite3
import sys
import unicodedata
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

BASE       = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA       = BASE / "data"
DB_PATH    = DATA / "dj_library_core.db"
PROOF_DIR  = BASE / "_proof" / "dj_library_core_phase4"
SEED_CSV   = DATA / "authority_seed_v1.csv"

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ═══════════════════════════════════════════════════════════════════
# PART A — DATABASE SCHEMA EXTENSION
# ═══════════════════════════════════════════════════════════════════

AUTHORITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS authority_artists (
    authority_artist_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_artist     TEXT    NOT NULL,
    normalized_artist    TEXT    NOT NULL,
    source               TEXT    NOT NULL,
    confidence           REAL    NOT NULL DEFAULT 0.0,
    times_seen           INTEGER NOT NULL DEFAULT 1,
    last_seen_timestamp  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_titles (
    authority_title_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_title      TEXT    NOT NULL,
    normalized_title     TEXT    NOT NULL,
    source               TEXT    NOT NULL,
    confidence           REAL    NOT NULL DEFAULT 0.0,
    times_seen           INTEGER NOT NULL DEFAULT 1,
    last_seen_timestamp  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_artist_aliases (
    alias_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    authority_artist_id  INTEGER NOT NULL REFERENCES authority_artists(authority_artist_id),
    alias_text           TEXT    NOT NULL,
    normalized_alias     TEXT    NOT NULL,
    source               TEXT    NOT NULL,
    confidence           REAL    NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS authority_artist_title_pairs (
    pair_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    authority_artist_id  INTEGER NOT NULL REFERENCES authority_artists(authority_artist_id),
    authority_title_id   INTEGER NOT NULL REFERENCES authority_titles(authority_title_id),
    pair_confidence      REAL    NOT NULL DEFAULT 0.0,
    times_seen           INTEGER NOT NULL DEFAULT 1,
    source               TEXT    NOT NULL,
    last_seen_timestamp  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_parse_history (
    parse_history_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id             INTEGER REFERENCES tracks(track_id),
    raw_file_name        TEXT,
    filename_artist_guess TEXT,
    filename_title_guess  TEXT,
    metadata_artist_guess TEXT,
    metadata_title_guess  TEXT,
    resolved_artist      TEXT,
    resolved_title       TEXT,
    source_used          TEXT,
    was_reversed         INTEGER DEFAULT 0,
    final_confidence     REAL,
    operator_verified    INTEGER DEFAULT 0,
    timestamp            TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_audit_log (
    authority_event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type           TEXT    NOT NULL,
    event_description    TEXT,
    timestamp            TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_artist_norm
    ON authority_artists(normalized_artist);
CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_title_norm
    ON authority_titles(normalized_title);
CREATE INDEX IF NOT EXISTS idx_auth_alias_norm
    ON authority_artist_aliases(normalized_alias);
CREATE UNIQUE INDEX IF NOT EXISTS idx_auth_pair_unique
    ON authority_artist_title_pairs(authority_artist_id, authority_title_id);
CREATE INDEX IF NOT EXISTS idx_auth_parse_track
    ON authority_parse_history(track_id);
"""


def part_a_schema(conn):
    log("═══ PART A: Schema Extension ═══")
    conn.executescript(AUTHORITY_SCHEMA)
    conn.commit()

    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'authority_%' ORDER BY name"
    ).fetchall()]
    log(f"Authority tables: {tables}")
    return tables


# ═══════════════════════════════════════════════════════════════════
# PART C — NORMALIZATION KEYS (defined before B so seeding can use them)
# ═══════════════════════════════════════════════════════════════════

# Preserve meaningful numeric artists
NUMERIC_ARTISTS = {
    "38 special", "50 cent", "2pac", "2 chainz", "21 savage",
    "3 doors down", "311", "360", "3oh3", "4 non blondes",
    "5 seconds of summer", "702", "98 degrees",
}

def normalize_artist(text):
    """
    Deterministic normalization key for artist matching.

    Rules:
    1. Unicode normalize (NFKD → strip accents → NFKC)
    2. Lowercase
    3. Strip leading/trailing whitespace
    4. Normalize apostrophes: ' ' ʼ → '
    5. Collapse multiple spaces to single
    6. Remove trailing punctuation noise: . , ; : !
    7. Remove known noise prefixes: "the " (kept for disambiguation)
    8. Preserve meaningful numeric artists
    9. Remove non-alphanumeric except spaces, hyphens, apostrophes
    """
    if not text:
        return ""
    t = text.strip()
    # Unicode normalize
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = unicodedata.normalize("NFKC", t)
    t = t.lower()
    # Normalize apostrophes
    t = t.replace("\u2019", "'").replace("\u2018", "'").replace("\u02bc", "'")
    # Collapse spaces
    t = re.sub(r"\s+", " ", t).strip()
    # Remove trailing punctuation
    t = re.sub(r"[.,;:!]+$", "", t).strip()
    # Remove non-essential characters (keep letters, digits, spaces, hyphens, apostrophes)
    t = re.sub(r"[^\w\s'\-]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_title(text):
    """
    Deterministic normalization key for title matching.

    Rules:
    1. Unicode normalize (same as artist)
    2. Lowercase
    3. Strip leading/trailing whitespace
    4. Normalize apostrophes
    5. Collapse multiple spaces
    6. Remove trailing junk: (official...), (lyric...), [explicit], etc.
    7. Remove trailing punctuation noise
    8. Preserve meaningful numbers and core words
    9. Remove non-alphanumeric except spaces, hyphens, apostrophes
    """
    if not text:
        return ""
    t = text.strip()
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = unicodedata.normalize("NFKC", t)
    t = t.lower()
    # Normalize apostrophes
    t = t.replace("\u2019", "'").replace("\u2018", "'").replace("\u02bc", "'")
    # Remove common trailing junk
    t = re.sub(r"\s*\(official[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(lyric[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(remaster[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\(live[^)]*\)", "", t, flags=re.I)
    t = re.sub(r"\s*\[explicit\]", "", t, flags=re.I)
    t = re.sub(r"\s*\bofficial\s+(music\s+)?video\b", "", t, flags=re.I)
    t = re.sub(r"\s*\bofficial\s+audio\b", "", t, flags=re.I)
    # Collapse spaces
    t = re.sub(r"\s+", " ", t).strip()
    # Remove trailing punctuation
    t = re.sub(r"[.,;:!]+$", "", t).strip()
    # Remove non-essential characters
    t = re.sub(r"[^\w\s'\-]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ═══════════════════════════════════════════════════════════════════
# PART B — TRUSTED SEED SOURCES
# ═══════════════════════════════════════════════════════════════════

def _get_or_create_artist(conn, canonical, source, confidence, now):
    """Get existing or insert new authority artist. Returns authority_artist_id."""
    norm = normalize_artist(canonical)
    if not norm:
        return None
    row = conn.execute(
        "SELECT authority_artist_id, times_seen FROM authority_artists WHERE normalized_artist = ?",
        (norm,)
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE authority_artists SET times_seen = times_seen + 1, "
            "last_seen_timestamp = ?, confidence = MAX(confidence, ?) WHERE authority_artist_id = ?",
            (now, confidence, row[0])
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO authority_artists "
            "(canonical_artist, normalized_artist, source, confidence, times_seen, last_seen_timestamp) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            (canonical.strip(), norm, source, confidence, now)
        )
        return cur.lastrowid


def _get_or_create_title(conn, canonical, source, confidence, now):
    """Get existing or insert new authority title. Returns authority_title_id."""
    norm = normalize_title(canonical)
    if not norm:
        return None
    row = conn.execute(
        "SELECT authority_title_id, times_seen FROM authority_titles WHERE normalized_title = ?",
        (norm,)
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE authority_titles SET times_seen = times_seen + 1, "
            "last_seen_timestamp = ?, confidence = MAX(confidence, ?) WHERE authority_title_id = ?",
            (now, confidence, row[0])
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO authority_titles "
            "(canonical_title, normalized_title, source, confidence, times_seen, last_seen_timestamp) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            (canonical.strip(), norm, source, confidence, now)
        )
        return cur.lastrowid


def _get_or_create_pair(conn, artist_id, title_id, source, confidence, now):
    """Get existing or insert new artist-title pair."""
    if not artist_id or not title_id:
        return None
    row = conn.execute(
        "SELECT pair_id FROM authority_artist_title_pairs "
        "WHERE authority_artist_id = ? AND authority_title_id = ?",
        (artist_id, title_id)
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE authority_artist_title_pairs SET times_seen = times_seen + 1, "
            "last_seen_timestamp = ?, pair_confidence = MAX(pair_confidence, ?) WHERE pair_id = ?",
            (now, confidence, row[0])
        )
        return row[0]
    else:
        cur = conn.execute(
            "INSERT INTO authority_artist_title_pairs "
            "(authority_artist_id, authority_title_id, pair_confidence, times_seen, source, last_seen_timestamp) "
            "VALUES (?, ?, ?, 1, ?, ?)",
            (artist_id, title_id, confidence, source, now)
        )
        return cur.lastrowid


def part_b_seed(conn):
    log("═══ PART B: Trusted Seed Sources ═══")
    now = datetime.now().isoformat()
    seed_results = []
    source_counts = Counter()

    # ─── SOURCE 1: Operator-verified seed CSV ─────────────────────
    if SEED_CSV.exists():
        log(f"Seeding from {SEED_CSV.name}")
        with open(SEED_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                artist = row.get("artist", "").strip()
                title = row.get("title", "").strip()
                conf = float(row.get("confidence", 1.0))
                src = "operator_verified"

                aid = _get_or_create_artist(conn, artist, src, conf, now)
                tid = _get_or_create_title(conn, title, src, conf, now)
                pid = _get_or_create_pair(conn, aid, tid, src, conf, now)

                seed_results.append({
                    "canonical_artist": artist,
                    "canonical_title": title,
                    "source": src,
                    "confidence": conf,
                    "seed_result": "seeded",
                })
                source_counts[src] += 1

        conn.execute(
            "INSERT INTO authority_audit_log (event_type, event_description, timestamp) "
            "VALUES (?, ?, ?)",
            ("SEED_CSV", f"Seeded {source_counts['operator_verified']} from {SEED_CSV.name}", now)
        )
        conn.commit()
        log(f"  operator_verified: {source_counts['operator_verified']}")
    else:
        log("  No authority_seed_v1.csv found — skipping")

    # ─── SOURCE 2: CLEAN high-confidence tracks ───────────────────
    log("Seeding from CLEAN high-confidence tracks")
    clean_rows = conn.execute(
        "SELECT hr.chosen_artist, hr.chosen_title, hr.final_confidence "
        "FROM track_status ts "
        "JOIN hybrid_resolution hr ON hr.track_id = ts.track_id "
        "JOIN metadata_tags mt ON mt.track_id = ts.track_id "
        "WHERE ts.status = 'CLEAN' "
        "AND hr.final_confidence >= 0.6 "
        "AND mt.metadata_junk_flag = 0 "
        "AND hr.requires_review = 0"
    ).fetchall()

    clean_count = 0
    for chosen_artist, chosen_title, conf in clean_rows:
        if not chosen_artist or not chosen_title:
            continue
        src = "clean_high_confidence"
        aid = _get_or_create_artist(conn, chosen_artist, src, conf, now)
        tid = _get_or_create_title(conn, chosen_title, src, conf, now)
        pid = _get_or_create_pair(conn, aid, tid, src, conf, now)
        clean_count += 1

        seed_results.append({
            "canonical_artist": chosen_artist,
            "canonical_title": chosen_title,
            "source": src,
            "confidence": round(conf, 3),
            "seed_result": "seeded",
        })

    source_counts["clean_high_confidence"] = clean_count
    conn.execute(
        "INSERT INTO authority_audit_log (event_type, event_description, timestamp) "
        "VALUES (?, ?, ?)",
        ("SEED_CLEAN", f"Seeded {clean_count} from CLEAN high-confidence tracks", now)
    )
    conn.commit()
    log(f"  clean_high_confidence: {clean_count}")

    # ─── SOURCE 3: READY_NORMALIZED high-confidence ───────────────
    log("Seeding from READY_NORMALIZED high-confidence tracks")
    rn_rows = conn.execute(
        "SELECT hr.chosen_artist, hr.chosen_title, hr.final_confidence "
        "FROM tracks t "
        "JOIN hybrid_resolution hr ON hr.track_id = t.track_id "
        "JOIN track_status ts ON ts.track_id = t.track_id "
        "JOIN metadata_tags mt ON mt.track_id = t.track_id "
        "WHERE t.folder = 'READY_NORMALIZED' "
        "AND hr.final_confidence >= 0.5 "
        "AND mt.metadata_junk_flag = 0 "
        "AND ts.status IN ('CLEAN', 'REVIEW')"
    ).fetchall()

    rn_count = 0
    for chosen_artist, chosen_title, conf in rn_rows:
        if not chosen_artist or not chosen_title:
            continue
        src = "ready_normalized"
        aid = _get_or_create_artist(conn, chosen_artist, src, min(conf * 0.9, 0.9), now)
        tid = _get_or_create_title(conn, chosen_title, src, min(conf * 0.9, 0.9), now)
        pid = _get_or_create_pair(conn, aid, tid, src, min(conf * 0.9, 0.9), now)
        rn_count += 1

        seed_results.append({
            "canonical_artist": chosen_artist,
            "canonical_title": chosen_title,
            "source": src,
            "confidence": round(min(conf * 0.9, 0.9), 3),
            "seed_result": "seeded",
        })

    source_counts["ready_normalized"] = rn_count
    conn.execute(
        "INSERT INTO authority_audit_log (event_type, event_description, timestamp) "
        "VALUES (?, ?, ?)",
        ("SEED_READY_NORM", f"Seeded {rn_count} from READY_NORMALIZED", now)
    )
    conn.commit()
    log(f"  ready_normalized: {rn_count}")

    # ─── SOURCE 4: Dashboard operator edits ───────────────────────
    log("Checking for dashboard operator edits")
    edit_rows = conn.execute(
        "SELECT al.track_id, al.event_type, al.event_description "
        "FROM audit_log al "
        "WHERE al.event_type IN ('ARTIST_EDIT', 'TITLE_EDIT', 'STATUS_CHANGE') "
        "AND al.track_id IS NOT NULL"
    ).fetchall()

    edit_track_ids = set()
    for track_id, event_type, desc in edit_rows:
        if event_type in ('ARTIST_EDIT', 'TITLE_EDIT'):
            edit_track_ids.add(track_id)

    edit_count = 0
    for tid in edit_track_ids:
        hr = conn.execute(
            "SELECT chosen_artist, chosen_title, final_confidence "
            "FROM hybrid_resolution WHERE track_id = ?", (tid,)
        ).fetchone()
        if hr and hr[0] and hr[1]:
            src = "operator_dashboard_edit"
            aid = _get_or_create_artist(conn, hr[0], src, 0.95, now)
            titid = _get_or_create_title(conn, hr[1], src, 0.95, now)
            pid = _get_or_create_pair(conn, aid, titid, src, 0.95, now)
            edit_count += 1

            seed_results.append({
                "canonical_artist": hr[0],
                "canonical_title": hr[1],
                "source": src,
                "confidence": 0.95,
                "seed_result": "seeded",
            })

    source_counts["operator_dashboard_edit"] = edit_count
    if edit_count > 0:
        conn.execute(
            "INSERT INTO authority_audit_log (event_type, event_description, timestamp) "
            "VALUES (?, ?, ?)",
            ("SEED_EDITS", f"Seeded {edit_count} from dashboard operator edits", now)
        )
    conn.commit()
    log(f"  operator_dashboard_edit: {edit_count}")

    log(f"Total seed results: {len(seed_results)} ({dict(source_counts)})")
    return seed_results, source_counts


# ═══════════════════════════════════════════════════════════════════
# PART D — INITIAL AUTHORITY SEED BUILD (parse history + CSV output)
# ═══════════════════════════════════════════════════════════════════

def part_d_parse_history(conn):
    log("═══ PART D: Parse History + Seed CSV ═══")
    now = datetime.now().isoformat()

    # Build parse history from all tracks
    rows = conn.execute(
        "SELECT t.track_id, t.file_name, "
        "fp.artist_guess, fp.title_guess, "
        "mt.artist_tag, mt.title_tag, "
        "hr.chosen_artist, hr.chosen_title, hr.source_used, "
        "hr.was_reversed, hr.final_confidence "
        "FROM tracks t "
        "JOIN filename_parse fp ON fp.track_id = t.track_id "
        "JOIN metadata_tags mt ON mt.track_id = t.track_id "
        "JOIN hybrid_resolution hr ON hr.track_id = t.track_id"
    ).fetchall()

    batch = []
    for (tid, fname, fn_a, fn_t, mt_a, mt_t,
         res_a, res_t, src, rev, conf) in rows:
        batch.append((
            tid, fname,
            fn_a or "", fn_t or "",
            mt_a or "", mt_t or "",
            res_a or "", res_t or "",
            src, int(rev), conf, 0, now
        ))

    conn.executemany(
        "INSERT INTO authority_parse_history "
        "(track_id, raw_file_name, filename_artist_guess, filename_title_guess, "
        "metadata_artist_guess, metadata_title_guess, resolved_artist, resolved_title, "
        "source_used, was_reversed, final_confidence, operator_verified, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch
    )
    conn.commit()
    log(f"Parse history: {len(batch)} rows")
    return len(batch)


def write_seed_csv(seed_results):
    path = DATA / "authority_seed_build_v1.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "canonical_artist", "canonical_title", "source", "confidence", "seed_result"
        ])
        w.writeheader()
        w.writerows(seed_results)
    log(f"Seed CSV: {path} ({len(seed_results)} rows)")


# ═══════════════════════════════════════════════════════════════════
# PART E — MATCH / LOOKUP HELPERS
# ═══════════════════════════════════════════════════════════════════

class AuthorityLookup:
    """
    Reusable lookup/scoring helpers for the Music Authority Layer.
    Designed for import by future ingest and dashboard phases.
    """

    def __init__(self, db_path=None):
        self.db_path = db_path or str(DB_PATH)
        self.conn: sqlite3.Connection

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    # ─── Exact match lookups ──────────────────────────────────────

    def lookup_artist(self, artist_text):
        """Look up exact normalized artist. Returns Row or None."""
        norm = normalize_artist(artist_text)
        if not norm:
            return None
        return self.conn.execute(
            "SELECT * FROM authority_artists WHERE normalized_artist = ?",
            (norm,)
        ).fetchone()

    def lookup_title(self, title_text):
        """Look up exact normalized title. Returns Row or None."""
        norm = normalize_title(title_text)
        if not norm:
            return None
        return self.conn.execute(
            "SELECT * FROM authority_titles WHERE normalized_title = ?",
            (norm,)
        ).fetchone()

    def lookup_pair(self, artist_text, title_text):
        """Look up artist-title pair. Returns Row or None."""
        artist = self.lookup_artist(artist_text)
        title = self.lookup_title(title_text)
        if not artist or not title:
            return None
        return self.conn.execute(
            "SELECT * FROM authority_artist_title_pairs "
            "WHERE authority_artist_id = ? AND authority_title_id = ?",
            (artist["authority_artist_id"], title["authority_title_id"])
        ).fetchone()

    # ─── Alias lookup ─────────────────────────────────────────────

    def lookup_artist_by_alias(self, alias_text):
        """Look up artist via alias. Returns authority_artists Row or None."""
        norm = normalize_artist(alias_text)
        if not norm:
            return None
        alias_row = self.conn.execute(
            "SELECT authority_artist_id FROM authority_artist_aliases WHERE normalized_alias = ?",
            (norm,)
        ).fetchone()
        if not alias_row:
            return None
        return self.conn.execute(
            "SELECT * FROM authority_artists WHERE authority_artist_id = ?",
            (alias_row["authority_artist_id"],)
        ).fetchone()

    # ─── Reversal detection ───────────────────────────────────────

    def detect_reversal(self, candidate_artist, candidate_title):
        """
        Check if swapping artist/title yields a known authority pair.
        Returns (is_reversed, pair_row, confidence).
        """
        # Normal order
        normal_pair = self.lookup_pair(candidate_artist, candidate_title)
        if normal_pair:
            return False, normal_pair, normal_pair["pair_confidence"]

        # Reversed order
        reversed_pair = self.lookup_pair(candidate_title, candidate_artist)
        if reversed_pair:
            return True, reversed_pair, reversed_pair["pair_confidence"]

        return False, None, 0.0

    # ─── Confidence scoring ───────────────────────────────────────

    def score_confidence(self, fn_artist, fn_title, meta_artist, meta_title):
        """
        Score confidence using all available authority data.
        Returns dict with component scores and final authority_boost.
        """
        scores = {
            "fn_artist_known": 0.0,
            "fn_title_known": 0.0,
            "meta_artist_known": 0.0,
            "meta_title_known": 0.0,
            "pair_known": 0.0,
            "reversal_detected": False,
            "authority_boost": 0.0,
        }

        # Check filename artist/title
        if fn_artist:
            a = self.lookup_artist(fn_artist)
            if a:
                scores["fn_artist_known"] = a["confidence"]
            else:
                a = self.lookup_artist_by_alias(fn_artist)
                if a:
                    scores["fn_artist_known"] = a["confidence"] * 0.8

        if fn_title:
            t = self.lookup_title(fn_title)
            if t:
                scores["fn_title_known"] = t["confidence"]

        # Check metadata artist/title
        if meta_artist:
            a = self.lookup_artist(meta_artist)
            if a:
                scores["meta_artist_known"] = a["confidence"]

        if meta_title:
            t = self.lookup_title(meta_title)
            if t:
                scores["meta_title_known"] = t["confidence"]

        # Check pairs (try filename pair then metadata pair)
        if fn_artist and fn_title:
            p = self.lookup_pair(fn_artist, fn_title)
            if p:
                scores["pair_known"] = p["pair_confidence"]

        if not scores["pair_known"] and meta_artist and meta_title:
            p = self.lookup_pair(meta_artist, meta_title)
            if p:
                scores["pair_known"] = p["pair_confidence"]

        # Check reversal
        if fn_artist and fn_title:
            is_rev, _, _ = self.detect_reversal(fn_artist, fn_title)
            scores["reversal_detected"] = is_rev

        # Compute boost
        component_sum = (
            scores["fn_artist_known"] * 0.2 +
            scores["fn_title_known"] * 0.2 +
            scores["meta_artist_known"] * 0.15 +
            scores["meta_title_known"] * 0.15 +
            scores["pair_known"] * 0.3
        )
        scores["authority_boost"] = round(min(1.0, component_sum), 3)

        return scores


# ═══════════════════════════════════════════════════════════════════
# PART F — AUTHORITY COVERAGE REPORT
# ═══════════════════════════════════════════════════════════════════

def part_f_coverage(conn):
    log("═══ PART F: Authority Coverage Report ═══")

    n_artists = conn.execute("SELECT COUNT(*) FROM authority_artists").fetchone()[0]
    n_titles = conn.execute("SELECT COUNT(*) FROM authority_titles").fetchone()[0]
    n_pairs = conn.execute("SELECT COUNT(*) FROM authority_artist_title_pairs").fetchone()[0]
    n_aliases = conn.execute("SELECT COUNT(*) FROM authority_artist_aliases").fetchone()[0]
    n_parse = conn.execute("SELECT COUNT(*) FROM authority_parse_history").fetchone()[0]

    # Count tracks matched by authority
    total_tracks = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

    matched = conn.execute(
        "SELECT COUNT(DISTINCT t.track_id) FROM tracks t "
        "JOIN hybrid_resolution hr ON hr.track_id = t.track_id "
        "JOIN authority_artists aa ON aa.normalized_artist = ? "
        "WHERE 1=0",  # placeholder — we do manual matching below
        ("",)
    ).fetchone()[0]

    # Manual matching: check each track's resolved artist against authority
    tracks = conn.execute(
        "SELECT hr.track_id, hr.chosen_artist, hr.chosen_title "
        "FROM hybrid_resolution hr"
    ).fetchall()

    # Build lookup sets
    auth_artists = set(
        r[0] for r in conn.execute("SELECT normalized_artist FROM authority_artists")
    )
    auth_titles = set(
        r[0] for r in conn.execute("SELECT normalized_title FROM authority_titles")
    )

    matched_count = 0
    unmatched_count = 0
    for tid, ca, ct in tracks:
        na = normalize_artist(ca) if ca else ""
        nt = normalize_title(ct) if ct else ""
        if na in auth_artists or nt in auth_titles:
            matched_count += 1
        else:
            unmatched_count += 1

    coverage = {
        "authority_artists": n_artists,
        "authority_titles": n_titles,
        "artist_title_pairs": n_pairs,
        "aliases": n_aliases,
        "parse_history_rows": n_parse,
        "tracks_matched_by_authority": matched_count,
        "tracks_unmatched": unmatched_count,
        "total_library_tracks": total_tracks,
        "coverage_pct": round(matched_count / total_tracks * 100, 1) if total_tracks else 0,
    }

    path = DATA / "authority_coverage_v1.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(coverage.keys()))
        w.writeheader()
        w.writerow(coverage)

    log(f"Coverage: {coverage}")
    log(f"Written: {path}")
    return coverage


# ═══════════════════════════════════════════════════════════════════
# PART G — REVERSAL / PAIRING INSIGHT REPORT
# ═══════════════════════════════════════════════════════════════════

def part_g_reversal_insights(conn):
    log("═══ PART G: Reversal / Pairing Insights ═══")

    # Get all reversed tracks
    reversed_rows = conn.execute(
        "SELECT t.track_id, t.file_name, "
        "fp.artist_guess, fp.title_guess, "
        "hr.chosen_artist, hr.chosen_title, hr.final_confidence "
        "FROM tracks t "
        "JOIN filename_parse fp ON fp.track_id = t.track_id "
        "JOIN hybrid_resolution hr ON hr.track_id = t.track_id "
        "WHERE hr.was_reversed = 1"
    ).fetchall()

    # Build pair lookup
    pairs = conn.execute(
        "SELECT aa.normalized_artist, at.normalized_title "
        "FROM authority_artist_title_pairs atp "
        "JOIN authority_artists aa ON aa.authority_artist_id = atp.authority_artist_id "
        "JOIN authority_titles at ON at.authority_title_id = atp.authority_title_id"
    ).fetchall()
    pair_set = set((r[0], r[1]) for r in pairs)

    insights = []
    for tid, fname, fn_a, fn_t, res_a, res_t, conf in reversed_rows:
        fn_a = fn_a or ""
        fn_t = fn_t or ""

        # Check if the correct (resolved) artist-title is a known pair
        norm_a = normalize_artist(res_a) if res_a else ""
        norm_t = normalize_title(res_t) if res_t else ""
        pair_match = (norm_a, norm_t) in pair_set

        # Check reversed order too
        rev_pair_match = (normalize_artist(fn_t), normalize_title(fn_a)) in pair_set

        notes_parts = []
        if pair_match:
            notes_parts.append("resolved pair is authority-known")
        if rev_pair_match:
            notes_parts.append("reversed order matches authority pair")
        if not pair_match and not rev_pair_match:
            notes_parts.append("no authority pair match yet")

        insights.append({
            "track_id": tid,
            "raw_file_name": fname,
            "detected_artist_candidate": fn_a,
            "detected_title_candidate": fn_t,
            "authority_pair_match": "yes" if (pair_match or rev_pair_match) else "no",
            "reversal_confidence": round(conf, 3),
            "notes": "; ".join(notes_parts),
        })

    path = DATA / "authority_reversal_insights_v1.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "track_id", "raw_file_name", "detected_artist_candidate",
            "detected_title_candidate", "authority_pair_match",
            "reversal_confidence", "notes",
        ])
        w.writeheader()
        w.writerows(insights)

    matched = sum(1 for i in insights if i["authority_pair_match"] == "yes")
    log(f"Reversal insights: {len(insights)} reversed tracks, {matched} with authority match")
    log(f"Written: {path}")
    return insights


# ═══════════════════════════════════════════════════════════════════
# PART H + I — PROOF ARTIFACTS + VALIDATION
# ═══════════════════════════════════════════════════════════════════

def part_h_proof(conn, seed_results, source_counts, coverage, insights, parse_count):
    log("═══ PART H: Proof Artifacts ═══")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now().isoformat()

    # 00 — Schema extension summary
    with open(PROOF_DIR / "00_schema_extension_summary.txt", "w", encoding="utf-8") as f:
        f.write("Schema Extension Summary\n" + "=" * 40 + "\n\n")
        f.write("NEW TABLES:\n")
        auth_tables = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name LIKE 'authority_%' ORDER BY name"
        ).fetchall()
        for name, sql in auth_tables:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            f.write(f"\n--- {name} ({cnt} rows) ---\n{sql}\n")
        f.write("\nINDICES:\n")
        indices = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND name LIKE 'idx_auth%' ORDER BY name"
        ).fetchall()
        for name, sql in indices:
            f.write(f"  {sql}\n")

    # 01 — Seed source summary
    with open(PROOF_DIR / "01_seed_source_summary.txt", "w", encoding="utf-8") as f:
        f.write("Seed Source Summary\n" + "=" * 40 + "\n\n")
        f.write("TRUSTED SOURCES USED (in priority order):\n")
        sources = [
            ("operator_verified", "authority_seed_v1.csv — manually verified"),
            ("clean_high_confidence", "CLEAN tracks, conf>=0.6, no junk, no review flag"),
            ("ready_normalized", "READY_NORMALIZED folder, conf>=0.5, no junk"),
            ("operator_dashboard_edit", "Dashboard edits (ARTIST_EDIT/TITLE_EDIT in audit_log)"),
        ]
        for src, desc in sources:
            cnt = source_counts.get(src, 0)
            f.write(f"\n  {src}: {cnt}\n    {desc}\n")
        f.write(f"\nTotal seed entries: {len(seed_results)}\n")
        f.write("\nEXCLUDED SOURCES:\n")
        f.write("  - JUNK tracks\n  - LONGFORM tracks\n")
        f.write("  - Low-confidence REVIEW rows\n  - Unresolved duplicates\n")
        f.write("  - Ambiguous metadata-only junk\n")

    # 02 — Normalization key rules
    with open(PROOF_DIR / "02_normalization_key_rules.txt", "w", encoding="utf-8") as f:
        f.write("Normalization Key Rules\n" + "=" * 40 + "\n\n")
        f.write("ARTIST NORMALIZATION (normalize_artist):\n")
        f.write("  1. Unicode normalize (NFKD → strip accents → NFKC)\n")
        f.write("  2. Lowercase\n")
        f.write("  3. Strip leading/trailing whitespace\n")
        f.write("  4. Normalize apostrophes: ' ' ʼ → '\n")
        f.write("  5. Collapse multiple spaces to single\n")
        f.write("  6. Remove trailing punctuation: . , ; : !\n")
        f.write("  7. Remove non-alphanumeric (keep spaces, hyphens, apostrophes)\n")
        f.write("  8. Preserve meaningful numeric: 50 Cent, 2Pac, 38 Special, etc.\n")
        f.write("\nTITLE NORMALIZATION (normalize_title):\n")
        f.write("  1. Unicode normalize (same as artist)\n")
        f.write("  2. Lowercase\n")
        f.write("  3. Strip leading/trailing whitespace\n")
        f.write("  4. Normalize apostrophes\n")
        f.write("  5. Remove trailing junk: (Official...), (Lyric...), [Explicit], etc.\n")
        f.write("  6. Remove 'Official Music Video', 'Official Audio'\n")
        f.write("  7. Collapse multiple spaces\n")
        f.write("  8. Remove trailing punctuation\n")
        f.write("  9. Remove non-alphanumeric (keep spaces, hyphens, apostrophes)\n")
        f.write("\nDETERMINISM GUARANTEE:\n")
        f.write("  Both functions are pure: same input always produces same output.\n")
        f.write("  No randomness, no external state, no LLM calls.\n")
        f.write("\nEXAMPLES:\n")
        examples = [
            ("AC/DC", normalize_artist("AC/DC")),
            ("Guns N' Roses", normalize_artist("Guns N' Roses")),
            ("50 Cent", normalize_artist("50 Cent")),
            ("The Beatles", normalize_artist("The Beatles")),
            ("Beyoncé", normalize_artist("Beyoncé")),
        ]
        for orig, norm in examples:
            f.write(f"  Artist: '{orig}' → '{norm}'\n")
        t_examples = [
            ("Bohemian Rhapsody (Official Video)", normalize_title("Bohemian Rhapsody (Official Video)")),
            ("Enter Sandman (Remastered)", normalize_title("Enter Sandman (Remastered)")),
            ("Don't Stop Believin'", normalize_title("Don't Stop Believin'")),
        ]
        for orig, norm in t_examples:
            f.write(f"  Title:  '{orig}' → '{norm}'\n")

    # 03 — Authority seed results
    with open(PROOF_DIR / "03_authority_seed_results.txt", "w", encoding="utf-8") as f:
        f.write("Authority Seed Results\n" + "=" * 40 + "\n\n")
        n_artists = conn.execute("SELECT COUNT(*) FROM authority_artists").fetchone()[0]
        n_titles = conn.execute("SELECT COUNT(*) FROM authority_titles").fetchone()[0]
        n_pairs = conn.execute("SELECT COUNT(*) FROM authority_artist_title_pairs").fetchone()[0]
        f.write(f"Unique authority artists: {n_artists}\n")
        f.write(f"Unique authority titles:  {n_titles}\n")
        f.write(f"Unique artist-title pairs: {n_pairs}\n")
        f.write(f"Parse history rows: {parse_count}\n")
        f.write(f"\nSeed entries processed: {len(seed_results)}\n")
        f.write(f"By source:\n")
        for src, cnt in source_counts.most_common():
            f.write(f"  {src}: {cnt}\n")
        f.write(f"\nTop artists by times_seen:\n")
        top_artists = conn.execute(
            "SELECT canonical_artist, times_seen, confidence FROM authority_artists "
            "ORDER BY times_seen DESC LIMIT 20"
        ).fetchall()
        for ca, ts, conf in top_artists:
            f.write(f"  {ca}: seen={ts}, conf={conf:.2f}\n")

    # 04 — Lookup helper summary
    with open(PROOF_DIR / "04_lookup_helper_summary.txt", "w", encoding="utf-8") as f:
        f.write("Lookup Helper Summary\n" + "=" * 40 + "\n\n")
        f.write("CLASS: AuthorityLookup\n")
        f.write("LOCATION: db/dj_authority_phase4.py\n\n")
        f.write("METHODS:\n")
        methods = [
            ("lookup_artist(text)", "Exact normalized artist match → Row or None"),
            ("lookup_title(text)", "Exact normalized title match → Row or None"),
            ("lookup_pair(artist, title)", "Artist-title pair match → Row or None"),
            ("lookup_artist_by_alias(text)", "Alias → canonical artist → Row or None"),
            ("detect_reversal(artist, title)", "Check if swapping yields known pair → (bool, Row, conf)"),
            ("score_confidence(fn_a, fn_t, meta_a, meta_t)",
             "Full scoring with authority data → dict with component scores + authority_boost"),
        ]
        for name, desc in methods:
            f.write(f"  {name}\n    {desc}\n\n")
        f.write("USAGE:\n")
        f.write("  with AuthorityLookup() as auth:\n")
        f.write("      artist = auth.lookup_artist('Metallica')\n")
        f.write("      pair = auth.lookup_pair('Metallica', 'Enter Sandman')\n")
        f.write("      scores = auth.score_confidence(fn_a, fn_t, meta_a, meta_t)\n")
        f.write("\nDETERMINISM: All lookups are deterministic. Same inputs → same results.\n")

    # 05 — Authority coverage
    with open(PROOF_DIR / "05_authority_coverage.txt", "w", encoding="utf-8") as f:
        f.write("Authority Coverage\n" + "=" * 40 + "\n\n")
        for k, v in coverage.items():
            f.write(f"  {k}: {v}\n")

    # 06 — Reversal insights
    matched_ins = sum(1 for i in insights if i["authority_pair_match"] == "yes")
    with open(PROOF_DIR / "06_reversal_insights.txt", "w", encoding="utf-8") as f:
        f.write("Reversal / Pairing Insights\n" + "=" * 40 + "\n\n")
        f.write(f"Total reversed tracks: {len(insights)}\n")
        f.write(f"Authority pair match found: {matched_ins}\n")
        f.write(f"No authority match yet: {len(insights) - matched_ins}\n\n")
        f.write("INSIGHT: Tracks with authority pair match could have their\n")
        f.write("reversal resolved automatically in future intake runs.\n\n")
        if matched_ins > 0:
            f.write("SAMPLE MATCHES:\n")
            for i in insights[:10]:
                if i["authority_pair_match"] == "yes":
                    f.write(f"  track_id={i['track_id']}: {i['detected_artist_candidate']} - "
                            f"{i['detected_title_candidate']} | {i['notes']}\n")

    # 07 — Validation checks
    log("═══ PART I: Validation ═══")
    checks = []

    # Schema checks
    auth_tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'authority_%' ORDER BY name"
    ).fetchall()]
    required_tables = {"authority_artists", "authority_titles", "authority_artist_aliases",
                       "authority_artist_title_pairs", "authority_parse_history", "authority_audit_log"}
    checks.append(("all_authority_tables_created",
                    required_tables.issubset(set(auth_tables)),
                    f"found={auth_tables}"))

    # No duplicate normalized artists
    dup_artists = conn.execute(
        "SELECT normalized_artist, COUNT(*) c FROM authority_artists GROUP BY normalized_artist HAVING c > 1"
    ).fetchall()
    checks.append(("no_duplicate_artists",
                    len(dup_artists) == 0,
                    f"{len(dup_artists)} duplicates"))

    # No duplicate normalized titles
    dup_titles = conn.execute(
        "SELECT normalized_title, COUNT(*) c FROM authority_titles GROUP BY normalized_title HAVING c > 1"
    ).fetchall()
    checks.append(("no_duplicate_titles",
                    len(dup_titles) == 0,
                    f"{len(dup_titles)} duplicates"))

    # No duplicate pairs
    dup_pairs = conn.execute(
        "SELECT authority_artist_id, authority_title_id, COUNT(*) c "
        "FROM authority_artist_title_pairs GROUP BY authority_artist_id, authority_title_id HAVING c > 1"
    ).fetchall()
    checks.append(("no_duplicate_pairs",
                    len(dup_pairs) == 0,
                    f"{len(dup_pairs)} duplicates"))

    # Seed from trusted sources only
    valid_sources = {"operator_verified", "clean_high_confidence",
                     "ready_normalized", "operator_dashboard_edit"}
    bad_src = conn.execute(
        "SELECT DISTINCT source FROM authority_artists WHERE source NOT IN "
        "('operator_verified','clean_high_confidence','ready_normalized','operator_dashboard_edit')"
    ).fetchall()
    checks.append(("trusted_sources_only",
                    len(bad_src) == 0,
                    f"invalid sources={[r[0] for r in bad_src]}" if bad_src else "all trusted"))

    # Coverage CSV exists
    checks.append(("coverage_csv_exists",
                    (DATA / "authority_coverage_v1.csv").exists(),
                    str(DATA / "authority_coverage_v1.csv")))

    # Reversal insights CSV exists
    checks.append(("reversal_csv_exists",
                    (DATA / "authority_reversal_insights_v1.csv").exists(),
                    str(DATA / "authority_reversal_insights_v1.csv")))

    # Seed build CSV exists
    checks.append(("seed_build_csv_exists",
                    (DATA / "authority_seed_build_v1.csv").exists(),
                    str(DATA / "authority_seed_build_v1.csv")))

    # Parse history populated
    ph_count = conn.execute("SELECT COUNT(*) FROM authority_parse_history").fetchone()[0]
    track_count = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    checks.append(("parse_history_populated",
                    ph_count == track_count,
                    f"{ph_count}/{track_count}"))

    # Library DB intact
    checks.append(("library_db_intact",
                    track_count == 3048,
                    f"tracks={track_count}"))

    # Authority tables populated
    for tbl in ["authority_artists", "authority_titles", "authority_artist_title_pairs"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        checks.append((f"{tbl}_populated",
                        cnt > 0,
                        f"{cnt} rows"))

    # Lookup helpers are deterministic (quick test)
    t1 = normalize_artist("Metallica")
    t2 = normalize_artist("Metallica")
    checks.append(("normalization_deterministic",
                    t1 == t2 == "metallica",
                    f"'{t1}' == '{t2}'"))

    # No filesystem mutations in this script
    # Build keywords dynamically to avoid self-referential false positives
    _danger_prefixes = ["os.ren", "os.rem", "shutil.mo", "shutil.rmt", "os.unl", "Path.ren"]
    _danger_suffixes = ["ame(", "ove(", "ve(", "ree(", "ink(", "ame("]
    danger_kws = [p + s for p, s in zip(_danger_prefixes, _danger_suffixes)]
    src = Path(__file__).read_text(encoding="utf-8")
    danger = any(kw in src for kw in danger_kws)
    checks.append(("no_filesystem_mutations",
                    not danger,
                    "no rename/delete/move calls"))

    import py_compile
    try:
        py_compile.compile(str(Path(__file__)), doraise=True)
        checks.append(("syntax_valid", True, "compiles OK"))
    except py_compile.PyCompileError as e:
        checks.append(("syntax_valid", False, str(e)))

    all_pass = all(ok for _, ok, _ in checks)

    with open(PROOF_DIR / "07_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write("Validation Checks\n" + "=" * 40 + "\n\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'} "
                f"({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})\n")

    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")
    log(f"Validation: {'ALL PASS' if all_pass else 'FAIL'} "
        f"({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")

    # 08 — Final report
    with open(PROOF_DIR / "08_final_report.txt", "w", encoding="utf-8") as f:
        f.write("DJ Library Core — Phase 4 Final Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {now}\n")
        f.write(f"GATE={'PASS' if all_pass else 'FAIL'}\n\n")
        f.write(f"Database: {DB_PATH} ({DB_PATH.stat().st_size:,} bytes)\n\n")
        f.write("AUTHORITY LAYER:\n")
        f.write(f"  Artists:          {coverage['authority_artists']}\n")
        f.write(f"  Titles:           {coverage['authority_titles']}\n")
        f.write(f"  Pairs:            {coverage['artist_title_pairs']}\n")
        f.write(f"  Aliases:          {coverage['aliases']}\n")
        f.write(f"  Parse history:    {coverage['parse_history_rows']}\n")
        f.write(f"\nCOVERAGE:\n")
        f.write(f"  Matched:   {coverage['tracks_matched_by_authority']} ({coverage['coverage_pct']}%)\n")
        f.write(f"  Unmatched: {coverage['tracks_unmatched']}\n")
        f.write(f"\nREVERSAL INSIGHTS:\n")
        f.write(f"  Reversed tracks:     {len(insights)}\n")
        f.write(f"  Authority matchable: {matched_ins}\n")
        f.write(f"\nSEED SOURCES: {dict(source_counts)}\n")
        f.write(f"VALIDATION: {sum(1 for _,ok,_ in checks if ok)}/{len(checks)} passed\n")

    # Execution log
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")

    # ZIP
    zip_path = BASE / "_proof" / "dj_library_core_phase4.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pf in sorted(PROOF_DIR.iterdir()):
            if pf.is_file():
                zf.write(pf, f"dj_library_core_phase4/{pf.name}")
        for csv_name in ["authority_seed_build_v1.csv", "authority_coverage_v1.csv",
                         "authority_reversal_insights_v1.csv"]:
            csv_path = DATA / csv_name
            if csv_path.exists():
                zf.write(csv_path, f"dj_library_core_phase4/{csv_name}")

    log(f"ZIP={zip_path} ({zip_path.stat().st_size:,} bytes)")

    # Rewrite log + rezip
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pf in sorted(PROOF_DIR.iterdir()):
            if pf.is_file():
                zf.write(pf, f"dj_library_core_phase4/{pf.name}")
        for csv_name in ["authority_seed_build_v1.csv", "authority_coverage_v1.csv",
                         "authority_reversal_insights_v1.csv"]:
            csv_path = DATA / csv_name
            if csv_path.exists():
                zf.write(csv_path, f"dj_library_core_phase4/{csv_name}")

    return checks, all_pass, zip_path


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log("DJ Library Core — Phase 4: Music Authority Layer — BEGIN")
    log(f"Database: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    part_a_schema(conn)
    seed_results, source_counts = part_b_seed(conn)
    parse_count = part_d_parse_history(conn)
    write_seed_csv(seed_results)
    coverage = part_f_coverage(conn)
    insights = part_g_reversal_insights(conn)
    checks, all_pass, zip_path = part_h_proof(
        conn, seed_results, source_counts, coverage, insights, parse_count
    )

    conn.close()

    log("")
    log("=" * 60)
    log("DJ LIBRARY CORE — PHASE 4 COMPLETE")
    log(f"  Authority artists:  {coverage['authority_artists']}")
    log(f"  Authority titles:   {coverage['authority_titles']}")
    log(f"  Artist-title pairs: {coverage['artist_title_pairs']}")
    log(f"  Coverage:           {coverage['coverage_pct']}%")
    log(f"  Reversed matchable: {sum(1 for i in insights if i['authority_pair_match'] == 'yes')}")
    log(f"  PF={PROOF_DIR}")
    log(f"  ZIP={zip_path}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
