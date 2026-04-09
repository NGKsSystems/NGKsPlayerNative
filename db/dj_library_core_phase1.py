#!/usr/bin/env python3
"""
DJ Library Core — Hybrid Ingest Database (Phase 1)
====================================================
Builds a persistent SQLite database as the single source of truth
for the music library, ingesting filename parses + metadata tags
separately, then computing hybrid resolution for canonical artist/title.

READ-ONLY on the filesystem. Only creates/writes:
  - data/dj_library_core.db  (SQLite)
  - data/*.csv                (reports)
  - _proof/*                  (artifacts)

Parts A–I as specified.
"""

import csv
import hashlib
import os
import re
import sqlite3
import sys
import zipfile
from collections import Counter
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import mutagen
from mutagen.id3 import ID3
from mutagen.id3._util import ID3NoHeaderError
from mutagen.mp3 import MP3

# ─── Configuration ─────────────────────────────────────────────────
BASE       = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
MUSIC_ROOT = Path(r"C:\Users\suppo\Downloads\New Music")
DATA       = BASE / "data"
DB_PATH    = DATA / "dj_library_core.db"
PROOF_DIR  = BASE / "_proof" / "dj_library_core_phase1"

AUDIO_EXTS = {".mp3"}

# ─── Junk detection patterns (reused from metadata audit) ──────────
JUNK_PATTERNS = [
    (re.compile(r"napalm\s*records", re.I),        "label_noise"),
    (re.compile(r"official", re.I),                 "official_noise"),
    (re.compile(r"youtube", re.I),                  "youtube_noise"),
    (re.compile(r"\bHD\b"),                         "quality_noise"),
    (re.compile(r"visuali[sz]er", re.I),            "visualizer_noise"),
    (re.compile(r"https?://", re.I),                "url_in_metadata"),
    (re.compile(r"www\.", re.I),                    "url_in_metadata"),
    (re.compile(r"interscope", re.I),               "label_noise"),
    (re.compile(r"warner\s*records", re.I),          "label_noise"),
    (re.compile(r"\bNCS\b"),                        "platform_noise"),
    (re.compile(r"copyright\s*free", re.I),         "platform_noise"),
    (re.compile(r"\blyric\s*video\b", re.I),        "video_noise"),
    (re.compile(r"\bmusic\s*video\b", re.I),        "video_noise"),
    (re.compile(r"\bfull\s*album\b", re.I),         "album_noise"),
    (re.compile(r"\bremaster(?:ed)?\b", re.I),      "remaster_noise"),
]

# ─── Longform detection ────────────────────────────────────────────
LONGFORM_PATTERNS = [
    re.compile(r"full\s+album", re.I),
    re.compile(r"playlist", re.I),
    re.compile(r"compilation", re.I),
    re.compile(r"mix\b", re.I),
    re.compile(r"\bhour", re.I),
    re.compile(r"best\s+(of|songs|hits)", re.I),
    re.compile(r"greatest\s+hits", re.I),
    re.compile(r"collection", re.I),
    re.compile(r"golden\s+(age|classics)", re.I),
    re.compile(r"timeless", re.I),
    re.compile(r"legends?\s+of", re.I),
    re.compile(r"ultimate.*playlist", re.I),
]
LONGFORM_SIZE_THRESHOLD = 30 * 1024 * 1024   # 30 MB
LONGFORM_DURATION_THRESHOLD = 600            # 10 minutes

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ═══════════════════════════════════════════════════════════════════
# PART A — DATABASE CREATION
# ═══════════════════════════════════════════════════════════════════

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tracks (
    track_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path       TEXT    UNIQUE NOT NULL,
    file_name       TEXT    NOT NULL,
    folder          TEXT    NOT NULL,
    file_size       INTEGER NOT NULL,
    duration        REAL,
    file_hash       TEXT,
    ingest_timestamp TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS filename_parse (
    track_id         INTEGER PRIMARY KEY REFERENCES tracks(track_id),
    artist_guess     TEXT,
    title_guess      TEXT,
    parse_confidence REAL,
    parse_method     TEXT
);

CREATE TABLE IF NOT EXISTS metadata_tags (
    track_id            INTEGER PRIMARY KEY REFERENCES tracks(track_id),
    artist_tag          TEXT,
    title_tag           TEXT,
    album               TEXT,
    genre               TEXT,
    track_number        TEXT,
    tag_version         TEXT,
    metadata_confidence REAL,
    metadata_junk_flag  INTEGER DEFAULT 0,
    metadata_junk_reason TEXT
);

CREATE TABLE IF NOT EXISTS hybrid_resolution (
    track_id         INTEGER PRIMARY KEY REFERENCES tracks(track_id),
    chosen_artist    TEXT,
    chosen_title     TEXT,
    source_used      TEXT CHECK(source_used IN ('filename','metadata','hybrid')),
    final_confidence REAL,
    was_reversed     INTEGER DEFAULT 0,
    requires_review  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS track_status (
    track_id          INTEGER PRIMARY KEY REFERENCES tracks(track_id),
    status            TEXT CHECK(status IN ('RAW','CLEAN','REVIEW','DUPLICATE','JUNK','LONGFORM')),
    duplicate_group_id INTEGER,
    is_primary        INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS audit_log (
    log_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id         INTEGER REFERENCES tracks(track_id),
    event_type       TEXT NOT NULL,
    event_description TEXT,
    timestamp        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tracks_folder ON tracks(folder);
CREATE INDEX IF NOT EXISTS idx_track_status_status ON track_status(status);
CREATE INDEX IF NOT EXISTS idx_hybrid_source ON hybrid_resolution(source_used);
CREATE INDEX IF NOT EXISTS idx_audit_track ON audit_log(track_id);
"""


def part_a_create_db():
    log("═══ PART A: Database Creation ═══")
    # Remove old DB if exists (fresh build)
    if DB_PATH.exists():
        DB_PATH.unlink()
        log(f"Removed existing DB: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.commit()

    # Verify tables
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    log(f"Created DB with tables: {tables}")
    return conn


# ═══════════════════════════════════════════════════════════════════
# PART B — INGEST FILES
# ═══════════════════════════════════════════════════════════════════

def part_b_ingest(conn):
    log("═══ PART B: Ingest Files ═══")
    now = datetime.now().isoformat()
    count = 0
    errors = 0

    for folder in sorted(MUSIC_ROOT.iterdir()):
        if not folder.is_dir():
            continue
        for f in sorted(folder.iterdir()):
            if not f.is_file() or f.suffix.lower() not in AUDIO_EXTS:
                continue
            try:
                st = f.stat()
                duration = None
                try:
                    audio = MP3(str(f))
                    if audio.info:
                        duration = round(audio.info.length, 1)
                except Exception:
                    pass

                conn.execute(
                    """INSERT INTO tracks
                       (file_path, file_name, folder, file_size, duration, ingest_timestamp)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (str(f), f.name, folder.name, st.st_size, duration, now)
                )
                count += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    log(f"  INGEST ERROR: {f.name}: {e}")

        if count % 500 == 0 and count > 0:
            conn.commit()

    conn.commit()
    log(f"Ingested {count} tracks ({errors} errors)")

    # Log per-folder counts
    rows = conn.execute(
        "SELECT folder, COUNT(*) FROM tracks GROUP BY folder ORDER BY folder"
    ).fetchall()
    for folder, cnt in rows:
        log(f"  {folder}: {cnt}")

    return count


# ═══════════════════════════════════════════════════════════════════
# PART C — FILENAME PARSE INGEST
# ═══════════════════════════════════════════════════════════════════

def parse_filename(filename):
    """Parse artist and title from filename. Returns (artist, title, confidence, method)."""
    base = filename
    for ext in (".mp3", ".flac", ".wav", ".m4a"):
        if base.lower().endswith(ext):
            base = base[:-len(ext)]
            break

    # Strip common junk
    base = re.sub(r"\s*\(Official[^)]*\)", "", base, flags=re.I)
    base = re.sub(r"\s*\(Lyric[^)]*\)", "", base, flags=re.I)
    base = re.sub(r"\s*\(Remaster[^)]*\)", "", base, flags=re.I)
    base = re.sub(r"\s*\(Live[^)]*\)", "", base, flags=re.I)
    base = re.sub(r"\s*\[Explicit\]", "", base, flags=re.I)
    base = re.sub(r"\s*\bOfficial\s+(Music\s+)?Video\b", "", base, flags=re.I)
    base = re.sub(r"\s*\bOfficial\s+Audio\b", "", base, flags=re.I)
    base = re.sub(r"\s*\b(4K|HD|HQ|1080p)\b", "", base, flags=re.I)

    # Normalize unicode separators
    base = base.replace("\u29f8", "/")
    base = base.replace("\uff5c", "|")
    base = base.replace("\uff1a", ":")
    base = base.replace("\uff02", '"')

    # Pattern 1: "Artist - Title"
    m = re.match(r"^(.+?)\s*[-–—]\s+(.+)$", base)
    if m:
        return m.group(1).strip(), m.group(2).strip(), 0.8, "dash_split"

    # Pattern 2: "Title by Artist"
    m = re.match(r"^(.+?)\s+by\s+(.+)$", base, re.I)
    if m:
        return m.group(2).strip(), m.group(1).strip(), 0.7, "by_pattern"

    # Pattern 3: "Artist-Title" (no space)
    m = re.match(r"^(.+?)[-–—](.+)$", base)
    if m:
        return m.group(1).strip(), m.group(2).strip(), 0.6, "tight_dash"

    # No separator
    return "", base.strip(), 0.3, "title_only"


def part_c_filename_parse(conn):
    log("═══ PART C: Filename Parse Ingest ═══")
    tracks = conn.execute("SELECT track_id, file_name FROM tracks").fetchall()

    batch = []
    method_counts = Counter()
    for track_id, fname in tracks:
        artist, title, conf, method = parse_filename(fname)
        batch.append((track_id, artist, title, conf, method))
        method_counts[method] += 1

    conn.executemany(
        """INSERT INTO filename_parse
           (track_id, artist_guess, title_guess, parse_confidence, parse_method)
           VALUES (?, ?, ?, ?, ?)""",
        batch
    )
    conn.commit()
    log(f"Parsed {len(batch)} filenames")
    for method, cnt in method_counts.most_common():
        log(f"  {method}: {cnt}")
    return len(batch)


# ═══════════════════════════════════════════════════════════════════
# PART D — METADATA INGEST
# ═══════════════════════════════════════════════════════════════════

def detect_junk(artist, title, album):
    """Check for junk patterns in metadata. Returns (flag, reasons)."""
    reasons = []
    for field in [artist, title, album]:
        if not field:
            continue
        for pattern, reason in JUNK_PATTERNS:
            if pattern.search(field):
                if reason not in reasons:
                    reasons.append(reason)
        if len(field) > 200:
            if "garbage_string" not in reasons:
                reasons.append("garbage_string")
    if artist:
        sep_count = len(re.findall(r"feat\.|ft\.|&|;|,|/", artist, re.I))
        if sep_count >= 4:
            reasons.append("excessive_artists")
    return bool(reasons), ";".join(reasons)


def extract_and_score_metadata(file_path):
    """Extract ID3 tags and compute confidence. Returns dict."""
    result = {
        "artist_tag": "", "title_tag": "", "album": "", "genre": "",
        "track_number": "", "tag_version": "", "metadata_confidence": 0.0,
        "metadata_junk_flag": 0, "metadata_junk_reason": "",
    }

    try:
        tags = ID3(file_path)
    except ID3NoHeaderError:
        result["tag_version"] = "NONE"
        return result
    except Exception as e:
        result["tag_version"] = f"ERROR:{type(e).__name__}"
        return result

    result["tag_version"] = f"ID3v2.{tags.version[1]}" if tags.version else "UNKNOWN"

    field_map = {
        "TPE1": "artist_tag", "TIT2": "title_tag",
        "TALB": "album", "TCON": "genre", "TRCK": "track_number",
    }
    filled = 0
    for tag_key, result_key in field_map.items():
        frame = tags.get(tag_key)
        if frame and frame.text:
            val = str(frame.text[0]).strip()
            result[result_key] = val
            if val:
                filled += 1

    # Base confidence from field coverage
    result["metadata_confidence"] = round(filled / 5.0, 2)

    # Junk detection
    flag, reasons = detect_junk(
        result["artist_tag"], result["title_tag"], result["album"]
    )
    result["metadata_junk_flag"] = int(flag)
    result["metadata_junk_reason"] = reasons

    # Penalize junk
    if flag:
        result["metadata_confidence"] = max(0.0, result["metadata_confidence"] - 0.2)

    return result


def part_d_metadata_ingest(conn):
    log("═══ PART D: Metadata Ingest ═══")
    tracks = conn.execute("SELECT track_id, file_path FROM tracks").fetchall()

    batch = []
    junk_count = 0
    errors = 0
    for i, (track_id, file_path) in enumerate(tracks):
        meta = extract_and_score_metadata(file_path)
        batch.append((
            track_id,
            meta["artist_tag"], meta["title_tag"], meta["album"],
            meta["genre"], meta["track_number"], meta["tag_version"],
            meta["metadata_confidence"],
            meta["metadata_junk_flag"], meta["metadata_junk_reason"],
        ))
        if meta["metadata_junk_flag"]:
            junk_count += 1
        if (i + 1) % 500 == 0:
            log(f"  Extracted {i + 1}/{len(tracks)}...")

    conn.executemany(
        """INSERT INTO metadata_tags
           (track_id, artist_tag, title_tag, album, genre, track_number,
            tag_version, metadata_confidence, metadata_junk_flag, metadata_junk_reason)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        batch
    )
    conn.commit()

    no_tags = sum(1 for b in batch if b[6] == "NONE")
    log(f"Metadata ingested: {len(batch)} tracks, {junk_count} junk, {no_tags} no tags")
    return len(batch), junk_count


# ═══════════════════════════════════════════════════════════════════
# PART E — HYBRID RESOLUTION ENGINE
# ═══════════════════════════════════════════════════════════════════

def normalize_for_compare(text):
    """Normalize text for fuzzy comparison."""
    if not text:
        return ""
    t = text.lower().strip()
    t = re.sub(r"\s*\(.*?\)", "", t)
    t = re.sub(r"\s*\[.*?\]", "", t)
    t = re.sub(r"\s*feat\.?\s+.*$", "", t, flags=re.I)
    t = re.sub(r"\s*ft\.?\s+.*$", "", t, flags=re.I)
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def compute_similarity(a, b):
    """Similarity score between two strings (0-1)."""
    na = normalize_for_compare(a)
    nb = normalize_for_compare(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    if na in nb or nb in na:
        return 0.9
    return SequenceMatcher(None, na, nb).ratio()


def check_reversed(fn_artist, fn_title, meta_artist, meta_title):
    """Check if artist/title are swapped between filename and metadata."""
    if not fn_artist or not fn_title or not meta_artist or not meta_title:
        return False
    a_as_t = compute_similarity(fn_artist, meta_title)
    t_as_a = compute_similarity(fn_title, meta_artist)
    return a_as_t > 0.7 and t_as_a > 0.7


def part_e_hybrid_resolution(conn):
    log("═══ PART E: Hybrid Resolution Engine ═══")
    rows = conn.execute("""
        SELECT t.track_id,
               fp.artist_guess, fp.title_guess, fp.parse_confidence,
               mt.artist_tag, mt.title_tag, mt.metadata_confidence,
               mt.metadata_junk_flag
        FROM tracks t
        JOIN filename_parse fp ON fp.track_id = t.track_id
        JOIN metadata_tags mt ON mt.track_id = t.track_id
    """).fetchall()

    batch = []
    source_counts = Counter()
    reversed_count = 0
    review_count = 0

    for (track_id, fn_artist, fn_title, fn_conf,
         meta_artist, meta_title, meta_conf, junk_flag) in rows:

        fn_artist = fn_artist or ""
        fn_title = fn_title or ""
        meta_artist = meta_artist or ""
        meta_title = meta_title or ""

        # Check for reversed artist/title
        was_reversed = check_reversed(fn_artist, fn_title, meta_artist, meta_title)
        if was_reversed:
            reversed_count += 1

        # Similarity scores
        artist_sim = compute_similarity(fn_artist, meta_artist)
        title_sim = compute_similarity(fn_title, meta_title)

        # Decision logic
        requires_review = False

        if artist_sim >= 0.7 and title_sim >= 0.7:
            # CASE 1: Strong match — use hybrid (prefer metadata if clean)
            if junk_flag:
                chosen_artist = fn_artist
                chosen_title = fn_title
                source = "filename"
            else:
                chosen_artist = meta_artist or fn_artist
                chosen_title = meta_title or fn_title
                source = "hybrid"
            final_conf = max(fn_conf, meta_conf) * ((artist_sim + title_sim) / 2)

        elif not meta_artist and not meta_title:
            # CASE 4 variant: Metadata empty
            chosen_artist = fn_artist
            chosen_title = fn_title
            source = "filename"
            final_conf = fn_conf * 0.8

        elif junk_flag:
            # CASE 2: Metadata has junk — filename wins
            chosen_artist = fn_artist
            chosen_title = fn_title
            source = "filename"
            final_conf = fn_conf * 0.7

        elif was_reversed:
            # Reversed: metadata has artist/title swapped vs filename
            chosen_artist = fn_artist
            chosen_title = fn_title
            source = "filename"
            final_conf = fn_conf * 0.6
            requires_review = True

        elif fn_conf >= meta_conf and fn_artist:
            # CASE 2: Filename stronger
            chosen_artist = fn_artist
            chosen_title = fn_title
            source = "filename"
            final_conf = fn_conf * 0.7
            if artist_sim < 0.3 and meta_artist:
                requires_review = True

        elif meta_conf > fn_conf and not junk_flag:
            # CASE 3: Metadata stronger (no junk)
            chosen_artist = meta_artist
            chosen_title = meta_title
            source = "metadata"
            final_conf = meta_conf * 0.8
            if artist_sim < 0.3 and fn_artist:
                requires_review = True

        else:
            # CASE 4: Conflict
            chosen_artist = fn_artist or meta_artist
            chosen_title = fn_title or meta_title
            source = "filename" if fn_artist else "metadata"
            final_conf = max(fn_conf, meta_conf) * 0.5
            requires_review = True

        if requires_review:
            review_count += 1

        final_conf = round(min(1.0, max(0.0, final_conf)), 3)

        batch.append((
            track_id, chosen_artist, chosen_title, source,
            final_conf, int(was_reversed), int(requires_review),
        ))
        source_counts[source] += 1

    conn.executemany(
        """INSERT INTO hybrid_resolution
           (track_id, chosen_artist, chosen_title, source_used,
            final_confidence, was_reversed, requires_review)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        batch
    )
    conn.commit()

    log(f"Hybrid resolution: {len(batch)} tracks")
    log(f"  Sources: {dict(source_counts)}")
    log(f"  Reversed: {reversed_count}")
    log(f"  Requires review: {review_count}")
    return len(batch), source_counts, reversed_count, review_count


# ═══════════════════════════════════════════════════════════════════
# PART F — STATUS ASSIGNMENT
# ═══════════════════════════════════════════════════════════════════

def is_longform(file_name, file_size, duration):
    """Detect longform content (compilations, playlists, full albums)."""
    for pat in LONGFORM_PATTERNS:
        if pat.search(file_name):
            return True
    if file_size and file_size >= LONGFORM_SIZE_THRESHOLD:
        return True
    if duration and duration >= LONGFORM_DURATION_THRESHOLD:
        return True
    return False


def part_f_status(conn):
    log("═══ PART F: Status Assignment ═══")
    rows = conn.execute("""
        SELECT t.track_id, t.file_name, t.file_size, t.duration,
               hr.final_confidence, hr.requires_review,
               mt.metadata_junk_flag
        FROM tracks t
        JOIN hybrid_resolution hr ON hr.track_id = t.track_id
        JOIN metadata_tags mt ON mt.track_id = t.track_id
    """).fetchall()

    batch = []
    status_counts = Counter()
    now = datetime.now().isoformat()

    for (track_id, fname, fsize, duration,
         confidence, requires_review, junk_flag) in rows:

        # Longform detection
        if is_longform(fname, fsize, duration):
            status = "LONGFORM"
        elif junk_flag and confidence < 0.3:
            status = "JUNK"
        elif requires_review:
            status = "REVIEW"
        elif confidence >= 0.5:
            status = "CLEAN"
        else:
            status = "REVIEW"

        batch.append((track_id, status, None, 1))
        status_counts[status] += 1

    conn.executemany(
        """INSERT INTO track_status
           (track_id, status, duplicate_group_id, is_primary)
           VALUES (?, ?, ?, ?)""",
        batch
    )

    # Log status assignment events
    audit_batch = []
    for track_id, status, _, _ in batch:
        audit_batch.append((
            track_id, "STATUS_ASSIGNED",
            f"Initial status: {status}", now,
        ))
    conn.executemany(
        """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
           VALUES (?, ?, ?, ?)""",
        audit_batch
    )
    conn.commit()

    log(f"Status assigned: {len(batch)} tracks")
    for status, cnt in status_counts.most_common():
        log(f"  {status}: {cnt}")
    return status_counts


# ═══════════════════════════════════════════════════════════════════
# PART G — VALIDATION
# ═══════════════════════════════════════════════════════════════════

def part_g_validate(conn, total_ingested):
    log("═══ PART G: Validation ═══")
    checks = []

    # 1. All tracks inserted
    track_count = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    checks.append(("all_tracks_inserted",
                    track_count == total_ingested,
                    f"{track_count} inserted vs {total_ingested} scanned"))

    # 2. No duplicate file_path
    dup_paths = conn.execute(
        "SELECT file_path, COUNT(*) c FROM tracks GROUP BY file_path HAVING c > 1"
    ).fetchall()
    checks.append(("no_duplicate_paths",
                    len(dup_paths) == 0,
                    f"{len(dup_paths)} duplicates"))

    # 3. All tables populated
    tables = ["filename_parse", "metadata_tags", "hybrid_resolution", "track_status"]
    for table in tables:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        checks.append((f"{table}_populated",
                        cnt == track_count,
                        f"{cnt}/{track_count}"))

    # 4. hybrid_resolution complete
    hr_count = conn.execute("SELECT COUNT(*) FROM hybrid_resolution").fetchone()[0]
    checks.append(("hybrid_complete",
                    hr_count == track_count,
                    f"{hr_count}/{track_count}"))

    # 5. Audit log has entries
    audit_count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    checks.append(("audit_log_populated",
                    audit_count >= track_count,
                    f"{audit_count} entries"))

    all_pass = all(ok for _, ok, _ in checks)
    log(f"Validation: {'ALL PASS' if all_pass else 'FAIL'} ({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")
    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")

    return checks, all_pass


# ═══════════════════════════════════════════════════════════════════
# PART H — OUTPUT REPORTS
# ═══════════════════════════════════════════════════════════════════

def part_h_reports(conn):
    log("═══ PART H: Output Reports ═══")

    # 1) dj_library_summary_v1.csv
    status_rows = conn.execute(
        "SELECT status, COUNT(*) FROM track_status GROUP BY status"
    ).fetchall()
    status_dict = dict(status_rows)
    total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]

    summary = {
        "total_tracks": total,
        "clean_tracks": status_dict.get("CLEAN", 0),
        "review_tracks": status_dict.get("REVIEW", 0),
        "junk_tracks": status_dict.get("JUNK", 0),
        "longform_tracks": status_dict.get("LONGFORM", 0),
        "duplicate_tracks": status_dict.get("DUPLICATE", 0),
    }
    summary_path = DATA / "dj_library_summary_v1.csv"
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary.keys()))
        w.writeheader()
        w.writerow(summary)
    log(f"Wrote {summary_path}")

    # 2) hybrid_confidence_distribution_v1.csv
    conf_rows = conn.execute("""
        SELECT
            CASE
                WHEN final_confidence >= 0.8 THEN '0.8-1.0'
                WHEN final_confidence >= 0.6 THEN '0.6-0.8'
                WHEN final_confidence >= 0.4 THEN '0.4-0.6'
                WHEN final_confidence >= 0.2 THEN '0.2-0.4'
                ELSE '0.0-0.2'
            END AS bucket,
            COUNT(*) as count,
            source_used
        FROM hybrid_resolution
        GROUP BY bucket, source_used
        ORDER BY bucket, source_used
    """).fetchall()

    dist_path = DATA / "hybrid_confidence_distribution_v1.csv"
    with open(dist_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["confidence_bucket", "count", "source_used"])
        w.writerows(conf_rows)
    log(f"Wrote {dist_path}")

    return summary


# ═══════════════════════════════════════════════════════════════════
# PART I — REPORTING (PROOF ARTIFACTS)
# ═══════════════════════════════════════════════════════════════════

def part_i_report(conn, summary, checks, all_pass,
                  source_counts, reversed_count, review_count,
                  status_counts, junk_count):
    log("═══ PART I: Reporting ═══")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    total = summary["total_tracks"]

    # 00 — DB schema
    with open(PROOF_DIR / "00_db_schema.txt", "w", encoding="utf-8") as f:
        f.write("DJ Library Core — Database Schema\n")
        f.write("=" * 50 + "\n\n")
        tables = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        for name, sql in tables:
            f.write(f"--- {name} ---\n{sql}\n\n")
        indices = conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name"
        ).fetchall()
        f.write("--- Indices ---\n")
        for name, sql in indices:
            f.write(f"{sql}\n")

    # 01 — Ingest summary
    with open(PROOF_DIR / "01_ingest_summary.txt", "w", encoding="utf-8") as f:
        f.write("Ingest Summary\n" + "=" * 40 + "\n")
        f.write(f"Total tracks: {total}\n")
        folder_rows = conn.execute(
            "SELECT folder, COUNT(*) FROM tracks GROUP BY folder ORDER BY folder"
        ).fetchall()
        for folder, cnt in folder_rows:
            f.write(f"  {folder}: {cnt}\n")
        size_mb = conn.execute("SELECT SUM(file_size) FROM tracks").fetchone()[0]
        f.write(f"\nTotal size: {size_mb / (1024*1024):,.1f} MB\n")
        dur = conn.execute("SELECT SUM(duration) FROM tracks WHERE duration IS NOT NULL").fetchone()[0]
        if dur:
            hours = dur / 3600
            f.write(f"Total duration: {hours:,.1f} hours\n")

    # 02 — Filename parse summary
    with open(PROOF_DIR / "02_filename_parse_summary.txt", "w", encoding="utf-8") as f:
        f.write("Filename Parse Summary\n" + "=" * 40 + "\n")
        method_rows = conn.execute(
            "SELECT parse_method, COUNT(*) FROM filename_parse GROUP BY parse_method ORDER BY COUNT(*) DESC"
        ).fetchall()
        for method, cnt in method_rows:
            f.write(f"  {method}: {cnt} ({cnt/total*100:.1f}%)\n")
        no_artist = conn.execute(
            "SELECT COUNT(*) FROM filename_parse WHERE artist_guess IS NULL OR artist_guess = ''"
        ).fetchone()[0]
        f.write(f"\nNo artist detected: {no_artist}\n")

    # 03 — Metadata summary
    with open(PROOF_DIR / "03_metadata_summary.txt", "w", encoding="utf-8") as f:
        f.write("Metadata Summary\n" + "=" * 40 + "\n")
        tag_rows = conn.execute(
            "SELECT tag_version, COUNT(*) FROM metadata_tags GROUP BY tag_version ORDER BY COUNT(*) DESC"
        ).fetchall()
        for ver, cnt in tag_rows:
            f.write(f"  {ver}: {cnt}\n")
        f.write(f"\nJunk detected: {junk_count}\n")
        no_meta = conn.execute(
            "SELECT COUNT(*) FROM metadata_tags WHERE tag_version = 'NONE'"
        ).fetchone()[0]
        f.write(f"No tags: {no_meta}\n")

    # 04 — Hybrid resolution summary
    with open(PROOF_DIR / "04_hybrid_resolution_summary.txt", "w", encoding="utf-8") as f:
        f.write("Hybrid Resolution Summary\n" + "=" * 40 + "\n")
        f.write(f"Total resolved: {total}\n")
        for src, cnt in source_counts.most_common():
            f.write(f"  {src}: {cnt} ({cnt/total*100:.1f}%)\n")
        f.write(f"\nReversed artist/title: {reversed_count}\n")
        f.write(f"Requires review: {review_count}\n")
        avg_conf = conn.execute(
            "SELECT AVG(final_confidence) FROM hybrid_resolution"
        ).fetchone()[0]
        f.write(f"Average confidence: {avg_conf:.3f}\n")

    # 05 — Status distribution
    with open(PROOF_DIR / "05_status_distribution.txt", "w", encoding="utf-8") as f:
        f.write("Status Distribution\n" + "=" * 40 + "\n")
        for status, cnt in status_counts.most_common():
            f.write(f"  {status}: {cnt} ({cnt/total*100:.1f}%)\n")

    # 06 — Validation checks
    with open(PROOF_DIR / "06_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write("Validation Checks\n" + "=" * 40 + "\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'}\n")

    # 07 — Final report
    with open(PROOF_DIR / "07_final_report.txt", "w", encoding="utf-8") as f:
        f.write("DJ Library Core — Phase 1 Final Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"GATE={'PASS' if all_pass else 'FAIL'}\n\n")
        f.write(f"Total tracks:        {total}\n")
        f.write(f"CLEAN:               {summary['clean_tracks']}\n")
        f.write(f"REVIEW:              {summary['review_tracks']}\n")
        f.write(f"JUNK:                {summary['junk_tracks']}\n")
        f.write(f"LONGFORM:            {summary['longform_tracks']}\n")
        f.write(f"DUPLICATE:           {summary['duplicate_tracks']}\n")
        f.write(f"\nHybrid sources:      {dict(source_counts)}\n")
        f.write(f"Reversed:            {reversed_count}\n")
        f.write(f"Requires review:     {review_count}\n")
        f.write(f"Metadata junk:       {junk_count}\n")
        f.write(f"\nDatabase: {DB_PATH}\n")
        f.write(f"DB size: {DB_PATH.stat().st_size:,} bytes\n")

    # Execution log
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")

    # Bundle zip
    zip_path = BASE / "_proof" / "dj_library_core_phase1.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pf in sorted(PROOF_DIR.iterdir()):
            if pf.is_file():
                zf.write(pf, f"dj_library_core_phase1/{pf.name}")
        # Include summary CSVs
        for csv_name in ["dj_library_summary_v1.csv",
                         "hybrid_confidence_distribution_v1.csv"]:
            csv_path = DATA / csv_name
            if csv_path.exists():
                zf.write(csv_path, f"dj_library_core_phase1/{csv_name}")

    log(f"ZIP={zip_path} ({zip_path.stat().st_size:,} bytes)")
    return zip_path


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log("DJ Library Core — Hybrid Ingest Database (Phase 1) — BEGIN")
    log(f"Working directory: {BASE}")
    log(f"Music root: {MUSIC_ROOT}")

    conn = part_a_create_db()
    total_ingested = part_b_ingest(conn)
    part_c_filename_parse(conn)
    meta_count, junk_count = part_d_metadata_ingest(conn)
    hr_count, source_counts, reversed_count, review_count = part_e_hybrid_resolution(conn)
    status_counts = part_f_status(conn)
    checks, all_pass = part_g_validate(conn, total_ingested)
    summary = part_h_reports(conn)
    zip_path = part_i_report(
        conn, summary, checks, all_pass,
        source_counts, reversed_count, review_count,
        status_counts, junk_count,
    )

    conn.close()

    log("")
    log("=" * 60)
    log("DJ LIBRARY CORE — PHASE 1 COMPLETE")
    log(f"  Total tracks:    {summary['total_tracks']}")
    log(f"  CLEAN:           {summary['clean_tracks']}")
    log(f"  REVIEW:          {summary['review_tracks']}")
    log(f"  JUNK:            {summary['junk_tracks']}")
    log(f"  LONGFORM:        {summary['longform_tracks']}")
    log(f"  DUPLICATE:       {summary['duplicate_tracks']}")
    log(f"  PF={PROOF_DIR}")
    log(f"  ZIP={zip_path}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
