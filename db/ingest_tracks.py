#!/usr/bin/env python3
"""
Track Library Ingestion — Phase 2
Reads tracks from the legacy library.db and populates db/song_analysis.db tracks table.
For files that exist on disk, reads audio metadata (sample_rate, channels, bit_depth, etc.)
via mutagen.  Deduplicates on file_path.  Logs skipped files.  Fail-closed.
"""

import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

try:
    import mutagen
except ImportError:
    print("ERROR: mutagen not installed.  pip install mutagen", file=sys.stderr)
    sys.exit(1)

# ── paths ──────────────────────────────────────────────────────────────────
WORKSPACE   = Path(__file__).resolve().parent.parent
LEGACY_DB   = Path(os.environ["APPDATA"]) / "ngksplayer" / "library.db"
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
PROOF_DIR   = WORKSPACE / "_proof" / "track_ingestion"

# ── helpers ────────────────────────────────────────────────────────────────
def sha256_file(path: Path) -> str | None:
    """Return hex SHA-256 of a file, or None if unreadable."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(1 << 16):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def read_audio_meta(path: Path) -> dict:
    """Use mutagen to read sample_rate, channels, bit_depth, duration_sec."""
    meta = {"sample_rate": None, "channels": None, "bit_depth": None, "duration_sec_audio": None}
    try:
        mf = mutagen.File(str(path))
        if mf is None:
            return meta
        if hasattr(mf.info, "sample_rate"):
            meta["sample_rate"] = mf.info.sample_rate
        if hasattr(mf.info, "channels"):
            meta["channels"] = mf.info.channels
        if hasattr(mf.info, "bits_per_sample"):
            meta["bit_depth"] = mf.info.bits_per_sample
        if hasattr(mf.info, "length"):
            meta["duration_sec_audio"] = round(mf.info.length, 6)
    except Exception:
        pass
    return meta


def file_format_from_ext(path: Path) -> str | None:
    ext = path.suffix.lower().lstrip(".")
    return ext if ext else None


# ── main ───────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # ── validate inputs ────────────────────────────────────────────────────
    if not LEGACY_DB.exists():
        print(f"FATAL: legacy DB not found: {LEGACY_DB}", file=sys.stderr)
        sys.exit(1)
    if not ANALYSIS_DB.exists():
        print(f"FATAL: analysis DB not found: {ANALYSIS_DB}", file=sys.stderr)
        sys.exit(1)

    # ── read legacy tracks ─────────────────────────────────────────────────
    legacy = sqlite3.connect(str(LEGACY_DB))
    legacy.row_factory = sqlite3.Row
    rows = legacy.execute(
        "SELECT id, title, artist, album, year, disc, trackNo, duration, filePath FROM tracks ORDER BY id"
    ).fetchall()
    legacy.close()
    print(f"Legacy DB: {len(rows)} tracks read")

    # ── open analysis DB ───────────────────────────────────────────────────
    ana = sqlite3.connect(str(ANALYSIS_DB))
    ana.execute("PRAGMA journal_mode=WAL;")
    ana.execute("PRAGMA foreign_keys=ON;")

    inserted = 0
    skipped_dup = 0
    skipped_err = 0
    file_missing = 0
    file_hashed = 0
    log_lines: list[str] = []

    for row in rows:
        file_path_raw = row["filePath"]
        if not file_path_raw:
            skipped_err += 1
            log_lines.append(f"SKIP id={row['id']} reason=empty_path")
            continue

        file_path = file_path_raw.strip()

        # ── check duplicate ────────────────────────────────────────────────
        existing = ana.execute("SELECT id FROM tracks WHERE file_path = ?", (file_path,)).fetchone()
        if existing:
            skipped_dup += 1
            log_lines.append(f"DUP  id={row['id']} file_path={file_path}")
            continue

        # ── probe the file on disk ─────────────────────────────────────────
        fp = Path(file_path)
        on_disk = fp.exists()

        audio_meta = {}
        file_hash = None
        file_size = None
        file_format = None

        if on_disk:
            audio_meta = read_audio_meta(fp)
            file_hash = sha256_file(fp)
            if file_hash:
                file_hashed += 1
            try:
                file_size = fp.stat().st_size
            except OSError:
                pass
            file_format = file_format_from_ext(fp)
        else:
            file_missing += 1
            file_format = file_format_from_ext(fp)
            log_lines.append(f"MISS id={row['id']} file_path={file_path}")

        # ── duration: prefer legacy DB value, fall back to mutagen ─────────
        duration = row["duration"]
        if duration is None and audio_meta.get("duration_sec_audio") is not None:
            duration = audio_meta["duration_sec_audio"]

        # ── INSERT ─────────────────────────────────────────────────────────
        try:
            ana.execute(
                """INSERT INTO tracks
                   (file_path, title, artist, album, album_artist, composer,
                    year, track_number, disc_number,
                    duration_sec, sample_rate, channels, bit_depth,
                    file_format, file_size_bytes, file_hash_sha256)
                   VALUES (?,?,?,?,?,?, ?,?,?, ?,?,?,?, ?,?,?)""",
                (
                    file_path,
                    row["title"],
                    row["artist"],
                    row["album"],
                    None,  # album_artist — not in legacy DB
                    None,  # composer — not in legacy DB
                    row["year"],
                    row["trackNo"],
                    row["disc"],
                    duration,
                    audio_meta.get("sample_rate"),
                    audio_meta.get("channels"),
                    audio_meta.get("bit_depth"),
                    file_format,
                    file_size,
                    file_hash,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError as e:
            skipped_err += 1
            log_lines.append(f"ERR  id={row['id']} file_path={file_path} error={e}")
        except Exception as e:
            skipped_err += 1
            log_lines.append(f"ERR  id={row['id']} file_path={file_path} error={e}")

    ana.commit()

    # ── post-ingest stats ──────────────────────────────────────────────────
    total_in_db = ana.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    null_hash   = ana.execute("SELECT COUNT(*) FROM tracks WHERE file_hash_sha256 IS NULL").fetchone()[0]
    null_sr     = ana.execute("SELECT COUNT(*) FROM tracks WHERE sample_rate IS NULL").fetchone()[0]
    null_dur    = ana.execute("SELECT COUNT(*) FROM tracks WHERE duration_sec IS NULL").fetchone()[0]
    ana.close()

    elapsed = round(time.time() - t0, 2)

    # ── summary ────────────────────────────────────────────────────────────
    summary = {
        "legacy_total": len(rows),
        "inserted": inserted,
        "skipped_duplicate": skipped_dup,
        "skipped_error": skipped_err,
        "file_missing_on_disk": file_missing,
        "files_hashed": file_hashed,
        "total_in_db_after": total_in_db,
        "null_hash_count": null_hash,
        "null_sample_rate_count": null_sr,
        "null_duration_count": null_dur,
        "elapsed_sec": elapsed,
    }

    print(f"\n{'='*60}")
    for k, v in summary.items():
        print(f"  {k:30s}: {v}")
    print(f"{'='*60}")

    # ── proof files ────────────────────────────────────────────────────────
    (PROOF_DIR / "00_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (PROOF_DIR / "01_skipped_log.txt").write_text("\n".join(log_lines) if log_lines else "(none)", encoding="utf-8")

    # sample rows
    ana2 = sqlite3.connect(str(ANALYSIS_DB))
    cur2 = ana2.cursor()
    cur2.execute(
        "SELECT id, file_path, title, artist, album, year, track_number, disc_number, "
        "duration_sec, sample_rate, channels, bit_depth, file_format, file_size_bytes, "
        "CASE WHEN file_hash_sha256 IS NOT NULL THEN 'Y' ELSE 'N' END AS hashed "
        "FROM tracks ORDER BY id LIMIT 20"
    )
    sample = cur2.fetchall()
    cols = [d[0] for d in cur2.description]
    ana2.close()

    sample_lines = ["\t".join(cols)]
    for r in sample:
        sample_lines.append("\t".join(str(x) for x in r))
    (PROOF_DIR / "02_sample_rows.tsv").write_text("\n".join(sample_lines), encoding="utf-8")

    # null audit
    null_audit = f"null_hash={null_hash}  null_sample_rate={null_sr}  null_duration={null_dur}\ntotal_in_db={total_in_db}"
    (PROOF_DIR / "03_null_audit.txt").write_text(null_audit, encoding="utf-8")

    if inserted == len(rows) - skipped_dup - skipped_err:
        gate = "PASS"
    else:
        gate = "FAIL"

    gate_line = f"GATE={gate}  inserted={inserted}  expected={len(rows) - skipped_dup - skipped_err}"
    (PROOF_DIR / "04_gate.txt").write_text(gate_line, encoding="utf-8")
    print(f"\n{gate_line}")

    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
