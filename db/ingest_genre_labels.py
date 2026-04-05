#!/usr/bin/env python3
"""
Genre Truth Collection - Phase 4
Step 1: Generate genre_labels_input.csv from legacy library.db ID3 genre tags.
Step 2: Ingest into track_genre_labels with strict validation.
"""

import csv
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
LEGACY_DB = Path(os.environ["APPDATA"]) / "ngksplayer" / "library.db"
INPUT_CSV = WORKSPACE / "data" / "genre_labels_input.csv"
PROOF_DIR = WORKSPACE / "_proof" / "genre_truth_collection"

GENRE_MAP = {
    "Country": ("Country", None),
    "Rock": ("Rock", None),
    "Hard Rock": ("Rock", "Classic Rock"),
    "Pop": ("Pop", None),
    "Metal": ("Metal", None),
    "Alternative": ("Rock", "Alternative"),
    "Alternative Rock": ("Rock", "Alternative"),
    "Reggae": ("Reggae", None),
    "World": ("World", None),
    "Classic Rock": ("Rock", "Classic Rock"),
    "Country Rock": ("Country", None),
    "Southern Rock": ("Rock", "Classic Rock"),
    "Bluegrass": ("Country", "Bluegrass"),
    "Folk": ("Folk", None),
    "Country Folk": ("Country", None),
    "New Wave": ("Rock", "Alternative"),
    "Soundtrack": ("Soundtrack", None),
    "Hip-Hop": ("Hip-Hop", None),
    "Hip-Hop, Rap": ("Hip-Hop", None),
    "Contemporary Hip Hop": ("Hip-Hop", None),
    "Rap": ("Hip-Hop", None),
    "Club Rap": ("Hip-Hop", None),
    "Pop Rap": ("Hip-Hop", None),
    "R&B, Soul": ("R&B", None),
    "Lovers Rock": ("Reggae", None),
}

UNMAPPABLE = {"Unknown Genre", "Comedy", "Pop, Rock"}


def main():
    t0 = time.time()
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    log = []
    conflicts = []
    invalid_labels = []
    unmatched_tracks = []

    def emit(msg):
        log.append(msg)
        print(msg)

    for p, label in [(ANALYSIS_DB, "analysis DB"), (LEGACY_DB, "legacy DB")]:
        if not p.exists():
            emit(f"FATAL: {label} not found: {p}")
            sys.exit(1)
        emit(f"SOURCE OK: {label} -> {p}")

    # STEP 1: Generate CSV
    emit("\n=== STEP 1: Generate genre_labels_input.csv ===")

    legacy = sqlite3.connect(str(LEGACY_DB))
    legacy.row_factory = sqlite3.Row
    legacy_rows = legacy.execute(
        "SELECT filePath, title, artist, genre FROM tracks "
        "WHERE genre IS NOT NULL AND genre != '' AND genre != 'Unknown Genre' "
        "ORDER BY id"
    ).fetchall()
    legacy.close()
    emit(f"Legacy tracks with genre: {len(legacy_rows)}")

    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    csv_rows_written = 0
    csv_rows_skipped = 0
    unmapped_genres = {}

    with open(INPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "library_path", "title", "artist", "genre", "subgenre",
            "label_role", "confidence", "notes"
        ])
        writer.writeheader()

        for row in legacy_rows:
            genre_raw = row["genre"].strip()

            if genre_raw in UNMAPPABLE:
                csv_rows_skipped += 1
                unmapped_genres[genre_raw] = unmapped_genres.get(genre_raw, 0) + 1
                continue

            mapping = GENRE_MAP.get(genre_raw)
            if mapping is None:
                csv_rows_skipped += 1
                unmapped_genres[genre_raw] = unmapped_genres.get(genre_raw, 0) + 1
                invalid_labels.append(
                    f"UNMAPPABLE genre='{genre_raw}' file={row['filePath']}"
                )
                continue

            genre_name, subgenre_name = mapping
            writer.writerow({
                "library_path": row["filePath"],
                "title": row["title"],
                "artist": row["artist"],
                "genre": genre_name,
                "subgenre": subgenre_name or "",
                "label_role": "primary",
                "confidence": "0.85",
                "notes": f"id3_tag:{genre_raw}",
            })
            csv_rows_written += 1

    emit(f"CSV written: {csv_rows_written} rows to {INPUT_CSV}")
    emit(f"CSV skipped: {csv_rows_skipped} (unmappable genres)")
    if unmapped_genres:
        emit(f"Unmapped genres: {unmapped_genres}")

    # STEP 2: Ingest CSV
    emit("\n=== STEP 2: Ingest CSV into track_genre_labels ===")

    ana = sqlite3.connect(str(ANALYSIS_DB))
    ana.execute("PRAGMA journal_mode=WAL;")
    ana.execute("PRAGMA foreign_keys=ON;")
    cur = ana.cursor()

    tracks_by_path = {}
    tracks_by_bn = {}
    tracks_by_ta = {}

    for tid, fp in cur.execute("SELECT id, file_path FROM tracks").fetchall():
        bn = os.path.basename(fp)
        tracks_by_path[fp.lower()] = tid
        if bn not in tracks_by_bn:
            tracks_by_bn[bn] = (tid, fp)

    for tid, title, artist in cur.execute(
        "SELECT id, title, artist FROM tracks"
    ).fetchall():
        if title and artist:
            key = f"{title.strip().lower()}|||{artist.strip().lower()}"
            tracks_by_ta[key] = tid

    emit(
        f"Tracks: {len(tracks_by_path)} by path, "
        f"{len(tracks_by_bn)} by basename, "
        f"{len(tracks_by_ta)} by title+artist"
    )

    genres = {}
    for gid, gname in cur.execute("SELECT id, name FROM genres").fetchall():
        genres[gname.lower()] = gid

    subgenres = {}
    for sid, sname, sgid in cur.execute(
        "SELECT id, name, genre_id FROM subgenres"
    ).fetchall():
        subgenres[(sname.lower(), sgid)] = sid

    emit(f"Genres: {len(genres)}, Subgenres: {len(subgenres)}")

    existing_primaries = set()
    for (tid,) in cur.execute(
        "SELECT track_id FROM track_genre_labels WHERE role='primary'"
    ).fetchall():
        existing_primaries.add(tid)

    emit(f"Existing primary labels: {len(existing_primaries)}")

    with open(INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
        input_rows = list(csv.DictReader(f))
    emit(f"Input CSV rows: {len(input_rows)}")

    inserted = 0
    skipped_match = 0
    skipped_genre = 0
    skipped_dup = 0
    matched_tracks = []

    for i, row in enumerate(input_rows):
        lib_path = row.get("library_path", "").strip()
        title = row.get("title", "").strip()
        artist = row.get("artist", "").strip()
        genre_name = row.get("genre", "").strip()
        subgenre_name = row.get("subgenre", "").strip()
        label_role = row.get("label_role", "primary").strip()
        confidence = row.get("confidence", "").strip()
        notes = row.get("notes", "").strip()

        track_id = None
        match_method = None

        if lib_path:
            tid = tracks_by_path.get(lib_path.lower())
            if tid:
                track_id = tid
                match_method = "path"
            else:
                bn = os.path.basename(lib_path)
                entry = tracks_by_bn.get(bn)
                if entry:
                    track_id = entry[0]
                    match_method = "basename"

        if track_id is None and title and artist:
            key = f"{title.lower()}|||{artist.lower()}"
            tid = tracks_by_ta.get(key)
            if tid:
                track_id = tid
                match_method = "title+artist"

        if track_id is None:
            skipped_match += 1
            unmatched_tracks.append(
                f"row={i+1} path={lib_path} title={title} artist={artist}"
            )
            continue

        genre_id = genres.get(genre_name.lower())
        if genre_id is None:
            skipped_genre += 1
            invalid_labels.append(
                f"row={i+1} INVALID_GENRE genre='{genre_name}' track_id={track_id}"
            )
            continue

        subgenre_id = None
        if subgenre_name:
            sg_key = (subgenre_name.lower(), genre_id)
            subgenre_id = subgenres.get(sg_key)
            if subgenre_id is None:
                skipped_genre += 1
                invalid_labels.append(
                    f"row={i+1} INVALID_SUBGENRE subgenre='{subgenre_name}' "
                    f"genre='{genre_name}' track_id={track_id}"
                )
                continue

        if label_role not in ("primary", "secondary", "candidate"):
            label_role = "primary"

        if label_role == "primary" and track_id in existing_primaries:
            skipped_dup += 1
            conflicts.append(
                f"row={i+1} DUP_PRIMARY track_id={track_id} genre={genre_name}"
            )
            continue

        try:
            conf = float(confidence) if confidence else 0.85
            if conf < 0 or conf > 1:
                conf = 0.85
        except ValueError:
            conf = 0.85

        applied = f"system_import:{notes}" if notes else "system_import"

        try:
            cur.execute(
                "INSERT INTO track_genre_labels "
                "(track_id, genre_id, subgenre_id, role, source, confidence, applied_by) "
                "VALUES (?, ?, ?, ?, 'manual', ?, ?)",
                (track_id, genre_id, subgenre_id, label_role, conf, applied),
            )
            inserted += 1
            if label_role == "primary":
                existing_primaries.add(track_id)
            matched_tracks.append(
                f"track_id={track_id:4d} genre={genre_name:12s} "
                f"sub={subgenre_name or '(none)':20s} match={match_method}"
            )
        except sqlite3.IntegrityError as e:
            skipped_dup += 1
            conflicts.append(
                f"row={i+1} INTEGRITY track_id={track_id} "
                f"genre={genre_name} err={e}"
            )

    ana.commit()
    emit(f"\nInserted: {inserted}")
    emit(f"Skipped (unmatched): {skipped_match}")
    emit(f"Skipped (invalid genre/subgenre): {skipped_genre}")
    emit(f"Skipped (duplicate primary): {skipped_dup}")

    # VALIDATION
    emit("\n=== VALIDATION ===")
    vq_results = ["=== VALIDATION QUERIES ===", ""]

    validation_queries = [
        "SELECT COUNT(*) FROM track_genre_labels;",
        "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary';",
        ("SELECT track_id, COUNT(*) FROM track_genre_labels "
         "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
        "SELECT COUNT(*) FROM track_genre_labels WHERE genre_id IS NULL;",
        "PRAGMA foreign_key_check;",
        ("SELECT g.name, COUNT(*) FROM track_genre_labels tgl "
         "JOIN genres g ON tgl.genre_id=g.id GROUP BY g.name ORDER BY COUNT(*) DESC;"),
    ]

    for q in validation_queries:
        result = cur.execute(q).fetchall()
        vq_results.append(f"-- {q}")
        if result:
            for r in result:
                vq_results.append(f"   {r}")
        else:
            vq_results.append("   (empty)")
        vq_results.append("")

    for line in vq_results[2:]:
        emit(line)

    total_labels = cur.execute(
        "SELECT COUNT(*) FROM track_genre_labels"
    ).fetchone()[0]

    dup_primaries = cur.execute(
        "SELECT COUNT(*) FROM ("
        "SELECT track_id FROM track_genre_labels WHERE role='primary' "
        "GROUP BY track_id HAVING COUNT(*) > 1)"
    ).fetchone()[0]

    null_genre = cur.execute(
        "SELECT COUNT(*) FROM track_genre_labels WHERE genre_id IS NULL"
    ).fetchone()[0]

    fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()

    ana.close()

    gate_ok = (
        inserted > 0
        and dup_primaries == 0
        and null_genre == 0
        and len(fk_violations) == 0
    )
    gate = "PASS" if gate_ok else "FAIL"
    elapsed = round(time.time() - t0, 2)

    emit(f"\nGATE={gate}")

    # PROOF FILES
    def w(name, text):
        (PROOF_DIR / name).write_text(text, encoding="utf-8")

    w("00_input_summary.txt", "\n".join([
        "=== INPUT SUMMARY ===",
        f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Legacy DB: {LEGACY_DB}",
        f"  Tracks with genre: {len(legacy_rows)}",
        f"  Unmappable genres skipped: {csv_rows_skipped}",
        f"  Unmapped: {unmapped_genres}",
        "",
        f"Generated CSV: {INPUT_CSV}",
        f"  Rows written: {csv_rows_written}",
        "",
        f"Genre mapping table ({len(GENRE_MAP)} entries):",
    ] + [
        f"  '{k}' -> {v[0]}" + (f" / {v[1]}" if v[1] else "")
        for k, v in sorted(GENRE_MAP.items())
    ]))

    w("01_tracks_matched.txt", "\n".join([
        f"=== TRACKS MATCHED: {len(matched_tracks)} ===",
        "",
    ] + matched_tracks))

    w("02_rows_inserted.txt", "\n".join([
        "=== ROWS INSERTED ===",
        f"track_genre_labels: {inserted}",
        "",
        f"Skipped (unmatched): {skipped_match}",
        f"Skipped (invalid genre/subgenre): {skipped_genre}",
        f"Skipped (duplicate primary): {skipped_dup}",
    ]))

    w("03_conflicts_logged.txt",
      "\n".join(conflicts) if conflicts else "(no conflicts)")

    w("04_invalid_labels.txt",
      "\n".join(invalid_labels) if invalid_labels else "(no invalid labels)")

    w("05_unmatched_tracks.txt",
      "\n".join(unmatched_tracks) if unmatched_tracks else "(all tracks matched)")

    w("06_validation_queries.txt", "\n".join(vq_results))

    summary = {
        "gate": gate,
        "elapsed_sec": elapsed,
        "input_csv_rows": csv_rows_written,
        "inserted": inserted,
        "skipped_unmatched": skipped_match,
        "skipped_invalid": skipped_genre,
        "skipped_duplicate": skipped_dup,
        "total_labels_in_db": total_labels,
        "dup_primaries": dup_primaries,
        "null_genre_ids": null_genre,
        "fk_violations": len(fk_violations),
    }

    w("07_final_report.txt", "\n".join([
        "=== GENRE TRUTH COLLECTION - FINAL REPORT ===",
        f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"GATE: {gate}",
        f"Elapsed: {elapsed}s",
        "",
        "Source: Legacy library.db ID3 genre tags -> strict mapping -> CSV",
        "",
    ] + [
        f"  {k:30s}: {v}" for k, v in summary.items()
    ] + [
        "",
        "Integrity:",
        f"  Duplicate primary labels: {dup_primaries}",
        f"  Null genre_id rows:       {null_genre}",
        f"  FK violations:            {len(fk_violations)}",
    ]))

    w("execution_log.txt", "\n".join(log))

    print(f"\n{'=' * 60}")
    for k, v in summary.items():
        print(f"  {k:30s}: {v}")
    print(f"{'=' * 60}")
    print(f"GATE={gate}")

    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
