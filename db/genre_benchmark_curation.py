#!/usr/bin/env python3
"""
Genre Benchmark Curation — Phase 6
Creates a curated benchmark set (100-250 tracks) with manually verified
genre/subgenre labels and full audit trail.

Stages:
  candidates  — select balanced candidate pool, write genre_benchmark_candidates.csv
  template    — generate review input template genre_benchmark_review.csv
  ingest      — process reviewed rows, update labels, populate benchmark_set_tracks

Usage:
  python db/genre_benchmark_curation.py candidates
  python db/genre_benchmark_curation.py template
  python db/genre_benchmark_curation.py ingest
  python db/genre_benchmark_curation.py all          # candidates + template (no ingest)
"""

import csv
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
CANDIDATES_CSV = WORKSPACE / "data" / "genre_benchmark_candidates.csv"
REVIEW_CSV = WORKSPACE / "data" / "genre_benchmark_review.csv"
PROOF_DIR = WORKSPACE / "_proof" / "genre_benchmark_curation"

BENCHMARK_NAME = "genre_benchmark_v1"
BENCHMARK_DESC = "Manually curated high-confidence genre/subgenre truth set"

# Target: 100-250 tracks, balanced across genres
# Sampling strategy: per-genre quota with minimums for small genres
TARGET_TOTAL = 200
MIN_PER_GENRE = 1  # every represented genre gets at least 1
UNLABELED_SAMPLE = 15  # include some from the 194 unlabeled tracks

VALID_ACTIONS = {"confirm", "replace", "add_secondary"}


class Pipeline:
    def __init__(self):
        self.log = []
        self.conflicts = []
        self.invalid_entries = []
        self.rows_updated = []
        self.rows_inserted = []
        self.benchmark_members = []
        self.t0 = time.time()

    def emit(self, msg):
        self.log.append(msg)
        print(msg)

    def connect(self, readonly=False):
        uri = f"file:{ANALYSIS_DB}" + ("?mode=ro" if readonly else "")
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    # ================================================================
    # STAGE 1: Build candidate pool
    # ================================================================
    def stage_candidates(self):
        self.emit("\n=== STAGE 1: Build candidate pool ===")
        conn = self.connect(readonly=True)

        # Get genre distribution
        genre_counts = conn.execute("""
            SELECT g.name AS genre, COUNT(*) AS cnt
            FROM track_genre_labels tgl
            JOIN genres g ON tgl.genre_id = g.id
            WHERE tgl.role = 'primary'
            GROUP BY g.name
            ORDER BY cnt DESC
        """).fetchall()

        total_labeled = sum(r["cnt"] for r in genre_counts)
        self.emit(f"Labeled tracks: {total_labeled} across {len(genre_counts)} genres")
        for r in genre_counts:
            self.emit(f"  {r['genre']:15s}: {r['cnt']}")

        # Calculate per-genre quotas (proportional, with minimums)
        labeled_budget = TARGET_TOTAL - UNLABELED_SAMPLE
        quotas = {}
        for r in genre_counts:
            genre = r["genre"]
            cnt = r["cnt"]
            # Proportional share, but at least MIN_PER_GENRE, at most available
            share = max(MIN_PER_GENRE, round(labeled_budget * cnt / total_labeled))
            quotas[genre] = min(share, cnt)

        # Adjust if over budget
        while sum(quotas.values()) > labeled_budget:
            # Trim largest genre first
            largest = max(quotas, key=quotas.get)
            quotas[largest] -= 1

        self.emit(f"\nSampling quotas (total={sum(quotas.values())} + {UNLABELED_SAMPLE} unlabeled):")
        for g, q in sorted(quotas.items(), key=lambda x: -x[1]):
            self.emit(f"  {g:15s}: {q}")

        # Sample tracks per genre using deterministic seed for reproducibility
        random.seed(42)
        candidates = []

        for genre, quota in quotas.items():
            rows = conn.execute("""
                SELECT t.id AS track_id, t.artist, t.title,
                       g.name AS current_genre,
                       COALESCE(sg.name, '') AS current_subgenre,
                       tgl.source, tgl.confidence
                FROM track_genre_labels tgl
                JOIN tracks t ON tgl.track_id = t.id
                JOIN genres g ON tgl.genre_id = g.id
                LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
                WHERE tgl.role = 'primary' AND g.name = ?
                ORDER BY t.id
            """, (genre,)).fetchall()

            sampled = random.sample(rows, min(quota, len(rows)))
            for r in sampled:
                candidates.append({
                    "track_id": r["track_id"],
                    "artist": r["artist"] or "",
                    "title": r["title"] or "",
                    "current_genre": r["current_genre"],
                    "current_subgenre": r["current_subgenre"],
                    "source": r["source"],
                    "notes": "labeled_candidate",
                })

        # Sample from unlabeled tracks
        unlabeled = conn.execute("""
            SELECT t.id AS track_id, t.artist, t.title
            FROM tracks t
            LEFT JOIN track_genre_labels tgl ON t.id = tgl.track_id
            WHERE tgl.id IS NULL
            ORDER BY t.id
        """).fetchall()

        unlabeled_sample = random.sample(
            list(unlabeled), min(UNLABELED_SAMPLE, len(unlabeled))
        )
        for r in unlabeled_sample:
            candidates.append({
                "track_id": r["track_id"],
                "artist": r["artist"] or "",
                "title": r["title"] or "",
                "current_genre": "",
                "current_subgenre": "",
                "source": "",
                "notes": "unlabeled_candidate",
            })

        conn.close()

        # Sort by track_id for consistency
        candidates.sort(key=lambda x: x["track_id"])

        CANDIDATES_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(CANDIDATES_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "current_genre",
                "current_subgenre", "source", "notes"
            ])
            writer.writeheader()
            for c in candidates:
                writer.writerow(c)

        self.emit(f"\nCandidates: {len(candidates)} tracks")
        self.emit(f"  Labeled: {len(candidates) - len(unlabeled_sample)}")
        self.emit(f"  Unlabeled: {len(unlabeled_sample)}")
        self.emit(f"Written to: {CANDIDATES_CSV}")

        return candidates

    # ================================================================
    # STAGE 2: Generate review template
    # ================================================================
    def stage_template(self):
        self.emit("\n=== STAGE 2: Generate review template ===")

        if not CANDIDATES_CSV.exists():
            self.emit(f"FATAL: Candidates CSV not found: {CANDIDATES_CSV}")
            self.emit("Run 'candidates' stage first.")
            return None

        with open(CANDIDATES_CSV, "r", encoding="utf-8-sig", newline="") as f:
            candidates = list(csv.DictReader(f))

        # Build review rows from ALL candidates (each must be individually reviewed)
        review_rows = []
        for c in candidates:
            has_label = bool(c.get("current_genre", "").strip())
            review_rows.append({
                "track_id": c["track_id"],
                "artist": c["artist"],
                "title": c["title"],
                "final_genre": c.get("current_genre", ""),
                "final_subgenre": c.get("current_subgenre", ""),
                "action": "confirm" if has_label else "replace",
                "confidence": "0.95" if has_label else "",
                "notes": "benchmark_review",
            })

        with open(REVIEW_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "final_genre",
                "final_subgenre", "action", "confidence", "notes"
            ])
            writer.writeheader()
            for r in review_rows:
                writer.writerow(r)

        self.emit(f"Review template: {len(review_rows)} rows")
        self.emit(f"  confirm: {sum(1 for r in review_rows if r['action'] == 'confirm')}")
        self.emit(f"  replace: {sum(1 for r in review_rows if r['action'] == 'replace')}")
        self.emit(f"Written to: {REVIEW_CSV}")
        self.emit("")
        self.emit("NEXT: Edit data/genre_benchmark_review.csv with your review decisions")
        self.emit("THEN: python db/genre_benchmark_curation.py ingest")

        return review_rows

    # ================================================================
    # STAGE 3: Ingest reviewed labels + populate benchmark set
    # ================================================================
    def stage_ingest(self):
        self.emit("\n=== STAGE 3: Ingest benchmark reviews ===")

        if not REVIEW_CSV.exists():
            self.emit(f"FATAL: Review CSV not found: {REVIEW_CSV}")
            return None

        with open(REVIEW_CSV, "r", encoding="utf-8-sig", newline="") as f:
            input_rows = list(csv.DictReader(f))

        self.emit(f"Input rows: {len(input_rows)}")

        if not (100 <= len(input_rows) <= 250):
            self.emit(
                f"WARNING: Expected 100-250 rows, got {len(input_rows)}. "
                f"Proceeding but flagging."
            )

        conn = self.connect()
        cur = conn.cursor()

        # ── Create or get benchmark set ──
        existing = cur.execute(
            "SELECT id FROM benchmark_sets WHERE name = ?",
            (BENCHMARK_NAME,)
        ).fetchone()

        if existing:
            bset_id = existing["id"]
            self.emit(f"Benchmark set exists: id={bset_id}")
        else:
            cur.execute(
                "INSERT INTO benchmark_sets (name, description) VALUES (?, ?)",
                (BENCHMARK_NAME, BENCHMARK_DESC)
            )
            bset_id = cur.lastrowid
            self.emit(f"Benchmark set created: id={bset_id} name={BENCHMARK_NAME}")

        # ── Lookups ──
        genres = {}
        for row in cur.execute("SELECT id, name FROM genres").fetchall():
            genres[row["name"].lower()] = row["id"]

        subgenres = {}
        for row in cur.execute(
            "SELECT id, name, genre_id FROM subgenres"
        ).fetchall():
            subgenres[(row["name"].lower(), row["genre_id"])] = row["id"]

        track_ids = set()
        for (tid,) in cur.execute("SELECT id FROM tracks").fetchall():
            track_ids.add(tid)

        existing_bst = set()
        for (tid,) in cur.execute(
            "SELECT track_id FROM benchmark_set_tracks WHERE benchmark_set_id = ?",
            (bset_id,)
        ).fetchall():
            existing_bst.add(tid)

        self.emit(
            f"Lookups: {len(genres)} genres, {len(subgenres)} subgenres, "
            f"{len(track_ids)} tracks, {len(existing_bst)} existing benchmark members"
        )

        processed = 0
        skipped = 0
        seen_ids = set()

        for i, row in enumerate(input_rows):
            row_num = i + 1

            try:
                track_id = int(row.get("track_id", "").strip())
            except (ValueError, AttributeError):
                self.invalid_entries.append(
                    f"row={row_num} INVALID track_id='{row.get('track_id', '')}'"
                )
                skipped += 1
                continue

            action = row.get("action", "").strip().lower()
            final_genre = row.get("final_genre", "").strip()
            final_subgenre = row.get("final_subgenre", "").strip()
            confidence_str = row.get("confidence", "").strip()
            notes = row.get("notes", "").strip()

            # ── validate track ──
            if track_id not in track_ids:
                self.invalid_entries.append(
                    f"row={row_num} MISSING_TRACK track_id={track_id}"
                )
                skipped += 1
                continue

            # ── validate action ──
            if action not in VALID_ACTIONS:
                self.invalid_entries.append(
                    f"row={row_num} INVALID_ACTION action='{action}' "
                    f"track_id={track_id}"
                )
                skipped += 1
                continue

            # ── duplicate check ──
            if track_id in seen_ids:
                self.conflicts.append(
                    f"row={row_num} DUPLICATE track_id={track_id} "
                    f"(already processed in this batch)"
                )
                skipped += 1
                continue
            seen_ids.add(track_id)

            # ── resolve genre/subgenre ──
            genre_id = None
            subgenre_id = None

            if action in ("confirm", "replace", "add_secondary"):
                if not final_genre:
                    self.invalid_entries.append(
                        f"row={row_num} MISSING_GENRE action={action} "
                        f"track_id={track_id}"
                    )
                    skipped += 1
                    continue

                genre_id = genres.get(final_genre.lower())
                if genre_id is None:
                    self.invalid_entries.append(
                        f"row={row_num} INVALID_GENRE genre='{final_genre}' "
                        f"track_id={track_id}"
                    )
                    skipped += 1
                    continue

                if final_subgenre:
                    subgenre_id = subgenres.get(
                        (final_subgenre.lower(), genre_id)
                    )
                    if subgenre_id is None:
                        self.invalid_entries.append(
                            f"row={row_num} INVALID_SUBGENRE "
                            f"subgenre='{final_subgenre}' "
                            f"genre='{final_genre}' track_id={track_id}"
                        )
                        skipped += 1
                        continue

            # ── confidence ──
            try:
                conf = float(confidence_str) if confidence_str else 0.95
                if conf < 0 or conf > 1:
                    conf = 0.95
            except ValueError:
                conf = 0.95

            # ── current primary ──
            current_primary = cur.execute(
                "SELECT id, genre_id, subgenre_id "
                "FROM track_genre_labels "
                "WHERE track_id = ? AND role = 'primary' "
                "ORDER BY id DESC LIMIT 1",
                (track_id,)
            ).fetchone()

            applied = f"benchmark_review:{notes}" if notes else "benchmark_review"

            # ── CONFIRM ──
            if action == "confirm":
                if current_primary is None:
                    self.conflicts.append(
                        f"row={row_num} CONFIRM_NO_PRIMARY track_id={track_id}"
                    )
                    skipped += 1
                    continue

                cur.execute(
                    "UPDATE track_genre_labels "
                    "SET source = 'manual', "
                    "    confidence = ?, "
                    "    applied_by = ?, "
                    "    created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                    "WHERE id = ?",
                    (conf, applied, current_primary["id"])
                )
                self.rows_updated.append(
                    f"track_id={track_id} label_id={current_primary['id']} "
                    f"action=confirm applied_by=benchmark_review"
                )

            # ── REPLACE ──
            elif action == "replace":
                if current_primary is not None:
                    cur.execute(
                        "UPDATE track_genre_labels "
                        "SET role = 'secondary' "
                        "WHERE id = ?",
                        (current_primary["id"],)
                    )
                    self.rows_updated.append(
                        f"track_id={track_id} label_id={current_primary['id']} "
                        f"action=downgrade_to_secondary"
                    )

                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, "
                        " confidence, applied_by) "
                        "VALUES (?, ?, ?, 'primary', 'manual', ?, ?)",
                        (track_id, genre_id, subgenre_id, conf, applied)
                    )
                    self.rows_inserted.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=primary "
                        f"action=replace"
                    )
                except sqlite3.IntegrityError as e:
                    self.conflicts.append(
                        f"row={row_num} INTEGRITY_REPLACE track_id={track_id} "
                        f"genre={final_genre} err={e}"
                    )
                    skipped += 1
                    continue

            # ── ADD_SECONDARY ──
            elif action == "add_secondary":
                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, "
                        " confidence, applied_by) "
                        "VALUES (?, ?, ?, 'secondary', 'manual', ?, ?)",
                        (track_id, genre_id, subgenre_id, conf, applied)
                    )
                    self.rows_inserted.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=secondary "
                        f"action=add_secondary"
                    )
                except sqlite3.IntegrityError as e:
                    self.conflicts.append(
                        f"row={row_num} INTEGRITY_SECONDARY track_id={track_id} "
                        f"genre={final_genre} err={e}"
                    )
                    skipped += 1
                    continue

            # ── Add to benchmark set ──
            if track_id not in existing_bst:
                # Resolve expected_genre string for benchmark_set_tracks
                eg = final_genre
                if final_subgenre:
                    eg = f"{final_genre} / {final_subgenre}"

                try:
                    cur.execute(
                        "INSERT INTO benchmark_set_tracks "
                        "(benchmark_set_id, track_id, expected_genre, notes) "
                        "VALUES (?, ?, ?, ?)",
                        (bset_id, track_id, eg,
                         f"role=genre_reference action={action}")
                    )
                    existing_bst.add(track_id)
                    self.benchmark_members.append(
                        f"track_id={track_id} genre={eg} action={action}"
                    )
                except sqlite3.IntegrityError as e:
                    self.conflicts.append(
                        f"row={row_num} BENCHMARK_DUP track_id={track_id} err={e}"
                    )

            processed += 1

        conn.commit()

        self.emit(f"\nProcessed: {processed}")
        self.emit(f"Skipped: {skipped}")
        self.emit(f"Label updates: {len(self.rows_updated)}")
        self.emit(f"Label inserts: {len(self.rows_inserted)}")
        self.emit(f"Benchmark members: {len(self.benchmark_members)}")
        self.emit(f"Conflicts: {len(self.conflicts)}")
        self.emit(f"Invalid: {len(self.invalid_entries)}")

        # ── VALIDATION ──
        self.emit("\n=== VALIDATION ===")
        vq_results = ["=== VALIDATION QUERIES ===", ""]

        validation_queries = [
            "SELECT COUNT(*) FROM benchmark_sets;",
            "SELECT COUNT(*) FROM benchmark_set_tracks;",
            (f"SELECT COUNT(*) FROM benchmark_set_tracks "
             f"WHERE benchmark_set_id = {bset_id};"),
            ("SELECT track_id, COUNT(*) FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
            ("SELECT COUNT(*) FROM track_genre_labels "
             "WHERE source='manual' AND applied_by LIKE 'benchmark_review%';"),
            "PRAGMA foreign_key_check;",
        ]

        for q in validation_queries:
            result = cur.execute(q).fetchall()
            vq_results.append(f"-- {q}")
            if result:
                for r in result:
                    vq_results.append(f"   {tuple(r)}")
            else:
                vq_results.append("   (empty)")
            vq_results.append("")

        for line in vq_results[2:]:
            self.emit(line)

        bset_count = cur.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = ?",
            (bset_id,)
        ).fetchone()[0]

        dup_primaries = cur.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels WHERE role='primary' "
            "  GROUP BY track_id HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]

        null_genre = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE genre_id IS NULL"
        ).fetchone()[0]

        fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()

        benchmark_review_count = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels "
            "WHERE source='manual' AND applied_by LIKE 'benchmark_review%'"
        ).fetchone()[0]

        conn.close()

        gate_ok = (
            processed > 0
            and 100 <= bset_count <= 250
            and dup_primaries == 0
            and null_genre == 0
            and len(fk_violations) == 0
        )
        gate = "PASS" if gate_ok else "FAIL"

        self.emit(f"\nGATE={gate}")

        return gate, {
            "benchmark_set_id": bset_id,
            "benchmark_name": BENCHMARK_NAME,
            "processed": processed,
            "skipped": skipped,
            "label_updates": len(self.rows_updated),
            "label_inserts": len(self.rows_inserted),
            "benchmark_members": bset_count,
            "benchmark_review_labels": benchmark_review_count,
            "conflicts": len(self.conflicts),
            "invalid": len(self.invalid_entries),
            "dup_primaries": dup_primaries,
            "null_genre_ids": null_genre,
            "fk_violations": len(fk_violations),
        }, vq_results, bset_id

    # ================================================================
    # PROOF FILES
    # ================================================================
    def write_proof(self, gate, summary, vq_results, bset_id=None):
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        w("00_benchmark_set_created.txt", "\n".join([
            "=== BENCHMARK SET ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"Name: {BENCHMARK_NAME}",
            f"Description: {BENCHMARK_DESC}",
            f"Set ID: {bset_id or 'N/A'}",
            f"Members: {summary.get('benchmark_members', 0)}",
        ]))

        # Candidate selection summary
        cand_info = "(not generated in this run)"
        if CANDIDATES_CSV.exists():
            with open(CANDIDATES_CSV, "r", encoding="utf-8-sig") as f:
                cand_count = sum(1 for _ in csv.DictReader(f))
            cand_info = f"Candidates CSV: {CANDIDATES_CSV}\nRows: {cand_count}"
        w("01_candidate_selection.txt", "\n".join([
            "=== CANDIDATE SELECTION ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            cand_info,
            "",
            f"Target: {TARGET_TOTAL} tracks",
            f"Unlabeled sample: {UNLABELED_SAMPLE}",
            f"Seed: 42 (deterministic)",
        ]))

        w("02_review_input_summary.txt", "\n".join([
            "=== REVIEW INPUT SUMMARY ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"File: {REVIEW_CSV}",
            f"Processed: {summary.get('processed', 0)}",
            f"Skipped: {summary.get('skipped', 0)}",
        ]))

        w("03_rows_updated.txt", "\n".join(
            [f"=== ROWS UPDATED: {len(self.rows_updated)} ===", ""]
            + (self.rows_updated if self.rows_updated else ["(none)"])
        ))

        w("04_rows_inserted.txt", "\n".join(
            [f"=== ROWS INSERTED: {len(self.rows_inserted)} ===", ""]
            + (self.rows_inserted if self.rows_inserted else ["(none)"])
        ))

        w("05_benchmark_membership.txt", "\n".join(
            [f"=== BENCHMARK MEMBERS: {len(self.benchmark_members)} ===", ""]
            + (self.benchmark_members if self.benchmark_members
               else ["(none)"])
        ))

        w("06_conflicts_logged.txt", "\n".join(
            [f"=== CONFLICTS: {len(self.conflicts)} ===", ""]
            + (self.conflicts if self.conflicts else ["(none)"])
        ))

        w("07_validation_queries.txt",
          "\n".join(vq_results) if vq_results else "(no validation run)")

        w("08_final_report.txt", "\n".join([
            "=== GENRE BENCHMARK CURATION - FINAL REPORT ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"GATE: {gate}",
            f"Elapsed: {elapsed}s",
            "",
        ] + [f"  {k:30s}: {v}" for k, v in summary.items()] + [
            "",
            "Integrity:",
            f"  Duplicate primaries: {summary.get('dup_primaries', 'N/A')}",
            f"  Null genre_ids:      {summary.get('null_genre_ids', 'N/A')}",
            f"  FK violations:       {summary.get('fk_violations', 'N/A')}",
        ]))

        w("execution_log.txt", "\n".join(self.log))

        self.emit(f"\nProof written to: {PROOF_DIR}")
        return PROOF_DIR


def main():
    if len(sys.argv) < 2:
        print("Usage: python db/genre_benchmark_curation.py "
              "<candidates|template|ingest|all>")
        return 1

    mode = sys.argv[1].lower()
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: DB not found: {ANALYSIS_DB}")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Mode: {mode}")

    if mode in ("candidates", "all"):
        p.stage_candidates()

    if mode in ("template", "all"):
        p.stage_template()

    if mode == "ingest":
        result = p.stage_ingest()
        if result is None:
            return 1
        gate, summary, vq_results, bset_id = result
        pf = p.write_proof(gate, summary, vq_results, bset_id)

        print(f"\n{'=' * 60}")
        for k, v in summary.items():
            print(f"  {k:30s}: {v}")
        print(f"{'=' * 60}")
        print(f"PF={pf}")
        print(f"GATE={gate}")
        return 0 if gate == "PASS" else 1

    if mode == "all":
        p.emit("\n=== STAGES 1+2 COMPLETE ===")
        p.emit(f"Candidates: {CANDIDATES_CSV}")
        p.emit(f"Review CSV: {REVIEW_CSV}")
        p.emit("")
        p.emit("NEXT:")
        p.emit("1. Edit data/genre_benchmark_review.csv")
        p.emit("2. Run: python db/genre_benchmark_curation.py ingest")

        partial = {
            "status": "candidates_and_template_generated",
            "benchmark_members": 0,
        }
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        p.write_proof("PENDING", partial, [])
        print(f"\nPF={PROOF_DIR}")
        print("GATE=PENDING")
        return 0

    if mode in ("candidates", "template"):
        return 0

    print(f"Unknown mode: {mode}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
