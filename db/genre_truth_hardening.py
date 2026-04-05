#!/usr/bin/env python3
"""
Genre Truth Hardening — Phase 5
Three stages:
  Stage 1: Generate review queue (data/genre_review_queue.csv)
  Stage 2: Generate review input template (data/genre_review_input.csv)
  Stage 3: Ingest review decisions from genre_review_input.csv

Usage:
  python db/genre_truth_hardening.py queue      # Stage 1 only
  python db/genre_truth_hardening.py template   # Stage 2 only
  python db/genre_truth_hardening.py ingest     # Stage 3 only
  python db/genre_truth_hardening.py all        # Stages 1+2 (no ingest without human review)

Rules:
  - NO automatic relabeling
  - NO silent updates
  - ALL changes logged + reversible
  - Fail-closed on ambiguity
"""

import csv
import os
import sqlite3
import sys
import time
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
REVIEW_QUEUE_CSV = WORKSPACE / "data" / "genre_review_queue.csv"
REVIEW_INPUT_CSV = WORKSPACE / "data" / "genre_review_input.csv"
PROOF_DIR = WORKSPACE / "_proof" / "genre_truth_hardening"

VALID_ACTIONS = {"approve", "replace", "add_secondary", "reject"}


class Pipeline:
    def __init__(self):
        self.log = []
        self.conflicts = []
        self.invalid_entries = []
        self.rows_updated = []
        self.rows_inserted = []
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
    # STAGE 1: Generate review queue
    # ================================================================
    def stage_review_queue(self):
        self.emit("\n=== STAGE 1: Generate review queue ===")
        conn = self.connect(readonly=True)

        # Priority 1: tracks with NO genre label at all
        q_no_label = """
            SELECT t.id AS track_id, t.artist, t.title,
                   NULL AS current_genre, NULL AS current_subgenre,
                   NULL AS source, NULL AS confidence,
                   1 AS priority, 'no_label' AS reason
            FROM tracks t
            LEFT JOIN track_genre_labels tgl ON t.id = tgl.track_id
            WHERE tgl.id IS NULL
        """

        # Priority 2: tracks with only system_import labels (bootstrap)
        q_import_only = """
            SELECT t.id AS track_id, t.artist, t.title,
                   g.name AS current_genre,
                   COALESCE(sg.name, '') AS current_subgenre,
                   tgl.source, tgl.confidence,
                   2 AS priority, 'import_only' AS reason
            FROM tracks t
            JOIN track_genre_labels tgl ON t.id = tgl.track_id AND tgl.role = 'primary'
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            WHERE tgl.applied_by LIKE 'system_import%'
              AND NOT EXISTS (
                  SELECT 1 FROM track_genre_labels tgl2
                  WHERE tgl2.track_id = t.id
                    AND tgl2.applied_by = 'review_pass'
              )
        """

        # Priority 3: tracks with ambiguous mappings (multiple labels same role)
        q_ambiguous = """
            SELECT t.id AS track_id, t.artist, t.title,
                   g.name AS current_genre,
                   COALESCE(sg.name, '') AS current_subgenre,
                   tgl.source, tgl.confidence,
                   3 AS priority, 'ambiguous' AS reason
            FROM tracks t
            JOIN track_genre_labels tgl ON t.id = tgl.track_id
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            WHERE t.id IN (
                SELECT track_id FROM track_genre_labels
                WHERE role = 'primary'
                GROUP BY track_id HAVING COUNT(*) > 1
            )
        """

        # Priority 4: tracks in benchmark sets
        q_benchmark = """
            SELECT t.id AS track_id, t.artist, t.title,
                   COALESCE(g.name, '') AS current_genre,
                   COALESCE(sg.name, '') AS current_subgenre,
                   tgl.source, tgl.confidence,
                   4 AS priority, 'benchmark' AS reason
            FROM benchmark_set_tracks bst
            JOIN tracks t ON bst.track_id = t.id
            LEFT JOIN track_genre_labels tgl ON t.id = tgl.track_id AND tgl.role = 'primary'
            LEFT JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            WHERE NOT EXISTS (
                SELECT 1 FROM track_genre_labels tgl2
                WHERE tgl2.track_id = t.id
                  AND tgl2.applied_by = 'review_pass'
            )
        """

        full_query = f"""
            SELECT * FROM (
                {q_no_label}
                UNION ALL
                {q_import_only}
                UNION ALL
                {q_ambiguous}
                UNION ALL
                {q_benchmark}
            )
            ORDER BY priority, track_id
        """

        rows = conn.execute(full_query).fetchall()
        conn.close()

        # Deduplicate by track_id (keep highest priority = lowest number)
        seen = set()
        deduped = []
        for r in rows:
            if r["track_id"] not in seen:
                seen.add(r["track_id"])
                deduped.append(r)

        REVIEW_QUEUE_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(REVIEW_QUEUE_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "current_genre",
                "current_subgenre", "source", "confidence",
                "priority", "reason"
            ])
            writer.writeheader()
            for r in deduped:
                writer.writerow({k: (r[k] if r[k] is not None else "") for k in writer.fieldnames})

        # Summary by reason
        reason_counts = {}
        for r in deduped:
            reason_counts[r["reason"]] = reason_counts.get(r["reason"], 0) + 1

        self.emit(f"Review queue: {len(deduped)} tracks")
        for reason, count in sorted(reason_counts.items()):
            self.emit(f"  {reason}: {count}")
        self.emit(f"Written to: {REVIEW_QUEUE_CSV}")

        return deduped, reason_counts

    # ================================================================
    # STAGE 2: Generate review input template
    # ================================================================
    def stage_review_template(self, queue_rows=None):
        self.emit("\n=== STAGE 2: Generate review input template ===")

        # If we have queue rows, pick examples from different categories
        examples = []
        if queue_rows:
            # Pick up to 5 examples from the queue
            by_reason = {}
            for r in queue_rows:
                by_reason.setdefault(r["reason"], []).append(r)

            for reason in ["no_label", "import_only", "ambiguous", "benchmark"]:
                candidates = by_reason.get(reason, [])
                for c in candidates[:2]:
                    action = "approve" if reason == "import_only" else "replace"
                    examples.append({
                        "track_id": c["track_id"],
                        "artist": c["artist"] or "",
                        "title": c["title"] or "",
                        "new_genre": c["current_genre"] or "Rock",
                        "new_subgenre": c["current_subgenre"] or "",
                        "action": action,
                        "notes": f"example_{reason}",
                    })
                    if len(examples) >= 5:
                        break
                if len(examples) >= 5:
                    break

        # If no queue rows, make generic examples
        if not examples:
            examples = [
                {"track_id": "1", "artist": "Artist Name", "title": "Song Title",
                 "new_genre": "Rock", "new_subgenre": "Classic Rock",
                 "action": "approve", "notes": "confirmed from ID3 tag"},
                {"track_id": "2", "artist": "Artist Name", "title": "Song Title",
                 "new_genre": "Hip-Hop", "new_subgenre": "Boom Bap",
                 "action": "replace", "notes": "was labeled Rock, corrected"},
                {"track_id": "3", "artist": "Artist Name", "title": "Song Title",
                 "new_genre": "Electronic", "new_subgenre": "House",
                 "action": "add_secondary", "notes": "also fits Electronic"},
                {"track_id": "4", "artist": "Artist Name", "title": "Song Title",
                 "new_genre": "", "new_subgenre": "",
                 "action": "reject", "notes": "genre tag was Comedy, not valid"},
            ]

        with open(REVIEW_INPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "new_genre",
                "new_subgenre", "action", "notes"
            ])
            writer.writeheader()
            for ex in examples:
                writer.writerow(ex)

        self.emit(f"Template written: {REVIEW_INPUT_CSV}")
        self.emit(f"  {len(examples)} example rows")
        self.emit("  EDIT THIS FILE, then run: python db/genre_truth_hardening.py ingest")

        return examples

    # ================================================================
    # STAGE 3: Ingest review decisions
    # ================================================================
    def stage_ingest(self):
        self.emit("\n=== STAGE 3: Ingest review decisions ===")

        if not REVIEW_INPUT_CSV.exists():
            self.emit(f"FATAL: Review input not found: {REVIEW_INPUT_CSV}")
            self.emit("Run 'python db/genre_truth_hardening.py template' first.")
            return False

        with open(REVIEW_INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
            input_rows = list(csv.DictReader(f))

        self.emit(f"Input rows: {len(input_rows)}")

        if len(input_rows) == 0:
            self.emit("FATAL: No rows in review input CSV.")
            return False

        conn = self.connect()
        cur = conn.cursor()

        # Load lookups
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

        self.emit(f"Lookups: {len(genres)} genres, {len(subgenres)} subgenres, {len(track_ids)} tracks")

        processed = 0
        skipped = 0
        seen_track_ids = set()

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
            new_genre = row.get("new_genre", "").strip()
            new_subgenre = row.get("new_subgenre", "").strip()
            notes = row.get("notes", "").strip()

            # ── validate track exists ──
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
                    f"track_id={track_id} (valid: {VALID_ACTIONS})"
                )
                skipped += 1
                continue

            # ── check for duplicate review entries ──
            if track_id in seen_track_ids and action in ("approve", "replace"):
                self.conflicts.append(
                    f"row={row_num} DUPLICATE_REVIEW track_id={track_id} "
                    f"action={action} (already processed in this batch)"
                )
                skipped += 1
                continue
            seen_track_ids.add(track_id)

            # ── resolve genre/subgenre IDs (needed for replace + add_secondary) ──
            genre_id = None
            subgenre_id = None

            if action in ("replace", "add_secondary"):
                if not new_genre:
                    self.invalid_entries.append(
                        f"row={row_num} MISSING_GENRE action={action} "
                        f"track_id={track_id}"
                    )
                    skipped += 1
                    continue

                genre_id = genres.get(new_genre.lower())
                if genre_id is None:
                    self.invalid_entries.append(
                        f"row={row_num} INVALID_GENRE genre='{new_genre}' "
                        f"track_id={track_id}"
                    )
                    skipped += 1
                    continue

                if new_subgenre:
                    subgenre_id = subgenres.get((new_subgenre.lower(), genre_id))
                    if subgenre_id is None:
                        self.invalid_entries.append(
                            f"row={row_num} INVALID_SUBGENRE "
                            f"subgenre='{new_subgenre}' genre='{new_genre}' "
                            f"track_id={track_id}"
                        )
                        skipped += 1
                        continue

            # ── get current primary label ──
            current_primary = cur.execute(
                "SELECT id, genre_id, subgenre_id, source, applied_by "
                "FROM track_genre_labels "
                "WHERE track_id = ? AND role = 'primary' "
                "ORDER BY id DESC LIMIT 1",
                (track_id,)
            ).fetchone()

            # ── APPROVE ──
            if action == "approve":
                if current_primary is None:
                    self.conflicts.append(
                        f"row={row_num} APPROVE_NO_PRIMARY track_id={track_id} "
                        f"(no primary label to approve)"
                    )
                    skipped += 1
                    continue

                cur.execute(
                    "UPDATE track_genre_labels "
                    "SET source = 'manual', "
                    "    applied_by = 'review_pass', "
                    "    created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
                    "WHERE id = ?",
                    (current_primary["id"],)
                )
                self.rows_updated.append(
                    f"track_id={track_id} label_id={current_primary['id']} "
                    f"action=approve applied_by=review_pass"
                )
                processed += 1

            # ── REPLACE ──
            elif action == "replace":
                # Downgrade existing primary to secondary
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

                # Insert new primary
                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, "
                        " confidence, applied_by) "
                        "VALUES (?, ?, ?, 'primary', 'manual', 1.0, ?)",
                        (track_id, genre_id, subgenre_id,
                         f"review_pass:{notes}" if notes else "review_pass")
                    )
                    self.rows_inserted.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=primary "
                        f"action=replace"
                    )
                    processed += 1
                except sqlite3.IntegrityError as e:
                    self.conflicts.append(
                        f"row={row_num} INTEGRITY_REPLACE track_id={track_id} "
                        f"genre={new_genre} err={e}"
                    )
                    skipped += 1

            # ── ADD_SECONDARY ──
            elif action == "add_secondary":
                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, "
                        " confidence, applied_by) "
                        "VALUES (?, ?, ?, 'secondary', 'manual', 0.7, ?)",
                        (track_id, genre_id, subgenre_id,
                         f"review_pass:{notes}" if notes else "review_pass")
                    )
                    self.rows_inserted.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=secondary "
                        f"action=add_secondary"
                    )
                    processed += 1
                except sqlite3.IntegrityError as e:
                    self.conflicts.append(
                        f"row={row_num} INTEGRITY_SECONDARY track_id={track_id} "
                        f"genre={new_genre} err={e}"
                    )
                    skipped += 1

            # ── REJECT ──
            elif action == "reject":
                if current_primary is None:
                    self.conflicts.append(
                        f"row={row_num} REJECT_NO_PRIMARY track_id={track_id} "
                        f"(no primary label to reject)"
                    )
                    skipped += 1
                    continue

                cur.execute(
                    "UPDATE track_genre_labels "
                    "SET source = 'rules', "
                    "    applied_by = ? "
                    "WHERE id = ?",
                    (f"rejected_review:{notes}" if notes else "rejected_review",
                     current_primary["id"])
                )
                self.rows_updated.append(
                    f"track_id={track_id} label_id={current_primary['id']} "
                    f"action=reject source->rules"
                )
                processed += 1

        conn.commit()

        self.emit(f"\nProcessed: {processed}")
        self.emit(f"Skipped: {skipped}")
        self.emit(f"Updates: {len(self.rows_updated)}")
        self.emit(f"Inserts: {len(self.rows_inserted)}")
        self.emit(f"Conflicts: {len(self.conflicts)}")
        self.emit(f"Invalid: {len(self.invalid_entries)}")

        # ── POST-INGEST VALIDATION ──
        self.emit("\n=== VALIDATION ===")
        vq_results = ["=== VALIDATION QUERIES ===", ""]

        validation_queries = [
            "SELECT COUNT(*) FROM track_genre_labels;",
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary';",
            ("SELECT track_id, COUNT(*) FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
            "SELECT COUNT(*) FROM track_genre_labels WHERE genre_id IS NULL;",
            ("SELECT COUNT(*) FROM track_genre_labels "
             "WHERE subgenre_id IS NOT NULL AND genre_id IS NULL;"),
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

        total = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels"
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
        orphan_sub = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels "
            "WHERE subgenre_id IS NOT NULL AND genre_id IS NULL"
        ).fetchone()[0]
        fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()

        conn.close()

        gate_ok = (
            processed > 0
            and dup_primaries == 0
            and null_genre == 0
            and orphan_sub == 0
            and len(fk_violations) == 0
        )
        gate = "PASS" if gate_ok else "FAIL"

        self.emit(f"\nGATE={gate}")

        return gate, {
            "processed": processed,
            "skipped": skipped,
            "updates": len(self.rows_updated),
            "inserts": len(self.rows_inserted),
            "conflicts": len(self.conflicts),
            "invalid": len(self.invalid_entries),
            "total_labels": total,
            "dup_primaries": dup_primaries,
            "null_genre_ids": null_genre,
            "orphan_subgenres": orphan_sub,
            "fk_violations": len(fk_violations),
        }, vq_results

    # ================================================================
    # PROOF FILES
    # ================================================================
    def write_proof(self, gate, summary, vq_results,
                    queue_summary=None, template_summary=None):
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        w("00_review_queue_summary.txt", "\n".join([
            "=== REVIEW QUEUE SUMMARY ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ] + ([
            f"Total tracks in queue: {sum(queue_summary.values())}",
            "",
        ] + [f"  {k}: {v}" for k, v in sorted(queue_summary.items())]
            if queue_summary else ["(queue not generated in this run)"]
        )))

        w("01_review_input_summary.txt", "\n".join([
            "=== REVIEW INPUT SUMMARY ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"File: {REVIEW_INPUT_CSV}",
            "",
        ] + ([f"Template rows: {template_summary}"]
             if template_summary else ["(template not generated in this run)"]
        )))

        w("02_rows_updated.txt", "\n".join(
            [f"=== ROWS UPDATED: {len(self.rows_updated)} ===", ""]
            + (self.rows_updated if self.rows_updated else ["(none)"])
        ))

        w("03_rows_inserted.txt", "\n".join(
            [f"=== ROWS INSERTED: {len(self.rows_inserted)} ===", ""]
            + (self.rows_inserted if self.rows_inserted else ["(none)"])
        ))

        w("04_conflicts_logged.txt", "\n".join(
            [f"=== CONFLICTS: {len(self.conflicts)} ===", ""]
            + (self.conflicts if self.conflicts else ["(none)"])
        ))

        w("05_invalid_entries.txt", "\n".join(
            [f"=== INVALID ENTRIES: {len(self.invalid_entries)} ===", ""]
            + (self.invalid_entries if self.invalid_entries else ["(none)"])
        ))

        w("06_validation_queries.txt",
          "\n".join(vq_results) if vq_results else "(no validation run)")

        w("07_final_report.txt", "\n".join([
            "=== GENRE TRUTH HARDENING - FINAL REPORT ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"GATE: {gate}",
            f"Elapsed: {elapsed}s",
            "",
        ] + [f"  {k:30s}: {v}" for k, v in summary.items()] + [
            "",
            "Integrity:",
            f"  Duplicate primary labels: {summary.get('dup_primaries', 'N/A')}",
            f"  Null genre_id rows:       {summary.get('null_genre_ids', 'N/A')}",
            f"  Orphan subgenres:         {summary.get('orphan_subgenres', 'N/A')}",
            f"  FK violations:            {summary.get('fk_violations', 'N/A')}",
        ]))

        w("execution_log.txt", "\n".join(self.log))

        self.emit(f"\nProof written to: {PROOF_DIR}")
        return PROOF_DIR


def main():
    if len(sys.argv) < 2:
        print("Usage: python db/genre_truth_hardening.py <queue|template|ingest|all>")
        return 1

    mode = sys.argv[1].lower()
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: DB not found: {ANALYSIS_DB}")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Mode: {mode}")

    queue_rows = None
    queue_summary = None
    template_count = None

    if mode in ("queue", "all"):
        queue_rows, queue_summary = p.stage_review_queue()

    if mode in ("template", "all"):
        # If queue wasn't generated this run, try to load from CSV
        if queue_rows is None and REVIEW_QUEUE_CSV.exists():
            with open(REVIEW_QUEUE_CSV, "r", encoding="utf-8-sig", newline="") as f:
                queue_rows = [dict(r) for r in csv.DictReader(f)]
        examples = p.stage_review_template(queue_rows)
        template_count = len(examples)

    if mode == "ingest":
        gate, summary, vq_results = p.stage_ingest()
        pf = p.write_proof(gate, summary, vq_results,
                           queue_summary, template_count)

        print(f"\n{'=' * 60}")
        for k, v in summary.items():
            print(f"  {k:30s}: {v}")
        print(f"{'=' * 60}")
        print(f"PF={pf}")
        print(f"GATE={gate}")
        return 0 if gate == "PASS" else 1

    if mode == "all":
        p.emit("\n=== STAGES 1+2 COMPLETE ===")
        p.emit(f"Review queue: {REVIEW_QUEUE_CSV}")
        p.emit(f"Review input: {REVIEW_INPUT_CSV}")
        p.emit("")
        p.emit("NEXT STEPS:")
        p.emit("1. Edit data/genre_review_input.csv with your review decisions")
        p.emit("2. Run: python db/genre_truth_hardening.py ingest")

        # Write partial proof (no ingest yet)
        partial_summary = {
            "status": "queue_and_template_generated",
            "queue_tracks": sum(queue_summary.values()) if queue_summary else 0,
            "template_rows": template_count or 0,
            "awaiting": "human review of genre_review_input.csv",
        }
        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        p.write_proof("PENDING", partial_summary, [],
                       queue_summary, template_count)

        print(f"\nPF={PROOF_DIR}")
        print("GATE=PENDING (awaiting human review)")
        return 0

    if mode == "queue":
        p.emit(f"\nQueue generated. Review at: {REVIEW_QUEUE_CSV}")
        return 0

    if mode == "template":
        p.emit(f"\nTemplate generated. Edit, then run ingest.")
        return 0

    print(f"Unknown mode: {mode}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
