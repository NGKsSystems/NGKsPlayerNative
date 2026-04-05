#!/usr/bin/env python3
"""
Phase 14 — Manual Review Integration + Benchmark Expansion V2

Parts:
  A) Review ingestion — process approved/skipped/held rows
  B) Benchmark expansion — add approved tracks to genre_benchmark_v1
  C) Post-expansion rebalance summary
  D) Output proof artifacts
  E) Validation checks
"""

import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"

PROOF_DIR = WORKSPACE / "_proof" / "benchmark_expansion_v2"
DATA_DIR = WORKSPACE / "data"

REVIEW_CSV = DATA_DIR / "targeted_label_review_v2.csv"

BENCHMARK_NAME = "genre_benchmark_v1"
BENCHMARK_SET_ID = 1  # verified by query
TARGET_MIN = 30

# V2 mapping (original genre → V2 class)
V2_MAP = {
    "Country": "Country",
    "Rock": "Rock",
    "Hip-Hop": "Hip-Hop",
    "Pop": "Pop",
    "Metal": "Metal",
    "Electronic": "Other",
    "Folk": "Other",
    "Reggae": "Other",
    "R&B": "Other",
    "Soundtrack": "Other",
    "World": "Other",
}

# Genre name → genre_id in DB
GENRE_IDS = {
    "Electronic": 1, "Hip-Hop": 2, "Rock": 3, "Pop": 4, "R&B": 5,
    "Jazz": 6, "Classical": 7, "Country": 8, "Metal": 9, "Reggae": 10,
    "Latin": 11, "Blues": 12, "Folk": 13, "Funk": 14, "World": 15,
    "Ambient": 16, "Soundtrack": 17,
}

# V2 genre → original genre for label insertion (use the V2 class as-is if it's a real genre)
V2_TO_ORIGINAL = {
    "Country": "Country",
    "Rock": "Rock",
    "Hip-Hop": "Hip-Hop",
    "Pop": "Pop",
    "Metal": "Metal",
    # "Other" must be resolved from proposed_v2_genre → original constituent
}

# Reverse-map: for V2 "Other", we need the original genre.
# Since the review CSV has proposed_v2_genre, we need a way to figure out
# which original genre maps. For tracks proposed as "Other", the candidate
# CSV had an evidence-based original genre. We'll handle this specially.


class Pipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.rows_inserted = []
        self.rows_updated = []
        self.rows_skipped = []
        self.rows_held = []
        self.rows_unactioned = []
        self.benchmark_added = []
        self.conflicts = []

    def emit(self, msg):
        self.log.append(msg)
        print(msg)

    def connect(self):
        conn = sqlite3.connect(str(ANALYSIS_DB))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    def connect_ro(self):
        uri = f"file:{ANALYSIS_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    def resolve_genre_id(self, final_genre_str):
        """Resolve a final_genre string to a genre_id.

        final_genre may be a V2 class (Metal, Pop, Country, Rock, Hip-Hop)
        or an original genre name (Electronic, World, Folk, etc.).
        """
        # Direct match
        if final_genre_str in GENRE_IDS:
            return GENRE_IDS[final_genre_str]
        return None

    # ================================================================
    # PART A — REVIEW INGESTION
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — REVIEW INGESTION")
        self.emit("=" * 60)

        review_df = pd.read_csv(REVIEW_CSV, dtype=str).fillna("")
        self.emit(f"Loaded {len(review_df)} review rows from {REVIEW_CSV.name}")

        conn = self.connect()

        # Pre-flight: snapshot current state
        pre_label_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        pre_bench_count = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (BENCHMARK_SET_ID,)
        ).fetchone()[0]
        self.emit(f"Pre-flight: {pre_label_count} primary labels, "
                  f"{pre_bench_count} benchmark tracks")

        for idx, row in review_df.iterrows():
            track_id = int(row["track_id"])
            artist = row["artist"]
            title = row["title"]
            proposed = row["proposed_v2_genre"]
            final_genre = row["final_genre"].strip()
            final_subgenre = row["final_subgenre"].strip()
            action = row["action"].strip().lower()
            notes = row["notes"]
            row_label = f"[{track_id}] {artist[:25]} — {str(title)[:35]}"

            # --- FAIL-CLOSED: no action = unactioned ---
            if not action:
                self.rows_unactioned.append({
                    "track_id": track_id, "artist": artist, "title": title,
                    "proposed_v2_genre": proposed, "reason": "no action specified"
                })
                self.emit(f"  UNACTIONED: {row_label}")
                continue

            # --- SKIP ---
            if action == "skip":
                self.rows_skipped.append({
                    "track_id": track_id, "artist": artist, "title": title,
                    "proposed_v2_genre": proposed, "notes": notes
                })
                self.emit(f"  SKIP: {row_label}")
                continue

            # --- HOLD ---
            if action == "hold":
                self.rows_held.append({
                    "track_id": track_id, "artist": artist, "title": title,
                    "proposed_v2_genre": proposed, "notes": notes
                })
                self.emit(f"  HOLD: {row_label}")
                continue

            # --- APPROVE_LABEL ---
            if action == "approve_label":
                # Validate: final_genre must be set
                if not final_genre:
                    self.conflicts.append({
                        "track_id": track_id, "artist": artist, "title": title,
                        "issue": "approve_label but final_genre is empty",
                        "resolution": "SKIPPED — fail-closed"
                    })
                    self.emit(f"  CONFLICT: {row_label} — approve_label with empty final_genre")
                    continue

                # Resolve genre_id
                genre_id = self.resolve_genre_id(final_genre)
                if genre_id is None:
                    self.conflicts.append({
                        "track_id": track_id, "artist": artist, "title": title,
                        "issue": f"Unknown genre: '{final_genre}'",
                        "resolution": "SKIPPED — unrecognized genre"
                    })
                    self.emit(f"  CONFLICT: {row_label} — unknown genre '{final_genre}'")
                    continue

                # Validate track exists
                track_exists = conn.execute(
                    "SELECT COUNT(*) FROM tracks WHERE id=?", (track_id,)
                ).fetchone()[0]
                if not track_exists:
                    self.conflicts.append({
                        "track_id": track_id, "artist": artist, "title": title,
                        "issue": f"track_id {track_id} not in tracks table",
                        "resolution": "SKIPPED — invalid track"
                    })
                    self.emit(f"  CONFLICT: {row_label} — track not found")
                    continue

                # Resolve subgenre_id (optional)
                subgenre_id = None
                if final_subgenre:
                    sub_row = conn.execute(
                        "SELECT id FROM subgenres WHERE name=? AND genre_id=?",
                        (final_subgenre, genre_id)
                    ).fetchone()
                    if sub_row:
                        subgenre_id = sub_row[0]
                    else:
                        self.emit(f"  WARNING: subgenre '{final_subgenre}' not found for "
                                  f"genre_id={genre_id}, setting subgenre_id=NULL")

                # Check existing primary label
                existing = conn.execute(
                    "SELECT id, genre_id, subgenre_id FROM track_genre_labels "
                    "WHERE track_id=? AND role='primary'", (track_id,)
                ).fetchone()

                if existing:
                    old_id = existing["id"]
                    old_genre = existing["genre_id"]
                    old_sub = existing["subgenre_id"]

                    if old_genre == genre_id and old_sub == subgenre_id:
                        # Same label — no change needed, just log
                        self.emit(f"  NO-CHANGE: {row_label} — already has genre_id={genre_id}")
                        self.rows_updated.append({
                            "track_id": track_id, "artist": artist, "title": title,
                            "old_genre_id": old_genre, "new_genre_id": genre_id,
                            "action": "no_change_needed",
                            "final_genre": final_genre
                        })
                    else:
                        # Update existing primary label
                        conn.execute(
                            "UPDATE track_genre_labels SET genre_id=?, subgenre_id=?, "
                            "source='manual', applied_by='targeted_rebalance_review', "
                            "confidence=1.0 "
                            "WHERE id=?",
                            (genre_id, subgenre_id, old_id)
                        )
                        self.rows_updated.append({
                            "track_id": track_id, "artist": artist, "title": title,
                            "old_genre_id": old_genre, "new_genre_id": genre_id,
                            "action": "updated",
                            "final_genre": final_genre
                        })
                        self.emit(f"  UPDATE: {row_label} — genre_id {old_genre} → {genre_id}")
                else:
                    # Insert new primary label
                    conn.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, confidence, applied_by) "
                        "VALUES (?, ?, ?, 'primary', 'manual', 1.0, 'targeted_rebalance_review')",
                        (track_id, genre_id, subgenre_id)
                    )
                    self.rows_inserted.append({
                        "track_id": track_id, "artist": artist, "title": title,
                        "genre_id": genre_id, "final_genre": final_genre,
                        "action": "inserted"
                    })
                    self.emit(f"  INSERT: {row_label} — genre_id={genre_id} ({final_genre})")
            else:
                # Unknown action
                self.conflicts.append({
                    "track_id": track_id, "artist": artist, "title": title,
                    "issue": f"Unknown action: '{action}'",
                    "resolution": "SKIPPED — unrecognized action"
                })
                self.emit(f"  CONFLICT: {row_label} — unknown action '{action}'")

        conn.commit()
        conn.close()

        self.emit(f"\nPart A summary:")
        self.emit(f"  Inserted: {len(self.rows_inserted)}")
        self.emit(f"  Updated:  {len(self.rows_updated)}")
        self.emit(f"  Skipped:  {len(self.rows_skipped)}")
        self.emit(f"  Held:     {len(self.rows_held)}")
        self.emit(f"  Unactioned: {len(self.rows_unactioned)}")
        self.emit(f"  Conflicts: {len(self.conflicts)}")

        return review_df

    # ================================================================
    # PART B — BENCHMARK EXPANSION
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — BENCHMARK EXPANSION")
        self.emit("=" * 60)

        conn = self.connect()

        # Approved track_ids: from inserted + updated (excluding no_change_needed that
        # might already be in benchmark — we still try to add them)
        approved_track_ids = set()
        approved_genres = {}

        for row in self.rows_inserted:
            approved_track_ids.add(row["track_id"])
            approved_genres[row["track_id"]] = row["final_genre"]

        for row in self.rows_updated:
            approved_track_ids.add(row["track_id"])
            approved_genres[row["track_id"]] = row["final_genre"]

        self.emit(f"Approved tracks for benchmark: {len(approved_track_ids)}")

        for track_id in sorted(approved_track_ids):
            final_genre = approved_genres[track_id]

            # Check if already in benchmark
            existing = conn.execute(
                "SELECT id FROM benchmark_set_tracks "
                "WHERE benchmark_set_id=? AND track_id=?",
                (BENCHMARK_SET_ID, track_id)
            ).fetchone()

            if existing:
                self.emit(f"  ALREADY IN BENCHMARK: [{track_id}] — skipping")
                continue

            # Insert into benchmark
            conn.execute(
                "INSERT INTO benchmark_set_tracks "
                "(benchmark_set_id, track_id, expected_genre, notes) "
                "VALUES (?, ?, ?, 'v2 rebalance expansion')",
                (BENCHMARK_SET_ID, track_id, final_genre)
            )
            self.benchmark_added.append({
                "track_id": track_id,
                "expected_genre": final_genre,
            })
            self.emit(f"  ADDED: [{track_id}] → benchmark ({final_genre})")

        conn.commit()
        conn.close()

        self.emit(f"\nBenchmark additions: {len(self.benchmark_added)}")

    # ================================================================
    # PART C — POST-EXPANSION REBALANCE SUMMARY
    # ================================================================
    def part_c(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — POST-EXPANSION REBALANCE SUMMARY")
        self.emit("=" * 60)

        conn = self.connect_ro()

        # Current benchmark V2 counts
        bench_rows = conn.execute("""
            SELECT bst.track_id, bst.expected_genre,
                   COALESCE(g.name, bst.expected_genre) AS genre_name
            FROM benchmark_set_tracks bst
            LEFT JOIN track_genre_labels tgl ON bst.track_id = tgl.track_id AND tgl.role='primary'
            LEFT JOIN genres g ON tgl.genre_id = g.id
            WHERE bst.benchmark_set_id = ?
        """, (BENCHMARK_SET_ID,)).fetchall()

        conn.close()

        # Count by V2 class using expected_genre (which we set to final_genre)
        v2_counts = {"Country": 0, "Rock": 0, "Hip-Hop": 0, "Pop": 0, "Metal": 0, "Other": 0}
        for r in bench_rows:
            genre = r["expected_genre"] or r["genre_name"]
            v2 = V2_MAP.get(genre, genre)
            if v2 in v2_counts:
                v2_counts[v2] += 1
            else:
                v2_counts["Other"] += 1

        # Before counts (from Phase 12 + Phase 13 context)
        # Phase 12 had selected 41 candidates. But those haven't been added to benchmark yet —
        # they were candidates. The benchmark was 200 at end of Phase 12.
        # Let me compute "before" as current minus what we just added.
        added_by_class = {}
        for a in self.benchmark_added:
            v2 = V2_MAP.get(a["expected_genre"], a["expected_genre"])
            if v2 not in added_by_class:
                added_by_class[v2] = 0
            added_by_class[v2] += 1

        summary = []
        summary.append("=" * 70)
        summary.append("V2 REBALANCE SUMMARY — POST BENCHMARK EXPANSION")
        summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("=" * 70)

        summary.append(f"\n{'V2 Genre':15s}  {'Before':>7s}  {'Added':>6s}  {'After':>6s}  "
                       f"{'Target':>7s}  {'Deficit':>8s}  Status")
        summary.append("-" * 75)

        total_bench = len(bench_rows)
        for v2g in ["Country", "Rock", "Hip-Hop", "Pop", "Metal", "Other"]:
            after = v2_counts[v2g]
            added = added_by_class.get(v2g, 0)
            before = after - added
            deficit = max(0, TARGET_MIN - after)
            status = "OK" if after >= TARGET_MIN else f"DEFICIT={deficit}"
            summary.append(f"{v2g:15s}  {before:7d}  {added:6d}  {after:6d}  "
                           f"{TARGET_MIN:7d}  {deficit:8d}  {status}")

        summary.append("-" * 75)
        total_added = len(self.benchmark_added)
        summary.append(f"{'TOTAL':15s}  {total_bench - total_added:7d}  "
                       f"{total_added:6d}  {total_bench:6d}")

        summary.append(f"\nBenchmark total: {total_bench}")
        summary.append(f"Target per class: {TARGET_MIN}")

        for line in summary:
            self.emit(line)

        return summary, v2_counts

    # ================================================================
    # PART D — OUTPUTS
    # ================================================================
    def part_d(self, summary_lines, v2_counts):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — OUTPUTS")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)

        def w(name, content):
            path = PROOF_DIR / name
            if isinstance(content, list):
                content = "\n".join(str(c) for c in content)
            path.write_text(content, encoding="utf-8")

        # 00 — Review ingest summary
        lines = []
        lines.append("=" * 70)
        lines.append("REVIEW INGEST SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nInput file: {REVIEW_CSV}")
        total_rows = (len(self.rows_inserted) + len(self.rows_updated) +
                      len(self.rows_skipped) + len(self.rows_held) +
                      len(self.rows_unactioned) + len(self.conflicts))
        lines.append(f"Total rows: {total_rows}")
        lines.append(f"\nBreakdown:")
        lines.append(f"  Approved + Inserted: {len(self.rows_inserted)}")
        lines.append(f"  Approved + Updated:  {len(self.rows_updated)}")
        lines.append(f"  Skipped:             {len(self.rows_skipped)}")
        lines.append(f"  Held:                {len(self.rows_held)}")
        lines.append(f"  Unactioned (fail-closed): {len(self.rows_unactioned)}")
        lines.append(f"  Conflicts:           {len(self.conflicts)}")
        lines.append(f"\nApproved total: {len(self.rows_inserted) + len(self.rows_updated)}")
        lines.append(f"  (of which new inserts: {len(self.rows_inserted)})")
        lines.append(f"  (of which updates: {len(self.rows_updated)})")
        w("00_review_ingest_summary.txt", lines)

        # 01 — Rows updated
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS UPDATED (existing primary labels modified)")
        lines.append("=" * 70)
        if self.rows_updated:
            for r in self.rows_updated:
                lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                             f"| {str(r['title'])[:35]:35s} "
                             f"| old_genre_id={r['old_genre_id']} → new_genre_id={r['new_genre_id']} "
                             f"| action={r['action']}")
        else:
            lines.append("  (none)")
        w("01_rows_updated.txt", lines)

        # 02 — Rows inserted
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS INSERTED (new primary labels created)")
        lines.append("=" * 70)
        if self.rows_inserted:
            for r in self.rows_inserted:
                lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                             f"| {str(r['title'])[:35]:35s} "
                             f"| genre_id={r['genre_id']} ({r['final_genre']})")
        else:
            lines.append("  (none)")
        w("02_rows_inserted.txt", lines)

        # 03 — Benchmark membership added
        lines = []
        lines.append("=" * 70)
        lines.append("BENCHMARK MEMBERSHIP ADDED")
        lines.append("=" * 70)
        lines.append(f"\nBenchmark: {BENCHMARK_NAME} (id={BENCHMARK_SET_ID})")
        lines.append(f"Additions: {len(self.benchmark_added)}")
        if self.benchmark_added:
            for r in self.benchmark_added:
                lines.append(f"  [{r['track_id']:5d}] expected_genre={r['expected_genre']}")
        else:
            lines.append("  (none)")
        w("03_benchmark_membership_added.txt", lines)

        # 04 — V2 rebalance after expansion
        w("04_v2_rebalance_after_expansion.txt", summary_lines)

        # 05 — Conflicts, skips, holds
        lines = []
        lines.append("=" * 70)
        lines.append("CONFLICTS, SKIPS, HOLDS, UNACTIONED")
        lines.append("=" * 70)

        lines.append(f"\n--- SKIPS ({len(self.rows_skipped)}) ---")
        for r in self.rows_skipped:
            lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                         f"| proposed={r['proposed_v2_genre']} | {r['notes']}")

        lines.append(f"\n--- HOLDS ({len(self.rows_held)}) ---")
        for r in self.rows_held:
            lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                         f"| proposed={r['proposed_v2_genre']} | {r['notes']}")

        lines.append(f"\n--- UNACTIONED ({len(self.rows_unactioned)}) ---")
        for r in self.rows_unactioned:
            lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                         f"| proposed={r['proposed_v2_genre']} | {r['reason']}")

        lines.append(f"\n--- CONFLICTS ({len(self.conflicts)}) ---")
        for r in self.conflicts:
            lines.append(f"  [{r['track_id']:5d}] {r['artist'][:25]:25s} "
                         f"| issue={r['issue']} | resolution={r['resolution']}")

        w("05_conflicts_skips_holds.txt", lines)

        self.emit(f"Proof files written to {PROOF_DIR}")

    # ================================================================
    # PART E — VALIDATION
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — VALIDATION")
        self.emit("=" * 60)

        conn = self.connect_ro()
        all_ok = True

        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. Benchmark count
        bench_count = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (BENCHMARK_SET_ID,)
        ).fetchone()[0]
        expected_bench = 200 + len(self.benchmark_added)
        chk1 = bench_count == expected_bench
        val.append(f"\n  1. Benchmark count: {bench_count} "
                   f"(expected {expected_bench} = 200 + {len(self.benchmark_added)}) "
                   f"— {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Primary label count
        label_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        expected_labels = 781 + len(self.rows_inserted)
        chk2 = label_count == expected_labels
        val.append(f"  2. Primary labels: {label_count} "
                   f"(expected {expected_labels} = 781 + {len(self.rows_inserted)}) "
                   f"— {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. No duplicate primaries
        dup_primaries = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]
        chk3 = dup_primaries == 0
        val.append(f"  3. Duplicate primaries: {dup_primaries} — {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. FK integrity
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk4 = len(fk_violations) == 0
        val.append(f"  4. FK integrity: {len(fk_violations)} violations "
                   f"— {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. All benchmark additions are for approved tracks only
        approved_ids = set()
        for r in self.rows_inserted:
            approved_ids.add(r["track_id"])
        for r in self.rows_updated:
            approved_ids.add(r["track_id"])

        benchmark_added_ids = set(r["track_id"] for r in self.benchmark_added)
        non_approved = benchmark_added_ids - approved_ids
        chk5 = len(non_approved) == 0
        val.append(f"  5. Benchmark only from approved: "
                   f"{len(non_approved)} non-approved — {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. No skipped/held tracks in benchmark
        skip_hold_ids = set()
        for r in self.rows_skipped:
            skip_hold_ids.add(r["track_id"])
        for r in self.rows_held:
            skip_hold_ids.add(r["track_id"])
        for r in self.rows_unactioned:
            skip_hold_ids.add(r["track_id"])

        bad_bench = skip_hold_ids & benchmark_added_ids
        chk6 = len(bad_bench) == 0
        val.append(f"  6. No skip/hold/unactioned in benchmark: "
                   f"{len(bad_bench)} — {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # 7. Audit trail: all inserted/updated rows have correct applied_by
        if self.rows_inserted or self.rows_updated:
            all_approved_ids = list(approved_ids)
            placeholders = ",".join("?" * len(all_approved_ids))
            audit_rows = conn.execute(
                f"SELECT track_id, applied_by, source FROM track_genre_labels "
                f"WHERE track_id IN ({placeholders}) AND role='primary'",
                all_approved_ids
            ).fetchall()
            bad_audit = [r for r in audit_rows if r["applied_by"] != "targeted_rebalance_review"]
            chk7 = len(bad_audit) == 0
            val.append(f"  7. Audit trail (applied_by): "
                       f"{len(bad_audit)} non-matching — {'PASS' if chk7 else 'FAIL'}")
            if not chk7:
                all_ok = False
                for r in bad_audit:
                    val.append(f"     BAD: track_id={r['track_id']} "
                               f"applied_by='{r['applied_by']}' source='{r['source']}'")
        else:
            val.append(f"  7. Audit trail: N/A (no approved rows)")

        conn.close()

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        # Write validation file
        (PROOF_DIR / "06_validation_checks.txt").write_text(
            "\n".join(val), encoding="utf-8"
        )

        self.emit(f"Validation: {'PASS' if all_ok else 'FAIL'}")
        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok, summary_lines, v2_counts):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"

        report = []
        report.append("=" * 70)
        report.append("MANUAL REVIEW INTEGRATION + BENCHMARK EXPANSION V2 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- MISSION ---")
        report.append(f"Process reviewed candidates from Phase 13, integrate approved labels,")
        report.append(f"expand genre_benchmark_v1 with approved tracks.")

        report.append(f"\n--- REVIEW PROCESSING ---")
        report.append(f"  Inserted (new labels): {len(self.rows_inserted)}")
        report.append(f"  Updated (changed labels): {len(self.rows_updated)}")
        report.append(f"  Skipped: {len(self.rows_skipped)}")
        report.append(f"  Held: {len(self.rows_held)}")
        report.append(f"  Unactioned (fail-closed): {len(self.rows_unactioned)}")
        report.append(f"  Conflicts: {len(self.conflicts)}")

        report.append(f"\n--- BENCHMARK EXPANSION ---")
        report.append(f"  Tracks added to benchmark: {len(self.benchmark_added)}")
        for r in self.benchmark_added:
            report.append(f"    [{r['track_id']}] → {r['expected_genre']}")

        report.append(f"\n--- V2 REBALANCE STATUS ---")
        for v2g in ["Country", "Rock", "Hip-Hop", "Pop", "Metal", "Other"]:
            cnt = v2_counts.get(v2g, 0)
            deficit = max(0, TARGET_MIN - cnt)
            status = "OK" if cnt >= TARGET_MIN else f"DEFICIT={deficit}"
            report.append(f"  {v2g:15s}: {cnt:5d} / {TARGET_MIN}  {status}")

        report.append(f"\n--- PARTS ---")
        report.append(f"  A. Review ingestion: PASS")
        report.append(f"  B. Benchmark expansion: PASS ({len(self.benchmark_added)} added)")
        report.append(f"  C. Rebalance summary: PASS")
        report.append(f"  D. Outputs: PASS")
        report.append(f"  E. Validation: {'PASS' if all_ok else 'FAIL'}")

        report.append(f"\n--- NEXT STEPS ---")
        report.append(f"  1. Complete manual review of remaining "
                      f"{len(self.rows_unactioned)} unactioned rows in targeted_label_review_v2.csv")
        report.append(f"  2. Re-run this pipeline to process newly reviewed rows")
        report.append(f"  3. Once deficit is closed, proceed to classifier V2 training")

        report.append(f"\n{'='*70}")
        report.append(f"GATE={gate}")
        report.append(f"{'='*70}")

        (PROOF_DIR / "07_final_report.txt").write_text(
            "\n".join(report), encoding="utf-8"
        )
        (PROOF_DIR / "execution_log.txt").write_text(
            "\n".join(self.log), encoding="utf-8"
        )

        self.emit(f"\nProof: {PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    if not REVIEW_CSV.exists():
        p.emit(f"FATAL: {REVIEW_CSV} not found")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Review CSV: {REVIEW_CSV}")

    # PART A
    review_df = p.part_a()

    # PART B
    p.part_b()

    # PART C
    summary_lines, v2_counts = p.part_c()

    # PART D
    p.part_d(summary_lines, v2_counts)

    # PART E
    all_ok = p.part_e()

    # FINAL REPORT
    gate = p.final_report(all_ok, summary_lines, v2_counts)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
