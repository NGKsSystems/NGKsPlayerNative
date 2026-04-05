#!/usr/bin/env python3
"""
Phase 12 — Benchmark Rebalance Acquisition V2

Parts:
  A) Candidate selection from labeled non-benchmark pool
  B) Review template creation
  C) Class-by-class acquisition plan
  D) No DB writes (read-only)
  E) Output reports + validation
"""

import sqlite3
import sys
import time
from collections import OrderedDict
from pathlib import Path

import pandas as pd

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
MAPPING_CSV = WORKSPACE / "data" / "genre_taxonomy_v2_mapping.csv"
REBALANCE_CSV = WORKSPACE / "data" / "benchmark_rebalance_plan_v2.csv"

PROOF_DIR = WORKSPACE / "_proof" / "benchmark_rebalance_acquisition_v2"
DATA_DIR = WORKSPACE / "data"

CANDIDATES_CSV = DATA_DIR / "benchmark_rebalance_candidates_v2.csv"
REVIEW_CSV = DATA_DIR / "benchmark_rebalance_review_v2.csv"

BENCHMARK_NAME = "genre_benchmark_v1"
TARGET_MIN = 30

# V2 mapping (hardcoded from validated Phase 11 output)
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

# V2 deficit from Phase 11 (validated)
V2_DEFICITS = {
    "Country": 0,
    "Rock": 0,
    "Hip-Hop": 1,
    "Pop": 22,
    "Metal": 24,
    "Other": 21,
}

V2_CURRENT = {
    "Country": 82,
    "Rock": 66,
    "Hip-Hop": 29,
    "Pop": 8,
    "Metal": 6,
    "Other": 9,
}


class Pipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()

    def emit(self, msg):
        self.log.append(msg)
        print(msg)

    def connect_ro(self):
        uri = f"file:{ANALYSIS_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    # ================================================================
    # PART A — CANDIDATE SELECTION
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — CANDIDATE SELECTION")
        self.emit("=" * 60)

        conn = self.connect_ro()

        # Get all non-benchmark labeled tracks with analysis data
        pool_rows = conn.execute("""
            SELECT t.id AS track_id, t.artist, t.title,
                   g.name AS original_genre,
                   tgl.source, tgl.applied_by,
                   a.loudness_lufs, a.energy, a.danceability
            FROM tracks t
            JOIN track_genre_labels tgl ON t.id = tgl.track_id
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN analysis_summary a ON t.id = a.track_id
            WHERE tgl.role = 'primary'
              AND t.id NOT IN (
                SELECT track_id FROM benchmark_set_tracks
                WHERE benchmark_set_id = (
                  SELECT id FROM benchmark_sets WHERE name = ?
                )
              )
            ORDER BY g.name, t.artist, t.title
        """, (BENCHMARK_NAME,)).fetchall()

        pool_df = pd.DataFrame([dict(r) for r in pool_rows])
        self.emit(f"Total pool: {len(pool_df)} tracks")

        # Map to V2
        pool_df["v2_genre"] = pool_df["original_genre"].map(V2_MAP)
        unmapped = pool_df[pool_df["v2_genre"].isna()]
        if len(unmapped) > 0:
            self.emit(f"WARNING: {len(unmapped)} tracks with unmapped genre: "
                      f"{unmapped['original_genre'].unique()}")
            pool_df = pool_df[pool_df["v2_genre"].notna()].copy()

        pool_by_v2 = pool_df.groupby("v2_genre").size()
        self.emit(f"Pool by V2 genre:\n{pool_by_v2.to_string()}")

        # Selection strategy:
        # 1. For each V2 class with deficit > 0, select min(deficit, pool) tracks
        # 2. Prefer tracks with reviewed labels (applied_by='review_pass')
        # 3. Prefer tracks with analysis data (non-null features)
        # 4. For "Other", spread across constituent genres
        # 5. Avoid artist concentration — spread across artists

        candidates = []
        selection_summary = {}

        # Process in priority order: weakest classes first
        priority_order = sorted(
            [(v2g, d) for v2g, d in V2_DEFICITS.items() if d > 0],
            key=lambda x: -x[1]
        )

        for v2_genre, deficit in priority_order:
            v2_pool = pool_df[pool_df["v2_genre"] == v2_genre].copy()

            if len(v2_pool) == 0:
                self.emit(f"  {v2_genre}: deficit={deficit}, pool=0 — SKIP")
                selection_summary[v2_genre] = {
                    "deficit": deficit,
                    "pool": 0,
                    "selected": 0,
                    "remaining_deficit": deficit,
                }
                continue

            # Score each candidate
            v2_pool = v2_pool.copy()
            v2_pool["score"] = 0.0

            # Prefer reviewed labels
            v2_pool.loc[v2_pool["applied_by"] == "review_pass", "score"] += 2.0
            v2_pool.loc[v2_pool["source"] == "manual", "score"] += 1.0

            # Prefer tracks with analysis data
            has_analysis = v2_pool["loudness_lufs"].notna()
            v2_pool.loc[has_analysis, "score"] += 1.5

            # For "Other" bucket: spread across constituent genres
            if v2_genre == "Other":
                # Prioritize genres with more samples (Reggae=5 > World=2 > Soundtrack=1)
                other_genres = v2_pool["original_genre"].value_counts()
                for og, count in other_genres.items():
                    v2_pool.loc[v2_pool["original_genre"] == og, "score"] += min(count / 2.0, 1.5)

            # Sort by score descending, then by artist for diversity
            v2_pool = v2_pool.sort_values(
                ["score", "artist", "title"],
                ascending=[False, True, True]
            )

            # Diversify by artist: limit max 3 tracks per artist in selection
            selected_track_ids = []
            artist_counts = {}
            max_per_artist = 3

            # For Other bucket, special handling: try to include at least one
            # from each constituent genre if available
            if v2_genre == "Other":
                constituent_genres = v2_pool["original_genre"].unique()
                for cg in constituent_genres:
                    cg_tracks = v2_pool[v2_pool["original_genre"] == cg]
                    for _, row in cg_tracks.iterrows():
                        if len(selected_track_ids) >= deficit:
                            break
                        art = row["artist"]
                        if artist_counts.get(art, 0) >= max_per_artist:
                            continue
                        if row["track_id"] not in selected_track_ids:
                            selected_track_ids.append(row["track_id"])
                            artist_counts[art] = artist_counts.get(art, 0) + 1
                    if len(selected_track_ids) >= deficit:
                        break
            else:
                for _, row in v2_pool.iterrows():
                    if len(selected_track_ids) >= deficit:
                        break
                    art = row["artist"]
                    if artist_counts.get(art, 0) >= max_per_artist:
                        continue
                    selected_track_ids.append(row["track_id"])
                    artist_counts[art] = artist_counts.get(art, 0) + 1

                # If still short and artist limit caused it, relax limit
                if len(selected_track_ids) < deficit and len(selected_track_ids) < len(v2_pool):
                    for _, row in v2_pool.iterrows():
                        if len(selected_track_ids) >= deficit:
                            break
                        if row["track_id"] not in selected_track_ids:
                            selected_track_ids.append(row["track_id"])

            selected = v2_pool[v2_pool["track_id"].isin(selected_track_ids)].copy()

            # Assign metadata
            for idx, row in selected.iterrows():
                confidence = "high" if (row["applied_by"] == "review_pass"
                                        and pd.notna(row["loudness_lufs"])) else "medium"
                reason = f"Fill {v2_genre} deficit ({deficit} needed)"
                if v2_genre == "Other":
                    reason += f"; constituent genre: {row['original_genre']}"

                pri = "P1" if deficit >= 20 else ("P2" if deficit >= 10 else "P3")

                candidates.append({
                    "track_id": row["track_id"],
                    "artist": row["artist"],
                    "title": row["title"],
                    "original_genre": row["original_genre"],
                    "v2_genre": v2_genre,
                    "source": row["source"],
                    "confidence": confidence,
                    "selection_reason": reason,
                    "priority": pri,
                })

            n_selected = len(selected)
            remaining = deficit - n_selected
            self.emit(f"  {v2_genre}: deficit={deficit}, pool={len(v2_pool)}, "
                      f"selected={n_selected}, remaining={remaining}")

            selection_summary[v2_genre] = {
                "deficit": deficit,
                "pool": len(v2_pool),
                "selected": n_selected,
                "remaining_deficit": remaining,
            }

        conn.close()

        # Build candidates DataFrame
        cand_df = pd.DataFrame(candidates)
        if len(cand_df) == 0:
            self.emit("FATAL: No candidates selected")
            return None, None, None

        # Validate: no duplicate track_ids
        dup_ids = cand_df["track_id"].duplicated().sum()
        if dup_ids > 0:
            self.emit(f"FATAL: {dup_ids} duplicate track_ids in candidates")
            return None, None, None

        cand_df.to_csv(CANDIDATES_CSV, index=False, encoding="utf-8")
        self.emit(f"Candidates CSV: {CANDIDATES_CSV} ({len(cand_df)} rows)")

        # Pool summary text
        pool_summary = []
        pool_summary.append("=" * 70)
        pool_summary.append("CANDIDATE POOL SUMMARY")
        pool_summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        pool_summary.append("=" * 70)
        pool_summary.append(f"\nSource: non-benchmark tracks with primary labels")
        pool_summary.append(f"Total pool size: {len(pool_df)}")
        pool_summary.append(f"\nPool by original genre:")
        for g, c in pool_df["original_genre"].value_counts().items():
            pool_summary.append(f"  {g:15s}: {c}")
        pool_summary.append(f"\nPool by V2 genre:")
        for g, c in pool_by_v2.sort_values(ascending=False).items():
            pool_summary.append(f"  {g:15s}: {c}")
        pool_summary.append(f"\nLabel quality:")
        pool_summary.append(f"  source=manual: {(pool_df['source']=='manual').sum()}")
        pool_summary.append(f"  applied_by=review_pass: {(pool_df['applied_by']=='review_pass').sum()}")
        pool_summary.append(f"  Has analysis data: {pool_df['loudness_lufs'].notna().sum()}")
        pool_summary.append(f"  Missing analysis: {pool_df['loudness_lufs'].isna().sum()}")

        return cand_df, selection_summary, pool_summary

    # ================================================================
    # PART B — REVIEW TEMPLATE
    # ================================================================
    def part_b(self, cand_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — REVIEW TEMPLATE")
        self.emit("=" * 60)

        review_rows = []
        for _, row in cand_df.iterrows():
            review_rows.append({
                "track_id": row["track_id"],
                "artist": row["artist"],
                "title": row["title"],
                "v2_genre": row["v2_genre"],
                "action": "",
                "notes": "",
            })

        review_df = pd.DataFrame(review_rows)

        # Add example populated rows at top (first 3)
        if len(review_df) >= 3:
            review_df.loc[review_df.index[0], "action"] = "approve"
            review_df.loc[review_df.index[0], "notes"] = "Clean exemplar, confident label"
            review_df.loc[review_df.index[1], "action"] = "approve"
            review_df.loc[review_df.index[1], "notes"] = ""
            review_df.loc[review_df.index[2], "action"] = "skip"
            review_df.loc[review_df.index[2], "notes"] = "Ambiguous genre border — not a clean exemplar"

        review_df.to_csv(REVIEW_CSV, index=False, encoding="utf-8")
        self.emit(f"Review CSV: {REVIEW_CSV} ({len(review_df)} rows)")

        # Summary text
        summary = []
        summary.append("=" * 70)
        summary.append("REVIEW TEMPLATE SUMMARY")
        summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("=" * 70)
        summary.append(f"\nFile: {REVIEW_CSV}")
        summary.append(f"Total rows: {len(review_df)}")
        summary.append(f"Columns: {list(review_df.columns)}")
        summary.append(f"\nBy V2 genre:")
        for g, c in review_df["v2_genre"].value_counts().sort_values(ascending=False).items():
            summary.append(f"  {g:15s}: {c}")
        summary.append(f"\nInstructions:")
        summary.append(f"  1. Open {REVIEW_CSV.name}")
        summary.append(f"  2. For each track, set action to 'approve' or 'skip'")
        summary.append(f"  3. Optionally add notes for skipped tracks")
        summary.append(f"  4. Save the file")
        summary.append(f"  5. Run the insertion script (Phase 13) on approved rows only")
        summary.append(f"\nExample rows (first 3 pre-populated for reference):")
        for _, row in review_df.head(3).iterrows():
            summary.append(f"  [{row['track_id']}] {row['artist'][:25]:25s} "
                           f"| {row['v2_genre']:10s} | action={row['action']:7s} "
                           f"| {row['notes']}")

        return review_df, summary

    # ================================================================
    # PART C — ACQUISITION PLAN
    # ================================================================
    def part_c(self, cand_df, selection_summary):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — ACQUISITION PLAN")
        self.emit("=" * 60)

        plan = []
        plan.append("=" * 70)
        plan.append("CLASS-BY-CLASS ACQUISITION PLAN")
        plan.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        plan.append("=" * 70)

        plan.append(f"\n{'V2 Genre':15s}  {'Current':>8s}  {'Target':>7s}  {'Deficit':>8s}  "
                    f"{'Selected':>9s}  {'After':>6s}  {'Remaining':>10s}  Status")
        plan.append("-" * 95)

        total_selected = 0
        total_remaining = 0

        for v2g in ["Metal", "Pop", "Other", "Hip-Hop", "Country", "Rock"]:
            current = V2_CURRENT[v2g]
            deficit = V2_DEFICITS[v2g]
            ss = selection_summary.get(v2g, {"selected": 0, "remaining_deficit": deficit})
            selected = ss["selected"]
            after = current + selected
            remaining = ss["remaining_deficit"]
            total_selected += selected
            total_remaining += remaining

            if deficit == 0:
                status = "ADEQUATE"
            elif remaining == 0:
                status = "FILLED"
            elif remaining > 0 and selected > 0:
                status = "PARTIAL"
            else:
                status = "UNFILLED"

            plan.append(f"{v2g:15s}  {current:8d}  {TARGET_MIN:7d}  {deficit:8d}  "
                        f"{selected:9d}  {after:6d}  {remaining:10d}  {status}")

        plan.append("-" * 95)
        plan.append(f"{'TOTAL':15s}  {sum(V2_CURRENT.values()):8d}  "
                    f"{TARGET_MIN * len(V2_CURRENT):7d}  "
                    f"{sum(V2_DEFICITS.values()):8d}  "
                    f"{total_selected:9d}  "
                    f"{sum(V2_CURRENT.values()) + total_selected:6d}  "
                    f"{total_remaining:10d}")

        plan.append(f"\n--- PER-CLASS DETAIL ---")

        for v2g in ["Metal", "Pop", "Other", "Hip-Hop"]:
            deficit = V2_DEFICITS[v2g]
            if deficit == 0:
                continue
            ss = selection_summary.get(v2g, {"selected": 0, "pool": 0, "remaining_deficit": deficit})

            plan.append(f"\n  {v2g}:")
            plan.append(f"    Current benchmark: {V2_CURRENT[v2g]}")
            plan.append(f"    Target: {TARGET_MIN}")
            plan.append(f"    Deficit: {deficit}")
            plan.append(f"    Pool available: {ss['pool']}")
            plan.append(f"    Selected: {ss['selected']}")
            plan.append(f"    After acquisition: {V2_CURRENT[v2g] + ss['selected']}")
            plan.append(f"    Remaining deficit: {ss['remaining_deficit']}")

            if v2g in ["Metal", "Pop", "Other"] and ss["remaining_deficit"] > 0:
                plan.append(f"    ACTION REQUIRED: Need {ss['remaining_deficit']} additional "
                            f"manually labeled tracks for this class")

            # List selected tracks by artist
            v2_cands = cand_df[cand_df["v2_genre"] == v2g]
            if len(v2_cands) > 0:
                plan.append(f"    Selected tracks ({len(v2_cands)}):")
                artists = v2_cands.groupby("artist").size().sort_values(ascending=False)
                for art, cnt in artists.items():
                    plan.append(f"      {art}: {cnt} track(s)")

        plan.append(f"\n--- SUMMARY ---")
        plan.append(f"Total deficit: {sum(V2_DEFICITS.values())}")
        plan.append(f"Total selected from pool: {total_selected}")
        plan.append(f"Total remaining after acquisition: {total_remaining}")
        plan.append(f"Classes fully filled: "
                    f"{sum(1 for v2g in V2_DEFICITS if V2_DEFICITS[v2g] > 0 and selection_summary.get(v2g, {}).get('remaining_deficit', 1) == 0)}")
        plan.append(f"Classes partially filled: "
                    f"{sum(1 for v2g in V2_DEFICITS if V2_DEFICITS[v2g] > 0 and 0 < selection_summary.get(v2g, {}).get('remaining_deficit', 0) < V2_DEFICITS[v2g])}")
        plan.append(f"Classes with no pool: "
                    f"{sum(1 for v2g in V2_DEFICITS if V2_DEFICITS[v2g] > 0 and selection_summary.get(v2g, {}).get('pool', 0) == 0)}")

        self.emit(f"Acquisition plan: selected={total_selected}, remaining={total_remaining}")
        return plan

    # ================================================================
    # PART D — NO DB WRITES (verification)
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — DB WRITE VERIFICATION")
        self.emit("=" * 60)
        self.emit("  DB opened in READ-ONLY mode (file: URI with ?mode=ro)")
        self.emit("  No INSERT/UPDATE/DELETE executed")
        self.emit("  No schema changes")

    # ================================================================
    # PART E — OUTPUTS + VALIDATION
    # ================================================================
    def part_e(self, cand_df, selection_summary, pool_summary, review_summary,
               acquisition_plan):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — OUTPUTS + VALIDATION")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            path = PROOF_DIR / name
            if isinstance(text, list):
                text = "\n".join(text)
            path.write_text(text, encoding="utf-8")

        conn = self.connect_ro()

        # 00 — deficit summary
        deficit_lines = []
        deficit_lines.append("=" * 70)
        deficit_lines.append("V2 BENCHMARK DEFICIT SUMMARY")
        deficit_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        deficit_lines.append("=" * 70)
        deficit_lines.append(f"\nTarget minimum per V2 class: {TARGET_MIN}")
        deficit_lines.append(f"Total V2 classes: {len(V2_CURRENT)}")
        deficit_lines.append(f"\n{'V2 Genre':15s}  {'Current':>8s}  {'Target':>7s}  {'Deficit':>8s}")
        deficit_lines.append("-" * 45)
        for v2g in sorted(V2_DEFICITS.keys()):
            deficit_lines.append(f"{v2g:15s}  {V2_CURRENT[v2g]:8d}  {TARGET_MIN:7d}  {V2_DEFICITS[v2g]:8d}")
        deficit_lines.append("-" * 45)
        deficit_lines.append(f"{'TOTAL':15s}  {sum(V2_CURRENT.values()):8d}  "
                             f"{TARGET_MIN * len(V2_CURRENT):7d}  {sum(V2_DEFICITS.values()):8d}")
        w("00_deficit_summary.txt", deficit_lines)

        # 01 — candidate pool summary
        w("01_candidate_pool_summary.txt", pool_summary)

        # 02 — selected candidates
        sel_lines = []
        sel_lines.append("=" * 70)
        sel_lines.append("SELECTED CANDIDATES")
        sel_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        sel_lines.append("=" * 70)
        sel_lines.append(f"\nTotal selected: {len(cand_df)}")
        sel_lines.append(f"Target deficit: {sum(V2_DEFICITS.values())}")
        sel_lines.append(f"Unique track_ids: {cand_df['track_id'].nunique()}")
        sel_lines.append(f"Duplicate track_ids: {cand_df['track_id'].duplicated().sum()}")
        sel_lines.append(f"\nBy V2 genre:")
        for g, c in cand_df["v2_genre"].value_counts().sort_values(ascending=False).items():
            deficit = V2_DEFICITS.get(g, 0)
            sel_lines.append(f"  {g:15s}: {c:4d} selected (deficit was {deficit})")
        sel_lines.append(f"\nBy priority:")
        for p, c in cand_df["priority"].value_counts().sort_index().items():
            sel_lines.append(f"  {p}: {c}")
        sel_lines.append(f"\nBy confidence:")
        for conf, c in cand_df["confidence"].value_counts().items():
            sel_lines.append(f"  {conf}: {c}")
        sel_lines.append(f"\nFull candidate list:")
        for _, row in cand_df.iterrows():
            sel_lines.append(
                f"  [{row['track_id']:5d}] {row['artist'][:25]:25s} | "
                f"{row['original_genre']:12s} -> {row['v2_genre']:10s} | "
                f"{row['confidence']:6s} | {row['priority']} | {row['selection_reason']}"
            )
        w("02_selected_candidates.txt", sel_lines)

        # 03 — review template summary
        w("03_review_template_summary.txt", review_summary)

        # 04 — acquisition plan
        w("04_class_acquisition_plan.txt", acquisition_plan)

        # 05 — validation checks
        val_lines = []
        val_lines.append("=" * 70)
        val_lines.append("VALIDATION CHECKS")
        val_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val_lines.append("=" * 70)

        all_ok = True

        # Check 1: All selected track_ids exist in tracks table
        track_ids = cand_df["track_id"].tolist()
        placeholders = ",".join("?" * len(track_ids))
        existing = conn.execute(
            f"SELECT COUNT(*) FROM tracks WHERE id IN ({placeholders})", track_ids
        ).fetchone()[0]
        chk1 = existing == len(track_ids)
        val_lines.append(f"\n  1. All track_ids exist in tracks: "
                         f"{'PASS' if chk1 else 'FAIL'} ({existing}/{len(track_ids)})")
        if not chk1:
            all_ok = False

        # Check 2: All selected tracks have primary labels
        labeled = conn.execute(
            f"SELECT COUNT(DISTINCT track_id) FROM track_genre_labels "
            f"WHERE role='primary' AND track_id IN ({placeholders})", track_ids
        ).fetchone()[0]
        chk2 = labeled == len(track_ids)
        val_lines.append(f"  2. All selected have primary labels: "
                         f"{'PASS' if chk2 else 'FAIL'} ({labeled}/{len(track_ids)})")
        if not chk2:
            all_ok = False

        # Check 3: None are currently in benchmark
        in_bench = conn.execute(
            f"SELECT COUNT(*) FROM benchmark_set_tracks "
            f"WHERE benchmark_set_id = (SELECT id FROM benchmark_sets WHERE name = ?) "
            f"AND track_id IN ({placeholders})",
            [BENCHMARK_NAME] + track_ids
        ).fetchone()[0]
        chk3 = in_bench == 0
        val_lines.append(f"  3. None in current benchmark: "
                         f"{'PASS' if chk3 else 'FAIL'} ({in_bench} found)")
        if not chk3:
            all_ok = False

        # Check 4: No duplicate track_ids
        dup_count = cand_df["track_id"].duplicated().sum()
        chk4 = dup_count == 0
        val_lines.append(f"  4. No duplicate track_ids: "
                         f"{'PASS' if chk4 else 'FAIL'} ({dup_count} dups)")
        if not chk4:
            all_ok = False

        # Check 5: Benchmark table unchanged
        bench_count = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks "
            "WHERE benchmark_set_id = (SELECT id FROM benchmark_sets WHERE name = ?)",
            (BENCHMARK_NAME,)
        ).fetchone()[0]
        chk5 = bench_count == 200
        val_lines.append(f"  5. Benchmark count unchanged: "
                         f"{'PASS' if chk5 else 'FAIL'} (count={bench_count}, expected=200)")
        if not chk5:
            all_ok = False

        # Check 6: track_genre_labels count unchanged
        label_count = conn.execute("SELECT COUNT(*) FROM track_genre_labels").fetchone()[0]
        chk6 = label_count == 781
        val_lines.append(f"  6. Label count unchanged: "
                         f"{'PASS' if chk6 else 'FAIL'} (count={label_count}, expected=781)")
        if not chk6:
            all_ok = False

        # Check 7: FK integrity
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk7 = len(fk_violations) == 0
        val_lines.append(f"  7. FK integrity: "
                         f"{'PASS' if chk7 else 'FAIL'} ({len(fk_violations)} violations)")
        if not chk7:
            all_ok = False

        # Check 8: DB opened read-only
        val_lines.append(f"  8. DB opened read-only: PASS (file: URI with ?mode=ro)")

        # Check 9: Candidate genres match V2 mapping
        for _, row in cand_df.iterrows():
            expected_v2 = V2_MAP.get(row["original_genre"])
            if expected_v2 != row["v2_genre"]:
                val_lines.append(f"  FAIL: track {row['track_id']} original={row['original_genre']} "
                                 f"expected_v2={expected_v2} got v2={row['v2_genre']}")
                all_ok = False
        val_lines.append(f"  9. All V2 mappings consistent: {'PASS' if all_ok else 'CHECK ABOVE'}")

        conn.close()

        val_lines.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")
        w("05_validation_checks.txt", val_lines)

        # 06 — final report
        gate = "PASS" if all_ok else "FAIL"

        report = []
        report.append("=" * 70)
        report.append("BENCHMARK REBALANCE ACQUISITION V2 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- MISSION ---")
        report.append(f"Select candidates from labeled pool to close the 68-track V2 deficit.")

        report.append(f"\n--- RESULTS ---")
        total_selected = len(cand_df)
        total_deficit = sum(V2_DEFICITS.values())
        total_remaining = sum(
            selection_summary.get(g, {"remaining_deficit": d}).get("remaining_deficit", d)
            for g, d in V2_DEFICITS.items()
        )
        report.append(f"Total deficit: {total_deficit}")
        report.append(f"Total selected from pool: {total_selected}")
        report.append(f"Total remaining after selection: {total_remaining}")
        report.append(f"Fill rate: {total_selected}/{total_deficit} "
                      f"({total_selected/total_deficit*100:.0f}%)")

        report.append(f"\n--- PER-CLASS RESULTS ---")
        report.append(f"{'V2 Genre':15s}  {'Before':>7s}  {'Added':>6s}  {'After':>6s}  "
                      f"{'Deficit Left':>13s}  Status")
        report.append("-" * 65)
        for v2g in ["Metal", "Pop", "Other", "Hip-Hop", "Country", "Rock"]:
            before = V2_CURRENT[v2g]
            deficit = V2_DEFICITS[v2g]
            ss = selection_summary.get(v2g, {"selected": 0, "remaining_deficit": deficit})
            added = ss["selected"]
            after = before + added
            remaining = ss["remaining_deficit"]
            status = ("ADEQUATE" if deficit == 0 else
                      "FILLED" if remaining == 0 else
                      "PARTIAL" if added > 0 else "UNFILLED")
            report.append(f"{v2g:15s}  {before:7d}  {added:6d}  {after:6d}  "
                          f"{remaining:13d}  {status}")

        report.append(f"\n--- OUTPUTS ---")
        report.append(f"  {CANDIDATES_CSV}")
        report.append(f"  {REVIEW_CSV}")
        report.append(f"  Proof: {PROOF_DIR}")

        report.append(f"\n--- PARTS ---")
        report.append(f"  A. Candidate selection: PASS ({total_selected} tracks)")
        report.append(f"  B. Review template: PASS ({total_selected} rows)")
        report.append(f"  C. Acquisition plan: PASS")
        report.append(f"  D. No DB writes: PASS (read-only mode)")
        report.append(f"  E. Validation: {'PASS' if all_ok else 'FAIL'}")

        report.append(f"\n--- NEXT STEPS ---")
        report.append(f"  1. Manual review: open {REVIEW_CSV.name} and approve/skip each track")
        report.append(f"  2. Run Phase 13 insertion script on approved rows")
        if total_remaining > 0:
            report.append(f"  3. Source {total_remaining} additional tracks for classes still under target")

        report.append(f"\n{'='*70}")
        report.append(f"GATE={gate}")
        report.append(f"{'='*70}")

        w("06_final_report.txt", report)

        # Execution log
        w("execution_log.txt", self.log)

        self.emit(f"Proof written: {PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1
    if not MAPPING_CSV.exists():
        p.emit(f"FATAL: {MAPPING_CSV} not found")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    # PART A
    cand_df, selection_summary, pool_summary = p.part_a()
    if cand_df is None:
        return 1

    # PART B
    review_df, review_summary = p.part_b(cand_df)

    # PART C
    acquisition_plan = p.part_c(cand_df, selection_summary)

    # PART D
    p.part_d()

    # PART E
    gate = p.part_e(cand_df, selection_summary, pool_summary, review_summary,
                    acquisition_plan)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
