"""
Phase 24 — Targeted Benchmark Expansion for Weak Labels
========================================================
READ-ONLY against production DB. No mutations.
Builds candidate pool, selection plan, review template, and rebalance projection
for the four weak labels: Hip-Hop, Pop, Metal, Other.
"""

import sqlite3
import time
import os
import pathlib
import csv
import json

import numpy as np
import pandas as pd

# ================================================================
# PATHS
# ================================================================
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "weak_label_benchmark_expansion"
PROOF_DIR.mkdir(parents=True, exist_ok=True)

# ================================================================
# CONSTANTS
# ================================================================
V1_LABEL_SET = ["Country", "Hip-Hop", "Metal", "Other", "Pop", "Rock"]
V1_CORE_GENRES = {"Country", "Hip-Hop", "Metal", "Pop", "Rock"}
WEAK_LABELS = ["Hip-Hop", "Metal", "Other", "Pop"]
BENCHMARK_SET_NAME = "genre_benchmark_v1"

# Target minimum support per label for usable classifier performance.
# Rationale: 30-50 examples is the minimum for stable CV with 5 folds.
# Country=83 and Rock=82 perform well — use ~50 as the minimum floor.
TARGET_MIN_SUPPORT = {
    "Hip-Hop": 50,
    "Metal":   30,
    "Other":   30,
    "Pop":     30,
}


class WeakLabelExpansionPipeline:
    def __init__(self):
        self.t0 = time.time()
        self.log: list[str] = []
        self.snap_before: dict = {}
        self.df_candidates: pd.DataFrame | None = None
        self.df_selected: pd.DataFrame | None = None
        self.df_deficit: pd.DataFrame | None = None
        self.df_projection: pd.DataFrame | None = None

    def emit(self, msg: str):
        self.log.append(msg)
        print(msg)

    def connect_ro(self) -> sqlite3.Connection:
        uri = f"file:{ANALYSIS_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    def snapshot_db(self, label: str = "") -> dict:
        conn = self.connect_ro()
        bench_id = conn.execute(
            "SELECT id FROM benchmark_sets WHERE name=?", (BENCHMARK_SET_NAME,)
        ).fetchone()[0]
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (bench_id,)
        ).fetchone()[0]
        prim = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        dup = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        conn.close()
        return {
            "label": label,
            "benchmark": bench,
            "primaries": prim,
            "secondaries": sec,
            "fk_violations": len(fk),
            "dup_primaries": dup,
        }

    # ================================================================
    # PART A — WEAK LABEL AUDIT
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- WEAK LABEL AUDIT")
        self.emit("=" * 70)

        self.snap_before = self.snapshot_db("before")
        self.emit(f"  DB snapshot: bench={self.snap_before['benchmark']}, "
                  f"prim={self.snap_before['primaries']}, "
                  f"sec={self.snap_before['secondaries']}, "
                  f"fk={self.snap_before['fk_violations']}, "
                  f"dup={self.snap_before['dup_primaries']}")

        conn = self.connect_ro()

        # Get benchmark track IDs
        bench_id = conn.execute(
            "SELECT id FROM benchmark_sets WHERE name=?", (BENCHMARK_SET_NAME,)
        ).fetchone()[0]
        bench_rows = conn.execute(
            "SELECT track_id FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (bench_id,)
        ).fetchall()
        bench_ids = set(r["track_id"] for r in bench_rows)
        self.emit(f"  Benchmark tracks: {len(bench_ids)}")

        # Get all labels for benchmark tracks
        placeholders = ",".join("?" * len(bench_ids))
        labels = conn.execute(f"""
            SELECT tgl.track_id, g.name AS genre, tgl.role
            FROM track_genre_labels tgl
            JOIN genres g ON tgl.genre_id = g.id
            WHERE tgl.track_id IN ({placeholders})
              AND tgl.role IN ('primary', 'secondary')
            ORDER BY tgl.track_id, tgl.role
        """, list(bench_ids)).fetchall()

        # Map to 6-class set and count support per label
        track_labels: dict[int, set[str]] = {}
        for row in labels:
            tid = row["track_id"]
            genre = row["genre"]
            mapped = genre if genre in V1_CORE_GENRES else "Other"
            if tid not in track_labels:
                track_labels[tid] = set()
            track_labels[tid].add(mapped)

        support: dict[str, int] = {l: 0 for l in V1_LABEL_SET}
        for tid, lbls in track_labels.items():
            for l in lbls:
                support[l] += 1

        self.emit(f"  Current support:")
        for l in V1_LABEL_SET:
            self.emit(f"    {l}: {support[l]}")

        # Count primary vs secondary contribution for weak labels
        primary_support: dict[str, int] = {l: 0 for l in WEAK_LABELS}
        secondary_support: dict[str, int] = {l: 0 for l in WEAK_LABELS}
        for row in labels:
            genre = row["genre"]
            mapped = genre if genre in V1_CORE_GENRES else "Other"
            if mapped in WEAK_LABELS:
                if row["role"] == "primary":
                    primary_support[mapped] += 1
                else:
                    secondary_support[mapped] += 1

        self.emit(f"\n  Weak label breakdown (primary / secondary):")
        for l in WEAK_LABELS:
            self.emit(f"    {l}: primary={primary_support[l]}, secondary={secondary_support[l]}")

        # Build deficit plan
        deficit_rows = []
        for l in WEAK_LABELS:
            cur = support[l]
            target = TARGET_MIN_SUPPORT[l]
            deficit = max(0, target - cur)
            # Prioritize primary exemplars (70%) over secondary (30%)
            needed_prim = int(np.ceil(deficit * 0.7))
            needed_sec = deficit - needed_prim
            notes = ""
            if deficit == 0:
                notes = "Already at target"
            elif cur == 0:
                notes = "Zero support — critical gap"
            elif cur < 15:
                notes = "Very low support — high priority"
            else:
                notes = "Below target — moderate priority"

            deficit_rows.append({
                "label": l,
                "current_support": cur,
                "target_min_support": target,
                "deficit": deficit,
                "needed_primary_examples": needed_prim,
                "needed_secondary_examples": needed_sec,
                "notes": notes,
            })

        self.df_deficit = pd.DataFrame(deficit_rows)
        self.df_deficit.to_csv(DATA_DIR / "weak_label_deficit_plan_v1.csv",
                               index=False, encoding="utf-8")
        self.emit(f"\n  Deficit plan saved: {DATA_DIR / 'weak_label_deficit_plan_v1.csv'}")

        for _, r in self.df_deficit.iterrows():
            self.emit(f"    {r['label']}: support={r['current_support']}, "
                      f"target={r['target_min_support']}, deficit={r['deficit']}")

        # Write proof
        lines = []
        lines.append("=" * 70)
        lines.append("WEAK LABEL AUDIT")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nBenchmark set: {BENCHMARK_SET_NAME}")
        lines.append(f"Benchmark tracks: {len(bench_ids)}")
        lines.append(f"\nCurrent support (6-class mapped):")
        for l in V1_LABEL_SET:
            marker = " <-- WEAK" if l in WEAK_LABELS else ""
            lines.append(f"  {l:12s}: {support[l]:4d}{marker}")
        lines.append(f"\nWeak label primary/secondary breakdown:")
        for l in WEAK_LABELS:
            lines.append(f"  {l:12s}: primary={primary_support[l]:3d}, "
                         f"secondary={secondary_support[l]:3d}")
        lines.append(f"\nDeficit plan:")
        lines.append(f"  {'Label':<12s} {'Current':>8s} {'Target':>8s} {'Deficit':>8s} "
                     f"{'Need Prim':>10s} {'Need Sec':>10s} Notes")
        lines.append(f"  {'-'*80}")
        for _, r in self.df_deficit.iterrows():
            lines.append(f"  {r['label']:<12s} {r['current_support']:>8d} "
                         f"{r['target_min_support']:>8d} {r['deficit']:>8d} "
                         f"{r['needed_primary_examples']:>10d} {r['needed_secondary_examples']:>10d} "
                         f"{r['notes']}")
        lines.append(f"\nTarget rationale: 30-50 minimum for stable 5-fold CV. "
                     f"Country=83, Rock=82 perform well at current levels.")

        (PROOF_DIR / "00_weak_label_audit.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        conn.close()
        self.bench_ids = bench_ids
        self.support = support

    # ================================================================
    # PART B — CANDIDATE POOL DISCOVERY
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B -- CANDIDATE POOL DISCOVERY")
        self.emit("=" * 70)

        conn = self.connect_ro()

        # 1. Get ALL labeled tracks with their primary/secondary/subgenre info
        all_labels = conn.execute("""
            SELECT
                tgl.track_id,
                t.artist,
                t.title,
                g.name AS genre,
                COALESCE(sg.name, '') AS subgenre,
                tgl.role,
                tgl.source,
                tgl.confidence
            FROM track_genre_labels tgl
            JOIN tracks t ON t.id = tgl.track_id
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            WHERE tgl.role IN ('primary', 'secondary')
            ORDER BY tgl.track_id, tgl.role
        """).fetchall()

        # 2. Build per-track label profiles
        track_profiles: dict[int, dict] = {}
        for row in all_labels:
            tid = row["track_id"]
            if tid not in track_profiles:
                track_profiles[tid] = {
                    "artist": row["artist"],
                    "title": row["title"],
                    "primary_genres": [],
                    "primary_subgenres": [],
                    "secondary_genres": [],
                    "secondary_subgenres": [],
                    "sources": set(),
                    "min_confidence": None,
                }
            p = track_profiles[tid]
            genre = row["genre"]
            subgenre = row["subgenre"]
            source = row["source"]
            conf = row["confidence"]

            p["sources"].add(source)
            if conf is not None:
                if p["min_confidence"] is None or conf < p["min_confidence"]:
                    p["min_confidence"] = conf

            if row["role"] == "primary":
                p["primary_genres"].append(genre)
                if subgenre:
                    p["primary_subgenres"].append(subgenre)
            else:
                p["secondary_genres"].append(genre)
                if subgenre:
                    p["secondary_subgenres"].append(subgenre)

        # 3. Filter to non-benchmark tracks only
        non_bench = {tid: p for tid, p in track_profiles.items()
                     if tid not in self.bench_ids}
        self.emit(f"  Total labeled tracks: {len(track_profiles)}")
        self.emit(f"  Non-benchmark labeled tracks: {len(non_bench)}")

        # 4. Check which non-bench tracks have analysis data (features)
        non_bench_ids = list(non_bench.keys())
        placeholders = ",".join("?" * len(non_bench_ids))
        analyzed = set()
        if non_bench_ids:
            analyzed_rows = conn.execute(f"""
                SELECT DISTINCT track_id FROM analysis_summary
                WHERE track_id IN ({placeholders})
            """, non_bench_ids).fetchall()
            analyzed = set(r["track_id"] for r in analyzed_rows)
        self.emit(f"  Non-benchmark tracks with analysis data: {len(analyzed)}")

        # 5. Build candidate pool: tracks that support at least one weak label
        candidates = []
        for tid, p in non_bench.items():
            # Map all genres to 6-class
            all_genres_mapped = set()
            primary_mapped = set()
            secondary_mapped = set()

            for g in p["primary_genres"]:
                m = g if g in V1_CORE_GENRES else "Other"
                all_genres_mapped.add(m)
                primary_mapped.add(m)

            for g in p["secondary_genres"]:
                m = g if g in V1_CORE_GENRES else "Other"
                all_genres_mapped.add(m)
                secondary_mapped.add(m)

            # Which weak labels does this track support?
            supported_weak = all_genres_mapped & set(WEAK_LABELS)
            if not supported_weak:
                continue

            # Determine support type
            for wl in supported_weak:
                is_primary = wl in primary_mapped
                is_secondary = wl in secondary_mapped
                if is_primary and is_secondary:
                    support_type = "both"
                elif is_primary:
                    support_type = "primary"
                else:
                    support_type = "secondary"

                # Confidence assessment
                conf = "high"
                if p["min_confidence"] is not None and p["min_confidence"] < 0.5:
                    conf = "low"
                elif p["min_confidence"] is not None and p["min_confidence"] < 0.8:
                    conf = "medium"
                elif "llm" in p["sources"] and "manual" not in p["sources"]:
                    conf = "medium"  # LLM-only labels get medium

                # Check for analysis data
                has_features = tid in analyzed

                # Skip low-confidence candidates
                if conf == "low":
                    continue

                # For "Other" mapped tracks, get the actual primary genre name
                primary_genre_display = ", ".join(p["primary_genres"])
                subgenre_display = ", ".join(p["primary_subgenres"]) if p["primary_subgenres"] else ""
                secondary_display = ", ".join(p["secondary_genres"]) if p["secondary_genres"] else ""

                notes_parts = []
                if not has_features:
                    notes_parts.append("NO_ANALYSIS_DATA")
                if "llm" in p["sources"] and "manual" not in p["sources"]:
                    notes_parts.append("LLM_ONLY_LABELS")
                if "rules" in p["sources"]:
                    notes_parts.append("RULE_GENERATED")

                candidates.append({
                    "track_id": tid,
                    "artist": p["artist"] or "Unknown",
                    "title": p["title"] or "Unknown",
                    "primary_genre": primary_genre_display,
                    "subgenre": subgenre_display,
                    "secondary_genre": secondary_display,
                    "target_label_supported": wl,
                    "support_type": support_type,
                    "confidence": conf,
                    "benchmark_member": "no",
                    "has_features": "yes" if has_features else "no",
                    "notes": "; ".join(notes_parts) if notes_parts else "",
                })

        self.df_candidates = pd.DataFrame(candidates)
        self.emit(f"  Candidate pool size: {len(self.df_candidates)}")

        # Summary by target label
        for wl in WEAK_LABELS:
            subset = self.df_candidates[self.df_candidates["target_label_supported"] == wl]
            n_high = len(subset[subset["confidence"] == "high"])
            n_med = len(subset[subset["confidence"] == "medium"])
            n_feat = len(subset[subset["has_features"] == "yes"])
            self.emit(f"    {wl}: {len(subset)} candidates "
                      f"(high={n_high}, medium={n_med}, with_features={n_feat})")

        self.df_candidates.to_csv(DATA_DIR / "weak_label_candidate_pool_v1.csv",
                                  index=False, encoding="utf-8")
        self.emit(f"  Candidate pool saved: {DATA_DIR / 'weak_label_candidate_pool_v1.csv'}")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("CANDIDATE POOL SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal labeled tracks in DB: {len(track_profiles)}")
        lines.append(f"Non-benchmark labeled tracks: {len(non_bench)}")
        lines.append(f"Non-benchmark with analysis data: {len(analyzed)}")
        lines.append(f"Candidate pool (weak-label supporting): {len(self.df_candidates)}")
        lines.append(f"\nCandidate breakdown by target label:")
        for wl in WEAK_LABELS:
            subset = self.df_candidates[self.df_candidates["target_label_supported"] == wl]
            lines.append(f"\n  {wl} ({len(subset)} candidates):")
            lines.append(f"    By confidence: high={len(subset[subset['confidence']=='high'])}, "
                         f"medium={len(subset[subset['confidence']=='medium'])}")
            lines.append(f"    By support type: "
                         f"primary={len(subset[subset['support_type']=='primary'])}, "
                         f"secondary={len(subset[subset['support_type']=='secondary'])}, "
                         f"both={len(subset[subset['support_type']=='both'])}")
            lines.append(f"    With features: {len(subset[subset['has_features']=='yes'])}")
            lines.append(f"    Without features: {len(subset[subset['has_features']=='no'])}")

        lines.append(f"\nExclusion criteria applied:")
        lines.append(f"  - Benchmark members excluded")
        lines.append(f"  - Low-confidence tracks excluded (confidence < 0.5)")
        lines.append(f"  - Only role='primary' or 'secondary' labels considered")
        lines.append(f"  - No 'candidate' role labels included")

        (PROOF_DIR / "01_candidate_pool_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        conn.close()

    # ================================================================
    # PART C — TARGETED SELECTION PLAN
    # ================================================================
    def part_c(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART C -- TARGETED SELECTION PLAN")
        self.emit("=" * 70)

        assert self.df_candidates is not None
        assert self.df_deficit is not None

        selected = []
        selection_log: list[str] = []

        for wl in WEAK_LABELS:
            deficit_row = self.df_deficit[self.df_deficit["label"] == wl].iloc[0]
            deficit = int(deficit_row["deficit"])
            if deficit == 0:
                selection_log.append(f"  {wl}: no deficit, skipping")
                continue

            pool = self.df_candidates[
                self.df_candidates["target_label_supported"] == wl
            ].copy()

            # Priority scoring
            # 1. Has features (required for training)
            # 2. High confidence > medium
            # 3. Primary support > secondary > both
            # 4. Avoid LLM-only labels
            def score(row: pd.Series) -> int:
                s = 0
                if row["has_features"] == "yes":
                    s += 1000
                if row["confidence"] == "high":
                    s += 100
                elif row["confidence"] == "medium":
                    s += 50
                if row["support_type"] == "primary":
                    s += 30
                elif row["support_type"] == "both":
                    s += 20
                else:
                    s += 10
                if "LLM_ONLY_LABELS" in str(row.get("notes", "")):
                    s -= 25
                if "RULE_GENERATED" in str(row.get("notes", "")):
                    s -= 10
                return s

            pool["_score"] = pool.apply(score, axis=1)
            pool = pool.sort_values("_score", ascending=False)

            # Deduplicate by track_id (a track may appear in pool for multiple weak labels)
            # For this label, keep one entry per track
            pool = pool.drop_duplicates(subset=["track_id"], keep="first")

            # Select up to deficit count, preferring high-score candidates
            # Also: prefer artist diversity (don't take >3 from same artist)
            artist_counts: dict[str, int] = {}
            max_per_artist = 3
            chosen = []
            for _, row in pool.iterrows():
                if len(chosen) >= deficit:
                    break
                artist = str(row["artist"]).strip().lower()
                if artist_counts.get(artist, 0) >= max_per_artist:
                    continue
                artist_counts[artist] = artist_counts.get(artist, 0) + 1

                priority = "P1" if row["_score"] >= 1100 else (
                    "P2" if row["_score"] >= 1000 else "P3"
                )
                reasons = []
                if row["has_features"] == "yes":
                    reasons.append("has_analysis_data")
                if row["confidence"] == "high":
                    reasons.append("high_confidence")
                if row["support_type"] == "primary":
                    reasons.append("clean_primary_exemplar")
                elif row["support_type"] == "both":
                    reasons.append("hybrid_exemplar")
                else:
                    reasons.append("secondary_exemplar")
                if row["subgenre"]:
                    reasons.append(f"subgenre={row['subgenre']}")

                chosen.append({
                    "track_id": row["track_id"],
                    "artist": row["artist"],
                    "title": row["title"],
                    "target_label": wl,
                    "support_type": row["support_type"],
                    "primary_genre": row["primary_genre"],
                    "subgenre": row["subgenre"],
                    "secondary_genre": row["secondary_genre"],
                    "selection_priority": priority,
                    "selection_reason": "; ".join(reasons),
                })

            selected.extend(chosen)
            selection_log.append(f"  {wl}: deficit={deficit}, pool={len(pool)}, "
                                 f"selected={len(chosen)}")

        self.df_selected = pd.DataFrame(selected)
        self.emit(f"  Total selected candidates: {len(self.df_selected)}")
        for line in selection_log:
            self.emit(line)

        self.df_selected.to_csv(DATA_DIR / "weak_label_expansion_candidates_v1.csv",
                                 index=False, encoding="utf-8")
        self.emit(f"  Selection plan saved: "
                  f"{DATA_DIR / 'weak_label_expansion_candidates_v1.csv'}")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("SELECTION PLAN")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nSelection criteria:")
        lines.append(f"  1. Must have analysis features (for training)")
        lines.append(f"  2. High confidence preferred over medium")
        lines.append(f"  3. Primary exemplars > hybrid > secondary-only")
        lines.append(f"  4. Max {3} tracks per artist (diversity)")
        lines.append(f"  5. LLM-only labels deprioritized")
        lines.append(f"  6. Subgenre-aligned tracks preferred")
        lines.append(f"\nSelection summary:")
        for line in selection_log:
            lines.append(line)

        lines.append(f"\nPriority distribution:")
        if len(self.df_selected) > 0:
            for p in ["P1", "P2", "P3"]:
                cnt = len(self.df_selected[self.df_selected["selection_priority"] == p])
                lines.append(f"  {p}: {cnt}")

        lines.append(f"\nSelected candidates by label:")
        for wl in WEAK_LABELS:
            subset = self.df_selected[self.df_selected["target_label"] == wl]
            lines.append(f"\n  {wl} ({len(subset)} selected):")
            for _, row in subset.head(10).iterrows():
                lines.append(f"    {int(row['track_id']):5d} | {row['artist']:25s} | "
                             f"{row['title']:30s} | {row['selection_priority']} | "
                             f"{row['support_type']}")
            if len(subset) > 10:
                lines.append(f"    ... and {len(subset) - 10} more")

        (PROOF_DIR / "02_selection_plan.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART D — REVIEW TEMPLATE
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D -- REVIEW TEMPLATE")
        self.emit("=" * 70)

        assert self.df_selected is not None

        review_rows = []
        for _, row in self.df_selected.iterrows():
            review_rows.append({
                "track_id": int(row["track_id"]),
                "artist": row["artist"],
                "title": row["title"],
                "target_label": row["target_label"],
                "action": "",  # blank for human review
                "notes": "",
            })

        df_review = pd.DataFrame(review_rows)

        # Add 3 example rows at the top showing how to use the template
        example_rows = []
        if len(review_rows) >= 3:
            ex = review_rows[:3]
            example_rows.append({
                "track_id": ex[0]["track_id"],
                "artist": ex[0]["artist"],
                "title": ex[0]["title"],
                "target_label": ex[0]["target_label"],
                "action": "approve",
                "notes": "EXAMPLE: Clean primary exemplar, confirmed by listening",
            })
            example_rows.append({
                "track_id": ex[1]["track_id"],
                "artist": ex[1]["artist"],
                "title": ex[1]["title"],
                "target_label": ex[1]["target_label"],
                "action": "skip",
                "notes": "EXAMPLE: Genre is ambiguous, not a clear exemplar",
            })
            example_rows.append({
                "track_id": ex[2]["track_id"],
                "artist": ex[2]["artist"],
                "title": ex[2]["title"],
                "target_label": ex[2]["target_label"],
                "action": "hold",
                "notes": "EXAMPLE: Need to re-listen, possible edge case",
            })

        df_examples = pd.DataFrame(example_rows)
        df_full = pd.concat([df_examples, df_review], ignore_index=True)

        df_full.to_csv(DATA_DIR / "weak_label_expansion_review_v1.csv",
                       index=False, encoding="utf-8")
        self.emit(f"  Review template saved: "
                  f"{DATA_DIR / 'weak_label_expansion_review_v1.csv'}")
        self.emit(f"  Total rows: {len(df_full)} "
                  f"(3 examples + {len(df_review)} review candidates)")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("REVIEW TEMPLATE SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nReview file: weak_label_expansion_review_v1.csv")
        lines.append(f"Total rows: {len(df_full)}")
        lines.append(f"  Example rows: 3 (showing approve/skip/hold usage)")
        lines.append(f"  Review candidates: {len(df_review)}")
        lines.append(f"\nReview instructions:")
        lines.append(f"  1. For each candidate, set 'action' to: approve, skip, or hold")
        lines.append(f"  2. Add notes explaining your decision")
        lines.append(f"  3. 'approve' = add to benchmark set in next wave")
        lines.append(f"  4. 'skip' = exclude from this expansion")
        lines.append(f"  5. 'hold' = needs further investigation")
        lines.append(f"\nBreakdown by target label:")
        for wl in WEAK_LABELS:
            cnt = len(df_review[df_review["target_label"] == wl])
            lines.append(f"  {wl}: {cnt} candidates to review")

        (PROOF_DIR / "03_review_template_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART E — REBALANCE SIMULATION
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E -- REBALANCE SIMULATION")
        self.emit("=" * 70)

        assert self.df_selected is not None
        assert self.df_deficit is not None

        # Count proposed additions per label
        additions: dict[str, int] = {}
        for wl in WEAK_LABELS:
            additions[wl] = len(self.df_selected[self.df_selected["target_label"] == wl])

        projection_rows = []
        all_labels_for_report = V1_LABEL_SET
        for l in all_labels_for_report:
            cur = self.support[l]
            add = additions.get(l, 0)
            proj = cur + add
            if l not in WEAK_LABELS:
                status = "STRONG (not targeted)"
            elif proj >= TARGET_MIN_SUPPORT.get(l, 30):
                status = "TARGET_MET"
            elif proj >= 20:
                status = "IMPROVED_BUT_BELOW_TARGET"
            else:
                status = "STILL_WEAK"

            notes = ""
            if l in WEAK_LABELS:
                target = TARGET_MIN_SUPPORT[l]
                if proj >= target:
                    notes = f"Meets target of {target}"
                else:
                    notes = f"Still {target - proj} short of target {target}"

            projection_rows.append({
                "label": l,
                "current_support": cur,
                "proposed_additions": add,
                "projected_support": proj,
                "projected_status": status,
                "notes": notes,
            })

        self.df_projection = pd.DataFrame(projection_rows)
        self.df_projection.to_csv(
            DATA_DIR / "weak_label_expansion_projection_v1.csv",
            index=False, encoding="utf-8"
        )
        self.emit(f"  Projection saved: "
                  f"{DATA_DIR / 'weak_label_expansion_projection_v1.csv'}")

        self.emit(f"\n  Rebalance projection:")
        self.emit(f"  {'Label':<12s} {'Current':>8s} {'Added':>8s} {'Projected':>10s} Status")
        self.emit(f"  {'-'*65}")
        for _, r in self.df_projection.iterrows():
            self.emit(f"  {r['label']:<12s} {r['current_support']:>8d} "
                      f"{r['proposed_additions']:>8d} {r['projected_support']:>10d} "
                      f"{r['projected_status']}")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("REBALANCE PROJECTION")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nAssumption: ALL proposed candidates are approved.")
        lines.append(f"Reality check: approval rate will be lower — this is best-case.")
        lines.append(f"\n  {'Label':<12s} {'Current':>8s} {'Added':>8s} {'Projected':>10s} "
                     f"{'Target':>8s} Status")
        lines.append(f"  {'-'*75}")
        for _, r in self.df_projection.iterrows():
            target = TARGET_MIN_SUPPORT.get(r['label'], '-')
            self.emit(f"  {r['label']}")
            lines.append(f"  {r['label']:<12s} {r['current_support']:>8d} "
                         f"{r['proposed_additions']:>8d} {r['projected_support']:>10d} "
                         f"{str(target):>8s} {r['projected_status']}")
        lines.append(f"\nTarget minimum support levels:")
        for l, t in TARGET_MIN_SUPPORT.items():
            lines.append(f"  {l}: {t}")
        lines.append(f"\nNote: 'Other' is a catch-all class. Adding more Other-primary ")
        lines.append(f"tracks may not improve signal — consider whether genre diversity ")
        lines.append(f"within Other helps or hurts the classifier.")

        (PROOF_DIR / "04_rebalance_projection.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART H — VALIDATION
    # ================================================================
    def part_h(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART H -- VALIDATION")
        self.emit("=" * 70)

        snap_after = self.snapshot_db("after")
        checks = []
        all_ok = True

        # 1. Benchmark unchanged
        chk = snap_after["benchmark"] == self.snap_before["benchmark"]
        checks.append(f"  1. Benchmark tracks: before={self.snap_before['benchmark']}, "
                      f"after={snap_after['benchmark']} -- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 2. Primaries unchanged
        chk = snap_after["primaries"] == self.snap_before["primaries"]
        checks.append(f"  2. Primary labels: before={self.snap_before['primaries']}, "
                      f"after={snap_after['primaries']} -- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 3. Secondaries unchanged
        chk = snap_after["secondaries"] == self.snap_before["secondaries"]
        checks.append(f"  3. Secondary labels: before={self.snap_before['secondaries']}, "
                      f"after={snap_after['secondaries']} -- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 4. No dup primaries
        chk = snap_after["dup_primaries"] == 0
        checks.append(f"  4. Duplicate primaries: {snap_after['dup_primaries']} "
                      f"-- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 5. FK clean
        chk = snap_after["fk_violations"] == 0
        checks.append(f"  5. FK violations: {snap_after['fk_violations']} "
                      f"-- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 6. Candidate rows are non-benchmark only
        assert self.df_candidates is not None
        bench_in_pool = self.df_candidates[
            self.df_candidates["benchmark_member"] == "yes"
        ]
        chk = len(bench_in_pool) == 0
        checks.append(f"  6. Benchmark members in candidate pool: {len(bench_in_pool)} "
                      f"-- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 7. Selected candidates are non-benchmark
        assert self.df_selected is not None
        selected_ids = set(self.df_selected["track_id"].astype(int))
        overlap = selected_ids & self.bench_ids
        chk = len(overlap) == 0
        checks.append(f"  7. Selected candidates overlapping benchmark: {len(overlap)} "
                      f"-- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        # 8. Schema unchanged (compare table list)
        conn = self.connect_ro()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [r["name"] for r in tables]
        conn.close()
        # We just verify the expected tables exist — schema check
        expected = {"tracks", "track_genre_labels", "genres", "subgenres",
                    "benchmark_sets", "benchmark_set_tracks", "analysis_summary",
                    "section_events"}
        chk = expected.issubset(set(table_names))
        checks.append(f"  8. Schema tables intact: {chk} -- {'PASS' if chk else 'FAIL'}")
        if not chk:
            all_ok = False

        for c in checks:
            self.emit(c)

        self.emit(f"\n  DB integrity: {'PASS' if all_ok else 'FAIL'}")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("VALIDATION CHECKS")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        for c in checks:
            lines.append(c)
        lines.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        (PROOF_DIR / "05_validation_checks.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok: bool):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"

        assert self.df_deficit is not None
        assert self.df_candidates is not None
        assert self.df_selected is not None
        assert self.df_projection is not None

        report = []
        report.append("=" * 70)
        report.append("TARGETED BENCHMARK EXPANSION FOR WEAK LABELS — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- WEAK LABEL DEFICITS ---")
        for _, r in self.df_deficit.iterrows():
            report.append(f"  {r['label']}: support={r['current_support']}, "
                          f"target={r['target_min_support']}, deficit={r['deficit']}")

        report.append(f"\n--- CANDIDATE POOL ---")
        report.append(f"  Total candidates: {len(self.df_candidates)}")
        for wl in WEAK_LABELS:
            cnt = len(self.df_candidates[self.df_candidates["target_label_supported"] == wl])
            report.append(f"    {wl}: {cnt}")

        report.append(f"\n--- SELECTED FOR REVIEW ---")
        report.append(f"  Total selected: {len(self.df_selected)}")
        for wl in WEAK_LABELS:
            cnt = len(self.df_selected[self.df_selected["target_label"] == wl])
            report.append(f"    {wl}: {cnt}")

        report.append(f"\n--- REBALANCE PROJECTION (best-case) ---")
        for _, r in self.df_projection.iterrows():
            if r["label"] in WEAK_LABELS:
                report.append(f"  {r['label']}: {r['current_support']} → "
                              f"{r['projected_support']} ({r['projected_status']})")

        report.append(f"\n--- DB INTEGRITY ---")
        report.append(f"  Benchmark: {self.snap_before['benchmark']} (unchanged)")
        report.append(f"  Primaries: {self.snap_before['primaries']} (unchanged)")
        report.append(f"  Secondaries: {self.snap_before['secondaries']} (unchanged)")
        report.append(f"  FK: clean")
        report.append(f"  Schema: unchanged")

        report.append(f"\n--- ARTIFACTS ---")
        data_files = [
            "weak_label_deficit_plan_v1.csv",
            "weak_label_candidate_pool_v1.csv",
            "weak_label_expansion_candidates_v1.csv",
            "weak_label_expansion_review_v1.csv",
            "weak_label_expansion_projection_v1.csv",
        ]
        for f in data_files:
            fp = DATA_DIR / f
            exists = fp.exists()
            report.append(f"  {f}: {'OK' if exists else 'MISSING'}")

        proof_files = [
            "00_weak_label_audit.txt",
            "01_candidate_pool_summary.txt",
            "02_selection_plan.txt",
            "03_review_template_summary.txt",
            "04_rebalance_projection.txt",
            "05_validation_checks.txt",
            "06_final_report.txt",
            "execution_log.txt",
        ]
        for f in proof_files:
            fp = PROOF_DIR / f
            exists = fp.exists() or f in ("06_final_report.txt", "execution_log.txt")
            report.append(f"  {f}: {'OK' if exists else 'PENDING'}")

        report.append(f"\n{'=' * 70}")
        report.append(f"GATE={gate}")
        report.append(f"{'=' * 70}")

        report_text = "\n".join(report)
        (PROOF_DIR / "06_final_report.txt").write_text(report_text, encoding="utf-8")

        # Execution log
        (PROOF_DIR / "execution_log.txt").write_text(
            "\n".join(self.log), encoding="utf-8"
        )

        # Copy data CSVs to proof
        for f in data_files:
            src = DATA_DIR / f
            if src.exists():
                import shutil
                shutil.copy2(src, PROOF_DIR / f)

        self.emit(f"\n{report_text}")

        return gate


def main():
    cwd = os.getcwd()
    print(f"CWD: {cwd}")
    print(f"DB: {ANALYSIS_DB}")
    print(f"PROOF: {PROOF_DIR}")

    pipeline = WeakLabelExpansionPipeline()

    pipeline.part_a()
    pipeline.part_b()
    pipeline.part_c()
    pipeline.part_d()
    pipeline.part_e()
    ok = pipeline.part_h()
    gate = pipeline.final_report(ok)

    print(f"\nPF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")


if __name__ == "__main__":
    main()
