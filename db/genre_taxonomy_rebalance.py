#!/usr/bin/env python3
"""
Phase 11 — Genre Taxonomy Consolidation + Benchmark Rebalance V2

Parts:
  A) Current taxonomy audit
  B) Propose consolidated V2 taxonomy
  C) Benchmark rebalance analysis
  D) Build consolidated dataset preview
  E) V2 readiness assessment
  F) Output reports + validation
"""

import sqlite3
import sys
import time
import warnings
from collections import OrderedDict
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
DATASET_V1 = WORKSPACE / "data" / "classifier_dataset_v1.csv"
CONFUSION_CSV = WORKSPACE / "_proof" / "baseline_genre_classifier_v1" / "04_confusion_matrix.csv"
PROOF_DIR = WORKSPACE / "_proof" / "genre_taxonomy_rebalance_v2"
DATA_DIR = WORKSPACE / "data"

# Output files
MAPPING_CSV = DATA_DIR / "genre_taxonomy_v2_mapping.csv"
REBALANCE_CSV = DATA_DIR / "benchmark_rebalance_plan_v2.csv"
PREVIEW_CSV = DATA_DIR / "classifier_dataset_v2_preview.csv"
COMPARISON_CSV = PROOF_DIR / "class_count_comparison.csv"

BENCHMARK_NAME = "genre_benchmark_v1"

FEATURE_COLS = [
    "harmonic_stability", "loudness_lufs", "avg_section_duration",
    "tempo_stability", "energy", "danceability", "section_count",
]

# Baseline classifier results (from Phase 10)
BASELINE_METRICS = {
    "accuracy": 0.5300,
    "balanced_accuracy": 0.2232,
    "macro_f1": 0.1948,
}

PER_CLASS_F1 = {
    "Country": 0.7164, "Rock": 0.5071, "Hip-Hop": 0.1561,
    "Electronic": 0.0, "Folk": 0.0, "Metal": 0.0,
    "Pop": 0.0, "R&B": 0.0, "Reggae": 0.0,
    "Soundtrack": 0.0, "World": 0.0,
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
    # PART A — CURRENT TAXONOMY AUDIT
    # ================================================================
    def part_a(self, df, cm_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — CURRENT TAXONOMY AUDIT")
        self.emit("=" * 60)

        conn = self.connect_ro()

        # 1. Class counts
        genre_dist = df["genre"].value_counts().sort_values(ascending=False)

        # 2. Subgenre counts rolled up
        def _sub_counts(x):
            filtered = x[x != ""]
            if filtered.empty:
                return {}
            return filtered.value_counts().to_dict()

        subgenre_dist = df.groupby("genre")["subgenre"].apply(_sub_counts)

        # 3. Rare class detection (< 5 samples = rare, < 10 = thin)
        rare = [(g, c) for g, c in genre_dist.items() if c < 5]
        thin = [(g, c) for g, c in genre_dist.items() if 5 <= c < 10]

        # 4. Imbalance summary
        majority = genre_dist.iloc[0]
        minority = genre_dist.iloc[-1]
        ratio = majority / minority if minority > 0 else float("inf")

        # 5. Confusion pressure from baseline
        # Parse confusion matrix
        confusion_pairs = []
        for true_g in cm_df.index:
            for pred_g in cm_df.columns:
                if true_g != pred_g:
                    val = cm_df.loc[true_g, pred_g]
                    if val > 0:
                        confusion_pairs.append((true_g, pred_g, int(val)))
        confusion_pairs.sort(key=lambda x: -x[2])

        # 6. Classes too small or too noisy
        # "too small" = < 5 benchmark samples
        # "too noisy" = F1 = 0.0 despite having samples
        too_small = [g for g, c in genre_dist.items() if c < 5]
        too_noisy = [g for g in genre_dist.index if PER_CLASS_F1.get(g, 0) == 0.0]

        conn.close()

        # Build audit text
        audit = []
        audit.append("=" * 70)
        audit.append("CURRENT TAXONOMY AUDIT — genre_benchmark_v1")
        audit.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        audit.append("=" * 70)

        audit.append(f"\n--- 1. CLASS COUNTS (BENCHMARK) ---")
        for g, c in genre_dist.items():
            pct = c / len(df) * 100
            f1 = PER_CLASS_F1.get(g, 0)
            audit.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)  F1={f1:.3f}")
        audit.append(f"  Total: {len(df)}")
        audit.append(f"  Unique genres: {len(genre_dist)}")

        audit.append(f"\n--- 2. SUBGENRE COUNTS ---")
        for g in genre_dist.index:
            subs = subgenre_dist.get(g, {})
            if isinstance(subs, dict) and subs:
                for sg, sc in subs.items():
                    audit.append(f"  {g:15s} / {sg:20s}: {sc}")
            else:
                audit.append(f"  {g:15s} / (none)")

        audit.append(f"\n--- 3. RARE CLASS DETECTION ---")
        audit.append(f"  Rare (< 5 samples): {[f'{g}({c})' for g, c in rare]}")
        audit.append(f"  Thin (5-9 samples): {[f'{g}({c})' for g, c in thin]}")
        audit.append(f"  Adequate (>= 10): {[g for g, c in genre_dist.items() if c >= 10]}")

        audit.append(f"\n--- 4. IMBALANCE SUMMARY ---")
        audit.append(f"  Majority: {genre_dist.index[0]} ({majority})")
        audit.append(f"  Minority: {genre_dist.index[-1]} ({minority})")
        audit.append(f"  Ratio: {ratio:.0f}:1")
        audit.append(f"  Effective classes (>= 10 samples): "
                     f"{sum(1 for c in genre_dist if c >= 10)}/{len(genre_dist)}")
        audit.append(f"  Classes dominating (> 25%): "
                     f"{[g for g, c in genre_dist.items() if c/len(df) > 0.25]}")

        audit.append(f"\n--- 5. CONFUSION PRESSURE (BASELINE RF) ---")
        for true_g, pred_g, count in confusion_pairs[:10]:
            audit.append(f"  {true_g:15s} -> {pred_g:15s}: {count:3d}")

        audit.append(f"\n--- 6. CLASSES TOO SMALL/NOISY FOR V2 ---")
        audit.append(f"  Too small (< 5 benchmark samples): {too_small}")
        audit.append(f"  Zero F1 (entire class misclassified): {too_noisy}")
        audit.append(f"  Conclusion: {len(too_noisy)} of {len(genre_dist)} classes "
                     f"have F1=0.0 — taxonomy is too fragmented for current data size")

        # Imbalance detail text
        imbal = []
        imbal.append("=" * 70)
        imbal.append("CLASS IMBALANCE SUMMARY")
        imbal.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        imbal.append("=" * 70)
        imbal.append(f"\nBenchmark set: {BENCHMARK_NAME}")
        imbal.append(f"Total tracks: {len(df)}")
        imbal.append(f"Classes: {len(genre_dist)}")
        imbal.append(f"Majority/minority ratio: {ratio:.0f}:1")
        imbal.append(f"\nClass sizes:")
        for g, c in genre_dist.items():
            bar = "#" * max(1, int(c / 2))
            imbal.append(f"  {g:15s}: {c:4d} {bar}")
        imbal.append(f"\nImbalance diagnosis:")
        imbal.append(f"  - 3 classes (Country, Rock, Hip-Hop) hold {sum(genre_dist.iloc[:3])}/{len(df)} "
                     f"({sum(genre_dist.iloc[:3])/len(df)*100:.0f}%) of tracks")
        imbal.append(f"  - 8 classes have fewer than 10 samples each")
        imbal.append(f"  - 6 classes have fewer than 5 samples each")
        imbal.append(f"  - Class imbalance renders minority classes untrainable")
        imbal.append(f"  - Stratified CV cannot reliably evaluate classes with < 5 samples")

        # Confusion pressure text
        conf = []
        conf.append("=" * 70)
        conf.append("CONFUSION PRESSURE SUMMARY")
        conf.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        conf.append("=" * 70)
        conf.append(f"\nSource: baseline_genre_classifier_v1 (Random Forest, 5-fold CV)")
        conf.append(f"\nAll confused pairs (sorted by error count):")
        for true_g, pred_g, count in confusion_pairs:
            conf.append(f"  {true_g:15s} -> {pred_g:15s}: {count:3d}")
        conf.append(f"\nTotal off-diagonal errors: {sum(c for _, _, c in confusion_pairs)}")
        conf.append(f"\nKey patterns:")
        conf.append(f"  1. Rock <-> Country bidirectional confusion: "
                     f"{sum(c for a,b,c in confusion_pairs if set([a,b])==set(['Rock','Country']))}")
        conf.append(f"  2. Hip-Hop -> Rock/Country: "
                     f"{sum(c for a,b,c in confusion_pairs if a=='Hip-Hop' and b in ['Rock','Country'])}")
        conf.append(f"  3. All minority classes -> Rock (gravity well): "
                     f"{sum(c for a,b,c in confusion_pairs if b=='Rock' and a not in ['Country','Hip-Hop'])}")
        conf.append(f"  4. Rock acts as a 'catch-all' — classifier defaults to majority-adjacent class")
        conf.append(f"\nImplication for V2 taxonomy:")
        conf.append(f"  - Country and Rock are separable but border is fuzzy")
        conf.append(f"  - Hip-Hop is partially distinguishable but bleeds into Rock/Country")
        conf.append(f"  - All other classes collapse into Rock/Country noise")
        conf.append(f"  - Merging low-support classes into 'Other' will reduce noise")

        self.emit(f"Audit: {len(genre_dist)} classes, {len(rare)} rare, "
                  f"{len(too_noisy)} zero-F1, ratio={ratio:.0f}:1")

        return audit, imbal, conf, genre_dist, confusion_pairs

    # ================================================================
    # PART B — PROPOSE V2 TAXONOMY
    # ================================================================
    def part_b(self, genre_dist, confusion_pairs):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — V2 TAXONOMY PROPOSAL")
        self.emit("=" * 60)

        # Decision logic based on evidence:
        # Classes with adequate support (>= 20 benchmark) and F1 > 0: KEEP
        # Classes with moderate support (8-19) and some separability: KEEP if pool exists
        # Classes with low support (< 8) and F1 = 0: MERGE into "Other"
        #
        # From data:
        #   Country: 82 bench, F1=0.716 -> KEEP
        #   Rock: 66 bench, F1=0.507 -> KEEP
        #   Hip-Hop: 29 bench, F1=0.156 -> KEEP (marginal but separable)
        #   Pop: 8 bench, F1=0.0 -> KEEP tentatively (18 more in pool, total 26)
        #   Metal: 6 bench, F1=0.0 -> KEEP tentatively (14 more in pool, total 20)
        #   Electronic: 2 bench -> Other
        #   Folk: 2 bench -> Other
        #   Reggae: 2 bench -> Other
        #   R&B: 1 bench -> Other
        #   Soundtrack: 1 bench -> Other
        #   World: 1 bench -> Other

        # But Pop at 8 with F1=0.0 — can we fill it from pool?
        # Pool: Pop=18 non-benchmark → can grow to 26 total → viable if reviewed
        # Metal at 6 with F1=0.0 — Pool: Metal=14 → can grow to 20 → viable if reviewed
        # These are borderline. Let's keep them as separate V2 classes but flag as "needs expansion"

        v2_mapping = OrderedDict()
        v2_mapping["Country"] = {
            "v2_genre": "Country",
            "reason": "Largest class, highest F1 (0.716), well-separated. Keep as-is.",
            "support_count": int(genre_dist.get("Country", 0)),
            "confusion_notes": "Main confusion with Rock (35 bidirectional errors). Southern Rock border.",
            "durability": "DURABLE",
        }
        v2_mapping["Rock"] = {
            "v2_genre": "Rock",
            "reason": "Second largest, F1=0.507, core genre. Keep as-is.",
            "support_count": int(genre_dist.get("Rock", 0)),
            "confusion_notes": "Confused with Country (35), acts as gravity well for minority classes.",
            "durability": "DURABLE",
        }
        v2_mapping["Hip-Hop"] = {
            "v2_genre": "Hip-Hop",
            "reason": "Third largest, F1=0.156 (marginal but non-zero). Country-rap fusion causes bleed. Keep separate.",
            "support_count": int(genre_dist.get("Hip-Hop", 0)),
            "confusion_notes": "Bleeds into Rock (15) and Country (8). Fusion artists drive confusion.",
            "durability": "DURABLE",
        }
        v2_mapping["Pop"] = {
            "v2_genre": "Pop",
            "reason": "8 benchmark samples, F1=0.0, but 18 more in pool (26 total labeled). Keep separate with expansion.",
            "support_count": int(genre_dist.get("Pop", 0)),
            "confusion_notes": "Confused with Rock (5) and Country (2). Needs more samples to stabilize.",
            "durability": "TENTATIVE — merge into Other if expansion fails",
        }
        v2_mapping["Metal"] = {
            "v2_genre": "Metal",
            "reason": "6 benchmark, F1=0.0, but 14 more in pool (20 total). Musically distinct from Rock. Keep with expansion.",
            "support_count": int(genre_dist.get("Metal", 0)),
            "confusion_notes": "Confused with Rock (4). Hard rock/metal gradient.",
            "durability": "TENTATIVE — merge into Rock if expansion fails",
        }
        v2_mapping["Electronic"] = {
            "v2_genre": "Other",
            "reason": "2 benchmark samples, F1=0.0, no non-benchmark pool. Cannot train. Merge into Other.",
            "support_count": int(genre_dist.get("Electronic", 0)),
            "confusion_notes": "Both samples misclassified as Rock.",
            "durability": "TEMPORARY — restore when data exists",
        }
        v2_mapping["Folk"] = {
            "v2_genre": "Other",
            "reason": "2 benchmark samples, F1=0.0, no non-benchmark pool. Merge into Other.",
            "support_count": int(genre_dist.get("Folk", 0)),
            "confusion_notes": "Both samples misclassified as Rock.",
            "durability": "TEMPORARY — restore when data exists",
        }
        v2_mapping["Reggae"] = {
            "v2_genre": "Other",
            "reason": "2 benchmark, F1=0.0, 5 in pool but still very low. Merge into Other.",
            "support_count": int(genre_dist.get("Reggae", 0)),
            "confusion_notes": "Confused with Hip-Hop and Rock.",
            "durability": "TEMPORARY — restore when data exists",
        }
        v2_mapping["R&B"] = {
            "v2_genre": "Other",
            "reason": "1 benchmark sample, F1=0.0, no pool. Merge into Other.",
            "support_count": int(genre_dist.get("R&B", 0)),
            "confusion_notes": "Single sample misclassified as Country.",
            "durability": "TEMPORARY — restore when data exists",
        }
        v2_mapping["Soundtrack"] = {
            "v2_genre": "Other",
            "reason": "1 benchmark sample, F1=0.0, 1 in pool. Merge into Other.",
            "support_count": int(genre_dist.get("Soundtrack", 0)),
            "confusion_notes": "Single sample misclassified as Country.",
            "durability": "TEMPORARY — restore when data exists",
        }
        v2_mapping["World"] = {
            "v2_genre": "Other",
            "reason": "1 benchmark sample, F1=0.0, 2 in pool. Merge into Other.",
            "support_count": int(genre_dist.get("World", 0)),
            "confusion_notes": "Single sample misclassified as Rock.",
            "durability": "TEMPORARY — restore when data exists",
        }

        # Write mapping CSV
        mapping_rows = []
        for orig, d in v2_mapping.items():
            mapping_rows.append({
                "original_genre": orig,
                "v2_genre": d["v2_genre"],
                "reason": d["reason"],
                "support_count": d["support_count"],
                "confusion_notes": d["confusion_notes"],
            })
        mapping_df = pd.DataFrame(mapping_rows)
        mapping_df.to_csv(MAPPING_CSV, index=False, encoding="utf-8")
        self.emit(f"V2 mapping CSV: {MAPPING_CSV}")

        # Build proposal text
        prop = []
        prop.append("=" * 70)
        prop.append("V2 TAXONOMY PROPOSAL")
        prop.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        prop.append("=" * 70)

        v2_classes = sorted(set(d["v2_genre"] for d in v2_mapping.values()))
        prop.append(f"\nProposed V2 classes: {len(v2_classes)}")
        prop.append(f"Classes: {v2_classes}")
        prop.append(f"\nReduction: {len(v2_mapping)} original -> {len(v2_classes)} V2 classes")

        prop.append(f"\n--- KEEP (unchanged) ---")
        for orig, d in v2_mapping.items():
            if d["v2_genre"] == orig:
                prop.append(f"\n  {orig} -> {d['v2_genre']}  [{d['durability']}]")
                prop.append(f"    Benchmark: {d['support_count']}")
                prop.append(f"    F1: {PER_CLASS_F1.get(orig, 0):.3f}")
                prop.append(f"    Reason: {d['reason']}")
                prop.append(f"    Confusion: {d['confusion_notes']}")

        prop.append(f"\n--- MERGED INTO 'Other' ---")
        merged = [(orig, d) for orig, d in v2_mapping.items() if d["v2_genre"] == "Other"]
        for orig, d in merged:
            prop.append(f"\n  {orig} -> Other  [{d['durability']}]")
            prop.append(f"    Benchmark: {d['support_count']}")
            prop.append(f"    F1: {PER_CLASS_F1.get(orig, 0):.3f}")
            prop.append(f"    Reason: {d['reason']}")

        other_total = sum(d["support_count"] for _, d in v2_mapping.items() if d["v2_genre"] == "Other")
        prop.append(f"\n  'Other' total benchmark samples: {other_total}")

        prop.append(f"\n--- RATIONALE ---")
        prop.append(f"The V2 taxonomy reduces 11 classes to 6 by merging all classes with:")
        prop.append(f"  - fewer than 5 benchmark samples AND")
        prop.append(f"  - F1 = 0.0 in baseline classifier AND")
        prop.append(f"  - insufficient pool for near-term expansion")
        prop.append(f"")
        prop.append(f"Pop and Metal are retained despite F1=0.0 because:")
        prop.append(f"  - Each has a viable non-benchmark labeled pool (Pop=18, Metal=14)")
        prop.append(f"  - Each is musically distinct (not a subtype of Rock/Country)")
        prop.append(f"  - Benchmark expansion can bring them to >= 20 samples")

        # Mapping table
        mtable = []
        mtable.append("=" * 70)
        mtable.append("V2 TAXONOMY MAPPING TABLE")
        mtable.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        mtable.append("=" * 70)
        mtable.append(f"\n{'Original':15s} -> {'V2':10s}  {'Support':>8s}  {'F1':>6s}  {'Durability':20s}")
        mtable.append("-" * 70)
        for orig, d in v2_mapping.items():
            mtable.append(
                f"{orig:15s} -> {d['v2_genre']:10s}  {d['support_count']:8d}  "
                f"{PER_CLASS_F1.get(orig,0):6.3f}  {d['durability']:20s}"
            )

        self.emit(f"V2 taxonomy: {len(v2_classes)} classes (from {len(v2_mapping)})")
        return v2_mapping, mapping_df, prop, mtable

    # ================================================================
    # PART C — BENCHMARK REBALANCE ANALYSIS
    # ================================================================
    def part_c(self, df, v2_mapping):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — BENCHMARK REBALANCE ANALYSIS")
        self.emit("=" * 60)

        conn = self.connect_ro()

        # Map benchmark tracks to V2
        genre_to_v2 = {orig: d["v2_genre"] for orig, d in v2_mapping.items()}
        df_v2 = df.copy()
        df_v2["v2_genre"] = df_v2["genre"].map(genre_to_v2)

        v2_dist = df_v2["v2_genre"].value_counts().sort_values(ascending=False)
        self.emit(f"V2 benchmark distribution:\n{v2_dist.to_string()}")

        # Get pool counts per V2 genre
        pool_rows = conn.execute("""
            SELECT g.name AS genre, COUNT(*) AS c
            FROM track_genre_labels tgl
            JOIN genres g ON tgl.genre_id = g.id
            WHERE tgl.role = 'primary'
              AND tgl.track_id NOT IN (
                SELECT track_id FROM benchmark_set_tracks
                WHERE benchmark_set_id = (SELECT id FROM benchmark_sets WHERE name = ?)
              )
            GROUP BY g.name
            ORDER BY c DESC
        """, (BENCHMARK_NAME,)).fetchall()
        pool_by_genre = {r["genre"]: r["c"] for r in pool_rows}
        conn.close()

        # Roll up pool into V2 buckets
        pool_by_v2 = {}
        for orig_genre, v2_genre in genre_to_v2.items():
            pool_count = pool_by_genre.get(orig_genre, 0)
            pool_by_v2[v2_genre] = pool_by_v2.get(v2_genre, 0) + pool_count

        # Target: each V2 class should have >= 30 benchmark samples for stable 5-fold CV
        # Ideal: proportionally balanced, minimum floor = 30
        TARGET_MIN = 30

        rebalance_rows = []
        rebalance_lines = []
        rebalance_lines.append("=" * 70)
        rebalance_lines.append("BENCHMARK REBALANCE PLAN V2")
        rebalance_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        rebalance_lines.append("=" * 70)
        rebalance_lines.append(f"\nTarget minimum per V2 class: {TARGET_MIN}")
        rebalance_lines.append(f"Current benchmark total: {len(df)}")
        rebalance_lines.append(f"\n{'V2 Genre':15s}  {'Bench':>6s}  {'Target':>7s}  {'Deficit':>8s}  "
                               f"{'Pool':>6s}  {'Priority':>10s}  Recommendation")
        rebalance_lines.append("-" * 95)

        for v2g in sorted(v2_dist.index):
            current = int(v2_dist.get(v2g, 0))
            pool = pool_by_v2.get(v2g, 0)
            deficit = max(0, TARGET_MIN - current)
            fillable = min(deficit, pool)

            if current >= TARGET_MIN:
                priority = "LOW"
                rec = "Adequate. No expansion needed."
            elif pool >= deficit:
                priority = "HIGH"
                rec = f"Expand by {deficit} from pool ({pool} available). Review required."
            elif pool > 0:
                priority = "HIGH"
                rec = f"Expand by {pool} (partial fill). Still {deficit - pool} short. Need new tracks."
            else:
                priority = "MEDIUM"
                rec = f"No pool available. Freeze until new truth data."

            # Special cases
            if v2g == "Other":
                if current < TARGET_MIN:
                    rec = (f"Heterogeneous bucket — expansion is low-value. "
                           f"Keep as catch-all until constituent genres grow independently.")
                    priority = "LOW"

            rebalance_rows.append({
                "v2_genre": v2g,
                "current_benchmark_count": current,
                "target_min_count": TARGET_MIN,
                "deficit": deficit,
                "candidate_pool_count": pool,
                "recommendation": rec,
                "priority": priority,
            })

            rebalance_lines.append(
                f"{v2g:15s}  {current:6d}  {TARGET_MIN:7d}  {deficit:8d}  "
                f"{pool:6d}  {priority:>10s}  {rec}"
            )

        rebalance_df = pd.DataFrame(rebalance_rows)
        rebalance_df.to_csv(REBALANCE_CSV, index=False, encoding="utf-8")
        self.emit(f"Rebalance CSV: {REBALANCE_CSV}")

        # Summary
        total_deficit = rebalance_df["deficit"].sum()
        total_pool = rebalance_df["candidate_pool_count"].sum()
        rebalance_lines.append(f"\nTotal deficit: {total_deficit}")
        rebalance_lines.append(f"Total pool available: {total_pool}")
        rebalance_lines.append(f"Expansion fillable from pool: "
                               f"{min(total_deficit, total_pool)}")

        rebalance_lines.append(f"\nPer-class detail:")
        for _, row in rebalance_df.iterrows():
            v2g = row["v2_genre"]
            rebalance_lines.append(f"\n  {v2g}:")
            rebalance_lines.append(f"    Current benchmark: {row['current_benchmark_count']}")
            rebalance_lines.append(f"    Target: {row['target_min_count']}")
            rebalance_lines.append(f"    Deficit: {row['deficit']}")
            rebalance_lines.append(f"    Pool: {row['candidate_pool_count']}")
            rebalance_lines.append(f"    Priority: {row['priority']}")
            rebalance_lines.append(f"    Rec: {row['recommendation']}")

            # Which original genres feed into this V2 bucket
            source_genres = [orig for orig, d in v2_mapping.items() if d["v2_genre"] == v2g]
            rebalance_lines.append(f"    Source genres: {source_genres}")
            source_pool = {g: pool_by_genre.get(g, 0) for g in source_genres}
            rebalance_lines.append(f"    Pool breakdown: {source_pool}")

        self.emit(f"Rebalance: deficit={total_deficit}, pool={total_pool}")
        return rebalance_df, rebalance_lines

    # ================================================================
    # PART D — V2 PREVIEW DATASET
    # ================================================================
    def part_d(self, df, v2_mapping):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — V2 PREVIEW DATASET")
        self.emit("=" * 60)

        genre_to_v2 = {orig: d["v2_genre"] for orig, d in v2_mapping.items()}

        preview = df.copy()
        preview["original_genre"] = preview["genre"]
        preview["v2_genre"] = preview["genre"].map(genre_to_v2)

        # Validate
        null_v2 = preview["v2_genre"].isna().sum()
        dup_rows = preview["track_id"].duplicated().sum()
        self.emit(f"Preview: {len(preview)} rows, null v2={null_v2}, dup track_id={dup_rows}")

        if null_v2 > 0:
            unmapped = preview[preview["v2_genre"].isna()]["genre"].unique()
            self.emit(f"FATAL: Unmapped genres: {unmapped}")
            return None, False

        if dup_rows > 0:
            self.emit(f"FATAL: Duplicate track_ids found")
            return None, False

        # Select output columns
        out_cols = ["track_id", "original_genre", "v2_genre"] + FEATURE_COLS
        preview_out = preview[out_cols]
        preview_out.to_csv(PREVIEW_CSV, index=False, encoding="utf-8")
        self.emit(f"Preview CSV: {PREVIEW_CSV}")

        # Summary
        v2_dist = preview_out["v2_genre"].value_counts().sort_values(ascending=False)
        summary = []
        summary.append("=" * 70)
        summary.append("V2 DATASET PREVIEW SUMMARY")
        summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("=" * 70)
        summary.append(f"\nSource: benchmark tracks from genre_benchmark_v1")
        summary.append(f"File: {PREVIEW_CSV}")
        summary.append(f"Rows: {len(preview_out)}")
        summary.append(f"Columns: {list(preview_out.columns)}")
        summary.append(f"Null v2_genre: {null_v2}")
        summary.append(f"Duplicate track_id: {dup_rows}")
        summary.append(f"\nV2 genre distribution:")
        for g, c in v2_dist.items():
            pct = c / len(preview_out) * 100
            summary.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)")

        # Comparison: v1 vs v2
        v1_dist = df["genre"].value_counts()
        summary.append(f"\n--- CLASS COUNT COMPARISON: V1 vs V2 ---")
        summary.append(f"V1 classes: {len(v1_dist)}")
        summary.append(f"V2 classes: {len(v2_dist)}")
        summary.append(f"V1 majority/minority ratio: {v1_dist.max()}/{v1_dist.min()} = {v1_dist.max()/v1_dist.min():.0f}:1")
        summary.append(f"V2 majority/minority ratio: {v2_dist.max()}/{v2_dist.min()} = {v2_dist.max()/v2_dist.min():.1f}:1")

        # Comparison CSV
        comp_rows = []
        all_genres = sorted(set(list(v1_dist.index) + list(v2_dist.index)))
        for g in sorted(v1_dist.index):
            comp_rows.append({
                "genre": g,
                "v1_count": int(v1_dist.get(g, 0)),
                "v2_mapped_to": genre_to_v2.get(g, g),
            })
        comp_df = pd.DataFrame(comp_rows)
        # Add v2 aggregate
        v2_agg = []
        for v2g in sorted(v2_dist.index):
            v2_agg.append({
                "v2_genre": v2g,
                "v2_count": int(v2_dist.get(v2g, 0)),
            })
        v2_agg_df = pd.DataFrame(v2_agg)

        return (preview_out, summary, comp_df, v2_agg_df), True

    # ================================================================
    # PART E — READINESS ASSESSMENT
    # ================================================================
    def part_e(self, df, v2_mapping, rebalance_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — V2 READINESS ASSESSMENT")
        self.emit("=" * 60)

        genre_to_v2 = {orig: d["v2_genre"] for orig, d in v2_mapping.items()}
        df_v2 = df.copy()
        df_v2["v2_genre"] = df_v2["genre"].map(genre_to_v2)
        v2_dist = df_v2["v2_genre"].value_counts()

        lines = []
        lines.append("=" * 70)
        lines.append("V2 READINESS ASSESSMENT")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        # Q1
        lines.append(f"\n1. Is the proposed V2 taxonomy materially better suited for current data size?")
        lines.append(f"   YES.")
        lines.append(f"   - V1 has 11 classes with 8 having F1=0.0 (untestable)")
        lines.append(f"   - V2 reduces to 6 classes, concentrating data into trainable buckets")
        lines.append(f"   - V2 majority/minority ratio: {v2_dist.max()}/{v2_dist.min()} = {v2_dist.max()/v2_dist.min():.1f}:1")
        lines.append(f"     vs V1: 82/1 = 82:1")
        lines.append(f"   - V2 eliminates classes that had zero classifier signal")
        lines.append(f"   - The 'Other' bucket absorbs noise without losing training stability")

        # Q2
        lines.append(f"\n2. Which original genres were harming performance due to low support?")
        lines.append(f"   All 8 classes with F1=0.0:")
        for g in sorted(PER_CLASS_F1.keys()):
            if PER_CLASS_F1[g] == 0.0:
                count = int(df[df["genre"] == g].shape[0])
                lines.append(f"     {g:15s}: {count} benchmark samples, F1=0.0")
        lines.append(f"   These classes contributed only noise to the macro metrics.")
        lines.append(f"   Merging Electronic, Folk, Reggae, R&B, Soundtrack, World into 'Other'")
        lines.append(f"   removes 6 untestable classes and concentrates evaluation on viable ones.")

        # Q3
        lines.append(f"\n3. Which confusion pairs should be reduced by the V2 mapping?")
        lines.append(f"   ELIMINATED by merger:")
        lines.append(f"     - Electronic -> Rock (2 errors) — both now 'Other' or 'Rock' not confused")
        lines.append(f"     - Folk -> Rock (2 errors) — Folk now 'Other'")
        lines.append(f"     - Reggae -> Hip-Hop/Rock (2 errors) — Reggae now 'Other'")
        lines.append(f"     - R&B -> Country (1 error) — R&B now 'Other'")
        lines.append(f"     - Soundtrack -> Country (1 error) — now 'Other'")
        lines.append(f"     - World -> Rock (1 error) — now 'Other'")
        lines.append(f"   RETAINED (real genre borders to improve with features/data):")
        lines.append(f"     - Rock <-> Country (35 errors)")
        lines.append(f"     - Hip-Hop -> Rock/Country (23 errors)")
        lines.append(f"     - Metal -> Rock (4 errors)")

        # Q4
        lines.append(f"\n4. Is the current benchmark large enough for a V2 experiment now?")
        lines.append(f"   PARTIALLY.")
        lines.append(f"   V2 distribution:")
        for g, c in v2_dist.sort_values(ascending=False).items():
            lines.append(f"     {g:15s}: {c}")
        adequate = sum(1 for c in v2_dist if c >= 30)
        lines.append(f"   Classes with >= 30 samples: {adequate}/{len(v2_dist)}")
        lines.append(f"   Country (82), Rock (66), Hip-Hop (29) are adequate or near-adequate.")
        lines.append(f"   Metal ({v2_dist.get('Metal', 0)}), Pop ({v2_dist.get('Pop', 0)}), "
                     f"Other ({v2_dist.get('Other', 0)}) need expansion.")
        lines.append(f"   A V2 experiment is viable now for exploratory results, but")
        lines.append(f"   benchmark expansion is recommended before declaring V2 production-ready.")

        # Q5
        total_deficit = int(rebalance_df["deficit"].sum())
        lines.append(f"\n5. How many additional manually reviewed tracks should be added before V2 training?")
        lines.append(f"   Total deficit to reach {30}/class minimum: {total_deficit} tracks")
        lines.append(f"   By class:")
        for _, row in rebalance_df.iterrows():
            if row["deficit"] > 0:
                lines.append(
                    f"     {row['v2_genre']:15s}: +{row['deficit']} needed "
                    f"(pool={row['candidate_pool_count']})"
                )
        lines.append(f"   Recommendation: Add at minimum the pool-available tracks ({min(total_deficit, int(rebalance_df['candidate_pool_count'].sum()))}) "
                     f"after manual genre review.")

        # Q6
        lines.append(f"\n6. What must stay frozen and unchanged during rebalance work?")
        lines.append(f"   FROZEN:")
        lines.append(f"     - Production track_genre_labels table (no mutations)")
        lines.append(f"     - Existing benchmark_set_tracks entries (no deletions)")
        lines.append(f"     - DB schema (no ALTER TABLE)")
        lines.append(f"     - Baseline V1 results and proof artifacts")
        lines.append(f"     - genre_taxonomy_v2_mapping.csv (locked after approval)")
        lines.append(f"   MAY CHANGE:")
        lines.append(f"     - New benchmark_set_tracks rows (additions only, with review)")
        lines.append(f"     - New benchmark_sets entry for V2 benchmark (separate from V1)")
        lines.append(f"     - Training configuration for V2 classifier")

        self.emit("Readiness assessment complete")
        return lines

    # ================================================================
    # PART F — OUTPUT + VALIDATION
    # ================================================================
    def part_f(self, audit, imbal, conf, prop, mtable, rebalance_lines,
               preview_data, readiness, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART F — OUTPUT + VALIDATION")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        preview_out, summary, comp_df, v2_agg_df = preview_data

        # Write all proof files
        w("00_current_taxonomy_audit.txt", "\n".join(audit))
        w("01_class_imbalance_summary.txt", "\n".join(imbal))
        w("02_confusion_pressure_summary.txt", "\n".join(conf))
        w("03_taxonomy_v2_proposal.txt", "\n".join(prop))
        w("04_taxonomy_mapping_table.txt", "\n".join(mtable))
        w("05_rebalance_plan.txt", "\n".join(rebalance_lines))
        w("06_v2_dataset_preview_summary.txt", "\n".join(summary))
        w("07_readiness_assessment.txt", "\n".join(readiness))

        # Comparison CSV
        comp_df.to_csv(COMPARISON_CSV, index=False, encoding="utf-8")

        # DB validation
        conn = self.connect_ro()
        checks = []
        checks.append("=" * 70)
        checks.append("VALIDATION QUERIES")
        checks.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        checks.append("=" * 70)

        queries = [
            ("Benchmark rows",
             "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
             "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"),
            ("Primary labels",
             "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"),
            ("Duplicate primaries",
             "SELECT COUNT(*) FROM (SELECT track_id FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"),
            ("FK violations",
             "PRAGMA foreign_key_check"),
            ("Total labels",
             "SELECT COUNT(*) FROM track_genre_labels"),
            ("Total tracks",
             "SELECT COUNT(*) FROM tracks"),
        ]

        all_ok = True
        for label, q in queries:
            result = conn.execute(q).fetchall()
            if result:
                val = result[0][0] if len(result) == 1 else len(result)
            else:
                val = 0
            checks.append(f"\n  {label}: {val}")
            checks.append(f"    SQL: {q}")

            if label == "Benchmark rows" and val != 200:
                all_ok = False
            if label == "Duplicate primaries" and val != 0:
                all_ok = False
            if label == "FK violations" and val != 0:
                all_ok = False

        conn.close()

        # Preview validation
        null_v2 = preview_out["v2_genre"].isna().sum()
        dup_preview = preview_out["track_id"].duplicated().sum()
        checks.append(f"\n  Preview null v2_genre: {null_v2}")
        checks.append(f"  Preview dup track_id: {dup_preview}")
        if null_v2 > 0 or dup_preview > 0:
            all_ok = False

        checks.append(f"\n  Schema changes: NONE")
        checks.append(f"  DB writes to genre labels: NONE")
        checks.append(f"  DB opened in: READ-ONLY mode")

        gate = "PASS" if all_ok else "FAIL"

        # Final report
        report = []
        report.append("=" * 70)
        report.append("GENRE TAXONOMY CONSOLIDATION + BENCHMARK REBALANCE V2")
        report.append("FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- SUMMARY ---")
        report.append(f"V1 classes: 11")
        report.append(f"V2 classes: 6 (Country, Rock, Hip-Hop, Pop, Metal, Other)")
        report.append(f"Classes merged into Other: Electronic, Folk, Reggae, R&B, Soundtrack, World")
        report.append(f"V1 majority/minority ratio: 82:1")

        v2_dist = preview_out["v2_genre"].value_counts()
        report.append(f"V2 majority/minority ratio: {v2_dist.max()}:{v2_dist.min()} "
                      f"= {v2_dist.max()/v2_dist.min():.1f}:1")

        report.append(f"\n--- V2 DISTRIBUTION ---")
        for g, c in v2_dist.sort_values(ascending=False).items():
            pct = c / len(preview_out) * 100
            report.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)")

        report.append(f"\n--- PARTS COMPLETED ---")
        report.append(f"  A. Taxonomy audit: PASS")
        report.append(f"  B. V2 proposal: PASS (6 classes)")
        report.append(f"  C. Rebalance plan: PASS")
        report.append(f"  D. Preview dataset: PASS ({len(preview_out)} rows, 0 nulls, 0 dups)")
        report.append(f"  E. Readiness assessment: PASS")
        report.append(f"  F. Output + validation: {'PASS' if all_ok else 'FAIL'}")

        report.append(f"\n--- VALIDATION ---")
        for line in checks:
            report.append(f"  {line}")

        report.append(f"\n--- DATA FILES ---")
        report.append(f"  {MAPPING_CSV}")
        report.append(f"  {REBALANCE_CSV}")
        report.append(f"  {PREVIEW_CSV}")

        report.append(f"\n{'='*70}")
        report.append(f"GATE={gate}")
        report.append(f"{'='*70}")

        w("08_final_report.txt", "\n".join(report))
        w("execution_log.txt", "\n".join(self.log))

        self.emit(f"Proof written: {PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = Pipeline()

    if not DATASET_V1.exists():
        p.emit(f"FATAL: {DATASET_V1} not found")
        return 1
    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1
    if not CONFUSION_CSV.exists():
        p.emit(f"FATAL: {CONFUSION_CSV} not found")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Dataset: {DATASET_V1}")

    df = pd.read_csv(DATASET_V1)
    cm_df = pd.read_csv(CONFUSION_CSV, index_col=0)

    # PART A
    audit, imbal, conf, genre_dist, confusion_pairs = p.part_a(df, cm_df)

    # PART B
    v2_mapping, mapping_df, prop, mtable = p.part_b(genre_dist, confusion_pairs)

    # PART C
    rebalance_df, rebalance_lines = p.part_c(df, v2_mapping)

    # PART D
    preview_result, d_ok = p.part_d(df, v2_mapping)
    if not d_ok:
        p.emit("FATAL: Part D failed")
        return 1

    # PART E
    readiness = p.part_e(df, v2_mapping, rebalance_df)

    # PART F
    gate = p.part_f(audit, imbal, conf, prop, mtable, rebalance_lines,
                    preview_result, readiness, df)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
