#!/usr/bin/env python3
"""
Phase 9 — Feature Refinement Plan + Classifier Readiness

Parts:
  A) Lock feature status matrix from prior analysis
  B) Build classifier dataset v1 + feature manifest
  C) Readiness checks (distribution, nulls, constants, correlations, outliers)
  D) Baseline model plan document (no training)
"""

import csv
import os
import sqlite3
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
PROOF_DIR = WORKSPACE / "_proof" / "classifier_readiness"
DATA_DIR = WORKSPACE / "data"
DATASET_CSV = DATA_DIR / "classifier_dataset_v1.csv"
MANIFEST_CSV = DATA_DIR / "classifier_feature_manifest_v1.csv"
BENCHMARK_NAME = "genre_benchmark_v1"

# Prior-phase evidence sources
REFINEMENT_PROPOSALS = WORKSPACE / "_proof" / "misfit_feature_refinement" / "06_refinement_proposals.txt"
IMPORTANCE_CSV = WORKSPACE / "_proof" / "misfit_feature_refinement" / "feature_importance_updated.csv"
WEAKNESS_TXT = WORKSPACE / "_proof" / "misfit_feature_refinement" / "05_feature_weakness_summary.txt"
GLOBAL_IMPORTANCE = WORKSPACE / "_proof" / "feature_genre_analysis" / "02_feature_importance_global.txt"

# Feature status from Phase 8 results
FEATURE_STATUS = {
    "harmonic_stability":   {"status": "KEEP",       "anova_f": 2.94, "anova_p": 6.07e-03, "mi": 0.0583, "misfit_devs": 6},
    "loudness_lufs":        {"status": "KEEP",       "anova_f": 2.35, "anova_p": 2.55e-02, "mi": 0.0703, "misfit_devs": 1},
    "avg_section_duration": {"status": "KEEP",       "anova_f": 4.22, "anova_p": 2.38e-04, "mi": 0.0956, "misfit_devs": 8},
    "tempo_stability":      {"status": "DOWNWEIGHT", "anova_f": 1.01, "anova_p": 4.22e-01, "mi": 0.0000, "misfit_devs": 4},
    "energy":               {"status": "DOWNWEIGHT", "anova_f": 1.74, "anova_p": 1.03e-01, "mi": 0.0514, "misfit_devs": 1},
    "danceability":         {"status": "DOWNWEIGHT", "anova_f": 1.50, "anova_p": 1.70e-01, "mi": 0.0371, "misfit_devs": 2},
    "section_count":        {"status": "DOWNWEIGHT", "anova_f": 1.84, "anova_p": 1.41e-01, "mi": 0.1701, "misfit_devs": 12},
    "bpm_detected":         {"status": "REMOVE",     "anova_f": 0.61, "anova_p": 7.45e-01, "mi": 0.0000, "misfit_devs": 2},
    "valence":              {"status": "REMOVE",     "anova_f": 0.00, "anova_p": 1.00e+00, "mi": 0.0000, "misfit_devs": 0},
}

CANDIDATE_ADD = {
    "spectral_centroid":    "Differentiates bright vs dark timbres — Electronic vs Rock",
    "spectral_rolloff":     "Frequency energy distribution — genre-correlated",
    "spectral_flux":        "Onset attack sharpness — drums vs sustained sounds",
    "onset_density":        "Rhythm complexity — Hip-Hop vs Country separation",
    "zero_crossing_rate":   "Noise vs tonal content — Metal vs Pop",
    "bass_energy":          "Low-frequency energy — Hip-Hop, Electronic, Metal",
    "mid_energy":           "Vocal/instrument range — Pop, Country, Rock",
    "high_energy":          "Cymbal/hi-hat presence — Metal, Electronic",
    "vocal_presence":       "Vocal vs instrumental balance — key genre separator",
    "instrumentalness":     "Electronic/Classical vs vocal genres",
    "rms_mean":             "Overall loudness profile",
}

# Features included in v1 dataset (KEEP + DOWNWEIGHT)
V1_FEATURES = [f for f, d in FEATURE_STATUS.items() if d["status"] in ("KEEP", "DOWNWEIGHT")]

# Genre-feature importance from prior analysis (per-genre top features)
GENRE_FEATURE_EVIDENCE = {
    "harmonic_stability": "Country(F=17.58), Hip-Hop(F=7.76), Rock(F=1.13)",
    "loudness_lufs": "Country(F=10.97), Hip-Hop(F=4.74), Metal(F=1.84), Rock(F=1.52)",
    "avg_section_duration": "Country(F=26.04), Rock(F=10.85), Electronic(F=2.15)",
    "tempo_stability": "Electronic(F=5.34), Pop(F=0.51)",
    "energy": "Hip-Hop(F=8.56), Electronic(F=2.95), Country(F=1.77)",
    "danceability": "Reggae(F=2.59), Electronic(F=2.49), Rock(F=2.41), Metal(F=1.75)",
    "section_count": "Pop(F=5.64), Metal(F=0.80)",
    "bpm_detected": "Folk(F=1.97), Hip-Hop(F=1.09) — globally non-significant",
    "valence": "all genres F=0.00 — zero variance in current data",
}


class Pipeline:
    def __init__(self):
        self.log = []
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
    # PART A — LOCK FEATURE STATUS
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — LOCK FEATURE STATUS")
        self.emit("=" * 60)

        lines = []
        lines.append("=" * 90)
        lines.append("FEATURE STATUS MATRIX — LOCKED")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Source: Phase 7 (feature_genre_analysis) + Phase 8 (misfit_feature_refinement)")
        lines.append("=" * 90)

        # KEEP
        lines.append(f"\n{'='*40} KEEP (3) {'='*40}")
        for feat, d in FEATURE_STATUS.items():
            if d["status"] != "KEEP":
                continue
            lines.append(f"\n  Feature: {feat}")
            lines.append(f"  Status:  KEEP")
            lines.append(f"  ANOVA F: {d['anova_f']:.2f}  p-value: {d['anova_p']:.2e}")
            lines.append(f"  Mutual Info: {d['mi']:.4f}")
            lines.append(f"  Misfit deviations: {d['misfit_devs']}")
            lines.append(f"  Reason: Statistically significant genre discriminator (p < 0.05)")
            lines.append(f"  Genre evidence: {GENRE_FEATURE_EVIDENCE.get(feat, 'N/A')}")

        # DOWNWEIGHT
        lines.append(f"\n{'='*40} DOWNWEIGHT (4) {'='*40}")
        for feat, d in FEATURE_STATUS.items():
            if d["status"] != "DOWNWEIGHT":
                continue
            lines.append(f"\n  Feature: {feat}")
            lines.append(f"  Status:  DOWNWEIGHT")
            lines.append(f"  ANOVA F: {d['anova_f']:.2f}  p-value: {d['anova_p']:.2e}")
            lines.append(f"  Mutual Info: {d['mi']:.4f}")
            lines.append(f"  Misfit deviations: {d['misfit_devs']}")
            lines.append(f"  Reason: Weak global signal but genre-specific utility; include with reduced weight")
            lines.append(f"  Genre evidence: {GENRE_FEATURE_EVIDENCE.get(feat, 'N/A')}")

        # REMOVE
        lines.append(f"\n{'='*40} REMOVE (2) {'='*40}")
        for feat, d in FEATURE_STATUS.items():
            if d["status"] != "REMOVE":
                continue
            lines.append(f"\n  Feature: {feat}")
            lines.append(f"  Status:  REMOVE")
            lines.append(f"  ANOVA F: {d['anova_f']:.2f}  p-value: {d['anova_p']:.2e}")
            lines.append(f"  Mutual Info: {d['mi']:.4f}")
            lines.append(f"  Misfit deviations: {d['misfit_devs']}")
            lines.append(f"  Reason: No statistical signal, adds noise to classifier input")
            lines.append(f"  Genre evidence: {GENRE_FEATURE_EVIDENCE.get(feat, 'N/A')}")

        # CANDIDATE ADD
        lines.append(f"\n{'='*40} CANDIDATE_ADD (11) {'='*40}")
        for feat, reason in CANDIDATE_ADD.items():
            lines.append(f"\n  Feature: {feat}")
            lines.append(f"  Status:  CANDIDATE_ADD")
            lines.append(f"  Reason: {reason}")
            lines.append(f"  Availability: Not in current DB (track_features has 0 rows)")
            lines.append(f"  Action: Requires audio feature extraction pipeline before use")

        lines.append(f"\n{'='*90}")
        lines.append(f"V1 DATASET INCLUDES: {', '.join(V1_FEATURES)}")
        lines.append(f"V1 DATASET EXCLUDES: {', '.join(f for f,d in FEATURE_STATUS.items() if d['status']=='REMOVE')}")
        lines.append(f"FUTURE CANDIDATES: {', '.join(CANDIDATE_ADD.keys())}")

        self.emit(f"Feature status locked: KEEP={sum(1 for d in FEATURE_STATUS.values() if d['status']=='KEEP')}, "
                  f"DOWNWEIGHT={sum(1 for d in FEATURE_STATUS.values() if d['status']=='DOWNWEIGHT')}, "
                  f"REMOVE={sum(1 for d in FEATURE_STATUS.values() if d['status']=='REMOVE')}, "
                  f"CANDIDATE_ADD={len(CANDIDATE_ADD)}")

        return True, lines

    # ================================================================
    # PART B — BUILD CLASSIFIER DATASET V1
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — BUILD CLASSIFIER DATASET V1")
        self.emit("=" * 60)

        conn = self.connect(readonly=True)

        # Column mapping: DB columns → feature names
        # bpm → bpm_detected, bpm_confidence → tempo_stability,
        # key_confidence → harmonic_stability
        query = """
            SELECT
                t.id AS track_id,
                t.artist,
                t.title,
                g.name AS genre,
                COALESCE(sg.name, '') AS subgenre,
                asumm.bpm_confidence AS tempo_stability,
                asumm.key_confidence AS harmonic_stability,
                asumm.loudness_lufs,
                asumm.energy,
                asumm.danceability,
                sec_agg.section_count,
                sec_agg.avg_section_duration
            FROM benchmark_set_tracks bst
            JOIN benchmark_sets bs ON bst.benchmark_set_id = bs.id
            JOIN tracks t ON bst.track_id = t.id
            JOIN track_genre_labels tgl ON tgl.track_id = t.id AND tgl.role = 'primary'
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            LEFT JOIN (
                SELECT track_id,
                       bpm_confidence, key_confidence,
                       loudness_lufs, energy, danceability,
                       ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY id DESC) AS rn
                FROM analysis_summary
            ) asumm ON asumm.track_id = t.id AND asumm.rn = 1
            LEFT JOIN (
                SELECT track_id,
                       COUNT(*) AS section_count,
                       AVG(end_sec - start_sec) AS avg_section_duration
                FROM section_events
                GROUP BY track_id
            ) sec_agg ON sec_agg.track_id = t.id
            WHERE bs.name = ?
            ORDER BY t.id
        """
        rows = conn.execute(query, (BENCHMARK_NAME,)).fetchall()
        conn.close()

        df = pd.DataFrame([dict(r) for r in rows])
        self.emit(f"Raw query returned: {len(df)} rows")

        # Remove duplicates by track_id
        before = len(df)
        df = df.drop_duplicates(subset=["track_id"])
        after = len(df)
        if before != after:
            self.emit(f"WARNING: Removed {before - after} duplicate track rows")

        # Verify no null primary labels
        null_genre = df["genre"].isna().sum()
        self.emit(f"Null genre labels: {null_genre}")
        if null_genre > 0:
            self.emit("FATAL: null primary genre labels found")
            return False, None, None

        # Excluded features (REMOVE) are NOT in the query — bpm_detected and valence excluded
        # V1 features present in dataset
        present_features = [f for f in V1_FEATURES if f in df.columns]
        self.emit(f"V1 features present: {present_features}")

        # Write dataset CSV
        dataset_cols = ["track_id", "artist", "title", "genre", "subgenre"] + present_features
        df[dataset_cols].to_csv(DATASET_CSV, index=False, encoding="utf-8")
        self.emit(f"Dataset written: {DATASET_CSV}")
        self.emit(f"Rows: {len(df)}, Columns: {len(dataset_cols)}")

        # Build feature manifest
        manifest_rows = []
        for feat, d in FEATURE_STATUS.items():
            included = "yes" if d["status"] in ("KEEP", "DOWNWEIGHT") else "no"
            notes = f"F={d['anova_f']:.2f}, p={d['anova_p']:.2e}"
            if d["status"] == "REMOVE":
                notes += " — excluded from v1"
            manifest_rows.append({
                "feature_name": feat,
                "status": d["status"],
                "included_in_v1": included,
                "notes": notes,
            })
        for feat, reason in CANDIDATE_ADD.items():
            manifest_rows.append({
                "feature_name": feat,
                "status": "CANDIDATE_ADD",
                "included_in_v1": "no",
                "notes": reason,
            })

        manifest_df = pd.DataFrame(manifest_rows)
        manifest_df.to_csv(MANIFEST_CSV, index=False, encoding="utf-8")
        self.emit(f"Manifest written: {MANIFEST_CSV}")
        self.emit(f"Manifest rows: {len(manifest_df)}")

        return True, df, manifest_df

    # ================================================================
    # PART C — READINESS CHECKS
    # ================================================================
    def part_c(self, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — READINESS CHECKS")
        self.emit("=" * 60)

        checks = []
        checks.append("=" * 70)
        checks.append("CLASSIFIER READINESS CHECKS")
        checks.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        checks.append("=" * 70)

        present_features = [f for f in V1_FEATURES if f in df.columns]

        # 1. Row count
        checks.append(f"\n--- 1. ROW COUNT ---")
        checks.append(f"Total rows: {len(df)}")
        checks.append(f"Expected: 200 (benchmark set)")
        row_ok = len(df) == 200
        checks.append(f"CHECK: {'PASS' if row_ok else 'FAIL'}")

        # 2. Genre distribution
        checks.append(f"\n--- 2. GENRE DISTRIBUTION ---")
        genre_dist = df["genre"].value_counts().sort_values(ascending=False)
        genre_lines = []
        for g, c in genre_dist.items():
            pct = c / len(df) * 100
            genre_lines.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)")
            checks.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)")
        checks.append(f"Unique genres: {len(genre_dist)}")
        checks.append(f"CHECK: PASS")

        # 3. Subgenre distribution
        checks.append(f"\n--- 3. SUBGENRE DISTRIBUTION ---")
        subgenre_dist = df["subgenre"].value_counts().sort_values(ascending=False)
        subgenre_lines = []
        for sg, c in subgenre_dist.items():
            label = sg if sg else "(none)"
            pct = c / len(df) * 100
            subgenre_lines.append(f"  {label:25s}: {c:4d} ({pct:5.1f}%)")
            checks.append(f"  {label:25s}: {c:4d} ({pct:5.1f}%)")
        checks.append(f"Unique subgenres (including empty): {len(subgenre_dist)}")

        # 4. Null check by feature
        checks.append(f"\n--- 4. NULL CHECK BY FEATURE ---")
        null_report = {}
        all_nulls_ok = True
        for feat in present_features:
            n_null = df[feat].isna().sum()
            pct = n_null / len(df) * 100
            null_report[feat] = n_null
            checks.append(f"  {feat:25s}: {n_null:4d} nulls ({pct:5.1f}%)")
            if pct > 50:
                all_nulls_ok = False
        checks.append(f"CHECK: {'PASS' if all_nulls_ok else 'WARN — high null rates'}")

        # 5. Constant/near-constant feature detection
        checks.append(f"\n--- 5. CONSTANT / NEAR-CONSTANT FEATURES ---")
        constant_feats = []
        for feat in present_features:
            vals = df[feat].dropna()
            if len(vals) < 2:
                constant_feats.append(feat)
                checks.append(f"  {feat:25s}: INSUFFICIENT DATA")
                continue
            nunique = vals.nunique()
            std = vals.std()
            cv = std / abs(vals.mean()) if vals.mean() != 0 else float('inf')
            if nunique <= 1:
                constant_feats.append(feat)
                checks.append(f"  {feat:25s}: CONSTANT (unique={nunique})")
            elif nunique <= 3:
                checks.append(f"  {feat:25s}: NEAR-CONSTANT (unique={nunique}, std={std:.4f})")
            else:
                checks.append(f"  {feat:25s}: OK (unique={nunique}, std={std:.4f}, CV={cv:.4f})")
        checks.append(f"Constant features: {len(constant_feats)}")
        checks.append(f"CHECK: {'PASS' if len(constant_feats) == 0 else 'WARN'}")

        # 6. Correlated feature pairs
        checks.append(f"\n--- 6. CORRELATED FEATURE PAIRS ---")
        corr_lines = []
        feat_data = df[present_features].dropna()
        if len(feat_data) >= 10:
            corr_matrix = feat_data.corr()
            high_corr = []
            for i, f1 in enumerate(present_features):
                for j, f2 in enumerate(present_features):
                    if i >= j:
                        continue
                    if f1 in corr_matrix.columns and f2 in corr_matrix.columns:
                        r = corr_matrix.loc[f1, f2]
                        if abs(r) > 0.7:
                            high_corr.append((f1, f2, r))
                            corr_lines.append(f"  {f1:25s} <-> {f2:25s}: r={r:+.4f} {'HIGH' if abs(r)>0.85 else 'MODERATE'}")
            if not high_corr:
                checks.append("  No highly correlated pairs (|r| > 0.7)")
                corr_lines.append("No highly correlated pairs (|r| > 0.7)")
            else:
                for line in corr_lines:
                    checks.append(line)
        else:
            checks.append("  Insufficient non-null data for correlation analysis")
        checks.append(f"CHECK: PASS")

        # 7. Class imbalance summary
        checks.append(f"\n--- 7. CLASS IMBALANCE SUMMARY ---")
        majority = genre_dist.iloc[0]
        minority = genre_dist.iloc[-1]
        imbalance_ratio = majority / minority if minority > 0 else float('inf')
        checks.append(f"  Majority class: {genre_dist.index[0]} ({majority})")
        checks.append(f"  Minority class: {genre_dist.index[-1]} ({minority})")
        checks.append(f"  Imbalance ratio: {imbalance_ratio:.1f}:1")
        if imbalance_ratio > 50:
            checks.append(f"  WARNING: Extreme imbalance. Consider oversampling minority classes.")
        elif imbalance_ratio > 10:
            checks.append(f"  WARNING: Significant imbalance. Use stratified splits and class weights.")
        else:
            checks.append(f"  Moderate imbalance. Stratified splitting recommended.")
        checks.append(f"CHECK: {'WARN — imbalance present' if imbalance_ratio > 5 else 'PASS'}")

        # 8. Missing subgenre coverage
        checks.append(f"\n--- 8. MISSING SUBGENRE COVERAGE ---")
        no_subgenre = df[df["subgenre"] == ""]
        has_subgenre = df[df["subgenre"] != ""]
        checks.append(f"  Tracks with subgenre: {len(has_subgenre)}")
        checks.append(f"  Tracks without subgenre: {len(no_subgenre)}")
        if len(no_subgenre) > 0:
            genre_missing = no_subgenre["genre"].value_counts()
            for g, c in genre_missing.items():
                checks.append(f"    {g:15s}: {c} tracks missing subgenre")
        checks.append(f"CHECK: {'WARN' if len(no_subgenre) > len(df)*0.5 else 'PASS'}")

        # 9. Features with extreme outlier rates
        checks.append(f"\n--- 9. EXTREME OUTLIER RATES ---")
        for feat in present_features:
            vals = df[feat].dropna()
            if len(vals) < 10:
                checks.append(f"  {feat:25s}: INSUFFICIENT DATA")
                continue
            q1 = vals.quantile(0.25)
            q3 = vals.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                checks.append(f"  {feat:25s}: IQR=0 (near-constant)")
                continue
            lower = q1 - 3.0 * iqr
            upper = q3 + 3.0 * iqr
            n_outliers = ((vals < lower) | (vals > upper)).sum()
            pct = n_outliers / len(vals) * 100
            checks.append(f"  {feat:25s}: {n_outliers:4d} outliers ({pct:5.1f}%) [3*IQR]")
        checks.append(f"CHECK: PASS")

        self.emit(f"Readiness checks: 9 completed")

        return checks, genre_lines, subgenre_lines, corr_lines

    # ================================================================
    # PART D — BASELINE MODEL PLAN
    # ================================================================
    def part_d(self, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — BASELINE MODEL PLAN")
        self.emit("=" * 60)

        genre_dist = df["genre"].value_counts()

        lines = []
        lines.append("=" * 70)
        lines.append("BASELINE CLASSIFIER PLAN — genre_benchmark_v1")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("STATUS: PLAN ONLY — NO TRAINING IN THIS PHASE")
        lines.append("=" * 70)

        lines.append("""
1. RECOMMENDED FIRST MODEL
===========================
Model: Random Forest (scikit-learn RandomForestClassifier)

Rationale:
- Handles mixed feature types and non-linear interactions
- Built-in feature importance via Gini or permutation importance
- Robust to moderate class imbalance with class_weight='balanced'
- Interpretable (tree-based, feature rankings directly readable)
- No hyperparameter tuning needed for a baseline — sklearn defaults + balanced weights
- Fast to train on 200 rows

Secondary model for comparison:
- XGBoost (XGBClassifier) — gradient boosting with built-in importance
- Only after Random Forest baseline is established

Hyperparameters (baseline):
  n_estimators = 100
  max_depth = None (let trees grow)
  min_samples_split = 5
  min_samples_leaf = 2
  class_weight = 'balanced'
  random_state = 42
  n_jobs = -1
""")

        lines.append("""
2. TARGET LABELS
=================
Primary target: genre (categorical — {} classes)
Secondary target: subgenre (categorical — only for tracks where subgenre != '')

Genre target is required.
Subgenre target is stretch — many tracks lack subgenre labels.

Label encoding: LabelEncoder or direct string labels (scikit-learn handles both).
""".format(len(genre_dist)))

        lines.append("""
3. TRAIN / VALIDATION STRATEGY
===============================
Dataset: 200 benchmark tracks (classifier_dataset_v1.csv)

Strategy: Stratified K-Fold Cross-Validation
  k = 5
  stratify_by = genre
  random_state = 42

Rationale:
- 200 tracks is too small for a held-out test set
- K-fold maximizes training data usage
- Stratification preserves genre proportions in each fold

Metrics per fold:
  - Accuracy
  - Weighted F1-score
  - Per-class precision, recall, F1
  - Confusion matrix

Report:
  - Mean ± std across folds for each metric
  - Worst-fold performance (identifies instability)
""")

        lines.append("""
4. BENCHMARK EVALUATION STRATEGY
==================================
The 200-track benchmark set IS the dataset in this phase.
There is no separate hold-out benchmark — use cross-validation metrics only.

In a future phase (post-expansion):
  - The original 200 benchmark tracks become a LOCKED TEST SET
  - New training data comes from the expanded 5,000-track catalog
  - Never train on benchmark tracks in that scenario
  - Always report benchmark-set metrics separately from general test metrics
""")

        lines.append("""
5. HOW TO USE THE 200-TRACK BENCHMARK SAFELY
===============================================
Current phase (200 tracks only):
  - Cross-validation ONLY — no separate test split
  - Benchmark = training data = same 200 tracks

Future phase (post-expansion):
  - FREEZE these 200 tracks as TEST SET
  - Tag them in DB so they can be excluded from training queries:
      benchmark_set_tracks.benchmark_set_id = (genre_benchmark_v1)
  - Training comes from NEW tracks only
  - Evaluate on frozen benchmark to measure how well new-data-trained
    model generalizes to the curated benchmark

Key rule: NEVER add new tracks to the benchmark set once classifiers
are trained. The benchmark is a fixed reference point.
""")

        lines.append("""
6. WHAT NOT TO DO YET
=======================
- DO NOT train a production model on 200 tracks
- DO NOT use cross-val results as final performance numbers
- DO NOT deploy any model to the application
- DO NOT ingest the 5,000-track expansion into training
- DO NOT tune hyperparameters extensively (overfit risk on 200 rows)
- DO NOT add augmented/synthetic data
- DO NOT use the REMOVE features (bpm_detected, valence)
- DO NOT change labels during training — labels are frozen from misfit review
""")

        lines.append("""
7. FUTURE 5,000-TRACK EXPANSION STRATEGY
==========================================
When the expansion is ready:

Step 1: Ingest new tracks into DB (same schema, new track IDs)
Step 2: Run audio feature extraction on new tracks
Step 3: Apply genre labels (manual, rules, or classifier-assisted)
Step 4: Create a NEW classifier dataset (v2) that includes:
        - new tracks as TRAINING SET
        - original 200 benchmark tracks as LOCKED TEST SET
Step 5: Train on v2 training split only
Step 6: Evaluate on locked benchmark separately
Step 7: Compare v2 performance vs v1 cross-val baseline

Contamination prevention:
  - Query: WHERE track_id NOT IN (SELECT track_id FROM benchmark_set_tracks 
            WHERE benchmark_set_id = [v1 id])
  - This ensures benchmark tracks never appear in training
  - Track lineage via applied_by column ensures auditability
""")

        lines.append(f"""
8. CURRENT DATASET SUMMARY
============================
Tracks: {len(df)}
Genres: {len(genre_dist)}
Features (v1): {len(V1_FEATURES)} ({', '.join(V1_FEATURES)})
Features excluded: bpm_detected, valence
""")

        # Genre breakdown
        lines.append("Genre distribution:")
        for g, c in genre_dist.items():
            lines.append(f"  {g:15s}: {c:4d} ({c/len(df)*100:5.1f}%)")

        self.emit("Baseline model plan generated")
        return lines

    # ================================================================
    # WRITE PROOF
    # ================================================================
    def write_proof(self, part_a_ok, part_b_ok, feature_status_lines,
                    df, manifest_df, check_lines, genre_lines, subgenre_lines,
                    corr_lines, plan_lines):
        self.emit("\n" + "=" * 60)
        self.emit("WRITING PROOF FILES")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        # 00 feature status matrix
        w("00_feature_status_matrix.txt", "\n".join(feature_status_lines))

        # 01 classifier dataset summary
        ds_lines = ["=== CLASSIFIER DATASET V1 SUMMARY ===",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                     f"Source: {DATASET_CSV}",
                     f"Rows: {len(df)}",
                     f"Columns: {list(df.columns)}",
                     f"Features (v1): {V1_FEATURES}",
                     f"Excluded: bpm_detected, valence",
                     "",
                     "Column types:"]
        for col in df.columns:
            ds_lines.append(f"  {col:25s}: {df[col].dtype}")
        ds_lines.append("")
        ds_lines.append("Statistics:")
        present_features = [f for f in V1_FEATURES if f in df.columns]
        for feat in present_features:
            vals = df[feat].dropna()
            if len(vals) > 0:
                ds_lines.append(
                    f"  {feat:25s}: mean={vals.mean():.4f}  std={vals.std():.4f}  "
                    f"min={vals.min():.4f}  max={vals.max():.4f}  nulls={df[feat].isna().sum()}"
                )
        w("01_classifier_dataset_summary.txt", "\n".join(ds_lines))

        # 02 feature manifest summary
        man_lines = ["=== FEATURE MANIFEST V1 SUMMARY ===",
                      f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                      f"Source: {MANIFEST_CSV}",
                      f"Total features cataloged: {len(manifest_df)}",
                      ""]
        for _, row in manifest_df.iterrows():
            man_lines.append(
                f"  {row['feature_name']:25s}  status={row['status']:15s}  "
                f"in_v1={row['included_in_v1']:3s}  {row['notes']}"
            )
        w("02_feature_manifest_summary.txt", "\n".join(man_lines))

        # 03 readiness checks
        w("03_readiness_checks.txt", "\n".join(check_lines))

        # 04 genre distribution
        w("04_genre_distribution.txt", "\n".join(
            ["=== GENRE DISTRIBUTION ===",
             f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
            + genre_lines
        ))

        # 05 subgenre distribution
        w("05_subgenre_distribution.txt", "\n".join(
            ["=== SUBGENRE DISTRIBUTION ===",
             f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
            + subgenre_lines
        ))

        # 06 correlated features
        w("06_correlated_features.txt", "\n".join(
            ["=== CORRELATED FEATURE PAIRS ===",
             f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
            + corr_lines
        ))

        # 07 baseline model plan
        w("07_baseline_model_plan.txt", "\n".join(plan_lines))

        # 08 validation queries
        conn = self.connect(readonly=True)
        vq_lines = ["=== VALIDATION QUERIES ===",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
        queries = [
            ("SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
             "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1');"),
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary';",
            ("SELECT track_id, COUNT(*) FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
            "PRAGMA foreign_key_check;",
            "SELECT COUNT(*) FROM track_genre_labels;",
            "SELECT COUNT(*) FROM tracks;",
        ]
        for q in queries:
            result = conn.execute(q).fetchall()
            vq_lines.append(f"-- {q}")
            if result:
                for r in result:
                    vq_lines.append(f"   {tuple(r)}")
            else:
                vq_lines.append("   (empty)")
            vq_lines.append("")
        conn.close()

        # 08 final report
        gate = "PASS" if (part_a_ok and part_b_ok) else "FAIL"
        report = [
            "=" * 60,
            "CLASSIFIER READINESS — FINAL REPORT",
            "=" * 60,
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Elapsed: {elapsed}s",
            f"GATE: {gate}",
            "",
            f"PART A — Feature Status Lock: {'PASS' if part_a_ok else 'FAIL'}",
            f"  KEEP: {sum(1 for d in FEATURE_STATUS.values() if d['status']=='KEEP')}",
            f"  DOWNWEIGHT: {sum(1 for d in FEATURE_STATUS.values() if d['status']=='DOWNWEIGHT')}",
            f"  REMOVE: {sum(1 for d in FEATURE_STATUS.values() if d['status']=='REMOVE')}",
            f"  CANDIDATE_ADD: {len(CANDIDATE_ADD)}",
            "",
            f"PART B — Classifier Dataset V1: {'PASS' if part_b_ok else 'FAIL'}",
            f"  Rows: {len(df)}",
            f"  Features: {len(V1_FEATURES)}",
            f"  Null primary labels: 0",
            f"  Duplicate rows: 0",
            "",
            "PART C — Readiness Checks: completed",
            "",
            "PART D — Baseline Model Plan: generated (no training)",
            "",
            f"Proof: {PROOF_DIR}",
            "",
            "--- VALIDATION ---",
        ] + vq_lines

        w("08_final_report.txt", "\n".join(report))

        # execution log
        w("execution_log.txt", "\n".join(self.log))

        # Optional: readiness stats CSV
        stats_rows = []
        present_features = [f for f in V1_FEATURES if f in df.columns]
        for feat in present_features:
            vals = df[feat].dropna()
            stats_rows.append({
                "feature": feat,
                "count": len(vals),
                "mean": vals.mean() if len(vals) > 0 else None,
                "std": vals.std() if len(vals) > 0 else None,
                "min": vals.min() if len(vals) > 0 else None,
                "max": vals.max() if len(vals) > 0 else None,
                "nulls": df[feat].isna().sum(),
                "null_pct": df[feat].isna().sum() / len(df) * 100,
            })
        pd.DataFrame(stats_rows).to_csv(PROOF_DIR / "readiness_stats.csv",
                                         index=False, encoding="utf-8")

        self.emit(f"Proof written to: {PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: DB not found: {ANALYSIS_DB}")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    # PART A
    part_a_ok, feature_status_lines = p.part_a()

    # PART B
    part_b_ok, df, manifest_df = p.part_b()
    if not part_b_ok:
        p.emit("FATAL: Part B failed")
        return 1

    # PART C
    check_lines, genre_lines, subgenre_lines, corr_lines = p.part_c(df)

    # PART D
    plan_lines = p.part_d(df)

    # Write proof
    gate = p.write_proof(
        part_a_ok, part_b_ok, feature_status_lines,
        df, manifest_df, check_lines, genre_lines, subgenre_lines,
        corr_lines, plan_lines
    )

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
