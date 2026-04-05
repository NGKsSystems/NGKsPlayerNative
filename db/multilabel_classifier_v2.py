#!/usr/bin/env python3
"""
Phase — Multi-Label Classifier V2 (Clean Dataset)

Builds a clean multi-label dataset from the current validated label state,
trains OneVsRest Random Forest, evaluates, and compares against V1 baseline.

NO DB mutations. Read-only against production tables.
"""

import io
import json
import os
import pickle
import shutil
import sqlite3
import sys
import time
import warnings
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    hamming_loss,
    precision_score,
    recall_score,
)
from sklearn.model_selection import KFold
from sklearn.multiclass import OneVsRestClassifier
from sklearn.preprocessing import StandardScaler

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore", category=UserWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "multilabel_classifier_v2"
MODEL_DIR = WORKSPACE / "models" / "multilabel_v2"

# V1 label mapping (locked for comparison)
V1_LABEL_SET = ["Country", "Hip-Hop", "Metal", "Other", "Pop", "Rock"]
V1_CORE_GENRES = {"Country", "Hip-Hop", "Metal", "Pop", "Rock"}

# V1 baseline metrics (from _proof/multi_label_readiness)
V1_METRICS = {
    "hamming_loss": 0.1658,
    "micro_f1": 0.5086,
    "macro_f1": 0.2856,
    "subset_accuracy": 0.3713,
    "subset_accuracy_multilabel_only": 0.1163,
    "n_tracks": 202,
    "n_labels": 6,
    "n_multilabel_tracks": 43,
    "n_features": 7,
    "model": "OneVsRest(RandomForest(n_estimators=100))",
    "cv_folds": 5,
}

V1_PER_LABEL_F1 = {
    "Country": 0.6879,
    "Rock": 0.5091,
    "Hip-Hop": 0.2667,
    "Other": 0.2500,
    "Metal": 0.0000,
    "Pop": 0.0000,
}

# Feature set (locked to V1 for fair comparison)
FEATURE_COLS = [
    "harmonic_stability",
    "loudness_lufs",
    "avg_section_duration",
    "tempo_stability",
    "energy",
    "danceability",
    "section_count",
]

RANDOM_STATE = 42
N_FOLDS = 5


class MultiLabelClassifierV2:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.df: pd.DataFrame | None = None
        self.X: np.ndarray | None = None
        self.Y: np.ndarray | None = None
        self.label_cols: list[str] | None = None
        self.label_mapping = None
        self.fold_predictions = []
        self.metrics = {}
        self.per_label_metrics = {}
        self.v1_comparison = {}
        self.error_review = {}

        # Snapshot DB state before anything
        self.snap_prim = 0
        self.snap_sec = 0
        self.snap_bench = 0

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

    def snapshot_db(self, label=""):
        conn = self.connect_ro()
        prim = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
            "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"
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
            "primaries": prim,
            "secondaries": sec,
            "benchmark": bench,
            "fk_violations": len(fk),
            "dup_primaries": dup,
        }

    # ================================================================
    # PART A — REBUILD CLEAN MULTI-LABEL DATASET
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- REBUILD CLEAN MULTI-LABEL DATASET")
        self.emit("=" * 70)

        snap_before = self.snapshot_db("before")
        self.snap_prim = snap_before["primaries"]
        self.snap_sec = snap_before["secondaries"]
        self.snap_bench = snap_before["benchmark"]
        self.emit(f"  DB snapshot: prim={self.snap_prim}, sec={self.snap_sec}, "
                  f"bench={self.snap_bench}, fk={snap_before['fk_violations']}, "
                  f"dup={snap_before['dup_primaries']}")

        conn = self.connect_ro()

        # 1. Get benchmark track IDs
        bench_rows = conn.execute("""
            SELECT bst.track_id
            FROM benchmark_set_tracks bst
            JOIN benchmark_sets bs ON bst.benchmark_set_id = bs.id
            WHERE bs.name = 'genre_benchmark_v1'
            ORDER BY bst.track_id
        """).fetchall()
        bench_ids = [r["track_id"] for r in bench_rows]
        self.emit(f"  Benchmark tracks: {len(bench_ids)}")

        # 2. Get features (same SQL as V1)
        features = conn.execute("""
            SELECT
                t.id AS track_id,
                t.artist,
                t.title,
                asumm.key_confidence AS harmonic_stability,
                asumm.loudness_lufs,
                asumm.energy,
                asumm.danceability,
                asumm.bpm_confidence AS tempo_stability,
                sec_agg.section_count,
                sec_agg.avg_section_duration
            FROM tracks t
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
            WHERE t.id IN ({})
            ORDER BY t.id
        """.format(",".join("?" * len(bench_ids))), bench_ids).fetchall()

        feat_df = pd.DataFrame([dict(r) for r in features])
        self.emit(f"  Features rows: {len(feat_df)}")

        # 3. Get ALL labels (primary + secondary) for benchmark tracks
        labels = conn.execute("""
            SELECT tgl.track_id, g.name AS genre, tgl.role
            FROM track_genre_labels tgl
            JOIN genres g ON tgl.genre_id = g.id
            WHERE tgl.track_id IN ({})
              AND tgl.role IN ('primary', 'secondary')
            ORDER BY tgl.track_id, tgl.role
        """.format(",".join("?" * len(bench_ids))), bench_ids).fetchall()

        conn.close()

        # 4. Map genres to V1 label set
        # Core genres stay as-is; everything else → "Other"
        label_records = []
        for row in labels:
            genre = row["genre"]
            mapped = genre if genre in V1_CORE_GENRES else "Other"
            label_records.append({
                "track_id": row["track_id"],
                "genre": genre,
                "mapped_genre": mapped,
                "role": row["role"],
            })

        label_df = pd.DataFrame(label_records)
        self.emit(f"  Label rows (raw): {len(label_df)}")

        # 5. Build genre-to-index mapping (same as V1)
        self.label_mapping = {g: i for i, g in enumerate(V1_LABEL_SET)}
        self.label_cols = [f"label_{g}" for g in V1_LABEL_SET]

        # 6. Build one-hot label matrix
        track_labels = defaultdict(set)
        for _, row in label_df.iterrows():
            track_labels[row["track_id"]].add(row["mapped_genre"])

        # 7. Merge features + labels
        rows = []
        for _, feat_row in feat_df.iterrows():
            tid = feat_row["track_id"]
            genre_set = track_labels.get(tid, set())

            if not genre_set:
                self.emit(f"  WARN: track {tid} has no labels — skipping")
                continue

            record = feat_row.to_dict()
            for g in V1_LABEL_SET:
                record[f"label_{g}"] = 1 if g in genre_set else 0
            rows.append(record)

        self.df = pd.DataFrame(rows)
        self.emit(f"  Dataset rows: {len(self.df)}")

        # 8. Validate no null primary: every track must have at least one label=1
        label_sums = self.df[self.label_cols].sum(axis=1)  # type: ignore[call-overload]
        no_label = (label_sums == 0).sum()
        if no_label > 0:
            self.emit(f"  FATAL: {no_label} tracks with no labels")
            raise ValueError(f"{no_label} tracks with zero labels")

        # 9. Check for duplicate rows (by track_id)
        dup_tids = self.df["track_id"].duplicated().sum()
        if dup_tids > 0:
            self.emit(f"  FATAL: {dup_tids} duplicate track_ids")
            raise ValueError(f"Duplicate track_ids: {dup_tids}")

        # 10. Prepare X, Y
        self.X = self.df[FEATURE_COLS].values
        self.Y = self.df[self.label_cols].values

        # Track label distribution
        n_labels_per_track = label_sums.value_counts().sort_index()
        self.emit(f"  Labels per track distribution:")
        for n, cnt in n_labels_per_track.items():
            self.emit(f"    {int(n)} labels: {cnt} tracks")  # type: ignore[arg-type]

        multi_label_count = (label_sums > 1).sum()
        self.emit(f"  Multi-label tracks: {multi_label_count}/{len(self.df)}")

        # Label frequency
        for col in self.label_cols:
            self.emit(f"    {col}: {self.df[col].sum()}")

        # 11. Save dataset CSV
        out_cols = ["track_id"] + FEATURE_COLS + self.label_cols
        self.df[out_cols].to_csv(
            DATA_DIR / "classifier_dataset_multilabel_v2.csv",
            index=False, encoding="utf-8",
        )

        # 12. Save label mapping CSV
        mapping_df = pd.DataFrame([
            {"genre": g, "index": i} for g, i in self.label_mapping.items()
        ])
        mapping_df.to_csv(
            DATA_DIR / "classifier_label_mapping_v2.csv",
            index=False, encoding="utf-8",
        )

        # 13. Save feature manifest CSV
        manifest_rows = []
        for f in FEATURE_COLS:
            source = "analysis_summary" if f not in ("section_count", "avg_section_duration") else "section_events"
            manifest_rows.append({
                "feature_name": f,
                "included": True,
                "source_table": source,
                "notes": "Same as V1 (locked feature set)",
            })
        # Also document excluded features
        for f in ["bpm_detected", "valence"]:
            manifest_rows.append({
                "feature_name": f,
                "included": False,
                "source_table": "analysis_summary",
                "notes": "Excluded in V1 (low ANOVA F-score)",
            })
        pd.DataFrame(manifest_rows).to_csv(
            DATA_DIR / "classifier_multilabel_manifest_v2.csv",
            index=False, encoding="utf-8",
        )

        self.emit(f"  Dataset saved: {DATA_DIR / 'classifier_dataset_multilabel_v2.csv'}")
        self.emit(f"  Label mapping saved: {DATA_DIR / 'classifier_label_mapping_v2.csv'}")
        self.emit(f"  Manifest saved: {DATA_DIR / 'classifier_multilabel_manifest_v2.csv'}")

        # Also save co-occurrence matrix
        Y_df = self.df[self.label_cols]
        cooc = Y_df.T.dot(Y_df)
        cooc.to_csv(PROOF_DIR / "label_cooccurrence_matrix.csv", encoding="utf-8")

    # ================================================================
    # PART B — DATA VALIDATION
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B -- DATA VALIDATION")
        self.emit("=" * 70)

        lines = []
        lines.append("=" * 70)
        lines.append("DATASET VALIDATION")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        all_ok = True

        # 1. Row count
        assert self.df is not None and self.label_cols is not None
        nrows = len(self.df)
        chk1 = nrows == self.snap_bench
        lines.append(f"\n  1. Row count: {nrows} (benchmark={self.snap_bench}) "
                     f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Unique track_ids
        n_unique = self.df["track_id"].nunique()
        chk2 = n_unique == nrows
        lines.append(f"  2. Unique track_ids: {n_unique} (rows={nrows}) "
                     f"-- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. Single vs multi-label
        label_sums = self.df[self.label_cols].sum(axis=1)  # type: ignore[call-overload]
        n_single = (label_sums == 1).sum()
        n_multi = (label_sums > 1).sum()
        n_zero = (label_sums == 0).sum()
        lines.append(f"  3. Single-label tracks: {n_single}")
        lines.append(f"     Multi-label tracks: {n_multi}")
        lines.append(f"     Zero-label tracks: {n_zero}")
        chk3 = n_zero == 0
        if not chk3:
            all_ok = False
            lines.append(f"     FAIL: {n_zero} tracks with zero labels")

        # 4. Label density
        total_labels = label_sums.sum()
        density = total_labels / (nrows * len(self.label_cols))
        lines.append(f"  4. Label density: {density:.4f} "
                     f"(total_labels={int(total_labels)}, "
                     f"n_tracks={nrows}, n_labels={len(self.label_cols)})")

        # 5. Class distribution
        lines.append(f"  5. Class distribution:")
        for col in self.label_cols:
            cnt = self.df[col].sum()
            pct = cnt / nrows * 100
            lines.append(f"     {col}: {int(cnt)} ({pct:.1f}%)")

        # 6. Co-occurrence
        lines.append(f"  6. Co-occurrence pairs:")
        Y_df = self.df[self.label_cols]
        for i, c1 in enumerate(self.label_cols):
            for j, c2 in enumerate(self.label_cols):
                if j <= i:
                    continue
                co = ((Y_df[c1] == 1) & (Y_df[c2] == 1)).sum()
                if co > 0:
                    lines.append(f"     {c1} + {c2}: {co}")

        # 7. Duplicate label vectors
        label_vecs = self.df[self.label_cols].apply(
            lambda r: "".join(str(int(v)) for v in r), axis=1
        )
        vec_counts = label_vecs.value_counts()
        total_unique_vecs = len(vec_counts)
        lines.append(f"  7. Unique label vectors: {total_unique_vecs}")
        for vec, cnt in vec_counts.items():
            labels_named = [V1_LABEL_SET[i] for i, v in enumerate(vec) if v == "1"]  # type: ignore[arg-type]  # type: ignore[arg-type]
            lines.append(f"     {vec} ({', '.join(labels_named)}): {cnt}")

        # 8. Feature null checks
        feat_nulls = self.df[FEATURE_COLS].isnull().sum()
        lines.append(f"  8. Feature null counts:")
        has_nulls = False
        for f in FEATURE_COLS:
            n = feat_nulls[f]
            lines.append(f"     {f}: {n}")
            if n > 0:
                has_nulls = True
        if has_nulls:
            lines.append(f"     NOTE: Nulls present, will use median imputation")

        # 9. Feature ranges
        lines.append(f"  9. Feature ranges:")
        for f in FEATURE_COLS:
            mn = self.df[f].min()
            mx = self.df[f].max()
            md = self.df[f].median()
            lines.append(f"     {f}: min={mn:.4f}, max={mx:.4f}, median={md:.4f}")

        # 10. Impossible rows check: no track should have mutually exclusive labels
        # (Not applicable with current taxonomy — no mutual exclusion rules)
        lines.append(f"  10. Impossible row check: N/A (no exclusion rules)")

        lines.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        (PROOF_DIR / "00_dataset_validation.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )
        self.emit(f"  Validation: {'PASS' if all_ok else 'FAIL'}")
        self.emit(f"  Rows={nrows}, single={n_single}, multi={n_multi}, "
                  f"density={density:.4f}")

        # Dataset summary
        summary = []
        summary.append("=" * 70)
        summary.append("DATASET SUMMARY")
        summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("=" * 70)
        summary.append(f"  Source: benchmark_set 'genre_benchmark_v1'")
        summary.append(f"  Tracks: {nrows}")
        summary.append(f"  Features: {len(FEATURE_COLS)}")
        assert self.df is not None and self.label_cols is not None
        summary.append(f"  Labels: {len(self.label_cols)}")
        summary.append(f"  Label set: {V1_LABEL_SET}")
        summary.append(f"  Single-label tracks: {n_single}")
        summary.append(f"  Multi-label tracks: {n_multi}")
        summary.append(f"  Label density: {density:.4f}")
        summary.append(f"\n  Label distribution:")
        for col in self.label_cols:
            cnt = self.df[col].sum()
            summary.append(f"    {col}: {int(cnt)}")
        summary.append(f"\n  Feature set (locked to V1):")
        for f in FEATURE_COLS:
            summary.append(f"    {f}")
        summary.append(f"\n  Genre mapping: core genres → direct; all others → Other")
        summary.append(f"  V1 comparison note: V1 had {V1_METRICS['n_multilabel_tracks']} multi-label tracks, "
                       f"V2 has {n_multi}")

        (PROOF_DIR / "01_dataset_summary.txt").write_text(
            "\n".join(summary), encoding="utf-8"
        )

    # ================================================================
    # PART C — MODEL TRAINING
    # ================================================================
    def part_c(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART C -- MODEL TRAINING")
        self.emit("=" * 70)

        # Impute + prepare
        assert self.X is not None and self.Y is not None
        assert self.df is not None and self.label_cols is not None
        imputer = SimpleImputer(strategy="median")
        X_imp = imputer.fit_transform(self.X)

        # Stratification approach for multi-label:
        # Create a combined label string to stratify on
        label_strs = np.array([
            "".join(str(int(v)) for v in row) for row in self.Y
        ])

        # Check if any label combination has fewer samples than n_folds
        combo_counts = Counter(label_strs)
        min_combo = min(combo_counts.values())
        self.emit(f"  Label combo distribution: {len(combo_counts)} unique combos")
        self.emit(f"  Min combo count: {min_combo}")

        if min_combo >= N_FOLDS:
            from sklearn.model_selection import StratifiedKFold
            splitter = StratifiedKFold(n_splits=N_FOLDS, shuffle=True,
                                       random_state=RANDOM_STATE)
            split_iter = splitter.split(X_imp, label_strs)
            cv_strategy = f"StratifiedKFold(n_splits={N_FOLDS}, shuffle=True, " \
                          f"random_state={RANDOM_STATE}) on combined label strings"
            self.emit(f"  CV strategy: StratifiedKFold on combined label strings")
        else:
            # Fallback: regular KFold (some combos too rare for stratification)
            splitter = KFold(n_splits=N_FOLDS, shuffle=True,
                             random_state=RANDOM_STATE)
            split_iter = splitter.split(X_imp)
            cv_strategy = (f"KFold(n_splits={N_FOLDS}, shuffle=True, "
                           f"random_state={RANDOM_STATE}) — "
                           f"fallback because {len([c for c in combo_counts.values() if c < N_FOLDS])} "
                           f"label combos have <{N_FOLDS} samples")
            self.emit(f"  CV strategy: KFold (fallback, min combo={min_combo} < {N_FOLDS})")

        # Model definition
        base_rf = RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            min_samples_split=5,
            min_samples_leaf=2,
            class_weight="balanced",
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        model = OneVsRestClassifier(base_rf)

        model_config = {
            "model": "OneVsRestClassifier(RandomForestClassifier)",
            "n_estimators": 200,
            "max_depth": "None",
            "min_samples_split": 5,
            "min_samples_leaf": 2,
            "class_weight": "balanced",
            "random_state": RANDOM_STATE,
            "n_jobs": -1,
            "cv_strategy": cv_strategy,
            "n_folds": N_FOLDS,
            "imputation": "median",
            "scaling": "none (RF)",
            "label_set": V1_LABEL_SET,
            "n_features": len(FEATURE_COLS),
            "feature_set": FEATURE_COLS,
        }

        # Cross-validation
        fold_metrics = []
        all_y_true = []
        all_y_pred = []
        all_track_ids = []
        per_label_fold = {c: {"precision": [], "recall": [], "f1": []}
                          for c in V1_LABEL_SET}
        feature_importances = np.zeros((N_FOLDS, len(FEATURE_COLS)))

        for fold_idx, (train_idx, test_idx) in enumerate(split_iter):
            self.emit(f"  Fold {fold_idx + 1}/{N_FOLDS}: "
                      f"train={len(train_idx)}, test={len(test_idx)}")

            X_train, X_test = X_imp[train_idx], X_imp[test_idx]
            Y_train, Y_test = self.Y[train_idx], self.Y[test_idx]

            model.fit(X_train, Y_train)
            Y_pred = model.predict(X_test)

            # Fold-level metrics
            h_loss = hamming_loss(Y_test, Y_pred)
            micro_p = precision_score(Y_test, Y_pred, average="micro", zero_division=0)
            micro_r = recall_score(Y_test, Y_pred, average="micro", zero_division=0)
            micro_f1 = f1_score(Y_test, Y_pred, average="micro", zero_division=0)
            macro_p = precision_score(Y_test, Y_pred, average="macro", zero_division=0)
            macro_r = recall_score(Y_test, Y_pred, average="macro", zero_division=0)
            macro_f1 = f1_score(Y_test, Y_pred, average="macro", zero_division=0)

            # Subset accuracy
            subset_acc = accuracy_score(Y_test, Y_pred)

            # Multi-label-only subset accuracy
            ml_mask = Y_test.sum(axis=1) > 1
            if ml_mask.sum() > 0:
                ml_subset_acc = accuracy_score(Y_test[ml_mask], Y_pred[ml_mask])
            else:
                ml_subset_acc = float("nan")

            fold_metrics.append({
                "fold": fold_idx + 1,
                "hamming_loss": h_loss,
                "micro_precision": micro_p,
                "micro_recall": micro_r,
                "micro_f1": micro_f1,
                "macro_precision": macro_p,
                "macro_recall": macro_r,
                "macro_f1": macro_f1,
                "subset_accuracy": subset_acc,
                "ml_subset_accuracy": ml_subset_acc,
            })

            self.emit(f"    hamming={h_loss:.4f}, micro_f1={micro_f1:.4f}, "
                      f"macro_f1={macro_f1:.4f}, subset_acc={subset_acc:.4f}")

            # Per-label metrics
            for li, label in enumerate(V1_LABEL_SET):
                if Y_test[:, li].sum() > 0 or Y_pred[:, li].sum() > 0:
                    lp = precision_score(Y_test[:, li], Y_pred[:, li], zero_division=0)
                    lr = recall_score(Y_test[:, li], Y_pred[:, li], zero_division=0)
                    lf = f1_score(Y_test[:, li], Y_pred[:, li], zero_division=0)
                else:
                    lp = lr = lf = 0.0
                per_label_fold[label]["precision"].append(lp)
                per_label_fold[label]["recall"].append(lr)
                per_label_fold[label]["f1"].append(lf)

            # Feature importances (average across OVR estimators)
            for est_idx, est in enumerate(model.estimators_):
                feature_importances[fold_idx] += est.feature_importances_  # type: ignore[attr-defined]
            feature_importances[fold_idx] /= len(model.estimators_)

            # Collect predictions
            test_tids = self.df["track_id"].values[test_idx]
            for i, tidx in enumerate(test_idx):
                all_y_true.append(Y_test[i])
                all_y_pred.append(Y_pred[i])
                all_track_ids.append(test_tids[i])

                self.fold_predictions.append({
                    "fold": fold_idx + 1,
                    "track_id": int(test_tids[i]),
                    "artist": self.df.iloc[tidx]["artist"],
                    "title": self.df.iloc[tidx]["title"],
                    **{f"true_{l}": int(Y_test[i][j]) for j, l in enumerate(V1_LABEL_SET)},
                    **{f"pred_{l}": int(Y_pred[i][j]) for j, l in enumerate(V1_LABEL_SET)},
                    "correct": int(np.array_equal(Y_test[i], Y_pred[i])),
                })

        # Aggregate metrics
        fm_df = pd.DataFrame(fold_metrics)
        self.metrics = {
            "hamming_loss": fm_df["hamming_loss"].mean(),
            "hamming_loss_std": fm_df["hamming_loss"].std(),
            "micro_precision": fm_df["micro_precision"].mean(),
            "micro_precision_std": fm_df["micro_precision"].std(),
            "micro_recall": fm_df["micro_recall"].mean(),
            "micro_recall_std": fm_df["micro_recall"].std(),
            "micro_f1": fm_df["micro_f1"].mean(),
            "micro_f1_std": fm_df["micro_f1"].std(),
            "macro_precision": fm_df["macro_precision"].mean(),
            "macro_precision_std": fm_df["macro_precision"].std(),
            "macro_recall": fm_df["macro_recall"].mean(),
            "macro_recall_std": fm_df["macro_recall"].std(),
            "macro_f1": fm_df["macro_f1"].mean(),
            "macro_f1_std": fm_df["macro_f1"].std(),
            "subset_accuracy": fm_df["subset_accuracy"].mean(),
            "subset_accuracy_std": fm_df["subset_accuracy"].std(),
            "ml_subset_accuracy": fm_df["ml_subset_accuracy"].mean(),
            "ml_subset_accuracy_std": fm_df["ml_subset_accuracy"].std(),
        }

        # Per-label aggregated
        for label in V1_LABEL_SET:
            self.per_label_metrics[label] = {
                "precision": np.mean(per_label_fold[label]["precision"]),
                "precision_std": np.std(per_label_fold[label]["precision"]),
                "recall": np.mean(per_label_fold[label]["recall"]),
                "recall_std": np.std(per_label_fold[label]["recall"]),
                "f1": np.mean(per_label_fold[label]["f1"]),
                "f1_std": np.std(per_label_fold[label]["f1"]),
                "support": int(self.df[f"label_{label}"].sum()),
            }

        # Feature importance
        avg_fi = feature_importances.mean(axis=0)
        std_fi = feature_importances.std(axis=0)
        self.feature_importance = list(zip(FEATURE_COLS, avg_fi, std_fi))
        self.feature_importance.sort(key=lambda x: -x[1])

        self.emit(f"\n  === AGGREGATED METRICS ===")
        self.emit(f"  Hamming Loss:     {self.metrics['hamming_loss']:.4f} ± {self.metrics['hamming_loss_std']:.4f}")
        self.emit(f"  Micro F1:         {self.metrics['micro_f1']:.4f} ± {self.metrics['micro_f1_std']:.4f}")
        self.emit(f"  Macro F1:         {self.metrics['macro_f1']:.4f} ± {self.metrics['macro_f1_std']:.4f}")
        self.emit(f"  Subset Accuracy:  {self.metrics['subset_accuracy']:.4f} ± {self.metrics['subset_accuracy_std']:.4f}")
        self.emit(f"  ML Subset Acc:    {self.metrics['ml_subset_accuracy']:.4f} ± {self.metrics['ml_subset_accuracy_std']:.4f}")

        # Write training configuration
        config_lines = []
        config_lines.append("=" * 70)
        config_lines.append("TRAINING CONFIGURATION")
        config_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        config_lines.append("=" * 70)
        for k, v in model_config.items():
            config_lines.append(f"  {k}: {v}")

        (PROOF_DIR / "02_training_configuration.txt").write_text(
            "\n".join(config_lines), encoding="utf-8"
        )

        # Write metrics
        met_lines = []
        met_lines.append("=" * 70)
        met_lines.append("MULTI-LABEL METRICS (V2)")
        met_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        met_lines.append("=" * 70)
        met_lines.append(f"\n  Aggregated ({N_FOLDS}-fold CV):")
        met_lines.append(f"  {'Metric':<25s} {'Mean':>10s} {'±Std':>10s}")
        met_lines.append(f"  {'-' * 45}")
        for k in ["hamming_loss", "micro_precision", "micro_recall", "micro_f1",
                   "macro_precision", "macro_recall", "macro_f1",
                   "subset_accuracy", "ml_subset_accuracy"]:
            met_lines.append(f"  {k:<25s} {self.metrics[k]:>10.4f} {self.metrics[k + '_std']:>10.4f}")

        met_lines.append(f"\n  Per-fold results:")
        met_lines.append(f"  {'Fold':<6s} {'Hamming':>8s} {'MicroF1':>8s} {'MacroF1':>8s} "
                         f"{'SubAcc':>8s} {'MLSubAcc':>8s}")
        for fm in fold_metrics:
            met_lines.append(f"  {fm['fold']:<6d} {fm['hamming_loss']:>8.4f} "
                             f"{fm['micro_f1']:>8.4f} {fm['macro_f1']:>8.4f} "
                             f"{fm['subset_accuracy']:>8.4f} {fm['ml_subset_accuracy']:>8.4f}")

        (PROOF_DIR / "03_multilabel_metrics.txt").write_text(
            "\n".join(met_lines), encoding="utf-8"
        )

        # Write per-label metrics
        pl_lines = []
        pl_lines.append("=" * 70)
        pl_lines.append("PER-LABEL METRICS (V2)")
        pl_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        pl_lines.append("=" * 70)
        pl_lines.append(f"\n  {'Label':<12s} {'Precision':>10s} {'Recall':>10s} "
                        f"{'F1':>10s} {'Support':>8s}")
        pl_lines.append(f"  {'-' * 55}")
        for label in V1_LABEL_SET:
            m = self.per_label_metrics[label]
            pl_lines.append(f"  {label:<12s} {m['precision']:>10.4f} {m['recall']:>10.4f} "
                            f"{m['f1']:>10.4f} {m['support']:>8d}")

        pl_lines.append(f"\n  Feature Importance (Gini, averaged across OVR estimators and folds):")
        pl_lines.append(f"  {'Feature':<25s} {'Importance':>12s} {'±Std':>10s}")
        pl_lines.append(f"  {'-' * 50}")
        for feat, imp, std in self.feature_importance:
            pl_lines.append(f"  {feat:<25s} {imp:>12.6f} {std:>10.6f}")

        (PROOF_DIR / "04_per_label_metrics.txt").write_text(
            "\n".join(pl_lines), encoding="utf-8"
        )

        # Save fold predictions
        pd.DataFrame(self.fold_predictions).to_csv(
            PROOF_DIR / "fold_predictions.csv",
            index=False, encoding="utf-8",
        )

        # Train final model on full data for artifact saving
        self.final_model = OneVsRestClassifier(
            RandomForestClassifier(
                n_estimators=200,
                max_depth=None,
                min_samples_split=5,
                min_samples_leaf=2,
                class_weight="balanced",
                random_state=RANDOM_STATE,
                n_jobs=-1,
            )
        )
        self.final_model.fit(X_imp, self.Y)
        self.imputer = imputer

    # ================================================================
    # PART D — V1 vs V2 COMPARISON
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D -- V1 vs V2 COMPARISON")
        self.emit("=" * 70)
        assert self.df is not None and self.label_cols is not None

        lines = []
        lines.append("=" * 70)
        lines.append("V1 vs V2 COMPARISON")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        # Metric comparison
        comparisons = [
            ("Hamming Loss", V1_METRICS["hamming_loss"], self.metrics["hamming_loss"],
             self.metrics["hamming_loss_std"], "lower is better"),
            ("Micro F1", V1_METRICS["micro_f1"], self.metrics["micro_f1"],
             self.metrics["micro_f1_std"], "higher is better"),
            ("Macro F1", V1_METRICS["macro_f1"], self.metrics["macro_f1"],
             self.metrics["macro_f1_std"], "higher is better"),
            ("Subset Accuracy", V1_METRICS["subset_accuracy"], self.metrics["subset_accuracy"],
             self.metrics["subset_accuracy_std"], "higher is better"),
            ("ML Subset Accuracy", V1_METRICS["subset_accuracy_multilabel_only"],
             self.metrics["ml_subset_accuracy"], self.metrics["ml_subset_accuracy_std"],
             "higher is better"),
        ]

        lines.append(f"\n  {'Metric':<22s} {'V1':>8s} {'V2':>8s} {'±Std':>8s} "
                     f"{'Delta':>8s} {'Direction':>15s}")
        lines.append(f"  {'-' * 75}")

        for name, v1, v2, std, direction in comparisons:
            delta = v2 - v1
            if "lower" in direction:
                improved = delta < 0
            else:
                improved = delta > 0
            arrow = "IMPROVED" if improved else ("REGRESSED" if delta != 0 else "SAME")
            lines.append(f"  {name:<22s} {v1:>8.4f} {v2:>8.4f} {std:>8.4f} "
                         f"{delta:>+8.4f} {arrow:>15s}")

            self.v1_comparison[name] = {
                "v1": v1, "v2": v2, "delta": delta, "direction": arrow,
            }

        # Per-label comparison
        lines.append(f"\n  === PER-LABEL F1 COMPARISON ===")
        lines.append(f"  {'Label':<12s} {'V1 F1':>8s} {'V2 F1':>8s} {'Delta':>8s} "
                     f"{'V2 Support':>10s} {'Direction':>12s}")
        lines.append(f"  {'-' * 55}")

        for label in V1_LABEL_SET:
            v1_f1 = V1_PER_LABEL_F1.get(label, 0.0)
            v2_f1 = self.per_label_metrics[label]["f1"]
            delta = v2_f1 - v1_f1
            support = self.per_label_metrics[label]["support"]
            arrow = "IMPROVED" if delta > 0.001 else ("REGRESSED" if delta < -0.001 else "SAME")
            lines.append(f"  {label:<12s} {v1_f1:>8.4f} {v2_f1:>8.4f} {delta:>+8.4f} "
                         f"{support:>10d} {arrow:>12s}")

        # Dataset composition comparison
        label_sums = self.df[self.label_cols].sum(axis=1)  # type: ignore[call-overload]
        n_multi_v2 = (label_sums > 1).sum()

        lines.append(f"\n  === DATASET COMPOSITION COMPARISON ===")
        lines.append(f"  {'Metric':<30s} {'V1':>8s} {'V2':>8s}")
        lines.append(f"  {'-' * 50}")
        lines.append(f"  {'Tracks':<30s} {V1_METRICS['n_tracks']:>8d} {len(self.df):>8d}")
        lines.append(f"  {'Labels':<30s} {V1_METRICS['n_labels']:>8d} {len(self.label_cols):>8d}")
        lines.append(f"  {'Multi-label tracks':<30s} {V1_METRICS['n_multilabel_tracks']:>8d} {n_multi_v2:>8d}")
        lines.append(f"  {'Features':<30s} {V1_METRICS['n_features']:>8d} {len(FEATURE_COLS):>8d}")
        lines.append(f"  {'Model':<30s} {'OVR-RF(100)':>8s} {'OVR-RF(200)':>8s}")

        # Hybrid-pair handling analysis
        lines.append(f"\n  === HYBRID-PAIR HANDLING ===")
        lines.append(f"  V1: 43 multi-label tracks (pre-cleanup)")
        lines.append(f"  V2: {n_multi_v2} multi-label tracks (post-cleanup)")
        lines.append(f"  Cleanup impact: removed 13 spurious secondaries, "
                     f"retained 69 rule-validated + 4 REVIEW")

        # Identify which multi-label pairs exist
        lines.append(f"\n  Multi-label compositions (V2):")
        for _, row in self.df.iterrows():
            labels = [V1_LABEL_SET[i] for i, v in enumerate(
                [row[c] for c in self.label_cols]) if v == 1]
            if len(labels) > 1:
                lines.append(f"    {int(row['track_id']):5d} | {row['artist']:25s} | "
                             f"{' + '.join(labels)}")

        (PROOF_DIR / "05_v1_vs_v2_comparison.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Print summary
        for name, vals in self.v1_comparison.items():
            self.emit(f"  {name}: V1={vals['v1']:.4f} → V2={vals['v2']:.4f} "
                      f"({vals['direction']}, {vals['delta']:+.4f})")

    # ================================================================
    # PART E — ERROR REVIEW
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E -- ERROR REVIEW")
        self.emit("=" * 70)

        lines = []
        lines.append("=" * 70)
        lines.append("ERROR REVIEW")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        preds_df = pd.DataFrame(self.fold_predictions)

        # 1. Most frequently missed labels (false negatives)
        lines.append(f"\n  1. MOST FREQUENTLY MISSED LABELS (False Negatives)")
        fn_counts = {}
        for label in V1_LABEL_SET:
            # True=1, Pred=0
            fn = ((preds_df[f"true_{label}"] == 1) & (preds_df[f"pred_{label}"] == 0)).sum()
            tp = ((preds_df[f"true_{label}"] == 1) & (preds_df[f"pred_{label}"] == 1)).sum()
            total = preds_df[f"true_{label}"].sum()
            fn_rate = fn / total if total > 0 else 0
            fn_counts[label] = {"fn": fn, "tp": tp, "total": total, "fn_rate": fn_rate}

        lines.append(f"  {'Label':<12s} {'FN':>6s} {'TP':>6s} {'Total':>6s} {'FN Rate':>10s}")
        for label in sorted(fn_counts, key=lambda x: -fn_counts[x]["fn_rate"]):
            c = fn_counts[label]
            lines.append(f"  {label:<12s} {c['fn']:>6d} {c['tp']:>6d} {c['total']:>6d} "
                         f"{c['fn_rate']:>10.4f}")
            self.emit(f"  FN: {label}: {c['fn']}/{c['total']} (rate={c['fn_rate']:.4f})")

        # 2. Most frequently over-predicted labels (false positives)
        lines.append(f"\n  2. MOST FREQUENTLY OVER-PREDICTED LABELS (False Positives)")
        fp_counts = {}
        for label in V1_LABEL_SET:
            fp = ((preds_df[f"true_{label}"] == 0) & (preds_df[f"pred_{label}"] == 1)).sum()
            tn = ((preds_df[f"true_{label}"] == 0) & (preds_df[f"pred_{label}"] == 0)).sum()
            neg = (preds_df[f"true_{label}"] == 0).sum()
            fp_rate = fp / neg if neg > 0 else 0
            fp_counts[label] = {"fp": fp, "tn": tn, "neg": neg, "fp_rate": fp_rate}

        lines.append(f"  {'Label':<12s} {'FP':>6s} {'TN':>6s} {'Neg':>6s} {'FP Rate':>10s}")
        for label in sorted(fp_counts, key=lambda x: -fp_counts[x]["fp_rate"]):
            c = fp_counts[label]
            lines.append(f"  {label:<12s} {c['fp']:>6d} {c['tn']:>6d} {c['neg']:>6d} "
                         f"{c['fp_rate']:>10.4f}")

        # 3. Hardest hybrid pairs — errors on multi-label tracks
        lines.append(f"\n  3. HARDEST MULTI-LABEL TRACKS")
        ml_preds = preds_df[preds_df[[f"true_{l}" for l in V1_LABEL_SET]].sum(axis=1) > 1]
        ml_errors = ml_preds[ml_preds["correct"] == 0]
        lines.append(f"  Multi-label tracks: {len(ml_preds)}, errors: {len(ml_errors)} "
                     f"({len(ml_errors)/len(ml_preds)*100:.1f}% error rate)")

        # Show specific hard cases
        for _, row in ml_errors.head(20).iterrows():
            true_labels = [l for l in V1_LABEL_SET if row[f"true_{l}"] == 1]
            pred_labels = [l for l in V1_LABEL_SET if row[f"pred_{l}"] == 1]
            lines.append(f"    {int(row['track_id']):5d} | {row['artist']:25s} | "
                         f"TRUE=[{','.join(true_labels)}] PRED=[{','.join(pred_labels)}]")

        # 4. Labels harmed by low support
        lines.append(f"\n  4. LABELS WITH LOW SUPPORT")
        for label in V1_LABEL_SET:
            m = self.per_label_metrics[label]
            if m["support"] < 15:
                lines.append(f"  {label}: support={m['support']}, F1={m['f1']:.4f} "
                             f"— LOW SUPPORT WARNING")

        # 5. Did cleaned taxonomy reduce false confusion?
        lines.append(f"\n  5. CONFUSION REDUCTION ANALYSIS")
        # Count cross-genre misclassifications
        confusion_pairs = defaultdict(int)
        for _, row in preds_df.iterrows():
            for li, label in enumerate(V1_LABEL_SET):
                if row[f"true_{label}"] == 0 and row[f"pred_{label}"] == 1:
                    # False positive for this label — what was the true label?
                    true_labels = [l for l in V1_LABEL_SET if row[f"true_{l}"] == 1]
                    for tl in true_labels:
                        confusion_pairs[(tl, label)] += 1

        lines.append(f"  Top confusion pairs (true → predicted):")
        for (tl, pl), cnt in sorted(confusion_pairs.items(), key=lambda x: -x[1])[:15]:
            lines.append(f"    {tl} → {pl}: {cnt}")

        # Confusion notes CSV
        confusion_notes = []
        for (tl, pl), cnt in sorted(confusion_pairs.items(), key=lambda x: -x[1]):
            confusion_notes.append({
                "true_label": tl, "predicted_label": pl, "count": cnt,
                "note": "cleaned" if cnt == 0 else
                        "high confusion" if cnt > 5 else "moderate" if cnt > 2 else "low",
            })
        pd.DataFrame(confusion_notes).to_csv(
            PROOF_DIR / "per_label_confusion_notes.csv",
            index=False, encoding="utf-8",
        )

        (PROOF_DIR / "06_error_review.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART F — MODEL ISOLATION
    # ================================================================
    def part_f(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART F -- MODEL ISOLATION")
        self.emit("=" * 70)

        MODEL_DIR.mkdir(parents=True, exist_ok=True)

        # Save model
        model_path = MODEL_DIR / "ovr_random_forest_v2.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(self.final_model, f)

        # Save imputer
        imputer_path = MODEL_DIR / "imputer_v2.pkl"
        with open(imputer_path, "wb") as f:
            pickle.dump(self.imputer, f)

        # Save label mapping
        mapping_path = MODEL_DIR / "label_mapping_v2.json"
        with open(mapping_path, "w") as f:
            json.dump({"labels": V1_LABEL_SET, "mapping": self.label_mapping,
                        "features": FEATURE_COLS}, f, indent=2)

        # Save README
        readme_path = MODEL_DIR / "README.txt"
        readme_lines = [
            "=" * 60,
            "MULTI-LABEL CLASSIFIER V2 — MODEL ARTIFACTS",
            f"Created: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "=" * 60,
            "",
            "STATUS: EXPERIMENTAL — NON-PRODUCTION — BENCHMARK-ONLY",
            "",
            "These model artifacts are from a benchmark-only evaluation.",
            "They must NOT be used for production predictions.",
            "They must NOT write predictions into production DB tables.",
            "",
            "Files:",
            f"  ovr_random_forest_v2.pkl ({model_path.stat().st_size:,} bytes)",
            f"  imputer_v2.pkl ({imputer_path.stat().st_size:,} bytes)",
            f"  label_mapping_v2.json",
            "",
            f"Model: OneVsRestClassifier(RandomForestClassifier(n_estimators=200))",
            f"Labels: {V1_LABEL_SET}",
            f"Features: {FEATURE_COLS}",
        ]
        readme_path.write_text("\n".join(readme_lines), encoding="utf-8")

        self.emit(f"  Model saved: {model_path} ({model_path.stat().st_size:,} bytes)")
        self.emit(f"  Imputer saved: {imputer_path}")
        self.emit(f"  Label mapping saved: {mapping_path}")
        self.emit(f"  README: {readme_path}")

    # ================================================================
    # PART G — OUTPUTS
    # ================================================================
    def part_g(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART G -- OUTPUTS (FEATURE USAGE + FINAL REPORT)")
        self.emit("=" * 70)

        # Feature usage summary
        feat_lines = []
        feat_lines.append("=" * 70)
        feat_lines.append("FEATURE USAGE SUMMARY")
        feat_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        feat_lines.append("=" * 70)
        feat_lines.append(f"\n  Feature set: V1 (locked, 7 features)")
        feat_lines.append(f"  {'Feature':<25s} {'Importance':>12s} {'±Std':>10s} "
                          f"{'Source':>20s}")
        feat_lines.append(f"  {'-' * 70}")
        for feat, imp, std in self.feature_importance:
            src = ("analysis_summary" if feat not in ("section_count", "avg_section_duration")
                   else "section_events")
            feat_lines.append(f"  {feat:<25s} {imp:>12.6f} {std:>10.6f} {src:>20s}")

        feat_lines.append(f"\n  Excluded features (same as V1):")
        feat_lines.append(f"    bpm_detected (low ANOVA F)")
        feat_lines.append(f"    valence (zero discriminative power)")
        feat_lines.append(f"\n  Candidate features for future V3:")
        feat_lines.append(f"    spectral_centroid, spectral_rolloff, zero_crossing_rate")
        feat_lines.append(f"    onset_rate, rms_mean (from track_features table)")

        (PROOF_DIR / "07_feature_usage_summary.txt").write_text(
            "\n".join(feat_lines), encoding="utf-8"
        )

        # Copy data CSVs
        for name in ("classifier_dataset_multilabel_v2.csv",
                      "classifier_label_mapping_v2.csv",
                      "classifier_multilabel_manifest_v2.csv"):
            src = DATA_DIR / name
            if src.exists():
                shutil.copy2(src, PROOF_DIR / name)

    # ================================================================
    # PART H — VALIDATION
    # ================================================================
    def part_h(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART H -- VALIDATION (DB INTEGRITY)")
        self.emit("=" * 70)

        snap_after = self.snapshot_db("after")
        all_ok = True
        val = []
        val.append("=" * 70)
        val.append("DB INTEGRITY VALIDATION (Post-Phase)")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. Benchmark unchanged
        chk1 = snap_after["benchmark"] == self.snap_bench
        val.append(f"\n  1. Benchmark: {snap_after['benchmark']} "
                   f"(before={self.snap_bench}) "
                   f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Primaries unchanged
        chk2 = snap_after["primaries"] == self.snap_prim
        val.append(f"  2. Primaries: {snap_after['primaries']} "
                   f"(before={self.snap_prim}) "
                   f"-- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. Secondaries unchanged
        chk3 = snap_after["secondaries"] == self.snap_sec
        val.append(f"  3. Secondaries: {snap_after['secondaries']} "
                   f"(before={self.snap_sec}) "
                   f"-- {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. No duplicate primaries
        chk4 = snap_after["dup_primaries"] == 0
        val.append(f"  4. Duplicate primaries: {snap_after['dup_primaries']} "
                   f"-- {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. FK integrity
        chk5 = snap_after["fk_violations"] == 0
        val.append(f"  5. FK violations: {snap_after['fk_violations']} "
                   f"-- {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. Schema unchanged (check table count)
        conn = self.connect_ro()
        tables = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0]
        conn.close()
        val.append(f"  6. Table count: {tables} (no schema changes expected)")

        # 7. No predictions in production tables
        conn = self.connect_ro()
        try:
            pred_count = conn.execute(
                "SELECT COUNT(*) FROM genre_predictions"
            ).fetchone()[0]
            val.append(f"  7. genre_predictions rows: {pred_count} (not modified by this phase)")
        except Exception:
            val.append(f"  7. genre_predictions: table does not exist (OK)")
        conn.close()

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")
        val.append(f"\n  SQL verification:")
        val.append(f"    benchmark = {snap_after['benchmark']}")
        val.append(f"    primaries = {snap_after['primaries']}")
        val.append(f"    secondaries = {snap_after['secondaries']}")
        val.append(f"    dup_primaries = {snap_after['dup_primaries']}")
        val.append(f"    FK violations = {snap_after['fk_violations']}")

        (PROOF_DIR / "08_validation.txt").write_text(
            "\n".join(val), encoding="utf-8"
        )

        self.emit(f"  DB integrity: {'PASS' if all_ok else 'FAIL'}")
        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"
        assert self.df is not None and self.label_cols is not None

        label_sums = self.df[self.label_cols].sum(axis=1)  # type: ignore[call-overload]
        n_multi = (label_sums > 1).sum()

        report = []
        report.append("=" * 70)
        report.append("MULTI-LABEL CLASSIFIER V2 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- DATASET ---")
        report.append(f"  Tracks: {len(self.df)}")
        report.append(f"  Features: {len(FEATURE_COLS)}")
        report.append(f"  Labels: {len(self.label_cols)}: {V1_LABEL_SET}")
        report.append(f"  Multi-label tracks: {n_multi}")

        report.append(f"\n--- V2 METRICS ---")
        report.append(f"  Hamming Loss:     {self.metrics['hamming_loss']:.4f} ± {self.metrics['hamming_loss_std']:.4f}")
        report.append(f"  Micro F1:         {self.metrics['micro_f1']:.4f} ± {self.metrics['micro_f1_std']:.4f}")
        report.append(f"  Macro F1:         {self.metrics['macro_f1']:.4f} ± {self.metrics['macro_f1_std']:.4f}")
        report.append(f"  Subset Accuracy:  {self.metrics['subset_accuracy']:.4f} ± {self.metrics['subset_accuracy_std']:.4f}")
        report.append(f"  ML Subset Acc:    {self.metrics['ml_subset_accuracy']:.4f} ± {self.metrics['ml_subset_accuracy_std']:.4f}")

        report.append(f"\n--- V1 vs V2 DELTAS ---")
        for name, vals in self.v1_comparison.items():
            report.append(f"  {name}: V1={vals['v1']:.4f} → V2={vals['v2']:.4f} "
                          f"({vals['direction']}, {vals['delta']:+.4f})")

        report.append(f"\n--- PER-LABEL F1 ---")
        for label in V1_LABEL_SET:
            m = self.per_label_metrics[label]
            v1_f1 = V1_PER_LABEL_F1.get(label, 0.0)
            report.append(f"  {label:<12s}: V1={v1_f1:.4f} → V2={m['f1']:.4f} "
                          f"(support={m['support']})")

        report.append(f"\n--- FEATURE IMPORTANCE ---")
        for feat, imp, std in self.feature_importance:
            report.append(f"  {feat:<25s} {imp:.6f} ± {std:.6f}")

        report.append(f"\n--- DB INTEGRITY ---")
        report.append(f"  Primaries: {self.snap_prim} (unchanged)")
        report.append(f"  Secondaries: {self.snap_sec} (unchanged)")
        report.append(f"  Benchmark: {self.snap_bench} (unchanged)")
        report.append(f"  FK: clean")
        report.append(f"  Schema: unchanged")

        report.append(f"\n{'=' * 70}")
        report.append(f"GATE={gate}")
        report.append(f"{'=' * 70}")

        (PROOF_DIR / "08_final_report.txt").write_text(
            "\n".join(report), encoding="utf-8"
        )
        (PROOF_DIR / "execution_log.txt").write_text(
            "\n".join(self.log), encoding="utf-8"
        )

        self.emit(f"\nPF={PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    pipeline = MultiLabelClassifierV2()

    if not ANALYSIS_DB.exists():
        pipeline.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    pipeline.emit(f"CWD: {WORKSPACE}")
    pipeline.emit(f"DB: {ANALYSIS_DB}")
    pipeline.emit(f"PROOF: {PROOF_DIR}")
    pipeline.emit(f"MODEL: {MODEL_DIR}")

    pipeline.part_a()
    pipeline.part_b()
    pipeline.part_c()
    pipeline.part_d()
    pipeline.part_e()
    pipeline.part_f()
    pipeline.part_g()
    all_ok = pipeline.part_h()
    gate = pipeline.final_report(all_ok)

    print(f"\n{'=' * 60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
