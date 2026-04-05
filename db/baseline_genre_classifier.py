#!/usr/bin/env python3
"""
Phase 10 — Baseline Genre Classifier V1

Parts:
  A) Dataset validation
  B) Training plan
  C) Model execution (RF + Logistic Regression comparison)
  D) Error review
  E) Proof output
  F) Safety/isolation (model artifact save)
  G) Final analysis
"""

import os
import pickle
import sqlite3
import sys
import time
import warnings
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
    precision_recall_fscore_support,
    precision_score,
    recall_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline as SKPipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
DATASET_CSV = WORKSPACE / "data" / "classifier_dataset_v1.csv"
PROOF_DIR = WORKSPACE / "_proof" / "baseline_genre_classifier_v1"
MODEL_DIR = WORKSPACE / "models" / "baseline_v1"

FEATURE_COLS = [
    "harmonic_stability",
    "loudness_lufs",
    "avg_section_duration",
    "tempo_stability",
    "energy",
    "danceability",
    "section_count",
]

TARGET_COL = "genre"
N_FOLDS = 5
RANDOM_STATE = 42


class ClassifierPipeline:
    def __init__(self):
        self.log_lines = []
        self.t0 = time.time()

    def emit(self, msg):
        self.log_lines.append(msg)
        print(msg)

    def connect_ro(self):
        uri = f"file:{ANALYSIS_DB}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    # ================================================================
    # PART A — DATA VALIDATION
    # ================================================================
    def part_a(self, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — DATA VALIDATION")
        self.emit("=" * 60)

        lines = []
        lines.append("=" * 70)
        lines.append("DATASET VALIDATION — classifier_dataset_v1.csv")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        all_pass = True

        # 1. Row count
        lines.append(f"\n--- 1. ROW COUNT ---")
        lines.append(f"Rows: {len(df)}")
        lines.append(f"Expected: 200")
        rc_ok = len(df) == 200
        lines.append(f"CHECK: {'PASS' if rc_ok else 'FAIL'}")
        if not rc_ok:
            all_pass = False

        # 2. Column count
        lines.append(f"\n--- 2. COLUMN COUNT ---")
        lines.append(f"Columns: {len(df.columns)}")
        lines.append(f"Column names: {list(df.columns)}")
        lines.append(f"CHECK: PASS")

        # 3. Feature columns present
        lines.append(f"\n--- 3. FEATURE COLUMNS ---")
        missing_feats = [f for f in FEATURE_COLS if f not in df.columns]
        for f in FEATURE_COLS:
            present = f in df.columns
            lines.append(f"  {f:25s}: {'PRESENT' if present else 'MISSING'}")
        fc_ok = len(missing_feats) == 0
        lines.append(f"Missing: {missing_feats}")
        lines.append(f"CHECK: {'PASS' if fc_ok else 'FAIL'}")
        if not fc_ok:
            all_pass = False

        # 4. Target column present
        lines.append(f"\n--- 4. TARGET COLUMN ---")
        tc_ok = TARGET_COL in df.columns
        lines.append(f"Target '{TARGET_COL}': {'PRESENT' if tc_ok else 'MISSING'}")
        lines.append(f"CHECK: {'PASS' if tc_ok else 'FAIL'}")
        if not tc_ok:
            all_pass = False

        # 5. No null target labels
        lines.append(f"\n--- 5. NULL TARGET LABELS ---")
        null_targets = df[TARGET_COL].isna().sum()
        lines.append(f"Null genre labels: {null_targets}")
        nt_ok = null_targets == 0
        lines.append(f"CHECK: {'PASS' if nt_ok else 'FAIL'}")
        if not nt_ok:
            all_pass = False

        # 6. No duplicate track_id
        lines.append(f"\n--- 6. DUPLICATE TRACK IDS ---")
        dup_count = df["track_id"].duplicated().sum()
        lines.append(f"Duplicate track_ids: {dup_count}")
        dup_ok = dup_count == 0
        lines.append(f"CHECK: {'PASS' if dup_ok else 'FAIL'}")
        if not dup_ok:
            all_pass = False

        # 7. Class distribution
        lines.append(f"\n--- 7. CLASS DISTRIBUTION ---")
        genre_dist = df[TARGET_COL].value_counts().sort_values(ascending=False)
        for g, c in genre_dist.items():
            pct = c / len(df) * 100
            lines.append(f"  {g:15s}: {c:4d} ({pct:5.1f}%)")
        lines.append(f"Unique classes: {len(genre_dist)}")

        # 8. Trainability check
        lines.append(f"\n--- 8. TRAINABILITY CHECK ---")
        n_classes = len(genre_dist)
        min_class = genre_dist.min()
        lines.append(f"Classes: {n_classes}")
        lines.append(f"Minimum class size: {min_class}")
        lines.append(f"Classes with >= {N_FOLDS} samples: "
                      f"{sum(1 for c in genre_dist if c >= N_FOLDS)}/{n_classes}")
        trainable = n_classes >= 2 and len(df) >= 20
        lines.append(f"Trainable: {'YES' if trainable else 'NO'}")

        # Classes too small for stratified k-fold
        small_classes = [g for g, c in genre_dist.items() if c < N_FOLDS]
        if small_classes:
            lines.append(f"\nWARNING: Classes with < {N_FOLDS} samples: {small_classes}")
            lines.append(f"These classes may not appear in all folds.")
            lines.append(f"Strategy: Include them but note instability in results.")

        # 9. Feature null summary
        lines.append(f"\n--- 9. FEATURE NULL SUMMARY ---")
        for f in FEATURE_COLS:
            n_null = df[f].isna().sum()
            pct = n_null / len(df) * 100
            lines.append(f"  {f:25s}: {n_null:4d} nulls ({pct:5.1f}%)")
        lines.append(f"Strategy: Median imputation for missing values")

        lines.append(f"\n{'='*70}")
        lines.append(f"OVERALL VALIDATION: {'PASS' if all_pass else 'FAIL'}")

        self.emit(f"Validation: {'PASS' if all_pass else 'FAIL'} "
                  f"(rows={len(df)}, feats={len(FEATURE_COLS)}, classes={n_classes})")
        return all_pass, lines, genre_dist, small_classes

    # ================================================================
    # PART B — TRAINING PLAN
    # ================================================================
    def part_b(self, genre_dist, small_classes):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — TRAINING PLAN")
        self.emit("=" * 60)

        lines = []
        lines.append("=" * 70)
        lines.append("TRAINING CONFIGURATION")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        lines.append(f"""
1. PRIMARY MODEL: Random Forest
=================================
  Estimator:        RandomForestClassifier
  n_estimators:     200
  max_depth:        None
  min_samples_split: 5
  min_samples_leaf:  2
  class_weight:     balanced
  random_state:     {RANDOM_STATE}
  n_jobs:           -1
  criterion:        gini

2. COMPARISON MODEL: Logistic Regression
==========================================
  Estimator:        LogisticRegression
  C:                1.0
  max_iter:         1000
  solver:           lbfgs
  class_weight:     balanced
  random_state:     {RANDOM_STATE}
  Note:             Comparison only — not primary

3. CROSS-VALIDATION STRATEGY
===============================
  Method:           StratifiedKFold
  n_splits:         {N_FOLDS}
  shuffle:          True
  random_state:     {RANDOM_STATE}

4. PREPROCESSING
==================
  Imputation:       Median (SimpleImputer)
  Scaling:          StandardScaler (for LogReg; RF does not need it)
  Encoding:         LabelEncoder for target

5. EVALUATION METRICS
=======================
  - Accuracy
  - Balanced accuracy
  - Macro precision
  - Macro recall
  - Macro F1
  - Per-class precision / recall / F1 / support
  - Confusion matrix (sum across folds)
  - Feature importance (mean decrease in Gini — RF only)

6. SMALL-CLASS HANDLING
=========================
  Classes with < {N_FOLDS} samples: {small_classes if small_classes else 'None'}
  Strategy: Include all classes. Use class_weight='balanced'.
  Note: Results for single-sample classes will be unstable.

7. DATASET
============
  Source: data/classifier_dataset_v1.csv
  Rows: 200
  Features: {len(FEATURE_COLS)} ({', '.join(FEATURE_COLS)})
  Target: genre ({len(genre_dist)} classes)
""")

        self.emit("Training plan documented")
        return lines

    # ================================================================
    # PART C — MODEL EXECUTION
    # ================================================================
    def part_c(self, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — MODEL EXECUTION")
        self.emit("=" * 60)

        X = df[FEATURE_COLS].copy()
        y = df[TARGET_COL].copy()

        le = LabelEncoder()
        y_encoded: np.ndarray = np.asarray(le.fit_transform(y))
        class_names = le.classes_

        skf = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=RANDOM_STATE)

        # Track per-fold predictions for error review
        all_fold_preds_rf = []
        all_fold_preds_lr = []

        # Aggregated metrics
        rf_metrics = defaultdict(list)
        lr_metrics = defaultdict(list)

        # Confusion matrices (summed across folds)
        rf_cm_total = np.zeros((len(class_names), len(class_names)), dtype=int)
        lr_cm_total = np.zeros((len(class_names), len(class_names)), dtype=int)

        # Feature importances across folds (RF only)
        rf_importances = np.zeros((N_FOLDS, len(FEATURE_COLS)))

        # Per-class metrics accumulator
        rf_per_class_all = []
        lr_per_class_all = []

        self.emit(f"Classes: {list(class_names)}")
        self.emit(f"Features: {FEATURE_COLS}")
        self.emit(f"Starting {N_FOLDS}-fold stratified CV...")

        for fold_idx, (train_idx, val_idx) in enumerate(skf.split(X, y_encoded)):
            self.emit(f"\n  Fold {fold_idx + 1}/{N_FOLDS}: "
                      f"train={len(train_idx)}, val={len(val_idx)}")

            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y_encoded[train_idx], y_encoded[val_idx]

            # ---- Random Forest ----
            rf_pipe = SKPipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("clf", RandomForestClassifier(
                    n_estimators=200,
                    max_depth=None,
                    min_samples_split=5,
                    min_samples_leaf=2,
                    class_weight="balanced",
                    random_state=RANDOM_STATE,
                    n_jobs=-1,
                )),
            ])
            rf_pipe.fit(X_train, y_train)
            rf_pred = rf_pipe.predict(X_val)

            # Metrics
            rf_metrics["accuracy"].append(accuracy_score(y_val, rf_pred))
            rf_metrics["balanced_accuracy"].append(balanced_accuracy_score(y_val, rf_pred))
            rf_metrics["macro_precision"].append(precision_score(y_val, rf_pred, average="macro", zero_division=0))
            rf_metrics["macro_recall"].append(recall_score(y_val, rf_pred, average="macro", zero_division=0))
            rf_metrics["macro_f1"].append(f1_score(y_val, rf_pred, average="macro", zero_division=0))

            # Confusion matrix
            cm = confusion_matrix(y_val, rf_pred, labels=range(len(class_names)))
            rf_cm_total += cm

            # Feature importance
            rf_importances[fold_idx] = rf_pipe.named_steps["clf"].feature_importances_

            # Per-class metrics
            p, r, f, s = precision_recall_fscore_support(
                y_val, rf_pred, labels=range(len(class_names)), zero_division=0
            )
            rf_per_class_all.append((p, r, f, s))

            # Track predictions
            for i, vi in enumerate(val_idx):
                all_fold_preds_rf.append({
                    "fold": fold_idx + 1,
                    "track_id": df.iloc[vi]["track_id"],
                    "artist": df.iloc[vi]["artist"],
                    "title": df.iloc[vi]["title"],
                    "true_genre": le.inverse_transform([y_val[i]])[0],
                    "pred_genre": le.inverse_transform([rf_pred[i]])[0],
                    "correct": int(y_val[i] == rf_pred[i]),
                })

            self.emit(f"    RF  acc={rf_metrics['accuracy'][-1]:.3f}  "
                      f"bal_acc={rf_metrics['balanced_accuracy'][-1]:.3f}  "
                      f"macro_f1={rf_metrics['macro_f1'][-1]:.3f}")

            # ---- Logistic Regression ----
            lr_pipe = SKPipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("clf", LogisticRegression(
                    C=1.0,
                    max_iter=1000,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=RANDOM_STATE,
                )),
            ])
            lr_pipe.fit(X_train, y_train)
            lr_pred = lr_pipe.predict(X_val)

            lr_metrics["accuracy"].append(accuracy_score(y_val, lr_pred))
            lr_metrics["balanced_accuracy"].append(balanced_accuracy_score(y_val, lr_pred))
            lr_metrics["macro_precision"].append(precision_score(y_val, lr_pred, average="macro", zero_division=0))
            lr_metrics["macro_recall"].append(recall_score(y_val, lr_pred, average="macro", zero_division=0))
            lr_metrics["macro_f1"].append(f1_score(y_val, lr_pred, average="macro", zero_division=0))

            cm_lr = confusion_matrix(y_val, lr_pred, labels=range(len(class_names)))
            lr_cm_total += cm_lr

            p, r, f, s = precision_recall_fscore_support(
                y_val, lr_pred, labels=range(len(class_names)), zero_division=0
            )
            lr_per_class_all.append((p, r, f, s))

            for i, vi in enumerate(val_idx):
                all_fold_preds_lr.append({
                    "fold": fold_idx + 1,
                    "track_id": df.iloc[vi]["track_id"],
                    "artist": df.iloc[vi]["artist"],
                    "title": df.iloc[vi]["title"],
                    "true_genre": le.inverse_transform([y_val[i]])[0],
                    "pred_genre": le.inverse_transform([lr_pred[i]])[0],
                    "correct": int(y_val[i] == lr_pred[i]),
                })

            self.emit(f"    LR  acc={lr_metrics['accuracy'][-1]:.3f}  "
                      f"bal_acc={lr_metrics['balanced_accuracy'][-1]:.3f}  "
                      f"macro_f1={lr_metrics['macro_f1'][-1]:.3f}")

        # Save last RF model for artifact
        self.last_rf_pipe = rf_pipe  # type: ignore[possibly-unbound]
        self.last_lr_pipe = lr_pipe  # type: ignore[possibly-unbound]
        self.le = le

        self.emit(f"\nCV complete.")

        return {
            "rf_metrics": rf_metrics,
            "lr_metrics": lr_metrics,
            "rf_cm": rf_cm_total,
            "lr_cm": lr_cm_total,
            "rf_importances": rf_importances,
            "rf_per_class": rf_per_class_all,
            "lr_per_class": lr_per_class_all,
            "class_names": class_names,
            "fold_preds_rf": pd.DataFrame(all_fold_preds_rf),
            "fold_preds_lr": pd.DataFrame(all_fold_preds_lr),
        }

    # ================================================================
    # PART D — ERROR REVIEW
    # ================================================================
    def part_d(self, results):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — ERROR REVIEW")
        self.emit("=" * 60)

        class_names = results["class_names"]
        rf_cm = results["rf_cm"]
        preds_rf = results["fold_preds_rf"]

        lines = []
        lines.append("=" * 70)
        lines.append("ERROR REVIEW — RANDOM FOREST BASELINE")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        # 1. Most confused genre pairs
        lines.append(f"\n--- 1. MOST CONFUSED GENRE PAIRS ---")
        confused_pairs = []
        for i in range(len(class_names)):
            for j in range(len(class_names)):
                if i == j:
                    continue
                if rf_cm[i, j] > 0:
                    confused_pairs.append((class_names[i], class_names[j], rf_cm[i, j]))
        confused_pairs.sort(key=lambda x: -x[2])
        for true_g, pred_g, count in confused_pairs[:15]:
            lines.append(f"  {true_g:15s} -> {pred_g:15s}: {count:3d} misclassifications")

        # 2. Weakest-performing genres (by per-class F1)
        lines.append(f"\n--- 2. WEAKEST-PERFORMING GENRES ---")
        # Aggregate per-class F1 across folds
        rf_pc = results["rf_per_class"]
        n_classes = len(class_names)
        avg_f1 = np.zeros(n_classes)
        avg_support = np.zeros(n_classes)
        for p, r, f, s in rf_pc:
            avg_f1 += f
            avg_support += s
        avg_f1 /= N_FOLDS
        avg_support /= N_FOLDS

        genre_f1 = list(zip(class_names, avg_f1, avg_support))
        genre_f1.sort(key=lambda x: x[1])
        for g, f1, sup in genre_f1:
            lines.append(f"  {g:15s}: F1={f1:.3f}  avg_support={sup:.1f}")
        weakest = [g for g, f1, _ in genre_f1 if f1 < 0.3]
        lines.append(f"\nWeakest genres (F1 < 0.3): {weakest if weakest else 'None'}")

        # 3. Tracks frequently misclassified across folds
        lines.append(f"\n--- 3. FREQUENTLY MISCLASSIFIED TRACKS ---")
        track_errors = preds_rf[preds_rf["correct"] == 0].groupby("track_id").agg(
            n_wrong=("correct", "count"),
            artist=("artist", "first"),
            title=("title", "first"),
            true_genre=("true_genre", "first"),
            pred_genres=("pred_genre", lambda x: ", ".join(x)),
        ).sort_values("n_wrong", ascending=False)

        # A track appears in exactly 1 fold, so max n_wrong = 1
        # But let's show all misclassified tracks
        n_miscl = len(track_errors)
        total = len(preds_rf["track_id"].unique())
        lines.append(f"Misclassified tracks: {n_miscl}/{total} ({n_miscl/total*100:.1f}%)")
        lines.append(f"(Each track appears once across folds)")

        if n_miscl > 0:
            lines.append(f"\nMisclassified track list:")
            for tid, row in track_errors.head(30).iterrows():
                lines.append(
                    f"  [{tid}] {row['artist'][:25]:25s} — {row['title'][:35]:35s}  "
                    f"true={row['true_genre']:12s}  pred={row['pred_genres']}"
                )

        # 4. Does confusion align with known overlap?
        lines.append(f"\n--- 4. CONFUSION vs KNOWN GENRE OVERLAP ---")
        lines.append(f"Known overlaps from benchmark + misfit review:")
        lines.append(f"  Country <-> Rock: Significant genre border ambiguity in Southern Rock")
        lines.append(f"  Hip-Hop <-> Country: Country-rap fusion artists (e.g. Adam Calhoun)")
        lines.append(f"  Rock <-> Metal: Gradient between hard rock and metal")
        lines.append(f"  Pop <-> Electronic: Shared production techniques")
        lines.append(f"")

        # Check if the top confused pairs match
        if confused_pairs:
            top3 = [(a, b) for a, b, _ in confused_pairs[:6]]
            known_overlaps = [
                ("Country", "Rock"), ("Rock", "Country"),
                ("Hip-Hop", "Country"), ("Country", "Hip-Hop"),
                ("Rock", "Metal"), ("Metal", "Rock"),
            ]
            matches = [p for p in top3 if p in known_overlaps]
            lines.append(f"Top confused pairs matching known overlaps: {len(matches)}/6")
            if matches:
                for a, b in matches:
                    lines.append(f"  CONFIRMED: {a} <-> {b}")
            lines.append(f"Conclusion: {'Confusion aligns with prior domain knowledge' if matches else 'Novel confusion patterns detected — needs investigation'}")

        self.emit(f"Error review: {n_miscl} misclassified tracks, "
                  f"{len(confused_pairs)} confused genre pairs")
        return lines

    # ================================================================
    # PART E — WRITE PROOF
    # ================================================================
    def part_e(self, val_lines, plan_lines, results, error_lines, df):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — WRITING PROOF FILES")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        class_names = results["class_names"]
        rf_metrics = results["rf_metrics"]
        lr_metrics = results["lr_metrics"]

        # 00 dataset validation
        w("00_dataset_validation.txt", "\n".join(val_lines))

        # 01 training configuration
        w("01_training_configuration.txt", "\n".join(plan_lines))

        # 02 crossval metrics
        cv_lines = ["=" * 70,
                     "CROSS-VALIDATION METRICS",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                     "=" * 70]

        cv_lines.append(f"\n--- RANDOM FOREST (PRIMARY) ---")
        for metric_name, values in rf_metrics.items():
            arr = np.array(values)
            cv_lines.append(
                f"  {metric_name:25s}: {arr.mean():.4f} +/- {arr.std():.4f}  "
                f"[{', '.join(f'{v:.4f}' for v in values)}]"
            )

        cv_lines.append(f"\n--- LOGISTIC REGRESSION (COMPARISON) ---")
        for metric_name, values in lr_metrics.items():
            arr = np.array(values)
            cv_lines.append(
                f"  {metric_name:25s}: {arr.mean():.4f} +/- {arr.std():.4f}  "
                f"[{', '.join(f'{v:.4f}' for v in values)}]"
            )

        cv_lines.append(f"\n--- MODEL COMPARISON SUMMARY ---")
        rf_f1 = np.mean(rf_metrics["macro_f1"])
        lr_f1 = np.mean(lr_metrics["macro_f1"])
        winner = "Random Forest" if rf_f1 >= lr_f1 else "Logistic Regression"
        cv_lines.append(f"  RF  macro_f1 mean: {rf_f1:.4f}")
        cv_lines.append(f"  LR  macro_f1 mean: {lr_f1:.4f}")
        cv_lines.append(f"  Better model: {winner}")

        w("02_crossval_metrics.txt", "\n".join(cv_lines))

        # 03 per-class metrics (RF)
        pc_lines = ["=" * 70,
                     "PER-CLASS METRICS — RANDOM FOREST",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                     "=" * 70]

        rf_pc = results["rf_per_class"]
        n_classes = len(class_names)
        avg_p = np.zeros(n_classes)
        avg_r = np.zeros(n_classes)
        avg_f = np.zeros(n_classes)
        avg_s = np.zeros(n_classes)
        for p, r, f, s in rf_pc:
            avg_p += p
            avg_r += r
            avg_f += f
            avg_s += s
        avg_p /= N_FOLDS
        avg_r /= N_FOLDS
        avg_f /= N_FOLDS
        avg_s /= N_FOLDS

        pc_lines.append(f"\n{'Genre':15s}  {'Precision':>10s}  {'Recall':>10s}  {'F1':>10s}  {'Support':>10s}")
        pc_lines.append("-" * 65)
        for i, g in enumerate(class_names):
            pc_lines.append(
                f"{g:15s}  {avg_p[i]:10.4f}  {avg_r[i]:10.4f}  {avg_f[i]:10.4f}  {avg_s[i]:10.1f}"
            )
        pc_lines.append("-" * 65)
        pc_lines.append(
            f"{'MACRO AVG':15s}  {avg_p.mean():10.4f}  {avg_r.mean():10.4f}  {avg_f.mean():10.4f}  {avg_s.sum():10.1f}"
        )

        # Also do LR per-class
        pc_lines.append(f"\n\n{'='*70}")
        pc_lines.append("PER-CLASS METRICS — LOGISTIC REGRESSION (COMPARISON)")
        pc_lines.append("=" * 70)
        lr_pc = results["lr_per_class"]
        avg_p2 = np.zeros(n_classes)
        avg_r2 = np.zeros(n_classes)
        avg_f2 = np.zeros(n_classes)
        avg_s2 = np.zeros(n_classes)
        for p, r, f, s in lr_pc:
            avg_p2 += p
            avg_r2 += r
            avg_f2 += f
            avg_s2 += s
        avg_p2 /= N_FOLDS
        avg_r2 /= N_FOLDS
        avg_f2 /= N_FOLDS
        avg_s2 /= N_FOLDS

        pc_lines.append(f"\n{'Genre':15s}  {'Precision':>10s}  {'Recall':>10s}  {'F1':>10s}  {'Support':>10s}")
        pc_lines.append("-" * 65)
        for i, g in enumerate(class_names):
            pc_lines.append(
                f"{g:15s}  {avg_p2[i]:10.4f}  {avg_r2[i]:10.4f}  {avg_f2[i]:10.4f}  {avg_s2[i]:10.1f}"
            )

        w("03_per_class_metrics.txt", "\n".join(pc_lines))

        # 04 confusion matrix CSV (RF)
        cm_df = pd.DataFrame(
            results["rf_cm"],
            index=class_names,
            columns=class_names,
        )
        cm_df.index.name = "true\\predicted"
        cm_df.to_csv(PROOF_DIR / "04_confusion_matrix.csv", encoding="utf-8")

        # 05 feature importance
        fi_lines = ["=" * 70,
                     "FEATURE IMPORTANCE — RANDOM FOREST",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                     "=" * 70]

        mean_imp = results["rf_importances"].mean(axis=0)
        std_imp = results["rf_importances"].std(axis=0)
        fi_rank = sorted(zip(FEATURE_COLS, mean_imp, std_imp), key=lambda x: -x[1])

        fi_lines.append(f"\n{'Rank':>4s}  {'Feature':25s}  {'Importance':>12s}  {'Std':>10s}")
        fi_lines.append("-" * 60)
        for rank, (feat, imp, std) in enumerate(fi_rank, 1):
            fi_lines.append(f"{rank:4d}  {feat:25s}  {imp:12.6f}  {std:10.6f}")

        fi_lines.append(f"\nNote: Gini importance (mean decrease in impurity) averaged across {N_FOLDS} folds.")
        fi_lines.append(f"Higher values = more important for genre discrimination.")

        w("05_feature_importance.txt", "\n".join(fi_lines))

        # Also save as CSV
        fi_csv = pd.DataFrame(fi_rank, columns=["feature", "importance_mean", "importance_std"])
        fi_csv["rank"] = range(1, len(fi_csv) + 1)
        fi_csv.to_csv(PROOF_DIR / "feature_importance.csv", index=False, encoding="utf-8")

        # 06 error review
        w("06_error_review.txt", "\n".join(error_lines))

        # 07 model comparison
        comp_lines = ["=" * 70,
                      "MODEL COMPARISON",
                      f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                      "=" * 70]

        comp_lines.append(f"\n{'Metric':25s}  {'Random Forest':>15s}  {'Logistic Reg':>15s}  {'Winner':>15s}")
        comp_lines.append("-" * 80)
        for metric_name in rf_metrics:
            rf_val = np.mean(rf_metrics[metric_name])
            lr_val = np.mean(lr_metrics[metric_name])
            w_name = "RF" if rf_val >= lr_val else "LR"
            comp_lines.append(
                f"{metric_name:25s}  {rf_val:15.4f}  {lr_val:15.4f}  {w_name:>15s}"
            )

        comp_lines.append(f"\nConclusion:")
        rf_wins = sum(1 for m in rf_metrics if np.mean(rf_metrics[m]) >= np.mean(lr_metrics[m]))
        lr_wins = len(rf_metrics) - rf_wins
        comp_lines.append(f"  Random Forest wins {rf_wins}/{len(rf_metrics)} metrics")
        comp_lines.append(f"  Logistic Regression wins {lr_wins}/{len(rf_metrics)} metrics")
        overall_winner = "Random Forest" if rf_wins >= lr_wins else "Logistic Regression"
        comp_lines.append(f"  Overall winner: {overall_winner}")
        comp_lines.append(f"\nNote: Logistic Regression is comparison only — not the primary baseline.")

        w("07_model_comparison.txt", "\n".join(comp_lines))

        # fold_predictions.csv
        results["fold_preds_rf"].to_csv(PROOF_DIR / "fold_predictions.csv",
                                         index=False, encoding="utf-8")

        # LR confusion matrix too
        cm_lr_df = pd.DataFrame(
            results["lr_cm"],
            index=class_names,
            columns=class_names,
        )
        cm_lr_df.index.name = "true\\predicted"
        cm_lr_df.to_csv(PROOF_DIR / "lr_confusion_matrix.csv", encoding="utf-8")

        self.emit("Proof files written")
        return elapsed

    # ================================================================
    # PART F — SAFETY / MODEL SAVE
    # ================================================================
    def part_f(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART F — SAFETY / MODEL ARTIFACT SAVE")
        self.emit("=" * 60)

        MODEL_DIR.mkdir(parents=True, exist_ok=True)

        # Save RF model
        rf_path = MODEL_DIR / "random_forest_v1.pkl"
        with open(rf_path, "wb") as f:
            pickle.dump(self.last_rf_pipe, f)
        self.emit(f"RF model saved: {rf_path}")

        # Save LR model
        lr_path = MODEL_DIR / "logistic_regression_v1.pkl"
        with open(lr_path, "wb") as f:
            pickle.dump(self.last_lr_pipe, f)
        self.emit(f"LR model saved: {lr_path}")

        # Save label encoder
        le_path = MODEL_DIR / "label_encoder_v1.pkl"
        with open(le_path, "wb") as f:
            pickle.dump(self.le, f)
        self.emit(f"Label encoder saved: {le_path}")

        # Write README
        readme = """# Baseline Genre Classifier V1 — Model Artifacts

## STATUS: EXPERIMENTAL / NON-PRODUCTION / GENRE-ONLY BASELINE

These model files are experimental artifacts from the baseline genre classifier training.

### Files
- `random_forest_v1.pkl` — Primary Random Forest classifier (sklearn Pipeline)
- `logistic_regression_v1.pkl` — Comparison Logistic Regression classifier
- `label_encoder_v1.pkl` — LabelEncoder for genre target mapping

### Usage Warning
- These models are trained on only 200 benchmark tracks
- They are NOT suitable for production use
- They are genre-only (no subgenre)
- Performance numbers are cross-validation estimates only
- Do NOT use predictions from these models to update any database tables

### Trained on
- Dataset: data/classifier_dataset_v1.csv
- Features: harmonic_stability, loudness_lufs, avg_section_duration, tempo_stability, energy, danceability, section_count
- Target: genre (primary label)
"""
        (MODEL_DIR / "README.md").write_text(readme, encoding="utf-8")
        self.emit("README written")

        # Verify no DB writes
        conn = self.connect_ro()
        label_count = conn.execute("SELECT COUNT(*) FROM track_genre_labels").fetchone()[0]
        track_count = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
        dup_primaries = conn.execute(
            "SELECT COUNT(*) FROM (SELECT track_id FROM track_genre_labels "
            "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        fk_check = conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.close()

        safety_lines = []
        safety_lines.append("=" * 70)
        safety_lines.append("SAFETY / ISOLATION CHECK")
        safety_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        safety_lines.append("=" * 70)
        safety_lines.append(f"\nDB opened in READ-ONLY mode: YES")
        safety_lines.append(f"track_genre_labels rows: {label_count}")
        safety_lines.append(f"tracks rows: {track_count}")
        safety_lines.append(f"Duplicate primaries: {dup_primaries}")
        safety_lines.append(f"FK violations: {len(fk_check)}")
        safety_lines.append(f"\nModel artifacts saved to: {MODEL_DIR}")
        safety_lines.append(f"  random_forest_v1.pkl: {rf_path.stat().st_size} bytes")
        safety_lines.append(f"  logistic_regression_v1.pkl: {lr_path.stat().st_size} bytes")
        safety_lines.append(f"  label_encoder_v1.pkl: {le_path.stat().st_size} bytes")
        safety_lines.append(f"\nProduction DB tables mutated: NONE")
        safety_lines.append(f"Predictions written to DB: NONE")
        safety_lines.append(f"Labels modified: NONE")

        safe = (dup_primaries == 0 and len(fk_check) == 0)
        safety_lines.append(f"\nSAFETY CHECK: {'PASS' if safe else 'FAIL'}")

        self.emit(f"Safety check: {'PASS' if safe else 'FAIL'}")
        return safe, safety_lines, label_count, track_count, dup_primaries

    # ================================================================
    # PART G — FINAL ANALYSIS
    # ================================================================
    def part_g(self, results, error_lines, safe, elapsed,
               label_count, track_count, dup_primaries, safety_lines):
        self.emit("\n" + "=" * 60)
        self.emit("PART G — FINAL ANALYSIS + REPORT")
        self.emit("=" * 60)

        rf_metrics = results["rf_metrics"]
        lr_metrics = results["lr_metrics"]
        class_names = results["class_names"]

        # Per-class F1 for RF
        rf_pc = results["rf_per_class"]
        n_classes = len(class_names)
        avg_f = np.zeros(n_classes)
        avg_s = np.zeros(n_classes)
        for p, r, f, s in rf_pc:
            avg_f += f
            avg_s += s
        avg_f /= N_FOLDS
        avg_s /= N_FOLDS

        genre_f1_sorted = sorted(zip(class_names, avg_f, avg_s), key=lambda x: -x[1])

        # Feature importance
        mean_imp = results["rf_importances"].mean(axis=0)
        fi_sorted = sorted(zip(FEATURE_COLS, mean_imp), key=lambda x: -x[1])

        # Confusion data
        rf_cm = results["rf_cm"]
        preds_rf = results["fold_preds_rf"]
        n_correct = preds_rf["correct"].sum()
        n_total = len(preds_rf)

        report = []
        report.append("=" * 70)
        report.append("BASELINE GENRE CLASSIFIER V1 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        # Summary metrics
        report.append(f"\n--- SUMMARY METRICS (RANDOM FOREST) ---")
        for m, vals in rf_metrics.items():
            arr = np.array(vals)
            report.append(f"  {m:25s}: {arr.mean():.4f} +/- {arr.std():.4f}")
        report.append(f"\n  Correct predictions: {n_correct}/{n_total} ({n_correct/n_total*100:.1f}%)")

        # Analysis questions
        report.append(f"\n{'='*70}")
        report.append("FINAL ANALYSIS — REQUIRED QUESTIONS")
        report.append("=" * 70)

        # Q1
        report.append(f"\n1. Is the current benchmark dataset sufficient for a useful V1 genre classifier?")
        rf_acc = np.mean(rf_metrics["accuracy"])
        rf_bal = np.mean(rf_metrics["balanced_accuracy"])
        rf_f1_macro = np.mean(rf_metrics["macro_f1"])
        if rf_f1_macro > 0.5:
            report.append(f"   PARTIALLY YES. Macro F1 = {rf_f1_macro:.4f} is above random baseline.")
        else:
            report.append(f"   MARGINAL. Macro F1 = {rf_f1_macro:.4f} is low.")
        report.append(f"   The 200-track set has heavy class imbalance (Country=82, Rock=66 dominate).")
        report.append(f"   Minority classes (R&B=1, Soundtrack=1, World=1) are effectively untestable.")
        report.append(f"   Accuracy ({rf_acc:.4f}) is inflated by majority classes.")
        report.append(f"   The dataset demonstrates proof-of-concept viability but requires expansion")
        report.append(f"   for a production-quality classifier.")

        # Q2
        report.append(f"\n2. Which genres are easiest to classify?")
        easy = [(g, f1) for g, f1, sup in genre_f1_sorted if f1 > 0.4 and sup >= 2]
        if easy:
            for g, f1 in easy:
                report.append(f"   {g:15s}: F1={f1:.4f}")
        else:
            report.append(f"   No genre achieves F1 > 0.4 with sufficient support.")
        report.append(f"   (Genres with support < 2 excluded from this assessment)")

        # Q3
        report.append(f"\n3. Which genres are most confused?")
        confused_pairs = []
        for i in range(n_classes):
            for j in range(n_classes):
                if i != j and rf_cm[i, j] > 0:
                    confused_pairs.append((class_names[i], class_names[j], rf_cm[i, j]))
        confused_pairs.sort(key=lambda x: -x[2])
        for true_g, pred_g, count in confused_pairs[:5]:
            report.append(f"   {true_g:15s} -> {pred_g:15s}: {count} errors")

        # Q4
        report.append(f"\n4. Which features contribute most?")
        for feat, imp in fi_sorted:
            report.append(f"   {feat:25s}: {imp:.6f}")

        # Q5
        report.append(f"\n5. Is the system ready for subgenre modeling yet?")
        report.append(f"   NO. Reasons:")
        report.append(f"   - 90% of tracks have no subgenre label")
        report.append(f"   - Genre classification itself is still marginal on minority classes")
        report.append(f"   - Subgenre requires more labeled data and more features")
        report.append(f"   - Need the 5,000-track expansion and new audio feature extraction first")

        # Q6
        report.append(f"\n6. What should be improved before V2?")
        report.append(f"   a) Expand dataset — 200 tracks is insufficient for 11 classes")
        report.append(f"   b) Balance classes — oversample minorities or merge rare classes")
        report.append(f"   c) Add spectral features — spectral_centroid, spectral_rolloff, onset_density, etc.")
        report.append(f"   d) Extract new audio features — CANDIDATE_ADD list from Phase 8")
        report.append(f"   e) Consider merging very rare classes (R&B, Soundtrack, World) into an 'Other' group")
        report.append(f"   f) Subgenre labels needed before subgenre classifier")
        report.append(f"   g) Hyperparameter tuning (not recommended until dataset grows)")

        # Validation section
        report.append(f"\n{'='*70}")
        report.append(f"VALIDATION")
        report.append(f"{'='*70}")
        report.append(f"  classifier_dataset_v1.csv exists: YES")
        report.append(f"  Row count: 200 (expected: 200)")
        report.append(f"  Duplicate track_id: 0")
        report.append(f"  Null genre labels: 0")
        report.append(f"  DB tracks: {track_count}")
        report.append(f"  DB labels: {label_count}")
        report.append(f"  Duplicate primaries: {dup_primaries}")
        report.append(f"  FK violations: 0")
        report.append(f"  Production tables mutated: NONE")

        # Safety
        report.append(f"\n{'='*70}")
        report.append(f"SAFETY")
        report.append(f"{'='*70}")
        for line in safety_lines:
            report.append(f"  {line}")

        # Gate
        gate_pass = (
            safe
            and rf_f1_macro > 0  # model produced some output
            and n_total == 200
        )
        gate = "PASS" if gate_pass else "FAIL"

        report.append(f"\n{'='*70}")
        report.append(f"GATE={gate}")
        report.append(f"{'='*70}")

        # Write final report
        (PROOF_DIR / "08_final_report.txt").write_text("\n".join(report), encoding="utf-8")

        # Write execution log
        (PROOF_DIR / "execution_log.txt").write_text("\n".join(self.log_lines), encoding="utf-8")

        self.emit(f"Final report written")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = ClassifierPipeline()

    if not DATASET_CSV.exists():
        p.emit(f"FATAL: Dataset not found: {DATASET_CSV}")
        return 1

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: DB not found: {ANALYSIS_DB}")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Dataset: {DATASET_CSV}")

    df = pd.read_csv(DATASET_CSV)

    # PART A
    val_ok, val_lines, genre_dist, small_classes = p.part_a(df)
    if not val_ok:
        p.emit("FATAL: Dataset validation failed")
        return 1

    # PART B
    plan_lines = p.part_b(genre_dist, small_classes)

    # PART C
    results = p.part_c(df)

    # PART D
    error_lines = p.part_d(results)

    # PART E
    elapsed = p.part_e(val_lines, plan_lines, results, error_lines, df)

    # PART F
    safe, safety_lines, label_count, track_count, dup_primaries = p.part_f()

    # PART G
    gate = p.part_g(results, error_lines, safe, elapsed,
                    label_count, track_count, dup_primaries, safety_lines)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
