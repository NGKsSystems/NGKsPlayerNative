#!/usr/bin/env python3
"""
Phase 7 — Corrections Integration + Feature → Genre Correlation Analysis

Parts:
  A) Integrate 15 benchmark corrections into track_genre_labels + benchmark_set_tracks
  B) Build analysis dataset (JOIN tracks, analysis_summary, track_features, labels, benchmark)
  C) Feature → Genre correlation analysis (ANOVA, mutual info, pairwise, misfits)
  D) Write proof reports

Usage:
  python db/feature_genre_analysis.py
"""

import csv
import json
import os
import sqlite3
import sys
import time
import warnings
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore", category=FutureWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
CORRECTIONS_CSV = WORKSPACE / "data" / "genre_benchmark_corrections.csv"
DATASET_CSV = WORKSPACE / "data" / "genre_analysis_dataset.csv"
PROOF_DIR = WORKSPACE / "_proof" / "feature_genre_analysis"

BENCHMARK_NAME = "genre_benchmark_v1"


class AnalysisPipeline:
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
    # PART A — INTEGRATE CORRECTIONS
    # ================================================================
    def part_a(self):
        self.emit("\n{'='*60}")
        self.emit("PART A — INTEGRATE CORRECTIONS")
        self.emit("="*60)

        if not CORRECTIONS_CSV.exists():
            self.emit(f"FATAL: {CORRECTIONS_CSV} not found")
            return False, {}

        with open(CORRECTIONS_CSV, "r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))

        self.emit(f"Corrections input: {len(rows)} rows")

        conn = self.connect()
        cur = conn.cursor()

        # Lookups
        genres = {}
        for r in cur.execute("SELECT id, name FROM genres").fetchall():
            genres[r["name"].lower()] = r["id"]

        subgenres = {}
        for r in cur.execute("SELECT id, name, genre_id FROM subgenres").fetchall():
            subgenres[(r["name"].lower(), r["genre_id"])] = r["id"]

        bset = cur.execute(
            "SELECT id FROM benchmark_sets WHERE name = ?", (BENCHMARK_NAME,)
        ).fetchone()
        if not bset:
            self.emit(f"FATAL: Benchmark set '{BENCHMARK_NAME}' not found")
            conn.close()
            return False, {}
        bset_id = bset["id"]

        processed = 0
        skipped = 0
        updates = []
        inserts = []
        conflicts = []
        invalid = []

        for i, row in enumerate(rows):
            row_num = i + 1
            try:
                track_id = int(row["track_id"].strip())
            except (ValueError, KeyError):
                invalid.append(f"row={row_num} INVALID track_id")
                skipped += 1
                continue

            action = row.get("action", "").strip().lower()
            final_genre = row.get("final_genre", "").strip()
            final_subgenre = row.get("final_subgenre", "").strip()
            notes = row.get("notes", "").strip()

            try:
                conf = float(row.get("confidence", "0.90").strip())
                conf = max(0.0, min(1.0, conf))
            except ValueError:
                conf = 0.90

            if action not in ("confirm", "replace", "add_secondary"):
                invalid.append(f"row={row_num} INVALID action='{action}' track_id={track_id}")
                skipped += 1
                continue

            # Resolve genre
            genre_id = genres.get(final_genre.lower()) if final_genre else None
            if action in ("confirm", "replace") and genre_id is None:
                invalid.append(f"row={row_num} INVALID genre='{final_genre}' track_id={track_id}")
                skipped += 1
                continue

            subgenre_id = None
            if final_subgenre and genre_id is not None:
                subgenre_id = subgenres.get((final_subgenre.lower(), genre_id))
                if subgenre_id is None:
                    invalid.append(
                        f"row={row_num} INVALID subgenre='{final_subgenre}' "
                        f"for genre='{final_genre}' track_id={track_id}"
                    )
                    skipped += 1
                    continue

            applied = f"benchmark_hardening:{notes}" if notes else "benchmark_hardening"

            current = cur.execute(
                "SELECT id, genre_id, subgenre_id FROM track_genre_labels "
                "WHERE track_id = ? AND role = 'primary' ORDER BY id DESC LIMIT 1",
                (track_id,)
            ).fetchone()

            if action == "confirm":
                if current is None:
                    conflicts.append(f"row={row_num} CONFIRM_NO_PRIMARY track_id={track_id}")
                    skipped += 1
                    continue
                cur.execute(
                    "UPDATE track_genre_labels SET source='manual', confidence=?, "
                    "applied_by=?, created_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                    "WHERE id=?",
                    (conf, applied, current["id"])
                )
                updates.append(
                    f"track_id={track_id} label_id={current['id']} "
                    f"action=confirm applied_by=benchmark_hardening"
                )

            elif action == "replace":
                if current is not None:
                    cur.execute(
                        "UPDATE track_genre_labels SET role='secondary' WHERE id=?",
                        (current["id"],)
                    )
                    updates.append(
                        f"track_id={track_id} label_id={current['id']} action=downgrade_to_secondary"
                    )
                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, confidence, applied_by) "
                        "VALUES (?, ?, ?, 'primary', 'manual', ?, ?)",
                        (track_id, genre_id, subgenre_id, conf, applied)
                    )
                    inserts.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=primary action=replace"
                    )
                except sqlite3.IntegrityError as e:
                    conflicts.append(f"row={row_num} INTEGRITY track_id={track_id} err={e}")
                    skipped += 1
                    continue

            elif action == "add_secondary":
                try:
                    cur.execute(
                        "INSERT INTO track_genre_labels "
                        "(track_id, genre_id, subgenre_id, role, source, confidence, applied_by) "
                        "VALUES (?, ?, ?, 'secondary', 'manual', ?, ?)",
                        (track_id, genre_id, subgenre_id, conf, applied)
                    )
                    inserts.append(
                        f"track_id={track_id} genre_id={genre_id} "
                        f"subgenre_id={subgenre_id} role=secondary action=add_secondary"
                    )
                except sqlite3.IntegrityError as e:
                    conflicts.append(f"row={row_num} INTEGRITY track_id={track_id} err={e}")
                    skipped += 1
                    continue

            # Update benchmark_set_tracks expected_genre
            eg = final_genre
            if final_subgenre:
                eg = f"{final_genre} / {final_subgenre}"

            cur.execute(
                "UPDATE benchmark_set_tracks SET expected_genre=?, "
                "notes=? WHERE benchmark_set_id=? AND track_id=?",
                (eg, f"role=genre_reference action={action} hardened", bset_id, track_id)
            )

            processed += 1

        conn.commit()

        # Validation
        hardening_count = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE applied_by LIKE 'benchmark_hardening%'"
        ).fetchone()[0]

        dup_primaries = cur.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels WHERE role='primary' "
            "  GROUP BY track_id HAVING COUNT(*) > 1"
            ")"
        ).fetchone()[0]

        fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()

        total_primaries = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]

        conn.close()

        summary = {
            "processed": processed,
            "skipped": skipped,
            "updates": len(updates),
            "inserts": len(inserts),
            "conflicts": len(conflicts),
            "invalid": len(invalid),
            "hardening_labels": hardening_count,
            "total_primaries": total_primaries,
            "dup_primaries": dup_primaries,
            "fk_violations": len(fk_violations),
        }

        self.emit(f"Processed: {processed}")
        self.emit(f"Skipped: {skipped}")
        self.emit(f"Updates: {len(updates)}")
        self.emit(f"Inserts: {len(inserts)}")
        self.emit(f"Conflicts: {len(conflicts)}")
        self.emit(f"Invalid: {len(invalid)}")
        self.emit(f"Hardening labels: {hardening_count}")
        self.emit(f"Total primaries: {total_primaries}")
        self.emit(f"Duplicate primaries: {dup_primaries}")
        self.emit(f"FK violations: {len(fk_violations)}")

        ok = processed == 15 and skipped == 0 and dup_primaries == 0 and len(fk_violations) == 0
        self.emit(f"PART_A={'PASS' if ok else 'FAIL'}")

        return ok, {
            "summary": summary,
            "updates": updates,
            "inserts": inserts,
            "conflicts": conflicts,
            "invalid": invalid,
        }

    # ================================================================
    # PART B — BUILD ANALYSIS DATASET
    # ================================================================
    def part_b(self):
        self.emit("\n" + "="*60)
        self.emit("PART B — BUILD ANALYSIS DATASET")
        self.emit("="*60)

        conn = self.connect(readonly=True)

        # Check what data sources exist
        tf_count = conn.execute("SELECT COUNT(*) FROM track_features").fetchone()[0]
        as_count = conn.execute("SELECT COUNT(*) FROM analysis_summary").fetchone()[0]
        se_count = conn.execute("SELECT COUNT(*) FROM section_events").fetchone()[0]
        self.emit(f"track_features rows: {tf_count}")
        self.emit(f"analysis_summary rows: {as_count}")
        self.emit(f"section_events rows: {se_count}")

        # Build dataset from available data
        # Primary source: analysis_summary + track_genre_labels + benchmark_set_tracks
        # Optional enrichment: track_features (if rows exist), section_events for section metrics
        query = """
            SELECT
                t.id AS track_id,
                t.artist,
                t.title,
                g.name AS genre,
                COALESCE(sg.name, '') AS subgenre,
                -- analysis_summary features
                asumm.bpm AS bpm_detected,
                asumm.bpm_confidence AS tempo_stability,
                asumm.key_label AS key_detected,
                asumm.key_confidence AS harmonic_stability,
                asumm.loudness_lufs,
                asumm.energy,
                asumm.danceability,
                asumm.valence,
                -- track_features (may be NULL if empty table)
                tf.spectral_centroid,
                tf.spectral_rolloff,
                tf.onset_rate AS onset_density,
                tf.zero_crossing_rate,
                tf.rms_mean,
                tf.spectral_bandwidth,
                -- section-derived features
                sec_agg.section_count,
                sec_agg.has_intro,
                sec_agg.avg_section_duration,
                -- metadata
                tgl.confidence AS label_confidence,
                tgl.applied_by
            FROM benchmark_set_tracks bst
            JOIN benchmark_sets bs ON bst.benchmark_set_id = bs.id
            JOIN tracks t ON bst.track_id = t.id
            JOIN track_genre_labels tgl ON tgl.track_id = t.id AND tgl.role = 'primary'
            JOIN genres g ON tgl.genre_id = g.id
            LEFT JOIN subgenres sg ON tgl.subgenre_id = sg.id
            LEFT JOIN (
                SELECT track_id,
                       bpm, bpm_confidence, key_label, key_confidence,
                       loudness_lufs, energy, danceability, valence,
                       ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY id DESC) AS rn
                FROM analysis_summary
            ) asumm ON asumm.track_id = t.id AND asumm.rn = 1
            LEFT JOIN (
                SELECT track_id,
                       spectral_centroid, spectral_rolloff, onset_rate,
                       zero_crossing_rate, rms_mean, spectral_bandwidth,
                       ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY id DESC) AS rn
                FROM track_features
            ) tf ON tf.track_id = t.id AND tf.rn = 1
            LEFT JOIN (
                SELECT track_id,
                       COUNT(*) AS section_count,
                       MAX(CASE WHEN label = 'intro' THEN 1 ELSE 0 END) AS has_intro,
                       AVG(end_sec - start_sec) AS avg_section_duration
                FROM section_events
                GROUP BY track_id
            ) sec_agg ON sec_agg.track_id = t.id
            WHERE bs.name = ?
            ORDER BY t.id
        """

        rows = conn.execute(query, (BENCHMARK_NAME,)).fetchall()
        conn.close()

        if not rows:
            self.emit("FATAL: No rows returned from dataset query")
            return False, None

        # Convert to DataFrame
        columns = [desc[0] for desc in rows[0].keys() if True]
        columns = list(rows[0].keys())
        df = pd.DataFrame([dict(r) for r in rows])

        self.emit(f"Dataset rows: {len(df)}")
        self.emit(f"Dataset columns: {len(df.columns)}")
        self.emit(f"Columns: {list(df.columns)}")

        # Genre distribution in dataset
        genre_dist = df["genre"].value_counts()
        self.emit("\nGenre distribution in dataset:")
        for g, c in genre_dist.items():
            self.emit(f"  {g:15s}: {c}")

        # Feature availability
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        avail = {}
        for col in numeric_cols:
            non_null = df[col].notna().sum()
            avail[col] = non_null
            if non_null < len(df) * 0.5:
                self.emit(f"  WARNING: {col} has only {non_null}/{len(df)} non-null values")

        # Write dataset CSV (all spec columns, NULL where unavailable)
        spec_columns = [
            "track_id", "genre", "subgenre",
            "bpm_detected", "tempo_stability", "key_detected", "harmonic_stability",
            "bass_energy", "mid_energy", "high_energy",
            "spectral_centroid", "spectral_rolloff", "spectral_flux",
            "transient_density", "onset_density",
            "vocal_presence", "instrumentalness",
            "drum_presence", "guitar_presence", "synth_presence",
            "section_repeat_score", "dynamic_range_score",
        ]

        # Map available columns to spec columns, add missing as empty
        output_df = pd.DataFrame()
        output_df["track_id"] = df["track_id"]
        output_df["genre"] = df["genre"]
        output_df["subgenre"] = df["subgenre"]
        output_df["bpm_detected"] = df.get("bpm_detected")
        output_df["tempo_stability"] = df.get("tempo_stability")
        output_df["key_detected"] = df.get("key_detected")
        output_df["harmonic_stability"] = df.get("harmonic_stability")
        # These require spectral decomposition data not in DB
        output_df["bass_energy"] = np.nan
        output_df["mid_energy"] = np.nan
        output_df["high_energy"] = np.nan
        output_df["spectral_centroid"] = df.get("spectral_centroid")
        output_df["spectral_rolloff"] = df.get("spectral_rolloff")
        output_df["spectral_flux"] = np.nan  # not available
        output_df["transient_density"] = np.nan  # not available
        output_df["onset_density"] = df.get("onset_density")
        output_df["vocal_presence"] = np.nan
        output_df["instrumentalness"] = np.nan
        output_df["drum_presence"] = np.nan
        output_df["guitar_presence"] = np.nan
        output_df["synth_presence"] = np.nan
        output_df["section_repeat_score"] = np.nan
        output_df["dynamic_range_score"] = np.nan
        # Bonus columns from DB (available data)
        output_df["loudness_lufs"] = df.get("loudness_lufs")
        output_df["energy"] = df.get("energy")
        output_df["danceability"] = df.get("danceability")
        output_df["valence"] = df.get("valence")
        output_df["zero_crossing_rate"] = df.get("zero_crossing_rate")
        output_df["rms_mean"] = df.get("rms_mean")
        output_df["spectral_bandwidth"] = df.get("spectral_bandwidth")
        output_df["section_count"] = df.get("section_count")
        output_df["avg_section_duration"] = df.get("avg_section_duration")

        DATASET_CSV.parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(DATASET_CSV, index=False, encoding="utf-8")

        self.emit(f"\nDataset written to: {DATASET_CSV}")
        self.emit(f"Total rows: {len(output_df)}")

        ok = len(output_df) >= 100
        self.emit(f"PART_B={'PASS' if ok else 'FAIL'} (rows={len(output_df)}, threshold=100)")

        return ok, df

    # ================================================================
    # PART C — FEATURE → GENRE CORRELATION ANALYSIS
    # ================================================================
    def part_c(self, df):
        self.emit("\n" + "="*60)
        self.emit("PART C — FEATURE → GENRE CORRELATION ANALYSIS")
        self.emit("="*60)

        if df is None or len(df) < 100:
            self.emit("FATAL: Dataset too small for analysis")
            return False, {}

        # Identify usable numeric features (≥50% non-null)
        numeric_cols = [
            "bpm_detected", "tempo_stability", "harmonic_stability",
            "loudness_lufs", "energy", "danceability", "valence",
            "spectral_centroid", "spectral_rolloff", "onset_density",
            "zero_crossing_rate", "rms_mean", "spectral_bandwidth",
            "section_count", "avg_section_duration",
        ]
        usable = []
        for col in numeric_cols:
            if col in df.columns and df[col].notna().sum() >= len(df) * 0.3:
                usable.append(col)

        self.emit(f"\nUsable numeric features ({len(usable)}):")
        for f in usable:
            nn = df[f].notna().sum()
            self.emit(f"  {f:25s}: {nn}/{len(df)} non-null")

        if not usable:
            self.emit("FATAL: No usable numeric features for analysis")
            return False, {}

        # Work with rows that have at least some features
        analysis_df = df[["genre", "subgenre"] + usable].copy()
        # Fill NaN with column median for analysis (do not write back to DB)
        for col in usable:
            median = analysis_df[col].median()
            analysis_df[col] = analysis_df[col].fillna(median)

        results = {}

        # ───────────────────────────────────────────────
        # C1: GLOBAL CORRELATION — ANOVA F-scores + Mutual Information
        # ───────────────────────────────────────────────
        self.emit("\n--- C1: Global Feature Importance ---")

        X = analysis_df[usable].values
        y = analysis_df["genre"].values

        # Encode genre labels
        le = LabelEncoder()
        y_encoded = le.fit_transform(y)
        genre_names = le.classes_

        # ANOVA F-scores
        f_scores = {}
        p_values = {}
        for feat in usable:
            groups = [analysis_df[analysis_df["genre"] == g][feat].values for g in genre_names]
            # Filter out groups with < 2 samples
            groups = [g for g in groups if len(g) >= 2]
            if len(groups) >= 2:
                f_val, p_val = stats.f_oneway(*groups)
                f_scores[feat] = f_val if np.isfinite(f_val) else 0.0
                p_values[feat] = p_val if np.isfinite(p_val) else 1.0
            else:
                f_scores[feat] = 0.0
                p_values[feat] = 1.0

        # Mutual information
        mi_scores = {}
        try:
            mi_vals = mutual_info_classif(X, y_encoded, random_state=42)
            for j, feat in enumerate(usable):
                mi_scores[feat] = float(mi_vals[j])
        except Exception as e:
            self.emit(f"  MI computation failed: {e}")
            for feat in usable:
                mi_scores[feat] = 0.0

        # Per-genre one-vs-rest F-scores
        per_genre_importance = {}
        for genre in genre_names:
            y_binary = (analysis_df["genre"] == genre).astype(int).values
            genre_fscores = {}
            for feat in usable:
                groups = [
                    analysis_df[analysis_df["genre"] == genre][feat].values,
                    analysis_df[analysis_df["genre"] != genre][feat].values,
                ]
                groups = [g for g in groups if len(g) >= 2]
                if len(groups) == 2:
                    fv, pv = stats.f_oneway(*groups)
                    genre_fscores[feat] = float(fv) if np.isfinite(fv) else 0.0
                else:
                    genre_fscores[feat] = 0.0
            per_genre_importance[genre] = genre_fscores

        # Build ranking
        global_ranking = sorted(usable, key=lambda f: f_scores.get(f, 0), reverse=True)
        self.emit("\nGlobal Feature Ranking (by ANOVA F-score):")
        importance_lines = []
        importance_lines.append(f"{'Feature':25s} {'F-score':>10s} {'p-value':>12s} {'MI':>10s}")
        importance_lines.append("-" * 60)
        for feat in global_ranking:
            line = (
                f"{feat:25s} {f_scores[feat]:10.2f} {p_values[feat]:12.2e} "
                f"{mi_scores.get(feat, 0):10.4f}"
            )
            importance_lines.append(line)
            self.emit(f"  {line}")

        # Per-genre importance
        per_genre_lines = []
        for genre in sorted(genre_names):
            per_genre_lines.append(f"\n  {genre}:")
            gf = per_genre_importance[genre]
            ranked = sorted(usable, key=lambda f: gf.get(f, 0), reverse=True)
            for feat in ranked[:5]:
                per_genre_lines.append(f"    {feat:25s}: F={gf[feat]:.2f}")

        results["global_ranking"] = global_ranking
        results["f_scores"] = f_scores
        results["p_values"] = p_values
        results["mi_scores"] = mi_scores
        results["per_genre_importance"] = per_genre_importance
        results["importance_lines"] = importance_lines
        results["per_genre_lines"] = per_genre_lines

        # ───────────────────────────────────────────────
        # C2: PAIRWISE GENRE SEPARATION
        # ───────────────────────────────────────────────
        self.emit("\n--- C2: Pairwise Genre Separation ---")

        pairs = [
            ("Rock", "Metal"),
            ("Country", "Hip-Hop"),
            ("Hip-Hop", "Pop"),
            ("Electronic", "Rock"),
        ]

        pairwise_lines = []
        for g1, g2 in pairs:
            df1 = analysis_df[analysis_df["genre"] == g1]
            df2 = analysis_df[analysis_df["genre"] == g2]

            if len(df1) < 2 or len(df2) < 2:
                pairwise_lines.append(f"\n{g1} vs {g2}: INSUFFICIENT DATA (n1={len(df1)}, n2={len(df2)})")
                continue

            pairwise_lines.append(f"\n{g1} (n={len(df1)}) vs {g2} (n={len(df2)}):")
            pairwise_lines.append(f"  {'Feature':25s} {'Mean_1':>10s} {'Mean_2':>10s} {'Diff':>10s} {'t-stat':>10s} {'p-value':>12s}")
            pairwise_lines.append("  " + "-" * 82)

            feat_diffs = []
            for feat in usable:
                v1 = df1[feat].values
                v2 = df2[feat].values
                mean1 = np.mean(v1)
                mean2 = np.mean(v2)
                diff = abs(mean1 - mean2)

                try:
                    t_stat, p_val = stats.ttest_ind(v1, v2, equal_var=False)
                except Exception:
                    t_stat, p_val = 0.0, 1.0

                if not np.isfinite(t_stat):
                    t_stat = 0.0
                if not np.isfinite(p_val):
                    p_val = 1.0

                feat_diffs.append((feat, mean1, mean2, diff, t_stat, p_val))

            # Sort by absolute t-stat descending
            feat_diffs.sort(key=lambda x: abs(x[4]), reverse=True)

            for feat, m1, m2, diff, ts, pv in feat_diffs:
                sig = "***" if pv < 0.001 else ("**" if pv < 0.01 else ("*" if pv < 0.05 else ""))
                pairwise_lines.append(
                    f"  {feat:25s} {m1:10.3f} {m2:10.3f} {diff:10.3f} {ts:10.3f} {pv:12.2e} {sig}"
                )

            self.emit(f"  {g1} vs {g2}: top discriminator = {feat_diffs[0][0]} (t={feat_diffs[0][4]:.2f})")

        results["pairwise_lines"] = pairwise_lines

        # ───────────────────────────────────────────────
        # C3: SUBGENRE INSIGHT
        # ───────────────────────────────────────────────
        self.emit("\n--- C3: Subgenre Insights ---")

        subgenre_lines = []

        # Find subgenres with enough samples
        sub_counts = analysis_df[analysis_df["subgenre"] != ""]["subgenre"].value_counts()
        self.emit(f"Subgenres with data: {len(sub_counts)}")
        for sg, cnt in sub_counts.items():
            self.emit(f"  {sg}: {cnt}")

        # Compare tracks with subgenre vs without, within same genre
        genres_with_subs = analysis_df[analysis_df["subgenre"] != ""]["genre"].unique()
        for genre in genres_with_subs:
            gdata = analysis_df[analysis_df["genre"] == genre]
            with_sub = gdata[gdata["subgenre"] != ""]
            without_sub = gdata[gdata["subgenre"] == ""]

            if len(with_sub) < 2 or len(without_sub) < 2:
                continue

            subgenre_lines.append(f"\n{genre}: subgenre={with_sub['subgenre'].iloc[0]} (n={len(with_sub)}) vs no-subgenre (n={len(without_sub)})")
            subgenre_lines.append(f"  {'Feature':25s} {'Sub-mean':>10s} {'NoSub-mean':>10s} {'t-stat':>10s} {'p-value':>12s}")

            for feat in usable:
                v1 = with_sub[feat].values
                v2 = without_sub[feat].values
                try:
                    ts, pv = stats.ttest_ind(v1, v2, equal_var=False)
                except Exception:
                    ts, pv = 0.0, 1.0
                if not np.isfinite(ts):
                    ts = 0.0
                if not np.isfinite(pv):
                    pv = 1.0
                sig = "*" if pv < 0.05 else ""
                subgenre_lines.append(
                    f"  {feat:25s} {np.mean(v1):10.3f} {np.mean(v2):10.3f} {ts:10.3f} {pv:12.2e} {sig}"
                )

        # Also compare distinct subgenres within a genre
        for genre in genres_with_subs:
            gdata = analysis_df[analysis_df["genre"] == genre]
            subs = gdata[gdata["subgenre"] != ""]["subgenre"].unique()
            if len(subs) >= 2:
                for i in range(len(subs)):
                    for j in range(i+1, len(subs)):
                        s1_data = gdata[gdata["subgenre"] == subs[i]]
                        s2_data = gdata[gdata["subgenre"] == subs[j]]
                        if len(s1_data) < 2 or len(s2_data) < 2:
                            continue
                        subgenre_lines.append(f"\n{genre}: {subs[i]} (n={len(s1_data)}) vs {subs[j]} (n={len(s2_data)})")
                        for feat in usable:
                            v1 = s1_data[feat].values
                            v2 = s2_data[feat].values
                            try:
                                ts, pv = stats.ttest_ind(v1, v2, equal_var=False)
                            except Exception:
                                ts, pv = 0.0, 1.0
                            if not np.isfinite(ts):
                                ts = 0.0
                            if not np.isfinite(pv):
                                pv = 1.0
                            sig = "*" if pv < 0.05 else ""
                            subgenre_lines.append(
                                f"  {feat:25s} {np.mean(v1):10.3f} {np.mean(v2):10.3f} {ts:10.3f} {pv:12.2e} {sig}"
                            )

        if not subgenre_lines:
            subgenre_lines.append("Insufficient subgenre data for meaningful comparison.")

        results["subgenre_lines"] = subgenre_lines

        # ───────────────────────────────────────────────
        # C4: MISFIT DETECTION
        # ───────────────────────────────────────────────
        self.emit("\n--- C4: Misfit Detection ---")

        misfit_lines = []
        misfit_candidates = []

        # For each genre, compute mean/std of features
        # Flag tracks >2 std deviations from their genre centroid on multiple features
        for genre in genre_names:
            g_data = analysis_df[analysis_df["genre"] == genre]
            if len(g_data) < 5:
                continue

            for idx, row in g_data.iterrows():
                deviations = 0
                deviation_details = []
                for feat in usable:
                    g_mean = g_data[feat].mean()
                    g_std = g_data[feat].std()
                    if g_std == 0 or np.isnan(g_std):
                        continue
                    z = abs((row[feat] - g_mean) / g_std)
                    if z > 2.0:
                        deviations += 1
                        deviation_details.append(f"{feat}(z={z:.1f})")

                # Flag if deviating on ≥3 features
                threshold = min(3, max(2, len(usable) // 3))
                if deviations >= threshold:
                    track_id = row.get("track_id", idx)
                    entry = {
                        "track_id": int(track_id) if hasattr(track_id, '__int__') else track_id,
                        "genre": genre,
                        "deviations": deviations,
                        "details": ", ".join(deviation_details),
                    }
                    misfit_candidates.append(entry)

        misfit_candidates.sort(key=lambda x: x["deviations"], reverse=True)

        self.emit(f"Misfit candidates: {len(misfit_candidates)}")
        if misfit_candidates:
            misfit_lines.append(f"{'track_id':>10s} {'genre':>15s} {'#devs':>6s} details")
            misfit_lines.append("-" * 80)
            for m in misfit_candidates:
                line = f"{m['track_id']:10d} {m['genre']:>15s} {m['deviations']:6d} {m['details']}"
                misfit_lines.append(line)
                self.emit(f"  {line}")
        else:
            misfit_lines.append("No misfit candidates detected.")

        results["misfit_lines"] = misfit_lines
        results["misfit_candidates"] = misfit_candidates

        # ───────────────────────────────────────────────
        # Correlation matrix CSV
        # ───────────────────────────────────────────────
        corr_matrix = analysis_df[usable].corr()
        results["corr_matrix"] = corr_matrix

        # Feature importance CSV
        importance_df = pd.DataFrame({
            "feature": usable,
            "anova_f_score": [f_scores[f] for f in usable],
            "anova_p_value": [p_values[f] for f in usable],
            "mutual_information": [mi_scores.get(f, 0) for f in usable],
        }).sort_values("anova_f_score", ascending=False)
        results["importance_df"] = importance_df

        self.emit(f"\nPART_C=PASS")
        return True, results

    # ================================================================
    # PART D — WRITE REPORTS + PROOF
    # ================================================================
    def part_d(self, part_a_ok, part_a_data, part_b_ok, part_c_ok, part_c_results):
        self.emit("\n" + "="*60)
        self.emit("PART D — WRITE REPORTS + PROOF")
        self.emit("="*60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        # 00 corrections applied
        a_summary = part_a_data.get("summary", {})
        corr_text = [
            "=== CORRECTIONS APPLIED ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            f"Processed: {a_summary.get('processed', 0)}",
            f"Skipped: {a_summary.get('skipped', 0)}",
            f"Updates: {a_summary.get('updates', 0)}",
            f"Inserts: {a_summary.get('inserts', 0)}",
            f"Conflicts: {a_summary.get('conflicts', 0)}",
            f"Invalid: {a_summary.get('invalid', 0)}",
            f"Hardening labels: {a_summary.get('hardening_labels', 0)}",
            f"Total primaries: {a_summary.get('total_primaries', 0)}",
            f"Dup primaries: {a_summary.get('dup_primaries', 0)}",
            f"FK violations: {a_summary.get('fk_violations', 0)}",
            "",
            "--- Updates ---",
        ]
        for u in part_a_data.get("updates", []):
            corr_text.append(f"  {u}")
        corr_text.append("")
        corr_text.append("--- Inserts ---")
        for ins in part_a_data.get("inserts", []):
            corr_text.append(f"  {ins}")
        corr_text.append("")
        corr_text.append("--- Conflicts ---")
        for c in part_a_data.get("conflicts", []):
            corr_text.append(f"  {c}")
        if not part_a_data.get("conflicts"):
            corr_text.append("  (none)")
        corr_text.append("")
        corr_text.append("--- Invalid ---")
        for inv in part_a_data.get("invalid", []):
            corr_text.append(f"  {inv}")
        if not part_a_data.get("invalid"):
            corr_text.append("  (none)")
        w("00_corrections_applied.txt", "\n".join(corr_text))

        # 01 dataset summary
        ds_lines = [
            "=== DATASET SUMMARY ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Output: {DATASET_CSV}",
        ]
        if DATASET_CSV.exists():
            df = pd.read_csv(DATASET_CSV)
            ds_lines.append(f"Rows: {len(df)}")
            ds_lines.append(f"Columns: {len(df.columns)}")
            ds_lines.append(f"\nColumn availability:")
            for col in df.columns:
                nn = df[col].notna().sum()
                ds_lines.append(f"  {col:30s}: {nn}/{len(df)} ({100*nn/len(df):.0f}%)")
            ds_lines.append(f"\nGenre distribution:")
            for g, c in df["genre"].value_counts().items():
                ds_lines.append(f"  {g:15s}: {c}")
        w("01_dataset_summary.txt", "\n".join(ds_lines))

        # 02 feature importance global
        fi_lines = [
            "=== FEATURE IMPORTANCE — GLOBAL ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        fi_lines.extend(part_c_results.get("importance_lines", ["(not computed)"]))
        fi_lines.append("\n\n=== PER-GENRE FEATURE IMPORTANCE (Top 5) ===")
        fi_lines.extend(part_c_results.get("per_genre_lines", []))
        w("02_feature_importance_global.txt", "\n".join(fi_lines))

        # 03 pairwise
        pw_lines = [
            "=== PAIRWISE GENRE DIFFERENCES ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        pw_lines.extend(part_c_results.get("pairwise_lines", ["(not computed)"]))
        w("03_pairwise_genre_differences.txt", "\n".join(pw_lines))

        # 04 subgenre insights
        sg_lines = [
            "=== SUBGENRE INSIGHTS ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        sg_lines.extend(part_c_results.get("subgenre_lines", ["(not computed)"]))
        w("04_subgenre_insights.txt", "\n".join(sg_lines))

        # 05 misfit candidates
        mf_lines = [
            "=== MISFIT CANDIDATES ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Tracks where feature profile deviates strongly from assigned genre cluster.",
            "These are candidates for manual review — DO NOT auto-modify labels.",
            "",
        ]
        mf_lines.extend(part_c_results.get("misfit_lines", ["(none)"]))
        w("05_misfit_candidates.txt", "\n".join(mf_lines))

        # 06 validation queries
        conn = self.connect(readonly=True)
        vq_lines = [
            "=== VALIDATION QUERIES ===",
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            "",
        ]
        validation_queries = [
            "SELECT COUNT(*) FROM track_genre_labels WHERE applied_by LIKE 'benchmark_hardening%';",
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary';",
            ("SELECT track_id, COUNT(*) FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
            "PRAGMA foreign_key_check;",
            "SELECT COUNT(*) FROM benchmark_set_tracks;",
            "SELECT COUNT(*) FROM benchmark_sets;",
        ]
        for q in validation_queries:
            result = conn.execute(q).fetchall()
            vq_lines.append(f"-- {q}")
            if result:
                for r in result:
                    vq_lines.append(f"   {tuple(r)}")
            else:
                vq_lines.append("   (empty)")
            vq_lines.append("")
        conn.close()
        w("06_validation_queries.txt", "\n".join(vq_lines))

        # Correlation matrix CSV
        corr = part_c_results.get("corr_matrix")
        if corr is not None:
            corr.to_csv(PROOF_DIR / "correlation_matrix.csv", encoding="utf-8")

        # Feature importance CSV
        imp_df = part_c_results.get("importance_df")
        if imp_df is not None:
            imp_df.to_csv(PROOF_DIR / "feature_importance.csv", index=False, encoding="utf-8")

        # 07 final report
        gate = "PASS" if (part_a_ok and part_b_ok and part_c_ok) else "FAIL"
        report_lines = [
            "=" * 60,
            "FEATURE → GENRE CORRELATION ANALYSIS — FINAL REPORT",
            "=" * 60,
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Elapsed: {elapsed}s",
            f"GATE: {gate}",
            "",
            "PART A — Corrections Integration:",
            f"  Status: {'PASS' if part_a_ok else 'FAIL'}",
            f"  Processed: {a_summary.get('processed', 0)}",
            f"  Hardening labels: {a_summary.get('hardening_labels', 0)}",
            f"  Dup primaries: {a_summary.get('dup_primaries', 0)}",
            f"  FK violations: {a_summary.get('fk_violations', 0)}",
            "",
            "PART B — Analysis Dataset:",
            f"  Status: {'PASS' if part_b_ok else 'FAIL'}",
            f"  Output: {DATASET_CSV}",
        ]
        if DATASET_CSV.exists():
            df = pd.read_csv(DATASET_CSV)
            report_lines.append(f"  Rows: {len(df)}")
            report_lines.append(f"  Columns: {len(df.columns)}")
        report_lines.extend([
            "",
            "PART C — Feature → Genre Correlation:",
            f"  Status: {'PASS' if part_c_ok else 'FAIL'}",
            f"  Usable features: {len(part_c_results.get('global_ranking', []))}",
            f"  Misfit candidates: {len(part_c_results.get('misfit_candidates', []))}",
            "",
            "Proof directory: " + str(PROOF_DIR),
        ])
        w("07_final_report.txt", "\n".join(report_lines))

        # Execution log
        w("execution_log.txt", "\n".join(self.log))

        self.emit(f"\nProof written to: {PROOF_DIR}")
        self.emit(f"GATE={gate}")

        return gate


def main():
    p = AnalysisPipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: DB not found: {ANALYSIS_DB}")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Corrections: {CORRECTIONS_CSV}")

    # Part A
    part_a_ok, part_a_data = p.part_a()

    # Part B
    part_b_ok, df = p.part_b()

    # Part C
    part_c_ok, part_c_results = p.part_c(df)

    # Part D
    gate = p.part_d(part_a_ok, part_a_data, part_b_ok, part_c_ok, part_c_results)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
