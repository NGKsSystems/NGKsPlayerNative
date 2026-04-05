#!/usr/bin/env python3
"""
Phase 8 — Misfit Review + Feature Refinement Loop

Parts:
  A) Build misfit review queue from feature-genre deviation analysis
  B) Create review input template (pre-filled with confirm_label for all)
  C) Ingest review decisions into track_genre_labels
  D) Feature diagnostics — per-track deviation from genre centroid
  E) Feature refinement proposals (report only, no DB changes)

Usage:
  python db/misfit_feature_refinement.py queue      # Parts A+B
  python db/misfit_feature_refinement.py ingest     # Part C
  python db/misfit_feature_refinement.py all        # A+B+C+D+E
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
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder

warnings.filterwarnings("ignore", category=FutureWarning)

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
DATASET_CSV = WORKSPACE / "data" / "genre_analysis_dataset.csv"
MISFIT_QUEUE_CSV = WORKSPACE / "data" / "misfit_review_queue.csv"
MISFIT_INPUT_CSV = WORKSPACE / "data" / "misfit_review_input.csv"
PROOF_DIR = WORKSPACE / "_proof" / "misfit_feature_refinement"
BENCHMARK_NAME = "genre_benchmark_v1"

# Features available in DB
USABLE_FEATURES = [
    "bpm_detected", "tempo_stability", "harmonic_stability",
    "loudness_lufs", "energy", "danceability", "valence",
    "section_count", "avg_section_duration",
]

# Minimum z-score threshold for deviation flagging
Z_THRESHOLD = 2.0
# Minimum deviating features to flag a track
MIN_DEVIATIONS = 2


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

    def load_benchmark_dataset(self):
        """Load the full benchmark dataset with all available features."""
        conn = self.connect(readonly=True)
        query = """
            SELECT
                t.id AS track_id,
                t.artist,
                t.title,
                g.name AS genre,
                COALESCE(sg.name, '') AS subgenre,
                asumm.bpm AS bpm_detected,
                asumm.bpm_confidence AS tempo_stability,
                asumm.key_label AS key_detected,
                asumm.key_confidence AS harmonic_stability,
                asumm.loudness_lufs,
                asumm.energy,
                asumm.danceability,
                asumm.valence,
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
                       bpm, bpm_confidence, key_label, key_confidence,
                       loudness_lufs, energy, danceability, valence,
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
        return df

    def compute_genre_centroids(self, df):
        """Compute mean and std per feature per genre."""
        centroids = {}
        for genre in df["genre"].unique():
            g_data = df[df["genre"] == genre]
            if len(g_data) < 2:
                continue
            stats_dict = {}
            for feat in USABLE_FEATURES:
                if feat in df.columns:
                    vals = g_data[feat].dropna()
                    if len(vals) >= 2:
                        stats_dict[feat] = {
                            "mean": vals.mean(),
                            "std": vals.std(),
                            "count": len(vals),
                        }
            centroids[genre] = stats_dict
        return centroids

    def compute_track_deviations(self, df, centroids):
        """For each track, compute z-score deviations from its genre centroid."""
        results = []
        for _, row in df.iterrows():
            genre = row["genre"]
            if genre not in centroids:
                continue
            centroid = centroids[genre]
            deviations = []
            total_z = 0.0
            n_features = 0
            for feat in USABLE_FEATURES:
                if feat not in centroid or feat not in df.columns:
                    continue
                val = row[feat]
                if pd.isna(val):
                    continue
                mu = centroid[feat]["mean"]
                sigma = centroid[feat]["std"]
                if sigma == 0 or np.isnan(sigma):
                    continue
                z = abs((val - mu) / sigma)
                total_z += z
                n_features += 1
                if z > Z_THRESHOLD:
                    deviations.append({"feature": feat, "z": round(z, 2),
                                       "value": round(val, 4),
                                       "genre_mean": round(mu, 4),
                                       "genre_std": round(sigma, 4)})

            anomaly_score = round(total_z / max(n_features, 1), 3)
            results.append({
                "track_id": int(row["track_id"]),
                "artist": row.get("artist", ""),
                "title": row.get("title", ""),
                "genre": genre,
                "subgenre": row.get("subgenre", ""),
                "n_deviations": len(deviations),
                "anomaly_score": anomaly_score,
                "deviations": deviations,
            })
        return results

    def find_top_similar_genres(self, row_data, df, centroids):
        """For a track, compute distance to each genre centroid and return top 3."""
        track_id = row_data["track_id"]
        track_row = df[df["track_id"] == track_id]
        if track_row.empty:
            return ""
        track_row = track_row.iloc[0]

        distances = {}
        for genre, centroid in centroids.items():
            dist = 0.0
            n = 0
            for feat in USABLE_FEATURES:
                if feat not in centroid or feat not in df.columns:
                    continue
                val = track_row[feat]
                if pd.isna(val):
                    continue
                mu = centroid[feat]["mean"]
                sigma = centroid[feat]["std"]
                if sigma == 0 or np.isnan(sigma):
                    continue
                dist += ((val - mu) / sigma) ** 2
                n += 1
            if n > 0:
                distances[genre] = round(np.sqrt(dist / n), 3)

        sorted_genres = sorted(distances.items(), key=lambda x: x[1])
        top3 = [f"{g}({d})" for g, d in sorted_genres[:3]]
        return "; ".join(top3)

    # ================================================================
    # PART A — BUILD MISFIT REVIEW QUEUE
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — BUILD MISFIT REVIEW QUEUE")
        self.emit("=" * 60)

        df = self.load_benchmark_dataset()
        self.emit(f"Benchmark dataset: {len(df)} tracks")

        # Fill NaN with median for analysis
        for feat in USABLE_FEATURES:
            if feat in df.columns:
                median = df[feat].median()
                df[feat] = df[feat].fillna(median)

        centroids = self.compute_genre_centroids(df)
        self.emit(f"Genre centroids computed for: {list(centroids.keys())}")

        deviations = self.compute_track_deviations(df, centroids)

        # Filter to misfits (MIN_DEVIATIONS or more features deviating)
        misfits = [d for d in deviations if d["n_deviations"] >= MIN_DEVIATIONS]
        misfits.sort(key=lambda x: (-x["n_deviations"], -x["anomaly_score"]))

        self.emit(f"Misfit candidates: {len(misfits)} (threshold: >={MIN_DEVIATIONS} features >z={Z_THRESHOLD})")

        # Build queue CSV
        queue_rows = []
        for m in misfits:
            top3 = self.find_top_similar_genres(m, df, centroids)
            detail_strs = [f"{d['feature']}(z={d['z']})" for d in m["deviations"]]
            queue_rows.append({
                "track_id": m["track_id"],
                "artist": m["artist"] or "",
                "title": m["title"] or "",
                "current_genre": m["genre"],
                "current_subgenre": m["subgenre"],
                "top_3_similar_genres": top3,
                "anomaly_score": m["anomaly_score"],
                "notes": f"deviations: {', '.join(detail_strs)}",
            })

        MISFIT_QUEUE_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(MISFIT_QUEUE_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "current_genre", "current_subgenre",
                "top_3_similar_genres", "anomaly_score", "notes",
            ])
            writer.writeheader()
            for r in queue_rows:
                writer.writerow(r)

        self.emit(f"Queue written to: {MISFIT_QUEUE_CSV}")
        self.emit(f"Queue size: {len(queue_rows)} tracks")

        # Genre distribution of misfits
        genre_dist = {}
        for r in queue_rows:
            g = r["current_genre"]
            genre_dist[g] = genre_dist.get(g, 0) + 1
        for g, c in sorted(genre_dist.items(), key=lambda x: -x[1]):
            self.emit(f"  {g:15s}: {c}")

        return len(queue_rows) > 0, df, centroids, misfits, queue_rows

    # ================================================================
    # PART B — CREATE REVIEW INPUT TEMPLATE
    # ================================================================
    def part_b(self, queue_rows):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — CREATE REVIEW INPUT TEMPLATE")
        self.emit("=" * 60)

        if not queue_rows:
            self.emit("No misfits to review — using empty template")
            queue_rows = []

        review_rows = []
        for r in queue_rows:
            # Default: confirm_label (feature deviation doesn't necessarily mean wrong label)
            # The most common case for misfits is unusual feature values, not wrong genre
            review_rows.append({
                "track_id": r["track_id"],
                "artist": r["artist"],
                "title": r["title"],
                "action": "confirm_label",
                "new_genre": "",
                "new_subgenre": "",
                "confidence": "0.85",
                "notes": r["notes"],
            })

        with open(MISFIT_INPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "track_id", "artist", "title", "action",
                "new_genre", "new_subgenre", "confidence", "notes",
            ])
            writer.writeheader()
            for r in review_rows:
                writer.writerow(r)

        self.emit(f"Review input written to: {MISFIT_INPUT_CSV}")
        self.emit(f"Rows: {len(review_rows)}")
        self.emit(f"  confirm_label: {sum(1 for r in review_rows if r['action'] == 'confirm_label')}")

        return len(review_rows) > 0, review_rows

    # ================================================================
    # PART C — INGEST REVIEW DECISIONS
    # ================================================================
    def part_c(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — INGEST REVIEW DECISIONS")
        self.emit("=" * 60)

        if not MISFIT_INPUT_CSV.exists():
            self.emit(f"FATAL: {MISFIT_INPUT_CSV} not found")
            return False, {}

        with open(MISFIT_INPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))

        self.emit(f"Input rows: {len(rows)}")

        conn = self.connect()
        cur = conn.cursor()

        # Lookups
        genres = {}
        for r in cur.execute("SELECT id, name FROM genres").fetchall():
            genres[r["name"].lower()] = r["id"]

        subgenres = {}
        for r in cur.execute("SELECT id, name, genre_id FROM subgenres").fetchall():
            subgenres[(r["name"].lower(), r["genre_id"])] = r["id"]

        valid_actions = {"confirm_label", "replace_label", "mark_hybrid", "mark_feature_issue"}

        processed = 0
        skipped = 0
        updates = []
        inserts = []
        feature_issues = []
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
            new_genre = row.get("new_genre", "").strip()
            new_subgenre = row.get("new_subgenre", "").strip()
            notes = row.get("notes", "").strip()

            try:
                conf = float(row.get("confidence", "0.85").strip())
                conf = max(0.0, min(1.0, conf))
            except ValueError:
                conf = 0.85

            if action not in valid_actions:
                invalid.append(f"row={row_num} INVALID action='{action}' track_id={track_id}")
                skipped += 1
                continue

            current = cur.execute(
                "SELECT id, genre_id, subgenre_id FROM track_genre_labels "
                "WHERE track_id = ? AND role = 'primary' ORDER BY id DESC LIMIT 1",
                (track_id,)
            ).fetchone()

            applied = "misfit_review"

            # ── CONFIRM_LABEL ──
            if action == "confirm_label":
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
                    f"track_id={track_id} label_id={current['id']} action=confirm_label"
                )

            # ── REPLACE_LABEL ──
            elif action == "replace_label":
                if not new_genre:
                    invalid.append(f"row={row_num} MISSING new_genre track_id={track_id}")
                    skipped += 1
                    continue
                genre_id = genres.get(new_genre.lower())
                if genre_id is None:
                    invalid.append(f"row={row_num} INVALID genre='{new_genre}' track_id={track_id}")
                    skipped += 1
                    continue
                subgenre_id = None
                if new_subgenre:
                    subgenre_id = subgenres.get((new_subgenre.lower(), genre_id))
                    if subgenre_id is None:
                        invalid.append(
                            f"row={row_num} INVALID subgenre='{new_subgenre}' "
                            f"genre='{new_genre}' track_id={track_id}"
                        )
                        skipped += 1
                        continue

                if current is not None:
                    cur.execute(
                        "UPDATE track_genre_labels SET role='secondary' WHERE id=?",
                        (current["id"],)
                    )
                    updates.append(
                        f"track_id={track_id} label_id={current['id']} action=downgrade"
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
                        f"subgenre_id={subgenre_id} action=replace_label"
                    )
                except sqlite3.IntegrityError as e:
                    conflicts.append(f"row={row_num} INTEGRITY track_id={track_id} err={e}")
                    skipped += 1
                    continue

            # ── MARK_HYBRID ──
            elif action == "mark_hybrid":
                # Keep current primary, optionally add secondary
                if new_genre:
                    genre_id = genres.get(new_genre.lower())
                    if genre_id is None:
                        invalid.append(f"row={row_num} INVALID genre='{new_genre}' track_id={track_id}")
                        skipped += 1
                        continue
                    subgenre_id = None
                    if new_subgenre:
                        subgenre_id = subgenres.get((new_subgenre.lower(), genre_id))
                        if subgenre_id is None:
                            invalid.append(
                                f"row={row_num} INVALID subgenre='{new_subgenre}' track_id={track_id}"
                            )
                            skipped += 1
                            continue
                    try:
                        cur.execute(
                            "INSERT INTO track_genre_labels "
                            "(track_id, genre_id, subgenre_id, role, source, confidence, applied_by) "
                            "VALUES (?, ?, ?, 'secondary', 'manual', ?, ?)",
                            (track_id, genre_id, subgenre_id, conf, applied)
                        )
                        inserts.append(
                            f"track_id={track_id} genre_id={genre_id} "
                            f"subgenre_id={subgenre_id} action=mark_hybrid_secondary"
                        )
                    except sqlite3.IntegrityError as e:
                        conflicts.append(f"row={row_num} INTEGRITY track_id={track_id} err={e}")
                        skipped += 1
                        continue
                # Also confirm primary
                if current is not None:
                    cur.execute(
                        "UPDATE track_genre_labels SET source='manual', confidence=?, "
                        "applied_by=?, created_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                        "WHERE id=?",
                        (conf, applied, current["id"])
                    )
                    updates.append(
                        f"track_id={track_id} label_id={current['id']} action=mark_hybrid_confirm"
                    )

            # ── MARK_FEATURE_ISSUE ──
            elif action == "mark_feature_issue":
                # DO NOT change labels — log only
                feature_issues.append(
                    f"track_id={track_id} genre={current['genre_id'] if current else 'UNKNOWN'} "
                    f"notes={notes}"
                )

            processed += 1

        conn.commit()

        # Validation
        misfit_review_count = cur.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE applied_by='misfit_review'"
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
            "feature_issues": len(feature_issues),
            "conflicts": len(conflicts),
            "invalid": len(invalid),
            "misfit_review_labels": misfit_review_count,
            "total_primaries": total_primaries,
            "dup_primaries": dup_primaries,
            "fk_violations": len(fk_violations),
        }

        self.emit(f"Processed: {processed}")
        self.emit(f"Skipped: {skipped}")
        self.emit(f"Updates: {len(updates)}")
        self.emit(f"Inserts: {len(inserts)}")
        self.emit(f"Feature issues: {len(feature_issues)}")
        self.emit(f"Conflicts: {len(conflicts)}")
        self.emit(f"Invalid: {len(invalid)}")
        self.emit(f"Misfit review labels: {misfit_review_count}")
        self.emit(f"Dup primaries: {dup_primaries}")
        self.emit(f"FK violations: {len(fk_violations)}")

        ok = processed > 0 and dup_primaries == 0 and len(fk_violations) == 0
        self.emit(f"PART_C={'PASS' if ok else 'FAIL'}")

        return ok, {
            "summary": summary,
            "updates": updates,
            "inserts": inserts,
            "feature_issues": feature_issues,
            "conflicts": conflicts,
            "invalid": invalid,
        }

    # ================================================================
    # PART D — FEATURE DIAGNOSTICS
    # ================================================================
    def part_d(self, df, centroids, misfits):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — FEATURE DIAGNOSTICS")
        self.emit("=" * 60)

        per_track_lines = []
        feature_weakness = {}  # feat -> list of z-scores from misfits

        for m in misfits:
            per_track_lines.append(
                f"\ntrack_id={m['track_id']} genre={m['genre']} "
                f"anomaly_score={m['anomaly_score']} deviations={m['n_deviations']}"
            )
            for d in m["deviations"]:
                feat = d["feature"]
                per_track_lines.append(
                    f"  {feat:25s}: value={d['value']:10.4f}  "
                    f"genre_mean={d['genre_mean']:10.4f}  "
                    f"genre_std={d['genre_std']:10.4f}  z={d['z']}"
                )
                if feat not in feature_weakness:
                    feature_weakness[feat] = []
                feature_weakness[feat].append(d["z"])

        # Aggregated weakness summary
        weakness_lines = []
        weakness_lines.append(f"{'Feature':25s} {'#Misfits':>10s} {'Avg_z':>10s} {'Max_z':>10s} {'Verdict'}")
        weakness_lines.append("-" * 75)
        for feat in sorted(feature_weakness.keys(), key=lambda f: -len(feature_weakness[f])):
            zs = feature_weakness[feat]
            avg_z = np.mean(zs)
            max_z = np.max(zs)
            count = len(zs)
            # Verdict
            if count >= len(misfits) * 0.5:
                verdict = "HIGH_NOISE — majority of misfits deviate on this"
            elif avg_z > 3.0:
                verdict = "EXTREME_OUTLIER — large deviations"
            elif count >= 3:
                verdict = "MODERATE — recurrent deviator"
            else:
                verdict = "LOW — isolated cases"
            weakness_lines.append(f"{feat:25s} {count:10d} {avg_z:10.2f} {max_z:10.2f} {verdict}")

        self.emit(f"Per-track deviation reports: {len(misfits)}")
        self.emit(f"Features appearing in deviations: {len(feature_weakness)}")
        for line in weakness_lines:
            self.emit(f"  {line}")

        # Distance matrix: misfit tracks x genres
        distance_rows = []
        for m in misfits:
            track_row = df[df["track_id"] == m["track_id"]]
            if track_row.empty:
                continue
            track_row = track_row.iloc[0]
            row_dict = {"track_id": m["track_id"], "current_genre": m["genre"]}
            for genre, centroid in centroids.items():
                dist = 0.0
                n = 0
                for feat in USABLE_FEATURES:
                    if feat not in centroid or feat not in df.columns:
                        continue
                    val = track_row[feat]
                    if pd.isna(val):
                        continue
                    mu = centroid[feat]["mean"]
                    sigma = centroid[feat]["std"]
                    if sigma == 0 or np.isnan(sigma):
                        continue
                    dist += ((val - mu) / sigma) ** 2
                    n += 1
                row_dict[genre] = round(np.sqrt(dist / max(n, 1)), 3) if n > 0 else None
            distance_rows.append(row_dict)

        distance_df = pd.DataFrame(distance_rows)

        return per_track_lines, weakness_lines, feature_weakness, distance_df

    # ================================================================
    # PART E — FEATURE REFINEMENT PROPOSALS
    # ================================================================
    def part_e(self, df, centroids, feature_weakness):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — FEATURE REFINEMENT PROPOSALS")
        self.emit("=" * 60)

        # Re-compute ANOVA for all features
        proposals = []

        # Fill NaN with median
        for feat in USABLE_FEATURES:
            if feat in df.columns:
                median = df[feat].median()
                df[feat] = df[feat].fillna(median)

        genre_names = df["genre"].unique()

        f_scores = {}
        p_values = {}
        for feat in USABLE_FEATURES:
            if feat not in df.columns:
                continue
            groups = [df[df["genre"] == g][feat].dropna().values for g in genre_names]
            groups = [g for g in groups if len(g) >= 2 and np.std(g) > 0]
            if len(groups) >= 2:
                try:
                    f_val, p_val = stats.f_oneway(*groups)
                    f_scores[feat] = float(f_val) if np.isfinite(f_val) else 0.0
                    p_values[feat] = float(p_val) if np.isfinite(p_val) else 1.0
                except Exception:
                    f_scores[feat] = 0.0
                    p_values[feat] = 1.0
            else:
                f_scores[feat] = 0.0
                p_values[feat] = 1.0

        # Mutual information
        X = df[USABLE_FEATURES].values
        le = LabelEncoder()
        y = le.fit_transform(df["genre"].values)
        try:
            mi_vals = mutual_info_classif(X, y, random_state=42)
            mi_scores = {feat: float(mi_vals[i]) for i, feat in enumerate(USABLE_FEATURES)}
        except Exception:
            mi_scores = {feat: 0.0 for feat in USABLE_FEATURES}

        # Categorize features
        keep = []
        downweight = []
        remove = []
        add_candidates = []

        for feat in USABLE_FEATURES:
            f = f_scores.get(feat, 0)
            p = p_values.get(feat, 1)
            mi = mi_scores.get(feat, 0)
            weakness_count = len(feature_weakness.get(feat, []))
            weakness_ratio = weakness_count / max(len(df), 1)

            if p < 0.05 and f > 2.0:
                category = "KEEP"
                reason = f"F={f:.2f}, p={p:.2e}, MI={mi:.4f} — statistically significant"
                keep.append((feat, reason))
            elif mi > 0.05 and p < 0.2:
                category = "KEEP"
                reason = f"F={f:.2f}, p={p:.2e}, MI={mi:.4f} — moderate signal via MI"
                keep.append((feat, reason))
            elif p > 0.4 and f < 1.0 and mi < 0.01:
                category = "REMOVE"
                reason = f"F={f:.2f}, p={p:.2e}, MI={mi:.4f} — no signal"
                remove.append((feat, reason))
            else:
                category = "DOWNWEIGHT"
                reason = f"F={f:.2f}, p={p:.2e}, MI={mi:.4f} — weak signal"
                downweight.append((feat, reason))

        # Missing features that would add value
        missing_features = [
            ("spectral_centroid", "Differentiates bright vs dark timbres — useful for Electronic vs Rock"),
            ("spectral_rolloff", "Frequency energy distribution — genre-correlated"),
            ("spectral_flux", "Onset attack sharpness — drums vs sustained sounds"),
            ("onset_density", "Rhythm complexity — Hip-Hop vs Country separation"),
            ("zero_crossing_rate", "Noise vs tonal content — Metal vs Pop"),
            ("bass_energy", "Low-frequency energy — Hip-Hop, Electronic, Metal"),
            ("mid_energy", "Vocal/instrument range — Pop, Country, Rock"),
            ("high_energy", "Cymbal/hi-hat presence — Metal, Electronic"),
            ("vocal_presence", "Vocal vs instrumental balance — key genre separator"),
            ("instrumentalness", "Electronic/Classical vs vocal genres"),
            ("rms_mean", "Overall loudness profile"),
        ]
        for feat, reason in missing_features:
            add_candidates.append((feat, reason))

        proposal_lines = []
        proposal_lines.append("=" * 70)
        proposal_lines.append("FEATURE REFINEMENT PROPOSALS (REPORT ONLY — NO DB CHANGES)")
        proposal_lines.append("=" * 70)

        proposal_lines.append(f"\n--- KEEP ({len(keep)} features) ---")
        for feat, reason in keep:
            proposal_lines.append(f"  [KEEP] {feat:25s}: {reason}")

        proposal_lines.append(f"\n--- DOWNWEIGHT ({len(downweight)} features) ---")
        for feat, reason in downweight:
            proposal_lines.append(f"  [DOWNWEIGHT] {feat:25s}: {reason}")

        proposal_lines.append(f"\n--- REMOVE ({len(remove)} features) ---")
        for feat, reason in remove:
            proposal_lines.append(f"  [REMOVE] {feat:25s}: {reason}")

        proposal_lines.append(f"\n--- ADD (recommended new features) ---")
        for feat, reason in add_candidates:
            proposal_lines.append(f"  [ADD] {feat:25s}: {reason}")

        proposal_lines.append("\n--- SUMMARY ---")
        proposal_lines.append(f"  Current usable features: {len(USABLE_FEATURES)}")
        proposal_lines.append(f"  KEEP: {len(keep)}")
        proposal_lines.append(f"  DOWNWEIGHT: {len(downweight)}")
        proposal_lines.append(f"  REMOVE: {len(remove)}")
        proposal_lines.append(f"  ADD candidates: {len(add_candidates)}")

        for line in proposal_lines:
            self.emit(line)

        # Feature importance updated CSV
        importance_rows = []
        for feat in USABLE_FEATURES:
            importance_rows.append({
                "feature": feat,
                "anova_f": f_scores.get(feat, 0),
                "anova_p": p_values.get(feat, 1),
                "mutual_info": mi_scores.get(feat, 0),
                "misfit_deviation_count": len(feature_weakness.get(feat, [])),
                "recommendation": next(
                    (cat for cat, items in [("KEEP", keep), ("DOWNWEIGHT", downweight), ("REMOVE", remove)]
                     for f, _ in items if f == feat),
                    "UNKNOWN"
                ),
            })
        importance_df = pd.DataFrame(importance_rows).sort_values("anova_f", ascending=False)

        return proposal_lines, importance_df

    # ================================================================
    # WRITE PROOF
    # ================================================================
    def write_proof(self, part_a_ok, part_b_ok, part_c_ok, part_c_data,
                    per_track_lines, weakness_lines, proposal_lines,
                    distance_df, importance_df, queue_rows):
        self.emit("\n" + "=" * 60)
        self.emit("WRITING PROOF FILES")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, text):
            (PROOF_DIR / name).write_text(text, encoding="utf-8")

        # 00 misfit queue summary
        lines = ["=== MISFIT QUEUE SUMMARY ===",
                 f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                 f"Source: {MISFIT_QUEUE_CSV}",
                 f"Total misfits: {len(queue_rows)}",
                 ""]
        for r in queue_rows:
            lines.append(
                f"  track_id={r['track_id']:5d}  genre={r['current_genre']:15s}  "
                f"anomaly={r['anomaly_score']}  {r['notes']}"
            )
        w("00_misfit_queue_summary.txt", "\n".join(lines))

        # 01 review input summary
        review_lines = ["=== REVIEW INPUT SUMMARY ===",
                        f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                        f"Source: {MISFIT_INPUT_CSV}"]
        if MISFIT_INPUT_CSV.exists():
            with open(MISFIT_INPUT_CSV, "r", encoding="utf-8-sig") as f:
                rir = list(csv.DictReader(f))
            review_lines.append(f"Rows: {len(rir)}")
            action_dist = {}
            for r in rir:
                a = r.get("action", "unknown")
                action_dist[a] = action_dist.get(a, 0) + 1
            for a, c in sorted(action_dist.items()):
                review_lines.append(f"  {a}: {c}")
        w("01_review_input_summary.txt", "\n".join(review_lines))

        # 02 rows updated
        c_summary = part_c_data.get("summary", {})
        w("02_rows_updated.txt", "\n".join(
            [f"=== ROWS UPDATED: {len(part_c_data.get('updates', []))} ===", ""]
            + (part_c_data.get("updates", []) or ["(none)"])
        ))

        # 03 rows inserted
        w("03_rows_inserted.txt", "\n".join(
            [f"=== ROWS INSERTED: {len(part_c_data.get('inserts', []))} ===", ""]
            + (part_c_data.get("inserts", []) or ["(none)"])
        ))

        # 04 feature deviation report
        w("04_feature_deviation_report.txt", "\n".join(
            ["=== PER-TRACK FEATURE DEVIATION REPORT ===",
             f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
            + per_track_lines
        ))

        # 05 feature weakness summary
        w("05_feature_weakness_summary.txt", "\n".join(
            ["=== FEATURE WEAKNESS SUMMARY ===",
             f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
            + weakness_lines
        ))

        # 06 refinement proposals
        w("06_refinement_proposals.txt", "\n".join(proposal_lines))

        # 07 validation queries
        conn = self.connect(readonly=True)
        vq_lines = ["=== VALIDATION QUERIES ===",
                     f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}", ""]
        queries = [
            "SELECT COUNT(*) FROM track_genre_labels WHERE applied_by='misfit_review';",
            ("SELECT track_id, COUNT(*) FROM track_genre_labels "
             "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1;"),
            "PRAGMA foreign_key_check;",
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary';",
            "SELECT COUNT(*) FROM track_genre_labels;",
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
        w("07_validation_queries.txt", "\n".join(vq_lines))

        # 08 final report
        gate = "PASS" if (part_a_ok and part_b_ok and part_c_ok) else "FAIL"
        report = [
            "=" * 60,
            "MISFIT REVIEW + FEATURE REFINEMENT — FINAL REPORT",
            "=" * 60,
            f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Elapsed: {elapsed}s",
            f"GATE: {gate}",
            "",
            f"PART A — Misfit Queue: {'PASS' if part_a_ok else 'FAIL'}",
            f"  Queue size: {len(queue_rows)}",
            "",
            f"PART B — Review Template: {'PASS' if part_b_ok else 'FAIL'}",
            "",
            f"PART C — Ingest Reviews: {'PASS' if part_c_ok else 'FAIL'}",
            f"  Processed: {c_summary.get('processed', 0)}",
            f"  Updates: {c_summary.get('updates', 0)}",
            f"  Inserts: {c_summary.get('inserts', 0)}",
            f"  Feature issues: {c_summary.get('feature_issues', 0)}",
            f"  Misfit review labels: {c_summary.get('misfit_review_labels', 0)}",
            f"  Dup primaries: {c_summary.get('dup_primaries', 0)}",
            f"  FK violations: {c_summary.get('fk_violations', 0)}",
            "",
            "PART D — Feature Diagnostics: computed",
            "PART E — Refinement Proposals: generated (no DB changes)",
            "",
            f"Proof: {PROOF_DIR}",
        ]
        w("08_final_report.txt", "\n".join(report))

        # execution log
        w("execution_log.txt", "\n".join(self.log))

        # Optional CSVs
        if distance_df is not None and len(distance_df) > 0:
            distance_df.to_csv(PROOF_DIR / "misfit_distance_matrix.csv",
                               index=False, encoding="utf-8")

        if importance_df is not None and len(importance_df) > 0:
            importance_df.to_csv(PROOF_DIR / "feature_importance_updated.csv",
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

    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    p.emit(f"Mode: {mode}")

    if mode in ("queue", "all"):
        part_a_ok, df, centroids, misfits, queue_rows = p.part_a()
        part_b_ok, review_rows = p.part_b(queue_rows)
    else:
        part_a_ok = True
        part_b_ok = True
        df = p.load_benchmark_dataset()
        for feat in USABLE_FEATURES:
            if feat in df.columns:
                df[feat] = df[feat].fillna(df[feat].median())
        centroids = p.compute_genre_centroids(df)
        misfits = [d for d in p.compute_track_deviations(df, centroids) if d["n_deviations"] >= MIN_DEVIATIONS]
        queue_rows = []

    if mode in ("ingest", "all"):
        part_c_ok, part_c_data = p.part_c()
    else:
        part_c_ok = True
        part_c_data = {"summary": {}, "updates": [], "inserts": [],
                       "feature_issues": [], "conflicts": [], "invalid": []}

    if mode == "all":
        per_track_lines, weakness_lines, feature_weakness, distance_df = p.part_d(df, centroids, misfits)
        proposal_lines, importance_df = p.part_e(df, centroids, feature_weakness)

        gate = p.write_proof(
            part_a_ok, part_b_ok, part_c_ok, part_c_data,
            per_track_lines, weakness_lines, proposal_lines,
            distance_df, importance_df, queue_rows
        )

        print(f"\n{'='*60}")
        print(f"PF={PROOF_DIR}")
        print(f"GATE={gate}")
        return 0 if gate == "PASS" else 1

    print(f"Mode '{mode}' completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
