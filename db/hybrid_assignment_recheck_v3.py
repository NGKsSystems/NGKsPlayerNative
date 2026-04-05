#!/usr/bin/env python3
"""
Phase — Hybrid Subgenre Assignment + Secondary Recheck V3

1. Assigns approved hybrid subgenres to blocked tracks' primary labels
2. Re-runs secondary evaluation on ALL 86 secondaries using Rule Engine V2
3. Removes REMOVE decisions, preserves KEEP/REVIEW
"""

import io
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
PROOF_DIR = WORKSPACE / "_proof" / "hybrid_assignment_recheck_v3"
DATA_DIR = WORKSPACE / "data"
BLOCKED_CSV = DATA_DIR / "blocked_hybrid_taxonomy_cases_v1.csv"
APPROVED_CSV = DATA_DIR / "approved_hybrid_subgenres_v1.csv"
RULE_ENGINE_V2 = DATA_DIR / "secondary_rule_engine_v2.csv"

ASSIGN_APPLIED_BY = "hybrid_subgenre_assignment_v1"
RECHECK_APPLIED_BY = "secondary_recheck_v3"


class HybridAssignmentRecheckPipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.assignments = []
        self.recheck_results = []
        self.removals = []
        self.remaining = []
        self.before_sec_count = 0
        self.after_sec_count = 0
        self.before_sub_coverage = 0
        self.after_sub_coverage = 0

    def emit(self, msg):
        self.log.append(msg)
        print(msg)

    def connect_rw(self):
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

    # ================================================================
    # PART A — LOAD ASSIGNMENT TARGETS
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- LOAD ASSIGNMENT TARGETS")
        self.emit("=" * 70)

        # Load blocked cases
        blocked = pd.read_csv(BLOCKED_CSV)
        self.emit(f"  Blocked cases loaded: {len(blocked)}")

        # Filter to only missing_hybrid_subgenre cases
        assignable = blocked[blocked["blocking_reason"] == "missing_hybrid_subgenre"].copy()
        self.emit(f"  Assignable (missing_hybrid_subgenre): {len(assignable)}")

        # Load approved subgenres
        approved = pd.read_csv(APPROVED_CSV)
        self.emit(f"  Approved subgenres: {len(approved)}")

        # Get subgenre IDs from DB
        conn = self.connect_ro()
        sub_map = {}
        for row in conn.execute(
            "SELECT s.id, s.name, g.name AS genre FROM subgenres s "
            "JOIN genres g ON s.genre_id = g.id "
            "WHERE s.name IN ('Country Rap', 'Rap Rock', 'Pop Rap')"
        ).fetchall():
            sub_map[(row["genre"], row["name"])] = row["id"]

        # Also record baseline
        self.before_sec_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        self.before_sub_coverage = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]
        conn.close()

        self.emit(f"  Subgenre ID map: {sub_map}")
        self.emit(f"  Before: secondaries={self.before_sec_count}, sub_coverage={self.before_sub_coverage}")

        # Build assignment map
        # missing_subgenre_candidate -> (parent_genre, subgenre_name) -> subgenre_id
        candidate_to_sub = {
            "Country Rap": ("Country", "Country Rap", sub_map.get(("Country", "Country Rap"))),
            "Rap Rock": ("Hip-Hop", "Rap Rock", sub_map.get(("Hip-Hop", "Rap Rock"))),
            "Pop Rap": ("Hip-Hop", "Pop Rap", sub_map.get(("Hip-Hop", "Pop Rap"))),
        }

        for _, row in assignable.iterrows():
            cand = row["missing_subgenre_candidate"]
            if cand not in candidate_to_sub:
                self.emit(f"  SKIP unknown candidate: {cand} for track {row['track_id']}")
                continue
            parent, sub_name, sub_id = candidate_to_sub[cand]
            if sub_id is None:
                self.emit(f"  FATAL: subgenre '{sub_name}' not found in DB")
                raise ValueError(f"Missing subgenre: {sub_name}")

            # Verify primary genre matches
            if row["primary_genre"] != parent:
                # Country Rap could also be assigned to Hip-Hop primary tracks
                # with Country secondary (Unknown/Dax case)
                # But the blocked CSV already captures the correct primary_genre
                self.emit(f"  NOTE: primary_genre mismatch for track {row['track_id']}: "
                          f"  expected {parent}, got {row['primary_genre']}")
                # The Hip-Hop→Country case (Dax): primary=Hip-Hop, but Country Rap
                # is under Country. This track needs different handling.
                # Actually, looking at the blocked CSV:
                # Unknown/Dax track: primary=Hip-Hop, secondary=Country
                # missing_subgenre_candidate = "Country Rap"
                # But Country Rap is under Country (genre_id=8), not Hip-Hop (genre_id=2)
                # So we can't assign Country Rap to a Hip-Hop primary.
                # This is a genuine mismatch - skip it.
                continue

            self.assignments.append({
                "track_id": int(row["track_id"]),
                "artist": row["artist"],
                "title": row["title"],
                "primary_genre": parent,
                "assigned_subgenre": sub_name,
                "subgenre_id": sub_id,
                "assignment_reason": f"Blocked hybrid case: {parent}→{cand}, "
                                     f"subgenre added in taxonomy expansion phase",
                "confidence": "high",
            })

        self.emit(f"  Assignment map built: {len(self.assignments)} tracks")

        # Breakdown
        by_sub = {}
        for a in self.assignments:
            by_sub[a["assigned_subgenre"]] = by_sub.get(a["assigned_subgenre"], 0) + 1
        for sub, cnt in sorted(by_sub.items(), key=lambda x: -x[1]):
            self.emit(f"    {sub}: {cnt}")

        # Write assignment CSV
        df = pd.DataFrame(self.assignments)
        cols = ["track_id", "artist", "title", "primary_genre",
                "assigned_subgenre", "assignment_reason", "confidence"]
        df[cols].to_csv(DATA_DIR / "hybrid_subgenre_assignment_v1.csv",
                        index=False, encoding="utf-8")

        # Proof 00
        lines = []
        lines.append("=" * 70)
        lines.append("ASSIGNMENT MAP")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"Total assignments: {len(self.assignments)}")
        lines.append(f"Skipped (genre mismatch): {len(assignable) - len(self.assignments)}")
        for sub, cnt in sorted(by_sub.items(), key=lambda x: -x[1]):
            lines.append(f"  {sub}: {cnt}")
        lines.append(f"\nDetail:")
        for a in self.assignments:
            lines.append(f"  {a['track_id']:5d} | {a['artist']:20s} | "
                         f"{a['assigned_subgenre']:12s} | {a['confidence']}")

        (PROOF_DIR / "00_assignment_map.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART B — APPLY SUBGENRE ASSIGNMENTS
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B -- APPLY SUBGENRE ASSIGNMENTS")
        self.emit("=" * 70)

        conn = self.connect_rw()
        updated = 0
        skipped_existing = 0

        try:
            for a in self.assignments:
                # Check current subgenre_id — DO NOT overwrite non-null
                row = conn.execute(
                    "SELECT id, subgenre_id, applied_by FROM track_genre_labels "
                    "WHERE track_id = ? AND role = 'primary'",
                    (a["track_id"],)
                ).fetchone()

                if row is None:
                    self.emit(f"  WARN: No primary label for track {a['track_id']}")
                    continue

                if row["subgenre_id"] is not None:
                    skipped_existing += 1
                    self.emit(f"  SKIP: track {a['track_id']} already has subgenre_id={row['subgenre_id']}")
                    continue

                conn.execute(
                    "UPDATE track_genre_labels SET subgenre_id = ?, applied_by = ? "
                    "WHERE id = ?",
                    (a["subgenre_id"], ASSIGN_APPLIED_BY, row["id"])
                )
                updated += 1

            conn.commit()
            self.emit(f"  Updated: {updated}")
            self.emit(f"  Skipped (existing subgenre): {skipped_existing}")

        except Exception as e:
            conn.rollback()
            self.emit(f"  FATAL: Rollback: {e}")
            raise
        finally:
            conn.close()

        # Verify coverage
        conn = self.connect_ro()
        self.after_sub_coverage = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]
        conn.close()

        self.emit(f"  Subgenre coverage: {self.before_sub_coverage} → {self.after_sub_coverage} "
                  f"(+{self.after_sub_coverage - self.before_sub_coverage})")

        # Proof 01
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS UPDATED (SUBGENRE ASSIGNMENTS)")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"Updated: {updated}")
        lines.append(f"Skipped (existing): {skipped_existing}")
        lines.append(f"Coverage: {self.before_sub_coverage} → {self.after_sub_coverage}")
        lines.append(f"applied_by: {ASSIGN_APPLIED_BY}")

        (PROOF_DIR / "01_rows_updated_subgenres.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART C — SECONDARY LABEL RECHECK V3
    # ================================================================
    def part_c(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART C -- SECONDARY LABEL RECHECK V3")
        self.emit("=" * 70)

        # Load rule engine V2
        rules_df = pd.read_csv(RULE_ENGINE_V2)
        rules = rules_df.to_dict("records")

        # Build rule lookups
        explicit_hybrid = {}   # (parent, subgenre, secondary) -> rule
        stylistic_bridge = {}  # (parent, subgenre, secondary) -> rule
        prohibited = {}        # (parent, subgenre) -> rule
        wildcard_rules = {}    # subgenre -> rule (parent = "*")

        for r in rules:
            pg = r["aligned_parent_genre"]
            sub = r["aligned_subgenre"]
            sec = r["allowed_secondary_genre"]
            rt = r["rule_type"]

            if pg == "*":
                wildcard_rules[sub] = r
            elif rt == "prohibited":
                prohibited[(pg, sub)] = r
            elif rt == "explicit_hybrid":
                explicit_hybrid[(pg, sub, sec)] = r
            elif rt == "stylistic_bridge":
                stylistic_bridge[(pg, sub, sec)] = r

        self.emit(f"  Rules loaded: explicit_hybrid={len(explicit_hybrid)}, "
                  f"stylistic_bridge={len(stylistic_bridge)}, "
                  f"prohibited={len(prohibited)}, wildcard={len(wildcard_rules)}")

        # Get all secondaries with their primary context
        conn = self.connect_ro()
        rows = conn.execute("""
            SELECT ts.id AS sec_label_id, ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   tp.subgenre_id AS prim_sub_id,
                   COALESCE(sp.name, '') AS aligned_subgenre
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            LEFT JOIN subgenres sp ON tp.subgenre_id = sp.id
            WHERE ts.role = 'secondary'
            ORDER BY gp.name, t.artist, t.title
        """).fetchall()
        conn.close()

        self.emit(f"  Secondaries to evaluate: {len(rows)}")

        for row in rows:
            r = dict(row)
            pg = r["primary_genre"]
            sg = r["secondary_genre"]
            sub = r["aligned_subgenre"]
            artist = r["artist"]

            decision = "REVIEW"
            rule_type = ""
            reason = ""

            if not sub:
                # No subgenre — cannot evaluate
                # Categorize edge cases
                if artist == "Unknown":
                    reason = "Placeholder artist, no subgenre, cannot evaluate"
                elif artist == "Weird Al Yankovic":
                    reason = "Comedy/parody artist, no applicable subgenre (HELD)"
                elif artist == "Zac Brown Band":
                    reason = "Ambiguous subgenre (Country Pop candidate but uncertain)"
                elif pg == "Pop" and sg == "Rock":
                    reason = "Pop→Rock edge case, no Pop subgenre assigned"
                else:
                    reason = f"No primary subgenre assigned for {pg}"
                decision = "REVIEW"
                rule_type = "no_subgenre"
            else:
                # Has subgenre — evaluate against rules
                triple = (pg, sub, sg)

                if triple in explicit_hybrid:
                    rule = explicit_hybrid[triple]
                    decision = "KEEP"
                    rule_type = "explicit_hybrid"
                    reason = (f"Rule match: {pg}/{sub} → {sg} "
                              f"[explicit_hybrid, {rule.get('confidence_tier', '')}]")
                elif triple in stylistic_bridge:
                    rule = stylistic_bridge[triple]
                    conf = rule.get("confidence_tier", "")
                    if conf == "high":
                        decision = "KEEP"
                        reason = (f"Rule match: {pg}/{sub} → {sg} "
                                  f"[stylistic_bridge, high confidence]")
                    else:
                        decision = "REMOVE"
                        reason = (f"Stylistic bridge [{conf}] insufficient evidence: "
                                  f"{pg}/{sub} → {sg}")
                    rule_type = "stylistic_bridge"
                elif (pg, sub) in prohibited:
                    decision = "REMOVE"
                    rule_type = "prohibited"
                    reason = f"Prohibited: {pg}/{sub} has prohibited secondary"
                else:
                    # No rule matches
                    decision = "REMOVE"
                    rule_type = "no_rule_match"
                    reason = f"No rule supports {pg}/{sub} → {sg}"

            self.recheck_results.append({
                "sec_label_id": r["sec_label_id"],
                "track_id": r["track_id"],
                "artist": artist,
                "title": r["title"],
                "primary_genre": pg,
                "aligned_subgenre": sub or "(null)",
                "secondary_genre": sg,
                "rule_type": rule_type,
                "decision": decision,
                "reason": reason,
            })

        # Summary
        counts = {"KEEP": 0, "REMOVE": 0, "REVIEW": 0}
        for rr in self.recheck_results:
            counts[rr["decision"]] += 1

        self.emit(f"  Results: KEEP={counts['KEEP']}, REMOVE={counts['REMOVE']}, "
                  f"REVIEW={counts['REVIEW']}")

        # Write recheck CSV
        df = pd.DataFrame(self.recheck_results)
        df.to_csv(DATA_DIR / "secondary_recheck_v3_results.csv",
                  index=False, encoding="utf-8")

        # Proof 02
        lines = []
        lines.append("=" * 70)
        lines.append("RECHECK V3 RESULTS")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"Total evaluated: {len(self.recheck_results)}")
        lines.append(f"KEEP: {counts['KEEP']}")
        lines.append(f"REMOVE: {counts['REMOVE']}")
        lines.append(f"REVIEW: {counts['REVIEW']}")
        lines.append(f"\nKEEP tracks:")
        for rr in self.recheck_results:
            if rr["decision"] == "KEEP":
                lines.append(f"  {rr['track_id']:5d} | {rr['artist']:25s} | "
                             f"{rr['primary_genre']:8s}/{rr['aligned_subgenre']:15s} → "
                             f"{rr['secondary_genre']:8s} | {rr['rule_type']}")
        lines.append(f"\nREMOVE tracks:")
        for rr in self.recheck_results:
            if rr["decision"] == "REMOVE":
                lines.append(f"  {rr['track_id']:5d} | {rr['artist']:25s} | "
                             f"{rr['primary_genre']:8s}/{rr['aligned_subgenre']:15s} → "
                             f"{rr['secondary_genre']:8s} | {rr['reason']}")
        lines.append(f"\nREVIEW tracks:")
        for rr in self.recheck_results:
            if rr["decision"] == "REVIEW":
                lines.append(f"  {rr['track_id']:5d} | {rr['artist']:25s} | "
                             f"{rr['primary_genre']:8s}/{rr['aligned_subgenre']:15s} → "
                             f"{rr['secondary_genre']:8s} | {rr['reason']}")

        (PROOF_DIR / "02_recheck_results.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART D — APPLY REMOVALS
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D -- APPLY REMOVALS")
        self.emit("=" * 70)

        to_remove = [rr for rr in self.recheck_results if rr["decision"] == "REMOVE"]
        self.emit(f"  Removals to apply: {len(to_remove)}")

        conn = self.connect_rw()
        removed = 0

        try:
            for rr in to_remove:
                # Delete the secondary label row by its label ID
                cur = conn.execute(
                    "DELETE FROM track_genre_labels WHERE id = ? AND role = 'secondary'",
                    (rr["sec_label_id"],)
                )
                if cur.rowcount == 1:
                    removed += 1
                    self.removals.append({
                        "track_id": rr["track_id"],
                        "artist": rr["artist"],
                        "title": rr["title"],
                        "removed_secondary_genre": rr["secondary_genre"],
                        "primary_genre": rr["primary_genre"],
                        "aligned_subgenre": rr["aligned_subgenre"],
                        "reason": rr["reason"],
                        "rule_type": rr["rule_type"],
                        "applied_by": RECHECK_APPLIED_BY,
                    })
                else:
                    self.emit(f"  WARN: Delete affected {cur.rowcount} rows "
                              f"for sec_label_id={rr['sec_label_id']}")

            conn.commit()
            self.emit(f"  Removed: {removed}")

        except Exception as e:
            conn.rollback()
            self.emit(f"  FATAL: Rollback: {e}")
            raise
        finally:
            conn.close()

        # Get after count
        conn = self.connect_ro()
        self.after_sec_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        conn.close()

        self.emit(f"  Secondaries: {self.before_sec_count} → {self.after_sec_count} "
                  f"(-{self.before_sec_count - self.after_sec_count})")

        # Build remaining secondaries list
        conn = self.connect_ro()
        surviving = conn.execute("""
            SELECT ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   COALESCE(sp.name, '(null)') AS aligned_subgenre
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            LEFT JOIN subgenres sp ON tp.subgenre_id = sp.id
            WHERE ts.role = 'secondary'
            ORDER BY gp.name, t.artist
        """).fetchall()
        conn.close()

        self.remaining = [dict(r) for r in surviving]

        # Proof 03
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS REMOVED (SECONDARY LABELS)")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"Total removed: {removed}")
        lines.append(f"Secondaries: {self.before_sec_count} → {self.after_sec_count}")
        lines.append(f"applied_by: {RECHECK_APPLIED_BY}")
        lines.append(f"\nRemoved rows:")
        for rm in self.removals:
            lines.append(f"  {rm['track_id']:5d} | {rm['artist']:25s} | "
                         f"{rm['primary_genre']}/{rm['aligned_subgenre']} → "
                         f"{rm['removed_secondary_genre']} | {rm['reason'][:60]}")

        (PROOF_DIR / "03_rows_removed.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Proof 04
        lines = []
        lines.append("=" * 70)
        lines.append("REMAINING SECONDARY LABELS")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"Count: {len(self.remaining)}")
        lines.append(f"\nDetail:")
        for r in self.remaining:
            lines.append(f"  {r['track_id']:5d} | {r['artist']:25s} | "
                         f"{r['primary_genre']:8s}/{r['aligned_subgenre']:15s} → "
                         f"{r['secondary_genre']}")

        (PROOF_DIR / "04_remaining_secondaries.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Write surviving and removed CSVs
        pd.DataFrame(self.remaining).to_csv(
            PROOF_DIR / "surviving_pairs.csv", index=False, encoding="utf-8")
        pd.DataFrame(self.removals).to_csv(
            PROOF_DIR / "removed_pairs.csv", index=False, encoding="utf-8")

    # ================================================================
    # PART E — FINAL STATE SUMMARY
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E -- FINAL STATE SUMMARY")
        self.emit("=" * 70)

        keep_count = sum(1 for rr in self.recheck_results if rr["decision"] == "KEEP")
        remove_count = sum(1 for rr in self.recheck_results if rr["decision"] == "REMOVE")
        review_count = sum(1 for rr in self.recheck_results if rr["decision"] == "REVIEW")

        # Genre pair distributions
        surviving_pairs = {}
        for r in self.remaining:
            pair = f"{r['primary_genre']} → {r['secondary_genre']}"
            surviving_pairs[pair] = surviving_pairs.get(pair, 0) + 1

        removed_pairs = {}
        for rm in self.removals:
            pair = f"{rm['primary_genre']} → {rm['removed_secondary_genre']}"
            removed_pairs[pair] = removed_pairs.get(pair, 0) + 1

        # Edge cases
        review_tracks = [rr for rr in self.recheck_results if rr["decision"] == "REVIEW"]

        lines = []
        lines.append("=" * 70)
        lines.append("DISTRIBUTION SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        lines.append(f"\n1. SECONDARY LABEL COUNTS:")
        lines.append(f"   Before: {self.before_sec_count}")
        lines.append(f"   After:  {self.after_sec_count}")
        lines.append(f"   KEEP:   {keep_count}")
        lines.append(f"   REMOVE: {remove_count}")
        lines.append(f"   REVIEW: {review_count}")

        lines.append(f"\n2. SURVIVING GENRE PAIRS:")
        for pair, cnt in sorted(surviving_pairs.items(), key=lambda x: -x[1]):
            lines.append(f"   {pair}: {cnt}")

        lines.append(f"\n3. REMOVED GENRE PAIRS:")
        for pair, cnt in sorted(removed_pairs.items(), key=lambda x: -x[1]):
            lines.append(f"   {pair}: {cnt}")

        lines.append(f"\n4. EDGE CASES (REVIEW):")
        for rr in review_tracks:
            lines.append(f"   {rr['track_id']:5d} | {rr['artist']:25s} | "
                         f"{rr['primary_genre']}/{rr['aligned_subgenre']} → "
                         f"{rr['secondary_genre']} | {rr['reason'][:80]}")

        lines.append(f"\n5. SUBGENRE COVERAGE:")
        lines.append(f"   Before: {self.before_sub_coverage}")
        lines.append(f"   After:  {self.after_sub_coverage}")
        lines.append(f"   Gain:   +{self.after_sub_coverage - self.before_sub_coverage}")

        self.emit(f"  Secondaries: {self.before_sec_count} → {self.after_sec_count}")
        self.emit(f"  Coverage: {self.before_sub_coverage} → {self.after_sub_coverage}")
        self.emit(f"  Remaining: {len(self.remaining)} ({keep_count} KEEP + {review_count} REVIEW)")

        (PROOF_DIR / "05_distribution_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART G — VALIDATION
    # ================================================================
    def part_g(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART G -- VALIDATION")
        self.emit("=" * 70)

        conn = self.connect_ro()
        all_ok = True
        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. Primary count unchanged
        prim = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk1 = prim == 783
        val.append(f"\n  1. Primary labels: {prim} (expected 783) "
                   f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Secondary count = before - removals
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        expected_sec = self.before_sec_count - len(self.removals)
        chk2 = sec == expected_sec
        val.append(f"  2. Secondary labels: {sec} (expected {expected_sec}) "
                   f"-- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. No duplicate primaries
        dup_prim = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk3 = dup_prim == 0
        val.append(f"  3. Duplicate primaries: {dup_prim} "
                   f"-- {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. Benchmark unchanged
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
            "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"
        ).fetchone()[0]
        chk4 = bench == 202
        val.append(f"  4. Benchmark: {bench} (expected 202) "
                   f"-- {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. FK integrity
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk5 = len(fk) == 0
        val.append(f"  5. FK violations: {len(fk)} "
                   f"-- {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. Subgenre assignments only on intended tracks
        assigned_ids = {a["track_id"] for a in self.assignments}
        actual = conn.execute(
            "SELECT track_id FROM track_genre_labels "
            "WHERE role='primary' AND applied_by = ?",
            (ASSIGN_APPLIED_BY,)
        ).fetchall()
        actual_ids = {r["track_id"] for r in actual}
        chk6 = actual_ids == assigned_ids
        val.append(f"  6. Subgenre assignments match intended: "
                   f"actual={len(actual_ids)}, expected={len(assigned_ids)} "
                   f"-- {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            diff = actual_ids.symmetric_difference(assigned_ids)
            val.append(f"     Mismatch track_ids: {diff}")
            all_ok = False

        # 7. Primary genres unchanged (spot check: count per genre)
        genre_counts = conn.execute(
            "SELECT g.name, COUNT(*) FROM track_genre_labels tgl "
            "JOIN genres g ON tgl.genre_id = g.id "
            "WHERE tgl.role = 'primary' GROUP BY g.name ORDER BY g.name"
        ).fetchall()
        val.append(f"  7. Primary genre distribution (spot check):")
        for gc in genre_counts:
            val.append(f"       {gc[0]}: {gc[1]}")

        # 8. Only REMOVE actions affected secondaries
        keep_ids = {rr["sec_label_id"] for rr in self.recheck_results if rr["decision"] == "KEEP"}
        review_ids = {rr["sec_label_id"] for rr in self.recheck_results if rr["decision"] == "REVIEW"}
        surviving_ids_in_db = set()
        for row in conn.execute(
            "SELECT id FROM track_genre_labels WHERE role='secondary'"
        ).fetchall():
            surviving_ids_in_db.add(row["id"])

        # Check KEEP and REVIEW ids are still in DB
        keep_still = keep_ids.issubset(surviving_ids_in_db)
        review_still = review_ids.issubset(surviving_ids_in_db)
        chk8 = keep_still and review_still
        val.append(f"  8. KEEP labels preserved: {keep_still}, "
                   f"REVIEW labels preserved: {review_still} "
                   f"-- {'PASS' if chk8 else 'FAIL'}")
        if not chk8:
            all_ok = False

        conn.close()

        val.append(f"\n  SQL verification:")
        val.append(f"    primaries: {prim}")
        val.append(f"    secondaries: {sec}")
        val.append(f"    dup_primaries: {dup_prim}")
        val.append(f"    benchmark: {bench}")
        val.append(f"    FK violations: {len(fk)}")

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        (PROOF_DIR / "06_validation_checks.txt").write_text(
            "\n".join(val), encoding="utf-8"
        )

        self.emit(f"  Validation: {'PASS' if all_ok else 'FAIL'}")
        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"

        keep_count = sum(1 for rr in self.recheck_results if rr["decision"] == "KEEP")
        remove_count = sum(1 for rr in self.recheck_results if rr["decision"] == "REMOVE")
        review_count = sum(1 for rr in self.recheck_results if rr["decision"] == "REVIEW")

        report = []
        report.append("=" * 70)
        report.append("HYBRID SUBGENRE ASSIGNMENT + SECONDARY RECHECK V3 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- SUBGENRE ASSIGNMENTS ---")
        report.append(f"  Tracks assigned: {len(self.assignments)}")
        by_sub = {}
        for a in self.assignments:
            by_sub[a["assigned_subgenre"]] = by_sub.get(a["assigned_subgenre"], 0) + 1
        for sub, cnt in sorted(by_sub.items(), key=lambda x: -x[1]):
            report.append(f"    {sub}: {cnt}")
        report.append(f"  Subgenre coverage: {self.before_sub_coverage} → {self.after_sub_coverage} "
                      f"(+{self.after_sub_coverage - self.before_sub_coverage})")

        report.append(f"\n--- SECONDARY RECHECK V3 ---")
        report.append(f"  Evaluated: {len(self.recheck_results)}")
        report.append(f"  KEEP: {keep_count}")
        report.append(f"  REMOVE: {remove_count}")
        report.append(f"  REVIEW: {review_count}")

        report.append(f"\n--- REMOVALS ---")
        report.append(f"  Removed: {len(self.removals)}")
        report.append(f"  Secondaries: {self.before_sec_count} → {self.after_sec_count} "
                      f"(-{self.before_sec_count - self.after_sec_count})")

        report.append(f"\n--- REMAINING SECONDARIES ---")
        report.append(f"  Total: {len(self.remaining)}")
        report.append(f"  KEEP: {keep_count}")
        report.append(f"  REVIEW: {review_count}")
        for r in self.remaining:
            report.append(f"    {r['track_id']:5d} | {r['artist']:25s} | "
                          f"{r['primary_genre']}/{r['aligned_subgenre']} → "
                          f"{r['secondary_genre']}")

        report.append(f"\n--- REVIEW EDGE CASES ---")
        for rr in self.recheck_results:
            if rr["decision"] == "REVIEW":
                report.append(f"    {rr['track_id']:5d} | {rr['artist']:25s} | "
                              f"{rr['reason'][:80]}")

        report.append(f"\n{'=' * 70}")
        report.append(f"GATE={gate}")
        report.append(f"{'=' * 70}")

        (PROOF_DIR / "07_final_report.txt").write_text(
            "\n".join(report), encoding="utf-8"
        )
        (PROOF_DIR / "execution_log.txt").write_text(
            "\n".join(self.log), encoding="utf-8"
        )

        # Copy data CSVs to proof
        for name in ("hybrid_subgenre_assignment_v1.csv",
                      "secondary_recheck_v3_results.csv"):
            src = DATA_DIR / name
            if src.exists():
                shutil.copy2(src, PROOF_DIR / name)

        self.emit(f"\nPF={PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = HybridAssignmentRecheckPipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    p.part_a()
    p.part_b()
    p.part_c()
    p.part_d()
    p.part_e()
    all_ok = p.part_g()
    gate = p.final_report(all_ok)

    print(f"\n{'=' * 60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
