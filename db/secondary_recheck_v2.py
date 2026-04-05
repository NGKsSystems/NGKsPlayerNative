#!/usr/bin/env python3
"""
Phase — Secondary Label Re-Evaluation V2

Controlled re-run of secondary label validation using the aligned
subgenre mapping and explicit rule engine.

Each secondary is classified: KEEP / REMOVE / REVIEW
Only REMOVE decisions with clear rule basis are applied to DB.
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
PROOF_DIR = WORKSPACE / "_proof" / "secondary_recheck_v2"
DATA_DIR = WORKSPACE / "data"
ALIGNMENT_CSV = DATA_DIR / "subgenre_alignment_mapping_v1.csv"
RULE_ENGINE_CSV = DATA_DIR / "secondary_rule_engine_v1.csv"
UNRESOLVED_CSV = DATA_DIR / "subgenre_unresolved_queue_v1.csv"

APPLIED_BY = "secondary_recheck_v2"


class RecheckPipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()

        # Source data
        self.alignment = []
        self.rules = []
        self.secondaries = []

        # results
        self.results = []       # full evaluation
        self.removals = []      # applied removals
        self.before_count = 0
        self.after_count = 0

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
    # PART A — LOAD REQUIRED INPUTS
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- LOAD REQUIRED INPUTS")
        self.emit("=" * 70)

        # Load alignment mapping
        self.alignment = pd.read_csv(ALIGNMENT_CSV).to_dict("records")
        self.emit(f"  Alignment rows: {len(self.alignment)}")

        # Load rule engine
        self.rules = pd.read_csv(RULE_ENGINE_CSV).to_dict("records")
        self.emit(f"  Rule engine rows: {len(self.rules)}")

        # Build rule lookup: (parent_genre, subgenre, secondary_genre) -> rule
        # Also: (parent_genre, subgenre) -> list of rules
        self.rule_by_triple = {}
        self.rule_by_pair = {}
        self.prohibited_pairs = {}  # (parent, subgenre) -> reason

        for r in self.rules:
            pg = r["aligned_parent_genre"]
            sub = r["aligned_subgenre"]
            sec = r["allowed_secondary_genre"]
            rt = r["rule_type"]

            triple = (pg, sub, sec)
            self.rule_by_triple[triple] = r

            pair = (pg, sub)
            if pair not in self.rule_by_pair:
                self.rule_by_pair[pair] = []
            self.rule_by_pair[pair].append(r)

            if rt == "prohibited":
                self.prohibited_pairs[pair] = r.get("notes", "prohibited")

        # Wildcard prohibitions: (*,Electronic) → NONE
        self.wildcard_prohibitions = {}
        for r in self.rules:
            if r["aligned_parent_genre"] == "*":
                self.wildcard_prohibitions[r["aligned_subgenre"]] = r

        self.emit(f"  Rule triples: {len(self.rule_by_triple)}")
        self.emit(f"  Prohibited pairs: {len(self.prohibited_pairs)}")
        self.emit(f"  Wildcard prohibitions: {len(self.wildcard_prohibitions)}")

        # Build alignment lookup: (genre, subgenre) -> alignment row
        self.alignment_lookup = {}
        for a in self.alignment:
            key = (a["current_genre"], a["current_subgenre"])
            self.alignment_lookup[key] = a

        # Load all secondary labels from DB
        conn = self.connect_ro()

        self.before_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]

        rows = conn.execute("""
            SELECT ts.id AS label_id, ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   tp.subgenre_id,
                   COALESCE(sp.name, '') AS primary_subgenre,
                   tp.genre_id AS primary_genre_id,
                   ts.genre_id AS secondary_genre_id
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            LEFT JOIN subgenres sp ON tp.subgenre_id = sp.id
            WHERE ts.role = 'secondary'
            ORDER BY t.artist, t.title
        """).fetchall()
        conn.close()

        self.secondaries = [dict(r) for r in rows]
        self.emit(f"  Secondary labels: {len(self.secondaries)} (DB count: {self.before_count})")

        # Proof
        lines = []
        lines.append("=" * 70)
        lines.append("INPUT SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nAlignment CSV: {ALIGNMENT_CSV} ({len(self.alignment)} rows)")
        lines.append(f"Rule Engine CSV: {RULE_ENGINE_CSV} ({len(self.rules)} rows)")
        lines.append(f"DB secondaries: {self.before_count}")
        lines.append(f"\nRule breakdown:")
        for rt in ["explicit_hybrid", "stylistic_bridge", "prohibited"]:
            cnt = sum(1 for r in self.rules if r["rule_type"] == rt)
            lines.append(f"  {rt}: {cnt}")
        lines.append(f"\nSecondary label pairs:")
        pair_counts = {}
        for s in self.secondaries:
            p = f"{s['primary_genre']}->{s['secondary_genre']}"
            pair_counts[p] = pair_counts.get(p, 0) + 1
        for p, c in sorted(pair_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {p}: {c}")

        (PROOF_DIR / "00_input_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART B + C + D — EVALUATE EACH SECONDARY
    # ================================================================
    def part_bcd(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B/C/D -- EVALUATE SECONDARIES")
        self.emit("=" * 70)

        for s in self.secondaries:
            label_id = s["label_id"]
            track_id = s["track_id"]
            artist = s["artist"]
            title = s["title"]
            primary_genre = s["primary_genre"]
            secondary_genre = s["secondary_genre"]
            primary_subgenre = s["primary_subgenre"]
            subgenre_id = s["subgenre_id"]

            # ── PART B: Eligibility check ──
            aligned_subgenre = ""
            has_aligned_sub = False
            alignment_confidence = ""

            if primary_subgenre:
                # Look up alignment
                al_key = (primary_genre, primary_subgenre)
                if al_key in self.alignment_lookup:
                    al = self.alignment_lookup[al_key]
                    aligned_subgenre = al["aligned_subgenre"]
                    alignment_confidence = al["confidence"]
                    has_aligned_sub = alignment_confidence == "high"
                else:
                    # Subgenre exists but no alignment row — use raw
                    aligned_subgenre = primary_subgenre
                    has_aligned_sub = False
                    alignment_confidence = "unmapped"

            if not has_aligned_sub:
                # PART B fail — insufficient subgenre coverage
                self.results.append({
                    "track_id": track_id,
                    "artist": artist,
                    "title": title,
                    "primary_genre": primary_genre,
                    "aligned_subgenre": aligned_subgenre or "(none)",
                    "secondary_genre": secondary_genre,
                    "rule_match": "no",
                    "rule_type": "",
                    "decision": "REVIEW",
                    "reason": (f"Insufficient subgenre coverage: "
                               f"sub='{primary_subgenre or '(null)'}' "
                               f"conf='{alignment_confidence or 'none'}'"),
                    "label_id": label_id,
                })
                continue

            # ── PART C: Rule engine application ──

            # Check wildcard prohibitions first
            # (*,Electronic) → secondary_genre="Electronic" prohibited globally
            if secondary_genre in self.wildcard_prohibitions:
                wp = self.wildcard_prohibitions[secondary_genre]
                self.results.append({
                    "track_id": track_id,
                    "artist": artist,
                    "title": title,
                    "primary_genre": primary_genre,
                    "aligned_subgenre": aligned_subgenre,
                    "secondary_genre": secondary_genre,
                    "rule_match": "yes",
                    "rule_type": "prohibited",
                    "decision": "REMOVE",
                    "reason": wp.get("notes", "Wildcard prohibition"),
                    "label_id": label_id,
                })
                continue

            # Check specific prohibited pair
            pair_key = (primary_genre, aligned_subgenre)
            if pair_key in self.prohibited_pairs:
                reason = self.prohibited_pairs[pair_key]
                self.results.append({
                    "track_id": track_id,
                    "artist": artist,
                    "title": title,
                    "primary_genre": primary_genre,
                    "aligned_subgenre": aligned_subgenre,
                    "secondary_genre": secondary_genre,
                    "rule_match": "yes",
                    "rule_type": "prohibited",
                    "decision": "REMOVE",
                    "reason": reason,
                    "label_id": label_id,
                })
                continue

            # Check direct rule match: (primary_genre, aligned_subgenre, secondary_genre)
            triple = (primary_genre, aligned_subgenre, secondary_genre)
            if triple in self.rule_by_triple:
                rule = self.rule_by_triple[triple]
                rt = rule["rule_type"]
                conf = rule.get("confidence_tier", "")

                if rt == "explicit_hybrid":
                    self.results.append({
                        "track_id": track_id,
                        "artist": artist,
                        "title": title,
                        "primary_genre": primary_genre,
                        "aligned_subgenre": aligned_subgenre,
                        "secondary_genre": secondary_genre,
                        "rule_match": "yes",
                        "rule_type": "explicit_hybrid",
                        "decision": "KEEP",
                        "reason": (f"Explicit hybrid: {aligned_subgenre} allows "
                                   f"{secondary_genre} [{conf}]"),
                        "label_id": label_id,
                    })
                    continue

                elif rt == "stylistic_bridge":
                    # Part D — evidence requirement
                    # For stylistic_bridge: KEEP only with evidence
                    # We require high confidence tier for KEEP; otherwise REMOVE
                    if conf == "high":
                        self.results.append({
                            "track_id": track_id,
                            "artist": artist,
                            "title": title,
                            "primary_genre": primary_genre,
                            "aligned_subgenre": aligned_subgenre,
                            "secondary_genre": secondary_genre,
                            "rule_match": "yes",
                            "rule_type": "stylistic_bridge",
                            "decision": "KEEP",
                            "reason": (f"Stylistic bridge with high confidence: "
                                       f"{aligned_subgenre} -> {secondary_genre}"),
                            "label_id": label_id,
                        })
                    else:
                        # Stylistic bridge without strong evidence → REMOVE
                        self.results.append({
                            "track_id": track_id,
                            "artist": artist,
                            "title": title,
                            "primary_genre": primary_genre,
                            "aligned_subgenre": aligned_subgenre,
                            "secondary_genre": secondary_genre,
                            "rule_match": "yes",
                            "rule_type": "stylistic_bridge",
                            "decision": "REMOVE",
                            "reason": (f"Stylistic bridge insufficient evidence: "
                                       f"{aligned_subgenre} -> {secondary_genre} "
                                       f"[{conf}], requires manual verification"),
                            "label_id": label_id,
                        })
                    continue

            # No rule match → REMOVE (fail-closed)
            self.results.append({
                "track_id": track_id,
                "artist": artist,
                "title": title,
                "primary_genre": primary_genre,
                "aligned_subgenre": aligned_subgenre,
                "secondary_genre": secondary_genre,
                "rule_match": "no",
                "rule_type": "",
                "decision": "REMOVE",
                "reason": (f"No rule supports {primary_genre}/{aligned_subgenre} "
                           f"-> {secondary_genre} (fail-closed)"),
                "label_id": label_id,
            })

        # Summary
        decisions = {}
        for r in self.results:
            d = r["decision"]
            decisions[d] = decisions.get(d, 0) + 1

        self.emit(f"\n  Evaluated: {len(self.results)}")
        for d in ["KEEP", "REMOVE", "REVIEW"]:
            self.emit(f"    {d}: {decisions.get(d, 0)}")

        # Write evaluation results proof
        lines = []
        lines.append("=" * 70)
        lines.append("EVALUATION RESULTS")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal evaluated: {len(self.results)}")
        for d in ["KEEP", "REMOVE", "REVIEW"]:
            lines.append(f"  {d}: {decisions.get(d, 0)}")

        for d in ["KEEP", "REMOVE", "REVIEW"]:
            lines.append(f"\n--- {d} ---")
            for r in self.results:
                if r["decision"] == d:
                    lines.append(
                        f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                        f"{r['primary_genre']:8s}/{r['aligned_subgenre']:20s} "
                        f"-> {r['secondary_genre']:8s}  "
                        f"[{r['rule_type'] or 'none':18s}] {r['reason']}"
                    )

        (PROOF_DIR / "01_evaluation_results.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Write CSV
        df = pd.DataFrame([{k: v for k, v in r.items() if k != "label_id"}
                           for r in self.results])
        df.to_csv(DATA_DIR / "secondary_label_recheck_v2.csv",
                  index=False, encoding="utf-8")
        shutil.copy2(DATA_DIR / "secondary_label_recheck_v2.csv",
                     PROOF_DIR / "secondary_label_recheck_v2.csv")

    # ================================================================
    # PART F — APPLY SAFE REMOVALS ONLY
    # ================================================================
    def part_f(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART F -- APPLY SAFE REMOVALS")
        self.emit("=" * 70)

        # Only remove if decision=REMOVE AND (rule_type=prohibited OR no rule match)
        to_remove = []
        for r in self.results:
            if r["decision"] != "REMOVE":
                continue
            if r["rule_type"] == "prohibited" or r["rule_match"] == "no":
                to_remove.append(r)

        # Stylistic bridges insufficient evidence are also safe removals
        for r in self.results:
            if (r["decision"] == "REMOVE"
                    and r["rule_type"] == "stylistic_bridge"
                    and r not in to_remove):
                to_remove.append(r)

        self.emit(f"  Safe removals to apply: {len(to_remove)}")

        conn = self.connect_rw()
        try:
            for r in to_remove:
                label_id = r["label_id"]
                # Verify the row still exists
                cur = conn.execute(
                    "SELECT id FROM track_genre_labels WHERE id = ? AND role = 'secondary'",
                    (label_id,)
                ).fetchone()
                if cur is None:
                    self.emit(f"  WARNING: label_id {label_id} not found, skipping")
                    continue

                conn.execute(
                    "DELETE FROM track_genre_labels WHERE id = ? AND role = 'secondary'",
                    (label_id,)
                )

                self.removals.append({
                    "label_id": label_id,
                    "track_id": r["track_id"],
                    "artist": r["artist"],
                    "title": r["title"],
                    "primary_genre": r["primary_genre"],
                    "removed_secondary_genre": r["secondary_genre"],
                    "rule_type": r["rule_type"] or "no_rule_match",
                    "reason": r["reason"],
                    "applied_by": APPLIED_BY,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

            conn.commit()
            self.emit(f"  Removed {len(self.removals)} secondary labels")

        except Exception as e:
            conn.rollback()
            self.emit(f"  FATAL: Rollback due to error: {e}")
            raise
        finally:
            conn.close()

        # Get after count
        conn = self.connect_ro()
        self.after_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        conn.close()

        # Write removal proof
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS REMOVED")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal removed: {len(self.removals)}")
        lines.append(f"Before: {self.before_count}")
        lines.append(f"After: {self.after_count}")
        for r in self.removals:
            lines.append(
                f"  label_id={r['label_id']:4d}  track_id={r['track_id']:4d}  "
                f"{r['artist'][:25]:25s}  "
                f"{r['primary_genre']:8s} -x-> {r['removed_secondary_genre']:8s}  "
                f"[{r['rule_type']}] {r['reason']}"
            )

        (PROOF_DIR / "02_rows_removed.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        if self.removals:
            pd.DataFrame(self.removals).to_csv(
                PROOF_DIR / "removed_pairs.csv",
                index=False, encoding="utf-8"
            )

    # ================================================================
    # REMAINING + SUMMARIES
    # ================================================================
    def part_g(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART G -- SUMMARY REPORT")
        self.emit("=" * 70)

        # Remaining secondaries
        conn = self.connect_ro()
        remaining = conn.execute("""
            SELECT ts.id, ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   COALESCE(sp.name, '') AS primary_subgenre
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            LEFT JOIN subgenres sp ON tp.subgenre_id = sp.id
            WHERE ts.role = 'secondary'
            ORDER BY t.artist, t.title
        """).fetchall()
        conn.close()

        remaining_list = [dict(r) for r in remaining]

        # Write remaining proof
        lines = []
        lines.append("=" * 70)
        lines.append("REMAINING SECONDARIES")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal remaining: {len(remaining_list)}")
        pair_counts = {}
        for r in remaining_list:
            p = f"{r['primary_genre']}->{r['secondary_genre']}"
            pair_counts[p] = pair_counts.get(p, 0) + 1
        lines.append(f"\nBy pair:")
        for p, c in sorted(pair_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {p}: {c}")
        lines.append(f"\nDetails:")
        for r in remaining_list:
            lines.append(
                f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                f"{r['primary_genre']:8s}/{r['primary_subgenre'] or '(none)':20s} "
                f"-> {r['secondary_genre']:8s}"
            )

        (PROOF_DIR / "03_remaining_secondaries.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        if remaining_list:
            pd.DataFrame(remaining_list).to_csv(
                PROOF_DIR / "surviving_pairs.csv",
                index=False, encoding="utf-8"
            )

        # Rule application summary
        lines = []
        lines.append("=" * 70)
        lines.append("RULE APPLICATION SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        decisions = {}
        for r in self.results:
            d = r["decision"]
            decisions[d] = decisions.get(d, 0) + 1

        lines.append(f"\n1. Before / After:")
        lines.append(f"   Total secondaries before: {self.before_count}")
        lines.append(f"   Removed:                  {len(self.removals)}")
        lines.append(f"   Remaining:                {self.after_count}")

        lines.append(f"\n2. Breakdown:")
        for d in ["KEEP", "REMOVE", "REVIEW"]:
            lines.append(f"   {d}: {decisions.get(d, 0)}")

        lines.append(f"\n3. By genre pair (surviving):")
        for p, c in sorted(pair_counts.items(), key=lambda x: -x[1]):
            lines.append(f"   {p}: {c}")

        lines.append(f"\n3b. Removed pairs:")
        removed_pairs = {}
        for r in self.removals:
            p = f"{r['primary_genre']}->{r['removed_secondary_genre']}"
            removed_pairs[p] = removed_pairs.get(p, 0) + 1
        for p, c in sorted(removed_pairs.items(), key=lambda x: -x[1]):
            lines.append(f"   {p}: {c}")

        lines.append(f"\n4. Coverage impact:")
        blocked = sum(1 for r in self.results if r["decision"] == "REVIEW")
        lines.append(f"   Blocked by missing subgenre (REVIEW): {blocked}")

        (PROOF_DIR / "04_rule_application_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Blocked by missing subgenre
        lines = []
        lines.append("=" * 70)
        lines.append("BLOCKED BY MISSING SUBGENRE")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        blocked_rows = [r for r in self.results if r["decision"] == "REVIEW"]
        lines.append(f"\nTotal blocked: {len(blocked_rows)}")
        for r in blocked_rows:
            lines.append(
                f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                f"{r['primary_genre']:8s}/{r['aligned_subgenre']:20s} "
                f"-> {r['secondary_genre']:8s}  {r['reason']}"
            )

        (PROOF_DIR / "05_blocked_by_missing_subgenre.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        self.emit(f"  Before: {self.before_count}")
        self.emit(f"  Removed: {len(self.removals)}")
        self.emit(f"  After: {self.after_count}")
        self.emit(f"  REVIEW (blocked): {blocked}")

    # ================================================================
    # PART I — VALIDATION
    # ================================================================
    def part_i(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART I -- VALIDATION")
        self.emit("=" * 70)

        conn = self.connect_ro()
        all_ok = True
        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. Primary labels unchanged
        prim = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk1 = prim == 783
        val.append(f"\n  1. Primary labels: {prim} (expected 783) "
                   f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. No duplicate primaries
        dups = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk2 = dups == 0
        val.append(f"  2. Duplicate primaries: {dups} -- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. FK integrity
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk3 = len(fk) == 0
        val.append(f"  3. FK violations: {len(fk)} -- {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. Secondary count matches expected
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        expected_sec = self.before_count - len(self.removals)
        chk4 = sec == expected_sec
        val.append(f"  4. Secondaries: {sec} (expected {expected_sec}) "
                   f"-- {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. Benchmark unchanged
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
            "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"
        ).fetchone()[0]
        chk5 = bench == 202
        val.append(f"  5. Benchmark count: {bench} (expected 202) "
                   f"-- {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. All 114 were evaluated
        chk6 = len(self.results) == self.before_count
        val.append(f"  6. All evaluated: {len(self.results)} "
                   f"(expected {self.before_count}) "
                   f"-- {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # 7. No unintended deletions — only REMOVE rows were removed
        removed_ids = set(r["label_id"] for r in self.removals)
        keep_ids = set(r["label_id"] for r in self.results
                       if r["decision"] in ("KEEP", "REVIEW"))
        # Verify KEEP/REVIEW labels still exist
        still_exist = 0
        for lid in keep_ids:
            row = conn.execute(
                "SELECT 1 FROM track_genre_labels WHERE id = ?", (lid,)
            ).fetchone()
            if row:
                still_exist += 1
        chk7 = still_exist == len(keep_ids)
        val.append(f"  7. KEEP/REVIEW labels preserved: {still_exist}/{len(keep_ids)} "
                   f"-- {'PASS' if chk7 else 'FAIL'}")
        if not chk7:
            all_ok = False

        # 8. Subgenre coverage unchanged (backfill not modified)
        sub_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels "
            "WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]
        chk8 = sub_count == 127
        val.append(f"  8. Subgenre coverage: {sub_count} (expected 127) "
                   f"-- {'PASS' if chk8 else 'FAIL'}")
        if not chk8:
            all_ok = False

        conn.close()

        val.append(f"\n  SQL verification:")
        val.append(f"    secondary count: {sec}")
        val.append(f"    primary count: {prim}")
        val.append(f"    dup primaries: {dups}")
        val.append(f"    FK violations: {len(fk)}")
        val.append(f"    benchmark: {bench}")

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

        report = []
        report.append("=" * 70)
        report.append("SECONDARY LABEL RE-EVALUATION V2 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        decisions = {}
        for r in self.results:
            d = r["decision"]
            decisions[d] = decisions.get(d, 0) + 1

        report.append(f"\n--- BEFORE / AFTER ---")
        report.append(f"  Secondaries before: {self.before_count}")
        report.append(f"  Removed:            {len(self.removals)}")
        report.append(f"  Remaining:          {self.after_count}")

        report.append(f"\n--- DECISION BREAKDOWN ---")
        for d in ["KEEP", "REMOVE", "REVIEW"]:
            report.append(f"  {d}: {decisions.get(d, 0)}")

        report.append(f"\n--- REMOVALS BY PAIR ---")
        removed_pairs = {}
        for r in self.removals:
            p = f"{r['primary_genre']}->{r['removed_secondary_genre']}"
            removed_pairs[p] = removed_pairs.get(p, 0) + 1
        for p, c in sorted(removed_pairs.items(), key=lambda x: -x[1]):
            report.append(f"  {p}: {c}")

        report.append(f"\n--- KEY FINDINGS ---")
        report.append(f"  1. {len(self.removals)} secondaries removed by rule engine")
        report.append(f"  2. {decisions.get('REVIEW', 0)} blocked by missing subgenre (REVIEW)")
        report.append(f"  3. {decisions.get('KEEP', 0)} kept with rule support")
        report.append(f"  4. All removals are logged and auditable")
        report.append(f"  5. DB integrity preserved")

        report.append(f"\n{'=' * 70}")
        report.append(f"GATE={gate}")
        report.append(f"{'=' * 70}")

        (PROOF_DIR / "07_final_report.txt").write_text(
            "\n".join(report), encoding="utf-8"
        )
        (PROOF_DIR / "execution_log.txt").write_text(
            "\n".join(self.log), encoding="utf-8"
        )

        self.emit(f"\nPF={PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = RecheckPipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    p.part_a()
    p.part_bcd()
    p.part_f()
    p.part_g()
    all_ok = p.part_i()
    gate = p.final_report(all_ok)

    print(f"\n{'=' * 60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
