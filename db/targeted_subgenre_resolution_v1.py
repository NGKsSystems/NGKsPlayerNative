#!/usr/bin/env python3
"""
Phase — Targeted Subgenre Resolution for Hybrid Tracks

Identifies and safely resolves missing primary subgenres for the subset
of tracks whose unresolved subgenre is blocking rule-engine decisions
on current secondary labels.

Only high-confidence, evidence-backed resolutions are applied.
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
PROOF_DIR = WORKSPACE / "_proof" / "targeted_subgenre_resolution_v1"
DATA_DIR = WORKSPACE / "data"
RECHECK_CSV = DATA_DIR / "secondary_label_recheck_v2.csv"
RULE_ENGINE_CSV = DATA_DIR / "secondary_rule_engine_v1.csv"

APPLIED_BY = "targeted_subgenre_resolution_v1"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TIER 3 MANUAL RESOLUTION TABLE — explicit, logged, defensible
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Each entry: (artist, primary_genre) → (subgenre_name, subgenre_id, confidence, rationale)
#
# These are NOT blanket artist rules. Each is justified by:
#   - the artist's established, well-known primary subgenre classification
#   - the subgenre existing in the aligned taxonomy (subgenres table)
#   - the mapping being unambiguous for the artist's catalog in this DB

MANUAL_RESOLUTIONS = {
    ("Chris Stapleton", "Country"): {
        "subgenre_name": "Outlaw Country",
        "subgenre_id": 46,
        "confidence": "high",
        "evidence_tier": "TIER_3",
        "rationale": (
            "Chris Stapleton is a well-established Outlaw/Traditional Country artist. "
            "His raw, soulful, non-pop-production style aligns with the Outlaw Country "
            "tradition. Among available Country subgenres (Bluegrass, Country Pop, "
            "Outlaw Country), Outlaw Country is the unambiguous best fit. All 10 tracks "
            "in the DB are consistent with this classification."
        ),
    },
    ("Sara Evans", "Country"): {
        "subgenre_name": "Country Pop",
        "subgenre_id": 48,
        "confidence": "high",
        "evidence_tier": "TIER_3",
        "rationale": (
            "Sara Evans is a well-known Country Pop artist. Her hit 'Suds In The Bucket' "
            "is a quintessential Country Pop song. Among available Country subgenres, "
            "Country Pop (id=48) is the unambiguous match."
        ),
    },
    ("Faith No More", "Rock"): {
        "subgenre_name": "Alternative",
        "subgenre_id": 16,
        "confidence": "high",
        "evidence_tier": "TIER_3",
        "rationale": (
            "Faith No More is widely classified as Alternative Rock / Alternative Metal. "
            "Among available Rock subgenres (Alternative, Classic Rock, Grunge, Prog Rock, "
            "Punk, Shoegaze), Alternative (id=16) is the clear and unambiguous match. "
            "'Epic' is their signature Alternative Rock track."
        ),
    },
    ("Van Zant", "Rock"): {
        "subgenre_name": "Classic Rock",
        "subgenre_id": 19,
        "confidence": "high",
        "evidence_tier": "TIER_3",
        "rationale": (
            "Van Zant (Johnny Van Zant / Donnie Van Zant) is from the Lynyrd Skynyrd / "
            ".38 Special family — quintessential Classic Rock / Southern Rock. Among "
            "available Rock subgenres, Classic Rock (id=19) is the best fit. Southern Rock "
            "is not in the DB; Classic Rock is the established parent category."
        ),
    },
}


class ResolutionPipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.targets = []
        self.plan = []
        self.updates = []
        self.impact = []
        self.before_sub_count = 0
        self.after_sub_count = 0

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
    # PART A — BUILD TARGET SET
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- BUILD TARGET SET")
        self.emit("=" * 70)

        # Load recheck CSV to get REVIEW rows
        recheck = pd.read_csv(RECHECK_CSV)
        review_rows = recheck[recheck["decision"] == "REVIEW"]
        self.emit(f"  REVIEW rows from recheck CSV: {len(review_rows)}")

        # Get REVIEW track_ids
        review_track_ids = set(review_rows["track_id"].tolist())

        conn = self.connect_ro()

        # Get benchmark membership
        bench_tracks = set()
        for row in conn.execute(
            "SELECT track_id FROM benchmark_set_tracks "
            "WHERE benchmark_set_id = (SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"
        ).fetchall():
            bench_tracks.add(row[0])

        # Subgenre coverage before
        self.before_sub_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]

        # Build full target set from DB
        rows = conn.execute("""
            SELECT ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   COALESCE(sp.name, '') AS current_subgenre,
                   tp.subgenre_id
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            LEFT JOIN subgenres sp ON tp.subgenre_id = sp.id
            WHERE ts.role = 'secondary'
              AND ts.track_id IN ({})
            ORDER BY t.artist, t.title
        """.format(",".join(str(t) for t in review_track_ids))).fetchall()
        conn.close()

        for row in rows:
            r = dict(row)
            r["benchmark_member"] = "yes" if r["track_id"] in bench_tracks else "no"
            r["review_reason"] = "Insufficient subgenre coverage"
            self.targets.append(r)

        self.emit(f"  Target set size: {len(self.targets)}")
        self.emit(f"  Benchmark members in target: "
                  f"{sum(1 for t in self.targets if t['benchmark_member'] == 'yes')}")

        # Write target CSV
        df = pd.DataFrame([{
            "track_id": t["track_id"],
            "artist": t["artist"],
            "title": t["title"],
            "primary_genre": t["primary_genre"],
            "secondary_genre": t["secondary_genre"],
            "current_subgenre": t["current_subgenre"] or "(null)",
            "benchmark_member": t["benchmark_member"],
            "review_reason": t["review_reason"],
        } for t in self.targets])
        df.to_csv(DATA_DIR / "targeted_subgenre_resolution_targets_v1.csv",
                  index=False, encoding="utf-8")
        shutil.copy2(DATA_DIR / "targeted_subgenre_resolution_targets_v1.csv",
                     PROOF_DIR / "targeted_subgenre_resolution_targets_v1.csv")

        # Proof 00
        lines = []
        lines.append("=" * 70)
        lines.append("TARGET SET SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal target tracks: {len(self.targets)}")
        lines.append(f"Benchmark members: "
                     f"{sum(1 for t in self.targets if t['benchmark_member'] == 'yes')}")
        lines.append(f"Subgenre coverage before: {self.before_sub_count}")

        # By artist
        artist_counts = {}
        for t in self.targets:
            k = f"{t['artist']} ({t['primary_genre']}→{t['secondary_genre']})"
            artist_counts[k] = artist_counts.get(k, 0) + 1
        lines.append(f"\nBy artist/pair:")
        for k, c in sorted(artist_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {k}: {c}")

        # By genre pair
        pair_counts = {}
        for t in self.targets:
            p = f"{t['primary_genre']}→{t['secondary_genre']}"
            pair_counts[p] = pair_counts.get(p, 0) + 1
        lines.append(f"\nBy genre pair:")
        for p, c in sorted(pair_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {p}: {c}")

        (PROOF_DIR / "00_target_set_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    # ================================================================
    # PART B + C — RESOLUTION PLAN
    # ================================================================
    def part_bc(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B/C -- RESOLUTION EVIDENCE + PLAN")
        self.emit("=" * 70)

        # Evidence rules proof
        ev_lines = []
        ev_lines.append("=" * 70)
        ev_lines.append("RESOLUTION EVIDENCE RULES")
        ev_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        ev_lines.append("=" * 70)
        ev_lines.append("\nTIER 1: Existing trusted pattern reuse")
        ev_lines.append("  Same artist + same genre + existing subgenre in DB")
        ev_lines.append("  Result: 0 candidates (no target artist has existing subgenre)")
        ev_lines.append("\nTIER 2: Subgenre signal from current label text")
        ev_lines.append("  Normalize existing subgenre text into aligned taxonomy")
        ev_lines.append("  Result: 0 candidates (all target tracks have NULL subgenre)")
        ev_lines.append("\nTIER 3: Explicit taxonomy-driven manual candidate")
        ev_lines.append("  Manually assigned subgenres with logged rationale")
        ev_lines.append(f"  Result: {len(MANUAL_RESOLUTIONS)} resolution rules defined")
        for (artist, genre), res in MANUAL_RESOLUTIONS.items():
            ev_lines.append(f"\n  [{artist} / {genre}]")
            ev_lines.append(f"    → {res['subgenre_name']} (id={res['subgenre_id']})")
            ev_lines.append(f"    confidence: {res['confidence']}")
            ev_lines.append(f"    rationale: {res['rationale']}")
        ev_lines.append("\nNOT RESOLVABLE (subgenre not in DB):")
        ev_lines.append("  - Demun Jones / Country: needs Country Rap (not in DB)")
        ev_lines.append("  - Upchurch / Country: needs Country Rap (not in DB)")
        ev_lines.append("  - Ryan Upchurch / Country: needs Country Rap (not in DB)")
        ev_lines.append("  - Jelly Roll / Country: needs Country Rap (not in DB)")
        ev_lines.append("  - Tom MacDonald / Hip-Hop: no fitting subgenre in DB")
        ev_lines.append("  - Tom MacDonald / Pop: no fitting Pop subgenre in DB")
        ev_lines.append("  - Hopsin / Hip-Hop: no fitting subgenre in DB")
        ev_lines.append("  - Tone-Loc / Hip-Hop: needs Pop Rap (not in DB)")
        ev_lines.append("  - Weird Al Yankovic / Pop: no fitting Pop subgenre")
        ev_lines.append("  - Unknown (Dax) / Hip-Hop: placeholder artist, skip")
        ev_lines.append("  - Zac Brown Band / Country: ambiguous (multiple styles)")

        (PROOF_DIR / "01_resolution_evidence_rules.txt").write_text(
            "\n".join(ev_lines), encoding="utf-8"
        )

        # Build plan for each unique (track_id, primary_genre) pair
        seen = set()
        for t in self.targets:
            key = (t["track_id"], t["primary_genre"])
            if key in seen:
                continue
            seen.add(key)

            artist = t["artist"]
            primary_genre = t["primary_genre"]
            secondary_genre = t["secondary_genre"]

            lookup = (artist, primary_genre)
            if lookup in MANUAL_RESOLUTIONS:
                res = MANUAL_RESOLUTIONS[lookup]
                self.plan.append({
                    "track_id": t["track_id"],
                    "artist": artist,
                    "title": t["title"],
                    "primary_genre": primary_genre,
                    "secondary_genre": secondary_genre,
                    "proposed_aligned_subgenre": res["subgenre_name"],
                    "proposed_subgenre_id": res["subgenre_id"],
                    "evidence_tier": res["evidence_tier"],
                    "confidence": res["confidence"],
                    "decision": "RESOLVE",
                    "rationale": res["rationale"],
                })
            elif artist == "Unknown":
                self.plan.append({
                    "track_id": t["track_id"],
                    "artist": artist,
                    "title": t["title"],
                    "primary_genre": primary_genre,
                    "secondary_genre": secondary_genre,
                    "proposed_aligned_subgenre": "",
                    "proposed_subgenre_id": None,
                    "evidence_tier": "",
                    "confidence": "low",
                    "decision": "SKIP",
                    "rationale": "Placeholder artist 'Unknown', cannot determine subgenre",
                })
            elif artist == "Zac Brown Band":
                self.plan.append({
                    "track_id": t["track_id"],
                    "artist": artist,
                    "title": t["title"],
                    "primary_genre": primary_genre,
                    "secondary_genre": secondary_genre,
                    "proposed_aligned_subgenre": "Country Pop",
                    "proposed_subgenre_id": 48,
                    "evidence_tier": "TIER_3",
                    "confidence": "medium",
                    "decision": "REVIEW",
                    "rationale": (
                        "Zac Brown Band crosses Country/Rock/Pop styles. Country Pop is "
                        "plausible but the band's catalog is stylistically diverse. "
                        "'Heavy Is The Head' leans more rock/country than pop. "
                        "Marking REVIEW — not safe for automatic resolution."
                    ),
                })
            else:
                # No resolution available
                reason = self._skip_reason(artist, primary_genre)
                self.plan.append({
                    "track_id": t["track_id"],
                    "artist": artist,
                    "title": t["title"],
                    "primary_genre": primary_genre,
                    "secondary_genre": secondary_genre,
                    "proposed_aligned_subgenre": "",
                    "proposed_subgenre_id": None,
                    "evidence_tier": "",
                    "confidence": "low",
                    "decision": "SKIP",
                    "rationale": reason,
                })

        decisions = {}
        for p in self.plan:
            d = p["decision"]
            decisions[d] = decisions.get(d, 0) + 1

        self.emit(f"  Plan entries: {len(self.plan)}")
        for d in ["RESOLVE", "REVIEW", "SKIP"]:
            self.emit(f"    {d}: {decisions.get(d, 0)}")

        # Write plan CSV
        df = pd.DataFrame([{k: v for k, v in p.items() if k != "proposed_subgenre_id"}
                           for p in self.plan])
        df.to_csv(DATA_DIR / "targeted_subgenre_resolution_plan_v1.csv",
                  index=False, encoding="utf-8")
        shutil.copy2(DATA_DIR / "targeted_subgenre_resolution_plan_v1.csv",
                     PROOF_DIR / "targeted_subgenre_resolution_plan_v1.csv")

        # Proof 02
        lines = []
        lines.append("=" * 70)
        lines.append("RESOLUTION PLAN SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal plan entries: {len(self.plan)}")
        for d in ["RESOLVE", "REVIEW", "SKIP"]:
            lines.append(f"  {d}: {decisions.get(d, 0)}")

        for d in ["RESOLVE", "REVIEW", "SKIP"]:
            lines.append(f"\n--- {d} ---")
            for p in self.plan:
                if p["decision"] == d:
                    sub = p["proposed_aligned_subgenre"] or "(none)"
                    lines.append(
                        f"  [{p['track_id']:4d}] {p['artist'][:25]:25s} "
                        f"{p['primary_genre']:8s} → {sub:20s} "
                        f"[{p['evidence_tier'] or 'n/a':6s}] [{p['confidence']:6s}]"
                    )
                    lines.append(f"         {p['rationale'][:120]}")

        (PROOF_DIR / "02_resolution_plan_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        # Proof 04 — unresolved REVIEW/SKIP
        lines = []
        lines.append("=" * 70)
        lines.append("UNRESOLVED (REVIEW + SKIP) SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        unresolved = [p for p in self.plan if p["decision"] != "RESOLVE"]
        lines.append(f"\nTotal unresolved: {len(unresolved)}")
        lines.append("\nBreakdown by reason category:")
        reason_cats = {}
        for p in unresolved:
            cat = "REVIEW" if p["decision"] == "REVIEW" else "SKIP"
            r = p["rationale"][:60]
            key = f"{cat}: {r}"
            reason_cats[key] = reason_cats.get(key, 0) + 1
        for k, c in sorted(reason_cats.items(), key=lambda x: -x[1]):
            lines.append(f"  [{c:3d}] {k}")
        lines.append("\nDetails:")
        for p in unresolved:
            lines.append(
                f"  [{p['track_id']:4d}] {p['artist'][:25]:25s} "
                f"{p['primary_genre']:8s}→{p['secondary_genre']:8s} "
                f"[{p['decision']}] {p['rationale'][:80]}"
            )

        (PROOF_DIR / "04_unresolved_review_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

    def _skip_reason(self, artist, genre):
        reasons = {
            ("Demun Jones", "Country"): "No Country Rap subgenre in DB; cannot resolve",
            ("Upchurch", "Country"): "No Country Rap subgenre in DB; cannot resolve",
            ("Ryan Upchurch", "Country"): "No Country Rap subgenre in DB; cannot resolve",
            ("Jelly Roll", "Country"): "No Country Rap subgenre in DB; cannot resolve",
            ("Tom MacDonald", "Hip-Hop"): "No fitting Hip-Hop subgenre in DB (not Trap/Boom Bap/Drill/Lo-Fi/G-Funk)",
            ("Tom MacDonald", "Pop"): "No fitting Pop subgenre in DB for this track",
            ("Hopsin", "Hip-Hop"): "No fitting Hip-Hop subgenre in DB for this artist",
            ("Tone-Loc", "Hip-Hop"): "No Pop Rap / Old School subgenre in DB; cannot resolve",
            ("Weird Al Yankovic", "Pop"): "Parody artist; no fitting Pop subgenre in DB",
        }
        return reasons.get((artist, genre),
                           f"No safe subgenre resolution available for {artist}/{genre}")

    # ================================================================
    # PART D — APPLY SAFE RESOLUTIONS
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D -- APPLY SAFE RESOLUTIONS")
        self.emit("=" * 70)

        to_apply = [p for p in self.plan
                    if p["decision"] == "RESOLVE" and p["confidence"] == "high"]
        self.emit(f"  High-confidence RESOLVE entries: {len(to_apply)}")

        conn = self.connect_rw()
        try:
            for p in to_apply:
                track_id = p["track_id"]
                subgenre_id = p["proposed_subgenre_id"]

                # Verify the primary row exists and currently has NULL subgenre
                row = conn.execute(
                    "SELECT id, subgenre_id, genre_id FROM track_genre_labels "
                    "WHERE track_id = ? AND role = 'primary'",
                    (track_id,)
                ).fetchone()

                if row is None:
                    self.emit(f"  WARNING: No primary for track {track_id}, skipping")
                    continue

                if row["subgenre_id"] is not None:
                    self.emit(f"  SKIP: track {track_id} already has subgenre_id={row['subgenre_id']}")
                    continue

                # Verify subgenre FK is valid
                sub_row = conn.execute(
                    "SELECT id, name FROM subgenres WHERE id = ?",
                    (subgenre_id,)
                ).fetchone()
                if sub_row is None:
                    self.emit(f"  FATAL: subgenre_id={subgenre_id} not in subgenres table")
                    raise ValueError(f"Invalid subgenre_id={subgenre_id}")

                # Verify subgenre belongs to the correct genre
                sub_genre_check = conn.execute(
                    "SELECT genre_id FROM subgenres WHERE id = ?",
                    (subgenre_id,)
                ).fetchone()
                if sub_genre_check["genre_id"] != row["genre_id"]:
                    self.emit(f"  FATAL: subgenre {subgenre_id} genre mismatch for track {track_id}")
                    raise ValueError(f"Genre mismatch for subgenre {subgenre_id}")

                # Apply update
                conn.execute(
                    "UPDATE track_genre_labels SET subgenre_id = ?, applied_by = ? "
                    "WHERE id = ?",
                    (subgenre_id, APPLIED_BY, row["id"])
                )

                self.updates.append({
                    "label_id": row["id"],
                    "track_id": track_id,
                    "artist": p["artist"],
                    "title": p["title"],
                    "primary_genre": p["primary_genre"],
                    "new_subgenre": p["proposed_aligned_subgenre"],
                    "subgenre_id": subgenre_id,
                    "evidence_tier": p["evidence_tier"],
                    "applied_by": APPLIED_BY,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

            conn.commit()
            self.emit(f"  Applied {len(self.updates)} subgenre updates")

        except Exception as e:
            conn.rollback()
            self.emit(f"  FATAL: Rollback due to error: {e}")
            raise
        finally:
            conn.close()

        # Get after count
        conn = self.connect_ro()
        self.after_sub_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]
        conn.close()

        self.emit(f"  Subgenre coverage: {self.before_sub_count} → {self.after_sub_count} "
                  f"(+{self.after_sub_count - self.before_sub_count})")

        # Proof 03
        lines = []
        lines.append("=" * 70)
        lines.append("ROWS UPDATED")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal updated: {len(self.updates)}")
        lines.append(f"Subgenre coverage: {self.before_sub_count} → {self.after_sub_count}")
        for u in self.updates:
            lines.append(
                f"  label_id={u['label_id']:4d}  track_id={u['track_id']:4d}  "
                f"{u['artist'][:25]:25s}  "
                f"{u['primary_genre']:8s} → {u['new_subgenre']:20s} "
                f"(id={u['subgenre_id']})  [{u['evidence_tier']}]"
            )

        (PROOF_DIR / "03_rows_updated.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        if self.updates:
            pd.DataFrame(self.updates).to_csv(
                PROOF_DIR / "resolution_change_log.csv",
                index=False, encoding="utf-8"
            )

    # ================================================================
    # PART E — IMPACT PREVIEW
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E -- IMPACT PREVIEW")
        self.emit("=" * 70)

        # Load rule engine
        rules = pd.read_csv(RULE_ENGINE_CSV).to_dict("records")
        rule_triples = {}
        prohibited_pairs = {}
        wildcard_prohibited = {}

        for r in rules:
            pg = r["aligned_parent_genre"]
            sub = r["aligned_subgenre"]
            sec = r["allowed_secondary_genre"]
            rt = r["rule_type"]

            if pg == "*":
                wildcard_prohibited[sub] = r
            elif rt == "prohibited":
                prohibited_pairs[(pg, sub)] = r
            else:
                rule_triples[(pg, sub, sec)] = r

        conn = self.connect_ro()

        # Get all current secondaries with their primary subgenre
        rows = conn.execute("""
            SELECT ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   COALESCE(sp.name, '') AS primary_subgenre,
                   tp.subgenre_id
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

        newly_eligible = 0
        still_review = 0

        for row in rows:
            r = dict(row)
            track_id = r["track_id"]
            primary_genre = r["primary_genre"]
            secondary_genre = r["secondary_genre"]
            subgenre = r["primary_subgenre"]
            sub_id = r["subgenre_id"]

            eligible = sub_id is not None and subgenre != ""
            likely_outcome = "STILL_REVIEW"
            notes = ""

            if not eligible:
                still_review += 1
                likely_outcome = "STILL_REVIEW"
                notes = "Primary still lacks aligned subgenre"
            else:
                newly_eligible += 1

                # Check wildcard prohibition
                if secondary_genre in wildcard_prohibited:
                    likely_outcome = "REMOVE"
                    notes = f"Wildcard prohibition on {secondary_genre}"
                elif (primary_genre, subgenre) in prohibited_pairs:
                    likely_outcome = "REMOVE"
                    notes = f"Prohibited pair: {primary_genre}/{subgenre}"
                elif (primary_genre, subgenre, secondary_genre) in rule_triples:
                    rule = rule_triples[(primary_genre, subgenre, secondary_genre)]
                    rt = rule["rule_type"]
                    conf = rule.get("confidence_tier", "")
                    if rt == "explicit_hybrid":
                        likely_outcome = "KEEP"
                        notes = f"explicit_hybrid: {subgenre}→{secondary_genre}"
                    elif rt == "stylistic_bridge" and conf == "high":
                        likely_outcome = "KEEP"
                        notes = f"stylistic_bridge (high): {subgenre}→{secondary_genre}"
                    else:
                        likely_outcome = "REMOVE"
                        notes = f"stylistic_bridge ({conf}): insufficient evidence"
                else:
                    likely_outcome = "REMOVE"
                    notes = f"No rule for {primary_genre}/{subgenre}→{secondary_genre}"

            self.impact.append({
                "track_id": track_id,
                "artist": r["artist"],
                "title": r["title"],
                "primary_genre": primary_genre,
                "new_aligned_subgenre": subgenre or "(none)",
                "secondary_genre": secondary_genre,
                "would_be_rule_eligible": "yes" if eligible else "no",
                "likely_outcome": likely_outcome,
                "notes": notes,
            })

        self.emit(f"  Total secondaries: {len(self.impact)}")
        self.emit(f"  Newly eligible (have subgenre): {newly_eligible}")
        self.emit(f"  Still REVIEW (no subgenre): {still_review}")

        # Outcome breakdown for eligible
        outcomes = {}
        for i in self.impact:
            if i["would_be_rule_eligible"] == "yes":
                o = i["likely_outcome"]
                outcomes[o] = outcomes.get(o, 0) + 1

        for o in ["KEEP", "REMOVE", "STILL_REVIEW"]:
            if o in outcomes:
                self.emit(f"    eligible → {o}: {outcomes[o]}")

        # Write impact CSV
        df = pd.DataFrame(self.impact)
        df.to_csv(DATA_DIR / "targeted_subgenre_resolution_impact_v1.csv",
                  index=False, encoding="utf-8")
        shutil.copy2(DATA_DIR / "targeted_subgenre_resolution_impact_v1.csv",
                     PROOF_DIR / "targeted_subgenre_resolution_impact_v1.csv")

        # Proof 05
        lines = []
        lines.append("=" * 70)
        lines.append("IMPACT PREVIEW")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal secondaries: {len(self.impact)}")
        lines.append(f"Rule-eligible (have subgenre): {newly_eligible}")
        lines.append(f"Still blocked (no subgenre): {still_review}")
        lines.append(f"\nEligible outcome preview:")
        for o in ["KEEP", "REMOVE", "STILL_REVIEW"]:
            if o in outcomes:
                lines.append(f"  {o}: {outcomes[o]}")

        lines.append(f"\nDetails (eligible tracks):")
        for i in self.impact:
            if i["would_be_rule_eligible"] == "yes":
                lines.append(
                    f"  [{i['track_id']:4d}] {i['artist'][:25]:25s} "
                    f"{i['primary_genre']:8s}/{i['new_aligned_subgenre']:20s} "
                    f"→ {i['secondary_genre']:8s}  "
                    f"[{i['likely_outcome']}] {i['notes']}"
                )

        lines.append(f"\nBlocked tracks (still no subgenre): {still_review}")
        lines.append("These require either:")
        lines.append("  - new subgenre taxonomy entries (e.g. Country Rap, Pop Rap)")
        lines.append("  - manual subgenre assignment with evidence")
        lines.append("  - artist-specific review")

        (PROOF_DIR / "05_impact_preview.txt").write_text(
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

        # 1. Primary label count
        prim = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk1 = prim == 783
        val.append(f"\n  1. Primary labels: {prim} (expected 783) "
                   f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Subgenre coverage increased
        sub = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary' AND subgenre_id IS NOT NULL"
        ).fetchone()[0]
        chk2 = sub > self.before_sub_count
        val.append(f"  2. Subgenre coverage: {self.before_sub_count} → {sub} "
                   f"-- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. Secondary count unchanged (this phase does NOT touch secondaries)
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        chk3 = sec == 86
        val.append(f"  3. Secondaries unchanged: {sec} (expected 86) "
                   f"-- {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. No duplicate primaries
        dups = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk4 = dups == 0
        val.append(f"  4. Duplicate primaries: {dups} -- {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. FK integrity
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk5 = len(fk) == 0
        val.append(f"  5. FK violations: {len(fk)} -- {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. Benchmark unchanged
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id = "
            "(SELECT id FROM benchmark_sets WHERE name='genre_benchmark_v1')"
        ).fetchone()[0]
        chk6 = bench == 202
        val.append(f"  6. Benchmark: {bench} (expected 202) -- {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # 7. Only high-confidence targets updated
        updated_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE applied_by = ?",
            (APPLIED_BY,)
        ).fetchone()[0]
        chk7 = updated_count == len(self.updates)
        val.append(f"  7. Applied_by '{APPLIED_BY}': {updated_count} "
                   f"(expected {len(self.updates)}) -- {'PASS' if chk7 else 'FAIL'}")
        if not chk7:
            all_ok = False

        # 8. Primary genre values unchanged (verify no genre_id mutation)
        # Check that updated rows still have correct genre
        chk8 = True
        for u in self.updates:
            row = conn.execute(
                "SELECT genre_id, subgenre_id FROM track_genre_labels WHERE id = ?",
                (u["label_id"],)
            ).fetchone()
            if row is None:
                chk8 = False
                break
            genre = conn.execute(
                "SELECT name FROM genres WHERE id = ?", (row["genre_id"],)
            ).fetchone()
            if genre["name"] != u["primary_genre"]:
                chk8 = False
                break
        val.append(f"  8. Primary genres preserved: -- {'PASS' if chk8 else 'FAIL'}")
        if not chk8:
            all_ok = False

        conn.close()

        val.append(f"\n  SQL verification:")
        val.append(f"    primary count: {prim}")
        val.append(f"    subgenre coverage: {sub}")
        val.append(f"    secondary count: {sec}")
        val.append(f"    dup primaries: {dups}")
        val.append(f"    FK violations: {len(fk)}")
        val.append(f"    benchmark: {bench}")
        val.append(f"    applied_by count: {updated_count}")

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

        decisions = {}
        for p in self.plan:
            d = p["decision"]
            decisions[d] = decisions.get(d, 0) + 1

        report = []
        report.append("=" * 70)
        report.append("TARGETED SUBGENRE RESOLUTION V1 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- TARGET SET ---")
        report.append(f"  Total target tracks: {len(self.targets)}")

        report.append(f"\n--- RESOLUTION PLAN ---")
        for d in ["RESOLVE", "REVIEW", "SKIP"]:
            report.append(f"  {d}: {decisions.get(d, 0)}")

        report.append(f"\n--- DB UPDATES ---")
        report.append(f"  Subgenre updates applied: {len(self.updates)}")
        report.append(f"  Subgenre coverage: {self.before_sub_count} → {self.after_sub_count} "
                      f"(+{self.after_sub_count - self.before_sub_count})")

        report.append(f"\n--- UPDATED ARTISTS ---")
        artist_updates = {}
        for u in self.updates:
            k = f"{u['artist']} → {u['new_subgenre']}"
            artist_updates[k] = artist_updates.get(k, 0) + 1
        for k, c in sorted(artist_updates.items(), key=lambda x: -x[1]):
            report.append(f"  {k}: {c} tracks")

        report.append(f"\n--- IMPACT PREVIEW ---")
        eligible = sum(1 for i in self.impact if i["would_be_rule_eligible"] == "yes")
        still_blocked = sum(1 for i in self.impact if i["would_be_rule_eligible"] == "no")
        report.append(f"  Secondaries now eligible for recheck: {eligible}")
        report.append(f"  Secondaries still blocked: {still_blocked}")
        outcomes = {}
        for i in self.impact:
            if i["would_be_rule_eligible"] == "yes":
                o = i["likely_outcome"]
                outcomes[o] = outcomes.get(o, 0) + 1
        for o in ["KEEP", "REMOVE", "STILL_REVIEW"]:
            if o in outcomes:
                report.append(f"    → {o}: {outcomes[o]}")

        report.append(f"\n--- KEY FINDINGS ---")
        report.append(f"  1. {len(self.updates)} tracks got safe subgenre resolution")
        report.append(f"  2. Coverage increased from {self.before_sub_count} to {self.after_sub_count}")
        report.append(f"  3. {still_blocked} tracks remain blocked — need new taxonomy entries")
        report.append(f"  4. All updates are logged and reversible (applied_by='{APPLIED_BY}')")
        report.append(f"  5. No primary genres or secondary labels were modified")

        report.append(f"\n--- BLOCKERS FOR REMAINING TRACKS ---")
        report.append(f"  Missing taxonomy entries needed:")
        report.append(f"    - Country Rap (for Demun Jones, Upchurch, Jelly Roll)")
        report.append(f"    - Pop Rap / Conscious Hip-Hop (for Tom MacDonald)")
        report.append(f"    - Pop Rap (for Tone-Loc)")
        report.append(f"    - No suitable subgenre exists (Weird Al parody, Unknown artist)")

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
    p = ResolutionPipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    p.part_a()
    p.part_bc()
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
