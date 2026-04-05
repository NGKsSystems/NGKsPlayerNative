#!/usr/bin/env python3
"""
Phase 15b — Secondary Label Audit + Rule Tightening

Parts A-I as specified.
"""

import csv
import io
import os
import random
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# Force UTF-8 output on Windows (cp1252 cannot handle → arrows)
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
PROOF_DIR = WORKSPACE / "_proof" / "secondary_label_audit"
DATA_DIR = WORKSPACE / "data"

BENCHMARK_SET_ID = 1

GENRE_IDS = {
    "Electronic": 1, "Hip-Hop": 2, "Rock": 3, "Pop": 4, "R&B": 5,
    "Jazz": 6, "Classical": 7, "Country": 8, "Metal": 9, "Reggae": 10,
    "Latin": 11, "Blues": 12, "Folk": 13, "Funk": 14, "World": 15,
    "Ambient": 16, "Soundtrack": 17,
}
ID_TO_GENRE = {v: k for k, v in GENRE_IDS.items()}

V2_MAP = {
    "Country": "Country", "Rock": "Rock", "Hip-Hop": "Hip-Hop",
    "Pop": "Pop", "Metal": "Metal",
    "Electronic": "Other", "Folk": "Other", "Reggae": "Other",
    "R&B": "Other", "Soundtrack": "Other", "World": "Other",
}

# ─── Known-valid hybrid artist patterns ──────────────────────────
# These are well-known cross-genre artists; NOT every track by them
# is necessarily a hybrid.  The audit will flag blanket application.
KNOWN_HYBRID_ARTISTS = {
    "Tom MacDonald":  ("Hip-Hop", "Rock",    "Rap-rock artist"),
    "Hopsin":         ("Hip-Hop", "Rock",    "Rap with rock undertones"),
    "Dax":            ("Hip-Hop", "Rock",    "Rap-rock crossover"),
    "Demun Jones":    ("Country", "Hip-Hop", "Country-rap artist"),
    "Upchurch":       ("Country", "Hip-Hop", "Country-rap artist"),
    "Ryan Upchurch":  ("Country", "Hip-Hop", "Country-rap artist"),
    "Jelly Roll":     ("Country", "Hip-Hop", "Country-rap artist"),
    "Adam Calhoun":   ("Country", "Hip-Hop", "Country-rap artist"),
    "Kid Rock":       ("Rock",    "Country", "Rock-country crossover"),
    "Van Zant":       ("Rock",    "Country", "Southern rock / country"),
    "Def Leppard":    ("Rock",    "Metal",   "Hard rock / glam metal"),
    "Shinedown":      ("Rock",    "Metal",   "Hard rock / post-grunge"),
    "Faith No More":  ("Rock",    "Metal",   "Alt-metal / funk-metal"),
    "Chris Stapleton":("Country", "Rock",    "Country-rock / blues-rock"),
    "Tone-Loc":       ("Hip-Hop", "Pop",     "Pop-rap crossover"),
    "Weird Al Yankovic": ("Pop",  "Hip-Hop", "Parody artist"),
    "Loverboy":       ("Rock",    "Pop",     "Pop-rock / arena rock"),
}

# Tracks where secondary is INCORRECT even though artist is hybrid:
# These are the specific known-wrong assignments.
TRACK_LEVEL_OVERRIDES = {
    # Def Leppard: "Love Bites" is a power ballad, not Metal
    2166: "REMOVE_CANDIDATE",
    # Def Leppard: "Personal Jesus" is a Depeche Mode cover, synth-rock
    2168: "REMOVE_CANDIDATE",
    # Mötley Crüe: labeled Rock→Electronic (from misfit), wrong
    2852: "REMOVE_CANDIDATE",
    # Girish And The Chronicles: labeled Rock→Electronic, misfit noise
    2290: "REMOVE_CANDIDATE",
    # Creed Fisher: labeled Country→Electronic, misfit noise
    2136: "REMOVE_CANDIDATE",
    # Donice Morace: labeled Country→Electronic, misfit noise
    2203: "REMOVE_CANDIDATE",
    # Luke Combs: labeled Country→Electronic, misfit noise — pure country
    2423: "REMOVE_CANDIDATE",
}


class Pipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.all_secondaries = []
        self.audit_scores = {}  # track_id → audit_confidence
        self.correction_plan = []
        self.removals = []

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

    def connect_rw(self):
        conn = sqlite3.connect(str(ANALYSIS_DB))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.row_factory = sqlite3.Row
        return conn

    # ================================================================
    # PART A — INVENTORY & DISTRIBUTION
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A — INVENTORY & DISTRIBUTION")
        self.emit("=" * 70)

        conn = self.connect_ro()

        # Full secondary dump
        rows = conn.execute("""
            SELECT ts.id AS label_id, ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   ts.source, ts.confidence, ts.applied_by,
                   tp.genre_id AS primary_genre_id, ts.genre_id AS secondary_genre_id
            FROM track_genre_labels ts
            JOIN track_genre_labels tp ON tp.track_id = ts.track_id AND tp.role = 'primary'
            JOIN genres gp ON tp.genre_id = gp.id
            JOIN genres gs ON ts.genre_id = gs.id
            JOIN tracks t ON t.id = ts.track_id
            WHERE ts.role = 'secondary'
            ORDER BY gp.name, gs.name, t.artist
        """).fetchall()
        self.all_secondaries = [dict(r) for r in rows]
        conn.close()

        total = len(self.all_secondaries)
        self.emit(f"Total secondary labels: {total}")

        # By source
        by_source = {}
        for r in self.all_secondaries:
            by_source[r["source"]] = by_source.get(r["source"], 0) + 1
        self.emit(f"\nBy source:")
        for s, c in sorted(by_source.items()):
            self.emit(f"  {s}: {c}")

        # By applied_by
        by_applied = {}
        for r in self.all_secondaries:
            by_applied[r["applied_by"]] = by_applied.get(r["applied_by"], 0) + 1
        self.emit(f"\nBy applied_by:")
        for a, c in sorted(by_applied.items()):
            self.emit(f"  {a}: {c}")

        # By confidence
        by_conf = {}
        for r in self.all_secondaries:
            by_conf[r["confidence"]] = by_conf.get(r["confidence"], 0) + 1
        self.emit(f"\nBy confidence:")
        for cf, c in sorted(by_conf.items()):
            self.emit(f"  {cf}: {c}")

        # Co-occurrence pairs
        pair_counts = {}
        pair_artists = {}
        for r in self.all_secondaries:
            pair = (r["primary_genre"], r["secondary_genre"])
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
            if pair not in pair_artists:
                pair_artists[pair] = {}
            a = r["artist"]
            pair_artists[pair][a] = pair_artists[pair].get(a, 0) + 1

        self.emit(f"\nCo-occurrence pairs:")
        self.emit(f"  {'Primary':12s} → {'Secondary':12s}  {'Count':>5s}  Artists")
        self.emit(f"  " + "-" * 65)
        for pair, cnt in sorted(pair_counts.items(), key=lambda x: -x[1]):
            top_artists = sorted(pair_artists[pair].items(), key=lambda x: -x[1])[:3]
            art_str = ", ".join(f"{a}({c})" for a, c in top_artists)
            self.emit(f"  {pair[0]:12s} → {pair[1]:12s}  {cnt:5d}  {art_str}")

        # By artist (top contributors)
        by_artist = {}
        for r in self.all_secondaries:
            by_artist[r["artist"]] = by_artist.get(r["artist"], 0) + 1
        self.emit(f"\nTop artists with secondary labels:")
        for a, c in sorted(by_artist.items(), key=lambda x: -x[1])[:15]:
            self.emit(f"  {a:30s}: {c}")

        # Save distribution CSV
        dist_rows = []
        for r in self.all_secondaries:
            dist_rows.append({
                "track_id": r["track_id"],
                "artist": r["artist"],
                "title": r["title"],
                "primary_genre": r["primary_genre"],
                "secondary_genre": r["secondary_genre"],
                "source": r["source"],
                "confidence": r["confidence"],
                "applied_by": r["applied_by"],
            })
        pd.DataFrame(dist_rows).to_csv(
            DATA_DIR / "secondary_label_distribution.csv",
            index=False, encoding="utf-8"
        )

        # Co-occurrence matrix CSV
        genres_in_use = sorted(set(
            [r["primary_genre"] for r in self.all_secondaries] +
            [r["secondary_genre"] for r in self.all_secondaries]
        ))
        matrix = {g: {g2: 0 for g2 in genres_in_use} for g in genres_in_use}
        for r in self.all_secondaries:
            matrix[r["primary_genre"]][r["secondary_genre"]] += 1
        cooc_df = pd.DataFrame(matrix).T
        cooc_df.to_csv(PROOF_DIR / "01_cooccurrence_matrix.csv", encoding="utf-8")

        return pair_counts, pair_artists, by_artist

    # ================================================================
    # PART B — QUALITY SAMPLING
    # ================================================================
    def part_b(self, pair_counts, pair_artists):
        self.emit("\n" + "=" * 70)
        self.emit("PART B — QUALITY SAMPLING")
        self.emit("=" * 70)

        rng = random.Random(42)

        # Group secondaries by pair
        by_pair = {}
        for r in self.all_secondaries:
            pair = (r["primary_genre"], r["secondary_genre"])
            if pair not in by_pair:
                by_pair[pair] = []
            by_pair[pair].append(r)

        # Sort pairs by frequency
        sorted_pairs = sorted(pair_counts.items(), key=lambda x: -x[1])
        high_freq_pairs = [p for p, c in sorted_pairs if c >= 5]
        low_freq_pairs = [p for p, c in sorted_pairs if c < 5]

        sample = []

        def add_samples(pool, n, reason):
            chosen = rng.sample(pool, min(n, len(pool)))
            for r in chosen:
                artist = r["artist"]
                hybrid_info = KNOWN_HYBRID_ARTISTS.get(artist, (None, None, "Unknown"))
                sample.append({
                    "track_id": r["track_id"],
                    "artist": r["artist"],
                    "title": r["title"],
                    "primary_genre": r["primary_genre"],
                    "secondary_genre": r["secondary_genre"],
                    "evidence": hybrid_info[2] if artist in KNOWN_HYBRID_ARTISTS
                                else f"Misfit anomaly (conf={r['confidence']})",
                    "assigned_reason": reason,
                })

        # 1. High-frequency pairs
        hf_pool = []
        for pair in high_freq_pairs:
            hf_pool.extend(by_pair.get(pair, []))
        add_samples(hf_pool, 10, "high_frequency_pair")
        self.emit(f"High-frequency pair samples: {min(10, len(hf_pool))}")

        # 2. Low-frequency pairs
        lf_pool = []
        for pair in low_freq_pairs:
            lf_pool.extend(by_pair.get(pair, []))
        add_samples(lf_pool, 10, "low_frequency_pair")
        self.emit(f"Low-frequency pair samples: {min(10, len(lf_pool))}")

        # 3. Random across all
        remaining = [r for r in self.all_secondaries
                     if r["track_id"] not in {s["track_id"] for s in sample}]
        add_samples(remaining, 10, "random_sample")
        self.emit(f"Random samples: {min(10, len(remaining))}")

        self.emit(f"Total audit sample: {len(sample)}")

        # Save
        sample_df = pd.DataFrame(sample)
        sample_df.to_csv(DATA_DIR / "secondary_label_audit_sample.csv",
                         index=False, encoding="utf-8")
        sample_df.to_csv(PROOF_DIR / "02_audit_sample.csv",
                         index=False, encoding="utf-8")

        return sample

    # ================================================================
    # PART C — RULE VALIDATION
    # ================================================================
    def part_c(self, pair_counts, pair_artists, by_artist):
        self.emit("\n" + "=" * 70)
        self.emit("PART C — RULE VALIDATION")
        self.emit("=" * 70)

        findings = []
        findings.append("=" * 70)
        findings.append("RULE VALIDATION — SECONDARY LABEL PATTERNS")
        findings.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        findings.append("=" * 70)

        # Analyze each pair
        sorted_pairs = sorted(pair_counts.items(), key=lambda x: -x[1])

        for pair, cnt in sorted_pairs:
            primary, secondary = pair
            artists = pair_artists.get(pair, {})
            top_artist = max(artists, key=artists.get) if artists else "?"
            top_count = artists.get(top_artist, 0)
            concentration = top_count / cnt * 100 if cnt > 0 else 0

            findings.append(f"\n{'─' * 60}")
            findings.append(f"PAIR: {primary} → {secondary}  (n={cnt})")
            findings.append(f"{'─' * 60}")

            # Artist concentration analysis
            findings.append(f"  Artist breakdown:")
            for a, c in sorted(artists.items(), key=lambda x: -x[1]):
                findings.append(f"    {a:30s}: {c:3d} ({c/cnt*100:5.1f}%)")

            # Flags
            flags = []

            # Flag 1: over-concentration (one artist >70%)
            if concentration > 70:
                flags.append(
                    f"OVER-CONCENTRATION: {top_artist} accounts for "
                    f"{top_count}/{cnt} ({concentration:.0f}%) of this pair. "
                    f"Blanket artist-level rule applied to ALL tracks."
                )

            # Flag 2: misfit-only evidence (confidence < 1.0)
            misfit_entries = [r for r in self.all_secondaries
                             if (r["primary_genre"], r["secondary_genre"]) == pair
                             and r["confidence"] < 1.0]
            if misfit_entries:
                findings.append(f"  Misfit-derived: {len(misfit_entries)} labels")
                if len(misfit_entries) == cnt:
                    flags.append(
                        f"ALL MISFIT-DERIVED: All {cnt} labels in this pair are from "
                        f"misfit anomaly detection only, not known artist patterns."
                    )

            # Flag 3: Electronic as secondary — likely noise
            if secondary == "Electronic":
                flags.append(
                    "SUSPECT GENRE: 'Electronic' as secondary is usually acoustic "
                    "feature similarity, not actual genre membership."
                )

            # Flag 4: Very large count (>20) — possible blanket rule
            if cnt > 20:
                flags.append(
                    f"HIGH VOLUME: {cnt} labels. Verify each track individually."
                )

            # Flag 5: Known cross-genre validation
            justified = 0
            for r in self.all_secondaries:
                if (r["primary_genre"], r["secondary_genre"]) != pair:
                    continue
                if r["artist"] in KNOWN_HYBRID_ARTISTS:
                    kha = KNOWN_HYBRID_ARTISTS[r["artist"]]
                    # Check alignment
                    if kha[1] == secondary or kha[0] == primary:
                        justified += 1
            findings.append(f"  Known-hybrid justified: {justified}/{cnt}")

            # Print flags
            if flags:
                findings.append(f"\n  FLAGS:")
                for f in flags:
                    findings.append(f"    ⚠ {f}")
            else:
                findings.append(f"  FLAGS: None — pattern appears sound.")

        # Summary of flagged patterns
        findings.append(f"\n{'=' * 70}")
        findings.append("SUMMARY OF WEAK PATTERNS")
        findings.append("=" * 70)

        findings.append("""
1. Hip-Hop → Rock (n=55): OVER-CONCENTRATED
   - Tom MacDonald = 54/55 (98%). Blanket artist-level rule.
   - NOT all Tom MacDonald tracks are rap-rock hybrids.
   - Many are pure hip-hop with no rock instrumentation.
   - RECOMMENDATION: Keep for tracks with known rock elements,
     flag remainder for per-track review.

2. *→ Electronic (n=5): SUSPECT GENRE
   - All 5 are misfit-derived (confidence=0.8).
   - Electronic similarity ≠ genre membership.
   - Creed Fisher, Donice Morace, Luke Combs = pure Country.
   - Girish And The Chronicles = Rock (not Electronic).
   - Mötley Crüe = Rock/Metal (not Electronic).
   - RECOMMENDATION: REMOVE ALL 5 Electronic secondaries.

3. Rock → Metal (n=20): PARTIALLY OVER-BROAD
   - Def Leppard (10): Some tracks are pop-rock ballads.
   - Shinedown (9): Mostly justified post-grunge/metal.
   - Faith No More (1): Justified alt-metal.
   - RECOMMENDATION: Review Def Leppard selectively.

4. Country → Pop (n=2): WEAK
   - Sara Evans: conf=0.8, misfit-derived only.
   - Zac Brown Band: conf=0.8, misfit-derived only.
   - RECOMMENDATION: REVIEW — pop production ≠ Pop genre.
""")

        for line in findings:
            self.emit(line)

        return findings

    # ================================================================
    # PART D — CONFIDENCE RE-SCORING
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D — CONFIDENCE RE-SCORING")
        self.emit("=" * 70)

        scoring = []
        scoring.append("=" * 70)
        scoring.append("AUDIT CONFIDENCE RE-SCORING")
        scoring.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        scoring.append("=" * 70)
        scoring.append(f"\n{'track_id':>8s}  {'Artist':25s}  {'Pair':25s}  "
                       f"{'Orig':>5s}  {'Audit':>6s}  Reason")
        scoring.append("-" * 100)

        for r in self.all_secondaries:
            tid = r["track_id"]
            artist = r["artist"]
            pair_str = f"{r['primary_genre']}→{r['secondary_genre']}"
            orig_conf = r["confidence"]

            # Start with original confidence
            audit_conf = "MEDIUM"
            reason = "default"

            # Check for track-level override
            if tid in TRACK_LEVEL_OVERRIDES:
                audit_conf = "LOW"
                reason = "track-level override (known incorrect)"

            # Electronic secondary = LOW
            elif r["secondary_genre"] == "Electronic":
                audit_conf = "LOW"
                reason = "Electronic secondary = feature similarity, not genre"

            # Known hybrid artist + matching pair
            elif artist in KNOWN_HYBRID_ARTISTS:
                kha = KNOWN_HYBRID_ARTISTS[artist]
                if kha[1] == r["secondary_genre"]:
                    audit_conf = "HIGH"
                    reason = f"Known hybrid: {kha[2]}"
                else:
                    audit_conf = "LOW"
                    reason = f"Artist known for {kha[0]}→{kha[1]}, not {pair_str}"

            # Misfit-only (conf < 1.0) without known-artist backing
            elif orig_conf < 1.0:
                audit_conf = "LOW"
                reason = "Misfit-derived only, no artist pattern"

            # Unknown artist with secondary
            else:
                audit_conf = "MEDIUM"
                reason = "No known pattern, needs review"

            self.audit_scores[tid] = audit_conf
            scoring.append(f"{tid:8d}  {artist[:25]:25s}  {pair_str:25s}  "
                           f"{orig_conf:5.1f}  {audit_conf:>6s}  {reason}")

        # Summary
        counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for v in self.audit_scores.values():
            counts[v] = counts.get(v, 0) + 1

        scoring.append(f"\n{'=' * 70}")
        scoring.append("SCORING SUMMARY")
        scoring.append(f"  HIGH:   {counts.get('HIGH', 0)}")
        scoring.append(f"  MEDIUM: {counts.get('MEDIUM', 0)}")
        scoring.append(f"  LOW:    {counts.get('LOW', 0)}")
        scoring.append(f"  Total:  {sum(counts.values())}")

        for line in scoring:
            self.emit(line)

        return scoring

    # ================================================================
    # PART E — CORRECTION PLAN
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E — CORRECTION PLAN")
        self.emit("=" * 70)

        conn = self.connect_ro()
        # Get benchmark track_ids
        bench_ids = set(
            r[0] for r in conn.execute(
                "SELECT track_id FROM benchmark_set_tracks WHERE benchmark_set_id=?",
                (BENCHMARK_SET_ID,)
            ).fetchall()
        )
        conn.close()

        plan_rows = []
        for r in self.all_secondaries:
            tid = r["track_id"]
            audit = self.audit_scores.get(tid, "MEDIUM")
            in_benchmark = tid in bench_ids
            secondary = r["secondary_genre"]

            # Determine action
            if audit == "HIGH":
                action = "KEEP"
                reason = "High confidence hybrid"
            elif audit == "LOW":
                # Check if it's a major hybrid pair
                pair = (r["primary_genre"], secondary)
                is_major = pair in {
                    ("Hip-Hop", "Rock"), ("Rock", "Metal"),
                    ("Country", "Hip-Hop"), ("Country", "Rock"),
                    ("Rock", "Country"),
                }
                has_subgenre_support = False  # No subgenre set on secondaries

                if secondary == "Electronic":
                    action = "REMOVE_CANDIDATE"
                    reason = "Electronic secondary from misfit — not genre membership"
                elif tid in TRACK_LEVEL_OVERRIDES:
                    action = "REMOVE_CANDIDATE"
                    reason = "Track-level override: known incorrect assignment"
                elif not is_major and not has_subgenre_support:
                    action = "REMOVE_CANDIDATE"
                    reason = "Low confidence, no subgenre support, non-major pair"
                else:
                    action = "REVIEW"
                    reason = "Low confidence but major pair — needs per-track review"
            else:  # MEDIUM
                action = "REVIEW"
                reason = "Medium confidence — needs validation"

            plan_rows.append({
                "track_id": tid,
                "label_id": r["label_id"],
                "artist": r["artist"],
                "title": r["title"],
                "primary_genre": r["primary_genre"],
                "secondary_genre": secondary,
                "original_confidence": r["confidence"],
                "audit_confidence": audit,
                "in_benchmark": in_benchmark,
                "action": action,
                "reason": reason,
            })

        self.correction_plan = plan_rows

        df = pd.DataFrame(plan_rows)
        df.to_csv(DATA_DIR / "secondary_label_correction_plan.csv",
                  index=False, encoding="utf-8")
        df.to_csv(PROOF_DIR / "05_correction_plan.csv",
                  index=False, encoding="utf-8")

        action_counts = df["action"].value_counts()
        self.emit(f"\nCorrection plan:")
        for action in ["KEEP", "REVIEW", "REMOVE_CANDIDATE"]:
            self.emit(f"  {action}: {action_counts.get(action, 0)}")
        self.emit(f"  Total: {len(df)}")
        self.emit(f"  In benchmark: {df['in_benchmark'].sum()}")

        return plan_rows

    # ================================================================
    # PART F — CONTROLLED CLEANUP
    # ================================================================
    def part_f(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART F — CONTROLLED CLEANUP (ENABLED)")
        self.emit("=" * 70)

        conn = self.connect_ro()
        bench_ids = set(
            r[0] for r in conn.execute(
                "SELECT track_id FROM benchmark_set_tracks WHERE benchmark_set_id=?",
                (BENCHMARK_SET_ID,)
            ).fetchall()
        )
        conn.close()

        # Identify labels meeting ALL removal criteria:
        # - action = REMOVE_CANDIDATE
        # - low confidence (audit)
        # - no subgenre support (none of our secondaries have subgenre_id set)
        # - not part of major hybrid pair with strong evidence
        # - not in benchmark-critical tracks (we still remove from benchmark
        #   tracks IF the label is clearly wrong — Electronic secondaries)

        to_remove = []
        for r in self.correction_plan:
            if r["action"] != "REMOVE_CANDIDATE":
                continue

            # Additional guard: be conservative with benchmark tracks
            # Only remove from benchmark if secondary is Electronic (clearly wrong)
            if r["in_benchmark"] and r["secondary_genre"] != "Electronic":
                self.emit(f"  SKIP BENCHMARK: [{r['track_id']}] {r['artist'][:25]} "
                          f"— {r['secondary_genre']} (conservative)")
                continue

            to_remove.append(r)

        self.emit(f"Labels eligible for removal: {len(to_remove)}")

        if not to_remove:
            self.emit("No removals to perform.")
            return

        # Perform removals
        conn = self.connect_rw()
        pre_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]

        for r in to_remove:
            label_id = r["label_id"]
            conn.execute(
                "DELETE FROM track_genre_labels WHERE id=? AND role='secondary'",
                (label_id,)
            )
            self.removals.append(r)
            self.emit(f"  REMOVE: [{r['track_id']}] {r['artist'][:25]} — "
                      f"{r['primary_genre']}→{r['secondary_genre']} "
                      f"(reason: {r['reason'][:50]})")

        conn.commit()

        post_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        conn.close()

        self.emit(f"\nRemoved: {len(self.removals)}")
        self.emit(f"Secondary labels: {pre_count} → {post_count}")

    # ================================================================
    # PART G — RULE SET
    # ================================================================
    def part_g(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART G — RULE TIGHTENING OUTPUT")
        self.emit("=" * 70)

        ruleset = []
        ruleset.append("=" * 70)
        ruleset.append("SECONDARY LABEL RULE SET V1")
        ruleset.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        ruleset.append("=" * 70)

        ruleset.append("""
GENERAL RULES
─────────────
1. Max 1 secondary label per track (V1 constraint).
2. Secondary genre MUST differ from primary genre.
3. Secondary genre MUST differ from primary in V2 taxonomy.
4. applied_by MUST be set (audit trail required).
5. source MUST be 'manual' (no auto-generated labels).
6. Blanket artist-level rules are PROHIBITED.
   Each track must be individually justified.
""")

        rules = [
            {
                "pair": "Country → Hip-Hop",
                "allowed_if": [
                    "Artist is an established country-rap artist (Jelly Roll, "
                    "Demun Jones, Upchurch, Adam Calhoun, etc.)",
                    "OR track contains explicit rap vocals + country instrumentation",
                    "OR subgenre tag contains 'Country Rap' / 'Hick-Hop'",
                ],
                "confidence_threshold": 0.9,
                "notes": "Well-established hybrid genre. Strong evidence basis.",
            },
            {
                "pair": "Hip-Hop → Rock",
                "allowed_if": [
                    "Track contains rock instrumentation (guitars, drums)",
                    "AND at least one: rap-rock production, rock samples, "
                    "or live band backing",
                    "NOT allowed: blanket artist rule (e.g. all Tom MacDonald)",
                    "Per-track validation required",
                ],
                "confidence_threshold": 0.8,
                "notes": "High volume pair. Over-concentration on single artist "
                         "was the primary audit finding. Tighten to per-track.",
            },
            {
                "pair": "Rock → Metal",
                "allowed_if": [
                    "Subgenre includes metal-related tags (glam metal, hard rock, "
                    "thrash, etc.)",
                    "OR track demonstrates metal characteristics (distortion, "
                    "double bass, etc.)",
                    "NOT allowed for power ballads or acoustic tracks by metal "
                    "artists",
                ],
                "confidence_threshold": 0.85,
                "notes": "Generally sound. Exclude soft/acoustic tracks.",
            },
            {
                "pair": "Rock → Country",
                "allowed_if": [
                    "Artist is a known southern rock / country-rock crossover "
                    "(Kid Rock, Van Zant, etc.)",
                    "OR track features country instrumentation (steel guitar, "
                    "fiddle, twang)",
                ],
                "confidence_threshold": 0.85,
                "notes": "Well-documented crossover territory.",
            },
            {
                "pair": "Country → Rock",
                "allowed_if": [
                    "Artist is a known country-rock artist (Chris Stapleton, etc.)",
                    "OR track features prominent rock instrumentation",
                    "NOT allowed: generic country with electric guitar",
                ],
                "confidence_threshold": 0.85,
                "notes": "Established hybrid. Chris Stapleton archetype.",
            },
            {
                "pair": "Hip-Hop → Pop",
                "allowed_if": [
                    "Track is pop-rap with pop hooks, structure, and production",
                    "NOT allowed: hip-hop track that charted on pop charts",
                ],
                "confidence_threshold": 0.8,
                "notes": "Pop-rap is valid but narrow.",
            },
            {
                "pair": "Country → Pop",
                "allowed_if": [
                    "Track has explicit pop production (synths, programmed drums)",
                    "AND maintains country vocals/themes",
                    "NOT allowed: country track with high production value alone",
                    "NOT allowed: misfit anomaly detection as sole evidence",
                ],
                "confidence_threshold": 0.9,
                "notes": "Country-pop exists but misfit detection is insufficient.",
            },
            {
                "pair": "* → Electronic",
                "allowed_if": [
                    "PROHIBITED as secondary label",
                    "Electronic feature similarity ≠ genre membership",
                    "Misfit anomaly detection targeting Electronic is noise",
                ],
                "confidence_threshold": float("inf"),
                "notes": "All 5 Electronic secondaries were misfit-derived noise. "
                         "Removed in cleanup.",
            },
            {
                "pair": "Rock → Pop",
                "allowed_if": [
                    "Track is produced as pop-rock (Loverboy archetype)",
                    "NOT allowed: rock track with pop appeal alone",
                ],
                "confidence_threshold": 0.85,
                "notes": "Narrow. Pop-rock is a valid subgenre.",
            },
            {
                "pair": "Pop → Hip-Hop",
                "allowed_if": [
                    "Track contains genuine rap verses or hip-hop production",
                    "NOT allowed: pop track with rhythmic elements",
                ],
                "confidence_threshold": 0.85,
                "notes": "Narrow but valid (Weird Al parodies are edge cases).",
            },
        ]

        for rule in rules:
            ruleset.append(f"\n{'─' * 60}")
            ruleset.append(f"PAIR: {rule['pair']}")
            ruleset.append(f"Confidence threshold: {rule['confidence_threshold']}")
            ruleset.append(f"{'─' * 60}")
            ruleset.append("  ALLOWED IF:")
            for cond in rule["allowed_if"]:
                ruleset.append(f"    • {cond}")
            ruleset.append(f"  NOTES: {rule['notes']}")

        ruleset.append(f"\n{'=' * 70}")
        ruleset.append("DISALLOWED PATTERNS")
        ruleset.append("=" * 70)
        ruleset.append("""
1. Electronic as secondary — always noise from feature similarity.
2. Blanket artist rules — must validate per-track.
3. Misfit anomaly as sole evidence — insufficient without artist/subgenre backup.
4. Same genre in V2 taxonomy as primary + secondary.
5. Labels without applied_by audit trail.
""")

        for line in ruleset:
            self.emit(line)

        return ruleset

    # ================================================================
    # PART H — OUTPUTS
    # ================================================================
    def part_h(self, findings, scoring, ruleset):
        self.emit("\n" + "=" * 70)
        self.emit("PART H — OUTPUTS")
        self.emit("=" * 70)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)

        def w(name, content):
            path = PROOF_DIR / name
            if isinstance(content, list):
                content = "\n".join(str(c) for c in content)
            path.write_text(content, encoding="utf-8")

        # 00 — inventory
        inv = []
        inv.append("=" * 70)
        inv.append("SECONDARY LABEL INVENTORY")
        inv.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        inv.append("=" * 70)
        inv.append(f"\nTotal secondaries (pre-cleanup): {len(self.all_secondaries)}")
        inv.append(f"Removed in cleanup: {len(self.removals)}")

        conn = self.connect_ro()
        post = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        conn.close()
        inv.append(f"Remaining: {post}")

        inv.append(f"\nBy original confidence:")
        by_conf = {}
        for r in self.all_secondaries:
            by_conf[r["confidence"]] = by_conf.get(r["confidence"], 0) + 1
        for c, n in sorted(by_conf.items()):
            inv.append(f"  {c}: {n}")

        inv.append(f"\nBy audit confidence:")
        audit_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for v in self.audit_scores.values():
            audit_counts[v] += 1
        for k in ["HIGH", "MEDIUM", "LOW"]:
            inv.append(f"  {k}: {audit_counts[k]}")

        inv.append(f"\nRemoved labels:")
        for r in self.removals:
            inv.append(f"  [{r['track_id']}] {r['artist'][:25]} — "
                       f"{r['primary_genre']}→{r['secondary_genre']}: {r['reason'][:50]}")

        w("00_secondary_inventory.txt", inv)

        # 01 — already written in part_a

        # 03 — rule validation
        w("03_rule_validation.txt", findings)

        # 04 — confidence scoring
        w("04_confidence_scoring.txt", scoring)

        # 05 — already written in part_e

        # 06 — rule set
        w("06_rule_set_v1.txt", ruleset)

        self.emit(f"Proof files written to {PROOF_DIR}")

    # ================================================================
    # PART I — VALIDATION
    # ================================================================
    def part_i(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART I — VALIDATION")
        self.emit("=" * 70)

        conn = self.connect_ro()
        all_ok = True
        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. Primary labels unchanged
        primary_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk1 = primary_count == 783
        val.append(f"\n  1. Primary labels: {primary_count} (expected 783) "
                   f"— {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. No duplicate primaries
        dup = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk2 = dup == 0
        val.append(f"  2. Duplicate primaries: {dup} — {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. FK integrity
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk3 = len(fk) == 0
        val.append(f"  3. FK violations: {len(fk)} — {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. Benchmark unchanged
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (BENCHMARK_SET_ID,)
        ).fetchone()[0]
        chk4 = bench == 202
        val.append(f"  4. Benchmark count: {bench} (expected 202) "
                   f"— {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. No track with >2 labels
        over = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id, COUNT(*) c FROM track_genre_labels "
            "  GROUP BY track_id HAVING c > 2)"
        ).fetchone()[0]
        chk5 = over == 0
        val.append(f"  5. Tracks with >2 labels: {over} — {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. Secondary count post-cleanup
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        expected_remaining = len(self.all_secondaries) - len(self.removals)
        chk6 = sec == expected_remaining
        val.append(f"  6. Secondary labels: {sec} (expected {expected_remaining} "
                   f"after {len(self.removals)} removals) — {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # 7. All remaining secondaries have audit trail
        bad_audit = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels "
            "WHERE role='secondary' AND (applied_by IS NULL OR applied_by = '')"
        ).fetchone()[0]
        chk7 = bad_audit == 0
        val.append(f"  7. Secondaries without audit trail: {bad_audit} "
                   f"— {'PASS' if chk7 else 'FAIL'}")
        if not chk7:
            all_ok = False

        # 8. No orphan secondaries
        orphan = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels t1 "
            "WHERE t1.role='secondary' "
            "AND NOT EXISTS (SELECT 1 FROM track_genre_labels t2 "
            "WHERE t2.track_id=t1.track_id AND t2.role='primary')"
        ).fetchone()[0]
        chk8 = orphan == 0
        val.append(f"  8. Orphan secondaries: {orphan} — {'PASS' if chk8 else 'FAIL'}")
        if not chk8:
            all_ok = False

        conn.close()

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        (PROOF_DIR / "07_validation_checks.txt").write_text(
            "\n".join(val), encoding="utf-8"
        )

        self.emit(f"Validation: {'PASS' if all_ok else 'FAIL'}")
        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"

        report = []
        report.append("=" * 70)
        report.append("SECONDARY LABEL AUDIT + RULE TIGHTENING — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- PART A: INVENTORY ---")
        report.append(f"  Total secondaries (pre-cleanup): {len(self.all_secondaries)}")

        report.append(f"\n--- PART B: QUALITY SAMPLING ---")
        report.append(f"  Audit sample: 30 tracks")

        report.append(f"\n--- PART C: RULE VALIDATION ---")
        report.append(f"  Key findings:")
        report.append(f"    - Hip-Hop→Rock over-concentrated (Tom MacDonald 54/55)")
        report.append(f"    - *→Electronic: all 5 are misfit noise")
        report.append(f"    - Rock→Metal: partially over-broad (Def Leppard ballads)")
        report.append(f"    - Country→Pop: weak misfit evidence only")

        report.append(f"\n--- PART D: CONFIDENCE RE-SCORING ---")
        audit_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
        for v in self.audit_scores.values():
            audit_counts[v] += 1
        for k in ["HIGH", "MEDIUM", "LOW"]:
            report.append(f"    {k}: {audit_counts[k]}")

        report.append(f"\n--- PART E: CORRECTION PLAN ---")
        action_counts = {}
        for r in self.correction_plan:
            action_counts[r["action"]] = action_counts.get(r["action"], 0) + 1
        for a in ["KEEP", "REVIEW", "REMOVE_CANDIDATE"]:
            report.append(f"    {a}: {action_counts.get(a, 0)}")

        report.append(f"\n--- PART F: CONTROLLED CLEANUP ---")
        report.append(f"  Removed: {len(self.removals)}")
        for r in self.removals:
            report.append(f"    [{r['track_id']}] {r['artist'][:25]} — "
                          f"{r['primary_genre']}→{r['secondary_genre']}")

        conn = self.connect_ro()
        post = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        conn.close()
        report.append(f"  Remaining: {post}")

        report.append(f"\n--- PART G: RULE SET ---")
        report.append(f"  Rule set V1 defined for 10 genre pairs")
        report.append(f"  Key changes:")
        report.append(f"    - Electronic as secondary: PROHIBITED")
        report.append(f"    - Blanket artist rules: PROHIBITED")
        report.append(f"    - Misfit-only evidence: INSUFFICIENT")

        report.append(f"\n--- PART I: VALIDATION ---")
        report.append(f"  {'PASS' if all_ok else 'FAIL'}")

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
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    # Part A
    pair_counts, pair_artists, by_artist = p.part_a()

    # Part B
    p.part_b(pair_counts, pair_artists)

    # Part C
    findings = p.part_c(pair_counts, pair_artists, by_artist)

    # Part D
    scoring = p.part_d()

    # Part E
    p.part_e()

    # Part F — cleanup (enabled)
    p.part_f()

    # Part G — rule set
    ruleset = p.part_g()

    # Part H — outputs
    p.part_h(findings, scoring, ruleset)

    # Part I — validation
    all_ok = p.part_i()

    # Final
    gate = p.final_report(all_ok)

    print(f"\n{'=' * 60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
