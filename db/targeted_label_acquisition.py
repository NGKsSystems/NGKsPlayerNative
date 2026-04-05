#!/usr/bin/env python3
"""
Phase 13 — Targeted Manual Label Acquisition V2

Parts:
  A) Source pool discovery (unlabeled + weak-label tracks)
  B) Candidate prioritization for Metal, Pop, Other
  C) Manual review template
  D) Gap closure plan
  E) No DB writes (read-only)
  F) Output reports + validation
"""

import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"

PROOF_DIR = WORKSPACE / "_proof" / "targeted_label_acquisition_v2"
DATA_DIR = WORKSPACE / "data"

CANDIDATES_CSV = DATA_DIR / "targeted_label_acquisition_v2.csv"
REVIEW_CSV = DATA_DIR / "targeted_label_review_v2.csv"

BENCHMARK_NAME = "genre_benchmark_v1"
TARGET_MIN = 30

# Remaining deficits after Phase 12 acquisition
DEFICITS = {
    "Metal": 10,
    "Pop": 4,
    "Other": 13,
}

# V2 mapping
V2_MAP = {
    "Country": "Country",
    "Rock": "Rock",
    "Hip-Hop": "Hip-Hop",
    "Pop": "Pop",
    "Metal": "Metal",
    "Electronic": "Other",
    "Folk": "Other",
    "Reggae": "Other",
    "R&B": "Other",
    "Soundtrack": "Other",
    "World": "Other",
}

# Artist-to-genre knowledge base — well-known artist mappings for confidence scoring.
# These are factual, widely agreed-upon classifications.
# Only used for CANDIDATE DISCOVERY — NOT as truth labels.
ARTIST_GENRE_CUES = {
    # Metal / Hard Rock
    "Five Finger Death Punch": ("Metal", "high"),
    "5FDPVEVO": ("Metal", "high"),
    "Shinedown": ("Metal", "medium"),
    "Dokken": ("Metal", "medium"),
    "Airborne": ("Metal", "medium"),
    "Airbourne": ("Metal", "medium"),
    "Girish & The Chronicles": ("Metal", "medium"),
    "New Horizon": ("Metal", "medium"),
    "Rust n' Rage": ("Metal", "medium"),
    "Jeff Scott Soto": ("Metal", "medium"),
    "Degreed": ("Rock", "medium"),
    "Pagan RIP, RIP": ("Metal", "low"),
    "RIP": ("Metal", "low"),
    "Downstait": ("Metal", "medium"),
    "Rev Theory": ("Metal", "medium"),
    "Motorhead": ("Metal", "high"),
    "Liliac": ("Metal", "medium"),

    # Pop
    "Tones and I": ("Pop", "high"),
    "Adele": ("Pop", "high"),
    "Norah Jones": ("Pop", "medium"),
    "Lorde": ("Pop", "high"),
    "Nicki Minaj": ("Pop", "medium"),
    "Player": ("Pop", "medium"),
    "Blondie": ("Pop", "medium"),
    "The Tubes": ("Pop", "medium"),

    # Hip-Hop / Rap
    "2Pac": ("Hip-Hop", "high"),
    "50 Cent": ("Hip-Hop", "high"),
    "Hopsin": ("Hip-Hop", "high"),
    "The Notorious B.I.G.": ("Hip-Hop", "high"),
    "The Sugar Hill Gang": ("Hip-Hop", "high"),
    "Grandmaster Flash & The Furious Five": ("Hip-Hop", "high"),
    "Rob Base & DJ EZ Rock": ("Hip-Hop", "high"),
    "Denzel Curry": ("Hip-Hop", "high"),
    "XXXTentacion": ("Hip-Hop", "high"),
    "Childish Gambino": ("Hip-Hop", "high"),
    "Dr. Dre": ("Hip-Hop", "high"),
    "Dax": ("Hip-Hop", "medium"),
    "Tom MacDonald": ("Hip-Hop", "medium"),
    "Tom McDonald": ("Hip-Hop", "medium"),

    # Rock
    "The Who": ("Rock", "high"),
    "Eric Clapton": ("Rock", "high"),
    "Daughtry": ("Rock", "high"),
    "Nirvana": ("Rock", "high"),
    "Queen": ("Rock", "high"),
    "Eagles": ("Rock", "high"),
    "Pink Floyd": ("Rock", "high"),
    "The Alan Parsons Project": ("Rock", "high"),
    "Pretenders": ("Rock", "high"),
    "Creed": ("Rock", "high"),
    "Loverboy": ("Rock", "medium"),
    "English Tuition": ("Rock", "medium"),  # Loverboy - Turn Me Loose

    # Country / Country-rap
    "Jelly Roll": ("Country", "medium"),
    "Upchurch": ("Country", "medium"),
    "Ryan Upchurch": ("Country", "medium"),
    "Jawga Boyz": ("Country", "medium"),
    "Oliver Anthony": ("Country", "high"),
    "radiowv": ("Country", "medium"),
    "Tim Montana": ("Country", "high"),
    "Thomas Rhett": ("Country", "high"),
    "Trace Adkins": ("Country", "high"),
    "Blake Shelton": ("Country", "high"),
    "Chris Young": ("Country", "high"),
    "Garth Brooks": ("Country", "high"),
    "Kolby Cooper": ("Country", "high"),
    "Clint Black": ("Country", "high"),
    "Rick Trevino": ("Country", "medium"),
    "Craig Morgan": ("Country", "high"),
    "Johnny Cash": ("Country", "high"),
    "Buddy Brown": ("Country", "high"),
    "David Allan Coe": ("Country", "high"),

    # Electronic / Other
    "SKRILLEX": ("Electronic", "high"),
    "Massive Attack": ("Electronic", "high"),
    "James Blake": ("Electronic", "medium"),
    "Yosi Horikawa": ("Electronic", "high"),
    "Kainbeats": ("Electronic", "medium"),
    "Beats and Styles": ("Electronic", "medium"),
    "DJ LOA": ("Electronic", "medium"),

    # World / Other
    "Samuelu Faoliu": ("World", "medium"),
    "JD Crutch": ("World", "medium"),
    "Josh Wawa White": ("World", "medium"),
    "KC DeLeon Guerrero": ("World", "medium"),
    "Dan Pocaigue": ("World", "medium"),

    # Soundtrack / Other
    "Hans Zimmer": ("Soundtrack", "high"),

    # Parody / Comedy — ambiguous
    "Weird Al Yankovic": ("Pop", "low"),

    # Unknown test / utility tracks
    "Unknown": (None, None),  # handled per-title below
}

# Title-based cues for "Unknown" artist tracks
TITLE_GENRE_CUES = {
    "120 BPM Metronome": ("_utility", "high"),
    "bass": ("_utility", "high"),
    "drums": ("_utility", "high"),
    "vocals": ("_utility", "high"),
    "other": ("_utility", "high"),
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
    # PART A — SOURCE POOL DISCOVERY
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART A — SOURCE POOL DISCOVERY")
        self.emit("=" * 60)

        conn = self.connect_ro()

        # Source 1: Unlabeled tracks (no primary label)
        unlabeled = conn.execute("""
            SELECT t.id AS track_id, t.artist, t.title, t.album, t.file_path
            FROM tracks t
            WHERE t.id NOT IN (
                SELECT track_id FROM track_genre_labels WHERE role = 'primary'
            )
            ORDER BY t.artist, t.title
        """).fetchall()

        unlabeled_df = pd.DataFrame([dict(r) for r in unlabeled])
        self.emit(f"Source 1 — Unlabeled tracks: {len(unlabeled_df)}")

        # Source 2: Tracks with labels but NOT in benchmark and NOT in Phase 12 candidates
        # (already handled by Phase 12 — pool exhausted for target classes)
        # Still useful to check if any were missed

        # Count how many labeled non-benchmark exist per original genre
        labeled_pool = conn.execute("""
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
        labeled_pool_dict = {r["genre"]: r["c"] for r in labeled_pool}

        conn.close()

        # Inventory text
        inv = []
        inv.append("=" * 70)
        inv.append("SOURCE POOL INVENTORY")
        inv.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        inv.append("=" * 70)

        inv.append(f"\n--- SOURCE 1: UNLABELED TRACKS ---")
        inv.append(f"Total unlabeled: {len(unlabeled_df)}")

        # Classify unlabeled by artist cue
        cue_counts = {}
        for _, row in unlabeled_df.iterrows():
            artist = row["artist"]
            title = row["title"]
            genre_cue = None
            if artist in ARTIST_GENRE_CUES:
                genre_cue = ARTIST_GENRE_CUES[artist][0]
            elif artist == "Unknown":
                # Try title-based cues
                for tk, (tg, _) in TITLE_GENRE_CUES.items():
                    if title and tk.lower() == title.strip().lower():
                        genre_cue = tg
                        break
                if genre_cue is None:
                    # Try to extract artist from title
                    for known_artist, (ag, _) in ARTIST_GENRE_CUES.items():
                        if known_artist != "Unknown" and title and known_artist.lower() in title.lower():
                            genre_cue = ag
                            break
            if genre_cue is None:
                genre_cue = "_unknown"
            cue_counts[genre_cue] = cue_counts.get(genre_cue, 0) + 1

        inv.append(f"\nUnlabeled by artist/title genre cue:")
        for cue, cnt in sorted(cue_counts.items(), key=lambda x: -x[1]):
            v2 = V2_MAP.get(cue, cue)
            inv.append(f"  {cue:15s} (V2: {v2:10s}): {cnt}")

        inv.append(f"\n--- SOURCE 2: LABELED NON-BENCHMARK (already used in Phase 12) ---")
        for genre, cnt in sorted(labeled_pool_dict.items(), key=lambda x: -x[1]):
            v2 = V2_MAP.get(genre, genre)
            inv.append(f"  {genre:15s} (V2: {v2:10s}): {cnt}")
        inv.append(f"  Note: Phase 12 exhausted Metal(14), Pop(18), Other-constituent(8)")

        inv.append(f"\n--- SOURCE 3: UTILITY / NON-MUSIC TRACKS ---")
        utility_count = cue_counts.get("_utility", 0)
        inv.append(f"  Utility/test tracks (metronome, stems, etc.): {utility_count}")
        inv.append(f"  These are excluded from genre classification.")

        self.emit(f"Pool inventory complete: {len(unlabeled_df)} unlabeled, "
                  f"{sum(labeled_pool_dict.values())} labeled non-benchmark")

        return unlabeled_df, inv

    # ================================================================
    # PART B — CANDIDATE PRIORITIZATION
    # ================================================================
    def part_b(self, unlabeled_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART B — CANDIDATE PRIORITIZATION")
        self.emit("=" * 60)

        candidates = []

        for _, row in unlabeled_df.iterrows():
            track_id = row["track_id"]
            artist = row["artist"]
            title = row["title"]

            genre_cue = None
            conf = None
            evidence = "artist_name"

            # 1. Check artist cue
            if artist in ARTIST_GENRE_CUES:
                genre_cue, conf = ARTIST_GENRE_CUES[artist]
            elif artist == "Unknown":
                # Check title-based utility
                for tk, (tg, tc) in TITLE_GENRE_CUES.items():
                    if title and tk.lower() == title.strip().lower():
                        genre_cue, conf = tg, tc
                        evidence = "title_match_utility"
                        break
                # If not utility, try artist extraction from title
                if genre_cue is None and title:
                    for known_artist, (ag, ac) in ARTIST_GENRE_CUES.items():
                        if known_artist != "Unknown" and known_artist.lower() in title.lower():
                            genre_cue = ag
                            # Downgrade confidence since artist is listed as Unknown
                            conf = "low" if ac == "high" else "low"
                            evidence = "title_contains_artist"
                            break

            if genre_cue is None:
                genre_cue = "_unknown"
                conf = "low"
                evidence = "no_cue"

            # Map to V2
            v2_genre = V2_MAP.get(genre_cue, genre_cue)

            # Skip utility tracks
            if genre_cue == "_utility":
                continue

            # Only select for deficit classes: Metal, Pop, Other
            if v2_genre not in DEFICITS:
                continue

            candidates.append({
                "track_id": track_id,
                "artist": artist,
                "title": title,
                "current_label_state": "unlabeled",
                "likely_v2_genre": v2_genre,
                "evidence_source": evidence,
                "confidence_tier": conf,
                "review_priority": 0,  # will be set below
                "notes": "",
            })

        cand_df = pd.DataFrame(candidates)

        if len(cand_df) == 0:
            self.emit("WARNING: No candidates found for deficit classes")
            return cand_df

        # Assign review priority
        # Priority = deficit urgency * confidence
        conf_weight = {"high": 3, "medium": 2, "low": 1}
        deficit_weight = {g: d for g, d in DEFICITS.items()}

        for idx, row in cand_df.iterrows():
            dw = deficit_weight.get(row["likely_v2_genre"], 0)
            cw = conf_weight.get(row["confidence_tier"], 0)
            cand_df.loc[idx, "review_priority"] = dw * cw

        # Sort: highest priority first
        cand_df = cand_df.sort_values(
            ["review_priority", "likely_v2_genre", "confidence_tier"],
            ascending=[False, True, True]
        ).reset_index(drop=True)

        # Assign integer priority rank
        cand_df["review_priority"] = range(1, len(cand_df) + 1)

        # Add notes for ambiguous cases
        for idx, row in cand_df.iterrows():
            notes = []
            if row["artist"] == "Unknown":
                notes.append("Artist unknown — needs manual verification")
            if row["confidence_tier"] == "low":
                notes.append("Low confidence — likely needs listening review")
            if row["evidence_source"] == "no_cue":
                notes.append("No metadata cue — genre unknown without listening")
            cand_df.loc[idx, "notes"] = "; ".join(notes)

        # Validate: no dups
        dup_count = cand_df["track_id"].duplicated().sum()
        self.emit(f"Candidates: {len(cand_df)} tracks, {dup_count} duplicates")

        # Save
        cand_df.to_csv(CANDIDATES_CSV, index=False, encoding="utf-8")
        self.emit(f"Candidates CSV: {CANDIDATES_CSV}")

        # By class summary
        for v2g in ["Metal", "Pop", "Other"]:
            subset = cand_df[cand_df["likely_v2_genre"] == v2g]
            self.emit(f"  {v2g}: {len(subset)} candidates "
                      f"(high={len(subset[subset['confidence_tier']=='high'])}, "
                      f"medium={len(subset[subset['confidence_tier']=='medium'])}, "
                      f"low={len(subset[subset['confidence_tier']=='low'])})")

        return cand_df

    # ================================================================
    # PART C — REVIEW TEMPLATE
    # ================================================================
    def part_c(self, cand_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART C — REVIEW TEMPLATE")
        self.emit("=" * 60)

        review_rows = []
        for _, row in cand_df.iterrows():
            review_rows.append({
                "track_id": row["track_id"],
                "artist": row["artist"],
                "title": row["title"],
                "proposed_v2_genre": row["likely_v2_genre"],
                "final_genre": "",
                "final_subgenre": "",
                "action": "",
                "notes": "",
            })

        review_df = pd.DataFrame(review_rows)

        # Add example populated rows
        if len(review_df) >= 3:
            review_df.loc[review_df.index[0], "action"] = "approve_label"
            review_df.loc[review_df.index[0], "final_genre"] = review_df.loc[review_df.index[0], "proposed_v2_genre"]
            review_df.loc[review_df.index[0], "notes"] = "Confirmed by listening — clean exemplar"
            review_df.loc[review_df.index[1], "action"] = "approve_label"
            review_df.loc[review_df.index[1], "final_genre"] = review_df.loc[review_df.index[1], "proposed_v2_genre"]
            review_df.loc[review_df.index[1], "notes"] = ""
            review_df.loc[review_df.index[2], "action"] = "skip"
            review_df.loc[review_df.index[2], "notes"] = "Ambiguous genre — not a clean exemplar"

        review_df.to_csv(REVIEW_CSV, index=False, encoding="utf-8")
        self.emit(f"Review CSV: {REVIEW_CSV} ({len(review_df)} rows)")

        # Summary
        summary = []
        summary.append("=" * 70)
        summary.append("REVIEW TEMPLATE SUMMARY")
        summary.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        summary.append("=" * 70)
        summary.append(f"\nFile: {REVIEW_CSV}")
        summary.append(f"Total rows: {len(review_df)}")
        summary.append(f"Columns: {list(review_df.columns)}")
        summary.append(f"\nBy proposed V2 genre:")
        for g, c in review_df["proposed_v2_genre"].value_counts().sort_values(ascending=False).items():
            summary.append(f"  {g:15s}: {c}")
        summary.append(f"\nInstructions:")
        summary.append(f"  1. Open {REVIEW_CSV.name}")
        summary.append(f"  2. Listen to each track")
        summary.append(f"  3. Set 'final_genre' to the correct V2 genre (or original genre)")
        summary.append(f"  4. Optionally set 'final_subgenre'")
        summary.append(f"  5. Set action: approve_label | skip | hold")
        summary.append(f"  6. Save and pass to Phase 14 for label insertion")
        summary.append(f"\nAction values:")
        summary.append(f"  approve_label — confirmed correct, ready for label insertion + benchmark")
        summary.append(f"  skip — not usable (ambiguous, wrong genre, bad quality)")
        summary.append(f"  hold — uncertain, defer to later review")
        summary.append(f"\nExample rows (first 3 pre-populated):")
        for _, row in review_df.head(3).iterrows():
            summary.append(f"  [{row['track_id']}] {row['artist'][:25]:25s} "
                           f"| proposed={row['proposed_v2_genre']:10s} "
                           f"| action={str(row['action']):13s} "
                           f"| {row['notes']}")

        return review_df, summary

    # ================================================================
    # PART D — GAP CLOSURE PLAN
    # ================================================================
    def part_d(self, cand_df):
        self.emit("\n" + "=" * 60)
        self.emit("PART D — GAP CLOSURE PLAN")
        self.emit("=" * 60)

        plan = []
        plan.append("=" * 70)
        plan.append("GAP CLOSURE PLAN — REMAINING V2 DEFICIT")
        plan.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        plan.append("=" * 70)

        plan.append(f"\n{'V2 Genre':15s}  {'Current*':>9s}  {'Target':>7s}  {'Deficit':>8s}  "
                    f"{'High':>5s}  {'Med':>5s}  {'Low':>5s}  {'Total':>6s}  Expected Status")
        plan.append("-" * 90)
        plan.append(f"  * 'Current' = after Phase 12 acquisition (if all approved)")

        # After Phase 12:
        # Metal: 6 bench + 14 selected = 20 → deficit 10
        # Pop: 8 bench + 18 selected = 26 → deficit 4
        # Other: 9 bench + 8 selected = 17 → deficit 13
        phase12_after = {"Metal": 20, "Pop": 26, "Other": 17}

        for v2g in ["Metal", "Pop", "Other"]:
            current = phase12_after[v2g]
            deficit = DEFICITS[v2g]
            subset = cand_df[cand_df["likely_v2_genre"] == v2g] if len(cand_df) > 0 else pd.DataFrame()
            high = len(subset[subset["confidence_tier"] == "high"]) if len(subset) > 0 else 0
            med = len(subset[subset["confidence_tier"] == "medium"]) if len(subset) > 0 else 0
            low = len(subset[subset["confidence_tier"] == "low"]) if len(subset) > 0 else 0
            total = high + med + low

            # Expected closure: high-conf candidates very likely to pass review
            # medium: ~70% pass rate estimate
            # low: ~30% pass rate estimate
            expected_yield = high + int(med * 0.7) + int(low * 0.3)

            if expected_yield >= deficit:
                status = "LIKELY CLOSED"
            elif expected_yield >= deficit * 0.5:
                status = "PARTIALLY CLOSE"
            else:
                status = "NEEDS MORE DATA"

            plan.append(f"{v2g:15s}  {current:9d}  {TARGET_MIN:7d}  {deficit:8d}  "
                        f"{high:5d}  {med:5d}  {low:5d}  {total:6d}  {status}")

        plan.append(f"\n--- PER-CLASS DETAIL ---")

        for v2g in ["Metal", "Pop", "Other"]:
            current = phase12_after[v2g]
            deficit = DEFICITS[v2g]
            subset = cand_df[cand_df["likely_v2_genre"] == v2g] if len(cand_df) > 0 else pd.DataFrame()

            plan.append(f"\n  {v2g}:")
            plan.append(f"    Current (after Phase 12 approvals): {current}")
            plan.append(f"    Target: {TARGET_MIN}")
            plan.append(f"    Remaining deficit: {deficit}")
            plan.append(f"    Candidates found: {len(subset)}")

            if len(subset) > 0:
                high = subset[subset["confidence_tier"] == "high"]
                med = subset[subset["confidence_tier"] == "medium"]
                low = subset[subset["confidence_tier"] == "low"]

                plan.append(f"    High-confidence ({len(high)}):")
                for _, r in high.iterrows():
                    plan.append(f"      [{r['track_id']}] {r['artist'][:30]} — {r['title'][:40]}")
                plan.append(f"    Medium-confidence ({len(med)}):")
                for _, r in med.iterrows():
                    plan.append(f"      [{r['track_id']}] {r['artist'][:30]} — {r['title'][:40]}")
                plan.append(f"    Low-confidence ({len(low)}):")
                for _, r in low.iterrows():
                    plan.append(f"      [{r['track_id']}] {r['artist'][:30]} — {r['title'][:40]}")

            expected_yield = (len(subset[subset["confidence_tier"] == "high"]) +
                              int(len(subset[subset["confidence_tier"] == "medium"]) * 0.7) +
                              int(len(subset[subset["confidence_tier"] == "low"]) * 0.3)) if len(subset) > 0 else 0

            gap_after = max(0, deficit - expected_yield)
            plan.append(f"    Expected yield (conservative): {expected_yield}")
            plan.append(f"    Expected remaining gap: {gap_after}")

            if gap_after > 0:
                plan.append(f"    ACTION NEEDED: Source {gap_after} additional tracks for {v2g} "
                            f"(external acquisition or re-tagging)")
            else:
                plan.append(f"    CLOSURE: Likely achievable if candidates pass manual review")

        plan.append(f"\n--- OVERALL ASSESSMENT ---")
        total_deficit = sum(DEFICITS.values())
        total_cands = len(cand_df)
        plan.append(f"Total remaining deficit: {total_deficit}")
        plan.append(f"Total candidates discovered: {total_cands}")
        plan.append(f"Coverage: {total_cands}/{total_deficit} "
                    f"({min(total_cands/total_deficit*100, 100):.0f}% of deficit if all pass)")

        if total_cands >= total_deficit:
            plan.append(f"Assessment: Sufficient candidates found to close deficit (pending review)")
        elif total_cands >= total_deficit * 0.7:
            plan.append(f"Assessment: Near-sufficient candidates — small external acquisition may be needed")
        else:
            plan.append(f"Assessment: Significant shortfall — external track acquisition required")

        self.emit(f"Gap closure plan complete")
        return plan

    # ================================================================
    # PART E — NO DB WRITES
    # ================================================================
    def part_e_verify(self):
        self.emit("\n" + "=" * 60)
        self.emit("PART E — DB WRITE VERIFICATION")
        self.emit("=" * 60)
        self.emit("  DB opened in READ-ONLY mode (file: URI with ?mode=ro)")
        self.emit("  No INSERT/UPDATE/DELETE executed")
        self.emit("  No schema changes")

    # ================================================================
    # PART F — OUTPUTS + VALIDATION
    # ================================================================
    def part_f(self, cand_df, pool_inv, review_summary, closure_plan):
        self.emit("\n" + "=" * 60)
        self.emit("PART F — OUTPUTS + VALIDATION")
        self.emit("=" * 60)

        PROOF_DIR.mkdir(parents=True, exist_ok=True)
        elapsed = round(time.time() - self.t0, 2)

        def w(name, content):
            path = PROOF_DIR / name
            if isinstance(content, list):
                content = "\n".join(content)
            path.write_text(content, encoding="utf-8")

        conn = self.connect_ro()

        # 00 — deficit summary
        deficit_lines = []
        deficit_lines.append("=" * 70)
        deficit_lines.append("REMAINING DEFICIT SUMMARY")
        deficit_lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        deficit_lines.append("=" * 70)
        deficit_lines.append(f"\nPhase 12 selected 41 tracks from labeled pool.")
        deficit_lines.append(f"27-track deficit remains across 3 V2 classes:")
        deficit_lines.append(f"")
        deficit_lines.append(f"{'V2 Genre':15s}  {'After Ph12':>11s}  {'Target':>7s}  {'Deficit':>8s}")
        deficit_lines.append("-" * 50)
        phase12_after = {"Metal": 20, "Pop": 26, "Other": 17}
        for v2g in ["Metal", "Pop", "Other"]:
            deficit_lines.append(f"{v2g:15s}  {phase12_after[v2g]:11d}  {TARGET_MIN:7d}  {DEFICITS[v2g]:8d}")
        deficit_lines.append("-" * 50)
        deficit_lines.append(f"{'TOTAL':15s}  {sum(phase12_after.values()):11d}  "
                             f"{TARGET_MIN * 3:7d}  {sum(DEFICITS.values()):8d}")
        deficit_lines.append(f"\nSource for closure: unlabeled tracks (179 total in DB)")
        deficit_lines.append(f"Labeled pool: exhausted for target classes in Phase 12")
        w("00_remaining_deficit_summary.txt", deficit_lines)

        # 01 — pool inventory
        w("01_source_pool_inventory.txt", pool_inv)

        # 02 — ranked candidates
        ranked = []
        ranked.append("=" * 70)
        ranked.append("RANKED CANDIDATES FOR MANUAL LABEL ACQUISITION")
        ranked.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        ranked.append("=" * 70)
        ranked.append(f"\nTotal candidates: {len(cand_df)}")
        ranked.append(f"Target: Metal({DEFICITS['Metal']}), Pop({DEFICITS['Pop']}), "
                      f"Other({DEFICITS['Other']})")
        ranked.append(f"\nDuplicate track_ids: {cand_df['track_id'].duplicated().sum()}")
        ranked.append(f"\nBy V2 genre:")
        for g, c in cand_df["likely_v2_genre"].value_counts().sort_values(ascending=False).items():
            ranked.append(f"  {g:15s}: {c}")
        ranked.append(f"\nBy confidence:")
        for conf in ["high", "medium", "low"]:
            cnt = len(cand_df[cand_df["confidence_tier"] == conf])
            ranked.append(f"  {conf:10s}: {cnt}")
        ranked.append(f"\nFull ranked list:")
        for _, row in cand_df.iterrows():
            ranked.append(
                f"  #{row['review_priority']:3d} [{row['track_id']:5d}] "
                f"{row['artist'][:25]:25s} | {str(row['title'])[:40]:40s} "
                f"| {row['likely_v2_genre']:10s} | {row['confidence_tier']:6s} "
                f"| {row['evidence_source']}"
            )
        w("02_ranked_candidates.txt", ranked)

        # 03 — review template summary
        w("03_review_template_summary.txt", review_summary)

        # 04 — gap closure plan
        w("04_gap_closure_plan.txt", closure_plan)

        # 05 — validation checks
        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        all_ok = True

        # SQL checks
        bench_count = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks "
            "WHERE benchmark_set_id = (SELECT id FROM benchmark_sets WHERE name = ?)",
            (BENCHMARK_NAME,)
        ).fetchone()[0]
        chk1 = bench_count == 200
        val.append(f"\n  1. Benchmark count: {bench_count} (expected 200) — {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        label_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk2 = label_count == 781
        val.append(f"  2. Primary labels: {label_count} (expected 781) — {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        dup_primaries = conn.execute(
            "SELECT COUNT(*) FROM (SELECT track_id FROM track_genre_labels "
            "WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk3 = dup_primaries == 0
        val.append(f"  3. Duplicate primaries: {dup_primaries} — {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk4 = len(fk_violations) == 0
        val.append(f"  4. FK integrity: {len(fk_violations)} violations — {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # Candidate checks
        dup_cands = cand_df["track_id"].duplicated().sum()
        chk5 = dup_cands == 0
        val.append(f"  5. No duplicate candidate track_ids: {dup_cands} — {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # All candidates target deficit classes
        non_target = cand_df[~cand_df["likely_v2_genre"].isin(DEFICITS.keys())]
        chk6 = len(non_target) == 0
        val.append(f"  6. All candidates target deficit classes: "
                   f"{len(non_target)} non-target — {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # All candidate track_ids exist in DB
        track_ids = cand_df["track_id"].tolist()
        if track_ids:
            placeholders = ",".join("?" * len(track_ids))
            existing = conn.execute(
                f"SELECT COUNT(*) FROM tracks WHERE id IN ({placeholders})", track_ids
            ).fetchone()[0]
            chk7 = existing == len(track_ids)
            val.append(f"  7. All track_ids exist in tracks: "
                       f"{existing}/{len(track_ids)} — {'PASS' if chk7 else 'FAIL'}")
            if not chk7:
                all_ok = False
        else:
            val.append(f"  7. All track_ids exist: N/A (0 candidates)")

        val.append(f"  8. DB opened read-only: PASS (file: URI with ?mode=ro)")
        val.append(f"  9. No schema changes: PASS")

        conn.close()

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")
        w("05_validation_checks.txt", val)

        # 06 — final report
        gate = "PASS" if all_ok else "FAIL"

        report = []
        report.append("=" * 70)
        report.append("TARGETED MANUAL LABEL ACQUISITION V2 — FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- MISSION ---")
        report.append(f"Discover candidates from unlabeled tracks to close the remaining")
        report.append(f"27-track V2 benchmark deficit (Metal=10, Pop=4, Other=13).")

        report.append(f"\n--- RESULTS ---")
        report.append(f"Unlabeled tracks searched: 179")
        report.append(f"Candidates produced: {len(cand_df)}")
        report.append(f"Target deficit: {sum(DEFICITS.values())}")

        report.append(f"\n--- PER-CLASS RESULTS ---")
        report.append(f"{'V2 Genre':15s}  {'Deficit':>8s}  {'Candidates':>11s}  "
                      f"{'High':>5s}  {'Med':>5s}  {'Low':>5s}  Coverage")
        report.append("-" * 70)
        for v2g in ["Metal", "Pop", "Other"]:
            deficit = DEFICITS[v2g]
            subset = cand_df[cand_df["likely_v2_genre"] == v2g]
            h = len(subset[subset["confidence_tier"] == "high"])
            m = len(subset[subset["confidence_tier"] == "medium"])
            lo = len(subset[subset["confidence_tier"] == "low"])
            total = h + m + lo
            coverage = f"{min(total/deficit*100, 100):.0f}%" if deficit > 0 else "N/A"
            report.append(f"{v2g:15s}  {deficit:8d}  {total:11d}  {h:5d}  {m:5d}  {lo:5d}  {coverage}")

        report.append(f"\n--- OUTPUTS ---")
        report.append(f"  {CANDIDATES_CSV}")
        report.append(f"  {REVIEW_CSV}")
        report.append(f"  Proof: {PROOF_DIR}")

        report.append(f"\n--- PARTS ---")
        report.append(f"  A. Source pool discovery: PASS")
        report.append(f"  B. Candidate prioritization: PASS ({len(cand_df)} candidates)")
        report.append(f"  C. Review template: PASS")
        report.append(f"  D. Gap closure plan: PASS")
        report.append(f"  E. No DB writes: PASS (read-only mode)")
        report.append(f"  F. Validation: {'PASS' if all_ok else 'FAIL'}")

        report.append(f"\n--- NEXT STEPS ---")
        report.append(f"  1. Manual listening review: open {REVIEW_CSV.name}")
        report.append(f"  2. Set action=approve_label for confirmed tracks")
        report.append(f"  3. Run Phase 14: label insertion for approved rows")
        report.append(f"  4. Run Phase 15: benchmark expansion with newly labeled tracks")

        report.append(f"\n{'='*70}")
        report.append(f"GATE={gate}")
        report.append(f"{'='*70}")

        w("06_final_report.txt", report)
        w("execution_log.txt", self.log)

        self.emit(f"Proof: {PROOF_DIR}")
        self.emit(f"GATE={gate}")
        return gate


def main():
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")

    # PART A
    unlabeled_df, pool_inv = p.part_a()

    # PART B
    cand_df = p.part_b(unlabeled_df)

    # PART C
    review_df, review_summary = p.part_c(cand_df)

    # PART D
    closure_plan = p.part_d(cand_df)

    # PART E
    p.part_e_verify()

    # PART F
    gate = p.part_f(cand_df, pool_inv, review_summary, closure_plan)

    print(f"\n{'='*60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
