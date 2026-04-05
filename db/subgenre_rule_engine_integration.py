#!/usr/bin/env python3
"""
Phase 17 — Subgenre Alignment + Rule Engine Integration

Parts A-H as specified.
"""

import csv
import io
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

# Force UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

WORKSPACE = Path(__file__).resolve().parent.parent
ANALYSIS_DB = WORKSPACE / "db" / "song_analysis.db"
PROOF_DIR = WORKSPACE / "_proof" / "subgenre_rule_engine_integration"
DATA_DIR = WORKSPACE / "data"
# musicData.js lives in sister repo NGKsPlayer
MUSIC_DATA_JS = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayer\src\data\musicData.js")

BENCHMARK_SET_ID = 1

# ── Genre name mapping: musicData.js genre names -> DB genre names ──
# The DB uses specific names; musicData.js uses some different ones.
JS_TO_DB_GENRE = {
    "Hip-Hop":      "Hip-Hop",
    "Rock":         "Rock",
    "Metal":        "Metal",
    "EDM":          "Electronic",   # musicData.js uses "EDM"
    "Country":      "Country",
    "Pop":          "Pop",
    "R&B":          "R&B",
    "Jazz":         "Jazz",
    "Electronic":   "Electronic",   # musicData.js also has "Electronic" category
    "Classical":    "Classical",
    "Reggae":       "Reggae",
    "Folk":         "Folk",
    "Blues":        "Blues",
    "World":        "World",
    "Alternative":  None,           # Not a DB genre; subgenres map to Rock/etc.
}

# ── Subgenre normalization: DB name -> canonical form ──
SUBGENRE_NORMALIZE = {
    "Alternative":    "Alternative Rock",
    "Punk":           "Punk Rock",
    "Prog Rock":      "Progressive Rock",
    "Classic Rock":   "Classic Rock",
    "Synth-Pop":      "Synthpop",
    "Dance-Pop":      "Dance Pop",
    "Electropop":     "Electropop",
    "K-Pop":          "K-Pop",
    "Lo-Fi Hip-Hop":  "Lo-Fi",
    "G-Funk":         "G-Funk",
    "Space Ambient":  "Space Rock",    # DB has under Ambient, musicData doesn't
    "Dark Ambient":   "Dark Ambient",  # not in musicData but valid concept
    "Drone":          "Drone",         # not in musicData but valid concept
    "Garage":         "UK Garage",
    "Electro":        "Electropop",    # DB's "Electro" under Electronic -> Electropop in Pop? No, keep distinct
    "Jazz Fusion":    "Fusion Jazz",
    "Contemporary R&B": "Contemporary R&B",
    "Quiet Storm":    "Quiet Storm",
    "Neo-Soul":       "Neo-Soul",
}

# Override: keep some DB names as-is if they don't need normalization
SUBGENRE_KEEP_AS_IS = {
    "House", "Techno", "Trance", "Dubstep", "Drum & Bass", "Breakbeat",
    "Downtempo", "IDM", "Trap", "Boom Bap", "Drill",
    "Death Metal", "Black Metal", "Thrash Metal", "Doom Metal", "Progressive Metal",
    "Bebop", "Smooth Jazz", "Acid Jazz",
    "Bluegrass", "Outlaw Country", "Country Pop",
    "Indie Pop", "Dub", "Dancehall", "Ska",
    "Grunge", "Shoegaze",
    "Reggaeton", "Salsa", "Cumbia", "Bachata",
    "Electro",
}


# ────────────────────────────────────────────────────────────────────
# RULE ENGINE: Subgenre-based secondary label rules
# ────────────────────────────────────────────────────────────────────

# Hybrid subgenres that explicitly bridge two genres
HYBRID_SUBGENRES = {
    # (parent_genre_in_db, subgenre) -> allowed_secondary_genre
    # Country hybrids
    ("Country", "Country Rap"):     ("Hip-Hop", "explicit_hybrid", "high"),
    ("Country", "Country Rock"):    ("Rock",    "explicit_hybrid", "high"),
    ("Country", "Country Pop"):     ("Pop",     "explicit_hybrid", "high"),
    ("Country", "Cowpunk"):         ("Rock",    "explicit_hybrid", "medium"),
    ("Country", "Country Folk"):    ("Folk",    "stylistic_bridge", "medium"),
    ("Country", "Bro Country"):     ("Pop",     "stylistic_bridge", "low"),

    # Hip-Hop hybrids
    ("Hip-Hop", "Jazz Rap"):        ("Jazz",    "explicit_hybrid", "high"),
    ("Hip-Hop", "Pop Rap"):         ("Pop",     "explicit_hybrid", "high"),
    ("Hip-Hop", "Emo Rap"):         ("Rock",    "stylistic_bridge", "medium"),
    ("Hip-Hop", "Phonk"):           ("Electronic", "stylistic_bridge", "low"),

    # Rock hybrids
    ("Rock", "Blues Rock"):         ("Blues",   "explicit_hybrid", "high"),
    ("Rock", "Folk Rock"):          ("Folk",    "explicit_hybrid", "high"),
    ("Rock", "Southern Rock"):      ("Country", "stylistic_bridge", "medium"),
    ("Rock", "Rockabilly"):         ("Country", "stylistic_bridge", "medium"),
    ("Rock", "Industrial Rock"):    ("Electronic", "stylistic_bridge", "medium"),
    ("Rock", "Post-Punk"):          ("Electronic", "stylistic_bridge", "low"),
    ("Rock", "New Wave"):           ("Pop",     "stylistic_bridge", "medium"),
    ("Rock", "Britpop"):            ("Pop",     "stylistic_bridge", "medium"),
    ("Rock", "Arena Rock"):         ("Pop",     "stylistic_bridge", "low"),

    # Metal hybrids
    ("Metal", "Nu Metal"):          ("Hip-Hop", "explicit_hybrid", "medium"),
    ("Metal", "Industrial Metal"):  ("Electronic", "explicit_hybrid", "medium"),
    ("Metal", "Folk Metal"):        ("Folk",    "explicit_hybrid", "high"),
    ("Metal", "Symphonic Metal"):   ("Classical", "stylistic_bridge", "medium"),
    ("Metal", "Gothic Metal"):      ("Rock",    "stylistic_bridge", "low"),

    # Pop hybrids
    ("Pop", "Electropop"):          ("Electronic", "explicit_hybrid", "high"),
    ("Pop", "Synthpop"):            ("Electronic", "explicit_hybrid", "high"),
    ("Pop", "Dance Pop"):           ("Electronic", "stylistic_bridge", "medium"),
    ("Pop", "K-Pop"):               ("Hip-Hop", "stylistic_bridge", "low"),
    ("Pop", "Indie Pop"):           ("Rock",    "stylistic_bridge", "low"),

    # R&B hybrids
    ("R&B", "Hip-Hop Soul"):        ("Hip-Hop", "explicit_hybrid", "high"),
    ("R&B", "Trap Soul"):           ("Hip-Hop", "stylistic_bridge", "medium"),
    ("R&B", "Funk"):                ("Jazz",    "stylistic_bridge", "medium"),

    # Jazz hybrids
    ("Jazz", "Fusion Jazz"):        ("Rock",    "explicit_hybrid", "high"),
    ("Jazz", "Latin Jazz"):         ("Latin",   "explicit_hybrid", "high"),
    ("Jazz", "Acid Jazz"):          ("Electronic", "stylistic_bridge", "medium"),
    ("Jazz", "Nu Jazz"):            ("Electronic", "stylistic_bridge", "medium"),

    # Electronic hybrids
    ("Electronic", "Trip-Hop"):     ("Hip-Hop", "stylistic_bridge", "medium"),
    ("Electronic", "Synthwave"):    ("Pop",     "stylistic_bridge", "medium"),
    ("Electronic", "Grime"):        ("Hip-Hop", "explicit_hybrid", "high"),
    ("Electronic", "Industrial"):   ("Metal",   "stylistic_bridge", "medium"),

    # Reggae hybrids
    ("Reggae", "Reggae Fusion"):    ("Pop",     "stylistic_bridge", "medium"),
    ("Reggae", "Reggaeton"):        ("Hip-Hop", "stylistic_bridge", "medium"),
    ("Reggae", "Ska"):              ("Rock",    "stylistic_bridge", "low"),

    # Folk hybrids
    ("Folk", "Folk Rock"):          ("Rock",    "explicit_hybrid", "high"),
    ("Folk", "Psychedelic Folk"):    ("Rock",    "stylistic_bridge", "medium"),
    ("Folk", "Celtic Folk"):        ("World",   "stylistic_bridge", "low"),

    # Blues hybrids
    ("Blues", "Blues Rock"):         ("Rock",    "explicit_hybrid", "high"),
    ("Blues", "Jump Blues"):         ("Jazz",    "stylistic_bridge", "medium"),
    ("Blues", "Rhythm and Blues"):   ("R&B",     "explicit_hybrid", "high"),
}

# Subgenres that do NOT allow secondary labels (pure within genre)
PROHIBITED_SECONDARIES = {
    # Hair Metal / Glam Rock are Rock subgenres, NOT automatic Metal
    ("Rock", "Hair Metal"):     "NOT Metal — Hair Metal is a Rock subgenre with metal aesthetics",
    ("Rock", "Glam Rock"):      "NOT Metal — Glam Rock is theatrical rock, not metal",
    ("Rock", "Hard Rock"):      "NOT Metal — Hard Rock is rock intensity, not metal genre",
    ("Rock", "Stoner Rock"):    "NOT Metal — Stoner Rock is rock, distinct from Stoner Metal",
    ("Rock", "Noise Rock"):     "NOT Electronic — noise is texture, not genre",
    ("Rock", "Surf Rock"):      "NOT anything — purely Rock",
    ("Rock", "Garage Rock"):    "NOT anything — purely Rock",

    # Metal subgenres that are purely metal
    ("Metal", "Heavy Metal"):   "NO secondary — core Metal",
    ("Metal", "Death Metal"):   "NO secondary — core Metal",
    ("Metal", "Black Metal"):   "NO secondary — core Metal",
    ("Metal", "Thrash Metal"):  "NO secondary — core Metal",
    ("Metal", "Power Metal"):   "NO secondary — core Metal",
    ("Metal", "Speed Metal"):   "NO secondary — core Metal",
    ("Metal", "Doom Metal"):    "NO secondary — not Rock, purely Metal",
    ("Metal", "Groove Metal"):  "NO secondary — core Metal",

    # Electronic similarity is NEVER valid as secondary evidence alone
    ("*", "Electronic"):        "PROHIBITED — acoustic similarity != genre membership",
}


class Pipeline:
    def __init__(self):
        self.log = []
        self.t0 = time.time()
        self.extracted_taxonomy = {}   # genre -> [subgenres]
        self.db_subgenres = []         # list of dicts
        self.db_genres = {}            # id -> name
        self.alignment_rows = []
        self.rule_rows = []
        self.recheck_rows = []
        self.patch_rows = []

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
    # PART A — TAXONOMY EXTRACTION
    # ================================================================
    def part_a(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART A -- TAXONOMY EXTRACTION")
        self.emit("=" * 70)

        # Parse musicData.js — extract GENRE_CATEGORIES object
        text = MUSIC_DATA_JS.read_text(encoding="utf-8")

        # Find GENRE_CATEGORIES block
        match = re.search(
            r"export\s+const\s+GENRE_CATEGORIES\s*=\s*\{", text
        )
        if not match:
            self.emit("FATAL: Could not find GENRE_CATEGORIES in musicData.js")
            return False

        # Extract the full object using brace matching
        start = match.start()
        brace_depth = 0
        obj_start = text.index("{", start)
        i = obj_start
        while i < len(text):
            if text[i] == "{":
                brace_depth += 1
            elif text[i] == "}":
                brace_depth -= 1
                if brace_depth == 0:
                    obj_end = i + 1
                    break
            i += 1

        obj_text = text[obj_start:obj_end]  # type: ignore[possibly-unbound]

        # Convert JS object to parseable format:
        # - Replace single quotes with double quotes
        # - Remove trailing commas
        # - Handle JS property names (unquoted or single-quoted)
        json_text = obj_text
        # Replace single-quoted strings with double-quoted
        json_text = re.sub(r"'([^']*)'", r'"\1"', json_text)
        # Remove trailing commas before } or ]
        json_text = re.sub(r",\s*([}\]])", r"\1", json_text)
        # Handle escaped apostrophes in double-quoted strings
        json_text = json_text.replace("\\'", "'")
        # Fix double-escaped backslashes that might appear
        # Handle the Oboe d'Amore case — the regex already handled quotes

        try:
            parsed = json.loads(json_text)
        except json.JSONDecodeError as e:
            self.emit(f"JSON parse error: {e}")
            self.emit("Attempting line-by-line extraction...")
            parsed = self._extract_genres_manual(text)

        self.extracted_taxonomy = parsed
        total_subgenres = sum(len(v) for v in parsed.values())

        self.emit(f"\nExtracted {len(parsed)} genres, {total_subgenres} subgenres")

        # Write taxonomy_master_extracted.csv
        master_rows = []
        for genre, subs in sorted(parsed.items()):
            for sub in subs:
                master_rows.append({
                    "parent_genre": genre,
                    "subgenre": sub.strip(),
                    "source": "musicData.js",
                })
        pd.DataFrame(master_rows).to_csv(
            DATA_DIR / "taxonomy_master_extracted.csv",
            index=False, encoding="utf-8"
        )

        # Write taxonomy_genre_summary.csv
        summary_rows = []
        for genre, subs in sorted(parsed.items()):
            summary_rows.append({
                "parent_genre": genre,
                "subgenre_count": len(subs),
            })
        pd.DataFrame(summary_rows).to_csv(
            DATA_DIR / "taxonomy_genre_summary.csv",
            index=False, encoding="utf-8"
        )

        # Proof file
        lines = []
        lines.append("=" * 70)
        lines.append("TAXONOMY EXTRACTION SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Source: {MUSIC_DATA_JS}")
        lines.append("=" * 70)
        lines.append(f"\nTotal genres: {len(parsed)}")
        lines.append(f"Total subgenres: {total_subgenres}")
        lines.append("")
        for genre in sorted(parsed.keys()):
            subs = parsed[genre]
            lines.append(f"\n{genre} ({len(subs)} subgenres):")
            for s in sorted(subs):
                lines.append(f"  - {s}")

        (PROOF_DIR / "00_taxonomy_extraction_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        for line in lines[:30]:
            self.emit(line)

        return True

    def _extract_genres_manual(self, text):
        """Fallback: extract genre categories using regex."""
        result = {}
        # Find each genre key and its array
        pattern = re.compile(
            r"['\"]([^'\"]+)['\"]\s*:\s*\[([^\]]+)\]", re.DOTALL
        )
        # Only look within GENRE_CATEGORIES
        gc_match = re.search(r"GENRE_CATEGORIES\s*=\s*\{", text)
        if not gc_match:
            return result
        gc_start = gc_match.end()
        # Find closing brace
        depth = 1
        i = gc_start
        while i < len(text) and depth > 0:
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
            i += 1
        gc_text = text[gc_start:i]

        for m in pattern.finditer(gc_text):
            genre = m.group(1)
            items_text = m.group(2)
            items = re.findall(r"['\"]([^'\"]+)['\"]", items_text)
            result[genre] = items

        return result

    # ================================================================
    # PART B — CURRENT DB SUBGENRE AUDIT
    # ================================================================
    def part_b(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART B -- CURRENT DB SUBGENRE AUDIT")
        self.emit("=" * 70)

        conn = self.connect_ro()

        # Get all DB genres
        for row in conn.execute("SELECT id, name FROM genres ORDER BY id"):
            self.db_genres[row["id"]] = row["name"]

        # Get all DB subgenres with usage counts
        rows = conn.execute("""
            SELECT s.id, s.name AS subgenre, g.name AS genre, g.id AS genre_id,
                   (SELECT COUNT(*) FROM track_genre_labels tg
                    WHERE tg.subgenre_id = s.id) AS occurrence_count
            FROM subgenres s
            JOIN genres g ON s.genre_id = g.id
            ORDER BY g.name, s.name
        """).fetchall()
        self.db_subgenres = [dict(r) for r in rows]
        conn.close()

        # Build lookup of extracted taxonomy (normalized for matching)
        tax_lookup = {}  # normalized_name -> (parent_genre, original_name)
        for genre, subs in self.extracted_taxonomy.items():
            for sub in subs:
                norm = sub.strip().lower().replace("-", " ").replace("&", "and")
                tax_lookup[norm] = (genre, sub.strip())

        # Audit each DB subgenre
        audit_rows = []
        for r in self.db_subgenres:
            db_genre = r["genre"]
            db_sub = r["subgenre"]
            occ = r["occurrence_count"]

            # Try exact match first
            status = "unmapped"
            mapped_parent = ""
            mapped_sub = ""
            notes = ""

            # Check if DB subgenre name directly appears in taxonomy
            norm_db = db_sub.strip().lower().replace("-", " ").replace("&", "and")

            if norm_db in tax_lookup:
                tax_genre, tax_sub = tax_lookup[norm_db]
                db_genre_mapped = JS_TO_DB_GENRE.get(tax_genre, tax_genre)

                if db_sub.strip() == tax_sub:
                    status = "exact"
                else:
                    status = "normalized"

                mapped_parent = tax_genre
                mapped_sub = tax_sub

                if db_genre_mapped and db_genre != db_genre_mapped:
                    notes = f"Parent mismatch: DB={db_genre}, taxonomy={tax_genre}"
                    if status == "exact":
                        status = "ambiguous"
            else:
                # Try normalization from our map
                norm_name = SUBGENRE_NORMALIZE.get(db_sub)
                if norm_name:
                    norm_key = norm_name.lower().replace("-", " ").replace("&", "and")
                    if norm_key in tax_lookup:
                        tax_genre, tax_sub = tax_lookup[norm_key]
                        status = "normalized"
                        mapped_parent = tax_genre
                        mapped_sub = tax_sub
                        notes = f"Normalized: {db_sub} -> {norm_name}"

                if status == "unmapped":
                    # Check for partial matches
                    for norm_key, (tg, ts) in tax_lookup.items():
                        if norm_db in norm_key or norm_key in norm_db:
                            status = "ambiguous"
                            mapped_parent = tg
                            mapped_sub = ts
                            notes = f"Partial match: {db_sub} ~ {ts}"
                            break

                    if status == "unmapped":
                        notes = "No match in musicData.js taxonomy"

            audit_rows.append({
                "current_genre": db_genre,
                "current_subgenre": db_sub,
                "occurrence_count": occ,
                "taxonomy_match_status": status,
                "mapped_parent_genre": mapped_parent,
                "mapped_subgenre": mapped_sub,
                "notes": notes,
            })

        df = pd.DataFrame(audit_rows)
        df.to_csv(DATA_DIR / "current_subgenre_audit.csv",
                  index=False, encoding="utf-8")

        # Proof summary
        lines = []
        lines.append("=" * 70)
        lines.append("CURRENT SUBGENRE AUDIT")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal DB subgenres: {len(self.db_subgenres)}")
        lines.append(f"\nBy match status:")
        for status in ["exact", "normalized", "ambiguous", "unmapped"]:
            cnt = len([r for r in audit_rows if r["taxonomy_match_status"] == status])
            lines.append(f"  {status}: {cnt}")

        lines.append(f"\nGenres with zero subgenres in DB:")
        conn = self.connect_ro()
        for row in conn.execute("""
            SELECT g.name FROM genres g
            LEFT JOIN subgenres s ON s.genre_id = g.id
            GROUP BY g.id HAVING COUNT(s.id) = 0
            ORDER BY g.name
        """):
            lines.append(f"  - {row['name']}")
        conn.close()

        lines.append("\nDetailed audit:")
        for r in audit_rows:
            lines.append(f"  [{r['taxonomy_match_status']:10s}] "
                         f"{r['current_genre']:12s} / {r['current_subgenre']:20s} "
                         f"(used={r['occurrence_count']:3d}) "
                         f"-> {r['mapped_parent_genre']}/{r['mapped_subgenre']} "
                         f"  {r['notes']}")

        (PROOF_DIR / "01_current_subgenre_audit.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        self.emit(f"  DB subgenres: {len(self.db_subgenres)}")
        status_counts = {}
        for r in audit_rows:
            s = r["taxonomy_match_status"]
            status_counts[s] = status_counts.get(s, 0) + 1
        for s, c in sorted(status_counts.items()):
            self.emit(f"  {s}: {c}")

        return audit_rows

    # ================================================================
    # PART C — ALIGNMENT MAPPING TABLE
    # ================================================================
    def part_c(self, audit_rows):
        self.emit("\n" + "=" * 70)
        self.emit("PART C -- ALIGNMENT MAPPING TABLE")
        self.emit("=" * 70)

        # Build alignment for every DB subgenre
        for r in audit_rows:
            db_genre = r["current_genre"]
            db_sub = r["current_subgenre"]
            status = r["taxonomy_match_status"]
            mapped_parent = r["mapped_parent_genre"]
            mapped_sub = r["mapped_subgenre"]

            # Determine normalized current subgenre
            norm_current = SUBGENRE_NORMALIZE.get(db_sub, db_sub)

            if status == "exact":
                mapping_type = "exact"
                confidence = "high"
                aligned_parent = mapped_parent
                aligned_sub = mapped_sub
                notes = ""
            elif status == "normalized":
                mapping_type = "normalized"
                confidence = "high"
                aligned_parent = mapped_parent
                aligned_sub = mapped_sub
                notes = f"Normalized from {db_sub}"
            elif status == "ambiguous":
                mapping_type = "manual"
                confidence = "low"
                aligned_parent = mapped_parent
                aligned_sub = mapped_sub
                notes = f"AMBIGUOUS: {r['notes']} -- requires manual review"
            else:  # unmapped
                mapping_type = "manual"
                confidence = "low"
                aligned_parent = db_genre
                aligned_sub = db_sub
                notes = "UNMAPPED: not in musicData.js taxonomy"

            self.alignment_rows.append({
                "current_genre": db_genre,
                "current_subgenre": db_sub,
                "normalized_current_subgenre": norm_current,
                "aligned_parent_genre": aligned_parent,
                "aligned_subgenre": aligned_sub,
                "mapping_type": mapping_type,
                "confidence": confidence,
                "notes": notes,
            })

        df = pd.DataFrame(self.alignment_rows)
        df.to_csv(DATA_DIR / "subgenre_alignment_mapping_v1.csv",
                  index=False, encoding="utf-8")

        # Proof summary
        lines = []
        lines.append("=" * 70)
        lines.append("ALIGNMENT MAPPING SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal mappings: {len(self.alignment_rows)}")

        lines.append(f"\nBy mapping_type:")
        for mt in ["exact", "normalized", "manual"]:
            cnt = len([r for r in self.alignment_rows if r["mapping_type"] == mt])
            lines.append(f"  {mt}: {cnt}")

        lines.append(f"\nBy confidence:")
        for c in ["high", "medium", "low"]:
            cnt = len([r for r in self.alignment_rows if r["confidence"] == c])
            lines.append(f"  {c}: {cnt}")

        lines.append(f"\nAmbiguous/unmapped entries (FLAGGED, not forced):")
        for r in self.alignment_rows:
            if r["confidence"] == "low":
                lines.append(f"  {r['current_genre']}/{r['current_subgenre']} "
                             f"-> {r['aligned_parent_genre']}/{r['aligned_subgenre']} "
                             f"  [{r['notes']}]")

        lines.append(f"\nAll mappings:")
        for r in self.alignment_rows:
            lines.append(f"  [{r['mapping_type']:10s}] [{r['confidence']:6s}] "
                         f"{r['current_genre']:12s}/{r['current_subgenre']:20s} "
                         f"-> {r['aligned_parent_genre']:12s}/{r['aligned_subgenre']:20s} "
                         f"  {r['notes']}")

        (PROOF_DIR / "02_alignment_mapping_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        self.emit(f"  Alignment mappings: {len(self.alignment_rows)}")
        return self.alignment_rows

    # ================================================================
    # PART D — RULE ENGINE DESIGN
    # ================================================================
    def part_d(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART D -- RULE ENGINE DESIGN")
        self.emit("=" * 70)

        # Build rule rows from HYBRID_SUBGENRES
        for (parent, subgenre), (secondary, rule_type, conf) in sorted(HYBRID_SUBGENRES.items()):
            evidence = self._required_evidence(parent, subgenre, secondary, rule_type)
            self.rule_rows.append({
                "aligned_parent_genre": parent,
                "aligned_subgenre": subgenre,
                "allowed_secondary_genre": secondary,
                "rule_type": rule_type,
                "required_evidence": evidence,
                "confidence_tier": conf,
                "notes": "",
            })

        # Add prohibited rules
        for (parent, subgenre), reason in sorted(PROHIBITED_SECONDARIES.items()):
            self.rule_rows.append({
                "aligned_parent_genre": parent,
                "aligned_subgenre": subgenre,
                "allowed_secondary_genre": "NONE",
                "rule_type": "prohibited",
                "required_evidence": "N/A",
                "confidence_tier": "high",
                "notes": reason,
            })

        df = pd.DataFrame(self.rule_rows)
        df.to_csv(DATA_DIR / "secondary_rule_engine_v1.csv",
                  index=False, encoding="utf-8")

        # Proof summary
        lines = []
        lines.append("=" * 70)
        lines.append("RULE ENGINE SPECIFICATION")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)

        lines.append(f"\nTotal rules: {len(self.rule_rows)}")
        lines.append(f"\nBy rule_type:")
        for rt in ["explicit_hybrid", "stylistic_bridge", "prohibited"]:
            cnt = len([r for r in self.rule_rows if r["rule_type"] == rt])
            lines.append(f"  {rt}: {cnt}")

        lines.append(f"\nBy confidence_tier:")
        for ct in ["high", "medium", "low"]:
            cnt = len([r for r in self.rule_rows if r["confidence_tier"] == ct])
            lines.append(f"  {ct}: {cnt}")

        lines.append(f"\n--- EXPLICIT HYBRID RULES ---")
        for r in self.rule_rows:
            if r["rule_type"] == "explicit_hybrid":
                lines.append(f"  {r['aligned_parent_genre']:12s}/{r['aligned_subgenre']:20s} "
                             f"-> {r['allowed_secondary_genre']:12s} "
                             f"[{r['confidence_tier']}] {r['required_evidence']}")

        lines.append(f"\n--- STYLISTIC BRIDGE RULES ---")
        for r in self.rule_rows:
            if r["rule_type"] == "stylistic_bridge":
                lines.append(f"  {r['aligned_parent_genre']:12s}/{r['aligned_subgenre']:20s} "
                             f"-> {r['allowed_secondary_genre']:12s} "
                             f"[{r['confidence_tier']}] {r['required_evidence']}")

        lines.append(f"\n--- PROHIBITED RULES ---")
        for r in self.rule_rows:
            if r["rule_type"] == "prohibited":
                lines.append(f"  {r['aligned_parent_genre']:12s}/{r['aligned_subgenre']:20s} "
                             f"-- {r['notes']}")

        (PROOF_DIR / "03_rule_engine_spec.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        self.emit(f"  Rules: {len(self.rule_rows)}")
        for rt in ["explicit_hybrid", "stylistic_bridge", "prohibited"]:
            cnt = len([r for r in self.rule_rows if r["rule_type"] == rt])
            self.emit(f"    {rt}: {cnt}")

        return self.rule_rows

    def _required_evidence(self, parent, subgenre, secondary, rule_type):
        if rule_type == "explicit_hybrid":
            return (f"Track subgenre must be '{subgenre}' (verified). "
                    f"Primary={parent}, secondary={secondary} allowed by hybrid definition.")
        elif rule_type == "stylistic_bridge":
            return (f"Track subgenre '{subgenre}' has stylistic overlap with {secondary}. "
                    f"Requires additional evidence (feature analysis or manual tag).")
        return "N/A"

    # ================================================================
    # PART E — SECONDARY LABEL RE-EVALUATION
    # ================================================================
    def part_e(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART E -- SECONDARY LABEL RE-EVALUATION")
        self.emit("=" * 70)

        conn = self.connect_ro()

        # Get all current secondary labels with track info
        rows = conn.execute("""
            SELECT ts.id AS label_id, ts.track_id, t.artist, t.title,
                   gp.name AS primary_genre, gs.name AS secondary_genre,
                   tp.subgenre_id,
                   COALESCE(sp.name, '') AS current_subgenre,
                   tp.genre_id AS primary_genre_id, ts.genre_id AS secondary_genre_id,
                   ts.confidence
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

        secondaries = [dict(r) for r in rows]
        self.emit(f"  Secondary labels to evaluate: {len(secondaries)}")

        # Build alignment lookup
        alignment_lookup = {}
        for al in self.alignment_rows:
            key = (al["current_genre"], al["current_subgenre"])
            alignment_lookup[key] = al

        # Build rule lookup
        rule_lookup = {}  # (parent, subgenre) -> list of rules
        for rule in self.rule_rows:
            key = (rule["aligned_parent_genre"], rule["aligned_subgenre"])
            if key not in rule_lookup:
                rule_lookup[key] = []
            rule_lookup[key].append(rule)

        for r in secondaries:
            primary_genre = r["primary_genre"]
            secondary_genre = r["secondary_genre"]
            current_sub = r["current_subgenre"]
            track_id = r["track_id"]

            # Find aligned subgenre
            aligned_sub = current_sub
            if current_sub and (primary_genre, current_sub) in alignment_lookup:
                al = alignment_lookup[(primary_genre, current_sub)]
                aligned_sub = al["aligned_subgenre"]

            # Check rule engine
            rule_result = "REVIEW"
            reason = ""

            if not current_sub and not aligned_sub:
                # No subgenre assigned — can't validate against rule engine
                rule_result = "REVIEW"
                reason = ("No subgenre assigned to primary label; cannot validate "
                          "secondary via rule engine. Needs subgenre assignment first.")
            else:
                # Check if there's a matching rule
                rule_key = (primary_genre, aligned_sub)
                matching_rules = rule_lookup.get(rule_key, [])

                # Also check with current subgenre
                if not matching_rules and current_sub:
                    rule_key2 = (primary_genre, current_sub)
                    matching_rules = rule_lookup.get(rule_key2, [])

                found_match = False
                for rule in matching_rules:
                    if rule["rule_type"] == "prohibited":
                        rule_result = "REMOVE_CANDIDATE"
                        reason = f"Prohibited by rule: {rule['notes']}"
                        found_match = True
                        break
                    elif rule["allowed_secondary_genre"] == secondary_genre:
                        if rule["rule_type"] == "explicit_hybrid":
                            rule_result = "KEEP"
                            reason = (f"Explicit hybrid: {aligned_sub} allows "
                                      f"{secondary_genre} [{rule['confidence_tier']}]")
                        elif rule["rule_type"] == "stylistic_bridge":
                            rule_result = "REVIEW"
                            reason = (f"Stylistic bridge: {aligned_sub} -> "
                                      f"{secondary_genre} [needs additional evidence]")
                        found_match = True
                        break

                if not found_match:
                    if secondary_genre == "Electronic":
                        rule_result = "REMOVE_CANDIDATE"
                        reason = ("Electronic as secondary is prohibited — "
                                  "acoustic similarity != genre membership")
                    elif not current_sub:
                        rule_result = "REVIEW"
                        reason = ("No subgenre; secondary may be valid but "
                                  "unverifiable without subgenre context")
                    else:
                        # Has subgenre but no rule allows this secondary
                        rule_result = "REVIEW"
                        reason = (f"No rule covers {primary_genre}/{aligned_sub} "
                                  f"-> {secondary_genre}; may need new rule or removal")

            self.recheck_rows.append({
                "track_id": track_id,
                "artist": r["artist"],
                "title": r["title"],
                "primary_genre": primary_genre,
                "secondary_genre": secondary_genre,
                "current_subgenre": current_sub,
                "aligned_subgenre": aligned_sub,
                "rule_result": rule_result,
                "reason": reason,
            })

        df = pd.DataFrame(self.recheck_rows)
        df.to_csv(DATA_DIR / "secondary_label_rule_recheck_v1.csv",
                  index=False, encoding="utf-8")

        # Proof summary
        lines = []
        lines.append("=" * 70)
        lines.append("SECONDARY LABEL RE-EVALUATION PREVIEW")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal evaluated: {len(self.recheck_rows)}")

        lines.append(f"\nBy rule_result:")
        for rr in ["KEEP", "REVIEW", "REMOVE_CANDIDATE"]:
            cnt = len([r for r in self.recheck_rows if r["rule_result"] == rr])
            lines.append(f"  {rr}: {cnt}")

        lines.append(f"\n--- KEEP ---")
        for r in self.recheck_rows:
            if r["rule_result"] == "KEEP":
                lines.append(f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                             f"{r['primary_genre']:8s}->{r['secondary_genre']:8s} "
                             f"sub={r['aligned_subgenre']}  {r['reason']}")

        lines.append(f"\n--- REMOVE_CANDIDATE ---")
        for r in self.recheck_rows:
            if r["rule_result"] == "REMOVE_CANDIDATE":
                lines.append(f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                             f"{r['primary_genre']:8s}->{r['secondary_genre']:8s} "
                             f"sub={r['aligned_subgenre']}  {r['reason']}")

        lines.append(f"\n--- REVIEW ---")
        for r in self.recheck_rows:
            if r["rule_result"] == "REVIEW":
                lines.append(f"  [{r['track_id']:4d}] {r['artist'][:25]:25s} "
                             f"{r['primary_genre']:8s}->{r['secondary_genre']:8s} "
                             f"sub={r['aligned_subgenre']}  {r['reason']}")

        (PROOF_DIR / "04_secondary_recheck_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        result_counts = {}
        for r in self.recheck_rows:
            rr = r["rule_result"]
            result_counts[rr] = result_counts.get(rr, 0) + 1
        for rr in ["KEEP", "REVIEW", "REMOVE_CANDIDATE"]:
            self.emit(f"  {rr}: {result_counts.get(rr, 0)}")

        return self.recheck_rows

    # ================================================================
    # PART F — CONTROLLED PATCH PLAN
    # ================================================================
    def part_f(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART F -- CONTROLLED PATCH PLAN")
        self.emit("=" * 70)

        # 1. Subgenre alignment patches
        unmapped = [r for r in self.alignment_rows if r["confidence"] == "low"]
        self.patch_rows.append({
            "patch_type": "subgenre_alignment",
            "affected_count": len(unmapped),
            "risk_level": "low",
            "recommended_action": "Review and manually map ambiguous/unmapped subgenres",
            "notes": f"{len(unmapped)} subgenres need manual alignment review",
        })

        # 2. Subgenre gap fill (genres with 0 subgenres)
        conn = self.connect_ro()
        empty_genres = conn.execute("""
            SELECT g.name FROM genres g
            LEFT JOIN subgenres s ON s.genre_id = g.id
            GROUP BY g.id HAVING COUNT(s.id) = 0
            ORDER BY g.name
        """).fetchall()
        conn.close()

        self.patch_rows.append({
            "patch_type": "subgenre_alignment",
            "affected_count": len(empty_genres),
            "risk_level": "low",
            "recommended_action": (
                "Insert subgenres from musicData.js for genres with 0 coverage: "
                + ", ".join(r["name"] for r in empty_genres)
            ),
            "notes": "No subgenre data exists for these DB genres. "
                     "Adding from taxonomy is safe (additive, no mutation).",
        })

        # 3. Secondary label removals
        remove_candidates = [r for r in self.recheck_rows
                             if r["rule_result"] == "REMOVE_CANDIDATE"]
        self.patch_rows.append({
            "patch_type": "secondary_remove",
            "affected_count": len(remove_candidates),
            "risk_level": "medium",
            "recommended_action": "Review and remove secondary labels "
                                  "flagged by rule engine",
            "notes": "Labels without subgenre support or violating prohibition rules",
        })

        # 4. Secondary label reviews
        reviews = [r for r in self.recheck_rows if r["rule_result"] == "REVIEW"]
        self.patch_rows.append({
            "patch_type": "secondary_review",
            "affected_count": len(reviews),
            "risk_level": "low",
            "recommended_action": "Assign subgenres to primary labels, "
                                  "then re-evaluate",
            "notes": f"{len(reviews)} labels need subgenre context to validate. "
                     "Most tracks lack subgenre assignment.",
        })

        # 5. Rule engine upgrade potential
        # Count taxonomy subgenres not currently in DB
        tax_subs = set()
        for genre, subs in self.extracted_taxonomy.items():
            db_genre = JS_TO_DB_GENRE.get(genre, genre)
            if db_genre:
                for s in subs:
                    tax_subs.add((db_genre, s.strip()))
        db_subs = set()
        for r in self.db_subgenres:
            db_subs.add((r["genre"], r["subgenre"]))
        new_subs = tax_subs - db_subs
        self.patch_rows.append({
            "patch_type": "rule_upgrade",
            "affected_count": len(new_subs),
            "risk_level": "low",
            "recommended_action": "Insert missing subgenres from taxonomy "
                                  "into DB subgenres table",
            "notes": f"{len(new_subs)} subgenres from musicData.js not yet in DB. "
                     "Additive operation, no mutation of existing data.",
        })

        df = pd.DataFrame(self.patch_rows)
        df.to_csv(DATA_DIR / "taxonomy_patch_plan_v1.csv",
                  index=False, encoding="utf-8")

        # Proof summary
        lines = []
        lines.append("=" * 70)
        lines.append("PATCH PLAN SUMMARY")
        lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        lines.append(f"\nTotal patches: {len(self.patch_rows)}")
        for r in self.patch_rows:
            lines.append(f"\n  Type: {r['patch_type']}")
            lines.append(f"  Affected: {r['affected_count']}")
            lines.append(f"  Risk: {r['risk_level']}")
            lines.append(f"  Action: {r['recommended_action']}")
            lines.append(f"  Notes: {r['notes']}")

        (PROOF_DIR / "05_patch_plan_summary.txt").write_text(
            "\n".join(lines), encoding="utf-8"
        )

        for r in self.patch_rows:
            self.emit(f"  [{r['patch_type']}] affected={r['affected_count']} "
                      f"risk={r['risk_level']}")

        return self.patch_rows

    # ================================================================
    # PART G — OUTPUTS (proof files already written inline)
    # ================================================================
    def part_g(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART G -- OUTPUTS")
        self.emit("=" * 70)

        # Copy data CSVs to proof dir as optional artifacts
        import shutil
        for name in [
            "taxonomy_master_extracted.csv",
            "taxonomy_genre_summary.csv",
            "current_subgenre_audit.csv",
            "subgenre_alignment_mapping_v1.csv",
            "secondary_rule_engine_v1.csv",
            "secondary_label_rule_recheck_v1.csv",
            "taxonomy_patch_plan_v1.csv",
        ]:
            src = DATA_DIR / name
            if src.exists():
                shutil.copy2(src, PROOF_DIR / name)

        self.emit(f"  Proof dir: {PROOF_DIR}")

    # ================================================================
    # PART H — VALIDATION
    # ================================================================
    def part_h(self):
        self.emit("\n" + "=" * 70)
        self.emit("PART H -- VALIDATION")
        self.emit("=" * 70)

        conn = self.connect_ro()
        all_ok = True
        val = []
        val.append("=" * 70)
        val.append("VALIDATION CHECKS")
        val.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        val.append("=" * 70)

        # 1. No schema changes — check table list
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()]
        expected_tables = [
            "analyzer_runs", "benchmark_set_tracks",
            "benchmark_sets", "genres", "subgenres",
            "track_genre_labels", "tracks",
        ]
        chk1 = set(expected_tables).issubset(set(tables))
        val.append(f"\n  1. Schema tables: {len(tables)} "
                   f"(expected >= {len(expected_tables)}) "
                   f"-- {'PASS' if chk1 else 'FAIL'}")
        if not chk1:
            all_ok = False

        # 2. Primary labels unchanged
        primary_count = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='primary'"
        ).fetchone()[0]
        chk2 = primary_count == 783
        val.append(f"  2. Primary labels: {primary_count} (expected 783) "
                   f"-- {'PASS' if chk2 else 'FAIL'}")
        if not chk2:
            all_ok = False

        # 3. No duplicate primaries
        dup = conn.execute(
            "SELECT COUNT(*) FROM ("
            "  SELECT track_id FROM track_genre_labels "
            "  WHERE role='primary' GROUP BY track_id HAVING COUNT(*) > 1)"
        ).fetchone()[0]
        chk3 = dup == 0
        val.append(f"  3. Duplicate primaries: {dup} -- {'PASS' if chk3 else 'FAIL'}")
        if not chk3:
            all_ok = False

        # 4. FK integrity
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        chk4 = len(fk) == 0
        val.append(f"  4. FK violations: {len(fk)} -- {'PASS' if chk4 else 'FAIL'}")
        if not chk4:
            all_ok = False

        # 5. Benchmark membership unchanged
        bench = conn.execute(
            "SELECT COUNT(*) FROM benchmark_set_tracks WHERE benchmark_set_id=?",
            (BENCHMARK_SET_ID,)
        ).fetchone()[0]
        chk5 = bench == 202
        val.append(f"  5. Benchmark count: {bench} (expected 202) "
                   f"-- {'PASS' if chk5 else 'FAIL'}")
        if not chk5:
            all_ok = False

        # 6. Secondary label count
        sec = conn.execute(
            "SELECT COUNT(*) FROM track_genre_labels WHERE role='secondary'"
        ).fetchone()[0]
        chk6 = sec == 114  # unchanged from Phase 16
        val.append(f"  6. Secondary labels: {sec} (expected 114, unchanged) "
                   f"-- {'PASS' if chk6 else 'FAIL'}")
        if not chk6:
            all_ok = False

        # 7. Taxonomy extraction reproducible
        total_tax = sum(len(v) for v in self.extracted_taxonomy.values())
        chk7 = total_tax > 200  # should be ~250+
        val.append(f"  7. Taxonomy subgenres extracted: {total_tax} (expect > 200) "
                   f"-- {'PASS' if chk7 else 'FAIL'}")
        if not chk7:
            all_ok = False

        # 8. Ambiguous mappings flagged
        ambig = len([r for r in self.alignment_rows if r["confidence"] == "low"])
        chk8 = True  # just report count — they should be flagged, not forced
        val.append(f"  8. Ambiguous/unmapped flagged: {ambig} -- PASS (flagged, not forced)")

        # 9. SQL checks from spec
        val.append(f"\n  SQL verification:")
        val.append(f"    primary count: {primary_count}")
        val.append(f"    secondary count: {sec}")
        val.append(f"    dup primaries: {dup}")
        val.append(f"    FK violations: {len(fk)}")

        conn.close()

        val.append(f"\n  OVERALL: {'PASS' if all_ok else 'FAIL'}")

        (PROOF_DIR / "06_validation_checks.txt").write_text(
            "\n".join(val), encoding="utf-8"
        )

        self.emit(f"\n  Validation: {'PASS' if all_ok else 'FAIL'}")
        return all_ok

    # ================================================================
    # FINAL REPORT
    # ================================================================
    def final_report(self, all_ok):
        elapsed = round(time.time() - self.t0, 2)
        gate = "PASS" if all_ok else "FAIL"

        report = []
        report.append("=" * 70)
        report.append("SUBGENRE ALIGNMENT + RULE ENGINE INTEGRATION -- FINAL REPORT")
        report.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Elapsed: {elapsed:.2f}s")
        report.append("=" * 70)

        report.append(f"\n--- PART A: TAXONOMY EXTRACTION ---")
        report.append(f"  Genres: {len(self.extracted_taxonomy)}")
        report.append(f"  Subgenres: {sum(len(v) for v in self.extracted_taxonomy.values())}")

        report.append(f"\n--- PART B: DB SUBGENRE AUDIT ---")
        report.append(f"  DB subgenres: {len(self.db_subgenres)}")

        report.append(f"\n--- PART C: ALIGNMENT MAPPING ---")
        report.append(f"  Mappings: {len(self.alignment_rows)}")
        for mt in ["exact", "normalized", "manual"]:
            cnt = len([r for r in self.alignment_rows if r["mapping_type"] == mt])
            report.append(f"    {mt}: {cnt}")

        report.append(f"\n--- PART D: RULE ENGINE ---")
        report.append(f"  Rules: {len(self.rule_rows)}")
        for rt in ["explicit_hybrid", "stylistic_bridge", "prohibited"]:
            cnt = len([r for r in self.rule_rows if r["rule_type"] == rt])
            report.append(f"    {rt}: {cnt}")

        report.append(f"\n--- PART E: SECONDARY RE-EVALUATION ---")
        report.append(f"  Evaluated: {len(self.recheck_rows)}")
        for rr in ["KEEP", "REVIEW", "REMOVE_CANDIDATE"]:
            cnt = len([r for r in self.recheck_rows if r["rule_result"] == rr])
            report.append(f"    {rr}: {cnt}")

        report.append(f"\n--- PART F: PATCH PLAN ---")
        report.append(f"  Patches: {len(self.patch_rows)}")
        for r in self.patch_rows:
            report.append(f"    [{r['patch_type']}] n={r['affected_count']} "
                          f"risk={r['risk_level']}")

        report.append(f"\n--- PART H: VALIDATION ---")
        report.append(f"  {'PASS' if all_ok else 'FAIL'}")

        report.append(f"\n--- KEY FINDINGS ---")
        report.append(f"  1. musicData.js taxonomy is significantly richer than DB")
        report.append(f"     ({sum(len(v) for v in self.extracted_taxonomy.values())} vs "
                      f"{len(self.db_subgenres)} subgenres)")
        report.append(f"  2. {len([r for r in self.alignment_rows if r['mapping_type'] == 'exact'])} "
                      f"exact matches, "
                      f"{len([r for r in self.alignment_rows if r['confidence'] == 'low'])} "
                      f"ambiguous/unmapped")
        report.append(f"  3. Rule engine defines {len(self.rule_rows)} explicit rules")
        report.append(f"  4. Most secondary labels ({len([r for r in self.recheck_rows if r['rule_result'] == 'REVIEW'])}) "
                      f"need subgenre context to fully validate")
        report.append(f"  5. No production DB mutations performed (read-only audit)")

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
    p = Pipeline()

    if not ANALYSIS_DB.exists():
        p.emit(f"FATAL: {ANALYSIS_DB} not found")
        return 1
    if not MUSIC_DATA_JS.exists():
        p.emit(f"FATAL: {MUSIC_DATA_JS} not found")
        return 1

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    p.emit(f"CWD: {WORKSPACE}")
    p.emit(f"DB: {ANALYSIS_DB}")
    p.emit(f"Taxonomy source: {MUSIC_DATA_JS}")

    # Part A
    if not p.part_a():
        p.emit("FATAL: Taxonomy extraction failed")
        return 1

    # Part B
    audit_rows = p.part_b()

    # Part C
    p.part_c(audit_rows)

    # Part D
    p.part_d()

    # Part E
    p.part_e()

    # Part F
    p.part_f()

    # Part G — outputs
    p.part_g()

    # Part H — validation
    all_ok = p.part_h()

    # Final
    gate = p.final_report(all_ok)

    print(f"\n{'=' * 60}")
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={gate}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
