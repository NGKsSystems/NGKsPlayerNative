#!/usr/bin/env python3
"""
Phase 16.5 — Aggressive Junk Filter + Auto-Clean Layer
=======================================================
READ-ONLY: No filesystem mutations. CSV updates only.

Parts:
  A — Load input (fix_required_v2.csv → working copy)
  B — Junk pattern library
  C — Clean transform (strip junk, normalize, fix dashes)
  D — Reclassification logic (RENAME / VERIFY / JUNK)
  E — Output files (fix_required_v3.csv, junk_candidates_v2.csv, reduction_summary)
  F — Safety validation (7 checks)
  G — Reporting (proof artifacts)
"""

import csv
import os
import re
import shutil
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from collections import Counter

# ── Config ──────────────────────────────────────────────────────────
BASE        = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA        = BASE / "data"
PROOF_DIR   = BASE / "_proof" / "library_normalization_phase16_5"
READY_NORM  = Path(r"C:\Users\suppo\Downloads\New Music\READY_NORMALIZED")
INPUT_CSV   = DATA / "fix_required_v2.csv"
WORKING_CSV = DATA / "fix_required_phase16_5_working.csv"
OUTPUT_V3   = DATA / "fix_required_v3.csv"
JUNK_V2     = DATA / "junk_candidates_v2.csv"
JUNK_V1     = DATA / "junk_candidates_v1.csv"
SUMMARY_CSV = DATA / "phase16_5_reduction_summary.csv"

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ═══════════════════════════════════════════════════════════════════
# PART B — JUNK PATTERN LIBRARY
# ═══════════════════════════════════════════════════════════════════

# Patterns to STRIP from filenames (case-insensitive, whole-word or bounded)
STRIP_PATTERNS_EXACT = [
    # Label / source noise
    r"\bNapalm\s+Records\b",
    r"\bOfficial\s+4K\s+Video\b",
    r"\bOfficial\s+Lyric\s+Video\b",
    r"\bOfficial\s+Music\s+Video\b",
    r"\bOfficial\s+Video\b",
    r"\bOfficial\s+Audio\b",
    r"\bOfficial\b",
    r"\bLyric\s+Video\b",
    r"\bLyrics?\s+Video\b",
    r"\bLyrics?\s+On\s+Screen\b",
    r"\bwith\s+lyrics\b",
    r"\bLyrics?!?\b",
    r"\b[Hh][Dd]\b(?!\w)",   # HD but not part of a word
    r"\bHQ\b",
    r"\bFull\s+Album\b",
    r"\bRemastered\s+Version\b",
    r"\bRemaster(?:ed)?\b",
    r"\bCopyright\s+Free\b",
    r"\bNCS\b",
    # Video resolution noise
    r"\b4K\b",
    r"\b1080p\b",
    r"\b720p\b",
]

# Patterns that indicate NON-TRACK / JUNK (for reclassification)
JUNK_INDICATORS = [
    r"\bGolden\s+Era\b",
    r"\bGreatest\s+Hits\b",
    r"\bHits\s+Playlist\b",
    r"\bMegamix\b",
    r"\bMix\s+20\d{2}\b",       # "Mix 2021"
    r"\bNonstop\s+Mix\b",
    r"\bCompilation\b",
    r"\bPlaylist\b",
    r"\bCollection\b",
    r"\bFull\s+Album\b",
    r"\bBest\s+of\b",
    r"\bTop\s+\d+\b",
    r"\bLegends\s+Collection\b",
    r"\bSoul\s+Legends\b",
]

# Unicode junk characters to strip
UNICODE_JUNK = [
    "\uff5c",   # ｜ fullwidth vertical bar
    "\uff02",   # ＂ fullwidth quotation mark
    "\uff1f",   # ？ fullwidth question mark
    "\u29f8",   # ⧸  big solidus
    "\u00bd",   # ½  broken fraction
    "\ufffd",   # � replacement character
    "\u2019",   # ' smart apostrophe → regular
]

# Emoji regex (broad coverage)
EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F9FF"   # misc symbols, emoticons, etc.
    "\U00002702-\U000027B0"
    "\U0000FE00-\U0000FE0F"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "]+", flags=re.UNICODE
)


def compile_strip_patterns():
    return [(re.compile(p, re.IGNORECASE), p) for p in STRIP_PATTERNS_EXACT]

def compile_junk_indicators():
    return [(re.compile(p, re.IGNORECASE), p) for p in JUNK_INDICATORS]

COMPILED_STRIP = compile_strip_patterns()
COMPILED_JUNK  = compile_junk_indicators()


# ═══════════════════════════════════════════════════════════════════
# PART C — CLEAN TRANSFORM
# ═══════════════════════════════════════════════════════════════════

def strip_extension(name):
    """Remove file extension."""
    for ext in [".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac", ".wmv.mp3", ".mmp3"]:
        if name.lower().endswith(ext):
            return name[:-len(ext)], ext
    base, ext = os.path.splitext(name)
    return base, ext

def clean_name(raw_name):
    """
    Apply all junk-stripping transforms to a filename.
    Returns (cleaned_name_with_ext, list_of_changes_applied).
    """
    base, ext = strip_extension(raw_name)
    if ext.lower() in (".mmp3",):
        ext = ".mp3"  # fix common typo

    changes = []
    original_base = base

    # 1. Strip emoji
    if EMOJI_RE.search(base):
        base = EMOJI_RE.sub("", base)
        changes.append("stripped_emoji")

    # 2. Strip unicode junk chars
    for ch in UNICODE_JUNK:
        if ch in base:
            if ch == "\u2019":
                base = base.replace(ch, "'")
                changes.append("smart_apostrophe_fixed")
            else:
                base = base.replace(ch, "")
                changes.append(f"stripped_unicode_{hex(ord(ch))}")

    # 3. Strip junk patterns
    for pattern, pat_str in COMPILED_STRIP:
        if pattern.search(base):
            base = pattern.sub("", base)
            changes.append(f"stripped:{pat_str[:40]}")

    # 4. Strip common parenthetical noise
    # e.g., "(with lyrics)", "(Official Video)", "(Remastered)"
    paren_noise = re.compile(
        r"\(\s*(?:with\s+lyrics|official(?:\s+\w+)*\s*video|remaster(?:ed)?|"
        r"lyric\s+video|audio|hd|hq|4k|full\s+version|copyright\s+free)\s*\)",
        re.IGNORECASE
    )
    if paren_noise.search(base):
        base = paren_noise.sub("", base)
        changes.append("stripped_paren_noise")

    # 5. Fix dash spacing: "Artist- Title" → "Artist - Title"
    #    Also: "Artist -Title", "Artist-Title" (if clear split)
    dash_fix = re.compile(r"(\w)\s*[-–—]\s*(\w)")
    if "- " not in base and " -" not in base:
        # Only fix if there's no proper " - " already
        if dash_fix.search(base):
            base = dash_fix.sub(r"\1 - \2", base, count=1)
            changes.append("fixed_dash_spacing")
    elif re.search(r"\w-\s", base):
        base = re.sub(r"(\w)-\s", r"\1 - ", base)
        changes.append("fixed_dash_spacing")
    elif re.search(r"\s-\w", base):
        base = re.sub(r"\s-(\w)", r" - \1", base)
        changes.append("fixed_dash_spacing")

    # 6. Strip trailing/leading junk separators and whitespace
    base = re.sub(r"[\s|~:;,]+$", "", base)
    base = re.sub(r"^[\s|~:;,]+", "", base)
    # Strip trailing " -" or "- " left over
    base = re.sub(r"\s*[-–—]\s*$", "", base)
    base = re.sub(r"^\s*[-–—]\s*", "", base)

    # 7. Collapse multiple spaces
    base = re.sub(r"\s{2,}", " ", base).strip()

    # 8. Strip leading/trailing parentheses left empty
    base = re.sub(r"\(\s*\)", "", base).strip()

    # 9. Strip leading @ handles (YouTube channel noise)
    base = re.sub(r"\s*@\w+", "", base).strip()

    if base != original_base:
        if "fixed_dash_spacing" not in changes and not any("stripped" in c for c in changes):
            changes.append("whitespace_cleanup")

    cleaned = (base + ext).strip() if base else raw_name
    return cleaned, changes


def has_valid_artist_title(name):
    """Check if name follows 'Artist - Title' structure."""
    base, _ = strip_extension(name)
    parts = base.split(" - ")
    if len(parts) >= 2:
        artist = parts[0].strip()
        title = " - ".join(parts[1:]).strip()
        return bool(artist) and bool(title) and len(artist) >= 2 and len(title) >= 2
    return False


def count_artists(name):
    """Estimate number of artists from common multi-artist patterns."""
    base, _ = strip_extension(name)
    # Count by common separators: feat., ft., &, ,, x, vs
    count = 1
    count += len(re.findall(r"\bfeat\.?\b", base, re.IGNORECASE))
    count += len(re.findall(r"\bft\.?\b", base, re.IGNORECASE))
    count += len(re.findall(r"\bvs\.?\b", base, re.IGNORECASE))
    # Count commas in artist portion (before " - ")
    parts = base.split(" - ")
    if len(parts) >= 2:
        artist_part = parts[0]
        count += artist_part.count(",")
        count += artist_part.count("&")
    return count


def is_compilation_or_junk(name):
    """Check if name matches junk indicator patterns."""
    for pattern, pat_str in COMPILED_JUNK:
        if pattern.search(name):
            return True, pat_str
    # Check for excessive multi-artist chains (>3 artists)
    if count_artists(name) > 3:
        return True, "excessive_multi_artist"
    return False, None


# ═══════════════════════════════════════════════════════════════════
# PART A — LOAD INPUT
# ═══════════════════════════════════════════════════════════════════

def part_a_load():
    log("═══ PART A: Load input ═══")
    rows = []
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    log(f"Loaded {len(rows)} rows from fix_required_v2.csv")

    actions = Counter(r["recommended_action"] for r in rows)
    log(f"Action distribution: {dict(actions)}")

    # Create working copy
    with open(WORKING_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log(f"Working copy: {WORKING_CSV}")

    return rows


# ═══════════════════════════════════════════════════════════════════
# PARTS C + D — CLEAN + RECLASSIFY
# ═══════════════════════════════════════════════════════════════════

def parts_cd_process(rows):
    log("═══ PARTS C+D: Clean transform + Reclassification ═══")

    fix_rows = []       # → fix_required_v3.csv
    junk_rows = []      # → junk_candidates_v2.csv
    stats = {
        "total_input": len(rows),
        "cleaned_to_rename": 0,
        "moved_to_junk": 0,
        "remaining_verify": 0,
        "remaining_hold": 0,
        "already_rename_kept": 0,
        "auto_clean_applied": 0,
    }

    clean_examples = []
    auto_clean_rows_log = []
    moved_to_junk_log = []
    remaining_verify_log = []

    for i, row in enumerate(rows):
        current_name = row.get("current_name", "")
        suggested = row.get("suggested_name", "")
        action = row.get("recommended_action", "")
        confidence = float(row.get("confidence", "0"))
        issue_type = row.get("issue_type", "")
        notes = row.get("notes", "")
        file_path = row.get("file_path", "")
        priority = row.get("priority_score", "2")
        google_assisted = row.get("google_assisted", "")

        # ── Step 1: Clean the suggested name (or current if no suggestion) ──
        name_to_clean = suggested if suggested else current_name
        cleaned_name, changes = clean_name(name_to_clean)
        cleanup_applied = len(changes) > 0

        # Also clean the current name for comparison
        cleaned_current, _ = clean_name(current_name)

        # ── Step 2: Check if this is junk / compilation ──
        is_junk, junk_reason = is_compilation_or_junk(current_name)
        # Also check cleaned name
        if not is_junk:
            is_junk, junk_reason = is_compilation_or_junk(cleaned_name)

        # ── CASE 3: Non-track / Junk → move to junk_candidates ──
        if is_junk:
            junk_rows.append({
                "file_path": file_path,
                "reason": junk_reason,
                "confidence": "0.9",
                "recommended_action": "IGNORE",
                "notes": f"Auto-detected: {junk_reason}. Original: {current_name}",
                "priority_score": "1",
            })
            stats["moved_to_junk"] += 1
            moved_to_junk_log.append(f"[{i}] {current_name} → JUNK ({junk_reason})")
            continue

        # ── CASE 1: Clean success (high confidence) ──
        valid_structure = has_valid_artist_title(cleaned_name)

        new_action = action
        new_confidence = confidence
        auto_clean = False

        if cleanup_applied and valid_structure:
            # Cleaned and valid structure → high confidence RENAME
            new_action = "RENAME"
            new_confidence = max(confidence, 0.85)
            auto_clean = True
            stats["cleaned_to_rename"] += 1
            stats["auto_clean_applied"] += 1
            auto_clean_rows_log.append(f"[{i}] {current_name} → {cleaned_name}")
            if len(clean_examples) < 30:
                clean_examples.append({
                    "original": current_name,
                    "cleaned": cleaned_name,
                    "changes": changes,
                })
        elif not cleanup_applied and valid_structure and action == "RENAME":
            # Already clean and valid — keep as RENAME
            new_action = "RENAME"
            stats["already_rename_kept"] += 1
        elif cleanup_applied and not valid_structure:
            # Cleaned but still no clear structure
            # Check if we can promote from HOLD → VERIFY at least
            if action == "HOLD" and confidence >= 0.3:
                new_action = "VERIFY"
                new_confidence = max(confidence, 0.5)
            else:
                new_action = action if action != "HOLD" else "VERIFY"
            stats["remaining_verify"] += 1
            remaining_verify_log.append(f"[{i}] [{new_action}] {cleaned_name}")
        else:
            # No cleanup possible or already fine
            if action == "HOLD":
                # Try to upgrade HOLD rows that already have valid structure
                if valid_structure:
                    new_action = "RENAME"
                    new_confidence = max(confidence, 0.7)
                    stats["cleaned_to_rename"] += 1
                    auto_clean_rows_log.append(f"[{i}] HOLD→RENAME (valid structure): {current_name}")
                else:
                    new_action = "VERIFY"
                    stats["remaining_verify"] += 1
                    remaining_verify_log.append(f"[{i}] [HOLD→VERIFY] {current_name}")
            elif action == "VERIFY":
                if valid_structure and confidence >= 0.5:
                    new_action = "RENAME"
                    new_confidence = max(confidence, 0.75)
                    stats["cleaned_to_rename"] += 1
                else:
                    stats["remaining_verify"] += 1
                    remaining_verify_log.append(f"[{i}] [VERIFY] {current_name}")
            else:
                stats["already_rename_kept"] += 1

        fix_rows.append({
            "file_path": file_path,
            "original_name": current_name,
            "cleaned_name": cleaned_name,
            "issue_type": issue_type,
            "confidence": str(round(new_confidence, 2)),
            "recommended_action": new_action,
            "auto_clean": str(auto_clean).lower(),
            "notes": "; ".join(changes) if changes else notes,
            "priority_score": priority,
        })

    stats["remaining_hold"] = sum(1 for r in fix_rows if r["recommended_action"] == "HOLD")

    log(f"Processed {len(rows)} rows:")
    log(f"  Cleaned → RENAME: {stats['cleaned_to_rename']}")
    log(f"  Moved to JUNK:    {stats['moved_to_junk']}")
    log(f"  Remaining VERIFY: {stats['remaining_verify']}")
    log(f"  Already RENAME:   {stats['already_rename_kept']}")
    log(f"  Auto-clean applied: {stats['auto_clean_applied']}")

    return fix_rows, junk_rows, stats, clean_examples, auto_clean_rows_log, moved_to_junk_log, remaining_verify_log


# ═══════════════════════════════════════════════════════════════════
# PART E — OUTPUT FILES
# ═══════════════════════════════════════════════════════════════════

def part_e_output(fix_rows, junk_rows, stats):
    log("═══ PART E: Output files ═══")

    # 1. fix_required_v3.csv
    v3_cols = ["file_path", "original_name", "cleaned_name", "issue_type",
               "confidence", "recommended_action", "auto_clean", "notes", "priority_score"]
    with open(OUTPUT_V3, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=v3_cols)
        writer.writeheader()
        writer.writerows(fix_rows)
    log(f"Wrote {OUTPUT_V3} ({len(fix_rows)} rows)")

    # 2. junk_candidates_v2.csv — merge with v1
    existing_junk = []
    if JUNK_V1.exists():
        with open(JUNK_V1, "r", encoding="utf-8") as f:
            existing_junk = list(csv.DictReader(f))
    all_junk = existing_junk + junk_rows
    junk_cols = ["file_path", "reason", "confidence", "recommended_action", "notes", "priority_score"]
    with open(JUNK_V2, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=junk_cols)
        writer.writeheader()
        writer.writerows(all_junk)
    log(f"Wrote {JUNK_V2} ({len(all_junk)} rows = {len(existing_junk)} existing + {len(junk_rows)} new)")

    # 3. Reduction summary
    v3_actions = Counter(r["recommended_action"] for r in fix_rows)
    with open(SUMMARY_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value"])
        writer.writerow(["original_fix_count", stats["total_input"]])
        writer.writerow(["cleaned_to_rename_count", stats["cleaned_to_rename"]])
        writer.writerow(["moved_to_junk_count", stats["moved_to_junk"]])
        writer.writerow(["remaining_verify_count", stats["remaining_verify"]])
        writer.writerow(["remaining_hold_count", stats["remaining_hold"]])
        writer.writerow(["v3_total_rows", len(fix_rows)])
        writer.writerow(["v3_RENAME", v3_actions.get("RENAME", 0)])
        writer.writerow(["v3_VERIFY", v3_actions.get("VERIFY", 0)])
        writer.writerow(["v3_HOLD", v3_actions.get("HOLD", 0)])
        writer.writerow(["junk_v2_total", len(all_junk)])
    log(f"Wrote {SUMMARY_CSV}")

    return v3_actions, len(all_junk)


# ═══════════════════════════════════════════════════════════════════
# PART F — SAFETY VALIDATION
# ═══════════════════════════════════════════════════════════════════

def part_f_validate(fix_rows, junk_rows, original_count):
    log("═══ PART F: Safety validation ═══")
    checks = []

    # 1. No filesystem changes — count actual calls (exclude the validation section itself)
    script_path = Path(__file__)
    script_text = script_path.read_text(encoding="utf-8")
    # Split at PART F marker to only scan the non-validation code
    parts = script_text.split("PART F")
    code_before_validation = parts[0] if len(parts) > 1 else ""
    has_rename = "os.rename(" in code_before_validation
    has_remove = "os.remove(" in code_before_validation
    has_move   = "shutil.move(" in code_before_validation
    no_fs = not has_rename and not has_remove and not has_move
    checks.append(("no_file_ops", no_fs, "Zero os.rename/os.remove/shutil.move calls in processing code"))

    # 2. READY_NORMALIZED untouched
    rn_count = len(list(READY_NORM.iterdir())) if READY_NORM.exists() else 0
    rn_ok = rn_count == 401
    checks.append(("ready_normalized_intact", rn_ok, f"READY_NORMALIZED: {rn_count} files (expected 401)"))

    # 3. No rows lost — fix_rows + junk_rows should equal original
    total_out = len(fix_rows) + len(junk_rows)
    rows_ok = total_out == original_count
    checks.append(("no_rows_lost", rows_ok, f"fix({len(fix_rows)}) + junk({len(junk_rows)}) = {total_out} (expected {original_count})"))

    # 4. Output files exist
    outputs_exist = OUTPUT_V3.exists() and JUNK_V2.exists() and SUMMARY_CSV.exists()
    checks.append(("outputs_created", outputs_exist, f"v3={OUTPUT_V3.exists()}, junk_v2={JUNK_V2.exists()}, summary={SUMMARY_CSV.exists()}"))

    # 5. Original input unchanged
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        original_rows = len(list(csv.DictReader(f)))
    orig_ok = original_rows == original_count
    checks.append(("original_unchanged", orig_ok, f"fix_required_v2.csv: {original_rows} rows (expected {original_count})"))

    # 6. No deletes in processing code
    checks.append(("no_deletes", not has_remove, "Zero os.remove calls in processing code"))

    # 7. No renames in processing code
    checks.append(("no_renames", not has_rename, "Zero os.rename calls in processing code"))

    all_pass = all(ok for _, ok, _ in checks)
    status = "ALL PASS" if all_pass else "FAIL"
    log(f"Safety: {status} ({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")
    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")

    return checks, all_pass


# ═══════════════════════════════════════════════════════════════════
# PART G — REPORTING
# ═══════════════════════════════════════════════════════════════════

def part_g_report(stats, v3_actions, junk_total, checks, all_pass,
                  clean_examples, auto_clean_log, moved_junk_log, remaining_verify_log):
    log("═══ PART G: Reporting ═══")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # 00_input_summary.txt
    with open(PROOF_DIR / "00_input_summary.txt", "w", encoding="utf-8") as f:
        f.write("Phase 16.5 — Input Summary\n")
        f.write("=" * 40 + "\n")
        f.write(f"Input file: {INPUT_CSV}\n")
        f.write(f"Total rows: {stats['total_input']}\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")

    # 01_junk_patterns.txt
    with open(PROOF_DIR / "01_junk_patterns.txt", "w", encoding="utf-8") as f:
        f.write("Junk Strip Patterns (removed from filenames)\n")
        f.write("=" * 50 + "\n")
        for p in STRIP_PATTERNS_EXACT:
            f.write(f"  {p}\n")
        f.write("\nJunk Indicator Patterns (trigger JUNK reclassification)\n")
        f.write("=" * 50 + "\n")
        for p in JUNK_INDICATORS:
            f.write(f"  {p}\n")
        f.write(f"\nUnicode junk chars: {len(UNICODE_JUNK)}\n")

    # 02_clean_transform_examples.txt
    with open(PROOF_DIR / "02_clean_transform_examples.txt", "w", encoding="utf-8") as f:
        f.write("Clean Transform Examples (first 30)\n")
        f.write("=" * 60 + "\n")
        for ex in clean_examples:
            f.write(f"ORIGINAL: {ex['original']}\n")
            f.write(f"CLEANED:  {ex['cleaned']}\n")
            f.write(f"CHANGES:  {', '.join(ex['changes'])}\n")
            f.write("-" * 60 + "\n")

    # 03_auto_clean_rows.txt
    with open(PROOF_DIR / "03_auto_clean_rows.txt", "w", encoding="utf-8") as f:
        f.write(f"Auto-Cleaned Rows ({len(auto_clean_log)})\n")
        f.write("=" * 60 + "\n")
        for line in auto_clean_log:
            f.write(line + "\n")

    # 04_moved_to_junk.txt
    with open(PROOF_DIR / "04_moved_to_junk.txt", "w", encoding="utf-8") as f:
        f.write(f"Moved to Junk ({len(moved_junk_log)})\n")
        f.write("=" * 60 + "\n")
        for line in moved_junk_log:
            f.write(line + "\n")

    # 05_remaining_verify.txt
    with open(PROOF_DIR / "05_remaining_verify.txt", "w", encoding="utf-8") as f:
        f.write(f"Remaining VERIFY Rows ({len(remaining_verify_log)})\n")
        f.write("=" * 60 + "\n")
        for line in remaining_verify_log:
            f.write(line + "\n")

    # 06_validation_checks.txt
    with open(PROOF_DIR / "06_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write("Safety Validation Checks\n")
        f.write("=" * 40 + "\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'}\n")

    # 07_final_report.txt
    with open(PROOF_DIR / "07_final_report.txt", "w", encoding="utf-8") as f:
        f.write("Phase 16.5 — Final Report\n")
        f.write("=" * 40 + "\n")
        f.write(f"Input rows:            {stats['total_input']}\n")
        f.write(f"Cleaned → RENAME:      {stats['cleaned_to_rename']}\n")
        f.write(f"Moved to JUNK:         {stats['moved_to_junk']}\n")
        f.write(f"Remaining VERIFY:      {stats['remaining_verify']}\n")
        f.write(f"Remaining HOLD:        {stats['remaining_hold']}\n")
        f.write(f"Auto-clean applied:    {stats['auto_clean_applied']}\n")
        f.write(f"Already RENAME kept:   {stats['already_rename_kept']}\n")
        f.write(f"V3 action distribution: {dict(v3_actions)}\n")
        f.write(f"Junk candidates total: {junk_total}\n")
        f.write(f"Safety:                {'PASS' if all_pass else 'FAIL'}\n")

    # execution_log.txt
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log("Phase 16.5: Aggressive Junk Filter + Auto-Clean — BEGIN")
    log(f"Working directory: {BASE}")

    # Part A
    rows = part_a_load()
    original_count = len(rows)

    # Part B (patterns compiled at module level)
    log("═══ PART B: Junk pattern library ═══")
    log(f"  Strip patterns: {len(STRIP_PATTERNS_EXACT)}")
    log(f"  Junk indicators: {len(JUNK_INDICATORS)}")
    log(f"  Unicode junk chars: {len(UNICODE_JUNK)}")

    # Parts C+D
    fix_rows, junk_rows, stats, clean_examples, auto_clean_log, moved_junk_log, remaining_verify_log = parts_cd_process(rows)

    # Part E
    v3_actions, junk_total = part_e_output(fix_rows, junk_rows, stats)

    # Part F
    checks, all_pass = part_f_validate(fix_rows, junk_rows, original_count)

    # Part G
    part_g_report(stats, v3_actions, junk_total, checks, all_pass,
                  clean_examples, auto_clean_log, moved_junk_log, remaining_verify_log)

    # Final summary
    log("")
    log("=" * 60)
    log("PHASE 16.5 COMPLETE")
    log(f"  Input rows:          {stats['total_input']}")
    log(f"  V3 fix queue:        {len(fix_rows)}")
    log(f"  Cleaned → RENAME:    {stats['cleaned_to_rename']}")
    log(f"  Moved to JUNK:       {stats['moved_to_junk']}")
    log(f"  Auto-clean applied:  {stats['auto_clean_applied']}")
    log(f"  V3 actions:          {dict(v3_actions)}")
    log(f"  Junk total (v2):     {junk_total}")
    log(f"  Safety:              {'PASS' if all_pass else 'FAIL'}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
