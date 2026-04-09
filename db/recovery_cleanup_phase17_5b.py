#!/usr/bin/env python3
"""
Phase 17.5b — Data Recovery + Cleanup
======================================
Recovers user corrections from annotated CSV (color data lost),
cleans residual junk from Column C, and resolves unicode path
mismatches that blocked 32 renames in Phase 17.5.

Parts:
  A — Load both CSVs, identify user-edited rows
  B — Build junk token set from Column B diffs
  C — Clean Column C (remove residual junk, fix encoding artifacts)
  D — Resolve unicode file paths (match actual filesystem files)
  E — Output recovered_rename_ready_v1.csv
  F — Validation
"""

import csv
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from collections import Counter

BASE     = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA     = BASE / "data"
PROOF_DIR = BASE / "_proof" / "library_normalization_phase17_5b"

ORIGINAL_V3 = DATA / "fix_required_v3.csv"
ANNOTATED   = DATA / "fix_required_v3  Corrections annotated.csv"
OUTPUT_CSV  = DATA / "recovered_rename_ready_v1.csv"

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ═══════════════════════════════════════════════════════════════════
# PART B — JUNK TOKEN SET
# ═══════════════════════════════════════════════════════════════════

# Known junk patterns to strip from cleaned names
JUNK_STRIP_PATTERNS = [
    # Label / source noise
    r"\bNapalm\s+Records\b",
    r"\bWarner\s+Records\b",
    r"\bInterscope\b",
    r"\bMizxySlime\.com\b",
    # Video / platform noise
    r"\bOfficial\s+4K\s+Video\b",
    r"\bOfficial\s+Lyric\s+Video\b",
    r"\bOfficial\s+Music\s+Video\b",
    r"\bOfficial\s+Video\b",
    r"\bOfficial\s+Audio\b",
    r"\bOfficial\s+Visuali[sz]er\b",
    r"\bOfficial\b",
    r"\bVisuali[sz]er\b",
    r"\bVideo\s+Oficial\b",
    r"\bLyric\s+Video\b",
    r"\bwith\s+lyrics\b",
    r"\bw[/⧸]lyrics\b",
    r"\bLyrics?!?\b",
    r"\bM[/⧸]V\b",
    r"\bMv\b",
    # Quality noise
    r"\b4K\b",
    r"\b1080p\b",
    r"\b432[Hh]z\b",
    r"\bHD\b(?!\w)",
    r"\bHQ\b",
    r"\bRemaster(?:ed)?\b",
    r"\bRemastered\s+Version\b",
    # Platform noise
    r"\bNCS\b",
    r"\bCopyright\s+Free(?:\s+Music)?\b",
    r"\bFull\s+Album\b",
    r"#\w+",                          # hashtags
    r"@\w+",                          # @handles
    # Performance / live qualifiers the user stripped
    r"\bLive\s+(?:at|from)\s+[^-]+",  # "Live at Capitol Theatre"
    r"\bEnglish\s+Version\b",
    r"\boriginal\s+video\s+\d{4}\b",
    # Parenthetical noise
    r"\(Official[^)]*\)",
    r"\(Remaster(?:ed)?\)",
    r"\(with\s+lyrics\)",
    r"\(Live[^)]*\)",
    r"\(feat\.[^)]+\)\s*\(Live[^)]*\)",
    r"\[Explicit\]",
    r"\[explicit\]",
]

# Unicode characters to clean
UNICODE_CLEANUP = {
    "\uff5c": "",      # ｜ fullwidth vertical bar
    "\uff02": "",      # ＂ fullwidth quotation mark
    "\uff1f": "",      # ？ fullwidth question mark
    "\u29f8": " - ",   # ⧸ big solidus → separator
    "\uff1a": ":",     # ： fullwidth colon
    "\u00bd": "",      # ½ fraction
    "\ufffd": "",      # � replacement char
    "\u2019": "'",     # ' smart apostrophe
    "\u2013": "-",     # – en dash
    "\u2014": "-",     # — em dash
}

# Latin-1 encoding artifacts (from saving UTF-8 as latin-1)
LATIN1_ARTIFACTS = {
    "ï¼":  "",   # fullwidth colon ： mangled
    "ï½":  "",   # fullwidth bar ｜ mangled  
    "â§¸":  "/",  # big solidus ⧸ mangled
    "ï":   "",   # lone artifact byte
    "Ã¨":  "è",
    "Ã ":  "à",
    "Ã¼":  "ü",
    "Ã¶":  "ö",
    "Ã©":  "é",
}


def compile_patterns():
    return [re.compile(p, re.IGNORECASE) for p in JUNK_STRIP_PATTERNS]

COMPILED = compile_patterns()


# ═══════════════════════════════════════════════════════════════════
# PART A — LOAD
# ═══════════════════════════════════════════════════════════════════

def part_a_load():
    log("═══ PART A: Load CSVs ═══")

    with open(ORIGINAL_V3, "r", encoding="utf-8") as f:
        v3 = list(csv.DictReader(f))
    log(f"Original v3: {len(v3)} rows")

    with open(ANNOTATED, "r", encoding="latin-1") as f:
        ann = list(csv.DictReader(f))
    log(f"Annotated: {len(ann)} rows")

    # Identify user-edited rows (Column C changed)
    user_edits = set()
    for i in range(len(v3)):
        v3c = v3[i]["cleaned_name"]
        annc = ann[i]["cleaned_name"]
        if v3c == annc:
            continue
        try:
            if v3c.encode("utf-8") == annc.encode("latin-1"):
                continue
        except:
            pass
        user_edits.add(i)

    log(f"User-edited rows: {len(user_edits)}")
    return v3, ann, user_edits


# ═══════════════════════════════════════════════════════════════════
# PART C — CLEAN COLUMN C
# ═══════════════════════════════════════════════════════════════════

def clean_residual_junk(name):
    """Remove residual junk from a cleaned name."""
    base, ext = split_ext(name)
    changes = []

    # Fallback: if base is empty/whitespace, reconstruct from original
    if not base.strip():
        return name, ["EMPTY_BASE_SKIP"]

    # Fix latin-1 encoding artifacts
    for artifact, replacement in LATIN1_ARTIFACTS.items():
        if artifact in base:
            base = base.replace(artifact, replacement)
            changes.append(f"fixed_artifact:{artifact}")

    # Fix unicode chars
    for ch, repl in UNICODE_CLEANUP.items():
        if ch in base:
            base = base.replace(ch, repl)
            changes.append(f"fixed_unicode:{hex(ord(ch))}")

    # Strip junk patterns
    for pattern in COMPILED:
        if pattern.search(base):
            base = pattern.sub("", base)
            changes.append("stripped_junk")

    # Fix dash spacing
    # "Artist- Title" → "Artist - Title"
    base = re.sub(r"(\w)\s*[-–—]\s+", r"\1 - ", base)
    base = re.sub(r"\s+[-–—]\s*(\w)", r" - \1", base)

    # Clean up trailing junk
    base = re.sub(r"[\s|~:;,\-]+$", "", base)
    base = re.sub(r"^[\s|~:;,\-]+", "", base)
    base = re.sub(r"\(\s*\)", "", base)   # empty parens
    base = re.sub(r"\s{2,}", " ", base).strip()

    # Ensure proper "Artist - Title" has no trailing "by " remnant
    base = re.sub(r"\s+by\s*$", "", base, flags=re.IGNORECASE)

    if not ext:
        ext = ".mp3"

    cleaned = f"{base}{ext}".strip()
    return cleaned, changes


def split_ext(name):
    """Split filename and extension."""
    for ext in [".wmv.mp3", ".mmp3", ".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac"]:
        if name.lower().endswith(ext):
            return name[:-len(ext)], ".mp3" if ext in (".wmv.mp3", ".mmp3") else ext
    b, e = os.path.splitext(name)
    return b, e


# ═══════════════════════════════════════════════════════════════════
# PART D — RESOLVE UNICODE FILE PATHS
# ═══════════════════════════════════════════════════════════════════

def resolve_file_path(file_path_str):
    """
    Try to find the actual file on disk. If the path has unicode chars
    that don't match, scan the parent directory for a fuzzy match.
    """
    fp = Path(file_path_str)
    if fp.exists():
        return str(fp), True

    # Try to find by scanning parent directory
    parent = fp.parent
    if not parent.exists():
        return file_path_str, False

    target_name = fp.name
    # Normalize for comparison: strip unicode variation selectors, etc.
    target_normalized = normalize_for_match(target_name)

    for child in parent.iterdir():
        if child.is_file():
            child_normalized = normalize_for_match(child.name)
            if child_normalized == target_normalized:
                return str(child), True
            # Also try: if target is a substring match
            if len(target_normalized) > 10 and (
                target_normalized[:20] == child_normalized[:20] and
                target_normalized[-10:] == child_normalized[-10:]
            ):
                return str(child), True

    return file_path_str, False


def normalize_for_match(name):
    """Normalize a filename for fuzzy matching by stripping special unicode."""
    # Replace all non-ASCII with empty for comparison
    result = ""
    for ch in name:
        if ord(ch) < 128:
            result += ch.lower()
        else:
            # Map common fullwidth chars to ASCII equivalents
            mapped = UNICODE_CLEANUP.get(ch, "")
            result += mapped.lower() if mapped else ""
    # Collapse spaces and dashes
    result = re.sub(r"[\s\-]+", " ", result).strip()
    return result


def fix_slash_merges(original_name, cleaned_name, changes):
    """
    Fix merged words when ⧸ (big solidus) was stripped without space.
    E.g., 'Trouble⧸Lumos!' → 'Troublelumos!' → 'Trouble - Lumos!'
    """
    base_clean, ext_clean = split_ext(cleaned_name)
    # Find all positions in original where ⧸ appears
    # Get the text around each ⧸ and check if it merged in cleaned
    parts = original_name.split("\u29f8")
    if len(parts) < 2:
        return cleaned_name

    for j in range(len(parts) - 1):
        # Get the end of left part and start of right part (lowered for matching)
        left_tail = parts[j].rstrip()[-6:] if parts[j].rstrip() else ""
        right_head = parts[j + 1].lstrip()[:6] if parts[j + 1].lstrip() else ""
        if not left_tail or not right_head:
            continue

        # Check if these are merged in the cleaned name (no space between)
        merged = left_tail + right_head
        if merged.lower() in base_clean.lower():
            # Insert separator, capitalize right side from original
            idx = base_clean.lower().find(merged.lower())
            if idx >= 0:
                split_point = idx + len(left_tail)
                right_part = base_clean[split_point:]
                # Capitalize first char using original casing
                orig_right = parts[j + 1].lstrip()
                if orig_right and right_part and right_part[0].lower() == orig_right[0].lower():
                    right_part = orig_right[0] + right_part[1:]
                base_clean = base_clean[:split_point] + " - " + right_part
                changes.append("fixed_slash_merge")

    return base_clean + ext_clean


# ═══════════════════════════════════════════════════════════════════
# MAIN PROCESSING
# ═══════════════════════════════════════════════════════════════════

def process_all(v3, ann, user_edits):
    log("═══ PARTS B+C+D: Build junk set, clean, resolve paths ═══")

    results = []
    stats = {
        "total": len(v3),
        "user_edited": len(user_edits),
        "cleaned": 0,
        "path_resolved": 0,
        "path_failed": 0,
        "ready_for_rename": 0,
        "already_done": 0,
        "needs_review": 0,
    }

    for i in range(len(v3)):
        row = v3[i]
        file_path = row["file_path"]
        original_name = row["original_name"]
        action = row["recommended_action"]

        # Determine which cleaned_name to use
        if i in user_edits:
            # User edited this row — use annotated version
            raw_cleaned = ann[i]["cleaned_name"]
            # Decode from latin-1 encoding artifacts
            try:
                raw_cleaned = raw_cleaned.encode("latin-1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
            source = "operator_corrected"
        else:
            # Use original v3 cleaned name
            raw_cleaned = row["cleaned_name"]
            source = "auto_cleaned"

        # Apply residual junk cleaning
        final_cleaned, changes = clean_residual_junk(raw_cleaned)

        # Fix merged words from unicode separator stripping:
        # If the original had ⧸ (big solidus) between characters and the
        # cleaned version merged them (no space), insert " - "
        if "\u29f8" in original_name:
            final_cleaned = fix_slash_merges(original_name, final_cleaned, changes)

        # If cleaned result is empty/extension-only, reconstruct from original
        base_check, _ = split_ext(final_cleaned)
        if not base_check.strip():
            reconstructed, rchanges = clean_residual_junk(original_name)
            if rchanges != ["EMPTY_BASE_SKIP"]:
                final_cleaned = reconstructed
                changes = ["reconstructed_from_original"] + rchanges

        # Resolve the actual file path on disk
        resolved_path, path_found = resolve_file_path(file_path)

        if path_found and resolved_path != file_path:
            stats["path_resolved"] += 1
        elif not path_found:
            stats["path_failed"] += 1

        # Determine if this rename is actionable
        if not path_found:
            # Check if the target (cleaned name) already exists in the parent dir
            parent = Path(file_path).parent
            target_path = parent / final_cleaned
            if target_path.exists():
                status = "ALREADY_DONE"
                stats["already_done"] += 1
            else:
                status = "PATH_NOT_FOUND"
                stats["needs_review"] += 1
        elif final_cleaned == original_name:
            status = "NO_CHANGE_NEEDED"
        elif not final_cleaned or len(final_cleaned) < 5:
            status = "CLEANED_TOO_SHORT"
            stats["needs_review"] += 1
        else:
            status = "READY"
            stats["ready_for_rename"] += 1

        if changes:
            stats["cleaned"] += 1

        notes_parts = []
        if source == "operator_corrected":
            notes_parts.append("operator_edit")
        if changes:
            notes_parts.append(f"cleaned:{len(changes)}")
        if resolved_path != file_path:
            notes_parts.append("path_resolved")

        results.append({
            "file_path": resolved_path,
            "original_path": file_path,
            "original_name": original_name,
            "cleaned_name": final_cleaned,
            "status": status,
            "source": source,
            "notes": "; ".join(notes_parts) if notes_parts else "",
        })

    log(f"Processing complete:")
    log(f"  Total rows:        {stats['total']}")
    log(f"  User-edited:       {stats['user_edited']}")
    log(f"  Residual cleaned:  {stats['cleaned']}")
    log(f"  Paths resolved:    {stats['path_resolved']}")
    log(f"  Paths not found:   {stats['path_failed']}")
    log(f"  Ready for rename:  {stats['ready_for_rename']}")
    log(f"  Already done:      {stats['already_done']}")
    log(f"  Needs review:      {stats['needs_review']}")

    return results, stats


# ═══════════════════════════════════════════════════════════════════
# PART E — OUTPUT
# ═══════════════════════════════════════════════════════════════════

def part_e_output(results):
    log("═══ PART E: Output ═══")

    cols = ["file_path", "original_name", "cleaned_name", "status", "source", "notes"]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r[k] for k in cols})
    log(f"Wrote {OUTPUT_CSV} ({len(results)} rows)")

    # Stats by status
    status_counts = Counter(r["status"] for r in results)
    log(f"  Status distribution: {dict(status_counts)}")

    return status_counts


# ═══════════════════════════════════════════════════════════════════
# PART F — VALIDATION
# ═══════════════════════════════════════════════════════════════════

def part_f_validate(results, stats):
    log("═══ PART F: Validation ═══")
    checks = []

    # 1. No empty cleaned names
    empty = sum(1 for r in results if not r["cleaned_name"].strip())
    checks.append(("no_empty_cleaned", empty == 0, f"{empty} empty cleaned names"))

    # 2. Extensions preserved
    bad_ext = 0
    for r in results:
        _, ext = split_ext(r["cleaned_name"])
        if not ext:
            bad_ext += 1
    checks.append(("extensions_preserved", bad_ext == 0, f"{bad_ext} missing extensions"))

    # 3. All rows accounted for
    checks.append(("all_rows_present", len(results) == stats["total"],
                    f"{len(results)} rows (expected {stats['total']})"))

    # 4. Output file exists
    checks.append(("output_exists", OUTPUT_CSV.exists(), str(OUTPUT_CSV.exists())))

    all_pass = all(ok for _, ok, _ in checks)
    log(f"Validation: {'ALL PASS' if all_pass else 'FAIL'} ({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")
    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")

    return checks, all_pass


# ═══════════════════════════════════════════════════════════════════
# REPORTING
# ═══════════════════════════════════════════════════════════════════

def report(results, stats, status_counts, checks, all_pass):
    log("═══ Reporting ═══")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # Summary
    with open(PROOF_DIR / "00_recovery_summary.txt", "w", encoding="utf-8") as f:
        f.write("Phase 17.5b — Data Recovery + Cleanup\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Total rows: {stats['total']}\n")
        f.write(f"User-edited: {stats['user_edited']}\n")
        f.write(f"Residual cleaned: {stats['cleaned']}\n")
        f.write(f"Paths resolved: {stats['path_resolved']}\n")
        f.write(f"Paths not found: {stats['path_failed']}\n")
        f.write(f"Ready for rename: {stats['ready_for_rename']}\n")
        f.write(f"Status: {dict(status_counts)}\n")

    # Operator corrections detail
    with open(PROOF_DIR / "01_operator_corrections.txt", "w", encoding="utf-8") as f:
        f.write("Operator-Corrected Rows\n")
        f.write("=" * 60 + "\n")
        for r in results:
            if r["source"] == "operator_corrected":
                f.write(f"ORIG: {r['original_name']}\n")
                f.write(f"NEW:  {r['cleaned_name']}\n")
                f.write(f"PATH: {r['file_path']}\n")
                f.write(f"STATUS: {r['status']}\n")
                f.write("-" * 60 + "\n")

    # Path resolutions
    with open(PROOF_DIR / "02_path_resolutions.txt", "w", encoding="utf-8") as f:
        f.write("Path Resolutions\n")
        f.write("=" * 60 + "\n")
        for r in results:
            if r["file_path"] != r["original_path"]:
                f.write(f"CSV:    {r['original_path'][:80]}\n")
                f.write(f"ACTUAL: {r['file_path'][:80]}\n")
                f.write("-" * 60 + "\n")
        not_found = [r for r in results if r["status"] == "PATH_NOT_FOUND"]
        if not_found:
            f.write(f"\nNOT FOUND ({len(not_found)}):\n")
            for r in not_found:
                f.write(f"  {r['original_path'][:80]}\n")

    # Validation
    with open(PROOF_DIR / "03_validation.txt", "w", encoding="utf-8") as f:
        f.write("Validation Checks\n")
        f.write("=" * 40 + "\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'}\n")

    # Execution log
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    log("Phase 17.5b: Data Recovery + Cleanup — BEGIN")
    log(f"Working directory: {BASE}")

    v3, ann, user_edits = part_a_load()
    results, stats = process_all(v3, ann, user_edits)
    status_counts = part_e_output(results)
    checks, all_pass = part_f_validate(results, stats)
    report(results, stats, status_counts, checks, all_pass)

    log("")
    log("=" * 60)
    log("PHASE 17.5b COMPLETE")
    log(f"  Total:           {stats['total']}")
    log(f"  Operator edits:  {stats['user_edited']}")
    log(f"  Ready to rename: {stats['ready_for_rename']}")
    log(f"  Already done:    {stats['already_done']}")
    log(f"  Paths resolved:  {stats['path_resolved']}")
    log(f"  Paths not found: {stats['path_failed']}")
    log(f"  Validation:      {'PASS' if all_pass else 'FAIL'}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
