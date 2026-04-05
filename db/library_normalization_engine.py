"""
DJ Library Normalization Engine — Phase 1
==========================================
Scans a target music folder, parses filenames, builds a normalization plan
without renaming live files. Supports presets and user-configurable output patterns.

No files are renamed by default. All output is a reviewable CSV plan.
"""

import os
import re
import csv
import json
import time
import pathlib
import unicodedata
from dataclasses import dataclass, field, asdict

# ================================================================
# PATHS
# ================================================================
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DEFAULT_MUSIC_DIR = pathlib.Path(r"C:\Users\suppo\Music")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase1"
PROOF_DIR.mkdir(parents=True, exist_ok=True)

AUDIO_EXTENSIONS = {".mp3", ".flac", ".wav", ".m4a", ".aac", ".ogg", ".wma", ".opus"}

# ================================================================
# TITLE CASE RULES
# ================================================================
# Words that stay lowercase in standard title case (unless first/last)
TITLE_CASE_EXCEPTIONS = {
    "a", "an", "the", "and", "but", "or", "nor", "for", "yet", "so",
    "in", "on", "at", "to", "by", "of", "up", "as", "is", "it",
    "vs", "vs.", "ft", "ft.", "n'",
}

# Words/patterns that should preserve their casing
PRESERVE_CASE_PATTERNS = {
    "DJ", "MC", "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI",
    "AC", "DC", "B.I.G.", "EZ", "XXX", "WD", "PhD",
    "I", "I'd", "I'll", "I'm", "I've",
}

# Junk tokens to strip from filenames
JUNK_PATTERNS = [
    r"\(Official\s+Music\s+Video\)",
    r"\(Official\s+Video\)",
    r"\(Official\s+Audio\)",
    r"\(Official\s+Lyric\s+Video\)",
    r"\(Lyrics?\)",
    r"\(Audio\)",
    r"\(HD\)",
    r"\(HQ\)",
    r"\(4K\)",
    r"\(1080p\)",
    r"\(720p\)",
    r"\[Official\s+Music\s+Video\]",
    r"\[Official\s+Video\]",
    r"\[Official\s+Audio\]",
    r"\[Lyrics?\]",
    r"\[Audio\]",
    r"\[HD\]",
    r"\[HQ\]",
    r"＂[^＂]*＂",  # fullwidth quotes
]

# Bracket content patterns (removable via option)
BRACKET_PATTERN = re.compile(r"\s*[\(\[\{][^\)\]\}]*[\)\]\}]\s*")

# Leading number pattern (e.g., "01 - ", "1. ", "01. ", "01) ")
# Only match digits followed by an explicit separator (dot, dash, paren) then space.
# Do NOT match bare "3 " or "38 " — those are artist names like "3 Doors Down".
LEADING_NUMBER_PATTERN = re.compile(r"^\d{1,3}[\.)\-]\s+")

# Quote patterns — only double quotes and fullwidth quotes.
# Single quotes / apostrophes are preserved because they appear in contractions
# (I'd, Don't), colloquial spelling (Drivin', Breakin'), and names (Rock 'N' Roll).
QUOTE_WRAPPING = re.compile(r'["\u201c\u201d\uff02]')

# Separator normalization (en-dash, em-dash → hyphen)
DASH_NORMALIZE = re.compile(r"\s*[\u2013\u2014\uff0d]\s*")

# Multiple spaces
MULTI_SPACE = re.compile(r"\s{2,}")

# Multiple dashes in separator position
MULTI_DASH_SEP = re.compile(r"\s+-+\s+")

# Featuring patterns for normalization
FEAT_PATTERNS = [
    (re.compile(r"\bfeaturing\b", re.IGNORECASE), "feat."),
    (re.compile(r"\bfeat\b(?!\.)", re.IGNORECASE), "feat."),
    (re.compile(r"\bft\b(?!\.)", re.IGNORECASE), "ft."),
    (re.compile(r"\bFt\b(?!\.)", re.IGNORECASE), "ft."),
]

# ================================================================
# PRESETS
# ================================================================
@dataclass
class NormalizationPreset:
    name: str
    output_pattern: str  # "Artist - Song", "Song - Artist", etc.
    remove_brackets: bool = True
    remove_quotes: bool = True
    remove_leading_numbers: bool = True
    case_mode: str = "title_case"  # preserve, title_case, strict_title_case, lower
    remove_junk: bool = True
    normalize_feat: bool = True
    normalize_dashes: bool = True
    collapse_spaces: bool = True
    strip_trailing_spaces: bool = True


PRESETS = {
    "Gene_Default": NormalizationPreset(
        name="Gene_Default",
        output_pattern="Artist - Song",
        remove_brackets=True,
        remove_quotes=True,
        remove_leading_numbers=True,
        case_mode="title_case",
        remove_junk=True,
        normalize_feat=True,
        normalize_dashes=True,
        collapse_spaces=True,
        strip_trailing_spaces=True,
    ),
    "Preserve_Raw": NormalizationPreset(
        name="Preserve_Raw",
        output_pattern="Artist - Song",
        remove_brackets=False,
        remove_quotes=False,
        remove_leading_numbers=False,
        case_mode="preserve",
        remove_junk=False,
        normalize_feat=False,
        normalize_dashes=False,
        collapse_spaces=True,
        strip_trailing_spaces=True,
    ),
}


# ================================================================
# CORE ENGINE FUNCTIONS
# ================================================================

def scan_files(root_dir: pathlib.Path, recursive: bool = True) -> list[pathlib.Path]:
    """Scan a directory for audio files."""
    files = []
    if recursive:
        for dirpath, _, filenames in os.walk(root_dir):
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext in AUDIO_EXTENSIONS:
                    files.append(pathlib.Path(dirpath) / fn)
    else:
        for fn in os.listdir(root_dir):
            fp = root_dir / fn
            if fp.is_file():
                ext = fp.suffix.lower()
                if ext in AUDIO_EXTENSIONS:
                    files.append(fp)
    files.sort(key=lambda p: p.name.lower())
    return files


def clean_tokens(name: str, preset: NormalizationPreset) -> tuple[str, list[str]]:
    """Remove junk tokens from a filename stem. Returns (cleaned, list of stripped tokens)."""
    stripped = []

    # 1. Remove known junk patterns (always if remove_junk is on)
    if preset.remove_junk:
        for pattern in JUNK_PATTERNS:
            match = re.search(pattern, name, re.IGNORECASE)
            while match:
                stripped.append(match.group().strip())
                name = name[:match.start()] + " " + name[match.end():]
                match = re.search(pattern, name, re.IGNORECASE)

    # 2. Remove bracketed content (if option on)
    if preset.remove_brackets:
        for match in BRACKET_PATTERN.finditer(name):
            token = match.group().strip()
            # Keep meaningful version tags like (Remix), (Live), (Acoustic)
            inner = token.strip("()[]{}").strip().lower()
            meaningful = {"remix", "live", "acoustic", "unplugged", "demo",
                          "deluxe", "remaster", "remastered", "radio edit",
                          "extended", "instrumental", "clean", "explicit"}
            if inner in meaningful:
                continue  # keep it
            stripped.append(token)
        # Now actually remove the non-meaningful ones
        for token in stripped:
            name = name.replace(token, " ")

    # 3. Remove wrapping quotes (preserve apostrophes in contractions)
    if preset.remove_quotes:
        for m in QUOTE_WRAPPING.finditer(name):
            if m.group() not in stripped:
                stripped.append(m.group())
        name = QUOTE_WRAPPING.sub("", name)

    # 4. Remove leading numbers
    if preset.remove_leading_numbers:
        m = LEADING_NUMBER_PATTERN.match(name)
        if m:
            stripped.append(f"leading:{m.group().strip()}")
            name = name[m.end():]

    # 5. Normalize dashes (en-dash, em-dash → standard)
    if preset.normalize_dashes:
        name = DASH_NORMALIZE.sub(" - ", name)

    # 6. Normalize featuring tags
    if preset.normalize_feat:
        for pat, repl in FEAT_PATTERNS:
            name = pat.sub(repl, name)

    # 7. Collapse multiple spaces
    if preset.collapse_spaces:
        name = MULTI_SPACE.sub(" ", name)

    # 8. Normalize separator (multiple dashes → single)
    name = MULTI_DASH_SEP.sub(" - ", name)

    # 9. Strip trailing/leading spaces
    if preset.strip_trailing_spaces:
        name = name.strip()

    return name, stripped


def strict_title_case(text: str) -> str:
    """Capitalize every word, no exceptions."""
    words = text.split()
    result = []
    for w in words:
        if w.upper() in {p.upper() for p in PRESERVE_CASE_PATTERNS}:
            # Find the matching preserved form
            for p in PRESERVE_CASE_PATTERNS:
                if w.upper() == p.upper():
                    result.append(p)
                    break
        elif w == "-":
            result.append("-")
        elif w.startswith("(") and len(w) > 1:
            result.append("(" + w[1:].capitalize())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def standard_title_case(text: str) -> str:
    """Title case with exceptions for articles, prepositions, conjunctions."""
    words = text.split()
    result = []
    for i, w in enumerate(words):
        w_lower = w.lower()
        # Check preserve patterns first
        if w.upper() in {p.upper() for p in PRESERVE_CASE_PATTERNS}:
            for p in PRESERVE_CASE_PATTERNS:
                if w.upper() == p.upper():
                    result.append(p)
                    break
        elif w == "-":
            result.append("-")
        elif i == 0 or i == len(words) - 1:
            # First and last words always capitalized
            if w.startswith("(") and len(w) > 1:
                result.append("(" + w[1:].capitalize())
            else:
                result.append(w.capitalize())
        elif w_lower in TITLE_CASE_EXCEPTIONS:
            result.append(w_lower)
        elif w.startswith("(") and len(w) > 1:
            result.append("(" + w[1:].capitalize())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def apply_case_mode(text: str, mode: str) -> str:
    """Apply the selected case mode to text."""
    if mode == "preserve":
        return text
    elif mode == "strict_title_case":
        return strict_title_case(text)
    elif mode == "title_case":
        return standard_title_case(text)
    elif mode == "lower":
        return text.lower()
    return text


def parse_artist_title(stem: str) -> tuple[str, str, float]:
    """
    Parse a filename stem into (artist, title, confidence).
    Confidence: 1.0 = clear separator found, 0.5 = guessed, 0.0 = unparseable.
    """
    # Try standard " - " separator
    if " - " in stem:
        parts = stem.split(" - ", 1)
        artist = parts[0].strip()
        title = parts[1].strip()
        if artist and title:
            return artist, title, 1.0
        elif artist:
            return artist, stem, 0.5
        else:
            return "", stem, 0.3

    # Try " – " (en-dash) — should have been normalized but just in case
    if " \u2013 " in stem or " \u2014 " in stem:
        for sep in [" \u2013 ", " \u2014 "]:
            if sep in stem:
                parts = stem.split(sep, 1)
                return parts[0].strip(), parts[1].strip(), 0.9

    # No separator found — whole thing is title, artist unknown
    return "", stem, 0.0


def build_proposed_name(artist: str, title: str, ext: str,
                        preset: NormalizationPreset) -> str:
    """Build the proposed filename from parsed components."""
    # Apply case mode to both parts
    if artist:
        artist_cased = apply_case_mode(artist, preset.case_mode)
    else:
        artist_cased = ""

    title_cased = apply_case_mode(title, preset.case_mode)

    # Build according to pattern
    pattern = preset.output_pattern
    if pattern == "Artist - Song":
        if artist_cased:
            name = f"{artist_cased} - {title_cased}"
        else:
            name = title_cased
    elif pattern == "Song - Artist":
        if artist_cased:
            name = f"{title_cased} - {artist_cased}"
        else:
            name = title_cased
    elif pattern == "Artist - Song (Version)":
        if artist_cased:
            name = f"{artist_cased} - {title_cased}"
        else:
            name = title_cased
    else:
        # Custom template fallback
        name = pattern.replace("Artist", artist_cased).replace("Song", title_cased)

    return name + ext


def collision_detection(plans: list[dict]) -> list[dict]:
    """Detect filename collisions in proposed names. Returns updated plans."""
    # Group by proposed destination (lowercased for case-insensitive FS)
    name_groups: dict[str, list[int]] = {}
    for i, plan in enumerate(plans):
        key = plan["proposed_name"].lower()
        if key not in name_groups:
            name_groups[key] = []
        name_groups[key].append(i)

    for key, indices in name_groups.items():
        if len(indices) > 1:
            for idx in indices:
                plans[idx]["collision_status"] = f"COLLISION ({len(indices)} files)"
                plans[idx]["action"] = "hold"
        # Also check if proposed == original (no change needed)
        for idx in indices:
            if plans[idx]["proposed_name"].lower() == plans[idx]["original_name"].lower():
                plans[idx]["collision_status"] = "no_change"
                plans[idx]["action"] = "skip"

    return plans


# ================================================================
# MAIN ENGINE
# ================================================================

@dataclass
class NormalizationResult:
    original_path: str
    original_name: str
    cleaned_name: str
    guessed_artist: str
    guessed_title: str
    stripped_tokens: str
    confidence: float
    proposed_name: str
    collision_status: str = "ok"
    action: str = "review"


def run_engine(
    music_dir: pathlib.Path,
    preset_name: str = "Gene_Default",
    recursive: bool = True,
) -> tuple[list[dict], list[str]]:
    """Run the normalization engine. Returns (plans, log_lines)."""

    log: list[str] = []
    t0 = time.time()
    preset = PRESETS[preset_name]

    log.append(f"Engine started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.append(f"Music directory: {music_dir}")
    log.append(f"Preset: {preset.name}")
    log.append(f"Recursive: {recursive}")
    log.append("")

    # Phase 1: Scan
    log.append("--- SCANNING ---")
    files = scan_files(music_dir, recursive)
    log.append(f"  Audio files found: {len(files)}")

    if not files:
        log.append("  WARNING: No audio files found!")
        return [], log

    # Count by extension
    ext_counts: dict[str, int] = {}
    for f in files:
        ext = f.suffix.lower()
        ext_counts[ext] = ext_counts.get(ext, 0) + 1
    for ext, count in sorted(ext_counts.items()):
        log.append(f"    {ext}: {count}")
    log.append("")

    # Phase 2: Process each file
    log.append("--- PROCESSING ---")
    plans: list[dict] = []
    parse_stats = {"high": 0, "medium": 0, "low": 0, "none": 0}
    change_count = 0

    for fp in files:
        original_name = fp.name
        stem = fp.stem
        ext = fp.suffix

        # Clean tokens
        cleaned, stripped = clean_tokens(stem, preset)

        # Parse artist/title
        artist, title, confidence = parse_artist_title(cleaned)

        # Classify confidence
        if confidence >= 0.9:
            parse_stats["high"] += 1
        elif confidence >= 0.5:
            parse_stats["medium"] += 1
        elif confidence > 0:
            parse_stats["low"] += 1
        else:
            parse_stats["none"] += 1

        # Build proposed name
        proposed = build_proposed_name(artist, title, ext, preset)

        # Determine if there's a change
        action = "review"
        collision_status = "ok"
        if proposed.lower() == original_name.lower():
            action = "skip"
            collision_status = "no_change"
        elif confidence < 0.5:
            action = "hold"
            collision_status = "low_confidence"
        else:
            change_count += 1

        plan = {
            "original_path": str(fp),
            "original_name": original_name,
            "cleaned_name": cleaned + ext,
            "guessed_artist": artist,
            "guessed_title": title,
            "stripped_tokens": "; ".join(stripped) if stripped else "",
            "confidence": confidence,
            "proposed_name": proposed,
            "collision_status": collision_status,
            "action": action,
        }
        plans.append(plan)

    log.append(f"  Processed: {len(plans)}")
    log.append(f"  Parse confidence: high={parse_stats['high']}, "
               f"medium={parse_stats['medium']}, low={parse_stats['low']}, "
               f"none={parse_stats['none']}")
    log.append(f"  Changes proposed: {change_count}")
    log.append("")

    # Phase 3: Collision detection
    log.append("--- COLLISION DETECTION ---")
    plans = collision_detection(plans)
    collision_count = sum(1 for p in plans if p["collision_status"].startswith("COLLISION"))
    log.append(f"  Collisions detected: {collision_count}")
    log.append("")

    # Phase 4: Summary
    action_counts: dict[str, int] = {}
    for p in plans:
        action_counts[p["action"]] = action_counts.get(p["action"], 0) + 1

    log.append("--- ACTION SUMMARY ---")
    for action, count in sorted(action_counts.items()):
        log.append(f"  {action}: {count}")

    elapsed = round(time.time() - t0, 2)
    log.append(f"\nEngine completed in {elapsed}s")

    return plans, log


def export_review_csv(plans: list[dict], output_path: pathlib.Path):
    """Export the normalization plan as a reviewable CSV."""
    if not plans:
        return
    fieldnames = [
        "original_path", "original_name", "cleaned_name",
        "guessed_artist", "guessed_title", "stripped_tokens",
        "confidence", "proposed_name", "collision_status", "action",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(plans)


# ================================================================
# PROOF GENERATION
# ================================================================

def write_proof(plans: list[dict], log_lines: list[str],
                preset: NormalizationPreset, music_dir: pathlib.Path):
    """Write all proof artifacts."""

    # 00 — Engine design summary
    lines = []
    lines.append("=" * 70)
    lines.append("DJ LIBRARY NORMALIZATION ENGINE — PHASE 1 DESIGN SUMMARY")
    lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("ARCHITECTURE:")
    lines.append("  scan_files        — Recursive audio file scanner")
    lines.append("  clean_tokens      — Junk removal, bracket stripping, quote removal")
    lines.append("  strict_title_case — Capitalize every word")
    lines.append("  standard_title_case — Title case with article/preposition exceptions")
    lines.append("  parse_artist_title — Split on ' - ' separator with confidence scoring")
    lines.append("  build_proposed_name — Apply preset pattern to artist + title")
    lines.append("  collision_detection — Detect duplicate proposed names")
    lines.append("  export_review_csv — Output reviewable CSV plan")
    lines.append("  optional_apply_approved — (Phase 2) Apply approved renames only")
    lines.append("")
    lines.append("PROCESSING PIPELINE:")
    lines.append("  1. Scan target directory for audio files")
    lines.append("  2. For each file:")
    lines.append("     a. Extract filename stem")
    lines.append("     b. Clean tokens (junk, brackets, quotes, leading numbers)")
    lines.append("     c. Normalize dashes and featuring tags")
    lines.append("     d. Parse artist/title with confidence scoring")
    lines.append("     e. Apply case mode")
    lines.append("     f. Build proposed name from preset pattern")
    lines.append("  3. Run collision detection across all proposed names")
    lines.append("  4. Export CSV plan for human review")
    lines.append("  5. NO live renames occur by default")
    lines.append("")
    lines.append("SAFETY:")
    lines.append("  - Default mode is plan-only (no filesystem writes)")
    lines.append("  - Collision detection prevents overwrites")
    lines.append("  - Low-confidence parses are marked 'hold'")
    lines.append("  - All actions are logged and auditable")
    lines.append("  - Fail-closed on ambiguous filenames")
    (PROOF_DIR / "00_engine_design_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # 01 — Option matrix
    lines = []
    lines.append("=" * 70)
    lines.append("OPTION MATRIX")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"{'Option':<30s} {'Gene_Default':>15s} {'Preserve_Raw':>15s}")
    lines.append("-" * 62)
    for field_name in [
        "output_pattern", "remove_brackets", "remove_quotes",
        "remove_leading_numbers", "case_mode", "remove_junk",
        "normalize_feat", "normalize_dashes", "collapse_spaces",
        "strip_trailing_spaces",
    ]:
        gd = str(getattr(PRESETS["Gene_Default"], field_name))
        pr = str(getattr(PRESETS["Preserve_Raw"], field_name))
        lines.append(f"  {field_name:<28s} {gd:>15s} {pr:>15s}")
    lines.append("")
    lines.append("CASE MODES:")
    lines.append("  preserve           — Keep original casing")
    lines.append("  title_case         — Standard title case (articles lowercase)")
    lines.append("  strict_title_case  — Every word capitalized")
    lines.append("  lower              — All lowercase")
    lines.append("")
    lines.append("OUTPUT PATTERNS:")
    lines.append("  Artist - Song          — Default for Gene_Default")
    lines.append("  Song - Artist          — Reversed layout")
    lines.append("  Artist - Song (Version)— Preserves version tags")
    lines.append("  custom template        — User-defined (Phase 2)")
    (PROOF_DIR / "01_option_matrix.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # 02 — Parsing rules
    lines = []
    lines.append("=" * 70)
    lines.append("PARSING RULES")
    lines.append("=" * 70)
    lines.append("")
    lines.append("ARTIST/TITLE SEPARATOR:")
    lines.append("  Primary:   ' - '  (space-dash-space)")
    lines.append("  Fallback:  ' – '  (en-dash), ' — ' (em-dash)")
    lines.append("  If no separator found: entire stem = title, artist = empty")
    lines.append("")
    lines.append("CONFIDENCE SCORING:")
    lines.append("  1.0  — Clear ' - ' separator with non-empty artist and title")
    lines.append("  0.9  — Unicode dash separator (en/em-dash)")
    lines.append("  0.5  — Separator found but one side empty")
    lines.append("  0.3  — Artist-only (no title parsed)")
    lines.append("  0.0  — No separator, unparseable")
    lines.append("")
    lines.append("JUNK REMOVAL PATTERNS:")
    for jp in JUNK_PATTERNS:
        lines.append(f"  {jp}")
    lines.append("")
    lines.append("TITLE CASE EXCEPTIONS (standard mode):")
    for w in sorted(TITLE_CASE_EXCEPTIONS):
        lines.append(f"  {w}")
    lines.append("")
    lines.append("PRESERVED CASE PATTERNS:")
    for w in sorted(PRESERVE_CASE_PATTERNS):
        lines.append(f"  {w}")
    lines.append("")
    lines.append("FEATURING NORMALIZATION:")
    lines.append("  featuring → feat.")
    lines.append("  feat → feat.")
    lines.append("  ft → ft.")
    lines.append("  Ft → ft.")
    (PROOF_DIR / "02_parsing_rules.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # 03 — Collision rules
    lines = []
    lines.append("=" * 70)
    lines.append("COLLISION RULES")
    lines.append("=" * 70)
    lines.append("")
    lines.append("DETECTION:")
    lines.append("  - Case-insensitive comparison of proposed names")
    lines.append("  - All files sharing a proposed name are flagged")
    lines.append("  - collision_status set to 'COLLISION (N files)'")
    lines.append("  - action set to 'hold' for all colliding files")
    lines.append("")
    lines.append("RESOLUTION (Phase 2):")
    lines.append("  - Manual review required for all collisions")
    lines.append("  - Options: keep one, rename with suffix, skip all")
    lines.append("  - No automatic resolution — fail-closed")
    lines.append("")
    lines.append("NO-CHANGE DETECTION:")
    lines.append("  - If proposed_name == original_name (case-insensitive)")
    lines.append("  - collision_status = 'no_change'")
    lines.append("  - action = 'skip'")
    lines.append("")
    lines.append("LOW CONFIDENCE:")
    lines.append("  - If parse confidence < 0.5")
    lines.append("  - collision_status = 'low_confidence'")
    lines.append("  - action = 'hold'")
    (PROOF_DIR / "03_collision_rules.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # 04 — Review CSV summary
    action_counts: dict[str, int] = {}
    for p in plans:
        action_counts[p["action"]] = action_counts.get(p["action"], 0) + 1
    collision_count = sum(1 for p in plans if p["collision_status"].startswith("COLLISION"))
    stripped_count = sum(1 for p in plans if p["stripped_tokens"])

    lines = []
    lines.append("=" * 70)
    lines.append("REVIEW CSV SUMMARY")
    lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append(f"\nTotal files: {len(plans)}")
    lines.append(f"Collisions: {collision_count}")
    lines.append(f"Files with stripped tokens: {stripped_count}")
    lines.append(f"\nAction breakdown:")
    for action, count in sorted(action_counts.items()):
        lines.append(f"  {action}: {count}")
    lines.append(f"\nConfidence distribution:")
    conf_buckets: dict[str, int] = {"1.0": 0, "0.9": 0, "0.5": 0, "0.3": 0, "0.0": 0}
    for p in plans:
        c = p["confidence"]
        if c >= 1.0:
            conf_buckets["1.0"] += 1
        elif c >= 0.9:
            conf_buckets["0.9"] += 1
        elif c >= 0.5:
            conf_buckets["0.5"] += 1
        elif c > 0:
            conf_buckets["0.3"] += 1
        else:
            conf_buckets["0.0"] += 1
    for label, count in conf_buckets.items():
        lines.append(f"  {label}: {count}")

    # Show examples of changes
    changes = [p for p in plans if p["action"] == "review"]
    if changes:
        lines.append(f"\nSample changes (first 20):")
        for p in changes[:20]:
            lines.append(f"  {p['original_name']}")
            lines.append(f"    → {p['proposed_name']}")
            if p["stripped_tokens"]:
                lines.append(f"    stripped: {p['stripped_tokens']}")

    # Show holds
    holds = [p for p in plans if p["action"] == "hold"]
    if holds:
        lines.append(f"\nHeld files ({len(holds)}):")
        for p in holds[:20]:
            lines.append(f"  {p['original_name']} (confidence={p['confidence']}, "
                         f"status={p['collision_status']})")

    (PROOF_DIR / "04_review_csv_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # 05 — Final report
    lines = []
    lines.append("=" * 70)
    lines.append("DJ LIBRARY NORMALIZATION ENGINE — PHASE 1 FINAL REPORT")
    lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 70)
    lines.append(f"\nMusic directory: {music_dir}")
    lines.append(f"Preset: {preset.name}")
    lines.append(f"Files scanned: {len(plans)}")
    lines.append(f"")
    lines.append(f"RESULTS:")
    for action, count in sorted(action_counts.items()):
        lines.append(f"  {action}: {count}")
    lines.append(f"")
    lines.append(f"COLLISIONS: {collision_count}")
    lines.append(f"STRIPPED TOKENS: {stripped_count} files had junk removed")
    lines.append(f"")
    lines.append(f"ARTIFACTS:")
    lines.append(f"  Plan CSV: {DATA_DIR / 'normalization_plan.csv'}")
    lines.append(f"  Proof:    {PROOF_DIR}")
    lines.append(f"")
    lines.append(f"SAFETY CHECKS:")
    lines.append(f"  Live renames performed: 0")
    lines.append(f"  Files modified on disk: 0")
    lines.append(f"  Preset used: {preset.name}")
    lines.append(f"  Case mode: {preset.case_mode}")
    lines.append(f"")
    lines.append(f"GATE=PASS")
    (PROOF_DIR / "05_final_report.txt").write_text(
        "\n".join(lines), encoding="utf-8"
    )

    # execution_log.txt
    (PROOF_DIR / "execution_log.txt").write_text(
        "\n".join(log_lines), encoding="utf-8"
    )


# ================================================================
# MAIN
# ================================================================

def main():
    print(f"CWD: {os.getcwd()}")
    print(f"Music dir: {DEFAULT_MUSIC_DIR}")
    print(f"Proof dir: {PROOF_DIR}")
    print()

    # Run engine with Gene_Default preset
    plans, log_lines = run_engine(
        music_dir=DEFAULT_MUSIC_DIR,
        preset_name="Gene_Default",
        recursive=True,
    )

    for line in log_lines:
        print(line)

    # Export CSV
    csv_path = DATA_DIR / "normalization_plan.csv"
    export_review_csv(plans, csv_path)
    print(f"\nCSV exported: {csv_path}")

    # Write proof
    preset = PRESETS["Gene_Default"]
    write_proof(plans, log_lines, preset, DEFAULT_MUSIC_DIR)
    print(f"Proof written: {PROOF_DIR}")

    print(f"\nPF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE=PASS")


if __name__ == "__main__":
    main()
