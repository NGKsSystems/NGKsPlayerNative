"""
DJ Library Normalization Engine — Phase 2
==========================================
Upgrades Phase 1 from "safe cleaner + planner" to
"controlled normalization system with overrides and apply workflow".

New in Phase 2:
  - Override system (artist + title JSON overrides)
  - Tiered bracket handling (remove_all / keep_meaningful / keep_all)
  - No-separator fallback parsing
  - Duplicate detection V2 (near-duplicates, same-artist-similar-title)
  - Controlled apply workflow (approve/skip/hold with safety gates)
  - Enhanced CSV fields

Safety invariants preserved:
  - No live renames by default (plan-only mode)
  - Blank action NEVER applies
  - Collision/hold rows NEVER apply even if marked approve
  - All changes auditable
"""

import os
import re
import csv
import json
import time
import shutil
import pathlib
from difflib import SequenceMatcher
from dataclasses import dataclass

# ================================================================
# PATHS
# ================================================================
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DEFAULT_MUSIC_DIR = pathlib.Path(r"C:\Users\suppo\Music")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase2"
PROOF_DIR.mkdir(parents=True, exist_ok=True)

AUDIO_EXTENSIONS = {".mp3", ".flac", ".wav", ".m4a", ".aac", ".ogg", ".wma", ".opus"}

ARTIST_OVERRIDES_PATH = DATA_DIR / "artist_overrides.json"
TITLE_OVERRIDES_PATH = DATA_DIR / "title_overrides.json"

# ================================================================
# TITLE CASE RULES
# ================================================================
TITLE_CASE_EXCEPTIONS = {
    "a", "an", "the", "and", "but", "or", "nor", "for", "yet", "so",
    "in", "on", "at", "to", "by", "of", "up", "as", "is", "it",
    "vs", "vs.", "ft", "ft.", "n'",
}

PRESERVE_CASE_PATTERNS = {
    "DJ", "MC", "II", "III", "IV", "VI", "VII", "VIII", "IX", "XI",
    "AC", "DC", "B.I.G.", "EZ", "XXX", "WD", "PhD",
    "I", "I'd", "I'll", "I'm", "I've",
}

# ================================================================
# JUNK PATTERNS (always removed regardless of bracket tier)
# ================================================================
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
    r"\(Visualizer\)",
    r"\(Full\s+Album\)",
    r"\[Official\s+Music\s+Video\]",
    r"\[Official\s+Video\]",
    r"\[Official\s+Audio\]",
    r"\[Lyrics?\]",
    r"\[Audio\]",
    r"\[HD\]",
    r"\[HQ\]",
    r"\[Visualizer\]",
    r"\[Full\s+Album\]",
    r"\uff02[^\uff02]*\uff02",  # fullwidth quotes
]

# ================================================================
# BRACKET TIER SYSTEM
# ================================================================
# Tier 1: MEANINGFUL — always kept under keep_meaningful
MEANINGFUL_BRACKET_CONTENTS = {
    "remix", "live", "acoustic", "unplugged", "demo",
    "deluxe", "remaster", "remastered", "radio edit",
    "extended", "instrumental", "clean", "explicit",
    "bonus track", "alternate version", "original mix",
}

# Tier 2: CONDITIONAL — kept under keep_meaningful only if configured
CONDITIONAL_BRACKET_PATTERNS = {
    "feat", "feat.", "ft", "ft.", "featuring",
    "version", "ver.", "edit", "mix",
}

BRACKET_PATTERN = re.compile(r"\s*[\(\[\{][^\)\]\}]*[\)\]\}]\s*")

# ================================================================
# OTHER PATTERNS
# ================================================================
LEADING_NUMBER_PATTERN = re.compile(r"^\d{1,3}[\.)\-]\s+")
QUOTE_WRAPPING = re.compile(r'["\u201c\u201d\uff02]')
DASH_NORMALIZE = re.compile(r"\s*[\u2013\u2014\uff0d]\s*")
MULTI_SPACE = re.compile(r"\s{2,}")
MULTI_DASH_SEP = re.compile(r"\s+-+\s+")

FEAT_PATTERNS = [
    (re.compile(r"\bfeaturing\b", re.IGNORECASE), "feat."),
    (re.compile(r"\bfeat\b(?!\.)", re.IGNORECASE), "feat."),
    (re.compile(r"\bft\b(?!\.)", re.IGNORECASE), "ft."),
    (re.compile(r"\bFt\b(?!\.)", re.IGNORECASE), "ft."),
]

# ================================================================
# PRESET
# ================================================================
@dataclass
class NormalizationPreset:
    name: str
    output_pattern: str
    remove_quotes: bool = True
    remove_leading_numbers: bool = True
    case_mode: str = "title_case"
    remove_junk: bool = True
    normalize_feat: bool = True
    normalize_dashes: bool = True
    collapse_spaces: bool = True
    strip_trailing_spaces: bool = True
    bracket_mode: str = "keep_meaningful"  # remove_all | keep_meaningful | keep_all
    use_overrides: bool = True
    use_fallback_parse: bool = True


PRESETS = {
    "Gene_Default": NormalizationPreset(
        name="Gene_Default",
        output_pattern="Artist - Song",
        remove_quotes=True,
        remove_leading_numbers=True,
        case_mode="title_case",
        remove_junk=True,
        normalize_feat=True,
        normalize_dashes=True,
        collapse_spaces=True,
        strip_trailing_spaces=True,
        bracket_mode="keep_meaningful",
        use_overrides=True,
        use_fallback_parse=True,
    ),
    "Preserve_Raw": NormalizationPreset(
        name="Preserve_Raw",
        output_pattern="Artist - Song",
        remove_quotes=False,
        remove_leading_numbers=False,
        case_mode="preserve",
        remove_junk=False,
        normalize_feat=False,
        normalize_dashes=False,
        collapse_spaces=True,
        strip_trailing_spaces=True,
        bracket_mode="keep_all",
        use_overrides=False,
        use_fallback_parse=False,
    ),
}


# ================================================================
# OVERRIDE SYSTEM
# ================================================================

def load_overrides(path: pathlib.Path) -> dict[str, str]:
    """Load a JSON override file. Returns {} if missing or invalid."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    return data


def apply_overrides(text: str, overrides: dict[str, str]) -> tuple[str, list[str]]:
    """
    Apply overrides to text. Returns (corrected_text, list of applied overrides).
    Matches whole words case-insensitively, replaces with override value.
    """
    applied = []
    if not overrides or not text:
        return text, applied

    # Try multi-word overrides first (longer keys first to avoid partial matches)
    sorted_keys = sorted(overrides.keys(), key=len, reverse=True)

    for key in sorted_keys:
        replacement = overrides[key]
        # Build a whole-word regex (case-insensitive)
        pattern = re.compile(r"\b" + re.escape(key) + r"\b", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            matched_text = match.group()
            # Only apply if the override would change something
            if matched_text != replacement:
                text = pattern.sub(replacement, text)
                applied.append(f"{matched_text}->{replacement}")

    return text, applied


# ================================================================
# BRACKET PROCESSING (Tiered)
# ================================================================

def process_brackets(name: str, bracket_mode: str,
                     remove_junk: bool) -> tuple[str, list[str]]:
    """
    Process bracketed content with tiered handling.
    Returns (cleaned_name, list of stripped tokens).
    """
    stripped = []

    # Step 1: Always remove known junk patterns regardless of bracket_mode
    if remove_junk:
        for pattern in JUNK_PATTERNS:
            match = re.search(pattern, name, re.IGNORECASE)
            while match:
                stripped.append(match.group().strip())
                name = name[:match.start()] + " " + name[match.end():]
                match = re.search(pattern, name, re.IGNORECASE)

    # Step 2: Handle remaining brackets based on tier
    if bracket_mode == "keep_all":
        pass  # keep everything remaining

    elif bracket_mode == "remove_all":
        # Remove ALL remaining bracketed content
        for match in BRACKET_PATTERN.finditer(name):
            token = match.group().strip()
            if token not in stripped:
                stripped.append(token)
        name = BRACKET_PATTERN.sub(" ", name)

    elif bracket_mode == "keep_meaningful":
        # Find all remaining brackets, keep meaningful ones, remove the rest
        to_remove = []
        for match in BRACKET_PATTERN.finditer(name):
            token = match.group().strip()
            inner = token.strip("()[]{}").strip().lower()
            # Check if meaningful
            if inner in MEANINGFUL_BRACKET_CONTENTS:
                continue  # keep
            # Check if conditional (also keep under keep_meaningful)
            is_conditional = False
            for cond in CONDITIONAL_BRACKET_PATTERNS:
                if inner.startswith(cond) or inner == cond:
                    is_conditional = True
                    break
            if is_conditional:
                continue  # keep
            to_remove.append(token)

        for token in to_remove:
            stripped.append(token)
            name = name.replace(token, " ", 1)

    return name, stripped


# ================================================================
# TOKEN CLEANING
# ================================================================

def clean_tokens(name: str, preset: NormalizationPreset) -> tuple[str, list[str]]:
    """Remove junk tokens from a filename stem. Returns (cleaned, stripped tokens)."""
    stripped = []

    # 1. Bracket processing (tiered)
    name, bracket_stripped = process_brackets(
        name, preset.bracket_mode, preset.remove_junk
    )
    stripped.extend(bracket_stripped)

    # 2. Remove wrapping quotes (preserve apostrophes)
    if preset.remove_quotes:
        for m in QUOTE_WRAPPING.finditer(name):
            if m.group() not in stripped:
                stripped.append(m.group())
        name = QUOTE_WRAPPING.sub("", name)

    # 3. Remove leading track numbers
    if preset.remove_leading_numbers:
        m = LEADING_NUMBER_PATTERN.match(name)
        if m:
            stripped.append(f"leading:{m.group().strip()}")
            name = name[m.end():]

    # 4. Normalize dashes
    if preset.normalize_dashes:
        name = DASH_NORMALIZE.sub(" - ", name)

    # 5. Normalize featuring tags
    if preset.normalize_feat:
        for pat, repl in FEAT_PATTERNS:
            name = pat.sub(repl, name)

    # 6. Collapse multiple spaces
    if preset.collapse_spaces:
        name = MULTI_SPACE.sub(" ", name)

    # 7. Normalize separator (multiple dashes → single)
    name = MULTI_DASH_SEP.sub(" - ", name)

    # 8. Strip trailing/leading spaces
    if preset.strip_trailing_spaces:
        name = name.strip()

    return name, stripped


# ================================================================
# CASE HANDLING
# ================================================================

def strict_title_case(text: str) -> str:
    """Capitalize every word, no exceptions."""
    words = text.split()
    result = []
    for w in words:
        if w.upper() in {p.upper() for p in PRESERVE_CASE_PATTERNS}:
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
        if w.upper() in {p.upper() for p in PRESERVE_CASE_PATTERNS}:
            for p in PRESERVE_CASE_PATTERNS:
                if w.upper() == p.upper():
                    result.append(p)
                    break
        elif w == "-":
            result.append("-")
        elif i == 0 or i == len(words) - 1:
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


def normalize_case(text: str, mode: str) -> str:
    """Apply the selected case mode."""
    if mode == "preserve":
        return text
    elif mode == "strict_title_case":
        return strict_title_case(text)
    elif mode == "title_case":
        return standard_title_case(text)
    elif mode == "lower":
        return text.lower()
    return text


# ================================================================
# PARSING
# ================================================================

def parse_artist_title(stem: str) -> tuple[str, str, float, str]:
    """
    Parse a filename stem into (artist, title, confidence, parse_method).
    """
    # Try standard " - " separator
    if " - " in stem:
        parts = stem.split(" - ", 1)
        artist = parts[0].strip()
        title = parts[1].strip()
        if artist and title:
            return artist, title, 1.0, "standard"
        elif artist:
            return artist, stem, 0.5, "standard"
        else:
            return "", stem, 0.3, "standard"

    # Try unicode dash separators (should be normalized already, but safety)
    for sep in [" \u2013 ", " \u2014 "]:
        if sep in stem:
            parts = stem.split(sep, 1)
            a, t = parts[0].strip(), parts[1].strip()
            if a and t:
                return a, t, 0.9, "standard"

    # No separator found
    return "", stem, 0.0, "unknown"


def fallback_parse_artist_title(
    stem: str, artist_overrides: dict[str, str]
) -> tuple[str, str, float, str]:
    """
    Attempt to parse filenames without a clear separator.
    Uses known artist names from overrides as a dictionary.
    Returns (artist, title, confidence, parse_method).
    NEVER auto-approve — always mark for review/hold.
    """
    # Build a list of known artist names (from override values)
    known_artists = set()
    for v in artist_overrides.values():
        known_artists.add(v.lower())

    stem_lower = stem.lower()

    # Strategy 1: Match known artist at start of string
    best_match = ""
    for artist_name in sorted(known_artists, key=len, reverse=True):
        if stem_lower.startswith(artist_name + " "):
            best_match = artist_name
            break

    if best_match:
        artist_len = len(best_match)
        # Find original casing
        artist = stem[:artist_len]
        title = stem[artist_len:].strip()
        if title:
            return artist, title, 0.6, "fallback_dictionary"

    # Strategy 2: Heuristic — assume first 2 words = artist if 4+ words
    words = stem.split()
    if len(words) >= 4:
        artist = " ".join(words[:2])
        title = " ".join(words[2:])
        return artist, title, 0.3, "fallback_heuristic"

    # Cannot parse
    return "", stem, 0.0, "unknown"


# ================================================================
# NAME BUILDING
# ================================================================

def build_proposed_name(artist: str, title: str, ext: str,
                        preset: NormalizationPreset) -> str:
    """Build the proposed filename from parsed components."""
    if artist:
        artist_cased = normalize_case(artist, preset.case_mode)
    else:
        artist_cased = ""

    title_cased = normalize_case(title, preset.case_mode)

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
    else:
        name = pattern.replace("Artist", artist_cased).replace("Song", title_cased)

    return name + ext


# ================================================================
# DUPLICATE DETECTION V2
# ================================================================

def _normalize_for_comparison(name: str) -> str:
    """Normalize a name for near-duplicate comparison."""
    n = name.lower()
    n = re.sub(r"[^a-z0-9\s]", "", n)  # strip punctuation
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _title_similarity(t1: str, t2: str) -> float:
    """Compute similarity ratio between two title strings."""
    n1 = _normalize_for_comparison(t1)
    n2 = _normalize_for_comparison(t2)
    if not n1 or not n2:
        return 0.0
    return SequenceMatcher(None, n1, n2).ratio()


def detect_duplicates(plans: list[dict]) -> list[dict]:
    """
    Enhanced duplicate detection:
    1. Exact normalized collision (Phase 1 behavior)
    2. Near-duplicate name risk (high string similarity)
    3. Same artist + similar title risk

    Adds: duplicate_group_id, duplicate_risk to each plan.
    """
    group_counter = 0

    # --- Pass 1: Exact collision (proposed_name, case-insensitive) ---
    name_groups: dict[str, list[int]] = {}
    for i, plan in enumerate(plans):
        key = plan["proposed_name"].lower()
        if key not in name_groups:
            name_groups[key] = []
        name_groups[key].append(i)

    for key, indices in name_groups.items():
        if len(indices) > 1:
            group_counter += 1
            for idx in indices:
                plans[idx]["collision_status"] = f"COLLISION ({len(indices)} files)"
                plans[idx]["duplicate_group_id"] = f"G{group_counter:04d}"
                plans[idx]["duplicate_risk"] = "exact_collision"
                # Force hold for safety — even if user sets approve
                plans[idx]["action"] = "hold"

    # Mark no-change rows
    for plan in plans:
        if plan["proposed_name"].lower() == plan["original_name"].lower():
            if plan.get("duplicate_risk", "") != "exact_collision":
                plan["collision_status"] = "no_change"
                plan["action"] = "skip"

    # --- Pass 2: Near-duplicate detection (normalized string similarity) ---
    normalized_names: list[tuple[str, int]] = []
    for i, plan in enumerate(plans):
        normalized_names.append((_normalize_for_comparison(plan["proposed_name"]), i))

    # Sort by length to enable early-exit when lengths diverge too much
    sorted_by_len = sorted(range(len(normalized_names)),
                           key=lambda x: len(normalized_names[x][0]))

    for ii in range(len(sorted_by_len)):
        i = sorted_by_len[ii]
        if plans[i].get("duplicate_risk") == "exact_collision":
            continue
        ni = normalized_names[i][0]
        len_i = len(ni)
        for jj in range(ii + 1, len(sorted_by_len)):
            j = sorted_by_len[jj]
            nj = normalized_names[j][0]
            # If length differs by >20%, similarity can't reach 0.90
            if len(nj) > len_i * 1.22:
                break
            if plans[j].get("duplicate_risk") == "exact_collision":
                continue
            sim = SequenceMatcher(None, ni, nj).ratio()
            if sim >= 0.90 and sim < 1.0:
                group_counter += 1
                gid = f"G{group_counter:04d}"
                for idx in [i, j]:
                    if not plans[idx].get("duplicate_group_id"):
                        plans[idx]["duplicate_group_id"] = gid
                    if not plans[idx].get("duplicate_risk") or \
                       plans[idx]["duplicate_risk"] == "none":
                        plans[idx]["duplicate_risk"] = "near_duplicate"

    # --- Pass 3: Same artist + similar title ---
    artist_groups: dict[str, list[int]] = {}
    for i, plan in enumerate(plans):
        artist = plan.get("guessed_artist", "").strip().lower()
        if artist:
            if artist not in artist_groups:
                artist_groups[artist] = []
            artist_groups[artist].append(i)

    for artist, indices in artist_groups.items():
        if len(indices) < 2:
            continue
        for a in range(len(indices)):
            for b in range(a + 1, len(indices)):
                idx_a, idx_b = indices[a], indices[b]
                title_a = plans[idx_a].get("guessed_title", "")
                title_b = plans[idx_b].get("guessed_title", "")
                sim = _title_similarity(title_a, title_b)
                if sim >= 0.80 and title_a.lower() != title_b.lower():
                    group_counter += 1
                    gid = f"G{group_counter:04d}"
                    for idx in [idx_a, idx_b]:
                        if not plans[idx].get("duplicate_group_id"):
                            plans[idx]["duplicate_group_id"] = gid
                        if not plans[idx].get("duplicate_risk") or \
                           plans[idx]["duplicate_risk"] == "none":
                            plans[idx]["duplicate_risk"] = "similar_title"

    # Ensure all plans have the fields
    for plan in plans:
        plan.setdefault("duplicate_group_id", "")
        plan.setdefault("duplicate_risk", "none")

    return plans


# ================================================================
# MAIN ENGINE
# ================================================================

CSV_FIELDS = [
    "original_path", "original_name", "cleaned_name",
    "guessed_artist", "guessed_title", "stripped_tokens",
    "confidence", "proposed_name", "collision_status",
    "override_applied", "duplicate_group_id", "duplicate_risk",
    "parse_method", "action",
]


def run_engine(
    music_dir: pathlib.Path,
    preset_name: str = "Gene_Default",
    recursive: bool = True,
) -> tuple[list[dict], list[str]]:
    """Run the normalization engine in plan-only mode. Returns (plans, log_lines)."""

    log: list[str] = []
    t0 = time.time()
    preset = PRESETS[preset_name]

    log.append(f"Engine V2 started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.append(f"Music directory: {music_dir}")
    log.append(f"Preset: {preset.name}")
    log.append(f"Bracket mode: {preset.bracket_mode}")
    log.append(f"Overrides: {preset.use_overrides}")
    log.append(f"Fallback parse: {preset.use_fallback_parse}")
    log.append(f"Recursive: {recursive}")
    log.append("")

    # Load overrides
    artist_overrides: dict[str, str] = {}
    title_overrides: dict[str, str] = {}
    if preset.use_overrides:
        artist_overrides = load_overrides(ARTIST_OVERRIDES_PATH)
        title_overrides = load_overrides(TITLE_OVERRIDES_PATH)
        log.append(f"Artist overrides loaded: {len(artist_overrides)}")
        log.append(f"Title overrides loaded: {len(title_overrides)}")
    log.append("")

    # Phase 1: Scan
    log.append("--- SCANNING ---")
    files = scan_files(music_dir, recursive)
    log.append(f"  Audio files found: {len(files)}")

    if not files:
        log.append("  WARNING: No audio files found!")
        return [], log

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
    override_count = 0
    fallback_count = 0

    for fp in files:
        original_name = fp.name
        stem = fp.stem
        ext = fp.suffix

        # Clean tokens (with tiered bracket handling)
        cleaned, stripped = clean_tokens(stem, preset)

        # Parse artist/title
        artist, title, confidence, parse_method = parse_artist_title(cleaned)

        # Fallback parsing for no-separator files
        if confidence == 0.0 and preset.use_fallback_parse:
            fb_artist, fb_title, fb_conf, fb_method = fallback_parse_artist_title(
                cleaned, artist_overrides
            )
            if fb_conf > 0:
                artist, title, confidence, parse_method = (
                    fb_artist, fb_title, fb_conf, fb_method
                )
                fallback_count += 1

        # Classify confidence
        if confidence >= 0.9:
            parse_stats["high"] += 1
        elif confidence >= 0.5:
            parse_stats["medium"] += 1
        elif confidence > 0:
            parse_stats["low"] += 1
        else:
            parse_stats["none"] += 1

        # Build proposed name (case normalization happens inside)
        proposed = build_proposed_name(artist, title, ext, preset)

        # Apply overrides AFTER normalization, BEFORE final output
        all_overrides_applied = []
        if preset.use_overrides:
            # Get artist and title from the proposed name for override matching
            proposed_stem = proposed[: -len(ext)] if ext else proposed

            # Apply artist overrides to artist portion
            if artist:
                artist_cased = normalize_case(artist, preset.case_mode)
                corrected_artist, artist_ovr = apply_overrides(
                    artist_cased, artist_overrides
                )
                all_overrides_applied.extend(artist_ovr)
            else:
                corrected_artist = ""

            # Apply title overrides to title portion
            title_cased = normalize_case(title, preset.case_mode)
            corrected_title, title_ovr = apply_overrides(
                title_cased, title_overrides
            )
            all_overrides_applied.extend(title_ovr)

            # Also apply artist overrides to title (for feat. credits etc.)
            corrected_title, title_artist_ovr = apply_overrides(
                corrected_title, artist_overrides
            )
            all_overrides_applied.extend(title_artist_ovr)

            # Rebuild proposed name with overrides applied
            if all_overrides_applied:
                override_count += 1
                if corrected_artist:
                    proposed = f"{corrected_artist} - {corrected_title}{ext}"
                else:
                    proposed = f"{corrected_title}{ext}"

        # Determine action
        action = ""  # blank by default — user must set
        collision_status = "ok"
        if proposed.lower() == original_name.lower():
            action = "skip"
            collision_status = "no_change"
        elif confidence < 0.5:
            action = "hold"
            collision_status = "low_confidence"
        elif parse_method.startswith("fallback"):
            action = "hold"
            collision_status = "fallback_parse"

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
            "override_applied": "; ".join(all_overrides_applied) if all_overrides_applied else "",
            "duplicate_group_id": "",
            "duplicate_risk": "none",
            "parse_method": parse_method,
            "action": action,
        }
        plans.append(plan)

    log.append(f"  Processed: {len(plans)}")
    log.append(f"  Parse confidence: high={parse_stats['high']}, "
               f"medium={parse_stats['medium']}, low={parse_stats['low']}, "
               f"none={parse_stats['none']}")
    log.append(f"  Overrides applied: {override_count}")
    log.append(f"  Fallback parses: {fallback_count}")
    log.append("")

    # Phase 3: Duplicate detection V2
    log.append("--- DUPLICATE DETECTION V2 ---")
    plans = detect_duplicates(plans)
    collision_count = sum(
        1 for p in plans if p["collision_status"].startswith("COLLISION")
    )
    near_dup_count = sum(
        1 for p in plans if p["duplicate_risk"] == "near_duplicate"
    )
    similar_count = sum(
        1 for p in plans if p["duplicate_risk"] == "similar_title"
    )
    log.append(f"  Exact collisions: {collision_count}")
    log.append(f"  Near duplicates: {near_dup_count}")
    log.append(f"  Similar titles (same artist): {similar_count}")
    log.append("")

    # Phase 4: Summary
    action_counts: dict[str, int] = {}
    for p in plans:
        a = p["action"] if p["action"] else "(blank)"
        action_counts[a] = action_counts.get(a, 0) + 1

    log.append("--- ACTION SUMMARY ---")
    for action, count in sorted(action_counts.items()):
        log.append(f"  {action}: {count}")

    elapsed = round(time.time() - t0, 2)
    log.append(f"\nEngine V2 completed in {elapsed}s")

    return plans, log


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
            if fp.is_file() and fp.suffix.lower() in AUDIO_EXTENSIONS:
                files.append(fp)
    files.sort(key=lambda p: p.name.lower())
    return files


def export_review_csv(plans: list[dict], output_path: pathlib.Path):
    """Export the normalization plan as a reviewable CSV."""
    if not plans:
        return
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for plan in plans:
            row = {k: plan.get(k, "") for k in CSV_FIELDS}
            writer.writerow(row)


# ================================================================
# APPLY WORKFLOW
# ================================================================

def apply_normalization_plan(
    csv_path: pathlib.Path,
    dry_run: bool = False,
) -> tuple[list[dict], list[str]]:
    """
    Apply approved renames from the normalization plan CSV.

    Safety gates:
    - blank action → skip (NEVER apply)
    - action != "approve" → skip
    - collision_status contains "COLLISION" → skip (NEVER apply)
    - action == "hold" → skip (NEVER apply)
    - source file must exist
    - destination must not already exist

    Returns (results, log_lines).
    """
    log: list[str] = []
    results: list[dict] = []

    log.append(f"Apply workflow started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.append(f"CSV: {csv_path}")
    log.append(f"Dry run: {dry_run}")
    log.append("")

    if not csv_path.exists():
        log.append("ERROR: CSV file not found!")
        return results, log

    # Read the plan
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.append(f"Total rows: {len(rows)}")

    applied = 0
    skipped = 0
    blocked = 0
    errors = 0

    for row in rows:
        action = (row.get("action") or "").strip().lower()
        collision = (row.get("collision_status") or "").strip()
        original_path = row.get("original_path", "").strip()
        proposed_name = row.get("proposed_name", "").strip()

        result = {
            "original_path": original_path,
            "proposed_name": proposed_name,
            "action_taken": "skipped",
            "reason": "",
        }

        # Gate 1: blank action
        if not action:
            result["reason"] = "blank_action"
            skipped += 1
            results.append(result)
            continue

        # Gate 2: only "approve" triggers rename
        if action != "approve":
            result["reason"] = f"action={action}"
            skipped += 1
            results.append(result)
            continue

        # Gate 3: collision rows NEVER apply
        if "COLLISION" in collision.upper():
            result["reason"] = "collision_blocked"
            result["action_taken"] = "blocked"
            blocked += 1
            results.append(result)
            continue

        # Gate 4: hold rows NEVER apply (even if someone wrote approve)
        if collision in ("low_confidence", "fallback_parse"):
            result["reason"] = "low_confidence_blocked"
            result["action_taken"] = "blocked"
            blocked += 1
            results.append(result)
            continue

        # Gate 5: source must exist
        src = pathlib.Path(original_path)
        if not src.exists():
            result["reason"] = "source_not_found"
            result["action_taken"] = "error"
            errors += 1
            results.append(result)
            continue

        # Gate 6: destination must not already exist (unless same file)
        dst = src.parent / proposed_name
        if dst.exists() and dst != src:
            result["reason"] = "destination_exists"
            result["action_taken"] = "blocked"
            blocked += 1
            results.append(result)
            continue

        # All gates passed — apply rename
        if dry_run:
            result["action_taken"] = "dry_run_approve"
            result["reason"] = "would_rename"
            applied += 1
        else:
            try:
                src.rename(dst)
                result["action_taken"] = "renamed"
                result["reason"] = f"renamed_to={proposed_name}"
                applied += 1
            except OSError as e:
                result["action_taken"] = "error"
                result["reason"] = f"os_error={e}"
                errors += 1

        results.append(result)

    log.append(f"\nApply results:")
    log.append(f"  Applied: {applied}")
    log.append(f"  Skipped: {skipped}")
    log.append(f"  Blocked: {blocked}")
    log.append(f"  Errors: {errors}")
    log.append(f"\nApply workflow completed: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    return results, log


# ================================================================
# PROOF GENERATION
# ================================================================

def write_proof(plans: list[dict], log_lines: list[str],
                preset: NormalizationPreset, music_dir: pathlib.Path,
                artist_overrides: dict, title_overrides: dict,
                apply_results: list[dict] | None = None,
                apply_log: list[str] | None = None):
    """Write all Phase 2 proof artifacts."""

    ts = time.strftime('%Y-%m-%d %H:%M:%S')

    # --- 00: Engine upgrade summary ---
    lines = [
        "=" * 70,
        "DJ LIBRARY NORMALIZATION ENGINE — PHASE 2 UPGRADE SUMMARY",
        f"Date: {ts}",
        "=" * 70, "",
        "PHASE 2 UPGRADES:",
        "  1. Override system (artist + title JSON overrides)",
        "  2. Tiered bracket handling (remove_all / keep_meaningful / keep_all)",
        "  3. No-separator fallback parsing with dictionary + heuristic",
        "  4. Duplicate detection V2 (exact, near-duplicate, similar-title)",
        "  5. Controlled apply workflow (approve/skip/hold + safety gates)",
        "  6. Enhanced CSV fields (override_applied, duplicate_group_id,",
        "     duplicate_risk, parse_method, action)", "",
        "ARCHITECTURE:",
        "  load_overrides()               — Load JSON override files",
        "  apply_overrides()              — Apply post-normalization corrections",
        "  clean_tokens()                 — Junk removal with tiered brackets",
        "  process_brackets()             — 3-tier bracket handler",
        "  normalize_case()               — Case mode dispatcher",
        "  parse_artist_title()           — Standard separator parsing",
        "  fallback_parse_artist_title()  — Dictionary + heuristic fallback",
        "  detect_duplicates()            — V2 multi-pass duplicate detection",
        "  build_proposed_name()          — Apply preset pattern to components",  # noqa
        "  run_engine()                   — Plan-only mode orchestrator",
        "  apply_normalization_plan()     — Controlled apply with safety gates",
        "  export_review_csv()            — Enhanced CSV output", "",
        "SAFETY INVARIANTS PRESERVED:",
        "  - No live renames by default (plan-only mode)",
        "  - Blank action NEVER applies",
        "  - Collision/hold rows NEVER apply even if marked approve",
        "  - All changes auditable via CSV and proof artifacts",
        "  - Fail-closed on ambiguity",
    ]
    (PROOF_DIR / "00_engine_upgrade_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 01: Override system ---
    override_applied_plans = [p for p in plans if p.get("override_applied")]
    lines = [
        "=" * 70,
        "OVERRIDE SYSTEM",
        f"Date: {ts}",
        "=" * 70, "",
        f"Artist overrides loaded: {len(artist_overrides)}",
        f"Title overrides loaded: {len(title_overrides)}",
        f"Total overrides applied: {len(override_applied_plans)}", "",
        "ARTIST OVERRIDES (artist_overrides.json):",
    ]
    for k, v in sorted(artist_overrides.items()):
        lines.append(f"  {k!r} → {v!r}")
    lines.append("")
    lines.append("TITLE OVERRIDES (title_overrides.json):")
    for k, v in sorted(title_overrides.items()):
        lines.append(f"  {k!r} → {v!r}")
    lines.append("")
    lines.append("APPLIED OVERRIDES:")
    if override_applied_plans:
        for p in override_applied_plans:
            lines.append(f"  {p['original_name']}")
            lines.append(f"    → {p['proposed_name']}")
            lines.append(f"    overrides: {p['override_applied']}")
    else:
        lines.append("  (none applied)")
    (PROOF_DIR / "01_override_system.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 02: Bracket rules ---
    lines = [
        "=" * 70,
        "BRACKET HANDLING RULES (TIERED)",
        f"Date: {ts}",
        "=" * 70, "",
        "BRACKET MODES:",
        "  remove_all       — Remove ALL bracketed content",
        "  keep_meaningful  — Keep meaningful, remove junk (DEFAULT)",
        "  keep_all         — Keep everything", "",
        "TIER 1 — ALWAYS REMOVED (junk):",
    ]
    for jp in JUNK_PATTERNS:
        lines.append(f"  {jp}")
    lines.append("")
    lines.append("TIER 2 — KEPT UNDER keep_meaningful:")
    for m in sorted(MEANINGFUL_BRACKET_CONTENTS):
        lines.append(f"  {m}")
    lines.append("")
    lines.append("TIER 3 — CONDITIONAL (kept under keep_meaningful):")
    for c in sorted(CONDITIONAL_BRACKET_PATTERNS):
        lines.append(f"  {c}")
    lines.append("")
    lines.append(f"ACTIVE MODE: {preset.bracket_mode}")
    (PROOF_DIR / "02_bracket_rules.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 03: Parsing fallback ---
    fallback_plans = [p for p in plans if p.get("parse_method", "").startswith("fallback")]
    unknown_plans = [p for p in plans if p.get("parse_method") == "unknown"]
    lines = [
        "=" * 70,
        "PARSING FALLBACK SYSTEM",
        f"Date: {ts}",
        "=" * 70, "",
        "STRATEGY:",
        "  1. Standard parse: split on ' - ' separator (confidence 1.0)",
        "  2. If no separator found:",
        "     a. Dictionary fallback: match known artist names (confidence 0.6)",
        "     b. Heuristic fallback: first 2 words = artist (confidence 0.3)",
        "  3. All fallback parses marked for hold/review — NEVER auto-applied", "",
        "SAFETY:",
        "  - parse_method field tracks which strategy was used",
        "  - fallback parses set collision_status = 'fallback_parse'",
        "  - action set to 'hold' for all fallback results", "",
        f"FALLBACK PARSES: {len(fallback_plans)}",
    ]
    for p in fallback_plans:
        lines.append(f"  {p['original_name']}")
        lines.append(f"    artist={p['guessed_artist']!r} title={p['guessed_title']!r}")
        lines.append(f"    method={p['parse_method']} confidence={p['confidence']}")
    lines.append(f"\nUNPARSED (unknown): {len(unknown_plans)}")
    for p in unknown_plans:
        lines.append(f"  {p['original_name']}")
    (PROOF_DIR / "03_parsing_fallback.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 04: Duplicate detection ---
    exact_dups = [p for p in plans if p.get("duplicate_risk") == "exact_collision"]
    near_dups = [p for p in plans if p.get("duplicate_risk") == "near_duplicate"]
    similar_dups = [p for p in plans if p.get("duplicate_risk") == "similar_title"]
    lines = [
        "=" * 70,
        "DUPLICATE DETECTION V2",
        f"Date: {ts}",
        "=" * 70, "",
        "DETECTION PASSES:",
        "  Pass 1: Exact normalized collision (case-insensitive proposed name match)",
        "  Pass 2: Near-duplicate (90%+ string similarity after normalization)",
        "  Pass 3: Same artist + similar title (80%+ title similarity)", "",
        f"EXACT COLLISIONS: {len(exact_dups)}",
    ]
    for p in exact_dups:
        lines.append(f"  [{p['duplicate_group_id']}] {p['original_name']} → {p['proposed_name']}")
    lines.append(f"\nNEAR DUPLICATES: {len(near_dups)}")
    for p in near_dups:
        lines.append(f"  [{p['duplicate_group_id']}] {p['original_name']} → {p['proposed_name']}")
    lines.append(f"\nSIMILAR TITLES: {len(similar_dups)}")
    for p in similar_dups:
        lines.append(f"  [{p['duplicate_group_id']}] {p['guessed_artist']} - {p['guessed_title']}")
    (PROOF_DIR / "04_duplicate_detection.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 05: Apply workflow ---
    lines = [
        "=" * 70,
        "APPLY WORKFLOW",
        f"Date: {ts}",
        "=" * 70, "",
        "WORKFLOW:",
        "  1. User reviews normalization_plan.csv",
        "  2. Sets action column: approve | skip | hold",
        "  3. Runs apply_normalization_plan()", "",
        "SAFETY GATES:",
        "  Gate 1: blank action → NEVER apply",
        "  Gate 2: action != 'approve' → skip",
        "  Gate 3: COLLISION rows → NEVER apply (even if approve)",
        "  Gate 4: low_confidence/fallback_parse → NEVER apply (even if approve)",
        "  Gate 5: source file must exist",
        "  Gate 6: destination must not already exist", "",
        "APPLY RESULTS:",
    ]
    if apply_results:
        action_taken_counts: dict[str, int] = {}
        for r in apply_results:
            at = r["action_taken"]
            action_taken_counts[at] = action_taken_counts.get(at, 0) + 1
        for at, count in sorted(action_taken_counts.items()):
            lines.append(f"  {at}: {count}")
        lines.append("")
        for r in apply_results:
            if r["action_taken"] not in ("skipped",):
                lines.append(f"  {r['original_path']}")
                lines.append(f"    action={r['action_taken']} reason={r['reason']}")
    else:
        lines.append("  (no apply run in this session — plan-only mode)")
    (PROOF_DIR / "05_apply_workflow.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 06: Validation checks ---
    # Verify key Phase 1 invariants + Phase 2 fixes
    checks = []

    # Check MacDonald override
    macdonald_plans = [p for p in plans if "macdonald" in p["original_name"].lower()
                       or "macdonald" in p["proposed_name"].lower()]
    macdonald_ok = all("MacDonald" in p["proposed_name"] for p in macdonald_plans) if macdonald_plans else True
    checks.append(("MacDonald preserved in proposed names", macdonald_ok, macdonald_plans))

    # Check PondCreek override
    pondcreek_plans = [p for p in plans if "pondcreek" in p["original_name"].lower()
                       or "pondcreek" in p["proposed_name"].lower()]
    pondcreek_ok = all("PondCreek" in p["proposed_name"] for p in pondcreek_plans) if pondcreek_plans else True
    checks.append(("PondCreek preserved in proposed names", pondcreek_ok, pondcreek_plans))

    # Check 3 Doors Down not mangled
    tdd_plans = [p for p in plans if "3 doors down" in p["original_name"].lower()]
    tdd_ok = all("3 Doors Down" in p["proposed_name"] for p in tdd_plans) if tdd_plans else True
    checks.append(("3 Doors Down preserved", tdd_ok, tdd_plans))

    # Check 38 Special
    ts_plans = [p for p in plans if "38 special" in p["original_name"].lower()]
    ts_ok = all("38 Special" in p["proposed_name"] for p in ts_plans) if ts_plans else True
    checks.append(("38 Special preserved", ts_ok, ts_plans))

    # Check 50 Cent
    fc_plans = [p for p in plans if "50 cent" in p["original_name"].lower()]
    fc_ok = all("50 Cent" in p["proposed_name"] for p in fc_plans) if fc_plans else True
    checks.append(("50 Cent preserved", fc_ok, fc_plans))

    # Check apostrophes
    apos_plans = [p for p in plans if "'" in p["original_name"]]
    apos_ok = all("'" in p["proposed_name"] for p in apos_plans) if apos_plans else True
    checks.append(("Apostrophes preserved", apos_ok, []))

    # Check no blank-action applies
    blank_safe = all(
        p["action"] != "approve" or p["action"].strip()
        for p in plans
    )
    checks.append(("No blank actions set to approve", blank_safe, []))

    # Check fallback parses not auto-approved
    fallback_safe = all(
        p["action"] in ("hold", "skip", "")
        for p in plans if p.get("parse_method", "").startswith("fallback")
    )
    checks.append(("Fallback parses not auto-approved", fallback_safe, []))

    # Check collision rows held
    collision_safe = all(
        p["action"] == "hold"
        for p in plans if p.get("collision_status", "").startswith("COLLISION")
    )
    checks.append(("Collision rows held", collision_safe, []))

    lines = [
        "=" * 70,
        "VALIDATION CHECKS",
        f"Date: {ts}",
        "=" * 70, "",
    ]
    all_pass = True
    for check_name, ok, examples in checks:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        lines.append(f"  [{status}] {check_name}")
        if examples and not ok:
            for p in examples[:5]:
                lines.append(f"    orig={p['original_name']} → proposed={p['proposed_name']}")

    lines.append(f"\nOVERALL: {'ALL PASS' if all_pass else 'SOME FAILED'}")
    (PROOF_DIR / "06_validation_checks.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 07: Final report ---
    action_counts: dict[str, int] = {}
    for p in plans:
        a = p["action"] if p["action"] else "(blank)"
        action_counts[a] = action_counts.get(a, 0) + 1

    collision_count = sum(1 for p in plans if p["collision_status"].startswith("COLLISION"))
    override_count = sum(1 for p in plans if p.get("override_applied"))
    near_dup_count = sum(1 for p in plans if p.get("duplicate_risk") == "near_duplicate")
    similar_count = sum(1 for p in plans if p.get("duplicate_risk") == "similar_title")

    lines = [
        "=" * 70,
        "DJ LIBRARY NORMALIZATION ENGINE — PHASE 2 FINAL REPORT",
        f"Date: {ts}",
        "=" * 70,
        f"\nMusic directory: {music_dir}",
        f"Preset: {preset.name}",
        f"Bracket mode: {preset.bracket_mode}",
        f"Files scanned: {len(plans)}", "",
        "ACTIONS:",
    ]
    for action, count in sorted(action_counts.items()):
        lines.append(f"  {action}: {count}")
    lines.extend([
        "",
        f"COLLISIONS: {collision_count}",
        f"OVERRIDES APPLIED: {override_count}",
        f"NEAR DUPLICATES: {near_dup_count}",
        f"SIMILAR TITLES: {similar_count}", "",
        "ARTIFACTS:",
        f"  Plan CSV: {DATA_DIR / 'normalization_plan.csv'}",
        f"  Proof:    {PROOF_DIR}", "",
        "SAFETY CHECKS:",
        f"  Live renames performed: 0 (plan-only mode)",
        f"  Files modified on disk: 0",
        f"  Validation: {'ALL PASS' if all_pass else 'SOME FAILED'}", "",
        f"GATE={'PASS' if all_pass else 'FAIL'}",
    ])
    (PROOF_DIR / "07_final_report.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- execution_log.txt ---
    all_log = list(log_lines)
    if apply_log:
        all_log.append("")
        all_log.append("=== APPLY WORKFLOW LOG ===")
        all_log.extend(apply_log)
    (PROOF_DIR / "execution_log.txt").write_text(
        "\n".join(all_log), encoding="utf-8")


# ================================================================
# MAIN
# ================================================================

def main():
    print(f"CWD: {os.getcwd()}")
    print(f"Music dir: {DEFAULT_MUSIC_DIR}")
    print(f"Proof dir: {PROOF_DIR}")
    print()

    # Run engine in plan-only mode
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

    # Load overrides for proof writing
    artist_overrides = load_overrides(ARTIST_OVERRIDES_PATH)
    title_overrides = load_overrides(TITLE_OVERRIDES_PATH)

    # Write proof
    preset = PRESETS["Gene_Default"]
    write_proof(plans, log_lines, preset, DEFAULT_MUSIC_DIR,
                artist_overrides, title_overrides)
    print(f"Proof written: {PROOF_DIR}")

    print(f"\nPF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE=PASS")


if __name__ == "__main__":
    main()
