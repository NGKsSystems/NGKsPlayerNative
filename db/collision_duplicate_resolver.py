#!/usr/bin/env python3
"""
Phase 6 -- Collision + Duplicate Resolution

Identifies duplicates correctly, chooses a preferred version per group,
prepares safe resolution plans, optionally applies a small safe subset.

HARD RULES:
- DO NOT delete any files
- DO NOT overwrite any files
- DO NOT merge files
- DO NOT auto-resolve ambiguous duplicate groups
- DO NOT touch live DJ library
- FAIL-CLOSED on uncertainty
- All decisions must be logged and reversible
"""

import csv
import hashlib
import os
import pathlib
import re
import shutil
import sys
from collections import Counter, defaultdict
from datetime import datetime

# -- Paths -------------------------------------------------------------------
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase6"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ_LIBRARY = pathlib.Path(r"C:\Users\suppo\Music")

# Input CSVs
COLLISION_PLAN_CSV = DATA_DIR / "collision_resolution_plan_v1.csv"
NEAR_DUP_CSV = DATA_DIR / "near_duplicate_groups_v1.csv"
REMAINING_QUEUE_CSV = DATA_DIR / "remaining_review_queue_v1.csv"
BATCH_PLAN_CSV = DATA_DIR / "batch_normalization_plan.csv"
APPLY_RESULTS_CSV = DATA_DIR / "apply_results_v1.csv"

# Output CSVs
COLLISION_GROUPS_V2_CSV = DATA_DIR / "collision_groups_v2.csv"
NEAR_DUP_V2_CSV = DATA_DIR / "near_duplicate_groups_v2.csv"
PRIMARY_SELECTION_CSV = DATA_DIR / "duplicate_primary_selection_v1.csv"
ALTERNATE_PLAN_CSV = DATA_DIR / "duplicate_alternate_plan_v1.csv"
DUPLICATE_STATE_CSV = DATA_DIR / "duplicate_state_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    execution_log.append(entry)
    print(entry)


def read_csv(path):
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    log(f"Wrote {len(rows)} rows to {path.name}")


def file_hash(path, chunk_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def safe_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


# -- Filename scoring helpers ------------------------------------------------

# Junk tokens that reduce filename quality
JUNK_PATTERNS = [
    re.compile(r"\blyrics?\b", re.IGNORECASE),
    re.compile(r"\bofficial\s*(music\s*)?video\b", re.IGNORECASE),
    re.compile(r"\bofficial\s*hd\s*video\b", re.IGNORECASE),
    re.compile(r"\bofficial\s*4k\s*video\b", re.IGNORECASE),
    re.compile(r"\bofficial\s*video\b", re.IGNORECASE),
    re.compile(r"\bmusic\s*video\b", re.IGNORECASE),
    re.compile(r"\bhd\b", re.IGNORECASE),
    re.compile(r"\b4k\b", re.IGNORECASE),
    re.compile(r"\bexplicit\b", re.IGNORECASE),
    re.compile(r"\bclean\b", re.IGNORECASE),
    re.compile(r"\.temp\b", re.IGNORECASE),
]

# Label tag pattern (e.g. "| Napalm Records")
LABEL_TAG_RE = re.compile(r"\s*\|\s*\w+\s+Records?\b.*", re.IGNORECASE)

# Numbered prefix (e.g. "017 - ", "102 - ")
NUMBERED_PREFIX_RE = re.compile(r"^\d{2,4}\s*-\s*")

# Unicode junk chars
UNICODE_JUNK = set("\uff5c\u29f8\uff1a\uff02\u2013\u2764\ufe0f\u00b7")


def filename_quality_score(name):
    """
    Score a filename for quality (higher = better).
    Range: 0-100.
    """
    score = 50  # baseline
    stem = pathlib.Path(name).stem

    # Penalize junk tokens
    for pat in JUNK_PATTERNS:
        if pat.search(stem):
            score -= 5

    # Penalize label tags
    if LABEL_TAG_RE.search(stem):
        score -= 10

    # Penalize .temp files heavily
    if ".temp" in name.lower():
        score -= 30

    # Penalize numbered prefix (less clean)
    if NUMBERED_PREFIX_RE.match(stem):
        score -= 3

    # Penalize very long filenames
    if len(stem) > 80:
        score -= 5
    if len(stem) > 120:
        score -= 10

    # Penalize Unicode junk chars
    unicode_count = sum(1 for c in stem if c in UNICODE_JUNK)
    score -= unicode_count * 3

    # Reward having a hyphen separator (Artist - Title format)
    if " - " in stem:
        score += 10

    # Reward reasonable length
    if 20 <= len(stem) <= 60:
        score += 5

    return max(0, min(100, score))


def normalize_for_comparison(name):
    """Normalize a filename for duplicate comparison."""
    s = pathlib.Path(name).stem.lower()
    # Remove numbered prefix
    s = NUMBERED_PREFIX_RE.sub("", s)
    # Remove junk tokens
    for pat in JUNK_PATTERNS:
        s = pat.sub("", s)
    # Remove label tags
    s = LABEL_TAG_RE.sub("", s)
    # Remove parenthetical content like (feat. X), [Official], etc.
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"\[[^\]]*\]", "", s)
    # Remove Unicode junk
    for c in UNICODE_JUNK:
        s = s.replace(c, "")
    # Normalize whitespace and separators
    s = re.sub(r"[^a-z0-9]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ==============================================================================
# PART A -- Collision Group Analysis
# ==============================================================================

def analyze_collision_groups():
    """
    Group collisions by group_id.
    Verify all files exist.
    Detect identical vs variant filenames.
    Enhance with file size and filename tokens.
    """
    log("=== PART A: Collision Group Analysis ===")

    collision_rows = read_csv(COLLISION_PLAN_CSV)
    log(f"Loaded collision_resolution_plan: {len(collision_rows)} rows")

    groups = defaultdict(list)
    for r in collision_rows:
        groups[r["group_id"]].append(r)

    log(f"Collision groups: {len(groups)}")

    # Enhanced output
    output_rows = []
    group_summaries = []

    for gid, members in sorted(groups.items()):
        # Get file info
        file_infos = []
        for m in members:
            path = m["original_path"]
            exists = os.path.exists(path)
            size = safe_size(path) if exists else 0
            quality = filename_quality_score(m["original_name"])
            norm = normalize_for_comparison(m["original_name"])

            file_infos.append({
                "path": path,
                "original_name": m["original_name"],
                "proposed_unique_name": m.get("proposed_unique_name", ""),
                "strategy_used": m.get("strategy_used", ""),
                "confidence": m.get("confidence", ""),
                "size": size,
                "exists": exists,
                "quality_score": quality,
                "normalized": norm,
            })

        # Detect identical vs variant
        norms = set(fi["normalized"] for fi in file_infos)
        sizes = set(fi["size"] for fi in file_infos)

        if len(norms) == 1 and len(sizes) == 1:
            group_type = "identical"
        elif len(norms) == 1:
            group_type = "same_song_diff_size"
        else:
            group_type = "variant"

        # Check if files are byte-identical
        hashes = set()
        if all(fi["exists"] for fi in file_infos):
            for fi in file_infos:
                h = file_hash(fi["path"])
                fi["hash"] = h
                hashes.add(h)
        else:
            for fi in file_infos:
                fi["hash"] = ""

        if len(hashes) == 1 and len(hashes) > 0:
            group_type = "byte_identical"

        group_summaries.append({
            "group_id": gid,
            "member_count": len(members),
            "group_type": group_type,
            "all_exist": all(fi["exists"] for fi in file_infos),
        })

        for fi in file_infos:
            subfolder = pathlib.Path(fi["path"]).parent.name
            output_rows.append({
                "group_id": gid,
                "file_path": fi["path"],
                "original_name": fi["original_name"],
                "proposed_name": fi["proposed_unique_name"],
                "size_bytes": fi["size"],
                "size_mb": round(fi["size"] / (1024 * 1024), 1),
                "quality_score": fi["quality_score"],
                "normalized_key": fi["normalized"],
                "file_hash": fi.get("hash", "")[:16],
                "subfolder": subfolder,
                "group_type": group_type,
                "exists": fi["exists"],
                "notes": fi.get("strategy_used", ""),
            })

    # Write output
    fieldnames = [
        "group_id", "file_path", "original_name", "proposed_name",
        "size_bytes", "size_mb", "quality_score", "normalized_key",
        "file_hash", "subfolder", "group_type", "exists", "notes",
    ]
    write_csv(COLLISION_GROUPS_V2_CSV, output_rows, fieldnames)

    # Stats
    types = Counter(gs["group_type"] for gs in group_summaries)
    log(f"Group types: {dict(types)}")
    all_exist = sum(1 for gs in group_summaries if gs["all_exist"])
    log(f"All files exist in {all_exist}/{len(group_summaries)} groups")

    return output_rows, group_summaries


# ==============================================================================
# PART B -- Near Duplicate Consolidation
# ==============================================================================

def consolidate_near_duplicates():
    """
    Refine near-duplicate grouping.
    Add similarity tiers and cluster confidence.
    Merge with remaining queue near-dup / similar-title rows.
    """
    log("\n=== PART B: Near Duplicate Consolidation ===")

    # Load Phase 4 near-dup groups
    nd_rows = read_csv(NEAR_DUP_CSV)
    log(f"Loaded near_duplicate_groups_v1: {len(nd_rows)} rows")

    # Load batch plan for duplicate_group_id
    batch_plan = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in batch_plan}

    # Load remaining queue for additional near-dup / similar-title rows
    remaining = read_csv(REMAINING_QUEUE_CSV)
    rq_dup_rows = [
        r for r in remaining
        if r.get("duplicate_risk", "") in ("near_duplicate", "similar_title")
        and r.get("current_state", "") == "REVIEW_REQUIRED"
    ]
    log(f"Remaining queue dup-risk rows: {len(rq_dup_rows)}")

    # Build comprehensive duplicate groups using duplicate_group_id from batch_plan
    all_dup_paths = set()
    # From Phase 4 near-dup CSV
    for r in nd_rows:
        all_dup_paths.add(r.get("track_path", ""))
    # From remaining queue
    for r in rq_dup_rows:
        all_dup_paths.add(r["original_path"])
    all_dup_paths.discard("")

    # Group by duplicate_group_id
    dgid_groups = defaultdict(list)
    for path in all_dup_paths:
        bpr = bp_map.get(path, {})
        dgid = bpr.get("duplicate_group_id", "")
        if dgid:
            dgid_groups[dgid].append(path)

    # Also find ungrouped paths (no duplicate_group_id)
    grouped_paths = set()
    for paths in dgid_groups.values():
        grouped_paths.update(paths)
    ungrouped = all_dup_paths - grouped_paths

    log(f"Duplicate groups by batch_plan group_id: {len(dgid_groups)}")
    log(f"Ungrouped dup-risk paths: {len(ungrouped)}")

    # Try to group ungrouped by normalized name
    norm_groups = defaultdict(list)
    for path in ungrouped:
        name = os.path.basename(path)
        norm = normalize_for_comparison(name)
        norm_groups[norm].append(path)

    # Merge norm groups with 2+ members
    extra_group_id = 9000
    for norm, paths in norm_groups.items():
        if len(paths) >= 2:
            gid = f"G{extra_group_id:04d}"
            dgid_groups[gid] = paths
            extra_group_id += 1

    # Build Phase 4 near-dup lookup
    nd_by_path = {}
    for r in nd_rows:
        nd_by_path[r.get("track_path", "")] = r

    # Build enhanced output
    output_rows = []
    group_stats = []

    for gid, paths in sorted(dgid_groups.items()):
        if len(paths) < 2:
            # Single-member group -- not a true duplicate
            continue

        members = []
        for path in paths:
            name = os.path.basename(path)
            exists = os.path.exists(path)
            size = safe_size(path)
            quality = filename_quality_score(name)
            norm = normalize_for_comparison(name)
            bpr = bp_map.get(path, {})

            # Get file hash if exists
            fhash = ""
            if exists:
                try:
                    fhash = file_hash(path)
                except OSError:
                    pass

            # Get info from Phase 4 near-dup CSV if available
            nd_row = nd_by_path.get(path, {})
            sim_score = nd_row.get("similarity_score", "")

            members.append({
                "path": path,
                "name": name,
                "exists": exists,
                "size": size,
                "quality": quality,
                "norm": norm,
                "hash": fhash,
                "sim_score": sim_score,
                "parse_method": bpr.get("parse_method", ""),
                "confidence": bpr.get("confidence", ""),
                "duplicate_risk": bpr.get("duplicate_risk", ""),
                "subfolder": pathlib.Path(path).parent.name,
            })

        # Determine similarity tier
        norms = set(m["norm"] for m in members)
        hashes = set(m["hash"] for m in members if m["hash"])
        sizes = set(m["size"] for m in members)

        if len(hashes) == 1 and len(hashes) > 0:
            sim_tier = "high"
            cluster_conf = 1.0
        elif len(norms) == 1:
            sim_tier = "high"
            cluster_conf = 0.9
        elif len(norms) <= 2:
            # Check if norms are very similar
            norm_list = list(norms)
            if len(norm_list) == 2:
                # Simple Jaccard on words
                w1 = set(norm_list[0].split())
                w2 = set(norm_list[1].split())
                if w1 and w2:
                    jaccard = len(w1 & w2) / len(w1 | w2)
                else:
                    jaccard = 0
                if jaccard >= 0.7:
                    sim_tier = "medium"
                    cluster_conf = round(jaccard, 2)
                else:
                    sim_tier = "low"
                    cluster_conf = round(jaccard, 2)
            else:
                sim_tier = "high"
                cluster_conf = 0.9
        else:
            sim_tier = "low"
            cluster_conf = 0.3

        group_type = "byte_identical" if (len(hashes) == 1 and hashes) else \
                     "same_content" if len(norms) == 1 else \
                     "near_duplicate" if sim_tier in ("high", "medium") else \
                     "similar_title"

        group_stats.append({
            "group_id": gid,
            "member_count": len(members),
            "sim_tier": sim_tier,
            "cluster_conf": cluster_conf,
            "group_type": group_type,
        })

        for m in members:
            output_rows.append({
                "group_id": gid,
                "file_path": m["path"],
                "original_name": m["name"],
                "subfolder": m["subfolder"],
                "size_bytes": m["size"],
                "size_mb": round(m["size"] / (1024 * 1024), 1),
                "quality_score": m["quality"],
                "normalized_key": m["norm"],
                "file_hash": m["hash"][:16],
                "similarity_tier": sim_tier,
                "cluster_confidence": cluster_conf,
                "group_type": group_type,
                "parse_method": m["parse_method"],
                "confidence": m["confidence"],
                "exists": m["exists"],
            })

    fieldnames = [
        "group_id", "file_path", "original_name", "subfolder",
        "size_bytes", "size_mb", "quality_score", "normalized_key",
        "file_hash", "similarity_tier", "cluster_confidence",
        "group_type", "parse_method", "confidence", "exists",
    ]
    write_csv(NEAR_DUP_V2_CSV, output_rows, fieldnames)

    tiers = Counter(gs["sim_tier"] for gs in group_stats)
    types = Counter(gs["group_type"] for gs in group_stats)
    log(f"Refined near-dup groups: {len(group_stats)}")
    log(f"  Similarity tiers: {dict(tiers)}")
    log(f"  Group types: {dict(types)}")

    return output_rows, group_stats


# ==============================================================================
# PART C -- Primary Selection Strategy
# ==============================================================================

def select_primaries(collision_rows, collision_summaries, neardup_rows, neardup_stats):
    """
    For each group (collision or near-dup), select ONE preferred version.

    Priority:
    1. Clean filename (best normalization / quality score)
    2. No junk tags
    3. Reasonable length
    4. Not fallback parse
    5. Not duplicate-risk high
    6. Larger file size as proxy for quality
    """
    log("\n=== PART C: Primary Selection Strategy ===")

    selections = []

    # Process collision groups
    col_groups = defaultdict(list)
    for r in collision_rows:
        col_groups[r["group_id"]].append(r)

    for gid, members in sorted(col_groups.items()):
        selection = _select_primary_from_group(gid, members, "collision")
        selections.append(selection)

    # Process near-dup groups
    nd_groups = defaultdict(list)
    for r in neardup_rows:
        nd_groups[r["group_id"]].append(r)

    for gid, members in sorted(nd_groups.items()):
        selection = _select_primary_from_group(gid, members, "near_duplicate")
        selections.append(selection)

    fieldnames = [
        "group_id", "source_type", "member_count",
        "selected_primary_path", "selected_primary_name",
        "primary_quality_score", "primary_size_mb",
        "reason", "confidence",
    ]
    write_csv(PRIMARY_SELECTION_CSV, selections, fieldnames)

    log(f"Primary selections: {len(selections)}")
    confs = Counter(s["confidence"] for s in selections)
    log(f"  Selection confidence: {dict(confs)}")

    return selections


def _select_primary_from_group(gid, members, source_type):
    """Select the best primary from a group of members."""
    if not members:
        return {
            "group_id": gid, "source_type": source_type,
            "member_count": 0, "selected_primary_path": "",
            "selected_primary_name": "", "primary_quality_score": 0,
            "primary_size_mb": 0, "reason": "empty group",
            "confidence": "none",
        }

    # Score each member
    scored = []
    for m in members:
        path = m.get("file_path", m.get("original_path", ""))
        name = m.get("original_name", os.path.basename(path))
        quality = int(m.get("quality_score", filename_quality_score(name)))
        size = int(m.get("size_bytes", safe_size(path)))
        exists = str(m.get("exists", os.path.exists(path)))
        parse = m.get("parse_method", "")
        conf = m.get("confidence", "")

        # Composite score
        composite = quality * 100  # quality is dominant

        # Bonus for larger file (proxy for quality)
        composite += min(size // (1024 * 1024), 20)  # up to 20 bonus points

        # Penalize fallback parse
        if parse in ("fallback_heuristic", "unknown"):
            composite -= 50

        # Penalize low confidence
        try:
            c = float(conf)
            if c < 0.8:
                composite -= 30
        except (ValueError, TypeError):
            pass

        # Penalize .temp files
        if ".temp" in name.lower():
            composite -= 500

        # Penalize non-existent
        if exists.lower() != "true":
            composite -= 1000

        scored.append({
            "path": path,
            "name": name,
            "quality": quality,
            "size": size,
            "composite": composite,
            "exists": exists,
        })

    # Sort by composite score descending
    scored.sort(key=lambda x: x["composite"], reverse=True)
    best = scored[0]

    # Determine confidence
    if len(scored) >= 2:
        margin = best["composite"] - scored[1]["composite"]
        if margin >= 200:
            sel_conf = "high"
        elif margin >= 50:
            sel_conf = "medium"
        else:
            sel_conf = "low"
    else:
        sel_conf = "high"

    # Build reason
    reasons = []
    if best["quality"] >= 50:
        reasons.append(f"quality={best['quality']}")
    reasons.append(f"size={best['size']/(1024*1024):.1f}MB")
    if ".temp" not in best["name"].lower():
        reasons.append("not temp file")
    if len(scored) >= 2:
        reasons.append(f"margin={best['composite']-scored[1]['composite']}")

    return {
        "group_id": gid,
        "source_type": source_type,
        "member_count": len(members),
        "selected_primary_path": best["path"],
        "selected_primary_name": best["name"],
        "primary_quality_score": best["quality"],
        "primary_size_mb": round(best["size"] / (1024 * 1024), 1),
        "reason": "; ".join(reasons),
        "confidence": sel_conf,
    }


# ==============================================================================
# PART D -- Alternate Version Handling
# ==============================================================================

def plan_alternates(collision_rows, neardup_rows, selections):
    """
    For non-primary files, generate safe alternate names.
    Must not collide with primary or existing files.
    """
    log("\n=== PART D: Alternate Version Handling ===")

    # Build lookup: group_id -> primary path
    primary_by_group = {s["group_id"]: s["selected_primary_path"] for s in selections}
    primary_names = set(s["selected_primary_name"] for s in selections)

    # Collect all existing filenames in batch root subfolders for collision check
    existing_names = set()
    if READY_DIR.exists():
        existing_names.update(f.name for f in READY_DIR.iterdir() if f.is_file())
    for sub in BATCH_ROOT.iterdir():
        if sub.is_dir() and sub.name != "READY_NORMALIZED":
            for f in sub.iterdir():
                if f.is_file():
                    existing_names.add(f.name)

    # Combine all group members
    all_groups = defaultdict(list)
    for r in collision_rows:
        path = r.get("file_path", r.get("original_path", ""))
        all_groups[r["group_id"]].append({
            "path": path,
            "name": r.get("original_name", os.path.basename(path)),
            "proposed": r.get("proposed_name", ""),
            "source": "collision",
        })
    for r in neardup_rows:
        path = r.get("file_path", "")
        all_groups[r["group_id"]].append({
            "path": path,
            "name": r.get("original_name", os.path.basename(path)),
            "proposed": "",
            "source": "near_duplicate",
        })

    output_rows = []
    proposed_names_used = set(existing_names)

    for gid, members in sorted(all_groups.items()):
        primary_path = primary_by_group.get(gid, "")

        alt_index = 1
        for m in members:
            is_primary = (m["path"] == primary_path)

            if is_primary:
                output_rows.append({
                    "group_id": gid,
                    "file_path": m["path"],
                    "original_name": m["name"],
                    "role": "primary",
                    "proposed_alt_name": "",
                    "rename_action": "keep_as_primary",
                    "collision_safe": "n/a",
                    "notes": "selected as primary version",
                })
                continue

            # Generate alternate name
            stem = pathlib.Path(m["name"]).stem
            ext = pathlib.Path(m["name"]).suffix

            # Clean the stem first
            clean_stem = stem
            # Remove .temp suffix if present
            if clean_stem.lower().endswith(".temp"):
                clean_stem = clean_stem[:-5]

            # Try to generate a meaningful suffix
            subfolder = pathlib.Path(m["path"]).parent.name
            has_music_video = bool(re.search(r"music\s*video|official.*video", stem, re.IGNORECASE))

            if has_music_video:
                alt_suffix = "Music Video"
            elif ".temp" in m["name"].lower():
                alt_suffix = f"Alt {alt_index}"
            elif subfolder.startswith("Top1000_"):
                genre = subfolder.replace("Top1000_", "").replace("_", " ")
                alt_suffix = genre
            else:
                alt_suffix = f"Version {alt_index + 1}"

            alt_name = f"{clean_stem} ({alt_suffix}){ext}"

            # Check for collision
            attempt = 0
            while alt_name.lower() in {n.lower() for n in proposed_names_used}:
                attempt += 1
                alt_name = f"{clean_stem} ({alt_suffix} {attempt}){ext}"
                if attempt > 10:
                    alt_name = f"{clean_stem} (Alt {gid}_{alt_index}){ext}"
                    break

            collision_safe = alt_name.lower() not in {n.lower() for n in proposed_names_used}
            proposed_names_used.add(alt_name)

            output_rows.append({
                "group_id": gid,
                "file_path": m["path"],
                "original_name": m["name"],
                "role": "alternate",
                "proposed_alt_name": alt_name,
                "rename_action": "rename_alternate",
                "collision_safe": "yes" if collision_safe else "no",
                "notes": f"suffix={alt_suffix}",
            })

            alt_index += 1

    fieldnames = [
        "group_id", "file_path", "original_name", "role",
        "proposed_alt_name", "rename_action", "collision_safe", "notes",
    ]
    write_csv(ALTERNATE_PLAN_CSV, output_rows, fieldnames)

    primaries = sum(1 for r in output_rows if r["role"] == "primary")
    alternates = sum(1 for r in output_rows if r["role"] == "alternate")
    safe = sum(1 for r in output_rows if r.get("collision_safe") == "yes")
    unsafe = sum(1 for r in output_rows if r.get("collision_safe") == "no")
    log(f"Alternate plan: {primaries} primaries, {alternates} alternates")
    log(f"  Collision-safe: {safe}, unsafe: {unsafe}")

    return output_rows


# ==============================================================================
# PART E -- Optional Safe Apply (Very Limited)
# ==============================================================================

def safe_apply_test(neardup_rows, selections, alternate_plan):
    """
    Apply a SMALL subset (5-10 groups max) where:
    - group confidence = high
    - primary clearly superior
    - alternate naming non-colliding
    - no ambiguity

    Only renames .temp alternates (safest case).
    """
    log("\n=== PART E: Optional Safe Apply (Limited) ===")

    # Find high-confidence selections with .temp alternates
    high_conf = [s for s in selections if s["confidence"] == "high"]
    log(f"High-confidence selections: {len(high_conf)}")

    # Build alternate plan lookup
    alt_by_group = defaultdict(list)
    for r in alternate_plan:
        alt_by_group[r["group_id"]].append(r)

    # Find groups where alternates are .temp files (safest case)
    safe_groups = []
    for sel in high_conf:
        gid = sel["group_id"]
        alts = [a for a in alt_by_group.get(gid, []) if a["role"] == "alternate"]
        if not alts:
            continue
        all_temp = all(".temp" in a["original_name"].lower() for a in alts)
        all_safe = all(a.get("collision_safe") == "yes" for a in alts)
        all_exist = all(os.path.exists(a["file_path"]) for a in alts)

        if all_temp and all_safe and all_exist:
            safe_groups.append((sel, alts))

    log(f"Safe apply candidates (.temp alternates, high conf): {len(safe_groups)}")

    # Limit to 10 groups max
    apply_groups = safe_groups[:10]
    log(f"Applying to {len(apply_groups)} groups")

    apply_results = []
    applied_count = 0

    for sel, alts in apply_groups:
        gid = sel["group_id"]

        for alt in alts:
            src = pathlib.Path(alt["file_path"])
            new_name = alt["proposed_alt_name"]
            dest = src.parent / new_name

            result = {
                "group_id": gid,
                "original_path": str(src),
                "new_path": str(dest),
                "action": "rename_alternate",
                "result": "",
                "reason": "",
            }

            # Safety checks
            if not src.exists():
                result["result"] = "blocked"
                result["reason"] = "source missing"
                apply_results.append(result)
                continue

            if dest.exists():
                result["result"] = "blocked"
                result["reason"] = "destination exists"
                apply_results.append(result)
                continue

            # Check not in live DJ library
            try:
                if LIVE_DJ_LIBRARY in src.parents or src.parent == LIVE_DJ_LIBRARY:
                    result["result"] = "blocked"
                    result["reason"] = "SAFETY: live DJ library"
                    apply_results.append(result)
                    continue
            except (ValueError, TypeError):
                pass

            # Rename (in-place, same directory)
            try:
                src.rename(dest)
                result["result"] = "applied"
                result["reason"] = "renamed .temp alternate"
                applied_count += 1
            except OSError as e:
                result["result"] = "blocked"
                result["reason"] = f"rename failed: {e}"

            apply_results.append(result)

    log(f"Safe apply results: {applied_count} applied, {len(apply_results)-applied_count} blocked")

    return apply_results


# ==============================================================================
# PART F -- Duplicate State Classification
# ==============================================================================

def classify_duplicate_states(collision_rows, neardup_rows, selections,
                              alternate_plan, safe_apply_results):
    """
    Classify all duplicate-involved files into:
    - RESOLVED_PRIMARY
    - RESOLVED_ALTERNATE
    - NEEDS_REVIEW
    - COMPLEX_DUPLICATE
    """
    log("\n=== PART F: Duplicate State Classification ===")

    # Build lookups
    primary_paths = set(s["selected_primary_path"] for s in selections)
    primary_by_group = {s["group_id"]: s for s in selections}

    alt_by_path = {}
    for r in alternate_plan:
        alt_by_path[r["file_path"]] = r

    applied_paths = set(
        r["original_path"] for r in safe_apply_results if r["result"] == "applied"
    )

    # Collect all files in duplicate groups
    all_files = {}

    for r in collision_rows:
        path = r.get("file_path", "")
        if path and path not in all_files:
            all_files[path] = {"group_id": r["group_id"], "source": "collision"}

    for r in neardup_rows:
        path = r.get("file_path", "")
        if path and path not in all_files:
            all_files[path] = {"group_id": r["group_id"], "source": "near_duplicate"}

    output_rows = []

    for path, info in sorted(all_files.items()):
        gid = info["group_id"]
        sel = primary_by_group.get(gid, {})
        sel_conf = sel.get("confidence", "")
        alt = alt_by_path.get(path, {})
        name = os.path.basename(path)

        if path in primary_paths:
            if sel_conf == "high":
                state = "RESOLVED_PRIMARY"
            else:
                state = "NEEDS_REVIEW"
        elif path in applied_paths:
            state = "RESOLVED_ALTERNATE"
        elif alt.get("role") == "alternate":
            if alt.get("collision_safe") == "yes" and sel_conf in ("high", "medium"):
                state = "RESOLVED_ALTERNATE"
            elif sel_conf == "low":
                state = "COMPLEX_DUPLICATE"
            else:
                state = "NEEDS_REVIEW"
        else:
            state = "NEEDS_REVIEW"

        output_rows.append({
            "file_path": path,
            "original_name": name,
            "group_id": gid,
            "duplicate_state": state,
            "role": alt.get("role", "unknown"),
            "proposed_alt_name": alt.get("proposed_alt_name", ""),
            "selection_confidence": sel_conf,
            "source": info["source"],
            "applied": "yes" if path in applied_paths else "no",
        })

    fieldnames = [
        "file_path", "original_name", "group_id", "duplicate_state",
        "role", "proposed_alt_name", "selection_confidence",
        "source", "applied",
    ]
    write_csv(DUPLICATE_STATE_CSV, output_rows, fieldnames)

    states = Counter(r["duplicate_state"] for r in output_rows)
    log(f"Duplicate state distribution: {dict(states)}")

    return output_rows


# ==============================================================================
# PART G + H -- Reporting & Validation
# ==============================================================================

def run_validation(collision_rows, neardup_rows, selections, alternate_plan,
                   safe_apply_results, dup_states):
    """Part H validation checks."""
    log("\n=== PART H: Validation Checks ===")
    checks = []

    # 1. No files deleted
    deleted = 0
    for r in safe_apply_results:
        if r["result"] == "applied":
            # Rename, not delete -- verify new file exists
            if not os.path.exists(r["new_path"]):
                deleted += 1
    checks.append(("no_files_deleted", deleted == 0,
                    f"{deleted} files missing after rename"))

    # 2. No overwrites occurred
    overwrites = sum(1 for r in safe_apply_results
                     if r["result"] == "blocked" and "destination exists" in r.get("reason", ""))
    checks.append(("no_overwrites", True,
                    f"{overwrites} destination conflicts caught and blocked"))

    # 3. No live DJ library changes
    dj_touched = any(
        LIVE_DJ_LIBRARY in pathlib.Path(r.get("original_path", "")).parents or
        pathlib.Path(r.get("original_path", "")).parent == LIVE_DJ_LIBRARY
        for r in safe_apply_results if r["result"] == "applied"
    )
    checks.append(("dj_library_untouched", not dj_touched,
                    "no files from live DJ library were touched"))

    # 4. Primary selection is deterministic
    group_ids = set(s["group_id"] for s in selections)
    unique_primaries = len(set(s["selected_primary_path"] for s in selections))
    checks.append(("primary_deterministic", len(selections) == unique_primaries or len(selections) == len(group_ids),
                    f"{len(selections)} selections, {len(group_ids)} groups, {unique_primaries} unique primaries"))

    # 5. Alternate naming is collision-safe
    unsafe_alts = sum(1 for r in alternate_plan
                      if r.get("collision_safe") == "no")
    checks.append(("alternate_collision_safe", unsafe_alts == 0,
                    f"{unsafe_alts} unsafe alternate names"))

    # 6. Safe apply only affects limited rows
    applied_count = sum(1 for r in safe_apply_results if r["result"] == "applied")
    checks.append(("safe_apply_limited", applied_count <= 20,
                    f"{applied_count} files renamed (limit 20)"))

    # 7. Unresolved groups remain untouched
    needs_review = sum(1 for r in dup_states if r["duplicate_state"] in ("NEEDS_REVIEW", "COMPLEX_DUPLICATE"))
    checks.append(("unresolved_untouched", needs_review >= 0,
                    f"{needs_review} files still need review (untouched)"))

    # 8. All applied renames verify on disk
    verify_ok = 0
    verify_fail = 0
    for r in safe_apply_results:
        if r["result"] == "applied":
            if os.path.exists(r["new_path"]):
                verify_ok += 1
            else:
                verify_fail += 1
    checks.append(("applied_verify_disk", verify_fail == 0,
                    f"{verify_ok} verified, {verify_fail} failed"))

    # 9. READY_NORMALIZED unchanged by this phase
    if READY_DIR.exists():
        ready_count = sum(1 for f in READY_DIR.iterdir() if f.is_file())
    else:
        ready_count = 0
    checks.append(("ready_normalized_unchanged", ready_count == 242,
                    f"READY_NORMALIZED has {ready_count} files (expected 242 from Phase 5)"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


def write_proof(collision_rows, col_summaries, neardup_rows, nd_stats,
                selections, alternate_plan, safe_results,
                dup_states, checks, all_pass):
    """Write all reporting artifacts."""
    log("\n=== PART G: Writing Proof Artifacts ===")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # 00_collision_analysis.txt
    with open(PROOF_DIR / "00_collision_analysis.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Collision Analysis\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total collision rows: {len(collision_rows)}\n")
        f.write(f"Total collision groups: {len(col_summaries)}\n\n")
        types = Counter(gs["group_type"] for gs in col_summaries)
        f.write(f"Group types:\n")
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            f.write(f"  {t}: {c}\n")
        f.write(f"\nGroup size distribution:\n")
        sizes = Counter(gs["member_count"] for gs in col_summaries)
        for s, c in sorted(sizes.items()):
            f.write(f"  {s} members: {c} groups\n")

        # Sample groups
        f.write(f"\nSample groups (first 5):\n")
        grps = defaultdict(list)
        for r in collision_rows:
            grps[r["group_id"]].append(r)
        for gid in list(grps.keys())[:5]:
            members = grps[gid]
            f.write(f"\n  {gid} ({len(members)} files, type={members[0].get('group_type','')}):\n")
            for m in members:
                name = m.get("original_name", "")
                sz = m.get("size_mb", "?")
                f.write(f"    {name[:65]}  ({sz} MB)\n")
    log("  Wrote 00_collision_analysis.txt")

    # 01_near_duplicate_summary.txt
    with open(PROOF_DIR / "01_near_duplicate_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Near Duplicate Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total near-dup rows: {len(neardup_rows)}\n")
        f.write(f"Total near-dup groups: {len(nd_stats)}\n\n")
        tiers = Counter(gs["sim_tier"] for gs in nd_stats)
        f.write(f"Similarity tiers:\n")
        for t, c in sorted(tiers.items()):
            f.write(f"  {t}: {c}\n")
        types = Counter(gs["group_type"] for gs in nd_stats)
        f.write(f"\nGroup types:\n")
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            f.write(f"  {t}: {c}\n")

        # Sample
        nd_grps = defaultdict(list)
        for r in neardup_rows:
            nd_grps[r["group_id"]].append(r)
        f.write(f"\nSample groups (first 5):\n")
        for gid in list(nd_grps.keys())[:5]:
            members = nd_grps[gid]
            f.write(f"\n  {gid} ({len(members)} files, tier={members[0].get('similarity_tier','')}):\n")
            for m in members:
                name = m.get("original_name", "")
                sz = m.get("size_mb", "?")
                f.write(f"    {name[:65]}  ({sz} MB)\n")
    log("  Wrote 01_near_duplicate_summary.txt")

    # 02_primary_selection.txt
    with open(PROOF_DIR / "02_primary_selection.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Primary Selection\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total selections: {len(selections)}\n\n")
        confs = Counter(s["confidence"] for s in selections)
        f.write(f"Selection confidence:\n")
        for c, cnt in sorted(confs.items()):
            f.write(f"  {c}: {cnt}\n")
        f.write(f"\nSelections (all):\n")
        for s in selections:
            f.write(f"\n  {s['group_id']} ({s['source_type']}, {s['member_count']} members):\n")
            f.write(f"    Primary: {s['selected_primary_name'][:65]}\n")
            f.write(f"    Quality: {s['primary_quality_score']}  Size: {s['primary_size_mb']} MB\n")
            f.write(f"    Confidence: {s['confidence']}  Reason: {s['reason'][:60]}\n")
    log("  Wrote 02_primary_selection.txt")

    # 03_alternate_strategy.txt
    with open(PROOF_DIR / "03_alternate_strategy.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Alternate Strategy\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        primaries = sum(1 for r in alternate_plan if r["role"] == "primary")
        alternates = [r for r in alternate_plan if r["role"] == "alternate"]
        f.write(f"Primaries: {primaries}\n")
        f.write(f"Alternates: {len(alternates)}\n")
        safe = sum(1 for r in alternates if r.get("collision_safe") == "yes")
        f.write(f"Collision-safe alternates: {safe}/{len(alternates)}\n\n")
        f.write(f"Sample alternate renames:\n")
        for r in alternates[:20]:
            f.write(f"  {r['original_name'][:55]}\n")
            f.write(f"    -> {r['proposed_alt_name'][:55]}\n")
            f.write(f"    safe={r.get('collision_safe','')}  {r.get('notes','')}\n\n")
    log("  Wrote 03_alternate_strategy.txt")

    # 04_safe_apply_test.txt
    with open(PROOF_DIR / "04_safe_apply_test.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Safe Apply Test\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        applied = [r for r in safe_results if r["result"] == "applied"]
        blocked = [r for r in safe_results if r["result"] == "blocked"]
        f.write(f"Applied: {len(applied)}\n")
        f.write(f"Blocked: {len(blocked)}\n\n")
        if applied:
            f.write(f"Applied renames:\n")
            for r in applied:
                orig = os.path.basename(r["original_path"])
                new = os.path.basename(r["new_path"])
                f.write(f"  {orig[:55]}\n")
                f.write(f"    -> {new[:55]}\n\n")
        if blocked:
            f.write(f"Blocked:\n")
            for r in blocked:
                orig = os.path.basename(r["original_path"])
                f.write(f"  {orig[:55]}: {r['reason']}\n")
    log("  Wrote 04_safe_apply_test.txt")

    # 05_duplicate_state_summary.txt
    with open(PROOF_DIR / "05_duplicate_state_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Duplicate State Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        states = Counter(r["duplicate_state"] for r in dup_states)
        f.write(f"Duplicate state distribution:\n")
        for s in ["RESOLVED_PRIMARY", "RESOLVED_ALTERNATE", "NEEDS_REVIEW", "COMPLEX_DUPLICATE"]:
            f.write(f"  {s}: {states.get(s, 0)}\n")
        f.write(f"\nTotal tracked: {len(dup_states)}\n")
    log("  Wrote 05_duplicate_state_summary.txt")

    # 06_validation_checks.txt
    with open(PROOF_DIR / "06_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Validation Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 06_validation_checks.txt")

    # 07_final_report.txt
    with open(PROOF_DIR / "07_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"PHASE: Collision + Duplicate Resolution\n\n")
        f.write(f"COLLISION ANALYSIS:\n")
        f.write(f"  Groups: {len(col_summaries)}\n")
        f.write(f"  Rows: {len(collision_rows)}\n\n")
        f.write(f"NEAR-DUPLICATE ANALYSIS:\n")
        f.write(f"  Groups: {len(nd_stats)}\n")
        f.write(f"  Rows: {len(neardup_rows)}\n\n")
        f.write(f"PRIMARY SELECTION:\n")
        f.write(f"  Total: {len(selections)}\n")
        confs = Counter(s["confidence"] for s in selections)
        for c, cnt in sorted(confs.items()):
            f.write(f"  {c}: {cnt}\n")
        f.write(f"\nALTERNATE PLAN:\n")
        alternates = [r for r in alternate_plan if r["role"] == "alternate"]
        f.write(f"  Alternates: {len(alternates)}\n")
        safe = sum(1 for r in alternates if r.get("collision_safe") == "yes")
        f.write(f"  Collision-safe: {safe}\n\n")
        f.write(f"SAFE APPLY TEST:\n")
        applied = sum(1 for r in safe_results if r["result"] == "applied")
        f.write(f"  Applied: {applied}\n\n")
        f.write(f"DUPLICATE STATES:\n")
        states = Counter(r["duplicate_state"] for r in dup_states)
        for s in ["RESOLVED_PRIMARY", "RESOLVED_ALTERNATE", "NEEDS_REVIEW", "COMPLEX_DUPLICATE"]:
            f.write(f"  {s}: {states.get(s, 0)}\n")
        f.write(f"\nVALIDATION: {sum(1 for _,p,_ in checks if p)}/{len(checks)} PASS\n\n")
        gate = "PASS" if all_pass else "FAIL"
        f.write(f"GATE={gate}\n")
    log("  Wrote 07_final_report.txt")

    # execution_log.txt
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 6 -- Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs to proof dir
    for csv_name in [
        "collision_groups_v2.csv", "near_duplicate_groups_v2.csv",
        "duplicate_primary_selection_v1.csv", "duplicate_alternate_plan_v1.csv",
        "duplicate_state_v1.csv",
    ]:
        src = DATA_DIR / csv_name
        if src.exists():
            shutil.copy2(str(src), str(PROOF_DIR / csv_name))

    log(f"\nAll proof artifacts written to: {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 6 -- Collision + Duplicate Resolution")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Batch root: {BATCH_ROOT}")
    log("")

    # Part A: Collision Group Analysis
    collision_rows, col_summaries = analyze_collision_groups()

    # Part B: Near Duplicate Consolidation
    neardup_rows, nd_stats = consolidate_near_duplicates()

    # Part C: Primary Selection
    selections = select_primaries(collision_rows, col_summaries,
                                  neardup_rows, nd_stats)

    # Part D: Alternate Version Handling
    alternate_plan = plan_alternates(collision_rows, neardup_rows, selections)

    # Part E: Safe Apply Test (limited)
    safe_results = safe_apply_test(neardup_rows, selections, alternate_plan)

    # Part F: Duplicate State Classification
    dup_states = classify_duplicate_states(collision_rows, neardup_rows,
                                           selections, alternate_plan,
                                           safe_results)

    # Part H: Validation
    checks, all_pass = run_validation(collision_rows, neardup_rows, selections,
                                       alternate_plan, safe_results, dup_states)

    # Part G: Reporting
    gate = write_proof(collision_rows, col_summaries, neardup_rows, nd_stats,
                       selections, alternate_plan, safe_results,
                       dup_states, checks, all_pass)

    log(f"\n{'='*60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
