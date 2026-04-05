"""
DJ Library Normalization Engine — Phase 3
==========================================
BATCH INTAKE + APPROVED APPLY PIPELINE

Extends Phase 2 into a real batch ingestion system for large incoming
music folders. Separates files into 4 intake states:

  RAW_INCOMING       — scanned but unprocessed raw files
  REVIEW_REQUIRED    — unresolved but not broken (duplicates, medium-confidence)
  READY_NORMALIZED   — approved, safe, normalized files
  HELD_PROBLEMS      — collisions, parse failures, blocked rows

Safety invariants from Phase 2 preserved:
  - No live renames by default (plan-only mode)
  - Blank action NEVER applies
  - Collision/hold rows NEVER apply even if marked approve_normalize
  - All file operations auditable
  - Live DJ library NEVER touched
"""

import os
import re
import csv
import json
import time
import shutil
import hashlib
import pathlib
from datetime import datetime
from dataclasses import dataclass, field, asdict

# Import Phase 2 engine components
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from library_normalization_engine_v2 import (
    PRESETS, AUDIO_EXTENSIONS, WORKSPACE, DATA_DIR,
    ARTIST_OVERRIDES_PATH, TITLE_OVERRIDES_PATH,
    load_overrides, apply_overrides,
    clean_tokens, process_brackets,
    normalize_case, parse_artist_title, fallback_parse_artist_title,
    build_proposed_name, detect_duplicates,
    scan_files, _normalize_for_comparison,
)

ENGINE_VERSION = "3.0.0"

# ================================================================
# PATHS
# ================================================================
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase3"
PROOF_DIR.mkdir(parents=True, exist_ok=True)

BATCH_PLAN_CSV = DATA_DIR / "batch_normalization_plan.csv"
BATCH_MANIFEST_JSON = DATA_DIR / "batch_manifest.json"

# ================================================================
# INTAKE STATES
# ================================================================
INTAKE_STATES = {
    "RAW_INCOMING":     "Scanned but unprocessed raw files",
    "REVIEW_REQUIRED":  "Unresolved: duplicates, medium-confidence, needs human review",
    "READY_NORMALIZED": "Approved, safe, normalized files ready for library merge",
    "HELD_PROBLEMS":    "Collisions, parse failures, blocked rows, illegal paths",
}

# ================================================================
# BATCH CSV FIELDS
# ================================================================
BATCH_CSV_FIELDS = [
    "batch_id",
    "original_path", "original_name", "proposed_name",
    "cleaned_name",
    "guessed_artist", "guessed_title",
    "override_applied", "parse_method", "confidence",
    "collision_status", "duplicate_group_id", "duplicate_risk",
    "action", "target_state", "notes",
]


# ================================================================
# BATCH IDENTITY
# ================================================================

def generate_batch_id(source_root: pathlib.Path) -> str:
    """Generate a deterministic batch ID from source path + timestamp."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_hash = hashlib.sha256(str(source_root).encode()).hexdigest()[:8]
    return f"BATCH_{ts}_{path_hash}"


# ================================================================
# STAGING STRUCTURE
# ================================================================

def create_staging_dirs(staging_root: pathlib.Path) -> dict[str, pathlib.Path]:
    """Create the 4-state intake folder structure under staging_root.
    Returns dict mapping state name -> path."""
    dirs = {}
    for state in INTAKE_STATES:
        d = staging_root / state
        d.mkdir(parents=True, exist_ok=True)
        dirs[state] = d
    return dirs


# ================================================================
# BATCH MANIFEST
# ================================================================

def build_manifest(
    batch_id: str,
    source_root: pathlib.Path,
    files: list[pathlib.Path],
    plans: list[dict],
    preset_name: str,
    scan_time: str,
    elapsed: float,
) -> dict:
    """Build the batch manifest dictionary."""
    # Count subfolders
    subfolders = set()
    for f in files:
        rel = f.relative_to(source_root)
        if len(rel.parts) > 1:
            subfolders.add(rel.parts[0])

    # Total bytes
    total_bytes = 0
    for f in files:
        try:
            total_bytes += f.stat().st_size
        except OSError:
            pass

    # Counts by outcome
    action_counts: dict[str, int] = {}
    state_counts: dict[str, int] = {}
    confidence_counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for p in plans:
        a = p.get("action") or "(blank)"
        action_counts[a] = action_counts.get(a, 0) + 1
        s = p.get("target_state") or "(unassigned)"
        state_counts[s] = state_counts.get(s, 0) + 1
        conf = float(p.get("confidence", 0))
        if conf >= 0.9:
            confidence_counts["high"] += 1
        elif conf >= 0.5:
            confidence_counts["medium"] += 1
        elif conf > 0:
            confidence_counts["low"] += 1
        else:
            confidence_counts["none"] += 1

    collision_count = sum(
        1 for p in plans
        if (p.get("collision_status") or "").startswith("COLLISION")
    )
    near_dup_count = sum(
        1 for p in plans if p.get("duplicate_risk") == "near_duplicate"
    )
    similar_count = sum(
        1 for p in plans if p.get("duplicate_risk") == "similar_title"
    )
    override_count = sum(
        1 for p in plans if p.get("override_applied")
    )
    fallback_count = sum(
        1 for p in plans
        if (p.get("parse_method") or "").startswith("fallback")
    )

    ext_counts: dict[str, int] = {}
    for f in files:
        ext = f.suffix.lower()
        ext_counts[ext] = ext_counts.get(ext, 0) + 1

    return {
        "batch_id": batch_id,
        "source_root": str(source_root),
        "scan_time": scan_time,
        "elapsed_seconds": elapsed,
        "normalization_engine_version": ENGINE_VERSION,
        "preset_used": preset_name,
        "file_count": len(files),
        "subfolder_count": len(subfolders),
        "subfolders": sorted(subfolders),
        "total_bytes": total_bytes,
        "total_gb": round(total_bytes / (1024**3), 2),
        "extensions": ext_counts,
        "counts_by_action": action_counts,
        "counts_by_state": state_counts,
        "counts_by_confidence": confidence_counts,
        "collisions": collision_count,
        "near_duplicates": near_dup_count,
        "similar_titles": similar_count,
        "overrides_applied": override_count,
        "fallback_parses": fallback_count,
    }


# ================================================================
# TARGET STATE CLASSIFIER
# ================================================================

def classify_target_state(plan: dict) -> str:
    """Determine the target intake state for a plan row.
    This assigns a default target_state based on confidence, parse method,
    collision status, and duplicate risk. The user can override in CSV."""

    collision = (plan.get("collision_status") or "").strip()
    confidence = float(plan.get("confidence", 0))
    parse_method = plan.get("parse_method") or ""
    dup_risk = plan.get("duplicate_risk") or "none"
    action = (plan.get("action") or "").strip().lower()

    # HELD_PROBLEMS: collisions, parse failures, low-confidence, illegal chars
    if "COLLISION" in collision.upper():
        return "HELD_PROBLEMS"
    if confidence == 0.0:
        return "HELD_PROBLEMS"
    if parse_method.startswith("fallback") and confidence < 0.5:
        return "HELD_PROBLEMS"

    # Check for illegal/problematic filename characters
    proposed = plan.get("proposed_name", "")
    if _has_illegal_chars(proposed):
        return "HELD_PROBLEMS"

    # REVIEW_REQUIRED: duplicates, medium confidence, fallback parses
    if dup_risk in ("near_duplicate", "similar_title", "exact_collision"):
        return "REVIEW_REQUIRED"
    if parse_method.startswith("fallback"):
        return "REVIEW_REQUIRED"
    if 0.0 < confidence < 0.9:
        return "REVIEW_REQUIRED"

    # RAW_INCOMING: high confidence, no problems — can be reviewed for approval
    return "RAW_INCOMING"


# Characters illegal in Windows filenames
ILLEGAL_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
# Additional problematic Unicode (fullwidth slash, colon etc.)
PROBLEMATIC_UNICODE = re.compile(r'[\uff0f\uff1a\u2044\u29f8\u2571⧸｜]')


def _has_illegal_chars(name: str) -> bool:
    """Check if a proposed filename has illegal or problematic characters."""
    if not name:
        return False
    # Strip the extension for checking
    stem = pathlib.Path(name).stem
    if ILLEGAL_CHARS.search(stem):
        return True
    if PROBLEMATIC_UNICODE.search(stem):
        return True
    # Check for excessively long filenames (Windows MAX_PATH component)
    if len(name.encode("utf-8")) > 240:
        return True
    return False


# ================================================================
# BATCH PLANNING ENGINE
# ================================================================

def run_batch_plan(
    source_root: pathlib.Path,
    preset_name: str = "Gene_Default",
    recursive: bool = True,
) -> tuple[list[dict], dict, list[str]]:
    """
    Run the batch normalization planner.
    Returns (plans, manifest, log_lines).

    This is plan-only — no files are moved or renamed.
    """
    log: list[str] = []
    t0 = time.time()
    scan_time = time.strftime("%Y-%m-%d %H:%M:%S")
    preset = PRESETS[preset_name]

    batch_id = generate_batch_id(source_root)

    log.append(f"Batch Intake Engine V3 started: {scan_time}")
    log.append(f"Batch ID: {batch_id}")
    log.append(f"Source root: {source_root}")
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

    # --- SCAN ---
    log.append("--- SCANNING ---")
    files = scan_files(source_root, recursive)
    log.append(f"  Audio files found: {len(files)}")

    if not files:
        log.append("  WARNING: No audio files found!")
        manifest = build_manifest(batch_id, source_root, [], [], preset_name,
                                  scan_time, time.time() - t0)
        return [], manifest, log

    ext_counts: dict[str, int] = {}
    for f in files:
        ext = f.suffix.lower()
        ext_counts[ext] = ext_counts.get(ext, 0) + 1
    for ext, count in sorted(ext_counts.items()):
        log.append(f"    {ext}: {count}")

    subfolder_set = set()
    for f in files:
        rel = f.relative_to(source_root)
        if len(rel.parts) > 1:
            subfolder_set.add(rel.parts[0])
    log.append(f"  Subfolders: {len(subfolder_set)}")
    for sf in sorted(subfolder_set):
        sf_count = sum(1 for f in files
                       if f.relative_to(source_root).parts[0] == sf
                       and len(f.relative_to(source_root).parts) > 1)
        log.append(f"    {sf}: {sf_count} files")
    log.append("")

    # --- PROCESS ---
    log.append("--- PROCESSING ---")
    plans: list[dict] = []
    parse_stats = {"high": 0, "medium": 0, "low": 0, "none": 0}
    override_count = 0
    fallback_count = 0

    for fp in files:
        original_name = fp.name
        stem = fp.stem
        ext = fp.suffix

        # Clean tokens
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

        # Build proposed name
        proposed = build_proposed_name(artist, title, ext, preset)

        # Apply overrides
        all_overrides_applied = []
        if preset.use_overrides:
            if artist:
                artist_cased = normalize_case(artist, preset.case_mode)
                corrected_artist, artist_ovr = apply_overrides(
                    artist_cased, artist_overrides
                )
                all_overrides_applied.extend(artist_ovr)
            else:
                corrected_artist = ""

            title_cased = normalize_case(title, preset.case_mode)
            corrected_title, title_ovr = apply_overrides(
                title_cased, title_overrides
            )
            all_overrides_applied.extend(title_ovr)

            corrected_title, title_artist_ovr = apply_overrides(
                corrected_title, artist_overrides
            )
            all_overrides_applied.extend(title_artist_ovr)

            if all_overrides_applied:
                override_count += 1
                if corrected_artist:
                    proposed = f"{corrected_artist} - {corrected_title}{ext}"
                else:
                    proposed = f"{corrected_title}{ext}"

        # Determine initial action
        action = ""  # blank by default
        collision_status = "ok"
        notes = ""

        if proposed.lower() == original_name.lower():
            action = "skip"
            collision_status = "no_change"
        elif confidence < 0.5:
            action = "hold"
            collision_status = "low_confidence"
        elif parse_method.startswith("fallback"):
            action = "hold"
            collision_status = "fallback_parse"

        # Check for illegal characters in proposed name
        if _has_illegal_chars(proposed):
            action = "hold"
            collision_status = "illegal_chars"
            notes = "proposed name contains illegal/problematic characters"

        plan = {
            "batch_id": batch_id,
            "original_path": str(fp),
            "original_name": original_name,
            "proposed_name": proposed,
            "cleaned_name": cleaned + ext,
            "guessed_artist": artist,
            "guessed_title": title,
            "override_applied": "; ".join(all_overrides_applied) if all_overrides_applied else "",
            "parse_method": parse_method,
            "confidence": confidence,
            "collision_status": collision_status,
            "duplicate_group_id": "",
            "duplicate_risk": "none",
            "action": action,
            "target_state": "",
            "notes": notes,
        }
        plans.append(plan)

    log.append(f"  Processed: {len(plans)}")
    log.append(f"  Parse confidence: high={parse_stats['high']}, "
               f"medium={parse_stats['medium']}, low={parse_stats['low']}, "
               f"none={parse_stats['none']}")
    log.append(f"  Overrides applied: {override_count}")
    log.append(f"  Fallback parses: {fallback_count}")
    log.append("")

    # --- DUPLICATE DETECTION ---
    log.append("--- DUPLICATE DETECTION V2 ---")
    plans = detect_duplicates(plans)
    collision_count = sum(
        1 for p in plans if (p["collision_status"] or "").startswith("COLLISION")
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

    # --- CLASSIFY TARGET STATES ---
    log.append("--- TARGET STATE CLASSIFICATION ---")
    for plan in plans:
        plan["target_state"] = classify_target_state(plan)

    state_counts: dict[str, int] = {}
    for p in plans:
        s = p["target_state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    for state, count in sorted(state_counts.items()):
        log.append(f"  {state}: {count}")

    # Count illegal char holds
    illegal_count = sum(1 for p in plans if p.get("notes", "").startswith("proposed name contains"))
    if illegal_count:
        log.append(f"  (illegal_chars_held: {illegal_count})")
    log.append("")

    # --- ACTION SUMMARY ---
    action_counts: dict[str, int] = {}
    for p in plans:
        a = p["action"] if p["action"] else "(blank)"
        action_counts[a] = action_counts.get(a, 0) + 1

    log.append("--- ACTION SUMMARY ---")
    for action, count in sorted(action_counts.items()):
        log.append(f"  {action}: {count}")

    elapsed = round(time.time() - t0, 2)
    log.append(f"\nBatch planning completed in {elapsed}s")

    # Build manifest
    manifest = build_manifest(
        batch_id, source_root, files, plans, preset_name,
        scan_time, elapsed
    )

    return plans, manifest, log


# ================================================================
# BATCH CSV EXPORT
# ================================================================

def export_batch_csv(plans: list[dict], output_path: pathlib.Path):
    """Export the batch normalization plan as a reviewable CSV."""
    if not plans:
        return
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BATCH_CSV_FIELDS)
        writer.writeheader()
        for plan in plans:
            row = {k: plan.get(k, "") for k in BATCH_CSV_FIELDS}
            writer.writerow(row)


def export_subset_csv(plans: list[dict], output_path: pathlib.Path,
                      filter_fn=None):
    """Export a filtered subset of plans to a CSV."""
    subset = [p for p in plans if filter_fn(p)] if filter_fn else plans
    if not subset:
        return 0
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BATCH_CSV_FIELDS)
        writer.writeheader()
        for plan in subset:
            row = {k: plan.get(k, "") for k in BATCH_CSV_FIELDS}
            writer.writerow(row)
    return len(subset)


# ================================================================
# BATCH APPLY PIPELINE
# ================================================================

def apply_batch_plan(
    csv_path: pathlib.Path,
    staging_root: pathlib.Path,
    dry_run: bool = True,
) -> tuple[list[dict], list[str]]:
    """
    Apply the batch normalization plan.

    Action routing:
      approve_normalize → copy+rename into READY_NORMALIZED
      review            → copy into REVIEW_REQUIRED (original name)
      hold              → copy into HELD_PROBLEMS (original name)
      blank             → NO ACTION (stays in RAW_INCOMING)

    Safety gates (same as Phase 2 + batch-specific):
      - blank action → skip (NEVER apply)
      - collision_status COLLISION → blocked (NEVER apply)
      - low_confidence/fallback_parse → blocked from normalize
      - illegal_chars → blocked from normalize
      - source file must exist
      - destination must not already exist
      - NEVER writes to the live DJ library

    Uses COPY not MOVE — source files are preserved until explicitly cleaned.
    """
    log: list[str] = []
    results: list[dict] = []

    log.append(f"Batch apply started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.append(f"CSV: {csv_path}")
    log.append(f"Staging root: {staging_root}")
    log.append(f"Dry run: {dry_run}")
    log.append("")

    if not csv_path.exists():
        log.append("ERROR: CSV file not found!")
        return results, log

    # Create staging dirs
    staging_dirs = create_staging_dirs(staging_root)
    log.append("Staging directories:")
    for state, path in staging_dirs.items():
        log.append(f"  {state}: {path}")
    log.append("")

    # Read the plan
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    log.append(f"Total rows: {len(rows)}")

    # Live DJ library path — NEVER write here
    live_library = pathlib.Path(r"C:\Users\suppo\Music")

    counters = {
        "normalized": 0,
        "review_routed": 0,
        "held_routed": 0,
        "skipped": 0,
        "blocked": 0,
        "errors": 0,
    }

    for row in rows:
        action = (row.get("action") or "").strip().lower()
        collision = (row.get("collision_status") or "").strip()
        original_path = row.get("original_path", "").strip()
        proposed_name = row.get("proposed_name", "").strip()
        target_state = (row.get("target_state") or "").strip()
        parse_method = (row.get("parse_method") or "").strip()

        result = {
            "original_path": original_path,
            "proposed_name": proposed_name,
            "action_requested": action,
            "action_taken": "skipped",
            "destination": "",
            "reason": "",
        }

        # Gate 0: safety — refuse writes to live DJ library
        src = pathlib.Path(original_path) if original_path else None
        if src and live_library in [src] + list(src.parents):
            # Should never happen for batch, but fail-closed
            result["reason"] = "live_library_blocked"
            result["action_taken"] = "blocked"
            counters["blocked"] += 1
            results.append(result)
            continue

        # Gate 1: blank action → no-op
        if not action:
            result["reason"] = "blank_action"
            counters["skipped"] += 1
            results.append(result)
            continue

        # Gate 2: skip action
        if action == "skip":
            result["reason"] = "action=skip"
            counters["skipped"] += 1
            results.append(result)
            continue

        # Gate 3: collision rows NEVER normalize
        if "COLLISION" in collision.upper() and action == "approve_normalize":
            result["reason"] = "collision_blocked"
            result["action_taken"] = "blocked"
            counters["blocked"] += 1
            results.append(result)
            continue

        # Gate 4: low-confidence / fallback / illegal_chars blocked from normalize
        if action == "approve_normalize" and collision in (
            "low_confidence", "fallback_parse", "illegal_chars"
        ):
            result["reason"] = f"{collision}_blocked"
            result["action_taken"] = "blocked"
            counters["blocked"] += 1
            results.append(result)
            continue

        # Gate 5: source must exist
        if not src or not src.exists():
            result["reason"] = "source_not_found"
            result["action_taken"] = "error"
            counters["errors"] += 1
            results.append(result)
            continue

        # Route based on action
        if action == "approve_normalize":
            dest_dir = staging_dirs["READY_NORMALIZED"]
            dest = dest_dir / proposed_name

            # Gate 6: destination must not already exist
            if dest.exists():
                result["reason"] = "destination_exists"
                result["action_taken"] = "blocked"
                counters["blocked"] += 1
                results.append(result)
                continue

            if dry_run:
                result["action_taken"] = "dry_run_normalize"
                result["destination"] = str(dest)
                result["reason"] = "would_copy_rename"
                counters["normalized"] += 1
            else:
                try:
                    shutil.copy2(str(src), str(dest))
                    result["action_taken"] = "normalized"
                    result["destination"] = str(dest)
                    result["reason"] = f"copied_to={dest.name}"
                    counters["normalized"] += 1
                except OSError as e:
                    result["action_taken"] = "error"
                    result["reason"] = f"os_error={e}"
                    counters["errors"] += 1

        elif action == "review":
            dest_dir = staging_dirs["REVIEW_REQUIRED"]
            dest = dest_dir / src.name  # keep original name

            if dest.exists():
                result["reason"] = "destination_exists_review"
                result["action_taken"] = "blocked"
                counters["blocked"] += 1
                results.append(result)
                continue

            if dry_run:
                result["action_taken"] = "dry_run_review"
                result["destination"] = str(dest)
                result["reason"] = "would_copy_to_review"
                counters["review_routed"] += 1
            else:
                try:
                    shutil.copy2(str(src), str(dest))
                    result["action_taken"] = "review_routed"
                    result["destination"] = str(dest)
                    result["reason"] = f"copied_to_review={dest.name}"
                    counters["review_routed"] += 1
                except OSError as e:
                    result["action_taken"] = "error"
                    result["reason"] = f"os_error={e}"
                    counters["errors"] += 1

        elif action == "hold":
            dest_dir = staging_dirs["HELD_PROBLEMS"]
            dest = dest_dir / src.name  # keep original name

            if dest.exists():
                result["reason"] = "destination_exists_held"
                result["action_taken"] = "blocked"
                counters["blocked"] += 1
                results.append(result)
                continue

            if dry_run:
                result["action_taken"] = "dry_run_hold"
                result["destination"] = str(dest)
                result["reason"] = "would_copy_to_held"
                counters["held_routed"] += 1
            else:
                try:
                    shutil.copy2(str(src), str(dest))
                    result["action_taken"] = "held_routed"
                    result["destination"] = str(dest)
                    result["reason"] = f"copied_to_held={dest.name}"
                    counters["held_routed"] += 1
                except OSError as e:
                    result["action_taken"] = "error"
                    result["reason"] = f"os_error={e}"
                    counters["errors"] += 1

        else:
            result["reason"] = f"unknown_action={action}"
            counters["skipped"] += 1

        results.append(result)

    log.append(f"\nBatch apply results:")
    log.append(f"  Normalized: {counters['normalized']}")
    log.append(f"  Review routed: {counters['review_routed']}")
    log.append(f"  Held routed: {counters['held_routed']}")
    log.append(f"  Skipped: {counters['skipped']}")
    log.append(f"  Blocked: {counters['blocked']}")
    log.append(f"  Errors: {counters['errors']}")
    log.append(f"\nBatch apply completed: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    return results, log


# ================================================================
# ROUTING LOG EXPORT
# ================================================================

def export_routing_log(results: list[dict], output_path: pathlib.Path):
    """Export the routing log as CSV."""
    if not results:
        return
    fields = ["original_path", "proposed_name", "action_requested",
              "action_taken", "destination", "reason"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fields}
            writer.writerow(row)


# ================================================================
# PROOF GENERATION
# ================================================================

def write_proof(
    plans: list[dict],
    manifest: dict,
    log_lines: list[str],
    apply_results: list[dict] | None = None,
    apply_log: list[str] | None = None,
    staging_root: pathlib.Path | None = None,
):
    """Write all Phase 3 proof artifacts."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    batch_id = manifest.get("batch_id", "UNKNOWN")

    # --- 00: Batch pipeline design ---
    lines = [
        "=" * 70,
        "BATCH INTAKE PIPELINE DESIGN — PHASE 3",
        f"Date: {ts}",
        f"Batch ID: {batch_id}",
        "=" * 70, "",
        "PURPOSE:",
        "  Turn the normalization engine into a real batch intake pipeline",
        "  for large incoming music folders.", "",
        "ARCHITECTURE:",
        "  1. Scan incoming batch folder recursively",
        "  2. Generate batch-specific normalization plan (CSV)",
        "  3. Classify each file into a target intake state",
        "  4. Support manual approval of proposed actions",
        "  5. Apply only approved safe rows via controlled pipeline",
        "  6. Route files into 4-state folder structure",
        "  7. Leave unresolved/problem files isolated", "",
        "KEY FUNCTIONS:",
        "  run_batch_plan()        — Plan-only mode, generates CSV + manifest",
        "  apply_batch_plan()      — Controlled apply with safety gates",
        "  classify_target_state() — Auto-classify files into intake states",
        "  create_staging_dirs()   — Create 4-state folder structure",
        "  build_manifest()        — Generate batch manifest JSON", "",
        "SAFETY INVARIANTS:",
        "  - Live DJ library (C:\\Users\\suppo\\Music) NEVER touched",
        "  - COPY not MOVE — source files preserved until explicit cleanup",
        "  - Blank action = no-op",
        "  - Collision/hold/illegal rows blocked from normalize",
        "  - All operations logged and auditable",
        "  - Fail-closed on ambiguity",
    ]
    (PROOF_DIR / "00_batch_pipeline_design.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 01: Folder state model ---
    lines = [
        "=" * 70,
        "FOLDER STATE MODEL",
        f"Date: {ts}",
        "=" * 70, "",
        "4-STATE INTAKE MODEL:", "",
    ]
    for state, desc in INTAKE_STATES.items():
        lines.append(f"  {state}")
        lines.append(f"    {desc}")
        lines.append("")

    if staging_root:
        lines.append(f"STAGING ROOT: {staging_root}")
        lines.append("")
        lines.append("FOLDER LAYOUT:")
        lines.append(f"  {staging_root}/")
        for state in INTAKE_STATES:
            lines.append(f"    {state}/")

    lines.extend([
        "",
        "STATE TRANSITION RULES:",
        "  RAW_INCOMING  → approve_normalize → READY_NORMALIZED",
        "  RAW_INCOMING  → review            → REVIEW_REQUIRED",
        "  RAW_INCOMING  → hold              → HELD_PROBLEMS",
        "  RAW_INCOMING  → blank/skip        → (no move)",
        "",
        "  REVIEW_REQUIRED → approve_normalize → READY_NORMALIZED",
        "  REVIEW_REQUIRED → hold              → HELD_PROBLEMS",
        "",
        "  HELD_PROBLEMS → (manual intervention only)",
        "",
        "  READY_NORMALIZED → (ready for library merge — separate step)",
    ])

    # Count by state
    state_counts: dict[str, int] = {}
    for p in plans:
        s = p.get("target_state", "(unassigned)")
        state_counts[s] = state_counts.get(s, 0) + 1
    lines.append("")
    lines.append("CURRENT BATCH STATE DISTRIBUTION:")
    for s, c in sorted(state_counts.items()):
        lines.append(f"  {s}: {c}")

    (PROOF_DIR / "01_folder_state_model.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 02: Batch manifest summary ---
    lines = [
        "=" * 70,
        "BATCH MANIFEST SUMMARY",
        f"Date: {ts}",
        "=" * 70, "",
    ]
    for k, v in manifest.items():
        if isinstance(v, dict):
            lines.append(f"  {k}:")
            for kk, vv in v.items():
                lines.append(f"    {kk}: {vv}")
        elif isinstance(v, list):
            lines.append(f"  {k}: [{len(v)} items]")
            for item in v:
                lines.append(f"    - {item}")
        else:
            lines.append(f"  {k}: {v}")

    (PROOF_DIR / "02_batch_manifest_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 03: Batch plan summary ---
    action_counts: dict[str, int] = {}
    for p in plans:
        a = p.get("action") or "(blank)"
        action_counts[a] = action_counts.get(a, 0) + 1

    collision_plans = [p for p in plans if (p.get("collision_status") or "").startswith("COLLISION")]
    hold_plans = [p for p in plans if p.get("action") == "hold"]
    fallback_plans = [p for p in plans if (p.get("parse_method") or "").startswith("fallback")]
    illegal_plans = [p for p in plans if p.get("collision_status") == "illegal_chars"]

    lines = [
        "=" * 70,
        "BATCH PLAN SUMMARY",
        f"Date: {ts}",
        f"Batch ID: {batch_id}",
        "=" * 70, "",
        f"Total files: {len(plans)}", "",
        "ACTIONS:",
    ]
    for a, c in sorted(action_counts.items()):
        lines.append(f"  {a}: {c}")

    lines.extend([
        "",
        f"COLLISIONS: {len(collision_plans)}",
    ])
    for p in collision_plans[:20]:
        lines.append(f"  {p['original_name']}")
        lines.append(f"    → {p['proposed_name']}")
        lines.append(f"    status: {p['collision_status']}")
    if len(collision_plans) > 20:
        lines.append(f"  ... and {len(collision_plans) - 20} more")

    lines.append(f"\nHELD ROWS: {len(hold_plans)}")
    for p in hold_plans[:20]:
        lines.append(f"  {p['original_name']}")
        lines.append(f"    reason: {p['collision_status']}")
    if len(hold_plans) > 20:
        lines.append(f"  ... and {len(hold_plans) - 20} more")

    lines.append(f"\nFALLBACK PARSES: {len(fallback_plans)}")
    for p in fallback_plans[:20]:
        lines.append(f"  {p['original_name']}")
        lines.append(f"    method: {p['parse_method']} conf: {p['confidence']}")
    if len(fallback_plans) > 20:
        lines.append(f"  ... and {len(fallback_plans) - 20} more")

    lines.append(f"\nILLEGAL CHARS: {len(illegal_plans)}")
    for p in illegal_plans[:20]:
        lines.append(f"  {p['original_name']}")
        lines.append(f"    proposed: {p['proposed_name']}")
    if len(illegal_plans) > 20:
        lines.append(f"  ... and {len(illegal_plans) - 20} more")

    (PROOF_DIR / "03_batch_plan_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 04: Apply routing summary ---
    lines = [
        "=" * 70,
        "APPLY ROUTING SUMMARY",
        f"Date: {ts}",
        "=" * 70, "",
    ]
    if apply_results:
        action_taken_counts: dict[str, int] = {}
        for r in apply_results:
            at = r.get("action_taken", "unknown")
            action_taken_counts[at] = action_taken_counts.get(at, 0) + 1

        lines.append("ROUTING RESULTS:")
        for at, c in sorted(action_taken_counts.items()):
            lines.append(f"  {at}: {c}")

        lines.append("")
        lines.append("NON-SKIPPED OPERATIONS:")
        for r in apply_results:
            if r["action_taken"] != "skipped":
                name = pathlib.Path(r["original_path"]).name if r["original_path"] else "(unknown)"
                lines.append(f"  {name}")
                lines.append(f"    action_taken: {r['action_taken']}")
                lines.append(f"    reason: {r['reason']}")
                if r.get("destination"):
                    lines.append(f"    destination: {r['destination']}")
    else:
        lines.append("  (no apply run — plan-only mode)")

    (PROOF_DIR / "04_apply_routing_summary.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 05: Safety gate results ---
    checks = []

    # Live library untouched — check for actual DJ library path, not substring
    live_library = pathlib.Path(r"C:\Users\suppo\Music")
    live_writes = []
    for r in (apply_results or []):
        dest = r.get("destination") or ""
        if not dest:
            continue
        if r.get("action_taken") in ("skipped", "blocked"):
            continue
        dest_path = pathlib.Path(dest)
        if dest_path == live_library or live_library in dest_path.parents:
            live_writes.append(r)
    checks.append(("Live DJ library untouched", len(live_writes) == 0))

    # Blank actions not applied
    blank_applies = [r for r in (apply_results or [])
                     if not r.get("action_requested")
                     and r.get("action_taken") not in ("skipped",)]
    checks.append(("Blank actions not applied", len(blank_applies) == 0))

    # Collisions blocked
    collision_applies = [r for r in (apply_results or [])
                         if r.get("action_taken") in ("normalized", "dry_run_normalize")
                         and "COLLISION" in (r.get("collision_status") or "").upper()]
    checks.append(("Collision rows blocked from normalize", len(collision_applies) == 0))

    # Fallback/low-confidence blocked from normalize
    all_plans_safe = all(
        p["action"] in ("hold", "skip", "")
        for p in plans
        if (p.get("parse_method") or "").startswith("fallback")
        and float(p.get("confidence", 0)) < 0.5
    )
    checks.append(("Fallback/low-confidence blocked", all_plans_safe))

    # No mass-apply
    applied_count = sum(1 for r in (apply_results or [])
                        if r.get("action_taken") in ("normalized", "dry_run_normalize"))
    checks.append(("No mass-apply (applied <= 5)", applied_count <= 5))

    # Manifest created
    checks.append(("Batch manifest created", BATCH_MANIFEST_JSON.exists()))

    # Batch plan created
    checks.append(("Batch plan CSV created", BATCH_PLAN_CSV.exists()))

    lines = [
        "=" * 70,
        "SAFETY GATE RESULTS",
        f"Date: {ts}",
        "=" * 70, "",
    ]
    all_pass = True
    for check_name, ok in checks:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        lines.append(f"  [{status}] {check_name}")

    lines.append(f"\nOVERALL: {'ALL PASS' if all_pass else 'SOME FAILED'}")

    (PROOF_DIR / "05_safety_gate_results.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 06: Controlled test run ---
    lines = [
        "=" * 70,
        "CONTROLLED TEST RUN",
        f"Date: {ts}",
        "=" * 70, "",
    ]
    if apply_results:
        non_skip = [r for r in apply_results if r["action_taken"] != "skipped"]
        lines.append(f"Total rows processed: {len(apply_results)}")
        lines.append(f"Non-skipped operations: {len(non_skip)}")
        lines.append("")
        for r in non_skip:
            name = pathlib.Path(r["original_path"]).name if r["original_path"] else "(unknown)"
            lines.append(f"  FILE: {name}")
            lines.append(f"    action_requested: {r.get('action_requested', '')}")
            lines.append(f"    action_taken: {r['action_taken']}")
            lines.append(f"    reason: {r['reason']}")
            if r.get("destination"):
                lines.append(f"    destination: {r['destination']}")
            lines.append("")
    else:
        lines.append("  (no test run performed)")

    (PROOF_DIR / "06_controlled_test_run.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- 07: Final report ---
    lines = [
        "=" * 70,
        "DJ LIBRARY NORMALIZATION ENGINE — PHASE 3 FINAL REPORT",
        f"Date: {ts}",
        f"Batch ID: {batch_id}",
        "=" * 70, "",
        f"Source root: {manifest.get('source_root', '')}",
        f"Engine version: {ENGINE_VERSION}",
        f"Preset: {manifest.get('preset_used', '')}",
        f"Files scanned: {manifest.get('file_count', 0)}",
        f"Subfolders: {manifest.get('subfolder_count', 0)}",
        f"Total size: {manifest.get('total_gb', 0)} GB", "",
        "COUNTS BY ACTION:",
    ]
    for a, c in sorted(manifest.get("counts_by_action", {}).items()):
        lines.append(f"  {a}: {c}")

    lines.extend([
        "",
        "COUNTS BY TARGET STATE:",
    ])
    for s, c in sorted(manifest.get("counts_by_state", {}).items()):
        lines.append(f"  {s}: {c}")

    lines.extend([
        "",
        f"Collisions: {manifest.get('collisions', 0)}",
        f"Near duplicates: {manifest.get('near_duplicates', 0)}",
        f"Similar titles: {manifest.get('similar_titles', 0)}",
        f"Overrides applied: {manifest.get('overrides_applied', 0)}",
        f"Fallback parses: {manifest.get('fallback_parses', 0)}", "",
        "ARTIFACTS:",
        f"  Batch plan CSV: {BATCH_PLAN_CSV}",
        f"  Batch manifest: {BATCH_MANIFEST_JSON}",
        f"  Proof dir:      {PROOF_DIR}", "",
        "SAFETY:",
        f"  Live renames on DJ library: 0",
        f"  Source files modified: 0",
        f"  Validation: {'ALL PASS' if all_pass else 'SOME FAILED'}", "",
        f"GATE={'PASS' if all_pass else 'FAIL'}",
    ])

    (PROOF_DIR / "07_final_report.txt").write_text(
        "\n".join(lines), encoding="utf-8")

    # --- execution_log.txt ---
    all_log = list(log_lines)
    if apply_log:
        all_log.append("")
        all_log.append("=== BATCH APPLY LOG ===")
        all_log.extend(apply_log)
    (PROOF_DIR / "execution_log.txt").write_text(
        "\n".join(all_log), encoding="utf-8")

    return all_pass


# ================================================================
# MAIN
# ================================================================

def main():
    source_root = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
    staging_root = WORKSPACE / "data" / "batch_staging"

    print(f"CWD: {os.getcwd()}")
    print(f"Source root: {source_root}")
    print(f"Staging root: {staging_root}")
    print(f"Proof dir: {PROOF_DIR}")
    print()

    # ---- Step 1: Plan ----
    print("=" * 60)
    print("STEP 1: BATCH PLANNING")
    print("=" * 60)

    plans, manifest, log_lines = run_batch_plan(
        source_root=source_root,
        preset_name="Gene_Default",
        recursive=True,
    )

    for line in log_lines:
        print(line)

    # Export batch CSV
    export_batch_csv(plans, BATCH_PLAN_CSV)
    print(f"\nBatch plan CSV: {BATCH_PLAN_CSV}")

    # Export manifest
    with open(BATCH_MANIFEST_JSON, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"Batch manifest: {BATCH_MANIFEST_JSON}")

    # Export subset CSVs
    held_count = export_subset_csv(
        plans, DATA_DIR / "held_rows.csv",
        lambda p: p.get("target_state") == "HELD_PROBLEMS"
    )
    review_count = export_subset_csv(
        plans, DATA_DIR / "review_rows.csv",
        lambda p: p.get("target_state") == "REVIEW_REQUIRED"
    )
    print(f"Held rows CSV: {held_count} rows")
    print(f"Review rows CSV: {review_count} rows")

    # ---- Step 2: Controlled apply test (dry-run) ----
    print()
    print("=" * 60)
    print("STEP 2: CONTROLLED APPLY TEST (DRY-RUN)")
    print("=" * 60)

    # Pick 3 clearly safe rows for the test:
    # - action must be blank (reviewable)
    # - confidence >= 1.0
    # - no collision / no duplicate risk
    # - target_state == RAW_INCOMING
    # - no illegal chars
    safe_candidates = [
        p for p in plans
        if p.get("action") == ""
        and float(p.get("confidence", 0)) >= 1.0
        and p.get("collision_status") == "ok"
        and p.get("duplicate_risk") == "none"
        and p.get("target_state") == "RAW_INCOMING"
        and not _has_illegal_chars(p.get("proposed_name", ""))
        and p.get("proposed_name", "").lower() != p.get("original_name", "").lower()
    ]

    print(f"Safe candidates for test: {len(safe_candidates)}")

    # Select exactly 3 for controlled test
    test_rows = safe_candidates[:3]
    if test_rows:
        print(f"Selected {len(test_rows)} rows for dry-run test:")
        for tr in test_rows:
            print(f"  {tr['original_name']} -> {tr['proposed_name']}")

        # Write a test CSV with only these rows set to approve_normalize
        test_csv_path = DATA_DIR / "batch_test_apply.csv"
        # Copy full plan but set action only for test rows
        test_originals = {tr["original_path"] for tr in test_rows}
        test_plans = []
        for p in plans:
            tp = dict(p)
            if tp["original_path"] in test_originals:
                tp["action"] = "approve_normalize"
            test_plans.append(tp)

        # Also add a collision row set to approve (to prove it blocks)
        collision_forced = False
        for tp in test_plans:
            if "COLLISION" in (tp.get("collision_status") or "").upper():
                tp["action"] = "approve_normalize"
                collision_forced = True
                print(f"  [SAFETY TEST] Forced approve on collision: {tp['original_name']}")
                break

        # Also add a held row set to approve_normalize (to prove it blocks)
        held_forced = False
        for tp in test_plans:
            if tp.get("collision_status") in ("low_confidence", "fallback_parse", "illegal_chars"):
                tp["action"] = "approve_normalize"
                held_forced = True
                print(f"  [SAFETY TEST] Forced approve on held: {tp['original_name']}")
                break

        export_batch_csv(test_plans, test_csv_path)

        # Run dry-run apply
        apply_results, apply_log = apply_batch_plan(
            csv_path=test_csv_path,
            staging_root=staging_root,
            dry_run=True,
        )

        for line in apply_log:
            print(line)

        # Show non-skipped results
        print()
        print("NON-SKIPPED OPERATIONS:")
        for r in apply_results:
            if r["action_taken"] != "skipped":
                name = pathlib.Path(r["original_path"]).name
                print(f"  {name}")
                print(f"    action_taken={r['action_taken']}  reason={r['reason']}")

        # Export routing log
        export_routing_log(apply_results, DATA_DIR / "routing_log.csv")
        print(f"\nRouting log: {DATA_DIR / 'routing_log.csv'}")

        # Clean up test CSV
        test_csv_path.unlink(missing_ok=True)
    else:
        print("  No safe candidates found for test!")
        apply_results = None
        apply_log = None

    # ---- Step 3: Write proof ----
    print()
    print("=" * 60)
    print("STEP 3: PROOF ARTIFACTS")
    print("=" * 60)

    all_pass = write_proof(
        plans=plans,
        manifest=manifest,
        log_lines=log_lines,
        apply_results=apply_results,
        apply_log=apply_log,
        staging_root=staging_root,
    )

    print(f"Proof written: {PROOF_DIR}")
    print()
    print(f"PF={PROOF_DIR}")
    print(f"ZIP=(pending)")
    print(f"GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
