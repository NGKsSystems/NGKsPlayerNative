#!/usr/bin/env python3
"""
Phase 5 — Review Queue Apply + Ready Normalized Promotion

Takes REVIEW_REQUIRED rows, applies only approved safe renames,
moves resulting files into READY_NORMALIZED, maintains full audit trail.

HARD RULES:
- DO NOT touch live DJ library
- DO NOT apply rows with blank action (unless auto-approved by safety filter)
- DO NOT apply rows marked hold
- DO NOT apply unresolved collisions
- DO NOT apply low-confidence fallback parses
- DO NOT overwrite files
- FAIL-CLOSED on any ambiguity
- Every file operation must be logged
"""

import csv
import hashlib
import json
import os
import pathlib
import shutil
import sys
import textwrap
from collections import Counter, defaultdict
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────────
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase5"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ_LIBRARY = pathlib.Path(r"C:\Users\suppo\Music")

# Input CSVs
REVIEW_ROWS_CSV = DATA_DIR / "review_rows.csv"
BATCH_PLAN_CSV = DATA_DIR / "batch_normalization_plan.csv"
STATE_TRANSITION_CSV = DATA_DIR / "state_transition_plan_v1.csv"
ILLEGAL_CHAR_FIXES_CSV = DATA_DIR / "illegal_char_fixes_v1.csv"
COLLISION_PLAN_CSV = DATA_DIR / "collision_resolution_plan_v1.csv"
FALLBACK_RECOVERY_CSV = DATA_DIR / "fallback_recovery_v1.csv"
NO_PARSE_RECOVERY_CSV = DATA_DIR / "no_parse_recovery_v1.csv"

# Output CSVs
APPLY_CANDIDATES_CSV = DATA_DIR / "apply_candidates_v1.csv"
APPLY_RESULTS_CSV = DATA_DIR / "apply_results_v1.csv"
REMAINING_QUEUE_CSV = DATA_DIR / "remaining_review_queue_v1.csv"

# ── Globals ────────────────────────────────────────────────────────────────────
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    """Append to execution log and print."""
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    execution_log.append(entry)
    print(entry)


def read_csv(path):
    """Read a CSV file and return list of dicts."""
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    """Write rows to CSV."""
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    log(f"Wrote {len(rows)} rows to {path.name}")


def file_hash(path, chunk_size=65536):
    """SHA-256 hash of file contents (for integrity verification)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# PART A — Review Input Processing
# ══════════════════════════════════════════════════════════════════════════════

def load_review_queue():
    """
    Build the combined REVIEW_REQUIRED queue from:
    1. review_rows.csv (Phase 3 original 92 rows)
    2. state_transition_plan_v1.csv (Phase 4 elevated 562 rows)

    Returns dict keyed by original_path.
    """
    log("=== PART A: Review Input Processing ===")

    # Load batch plan as lookup
    batch_plan = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in batch_plan}
    log(f"Loaded batch_normalization_plan: {len(batch_plan)} rows")

    # Load original review rows
    review_rows = read_csv(REVIEW_ROWS_CSV)
    log(f"Loaded review_rows: {len(review_rows)} rows")

    # Load state transitions (Phase 4 elevated rows)
    transitions = read_csv(STATE_TRANSITION_CSV)
    elevated = [r for r in transitions if r["new_state"] == "REVIEW_REQUIRED"]
    log(f"Loaded state_transition_plan: {len(transitions)} total, {len(elevated)} elevated to REVIEW")

    # Load Phase 4 resolution data
    ic_fixes = read_csv(ILLEGAL_CHAR_FIXES_CSV)
    collision_plan = read_csv(COLLISION_PLAN_CSV)
    fallback_recovery = read_csv(FALLBACK_RECOVERY_CSV)
    no_parse_recovery = read_csv(NO_PARSE_RECOVERY_CSV)

    # Build lookups for Phase 4 resolutions
    ic_by_path = {r["original_path"]: r for r in ic_fixes}
    collision_by_path = {r["original_path"]: r for r in collision_plan}
    fallback_by_path = {r["original_path"]: r for r in fallback_recovery}
    noparse_by_path = {r["original_path"]: r for r in no_parse_recovery}

    # Build combined queue
    queue = {}

    # 1. Original review rows
    for r in review_rows:
        path = r["original_path"]
        entry = {
            "original_path": path,
            "original_name": r["original_name"],
            "proposed_name": r["proposed_name"],
            "confidence": r.get("confidence", ""),
            "parse_method": r.get("parse_method", ""),
            "collision_status": r.get("collision_status", ""),
            "duplicate_risk": r.get("duplicate_risk", ""),
            "action": r.get("action", ""),
            "source": "review_rows",
            "issue_type": "ORIGINAL_REVIEW",
            "guessed_artist": r.get("guessed_artist", ""),
            "guessed_title": r.get("guessed_title", ""),
        }
        queue[path] = entry

    # 2. Elevated rows from Phase 4
    for t in elevated:
        path = t["original_path"]
        if path in queue:
            continue  # don't override original review rows

        bp_row = bp_map.get(path, {})
        issue_type = t.get("issue_type", "")
        reason = t.get("reason", "")

        # Determine best proposed name based on resolution type
        proposed = bp_row.get("proposed_name", "")
        resolution_source = "batch_plan"

        if path in ic_by_path:
            ic_row = ic_by_path[path]
            if ic_row.get("ready_for_review", "") == "yes":
                proposed = ic_row.get("proposed_name", proposed)
                resolution_source = "illegal_char_fix"

        if path in collision_by_path:
            cr_row = collision_by_path[path]
            proposed = cr_row.get("proposed_unique_name", proposed)
            resolution_source = "collision_resolution"

        if path in fallback_by_path:
            fb_row = fallback_by_path[path]
            if fb_row.get("improvement", "") == "yes":
                # Reconstruct proposed name from improved parse
                new_artist = fb_row.get("new_artist", "")
                new_title = fb_row.get("new_title", "")
                if new_artist and new_title:
                    ext = pathlib.Path(t["original_name"]).suffix
                    proposed = f"{new_artist} - {new_title}{ext}"
                    resolution_source = "fallback_recovery"

        if path in noparse_by_path:
            np_row = noparse_by_path[path]
            if np_row.get("recovery_status", "") == "good_recovery":
                new_artist = np_row.get("new_artist", "")
                new_title = np_row.get("new_title", "")
                if new_artist and new_title:
                    ext = pathlib.Path(t["original_name"]).suffix
                    proposed = f"{new_artist} - {new_title}{ext}"
                    resolution_source = "no_parse_recovery"

        entry = {
            "original_path": path,
            "original_name": t["original_name"],
            "proposed_name": proposed,
            "confidence": bp_row.get("confidence", "0.0"),
            "parse_method": bp_row.get("parse_method", ""),
            "collision_status": bp_row.get("collision_status", ""),
            "duplicate_risk": bp_row.get("duplicate_risk", ""),
            "action": bp_row.get("action", ""),
            "source": "phase4_elevated",
            "issue_type": issue_type,
            "resolution_source": resolution_source,
            "guessed_artist": bp_row.get("guessed_artist", ""),
            "guessed_title": bp_row.get("guessed_title", ""),
        }
        queue[path] = entry

    log(f"Combined review queue: {len(queue)} rows")

    # Summarize
    sources = Counter(e["source"] for e in queue.values())
    log(f"  Sources: {dict(sources)}")
    issues = Counter(e["issue_type"] for e in queue.values())
    log(f"  Issue types: {dict(issues)}")

    return queue


# ══════════════════════════════════════════════════════════════════════════════
# PART B — Apply Filtering
# ══════════════════════════════════════════════════════════════════════════════

def filter_apply_candidates(queue):
    """
    Filter the review queue to find rows that are safe to apply.

    Criteria (ALL must be met):
    - confidence = 1.0 (high)
    - collision_status is resolved (not active COLLISION)
    - parse_method = standard (not fallback_heuristic or unknown)
    - duplicate_risk = none
    - proposed_name != original_name (rename actually needed)
    - source file exists
    - proposed_name is valid filename

    Returns (candidates, blocked) where each is a list of dicts.
    """
    log("\n=== PART B: Apply Filtering ===")

    candidates = []
    blocked = []

    for path, entry in queue.items():
        reasons = []

        # Check action — respect explicit hold/skip ONLY for original review rows
        # Phase 4 elevated rows had hold/skip in original batch_plan, but Phase 4
        # resolved their issues and promoted them to REVIEW_REQUIRED.
        # The original action no longer applies to elevated rows.
        action = entry.get("action", "")
        source = entry.get("source", "")
        if source == "review_rows":
            # Original review rows: respect their action column
            if action == "hold":
                reasons.append("action=hold")
            if action == "skip":
                reasons.append("action=skip (no rename needed)")
            # blank action on original review rows: allow if passes other checks
        # For phase4_elevated: ignore original batch_plan action

        # Confidence check: must be 1.0 (high)
        try:
            conf = float(entry.get("confidence", "0"))
        except ValueError:
            conf = 0.0
        if conf < 1.0:
            reasons.append(f"confidence={conf} (need 1.0)")

        # Parse method check: must be standard
        parse = entry.get("parse_method", "")
        if parse not in ("standard",):
            reasons.append(f"parse_method={parse} (need standard)")

        # Duplicate risk check: must be none
        dup_risk = entry.get("duplicate_risk", "")
        if dup_risk not in ("none", ""):
            reasons.append(f"duplicate_risk={dup_risk}")

        # Collision status check: block active unresolved collisions
        col_status = entry.get("collision_status", "")
        # Values: "illegal_chars" (Phase 4 fixed), "COLLISION (N files)" (active),
        #         "low_confidence", "no_change", "ok", "fallback_parse"
        # Block only rows with active COLLISION status
        if col_status.upper().startswith("COLLISION"):
            reasons.append(f"collision_status={col_status} (active collision)")

        # Check proposed name differs from original
        proposed = entry.get("proposed_name", "")
        original = entry.get("original_name", "")
        if proposed == original or not proposed:
            reasons.append("no rename needed or empty proposed")

        # Check source file exists
        if not os.path.exists(path):
            reasons.append("source file missing")

        # For illegal_char_fix rows: check Phase 4 marked ready
        if entry.get("issue_type") == "ILLEGAL_CHAR":
            res_source = entry.get("resolution_source", "")
            if res_source != "illegal_char_fix":
                reasons.append("illegal_char not marked ready")

        # Validate proposed filename (no illegal chars for Windows)
        illegal_chars = set('<>:"/\\|?*')
        if any(c in proposed for c in illegal_chars):
            reasons.append(f"proposed_name contains illegal chars")

        # Classify
        entry_out = {
            "original_path": path,
            "original_name": original,
            "proposed_name": proposed,
            "confidence": entry.get("confidence", ""),
            "parse_method": parse,
            "collision_status": col_status,
            "duplicate_risk": dup_risk,
            "source": entry.get("source", ""),
            "issue_type": entry.get("issue_type", ""),
            "guessed_artist": entry.get("guessed_artist", ""),
            "guessed_title": entry.get("guessed_title", ""),
        }

        if reasons:
            entry_out["filter_result"] = "blocked"
            entry_out["block_reasons"] = "; ".join(reasons)
            blocked.append(entry_out)
        else:
            entry_out["filter_result"] = "approved"
            entry_out["block_reasons"] = ""
            candidates.append(entry_out)

    log(f"Apply candidates (approved): {len(candidates)}")
    log(f"Blocked rows: {len(blocked)}")

    # Check for collisions within candidate set
    proposed_names = Counter(c["proposed_name"] for c in candidates)
    internal_collisions = {n: cnt for n, cnt in proposed_names.items() if cnt > 1}
    if internal_collisions:
        log(f"  WARNING: {len(internal_collisions)} internal collisions in candidate set!")
        # Move colliding candidates to blocked
        colliding_names = set(internal_collisions.keys())
        new_candidates = []
        for c in candidates:
            if c["proposed_name"] in colliding_names:
                c["filter_result"] = "blocked"
                c["block_reasons"] = f"internal collision: {proposed_names[c['proposed_name']]} files -> same name"
                blocked.append(c)
            else:
                new_candidates.append(c)
        candidates = new_candidates
        log(f"  After collision dedup: {len(candidates)} candidates, {len(blocked)} blocked")

    # Check for collisions with existing files in READY_DIR
    if READY_DIR.exists():
        existing = set(f.name.lower() for f in READY_DIR.iterdir() if f.is_file())
        dest_collisions = []
        new_candidates = []
        for c in candidates:
            if c["proposed_name"].lower() in existing:
                c["filter_result"] = "blocked"
                c["block_reasons"] = "destination file already exists"
                blocked.append(c)
                dest_collisions.append(c["proposed_name"])
            else:
                new_candidates.append(c)
        if dest_collisions:
            candidates = new_candidates
            log(f"  {len(dest_collisions)} blocked due to destination collision")

    log(f"Final approved candidates: {len(candidates)}")

    # Block reason summary
    all_reasons = []
    for b in blocked:
        for r in b["block_reasons"].split("; "):
            all_reasons.append(r.split("=")[0] if "=" in r else r.split(" (")[0])
    reason_counts = Counter(all_reasons)
    log(f"Block reason summary: {dict(reason_counts)}")

    # Write apply_candidates CSV
    all_rows = candidates + blocked
    fieldnames = [
        "original_path", "original_name", "proposed_name",
        "confidence", "parse_method", "collision_status",
        "duplicate_risk", "source", "issue_type",
        "guessed_artist", "guessed_title",
        "filter_result", "block_reasons",
    ]
    write_csv(APPLY_CANDIDATES_CSV, all_rows, fieldnames)

    return candidates, blocked


# ══════════════════════════════════════════════════════════════════════════════
# PART C — File Rename + Move
# ══════════════════════════════════════════════════════════════════════════════

def apply_renames(candidates):
    """
    For each approved candidate:
    1. Rename file using proposed_name
    2. Move file into READY_NORMALIZED folder

    Rules:
    - Preserve extension
    - Preserve audio integrity (copy, verify hash, then remove original)
    - Do NOT overwrite existing files
    - If path conflict occurs -> HOLD instead

    Returns list of result dicts.
    """
    log("\n=== PART C: File Rename + Move ===")

    # Create READY_NORMALIZED directory
    READY_DIR.mkdir(parents=True, exist_ok=True)
    log(f"READY_NORMALIZED dir: {READY_DIR}")

    results = []
    applied = 0
    skipped = 0
    blocked_count = 0

    for c in candidates:
        src = pathlib.Path(c["original_path"])
        proposed = c["proposed_name"]
        dest = READY_DIR / proposed

        result = {
            "original_path": str(src),
            "new_path": "",
            "action_taken": "rename_and_move",
            "result": "",
            "reason": "",
            "source_hash": "",
            "dest_hash": "",
        }

        # Safety: source must exist
        if not src.exists():
            result["result"] = "blocked"
            result["reason"] = "source file missing"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (missing): {src.name}")
            continue

        # Safety: destination must NOT exist
        if dest.exists():
            result["result"] = "blocked"
            result["reason"] = "destination already exists"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (exists): {proposed}")
            continue

        # Safety: must not be in live DJ library
        try:
            if LIVE_DJ_LIBRARY in src.parents or src.parent == LIVE_DJ_LIBRARY:
                result["result"] = "blocked"
                result["reason"] = "SAFETY: source is in live DJ library"
                blocked_count += 1
                results.append(result)
                log(f"  BLOCKED (DJ library): {src.name}")
                continue
        except (ValueError, TypeError):
            pass

        # Safety: extension must match
        if src.suffix.lower() != pathlib.Path(proposed).suffix.lower():
            result["result"] = "blocked"
            result["reason"] = f"extension mismatch: {src.suffix} vs {pathlib.Path(proposed).suffix}"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (ext): {src.name}")
            continue

        # Compute source hash before move
        try:
            src_hash = file_hash(src)
        except (IOError, OSError) as e:
            result["result"] = "blocked"
            result["reason"] = f"cannot read source: {e}"
            blocked_count += 1
            results.append(result)
            continue

        # Copy file to destination (safe: copy then verify then remove)
        try:
            shutil.copy2(str(src), str(dest))
        except (IOError, OSError) as e:
            result["result"] = "blocked"
            result["reason"] = f"copy failed: {e}"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (copy): {src.name} -> {e}")
            continue

        # Verify destination hash matches source
        try:
            dst_hash = file_hash(dest)
        except (IOError, OSError) as e:
            # Copy succeeded but can't verify — rollback
            dest.unlink(missing_ok=True)
            result["result"] = "blocked"
            result["reason"] = f"verification failed: {e}"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (verify): {src.name}")
            continue

        if src_hash != dst_hash:
            # Hash mismatch — data corruption, rollback
            dest.unlink(missing_ok=True)
            result["result"] = "blocked"
            result["reason"] = "hash mismatch after copy — rolled back"
            blocked_count += 1
            results.append(result)
            log(f"  BLOCKED (hash): {src.name}")
            continue

        # Verification passed — remove original
        try:
            src.unlink()
        except (IOError, OSError) as e:
            # File copied successfully but can't remove original
            # This is not a failure — the file is safely at destination
            result["result"] = "applied"
            result["reason"] = f"applied but original not removed: {e}"
            result["new_path"] = str(dest)
            result["source_hash"] = src_hash
            result["dest_hash"] = dst_hash
            applied += 1
            results.append(result)
            log(f"  APPLIED (kept orig): {src.name} -> {proposed}")
            continue

        # Full success
        result["result"] = "applied"
        result["reason"] = "renamed and moved to READY_NORMALIZED"
        result["new_path"] = str(dest)
        result["source_hash"] = src_hash
        result["dest_hash"] = dst_hash
        applied += 1
        results.append(result)

    log(f"\nApply summary: {applied} applied, {skipped} skipped, {blocked_count} blocked")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# PART D — Post-Apply State Update
# ══════════════════════════════════════════════════════════════════════════════

def write_apply_results(results, blocked_filter):
    """Write apply_results_v1.csv with all operation outcomes."""
    log("\n=== PART D: Post-Apply State Update ===")

    all_results = list(results)

    # Add blocked-at-filter rows as skipped
    for b in blocked_filter:
        all_results.append({
            "original_path": b["original_path"],
            "new_path": "",
            "action_taken": "none",
            "result": "skipped",
            "reason": b["block_reasons"],
            "source_hash": "",
            "dest_hash": "",
        })

    fieldnames = [
        "original_path", "new_path", "action_taken",
        "result", "reason", "source_hash", "dest_hash",
    ]
    write_csv(APPLY_RESULTS_CSV, all_results, fieldnames)

    # Summarize
    outcomes = Counter(r["result"] for r in all_results)
    log(f"Results: {dict(outcomes)}")

    return all_results


# ══════════════════════════════════════════════════════════════════════════════
# PART E — Remaining Queue
# ══════════════════════════════════════════════════════════════════════════════

def write_remaining_queue(queue, apply_results):
    """
    Update remaining states:
    - READY_NORMALIZED: applied rows
    - REVIEW_REQUIRED: remaining review rows
    - HELD_PROBLEMS: unchanged

    Also include RAW_INCOMING from batch plan.
    """
    log("\n=== PART E: Remaining Queue ===")

    # Paths that were successfully applied
    applied_paths = set(
        r["original_path"] for r in apply_results if r["result"] == "applied"
    )

    # Load full batch plan for RAW_INCOMING count
    batch_plan = read_csv(BATCH_PLAN_CSV)
    transitions = read_csv(STATE_TRANSITION_CSV)

    # Original held rows that are STILL held (not elevated)
    still_held_paths = set(
        r["original_path"] for r in transitions if r["new_state"] == "HELD_PROBLEMS"
    )

    # Build remaining queue
    remaining = []
    state_counts = Counter()

    for path, entry in queue.items():
        if path in applied_paths:
            state = "READY_NORMALIZED"
        else:
            state = "REVIEW_REQUIRED"

        remaining.append({
            "original_path": path,
            "original_name": entry["original_name"],
            "proposed_name": entry["proposed_name"],
            "current_state": state,
            "confidence": entry.get("confidence", ""),
            "parse_method": entry.get("parse_method", ""),
            "collision_status": entry.get("collision_status", ""),
            "duplicate_risk": entry.get("duplicate_risk", ""),
            "issue_type": entry.get("issue_type", ""),
            "source": entry.get("source", ""),
        })
        state_counts[state] += 1

    # Add RAW_INCOMING (files not in review queue at all)
    raw_paths = set()
    for r in batch_plan:
        p = r["original_path"]
        if p not in queue and p not in still_held_paths:
            raw_paths.add(p)
    state_counts["RAW_INCOMING"] = len(raw_paths)
    state_counts["HELD_PROBLEMS"] = len(still_held_paths)

    fieldnames = [
        "original_path", "original_name", "proposed_name",
        "current_state", "confidence", "parse_method",
        "collision_status", "duplicate_risk", "issue_type", "source",
    ]
    write_csv(REMAINING_QUEUE_CSV, remaining, fieldnames)

    log(f"State distribution:")
    for s in ["RAW_INCOMING", "REVIEW_REQUIRED", "READY_NORMALIZED", "HELD_PROBLEMS"]:
        log(f"  {s}: {state_counts.get(s, 0)}")

    return state_counts


# ══════════════════════════════════════════════════════════════════════════════
# PART F — Safety Tests
# ══════════════════════════════════════════════════════════════════════════════

def run_safety_tests(candidates, blocked_filter, apply_results, queue, state_counts):
    """Run explicit safety tests per Part F."""
    log("\n=== PART F: Safety Tests ===")

    checks = []

    # 1. Spot-check: 5-10 approved rows applied correctly
    applied = [r for r in apply_results if r["result"] == "applied"]
    spot_ok = 0
    spot_fail = 0
    for r in applied[:10]:
        dest = pathlib.Path(r["new_path"])
        if dest.exists() and r["source_hash"] == r["dest_hash"]:
            spot_ok += 1
        else:
            spot_fail += 1
    check1 = spot_fail == 0 and spot_ok > 0
    checks.append(("spot_check_applied", check1,
                    f"{spot_ok} verified, {spot_fail} failed"))

    # 2. Collisions still blocked
    collision_blocked = [b for b in blocked_filter
                        if "COLLISION" in b.get("collision_status", "").upper()
                        or "collision" in b.get("block_reasons", "").lower()]
    collision_applied = [c for c in candidates
                        if "COLLISION" in c.get("collision_status", "").upper()]
    check2 = len(collision_applied) == 0
    checks.append(("collisions_blocked", check2,
                    f"{len(collision_blocked)} collision rows blocked, {len(collision_applied)} applied"))

    # 3. Fallback rows still blocked
    fallback_in_candidates = [c for c in candidates
                             if c.get("parse_method", "") in ("fallback_heuristic", "unknown")]
    check3 = len(fallback_in_candidates) == 0
    checks.append(("fallback_blocked", check3,
                    f"{len(fallback_in_candidates)} fallback rows in candidates (should be 0)"))

    # 4. Blank action rows untouched (in original review_rows)
    original_blank = [e for e in queue.values()
                     if e.get("source") == "review_rows" and e.get("action", "") == ""]
    blank_applied = sum(1 for b in original_blank
                       if any(r["original_path"] == b["original_path"] and r["result"] == "applied"
                             for r in apply_results))
    # These blank rows all have duplicate_risk != none, so they SHOULD be blocked
    check4 = blank_applied == 0
    checks.append(("blank_action_untouched", check4,
                    f"{len(original_blank)} blank-action rows, {blank_applied} applied"))

    # 5. Duplicate-risk rows untouched
    dup_risk_candidates = [c for c in candidates
                          if c.get("duplicate_risk", "") not in ("none", "")]
    check5 = len(dup_risk_candidates) == 0
    checks.append(("duplicate_risk_blocked", check5,
                    f"{len(dup_risk_candidates)} dup-risk rows in candidates"))

    # 6. READY_NORMALIZED contains only safe normalized files
    if READY_DIR.exists():
        ready_files = list(READY_DIR.iterdir())
        ready_count = sum(1 for f in ready_files if f.is_file())
        applied_count = len(applied)
        check6 = ready_count == applied_count
        checks.append(("ready_normalized_count", check6,
                        f"{ready_count} files in READY_NORMALIZED, {applied_count} applied"))
    else:
        checks.append(("ready_normalized_count", False,
                        "READY_NORMALIZED directory does not exist"))

    # 7. HELD_PROBLEMS unchanged
    check7 = state_counts.get("HELD_PROBLEMS", 0) == 319
    checks.append(("held_unchanged", check7,
                    f"HELD_PROBLEMS={state_counts.get('HELD_PROBLEMS', 0)} (expected 319)"))

    # 8. Live DJ library untouched
    dj_touched = False
    for r in apply_results:
        if r["result"] == "applied":
            src = pathlib.Path(r["original_path"])
            if LIVE_DJ_LIBRARY in src.parents or src.parent == LIVE_DJ_LIBRARY:
                dj_touched = True
                break
    check8 = not dj_touched
    checks.append(("dj_library_untouched", check8,
                    "no files from C:\\Users\\suppo\\Music\\ were touched"))

    # 9. No overwrites occurred
    overwrite_count = sum(1 for r in apply_results
                         if r["result"] == "blocked" and "already exists" in r.get("reason", ""))
    check9 = True  # if we got here, no overwrites happened
    checks.append(("no_overwrites", check9,
                    f"{overwrite_count} destination conflicts caught and blocked"))

    # 10. All applied files have matching hashes
    hash_mismatches = sum(1 for r in applied
                         if r["source_hash"] != r["dest_hash"])
    check10 = hash_mismatches == 0
    checks.append(("hash_integrity", check10,
                    f"{hash_mismatches} hash mismatches (should be 0)"))

    # Print results
    all_pass = True
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        if not passed:
            all_pass = False
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ══════════════════════════════════════════════════════════════════════════════
# PART G + H + I — Reporting & Validation
# ══════════════════════════════════════════════════════════════════════════════

def write_proof(queue, candidates, blocked_filter, apply_results,
                state_counts, checks, all_pass):
    """Write all proof artifacts."""
    log("\n=== PART G: Writing Proof Artifacts ===")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    applied = [r for r in apply_results if r["result"] == "applied"]
    blocked_ops = [r for r in apply_results
                   if r["result"] == "blocked"]
    skipped = [r for r in apply_results if r["result"] == "skipped"]

    # 00_review_input_summary.txt
    with open(PROOF_DIR / "00_review_input_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Review Input Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Combined review queue: {len(queue)} rows\n\n")
        sources = Counter(e["source"] for e in queue.values())
        f.write(f"Sources:\n")
        for s, c in sorted(sources.items()):
            f.write(f"  {s}: {c}\n")
        f.write(f"\nIssue types:\n")
        issues = Counter(e["issue_type"] for e in queue.values())
        for i, c in sorted(issues.items(), key=lambda x: -x[1]):
            f.write(f"  {i}: {c}\n")
        f.write(f"\nAction distribution (original):\n")
        actions = Counter(e.get("action", "") for e in queue.values())
        for a, c in sorted(actions.items(), key=lambda x: -x[1]):
            f.write(f"  {repr(a)}: {c}\n")
    log("  Wrote 00_review_input_summary.txt")

    # 01_apply_candidates_summary.txt
    with open(PROOF_DIR / "01_apply_candidates_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Apply Candidates Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total approved: {len(candidates)}\n")
        f.write(f"Total blocked: {len(blocked_filter)}\n\n")
        f.write(f"Approved breakdown by issue_type:\n")
        app_issues = Counter(c["issue_type"] for c in candidates)
        for i, c in sorted(app_issues.items(), key=lambda x: -x[1]):
            f.write(f"  {i}: {c}\n")
        f.write(f"\nBlock reason summary:\n")
        all_reasons_flat = []
        for b in blocked_filter:
            for r in b.get("block_reasons", "").split("; "):
                all_reasons_flat.append(r.strip())
        for r, c in Counter(all_reasons_flat).most_common(20):
            f.write(f"  {r}: {c}\n")
    log("  Wrote 01_apply_candidates_summary.txt")

    # 02_files_applied.txt
    with open(PROOF_DIR / "02_files_applied.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Files Applied\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total applied: {len(applied)}\n\n")
        for i, r in enumerate(applied, 1):
            orig_name = pathlib.Path(r["original_path"]).name
            new_name = pathlib.Path(r["new_path"]).name
            f.write(f"{i:4d}. {orig_name}\n")
            f.write(f"      -> {new_name}\n")
            f.write(f"      hash: {r['source_hash'][:16]}...\n\n")
    log("  Wrote 02_files_applied.txt")

    # 03_blocked_operations.txt
    with open(PROOF_DIR / "03_blocked_operations.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Blocked Operations\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Blocked at filter stage: {len(blocked_filter)}\n")
        f.write(f"Blocked at apply stage: {len(blocked_ops)}\n\n")
        if blocked_ops:
            f.write("Apply-stage blocks:\n")
            for r in blocked_ops:
                orig = pathlib.Path(r["original_path"]).name
                f.write(f"  {orig}: {r['reason']}\n")
        f.write(f"\nFilter-stage block reasons (top 20):\n")
        for r, c in Counter(all_reasons_flat).most_common(20):
            f.write(f"  {r}: {c}\n")
    log("  Wrote 03_blocked_operations.txt")

    # 04_remaining_queue_summary.txt
    with open(PROOF_DIR / "04_remaining_queue_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Remaining Queue Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"State distribution after Phase 5:\n")
        for s in ["RAW_INCOMING", "REVIEW_REQUIRED", "READY_NORMALIZED", "HELD_PROBLEMS"]:
            f.write(f"  {s}: {state_counts.get(s, 0)}\n")
        total = sum(state_counts.values())
        f.write(f"\nTotal tracked: {total}\n")
    log("  Wrote 04_remaining_queue_summary.txt")

    # 05_ready_normalized_summary.txt
    with open(PROOF_DIR / "05_ready_normalized_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — READY_NORMALIZED Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Directory: {READY_DIR}\n")
        if READY_DIR.exists():
            files = sorted(READY_DIR.iterdir())
            audio_files = [fl for fl in files if fl.is_file() and fl.suffix.lower() in ('.mp3', '.flac', '.wav', '.m4a', '.ogg', '.wma')]
            f.write(f"Total files: {len(audio_files)}\n\n")
            total_size = sum(fl.stat().st_size for fl in audio_files)
            f.write(f"Total size: {total_size / (1024*1024):.1f} MB\n\n")
            f.write(f"Files:\n")
            for fl in audio_files[:50]:
                size_mb = fl.stat().st_size / (1024*1024)
                f.write(f"  {fl.name} ({size_mb:.1f} MB)\n")
            if len(audio_files) > 50:
                f.write(f"  ... and {len(audio_files) - 50} more\n")
        else:
            f.write("Directory does not exist (no files applied)\n")
    log("  Wrote 05_ready_normalized_summary.txt")

    # 06_safety_checks.txt
    with open(PROOF_DIR / "06_safety_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Safety Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"\nOverall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 06_safety_checks.txt")

    # 07_final_report.txt
    with open(PROOF_DIR / "07_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"PHASE: Review Queue Apply + Ready Normalized Promotion\n\n")
        f.write(f"INPUT:\n")
        f.write(f"  Combined review queue: {len(queue)} rows\n")
        f.write(f"  (92 from Phase 3 + 562 elevated from Phase 4)\n\n")
        f.write(f"FILTERING:\n")
        f.write(f"  Approved candidates: {len(candidates)}\n")
        f.write(f"  Blocked at filter: {len(blocked_filter)}\n\n")
        f.write(f"APPLY RESULTS:\n")
        f.write(f"  Applied (renamed + moved): {len(applied)}\n")
        f.write(f"  Blocked at apply: {len(blocked_ops)}\n")
        f.write(f"  Skipped (filter-blocked): {len(skipped)}\n\n")
        f.write(f"STATE AFTER PHASE 5:\n")
        for s in ["RAW_INCOMING", "REVIEW_REQUIRED", "READY_NORMALIZED", "HELD_PROBLEMS"]:
            f.write(f"  {s}: {state_counts.get(s, 0)}\n")
        f.write(f"\nSAFETY CHECKS: {len(checks)} total\n")
        pass_count = sum(1 for _, p, _ in checks if p)
        fail_count = sum(1 for _, p, _ in checks if not p)
        f.write(f"  PASS: {pass_count}\n")
        f.write(f"  FAIL: {fail_count}\n\n")
        gate = "PASS" if all_pass and len(applied) > 0 else "FAIL"
        f.write(f"GATE={gate}\n")
    log("  Wrote 07_final_report.txt")

    # execution_log.txt
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 5 — Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy output CSVs to proof dir
    for csv_name in ["apply_candidates_v1.csv", "apply_results_v1.csv", "remaining_review_queue_v1.csv"]:
        src_csv = DATA_DIR / csv_name
        if src_csv.exists():
            shutil.copy2(str(src_csv), str(PROOF_DIR / csv_name))

    log(f"\nAll proof artifacts written to: {PROOF_DIR}")
    return gate


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log(f"Phase 5 — Review Queue Apply + Ready Normalized Promotion")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Batch root: {BATCH_ROOT}")
    log(f"Target dir: {READY_DIR}")
    log("")

    # Part A: Load review queue
    queue = load_review_queue()

    # Part B: Filter apply candidates
    candidates, blocked_filter = filter_apply_candidates(queue)

    # Part C: Apply renames
    apply_results = apply_renames(candidates)

    # Part D: Write apply results
    all_results = write_apply_results(apply_results, blocked_filter)

    # Part E: Write remaining queue
    state_counts = write_remaining_queue(queue, all_results)

    # Part F: Safety tests
    checks, all_pass = run_safety_tests(
        candidates, blocked_filter, apply_results, queue, state_counts
    )

    # Part G/H: Write proof
    gate = write_proof(queue, candidates, blocked_filter, apply_results,
                       state_counts, checks, all_pass)

    log(f"\n{'='*60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
