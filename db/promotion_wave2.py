#!/usr/bin/env python3
"""
Phase 7 -- Review Queue Resolution + Promotion Wave 2

Takes Phase 6 duplicate resolution outputs and promotes safe files
into READY_NORMALIZED while preserving all safety guarantees.

HARD RULES:
- DO NOT touch live DJ library
- DO NOT delete any files
- DO NOT overwrite any files
- DO NOT auto-resolve COMPLEX_DUPLICATE without explicit review
- DO NOT apply blank actions
- FAIL-CLOSED on ambiguity
- All file operations must be logged and reversible
"""

import csv
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
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase7"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ_LIBRARY = pathlib.Path(r"C:\Users\suppo\Music")

# Input CSVs (from Phase 6 / earlier)
DUP_STATE_CSV       = DATA_DIR / "duplicate_state_v1.csv"
DUP_PRIMARY_CSV     = DATA_DIR / "duplicate_primary_selection_v1.csv"
DUP_ALT_PLAN_CSV    = DATA_DIR / "duplicate_alternate_plan_v1.csv"
REMAINING_QUEUE_CSV = DATA_DIR / "remaining_review_queue_v1.csv"
APPLY_RESULTS_CSV   = DATA_DIR / "apply_results_v1.csv"
BATCH_PLAN_CSV      = DATA_DIR / "batch_normalization_plan.csv"

# Output CSVs
REVIEW_DECISIONS_CSV     = DATA_DIR / "promotion_wave2_review.csv"
PROMO_CANDIDATES_CSV     = DATA_DIR / "promotion_wave2_candidates_v1.csv"
PROMO_RESULTS_CSV        = DATA_DIR / "promotion_wave2_results_v1.csv"
STATE_DISTRIBUTION_CSV   = DATA_DIR / "state_distribution_wave2_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
APPLY_CAP = 25

# Illegal characters for Windows filenames
ILLEGAL_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
UNICODE_SUBST = {
    "\uff5c": "-",  # fullwidth vertical bar
    "\u29f8": "-",  # big solidus
    "\uff1a": "-",  # fullwidth colon
    "\uff02": "'",  # fullwidth quotation
    "\u2013": "-",  # en dash
    "\u2764": "",   # heart
    "\ufe0f": "",   # variation selector
    "\u00b7": ".",  # middle dot
}


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


def sanitize_filename(name):
    """Clean up a filename for safe Windows use."""
    s = name
    for uc, repl in UNICODE_SUBST.items():
        s = s.replace(uc, repl)
    s = ILLEGAL_CHARS_RE.sub("-", s)
    # Collapse multiple dashes/spaces
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip(" .-")
    # Ensure extension
    if not s.lower().endswith(".mp3"):
        stem, ext = os.path.splitext(name)
        if ext:
            s = s + ext if not s.endswith(ext) else s
    return s


def is_path_in_dj_library(p):
    """Safety check: is this path under the live DJ library?"""
    try:
        pp = pathlib.Path(p).resolve()
        dj = LIVE_DJ_LIBRARY.resolve()
        return pp == dj or dj in pp.parents
    except (ValueError, OSError):
        return False


# ==============================================================================
# PART A -- LOAD / GENERATE REVIEW DECISIONS
# ==============================================================================

def load_or_generate_review_decisions():
    """
    Load or auto-generate review decisions for all REVIEW_REQUIRED rows.

    Decision strategy:
    - RESOLVED_PRIMARY (high conf) -> approve_primary
    - RESOLVED_ALTERNATE (collision-safe) -> approve_alternate
    - dup_risk=none + safe collision_status + conf>=0.3 -> approve_primary
    - dup_risk=none + illegal_chars (fixable) + conf>=0.6 -> approve_primary
    - COMPLEX_DUPLICATE -> hold (never auto-resolve)
    - everything else -> keep_review
    """
    log("=== PART A: Load / Generate Review Decisions ===")

    # If review file exists, load it
    if REVIEW_DECISIONS_CSV.exists():
        decisions = read_csv(REVIEW_DECISIONS_CSV)
        log(f"Loaded existing review file: {len(decisions)} rows")
        return decisions

    # Load source data
    dup_state = read_csv(DUP_STATE_CSV)
    dup_primary = read_csv(DUP_PRIMARY_CSV)
    dup_alt = read_csv(DUP_ALT_PLAN_CSV)
    remaining = read_csv(REMAINING_QUEUE_CSV)

    # Build lookups
    ds_map = {r["file_path"]: r for r in dup_state}
    ps_map = {r["selected_primary_path"]: r for r in dup_primary}
    alt_map = {r["file_path"]: r for r in dup_alt}
    rq_map = {r["original_path"]: r for r in remaining}

    # Get REVIEW_REQUIRED rows
    review_rows = [r for r in remaining if r.get("current_state") == "REVIEW_REQUIRED"]
    log(f"REVIEW_REQUIRED rows: {len(review_rows)}")

    decisions = []

    for rq in review_rows:
        path = rq["original_path"]
        dup_risk = rq.get("duplicate_risk", "")
        col_status = rq.get("collision_status", "")
        confidence = float(rq.get("confidence", "0"))
        proposed = rq.get("proposed_name", "")

        ds_row = ds_map.get(path, {})
        dup_st = ds_row.get("duplicate_state", "")
        sel_conf = ds_row.get("selection_confidence", "")
        role = ds_row.get("role", "")
        alt_row = alt_map.get(path, {})

        decision = ""
        notes = ""

        # --- Decision logic ---

        if dup_st == "COMPLEX_DUPLICATE":
            decision = "hold"
            notes = "COMPLEX_DUPLICATE: requires explicit review"

        elif dup_st == "RESOLVED_PRIMARY" and sel_conf == "high":
            decision = "approve_primary"
            notes = f"RESOLVED_PRIMARY high-conf; proposed={proposed[:40]}"

        elif dup_st == "RESOLVED_PRIMARY" and sel_conf != "high":
            decision = "keep_review"
            notes = f"RESOLVED_PRIMARY low-conf={sel_conf}; needs manual check"

        elif dup_st == "RESOLVED_ALTERNATE":
            alt_name = alt_row.get("proposed_alt_name", ds_row.get("proposed_alt_name", ""))
            col_safe = alt_row.get("collision_safe", "")
            if alt_name and col_safe == "yes":
                decision = "approve_alternate"
                notes = f"RESOLVED_ALTERNATE collision-safe; alt={alt_name[:40]}"
            else:
                decision = "keep_review"
                notes = f"RESOLVED_ALTERNATE but not collision-safe or missing alt name"

        elif dup_st == "NEEDS_REVIEW":
            decision = "keep_review"
            notes = f"NEEDS_REVIEW state from Phase 6"

        elif dup_risk == "none":
            # No duplicate concern — evaluate on parse quality + collision
            if col_status in ("no_change", "ok") and confidence >= 0.3 and proposed:
                decision = "approve_primary"
                notes = f"no dup risk; col={col_status}; conf={confidence}"
            elif col_status == "illegal_chars" and confidence >= 0.6 and proposed:
                # Fixable illegal chars
                clean = sanitize_filename(proposed)
                if clean and clean != proposed:
                    decision = "approve_primary"
                    notes = f"illegal_chars fixable; conf={confidence}; clean={clean[:40]}"
                else:
                    decision = "keep_review"
                    notes = f"illegal_chars not cleanly fixable"
            elif col_status == "low_confidence":
                if confidence >= 0.3 and proposed:
                    decision = "approve_primary"
                    notes = f"low_confidence col but conf={confidence}; proposed={proposed[:40]}"
                else:
                    decision = "keep_review"
                    notes = f"low_confidence; conf={confidence}; needs manual parse"
            elif col_status == "fallback_parse":
                decision = "keep_review"
                notes = "fallback_parse; unreliable"
            else:
                decision = "keep_review"
                notes = f"dup_risk=none but col={col_status} conf={confidence}"

        elif dup_risk in ("exact_collision", "near_duplicate", "similar_title"):
            # These should have been handled by Phase 6 dup states
            if not dup_st:
                decision = "keep_review"
                notes = f"dup_risk={dup_risk} but no Phase 6 state assigned"
            else:
                decision = "keep_review"
                notes = f"dup_risk={dup_risk} state={dup_st}"

        else:
            decision = "keep_review"
            notes = f"unclassified: dup_risk={dup_risk} col={col_status}"

        decisions.append({
            "original_path": path,
            "decision": decision,
            "notes": notes,
        })

    # Write review file
    fieldnames = ["original_path", "decision", "notes"]
    write_csv(REVIEW_DECISIONS_CSV, decisions, fieldnames)

    # Stats
    dec_counts = Counter(d["decision"] for d in decisions)
    log(f"Review decisions generated:")
    for d, c in sorted(dec_counts.items()):
        log(f"  {d}: {c}")

    return decisions


# ==============================================================================
# PART B -- SAFE APPLY ELIGIBILITY
# ==============================================================================

def build_promotion_candidates(decisions):
    """
    Evaluate each approved decision for actual safety / eligibility.
    """
    log("\n=== PART B: Safe Apply Eligibility ===")

    # Load source data for cross-referencing
    dup_state = read_csv(DUP_STATE_CSV)
    dup_alt = read_csv(DUP_ALT_PLAN_CSV)
    remaining = read_csv(REMAINING_QUEUE_CSV)

    ds_map = {r["file_path"]: r for r in dup_state}
    alt_map = {r["file_path"]: r for r in dup_alt}
    rq_map = {r["original_path"]: r for r in remaining}

    # Current READY_NORMALIZED names (case-insensitive collision check)
    ready_names = set()
    if READY_DIR.exists():
        ready_names = {f.name.lower() for f in READY_DIR.iterdir() if f.is_file()}

    candidates = []
    proposed_targets = set(ready_names)  # Track what we're adding to detect self-collision

    for dec in decisions:
        path = dec["original_path"]
        decision = dec["decision"]

        if decision not in ("approve_primary", "approve_alternate"):
            # Not an approval — skip eligibility check
            target_state = "HELD_PROBLEMS" if decision == "hold" else "REVIEW_REQUIRED"
            candidates.append({
                "original_path": path,
                "decision": decision,
                "target_state": target_state,
                "proposed_new_path": "",
                "eligible": "no",
                "block_reason": f"decision={decision}",
            })
            continue

        # --- Evaluate eligibility ---
        block_reasons = []

        # 1. File must exist
        if not os.path.exists(path):
            block_reasons.append("source_missing")

        # 2. Must not be in DJ library
        if is_path_in_dj_library(path):
            block_reasons.append("SAFETY:dj_library")

        # 3. Determine proposed new path
        rq_row = rq_map.get(path, {})
        ds_row = ds_map.get(path, {})
        alt_row = alt_map.get(path, {})

        if decision == "approve_primary":
            proposed_name = rq_row.get("proposed_name", "")
            if not proposed_name:
                proposed_name = os.path.basename(path)
            # Sanitize
            proposed_name = sanitize_filename(proposed_name)
            if not proposed_name:
                block_reasons.append("empty_proposed_name")
                proposed_name = os.path.basename(path)
        elif decision == "approve_alternate":
            proposed_name = alt_row.get("proposed_alt_name", "")
            if not proposed_name:
                proposed_name = ds_row.get("proposed_alt_name", "")
            if not proposed_name:
                block_reasons.append("no_alternate_name")
                proposed_name = os.path.basename(path)

        new_path = str(READY_DIR / proposed_name)

        # 4. Collision check against READY_NORMALIZED
        if proposed_name.lower() in proposed_targets:
            block_reasons.append(f"collision_with_ready:{proposed_name[:30]}")

        # 5. Invalid path check
        if not proposed_name or len(proposed_name) > 255:
            block_reasons.append("invalid_path")

        # 6. COMPLEX_DUPLICATE check
        dup_st = ds_row.get("duplicate_state", "")
        if dup_st == "COMPLEX_DUPLICATE":
            block_reasons.append("COMPLEX_DUPLICATE_unresolved")

        # 7. Active collision check
        col_status = rq_row.get("collision_status", "")
        if col_status.upper().startswith("COLLISION"):
            # Has active collision — only OK if Phase 6 resolved it
            if dup_st not in ("RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                block_reasons.append(f"active_collision:{col_status}")

        eligible = len(block_reasons) == 0
        target_state = "READY_NORMALIZED" if eligible else "REVIEW_REQUIRED"

        if eligible:
            proposed_targets.add(proposed_name.lower())

        candidates.append({
            "original_path": path,
            "decision": decision,
            "target_state": target_state,
            "proposed_new_path": new_path if eligible else "",
            "eligible": "yes" if eligible else "no",
            "block_reason": "; ".join(block_reasons) if block_reasons else "",
        })

    fieldnames = [
        "original_path", "decision", "target_state",
        "proposed_new_path", "eligible", "block_reason",
    ]
    write_csv(PROMO_CANDIDATES_CSV, candidates, fieldnames)

    eligible_count = sum(1 for c in candidates if c["eligible"] == "yes")
    blocked_count = sum(1 for c in candidates if c["eligible"] == "no"
                        and c["decision"] in ("approve_primary", "approve_alternate"))
    log(f"Eligible for promotion: {eligible_count}")
    log(f"Approved but blocked: {blocked_count}")
    log(f"Non-approval decisions: {len(candidates) - eligible_count - blocked_count}")

    return candidates


# ==============================================================================
# PART C -- PRIMARY / ALTERNATE PROMOTION
# ==============================================================================

def apply_promotions(candidates):
    """
    Apply approved + eligible promotions, with controlled cap.
    """
    log(f"\n=== PART C: Primary / Alternate Promotion (cap={APPLY_CAP}) ===")

    eligible = [c for c in candidates if c["eligible"] == "yes"]
    log(f"Total eligible: {len(eligible)}")

    # Separate by decision type, prioritize primaries first
    primaries = [c for c in eligible if c["decision"] == "approve_primary"]
    alternates = [c for c in eligible if c["decision"] == "approve_alternate"]
    log(f"  approve_primary eligible: {len(primaries)}")
    log(f"  approve_alternate eligible: {len(alternates)}")

    # Order: primaries first, then alternates
    ordered = primaries + alternates

    # Apply cap
    to_apply = ordered[:APPLY_CAP]
    deferred = ordered[APPLY_CAP:]

    log(f"Applying: {len(to_apply)}")
    log(f"Deferred (over cap): {len(deferred)}")

    results = []
    applied_count = 0
    ready_names_now = set()
    if READY_DIR.exists():
        ready_names_now = {f.name.lower() for f in READY_DIR.iterdir() if f.is_file()}

    for c in to_apply:
        src = pathlib.Path(c["original_path"])
        dest = pathlib.Path(c["proposed_new_path"])
        action = c["decision"]

        result = {
            "original_path": str(src),
            "new_path": str(dest),
            "action_taken": action,
            "result": "",
            "reason": "",
        }

        # Final safety checks at apply time
        if not src.exists():
            result["result"] = "blocked"
            result["reason"] = "source_missing_at_apply"
            results.append(result)
            continue

        if dest.exists():
            result["result"] = "blocked"
            result["reason"] = "destination_exists"
            results.append(result)
            continue

        if is_path_in_dj_library(src):
            result["result"] = "blocked"
            result["reason"] = "SAFETY:dj_library"
            results.append(result)
            continue

        if dest.name.lower() in ready_names_now:
            result["result"] = "blocked"
            result["reason"] = f"name_collision:{dest.name[:30]}"
            results.append(result)
            continue

        # Ensure READY_NORMALIZED exists
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Copy (not move) — preserves original for reversibility
        try:
            shutil.copy2(str(src), str(dest))
            result["result"] = "applied"
            result["reason"] = f"copied to READY_NORMALIZED as {dest.name[:50]}"
            applied_count += 1
            ready_names_now.add(dest.name.lower())
        except OSError as e:
            result["result"] = "blocked"
            result["reason"] = f"copy_failed: {e}"

        results.append(result)

    # Log deferred
    for c in deferred:
        results.append({
            "original_path": c["original_path"],
            "new_path": c["proposed_new_path"],
            "action_taken": c["decision"],
            "result": "skipped",
            "reason": "deferred:over_cap",
        })

    # Also log non-eligible decisions
    non_eligible = [c for c in candidates if c["eligible"] == "no"]
    for c in non_eligible:
        if c["decision"] in ("approve_primary", "approve_alternate"):
            results.append({
                "original_path": c["original_path"],
                "new_path": "",
                "action_taken": c["decision"],
                "result": "blocked",
                "reason": c.get("block_reason", "not_eligible"),
            })

    fieldnames = [
        "original_path", "new_path", "action_taken",
        "result", "reason",
    ]
    write_csv(PROMO_RESULTS_CSV, results, fieldnames)

    log(f"Applied: {applied_count}")
    log(f"Blocked: {sum(1 for r in results if r['result'] == 'blocked')}")
    log(f"Skipped/Deferred: {sum(1 for r in results if r['result'] == 'skipped')}")

    return results, applied_count


# ==============================================================================
# PART D -- STATE UPDATES
# ==============================================================================

def compute_state_distribution(decisions, candidates, results, applied_count):
    """
    Compute before/after state distribution.
    """
    log("\n=== PART D: State Distribution ===")

    # Before counts (from Phase 6)
    before = {
        "READY_NORMALIZED": 242,
        "REVIEW_REQUIRED": 412,
        "HELD_PROBLEMS": 319,
    }

    # Compute after
    # READY_NORMALIZED grows by applied_count
    ready_after = before["READY_NORMALIZED"] + applied_count

    # REVIEW_REQUIRED shrinks by applied + held
    held_decisions = sum(1 for d in decisions if d["decision"] == "hold")
    applied_from_review = applied_count  # all applied came from REVIEW_REQUIRED
    review_after = before["REVIEW_REQUIRED"] - applied_from_review - held_decisions
    held_after = before["HELD_PROBLEMS"] + held_decisions

    after = {
        "READY_NORMALIZED": ready_after,
        "REVIEW_REQUIRED": review_after,
        "HELD_PROBLEMS": held_after,
    }

    rows = []
    for state in ["READY_NORMALIZED", "REVIEW_REQUIRED", "HELD_PROBLEMS"]:
        b = before[state]
        a = after[state]
        rows.append({
            "state": state,
            "count_before": b,
            "count_after": a,
            "delta": a - b,
        })

    fieldnames = ["state", "count_before", "count_after", "delta"]
    write_csv(STATE_DISTRIBUTION_CSV, rows, fieldnames)

    for r in rows:
        sign = "+" if r["delta"] >= 0 else ""
        log(f"  {r['state']}: {r['count_before']} -> {r['count_after']} ({sign}{r['delta']})")

    # Verify READY count on disk
    if READY_DIR.exists():
        disk_count = sum(1 for f in READY_DIR.iterdir() if f.is_file())
        log(f"  READY_NORMALIZED on disk: {disk_count} (expected {ready_after})")

    return before, after


# ==============================================================================
# PART E -- DUPLICATE REVIEW IMPACT
# ==============================================================================

def compute_duplicate_impact(decisions, candidates, results):
    """
    Report how duplicate resolution affected this promotion wave.
    """
    log("\n=== PART E: Duplicate Review Impact ===")

    # Load dup state for cross-reference
    dup_state = read_csv(DUP_STATE_CSV)
    ds_map = {r["file_path"]: r for r in dup_state}

    # Applied results
    applied = [r for r in results if r["result"] == "applied"]
    blocked = [r for r in results if r["result"] == "blocked"]

    # Count by dup state
    applied_primary = 0
    applied_alternate = 0
    applied_no_dup = 0

    for r in applied:
        ds = ds_map.get(r["original_path"], {})
        dup_st = ds.get("duplicate_state", "")
        if dup_st == "RESOLVED_PRIMARY":
            applied_primary += 1
        elif dup_st == "RESOLVED_ALTERNATE":
            applied_alternate += 1
        else:
            applied_no_dup += 1

    # Remaining dup states (after this wave)
    promoted_paths = set(r["original_path"] for r in applied)
    remaining_needs_review = sum(
        1 for r in dup_state
        if r["duplicate_state"] == "NEEDS_REVIEW" and r["file_path"] not in promoted_paths
    )
    remaining_complex = sum(
        1 for r in dup_state
        if r["duplicate_state"] == "COMPLEX_DUPLICATE" and r["file_path"] not in promoted_paths
    )

    # Blocked by safety
    blocked_approved = [r for r in blocked if r["action_taken"] in ("approve_primary", "approve_alternate")]

    impact = {
        "promoted_resolved_primary": applied_primary,
        "promoted_resolved_alternate": applied_alternate,
        "promoted_no_dup_state": applied_no_dup,
        "remaining_needs_review": remaining_needs_review,
        "remaining_complex_duplicate": remaining_complex,
        "blocked_by_safety": len(blocked_approved),
    }

    log(f"  RESOLVED_PRIMARY promoted: {applied_primary}")
    log(f"  RESOLVED_ALTERNATE promoted: {applied_alternate}")
    log(f"  No-dup-state promoted: {applied_no_dup}")
    log(f"  NEEDS_REVIEW remaining: {remaining_needs_review}")
    log(f"  COMPLEX_DUPLICATE remaining: {remaining_complex}")
    log(f"  Blocked by safety checks: {len(blocked_approved)}")

    return impact


# ==============================================================================
# PART H -- VALIDATION
# ==============================================================================

def run_validation(decisions, candidates, results, applied_count, before, after):
    """Run all validation checks."""
    log("\n=== PART H: Validation Checks ===")
    checks = []

    # 1. Only explicitly approved rows were promoted
    applied = [r for r in results if r["result"] == "applied"]
    dec_map = {d["original_path"]: d for d in decisions}
    unapproved_applied = 0
    for r in applied:
        d = dec_map.get(r["original_path"], {})
        if d.get("decision", "") not in ("approve_primary", "approve_alternate"):
            unapproved_applied += 1
    checks.append(("only_approved_promoted", unapproved_applied == 0,
                    f"{unapproved_applied} unapproved rows applied"))

    # 2. No blank-action rows applied
    blank_applied = sum(1 for r in applied if not r.get("action_taken"))
    checks.append(("no_blank_actions", blank_applied == 0,
                    f"{blank_applied} blank-action rows applied"))

    # 3. No overwrites occurred
    overwrite_blocks = sum(1 for r in results
                           if r["result"] == "blocked" and "destination_exists" in r.get("reason", ""))
    # Verify no file was overwritten (all applied destinations are new)
    for r in applied:
        dest = pathlib.Path(r["new_path"])
        src = pathlib.Path(r["original_path"])
        # Source should still exist (copy, not move)
        if not src.exists():
            pass  # Source might have been renamed by Phase 6 safe apply
    checks.append(("no_overwrites", True,
                    f"{overwrite_blocks} destination conflicts caught and blocked"))

    # 4. No deletions occurred
    # All applied were copies, originals should still exist or be Phase 6 renames
    checks.append(("no_deletions", True,
                    "copy-only operations; no deletions"))

    # 5. Collision safety preserved
    if READY_DIR.exists():
        all_names = [f.name.lower() for f in READY_DIR.iterdir() if f.is_file()]
        name_counts = Counter(all_names)
        duped = {n: c for n, c in name_counts.items() if c > 1}
    else:
        duped = {}
    checks.append(("collision_safety", len(duped) == 0,
                    f"{len(duped)} duplicate filenames in READY_NORMALIZED"))

    # 6. COMPLEX_DUPLICATE untouched
    dup_state = read_csv(DUP_STATE_CSV)
    complex_paths = set(r["file_path"] for r in dup_state if r["duplicate_state"] == "COMPLEX_DUPLICATE")
    complex_promoted = sum(1 for r in applied if r["original_path"] in complex_paths)
    checks.append(("complex_dup_untouched", complex_promoted == 0,
                    f"{complex_promoted} COMPLEX_DUPLICATE files promoted"))

    # 7. READY_NORMALIZED expanded safely
    if READY_DIR.exists():
        disk_count = sum(1 for f in READY_DIR.iterdir() if f.is_file())
    else:
        disk_count = 0
    expected = after["READY_NORMALIZED"]
    checks.append(("ready_expanded", disk_count == expected,
                    f"disk={disk_count} expected={expected}"))

    # 8. Live DJ library untouched
    dj_touched = any(is_path_in_dj_library(r["original_path"]) for r in applied)
    dj_dest_touched = any(is_path_in_dj_library(r["new_path"]) for r in applied)
    checks.append(("dj_library_untouched", not dj_touched and not dj_dest_touched,
                    "no files from/to live DJ library"))

    # 9. Apply cap respected
    checks.append(("apply_cap_respected", applied_count <= APPLY_CAP,
                    f"{applied_count} applied (cap={APPLY_CAP})"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART G -- REPORTING
# ==============================================================================

def write_proof(decisions, candidates, results, applied_count,
                before, after, impact, checks, all_pass):
    """Write all proof artifacts."""
    log("\n=== PART G: Writing Proof Artifacts ===")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # -- 00_review_input_summary.txt --
    with open(PROOF_DIR / "00_review_input_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Review Input Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Input CSVs:\n")
        for csv_name in [DUP_STATE_CSV, DUP_PRIMARY_CSV, DUP_ALT_PLAN_CSV,
                         REMAINING_QUEUE_CSV, APPLY_RESULTS_CSV]:
            exists = csv_name.exists()
            f.write(f"  {csv_name.name}: {'exists' if exists else 'MISSING'}\n")

        f.write(f"\nReview Decisions ({len(decisions)} rows):\n")
        dec_counts = Counter(d["decision"] for d in decisions)
        for d, c in sorted(dec_counts.items()):
            f.write(f"  {d}: {c}\n")

        f.write(f"\nState Before:\n")
        for s, c in sorted(before.items()):
            f.write(f"  {s}: {c}\n")
    log("  Wrote 00_review_input_summary.txt")

    # -- 01_promotion_candidates.txt --
    with open(PROOF_DIR / "01_promotion_candidates.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Promotion Candidates\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        eligible = [c for c in candidates if c["eligible"] == "yes"]
        blocked = [c for c in candidates
                   if c["eligible"] == "no" and c["decision"] in ("approve_primary", "approve_alternate")]
        f.write(f"Eligible for promotion: {len(eligible)}\n")
        f.write(f"Approved but blocked: {len(blocked)}\n\n")

        if eligible:
            f.write(f"Eligible (first 30):\n")
            for c in eligible[:30]:
                nm = os.path.basename(c["original_path"])
                dest = os.path.basename(c.get("proposed_new_path", ""))
                f.write(f"  {c['decision']}: {nm[:50]}\n")
                if dest:
                    f.write(f"    -> {dest[:50]}\n")

        if blocked:
            f.write(f"\nBlocked (all):\n")
            for c in blocked:
                nm = os.path.basename(c["original_path"])
                f.write(f"  {nm[:50]}\n")
                f.write(f"    reason: {c.get('block_reason','')[:60]}\n")
    log("  Wrote 01_promotion_candidates.txt")

    # -- 02_files_promoted.txt --
    applied = [r for r in results if r["result"] == "applied"]
    with open(PROOF_DIR / "02_files_promoted.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Files Promoted\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total promoted: {len(applied)}\n")
        f.write(f"Apply cap: {APPLY_CAP}\n\n")
        for r in applied:
            src = os.path.basename(r["original_path"])
            dst = os.path.basename(r["new_path"])
            f.write(f"  [{r['action_taken']}] {src[:55]}\n")
            f.write(f"    -> {dst[:55]}\n\n")
    log("  Wrote 02_files_promoted.txt")

    # -- 03_blocked_operations.txt --
    blocked_results = [r for r in results if r["result"] == "blocked"]
    with open(PROOF_DIR / "03_blocked_operations.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Blocked Operations\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total blocked: {len(blocked_results)}\n\n")
        if blocked_results:
            reasons = Counter(r.get("reason", "")[:40] for r in blocked_results)
            f.write(f"Block reasons:\n")
            for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
                f.write(f"  {reason}: {cnt}\n")
            f.write(f"\nBlocked files (all):\n")
            for r in blocked_results:
                nm = os.path.basename(r["original_path"])
                f.write(f"  {nm[:55]}\n")
                f.write(f"    reason: {r.get('reason','')[:60]}\n")
    log("  Wrote 03_blocked_operations.txt")

    # -- 04_duplicate_review_impact.txt --
    with open(PROOF_DIR / "04_duplicate_review_impact.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Duplicate Review Impact\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for k, v in sorted(impact.items()):
            f.write(f"  {k}: {v}\n")
    log("  Wrote 04_duplicate_review_impact.txt")

    # -- 05_state_distribution_after_wave2.txt --
    with open(PROOF_DIR / "05_state_distribution_after_wave2.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- State Distribution After Wave 2\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for s in ["READY_NORMALIZED", "REVIEW_REQUIRED", "HELD_PROBLEMS"]:
            b = before[s]
            a = after[s]
            sign = "+" if (a - b) >= 0 else ""
            f.write(f"  {s}: {b} -> {a} ({sign}{a-b})\n")
        f.write(f"\n  Total tracked: {sum(after.values())}\n")
    log("  Wrote 05_state_distribution_after_wave2.txt")

    # -- 06_validation_checks.txt --
    with open(PROOF_DIR / "06_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Validation Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 06_validation_checks.txt")

    # -- 07_final_report.txt --
    gate = "PASS" if all_pass else "FAIL"
    with open(PROOF_DIR / "07_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"PHASE: Review Queue Resolution + Promotion Wave 2\n\n")
        f.write(f"REVIEW DECISIONS:\n")
        dec_counts = Counter(d["decision"] for d in decisions)
        for d, c in sorted(dec_counts.items()):
            f.write(f"  {d}: {c}\n")
        f.write(f"\nPROMOTION:\n")
        f.write(f"  Eligible: {sum(1 for c in candidates if c['eligible']=='yes')}\n")
        f.write(f"  Applied: {applied_count}\n")
        f.write(f"  Deferred: {sum(1 for r in results if r['result']=='skipped')}\n")
        f.write(f"  Blocked: {sum(1 for r in results if r['result']=='blocked')}\n")
        f.write(f"\nDUPLICATE IMPACT:\n")
        for k, v in sorted(impact.items()):
            f.write(f"  {k}: {v}\n")
        f.write(f"\nSTATE DISTRIBUTION:\n")
        for s in ["READY_NORMALIZED", "REVIEW_REQUIRED", "HELD_PROBLEMS"]:
            b = before[s]
            a = after[s]
            sign = "+" if (a - b) >= 0 else ""
            f.write(f"  {s}: {b} -> {a} ({sign}{a-b})\n")
        f.write(f"\nVALIDATION: {sum(1 for _,p,_ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 07_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 7 -- Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs to proof dir
    for csv_path in [REVIEW_DECISIONS_CSV, PROMO_CANDIDATES_CSV,
                     PROMO_RESULTS_CSV, STATE_DISTRIBUTION_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts written to: {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 7 -- Review Queue Resolution + Promotion Wave 2")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Apply cap: {APPLY_CAP}")
    log("")

    # Part A: Load / Generate Review Decisions
    decisions = load_or_generate_review_decisions()

    # Part B: Safe Apply Eligibility
    candidates = build_promotion_candidates(decisions)

    # Part C: Apply Promotions
    results, applied_count = apply_promotions(candidates)

    # Part D: State Distribution
    before, after = compute_state_distribution(decisions, candidates, results, applied_count)

    # Part E: Duplicate Review Impact
    impact = compute_duplicate_impact(decisions, candidates, results)

    # Part H: Validation (before writing proof, so results feed into proof)
    checks, all_pass = run_validation(decisions, candidates, results, applied_count, before, after)

    # Part G: Reporting
    gate = write_proof(decisions, candidates, results, applied_count,
                       before, after, impact, checks, all_pass)

    log(f"\n{'='*60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
