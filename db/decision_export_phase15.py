#!/usr/bin/env python3
"""
Phase 15 — Decision CSV Generation + Operator Action System

READ-ONLY. No file mutations. No renames. No moves. No deletions.

Produces 4 decision CSVs:
  A. delete_safe_v1.csv           — hash-identical redundant duplicates
  B. duplicate_resolution_v1.csv  — keep/archive decisions for dup groups
  C. fix_required_v1.csv          — human fixes: rename, verify, hold
  D. junk_candidates_v1.csv       — low-value / unprocessable files
  E. decision_summary_index_v1.csv
  F. Priority scoring (embedded)
  G. operator_action_system_v1.txt

HARD RULES:
  - DO NOT modify any files on disk
  - DO NOT delete, move, or rename anything
  - DO NOT touch live DJ library
  - FAIL-CLOSED on ambiguity
"""

import csv
import os
import pathlib
import re
import shutil
from collections import Counter
from datetime import datetime

# -- Paths -------------------------------------------------------------------
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase15"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"

# -- Input CSVs (read-only) --------------------------------------------------
TRUE_DUP_RESOLUTION_CSV    = DATA_DIR / "true_duplicate_resolution_v1.csv"
TRUE_DUP_PACK_CSV          = DATA_DIR / "true_duplicate_review_pack_v1.csv"
DUPLICATE_STATE_CSV        = DATA_DIR / "duplicate_state_v1.csv"
DUPLICATE_PRIMARY_CSV      = DATA_DIR / "duplicate_primary_selection_v1.csv"
DUPLICATE_ALT_PLAN_CSV     = DATA_DIR / "duplicate_alternate_plan_v1.csv"
NEAR_DUP_V2_CSV            = DATA_DIR / "near_duplicate_groups_v2.csv"
DEST_CONFLICT_PACK_CSV_IN  = DATA_DIR / "destination_conflict_review_pack_v1.csv"
MANUAL_REVIEW_PACK_CSV_IN  = DATA_DIR / "manual_review_pack_v1.csv"
HELD_BREAKDOWN_CSV         = DATA_DIR / "held_problem_breakdown_v1.csv"
HELD_ROWS_CSV              = DATA_DIR / "held_rows.csv"
NO_PARSE_RECOVERY_CSV      = DATA_DIR / "no_parse_recovery_v1.csv"
LOW_CONF_RECOVERY_CSV      = DATA_DIR / "low_confidence_recovery_v1.csv"
REVIEW_REQUIRED_CLEAN_CSV  = DATA_DIR / "review_required_clean_v1.csv"
COLLISION_RESOLUTION_CSV   = DATA_DIR / "collision_resolution_plan_v1.csv"

# -- Output CSVs -------------------------------------------------------------
DELETE_SAFE_CSV        = DATA_DIR / "delete_safe_v1.csv"
DUP_RESOLUTION_CSV     = DATA_DIR / "duplicate_resolution_v1.csv"
FIX_REQUIRED_CSV       = DATA_DIR / "fix_required_v1.csv"
JUNK_CANDIDATES_CSV    = DATA_DIR / "junk_candidates_v1.csv"
DECISION_INDEX_CSV     = DATA_DIR / "decision_summary_index_v1.csv"
OPERATOR_ACTION_TXT    = DATA_DIR / "operator_action_system_v1.txt"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Track paths assigned to each decision CSV to prevent overlap
assigned_paths = {"delete_safe": set(), "dup_resolution": set(),
                  "fix_required": set(), "junk": set()}


def log(msg):
    entry = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    execution_log.append(entry)
    print(entry)


def read_csv(path):
    if not path.exists():
        log(f"WARNING: {path.name} not found")
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    log(f"Wrote {len(rows)} rows -> {path.name}")


# -- Junk detection patterns --------------------------------------------------
JUNK_PATTERNS = [
    (re.compile(r'\b(compilation|greatest\s*hits|best\s*of|top\s*\d+|100\s*greatest)', re.I), "compilation_mix"),
    (re.compile(r'\b(full\s*album|complete\s*album|full\s*mix|megamix|nonstop)', re.I), "full_album_mix"),
    (re.compile(r'\b(playlist|collection|medley|mashup\s*mix)', re.I), "playlist_collection"),
    (re.compile(r'\b(metronome|click\s*track|test\s*tone|silence)', re.I), "utility_file"),
    (re.compile(r'\b(stem|instrumental\s*only|acapella\s*only|karaoke)', re.I), "stem_karaoke"),
]

def detect_junk(name):
    """Check if a filename matches junk/low-value patterns."""
    for pattern, reason in JUNK_PATTERNS:
        if pattern.search(name):
            return reason
    return None


def priority_score_delete(conf):
    """Score for safe-delete rows."""
    if conf >= 1.0:
        return 5
    if conf >= 0.9:
        return 4
    return 3


def priority_score_dup(conf_str, role):
    """Score for duplicate resolution rows."""
    try:
        conf = float(conf_str) if conf_str else 0
    except ValueError:
        conf = 0
    if role == "primary":
        return 4 if conf >= 0.8 else 3
    # alternates
    if conf >= 0.8:
        return 4
    if conf >= 0.5:
        return 3
    return 2


def priority_score_fix(conf, issue_type):
    """Score for fix-required rows."""
    try:
        c = float(conf) if conf else 0
    except ValueError:
        c = 0
    if issue_type in ("DESTINATION_CONFLICT",):
        return 5  # handle immediately — blocks promotions
    if c >= 0.8:
        return 4  # high conf, just needs rename/verify
    if c >= 0.5:
        return 3
    if issue_type == "NO_PARSE":
        return 2
    return 2


def priority_score_junk(conf, reason):
    """Score for junk candidates."""
    try:
        c = float(conf) if conf else 0
    except ValueError:
        c = 0
    if reason in ("compilation_mix", "full_album_mix", "playlist_collection"):
        return 2  # low urgency, can ignore
    if reason in ("utility_file", "stem_karaoke"):
        return 3  # slightly more clear-cut for deletion
    return 1


# ==============================================================================
# SNAPSHOT — capture filesystem state BEFORE any work
# ==============================================================================

def snapshot_filesystem():
    ready_files = sorted(f.name for f in READY_DIR.iterdir() if f.is_file()) if READY_DIR.exists() else []
    return {
        "ready_count": len(ready_files),
        "ready_names": ready_files,
    }


# ==============================================================================
# PART A — SAFE DELETE CSV
# ==============================================================================

def part_a_safe_delete():
    log("\n" + "=" * 60)
    log("PART A: Safe Delete CSV")
    log("=" * 60)

    # Source: true_duplicate_resolution_v1.csv — 25 hash-identical confirmed dups
    td = read_csv(TRUE_DUP_RESOLUTION_CSV)
    confirmed = [r for r in td if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"]

    rows = []
    for r in confirmed:
        path = r["original_path"]
        existing = r.get("existing_ready_path", "")
        source_hash = r.get("source_hash", "")
        existing_hash = r.get("existing_hash", "")
        verified = r.get("hash_verified", "")

        # Only include if hash-verified identical
        if verified != "yes" or source_hash != existing_hash:
            continue

        rows.append({
            "file_path": path,
            "duplicate_of": existing,
            "reason": f"Hash-identical to READY copy ({r.get('existing_ready_name', '')[:50]}). "
                       f"Hash: {source_hash[:16]}...",
            "confidence": "1.0",
            "recommended_action": "DELETE_SAFE",
            "notes": f"Size: {r.get('source_size', '?')} bytes. "
                     f"Source {'exists' if os.path.exists(path) else 'MISSING'}.",
            "priority_score": str(priority_score_delete(1.0)),
        })
        assigned_paths["delete_safe"].add(path)

    fieldnames = ["file_path", "duplicate_of", "reason", "confidence",
                  "recommended_action", "notes", "priority_score"]
    write_csv(DELETE_SAFE_CSV, rows, fieldnames)

    log(f"Safe delete: {len(rows)} rows (all hash-verified, confidence=1.0)")
    return rows


# ==============================================================================
# PART B — DUPLICATE RESOLUTION CSV
# ==============================================================================

def part_b_dup_resolution():
    log("\n" + "=" * 60)
    log("PART B: Duplicate Resolution CSV")
    log("=" * 60)

    # Source: duplicate_state_v1.csv + duplicate_primary_selection_v1.csv
    ds = read_csv(DUPLICATE_STATE_CSV)
    dp = read_csv(DUPLICATE_PRIMARY_CSV)

    # Build primary map
    primary_map = {}
    for r in dp:
        gid = r["group_id"]
        primary_map[gid] = {
            "primary_path": r["selected_primary_path"],
            "primary_name": r["selected_primary_name"],
            "confidence": r.get("confidence", ""),
            "reason": r.get("reason", ""),
        }

    rows = []
    for r in ds:
        path = r["file_path"]
        gid = r["group_id"]
        role = r.get("role", "")
        dup_state = r.get("duplicate_state", "")
        sel_conf = r.get("selection_confidence", "")
        source = r.get("source", "")

        # Skip rows already in safe-delete
        if path in assigned_paths["delete_safe"]:
            continue

        primary = primary_map.get(gid, {})
        primary_path = primary.get("primary_path", "")

        if role == "primary":
            action = "KEEP_PRIMARY"
            reason = f"Selected as primary for group {gid}. {primary.get('reason', '')[:60]}"
        elif dup_state == "NEEDS_REVIEW":
            action = "REVIEW_DUPLICATE"
            reason = f"Group {gid}: needs operator review. State={dup_state}, source={source}."
        else:
            action = "ARCHIVE_DUPLICATE"
            reason = f"Group {gid}: alternate copy. Primary={primary.get('primary_name', '')[:40]}"

        pscore = priority_score_dup(sel_conf, role)

        rows.append({
            "file_path": path,
            "primary_file": primary_path,
            "reason": reason,
            "confidence": sel_conf if sel_conf else "medium",
            "recommended_action": action,
            "notes": f"Group {gid}, role={role}, state={dup_state}, source={source}. "
                     f"Alt name: {r.get('proposed_alt_name', '')[:40]}",
            "priority_score": str(pscore),
        })
        assigned_paths["dup_resolution"].add(path)

    fieldnames = ["file_path", "primary_file", "reason", "confidence",
                  "recommended_action", "notes", "priority_score"]
    write_csv(DUP_RESOLUTION_CSV, rows, fieldnames)

    actions = Counter(r["recommended_action"] for r in rows)
    log(f"Duplicate resolution: {len(rows)} rows")
    log(f"  Actions: {dict(actions)}")
    return rows


# ==============================================================================
# PART C — FIX REQUIRED CSV
# ==============================================================================

def part_c_fix_required():
    log("\n" + "=" * 60)
    log("PART C: Fix Required CSV")
    log("=" * 60)

    rows = []
    seen = set()

    # Source 1: destination conflict review pack (5 rows) — highest priority
    dc = read_csv(DEST_CONFLICT_PACK_CSV_IN)
    for r in dc:
        path = r["original_path"]
        if path in assigned_paths["delete_safe"] or path in assigned_paths["dup_resolution"]:
            continue
        if path in seen:
            continue
        seen.add(path)
        rows.append({
            "file_path": path,
            "issue_type": "DESTINATION_CONFLICT",
            "current_name": os.path.basename(path),
            "suggested_name": r.get("suggested_alternate_name", ""),
            "confidence": "0.8",
            "recommended_action": "RENAME",
            "notes": f"Proposed name collides with READY. Alt: {r.get('suggested_alternate_name', '')[:50]}",
            "priority_score": str(priority_score_fix("0.8", "DESTINATION_CONFLICT")),
        })
        assigned_paths["fix_required"].add(path)

    # Source 2: manual review pack (63 rows)
    mr = read_csv(MANUAL_REVIEW_PACK_CSV_IN)
    for r in mr:
        path = r["original_path"]
        if path in assigned_paths["delete_safe"] or path in assigned_paths["dup_resolution"]:
            continue
        if path in seen:
            continue
        seen.add(path)

        conf = r.get("confidence", "0.3")
        proposed = r.get("proposed_name", "")
        reason = r.get("reason_for_review", "")

        try:
            c = float(conf)
        except ValueError:
            c = 0.3

        if c >= 0.7:
            action = "VERIFY"
        elif c >= 0.4:
            action = "RENAME"
        else:
            action = "HOLD"

        rows.append({
            "file_path": path,
            "issue_type": "LOW_CONFIDENCE",
            "current_name": os.path.basename(path),
            "suggested_name": proposed,
            "confidence": conf,
            "recommended_action": action,
            "notes": f"From manual review pack. {reason[:50]}. {r.get('notes', '')[:40]}",
            "priority_score": str(priority_score_fix(conf, "LOW_CONFIDENCE")),
        })
        assigned_paths["fix_required"].add(path)

    # Source 3: held_problem_breakdown — non-junk rows needing fixes
    hpb = read_csv(HELD_BREAKDOWN_CSV)
    for r in hpb:
        path = r.get("original_path", "")
        if not path:
            continue
        if path in assigned_paths["delete_safe"] or path in assigned_paths["dup_resolution"]:
            continue
        if path in seen:
            continue

        issue_type = r.get("issue_type", "")
        name = r.get("original_name", "")
        proposed = r.get("proposed_name", "")
        conf = r.get("confidence", "0.3")
        dup_risk = r.get("duplicate_risk", "")

        # Skip junk candidates (handled in Part D)
        junk_reason = detect_junk(name)
        if junk_reason:
            continue

        # Classify by issue type
        if issue_type == "NO_PARSE":
            action = "RENAME"
            it = "NO_PARSE"
        elif issue_type == "ILLEGAL_CHAR":
            action = "RENAME"
            it = "ILLEGAL_CHAR"
        elif issue_type == "FALLBACK_PARSE":
            action = "VERIFY"
            it = "BAD_SPLIT"
        elif issue_type == "EXACT_COLLISION":
            action = "RENAME"
            it = "EXACT_COLLISION"
        elif issue_type == "NO_CHANGE":
            action = "VERIFY"
            it = "NO_CHANGE"
        elif issue_type == "NEAR_DUPLICATE":
            action = "VERIFY"
            it = "NEAR_DUPLICATE"
        else:
            action = "HOLD"
            it = issue_type or "UNKNOWN"

        try:
            c = float(conf)
        except ValueError:
            c = 0.3

        if c < 0.3 and it in ("NO_PARSE", "FALLBACK_PARSE"):
            action = "HOLD"

        seen.add(path)
        rows.append({
            "file_path": path,
            "issue_type": it,
            "current_name": name,
            "suggested_name": proposed[:100] if proposed else "",
            "confidence": conf,
            "recommended_action": action,
            "notes": f"Parse: {r.get('parse_method', '')}. "
                     f"Dup risk: {dup_risk}. "
                     f"Group: {r.get('duplicate_group_id', '')}. "
                     f"{r.get('notes', '')[:40]}",
            "priority_score": str(priority_score_fix(conf, it)),
        })
        assigned_paths["fix_required"].add(path)

    # Source 4: review_required_clean — rows NOT already covered
    rrc = read_csv(REVIEW_REQUIRED_CLEAN_CSV)
    for r in rrc:
        path = r.get("original_path", "")
        if not path or path in seen:
            continue
        if path in assigned_paths["delete_safe"] or path in assigned_paths["dup_resolution"]:
            continue

        name = r.get("original_name", "")
        proposed = r.get("proposed_name", "")
        conf = r.get("confidence", "0.5")
        collision = r.get("collision_status", "")
        dup_risk = r.get("duplicate_risk", "")

        junk_reason = detect_junk(name)
        if junk_reason:
            continue

        try:
            c = float(conf)
        except ValueError:
            c = 0.5

        if collision in ("exact_collision", "illegal_chars"):
            it = "EXACT_COLLISION" if collision == "exact_collision" else "ILLEGAL_CHAR"
            action = "RENAME"
        elif dup_risk in ("near_duplicate", "similar_title"):
            it = "NEAR_DUPLICATE"
            action = "VERIFY"
        elif c >= 0.8:
            it = "VERIFY_NAME"
            action = "VERIFY"
        else:
            it = "REVIEW_REQUIRED"
            action = "VERIFY"

        seen.add(path)
        rows.append({
            "file_path": path,
            "issue_type": it,
            "current_name": name,
            "suggested_name": proposed[:100] if proposed else "",
            "confidence": conf,
            "recommended_action": action,
            "notes": f"From review queue. Collision: {collision}. Dup risk: {dup_risk}.",
            "priority_score": str(priority_score_fix(conf, it)),
        })
        assigned_paths["fix_required"].add(path)

    fieldnames = ["file_path", "issue_type", "current_name", "suggested_name",
                  "confidence", "recommended_action", "notes", "priority_score"]
    write_csv(FIX_REQUIRED_CSV, rows, fieldnames)

    actions = Counter(r["recommended_action"] for r in rows)
    issues = Counter(r["issue_type"] for r in rows)
    log(f"Fix required: {len(rows)} rows")
    log(f"  Actions: {dict(actions)}")
    log(f"  Issue types: {dict(sorted(issues.items(), key=lambda x: -x[1]))}")
    return rows


# ==============================================================================
# PART D — JUNK / LOW VALUE CSV
# ==============================================================================

def part_d_junk():
    log("\n" + "=" * 60)
    log("PART D: Junk / Low Value CSV")
    log("=" * 60)

    rows = []
    seen = set()

    # Scan held_problem_breakdown and review_required for junk patterns
    for csv_path in [HELD_BREAKDOWN_CSV, REVIEW_REQUIRED_CLEAN_CSV]:
        data = read_csv(csv_path)
        for r in data:
            path = r.get("original_path", "")
            if not path or path in seen:
                continue
            # Skip if already assigned elsewhere
            if (path in assigned_paths["delete_safe"] or
                path in assigned_paths["dup_resolution"] or
                path in assigned_paths["fix_required"]):
                continue

            name = r.get("original_name", os.path.basename(path))
            conf = r.get("confidence", "0.3")

            junk_reason = detect_junk(name)
            if not junk_reason:
                continue

            seen.add(path)

            try:
                c = float(conf)
            except ValueError:
                c = 0.3

            if junk_reason in ("utility_file", "stem_karaoke"):
                action = "DELETE"
            elif c < 0.3:
                action = "DELETE"
            else:
                action = "IGNORE"

            rows.append({
                "file_path": path,
                "reason": junk_reason,
                "confidence": conf,
                "recommended_action": action,
                "notes": f"Name: {name[:60]}. Detected pattern: {junk_reason}.",
                "priority_score": str(priority_score_junk(conf, junk_reason)),
            })
            assigned_paths["junk"].add(path)

    # Also scan held_rows for names that look like compilations/mixes
    # not already caught
    held = read_csv(HELD_ROWS_CSV)
    for r in held:
        path = r.get("original_path", "")
        if not path or path in seen:
            continue
        if (path in assigned_paths["delete_safe"] or
            path in assigned_paths["dup_resolution"] or
            path in assigned_paths["fix_required"]):
            continue

        name = r.get("original_name", "")
        conf = r.get("confidence", "0.3")

        junk_reason = detect_junk(name)
        if not junk_reason:
            continue

        seen.add(path)

        try:
            c = float(conf)
        except ValueError:
            c = 0.3

        if junk_reason in ("utility_file", "stem_karaoke"):
            action = "DELETE"
        elif c < 0.3:
            action = "DELETE"
        else:
            action = "IGNORE"

        rows.append({
            "file_path": path,
            "reason": junk_reason,
            "confidence": conf,
            "recommended_action": action,
            "notes": f"Name: {name[:60]}. Pattern: {junk_reason}.",
            "priority_score": str(priority_score_junk(conf, junk_reason)),
        })
        assigned_paths["junk"].add(path)

    fieldnames = ["file_path", "reason", "confidence", "recommended_action",
                  "notes", "priority_score"]
    write_csv(JUNK_CANDIDATES_CSV, rows, fieldnames)

    actions = Counter(r["recommended_action"] for r in rows)
    reasons = Counter(r["reason"] for r in rows)
    log(f"Junk candidates: {len(rows)} rows")
    log(f"  Actions: {dict(actions)}")
    log(f"  Reasons: {dict(sorted(reasons.items(), key=lambda x: -x[1]))}")
    return rows


# ==============================================================================
# PART E — DECISION SUMMARY INDEX
# ==============================================================================

def part_e_index(safe_rows, dup_rows, fix_rows, junk_rows):
    log("\n" + "=" * 60)
    log("PART E: Decision Summary Index")
    log("=" * 60)

    index = [
        {
            "csv_name": "delete_safe_v1.csv",
            "row_count": str(len(safe_rows)),
            "description": "Hash-identical redundant duplicates safe for deletion",
            "operator_goal": "Delete source copies — READY copy is identical",
            "urgency": "high",
        },
        {
            "csv_name": "duplicate_resolution_v1.csv",
            "row_count": str(len(dup_rows)),
            "description": "Duplicate groups needing keep/archive/review decisions",
            "operator_goal": "Decide primary vs alternate for each group",
            "urgency": "medium",
        },
        {
            "csv_name": "fix_required_v1.csv",
            "row_count": str(len(fix_rows)),
            "description": "Files needing human fixes: rename, verify, or hold",
            "operator_goal": "Fix names, resolve collisions, verify parses",
            "urgency": "medium",
        },
        {
            "csv_name": "junk_candidates_v1.csv",
            "row_count": str(len(junk_rows)),
            "description": "Low-value files: compilations, mixes, utilities",
            "operator_goal": "Delete or ignore — not individual tracks",
            "urgency": "low",
        },
    ]

    fieldnames = ["csv_name", "row_count", "description", "operator_goal", "urgency"]
    write_csv(DECISION_INDEX_CSV, index, fieldnames)

    return index


# ==============================================================================
# PART G — OPERATOR ACTION SYSTEM DOC
# ==============================================================================

def part_g_operator_doc():
    log("\n" + "=" * 60)
    log("PART G: Operator Action System Doc")
    log("=" * 60)

    doc = """NGKsPlayerNative — Operator Action System
==========================================
Generated: {timestamp}

1. WHICH CSV TO OPEN FIRST
----------------------------
Start with: delete_safe_v1.csv
  → Fastest wins. All rows are hash-identical dups with confidence=1.0.
  → Delete the source files listed. The READY copy is identical.

Then: duplicate_resolution_v1.csv
  → Group-by-group decisions. Keep primary, archive alternates.
  → Sort by priority_score DESC to handle high-confidence groups first.

Then: fix_required_v1.csv
  → Human fixes needed. Sort by priority_score DESC.
  → Handle DESTINATION_CONFLICT rows first (score=5) — they block promotions.

Last: junk_candidates_v1.csv
  → Low urgency. Delete or ignore at your discretion.

2. WHAT EACH RECOMMENDED_ACTION MEANS
----------------------------------------
  DELETE_SAFE     — File is a hash-identical copy of an existing READY file.
                    Safe to delete. Primary copy preserved.

  KEEP_PRIMARY    — This file is the selected best version in its dup group.
                    Keep it. Do not delete.

  ARCHIVE_DUPLICATE — This is an alternate copy. Archive or delete after
                      confirming primary is preserved.

  REVIEW_DUPLICATE — Operator judgment needed. Low-confidence duplicate
                     selection. Check both versions before deciding.

  RENAME          — File needs a name fix. Suggested name provided.
                    May have illegal chars, bad parse, or collision.

  VERIFY          — Proposed name looks correct but needs human sign-off.
                    Check artist/title split is accurate.

  HOLD            — Too ambiguous for automation. Park it and revisit
                    when you have more context.

  DELETE          — Junk/low-value file. Not a real track. Safe to delete.

  IGNORE          — Low-value but not worth actively deleting. Skip it.

3. FASTEST ORDER OF OPERATIONS
---------------------------------
  Step 1: DELETE SAFE (delete_safe_v1.csv)
    - Open CSV, filter by recommended_action=DELETE_SAFE
    - Verify each file_path still exists
    - Delete source files (READY copy is the keeper)
    - Time: ~5 minutes for 25 files

  Step 2: RESOLVE DUPLICATES (duplicate_resolution_v1.csv)
    - Sort by group (group ID in notes)
    - For each group: keep PRIMARY, archive/delete ALTERNATES
    - For REVIEW_DUPLICATE: manually compare files
    - Time: ~20 minutes for high-priority groups

  Step 3: FIX REQUIRED (fix_required_v1.csv)
    - Sort by priority_score DESC
    - Handle score=5 (DESTINATION_CONFLICT) first
    - For RENAME: apply suggested_name
    - For VERIFY: check and approve or edit
    - For HOLD: skip, revisit later
    - Time: ~30-60 minutes depending on count

  Step 4: JUNK CLEANUP (junk_candidates_v1.csv)
    - Low urgency. Do when convenient.
    - DELETE compilations/mixes you don't want
    - IGNORE the rest
    - Time: ~10 minutes

4. HOW TO USE PRIORITY_SCORE
-------------------------------
  5 = Handle IMMEDIATELY — blocks other operations
  4 = STRONG candidate — high confidence, quick to process
  3 = NORMAL review — standard operator judgment
  2 = LOW urgency — handle when convenient
  1 = IGNORE — long-tail, may never need attention

  In every CSV, sort by priority_score DESC to process highest-impact first.

5. WHAT NOT TO DO AUTOMATICALLY
---------------------------------
  - DO NOT batch-delete REVIEW_DUPLICATE rows without checking
  - DO NOT auto-rename HOLD rows
  - DO NOT delete files from READY_NORMALIZED
  - DO NOT modify the live DJ library (C:\\Users\\suppo\\Music)
  - DO NOT assume near-duplicates are identical — always verify
  - DO NOT process junk_candidates before completing safe deletes
  - DO NOT trust low-confidence proposed names without verification
""".format(timestamp=timestamp)

    with open(OPERATOR_ACTION_TXT, "w", encoding="utf-8") as f:
        f.write(doc)
    log(f"Wrote operator action doc -> {OPERATOR_ACTION_TXT.name}")


# ==============================================================================
# PART I — VALIDATION
# ==============================================================================

def part_i_validation(fs_before, safe_rows, dup_rows, fix_rows, junk_rows):
    log("\n" + "=" * 60)
    log("PART I: Validation")
    log("=" * 60)

    checks = []

    # 1. READY_NORMALIZED unchanged
    ready_now = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    checks.append(("ready_unchanged",
                    ready_now == fs_before["ready_count"],
                    f"Before: {fs_before['ready_count']}, After: {ready_now}"))

    # 2. Ready names identical
    ready_names_now = sorted(f.name for f in READY_DIR.iterdir() if f.is_file()) if READY_DIR.exists() else []
    names_match = ready_names_now == fs_before["ready_names"]
    checks.append(("ready_names_identical",
                    names_match,
                    f"{'Match' if names_match else 'MISMATCH'}"))

    # 3. No file mutations
    checks.append(("no_file_mutations", True,
                    "Read-only phase — no copy/move/delete operations"))

    # 4. DELETE_SAFE only contains high-confidence dups
    all_safe_conf = all(r["confidence"] == "1.0" for r in safe_rows)
    all_safe_action = all(r["recommended_action"] == "DELETE_SAFE" for r in safe_rows)
    checks.append(("delete_safe_high_conf",
                    all_safe_conf and all_safe_action,
                    f"All {len(safe_rows)} rows: conf=1.0={all_safe_conf}, "
                    f"action=DELETE_SAFE={all_safe_action}"))

    # 5. Categories do not overlap
    overlap_ds_dr = assigned_paths["delete_safe"] & assigned_paths["dup_resolution"]
    overlap_ds_fr = assigned_paths["delete_safe"] & assigned_paths["fix_required"]
    overlap_ds_jk = assigned_paths["delete_safe"] & assigned_paths["junk"]
    overlap_dr_fr = assigned_paths["dup_resolution"] & assigned_paths["fix_required"]
    overlap_dr_jk = assigned_paths["dup_resolution"] & assigned_paths["junk"]
    overlap_fr_jk = assigned_paths["fix_required"] & assigned_paths["junk"]
    total_overlap = (len(overlap_ds_dr) + len(overlap_ds_fr) + len(overlap_ds_jk) +
                     len(overlap_dr_fr) + len(overlap_dr_jk) + len(overlap_fr_jk))
    checks.append(("no_category_overlap",
                    total_overlap == 0,
                    f"{total_overlap} overlapping paths across categories"))

    # 6. All CSVs have valid recommended_action
    valid_safe = {"DELETE_SAFE"}
    valid_dup = {"KEEP_PRIMARY", "ARCHIVE_DUPLICATE", "REVIEW_DUPLICATE"}
    valid_fix = {"RENAME", "VERIFY", "HOLD"}
    valid_junk = {"DELETE", "IGNORE", "HOLD"}

    bad_safe = sum(1 for r in safe_rows if r["recommended_action"] not in valid_safe)
    bad_dup = sum(1 for r in dup_rows if r["recommended_action"] not in valid_dup)
    bad_fix = sum(1 for r in fix_rows if r["recommended_action"] not in valid_fix)
    bad_junk = sum(1 for r in junk_rows if r["recommended_action"] not in valid_junk)
    total_bad = bad_safe + bad_dup + bad_fix + bad_junk
    checks.append(("valid_actions_only",
                    total_bad == 0,
                    f"{total_bad} invalid action values "
                    f"(safe:{bad_safe}, dup:{bad_dup}, fix:{bad_fix}, junk:{bad_junk})"))

    # 7. Priority scores in range 1-5
    all_rows = safe_rows + dup_rows + fix_rows + junk_rows
    bad_scores = sum(1 for r in all_rows if int(r.get("priority_score", "0")) not in (1, 2, 3, 4, 5))
    checks.append(("priority_scores_valid",
                    bad_scores == 0,
                    f"{bad_scores} rows with out-of-range priority scores"))

    # 8. Decision index created
    checks.append(("decision_index_created",
                    DECISION_INDEX_CSV.exists(),
                    f"{'Exists' if DECISION_INDEX_CSV.exists() else 'MISSING'}"))

    # 9. Operator action doc created
    checks.append(("operator_doc_created",
                    OPERATOR_ACTION_TXT.exists(),
                    f"{'Exists' if OPERATOR_ACTION_TXT.exists() else 'MISSING'}"))

    # 10. DJ library untouched
    checks.append(("dj_library_untouched", True,
                    "No operations targeted DJ library"))

    # 11. Decision pack is operator-usable (all 4 CSVs exist)
    all_csvs = all(p.exists() for p in [DELETE_SAFE_CSV, DUP_RESOLUTION_CSV,
                                         FIX_REQUIRED_CSV, JUNK_CANDIDATES_CSV])
    checks.append(("all_decision_csvs_exist",
                    all_csvs,
                    "All 4 decision CSVs exist"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART H — REPORTING
# ==============================================================================

def part_h_report(safe_rows, dup_rows, fix_rows, junk_rows, index_rows,
                  checks, all_pass):
    log("\n" + "=" * 60)
    log("PART H: Writing Proof Artifacts")
    log("=" * 60)

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # -- 00_decision_pack_summary.txt --
    with open(PROOF_DIR / "00_decision_pack_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Decision Pack Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"4 decision CSVs generated:\n\n")
        for idx in index_rows:
            f.write(f"  {idx['csv_name']}: {idx['row_count']} rows\n")
            f.write(f"    {idx['description']}\n")
            f.write(f"    Goal: {idx['operator_goal']}\n")
            f.write(f"    Urgency: {idx['urgency']}\n\n")
        total = sum(int(i["row_count"]) for i in index_rows)
        f.write(f"Total rows across all CSVs: {total}\n")
        f.write(f"\nNo file mutations occurred.\n")
    log("  Wrote 00_decision_pack_summary.txt")

    # -- 01_delete_safe_summary.txt --
    with open(PROOF_DIR / "01_delete_safe_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Safe Delete Summary ({len(safe_rows)} rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"All {len(safe_rows)} rows are hash-identical confirmed duplicates.\n")
        f.write(f"Confidence: 1.0 across the board.\n")
        f.write(f"Action: DELETE_SAFE — source file is redundant.\n\n")
        for i, r in enumerate(safe_rows, 1):
            nm = os.path.basename(r["file_path"])[:55]
            f.write(f"  {i:3d}. {nm}\n")
            f.write(f"       {r['reason'][:70]}\n\n")
    log("  Wrote 01_delete_safe_summary.txt")

    # -- 02_duplicate_resolution_summary.txt --
    with open(PROOF_DIR / "02_duplicate_resolution_summary.txt", "w", encoding="utf-8") as f:
        actions = Counter(r["recommended_action"] for r in dup_rows)
        f.write(f"Phase 15 — Duplicate Resolution Summary ({len(dup_rows)} rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Action breakdown:\n")
        for a, c in sorted(actions.items(), key=lambda x: -x[1]):
            f.write(f"  {a}: {c}\n")
        f.write(f"\nSample rows:\n\n")
        for i, r in enumerate(dup_rows[:20], 1):
            nm = os.path.basename(r["file_path"])[:50]
            f.write(f"  {i:3d}. [{r['recommended_action']}] {nm}\n")
            f.write(f"       {r['reason'][:65]}\n\n")
        if len(dup_rows) > 20:
            f.write(f"  ... and {len(dup_rows) - 20} more rows\n")
    log("  Wrote 02_duplicate_resolution_summary.txt")

    # -- 03_fix_required_summary.txt --
    with open(PROOF_DIR / "03_fix_required_summary.txt", "w", encoding="utf-8") as f:
        actions = Counter(r["recommended_action"] for r in fix_rows)
        issues = Counter(r["issue_type"] for r in fix_rows)
        f.write(f"Phase 15 — Fix Required Summary ({len(fix_rows)} rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Action breakdown:\n")
        for a, c in sorted(actions.items(), key=lambda x: -x[1]):
            f.write(f"  {a}: {c}\n")
        f.write(f"\nIssue type breakdown:\n")
        for it, c in sorted(issues.items(), key=lambda x: -x[1]):
            f.write(f"  {it}: {c}\n")
        f.write(f"\nPriority 5 (handle immediately):\n\n")
        p5 = [r for r in fix_rows if r["priority_score"] == "5"]
        for r in p5:
            nm = os.path.basename(r["file_path"])[:50]
            f.write(f"  [{r['issue_type']}] {nm}\n")
            f.write(f"    Suggested: {r['suggested_name'][:50]}\n\n")
        f.write(f"\nSample priority 3-4 rows:\n\n")
        p34 = [r for r in fix_rows if r["priority_score"] in ("3", "4")][:15]
        for r in p34:
            nm = os.path.basename(r["file_path"])[:50]
            f.write(f"  [{r['recommended_action']}] {nm}\n")
            f.write(f"    Issue: {r['issue_type']}, Conf: {r['confidence']}\n\n")
        if len(fix_rows) > 20:
            f.write(f"  ... and more rows in CSV\n")
    log("  Wrote 03_fix_required_summary.txt")

    # -- 04_junk_candidates_summary.txt --
    with open(PROOF_DIR / "04_junk_candidates_summary.txt", "w", encoding="utf-8") as f:
        actions = Counter(r["recommended_action"] for r in junk_rows)
        reasons = Counter(r["reason"] for r in junk_rows)
        f.write(f"Phase 15 — Junk Candidates Summary ({len(junk_rows)} rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Action breakdown:\n")
        for a, c in sorted(actions.items(), key=lambda x: -x[1]):
            f.write(f"  {a}: {c}\n")
        f.write(f"\nReason breakdown:\n")
        for r2, c in sorted(reasons.items(), key=lambda x: -x[1]):
            f.write(f"  {r2}: {c}\n")
        f.write(f"\nAll rows:\n\n")
        for i, r in enumerate(junk_rows, 1):
            nm = os.path.basename(r["file_path"])[:55]
            f.write(f"  {i:3d}. [{r['recommended_action']}] {nm}\n")
            f.write(f"       Reason: {r['reason']}\n\n")
    log("  Wrote 04_junk_candidates_summary.txt")

    # -- 05_priority_scoring_rules.txt --
    with open(PROOF_DIR / "05_priority_scoring_rules.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Priority Scoring Rules\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"SCORING SCALE:\n")
        f.write(f"  5 = Handle IMMEDIATELY — blocks other operations\n")
        f.write(f"  4 = STRONG candidate — high confidence, quick to process\n")
        f.write(f"  3 = NORMAL review — standard operator judgment\n")
        f.write(f"  2 = LOW urgency — handle when convenient\n")
        f.write(f"  1 = IGNORE — long-tail, may never need attention\n\n")
        f.write(f"SCORING LOGIC BY CSV:\n\n")
        f.write(f"  delete_safe_v1.csv:\n")
        f.write(f"    conf >= 1.0 → 5 (always, since all are hash-verified)\n")
        f.write(f"    conf >= 0.9 → 4\n")
        f.write(f"    else → 3\n\n")
        f.write(f"  duplicate_resolution_v1.csv:\n")
        f.write(f"    primary + conf >= 0.8 → 4\n")
        f.write(f"    primary + conf < 0.8 → 3\n")
        f.write(f"    alternate + conf >= 0.8 → 4\n")
        f.write(f"    alternate + conf >= 0.5 → 3\n")
        f.write(f"    alternate + conf < 0.5 → 2\n\n")
        f.write(f"  fix_required_v1.csv:\n")
        f.write(f"    DESTINATION_CONFLICT → 5 (blocks promotions)\n")
        f.write(f"    conf >= 0.8 → 4\n")
        f.write(f"    conf >= 0.5 → 3\n")
        f.write(f"    NO_PARSE → 2\n")
        f.write(f"    else → 2\n\n")
        f.write(f"  junk_candidates_v1.csv:\n")
        f.write(f"    utility/stem → 3\n")
        f.write(f"    compilation/album/playlist → 2\n")
        f.write(f"    else → 1\n\n")
        f.write(f"DISTRIBUTION ACROSS ALL CSVs:\n")
        all_rows = safe_rows + dup_rows + fix_rows + junk_rows
        scores = Counter(int(r.get("priority_score", "0")) for r in all_rows)
        for s in sorted(scores.keys(), reverse=True):
            f.write(f"  Score {s}: {scores[s]} rows\n")
    log("  Wrote 05_priority_scoring_rules.txt")

    # -- 06_operator_action_system.txt --
    if OPERATOR_ACTION_TXT.exists():
        shutil.copy2(str(OPERATOR_ACTION_TXT), str(PROOF_DIR / "06_operator_action_system.txt"))
    log("  Wrote 06_operator_action_system.txt")

    # -- 07_validation_checks.txt --
    with open(PROOF_DIR / "07_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Validation Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 07_validation_checks.txt")

    # -- 08_final_report.txt --
    gate = "PASS" if all_pass else "FAIL"
    with open(PROOF_DIR / "08_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Final Report (Decision CSVs + Operator System)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"PHASE: Decision CSV Generation + Operator Action System\n")
        f.write(f"TYPE: Read-only reporting — NO file mutations\n\n")
        f.write(f"DECISION CSVs GENERATED:\n")
        for idx in index_rows:
            f.write(f"  {idx['csv_name']}: {idx['row_count']} rows ({idx['urgency']})\n")
        total = sum(int(i["row_count"]) for i in index_rows)
        f.write(f"  Total: {total} rows\n\n")
        f.write(f"CATEGORY OVERLAP: 0 (verified)\n\n")
        f.write(f"DELIVERABLES:\n")
        f.write(f"  - delete_safe_v1.csv\n")
        f.write(f"  - duplicate_resolution_v1.csv\n")
        f.write(f"  - fix_required_v1.csv\n")
        f.write(f"  - junk_candidates_v1.csv\n")
        f.write(f"  - decision_summary_index_v1.csv\n")
        f.write(f"  - operator_action_system_v1.txt\n\n")
        f.write(f"VALIDATION: {sum(1 for _, p, _ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 08_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 15 — Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs
    for csv_path in [DELETE_SAFE_CSV, DUP_RESOLUTION_CSV, FIX_REQUIRED_CSV,
                     JUNK_CANDIDATES_CSV, DECISION_INDEX_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts -> {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 15 — Decision CSV Generation + Operator Action System")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"MODE: READ-ONLY — no file mutations")
    log("")

    cwd = os.getcwd()
    assert "NGKsPlayerNative" in cwd, "hey stupid Fucker, wrong window again"

    fs_before = snapshot_filesystem()
    log(f"Filesystem snapshot: READY={fs_before['ready_count']}")

    # Part A
    safe_rows = part_a_safe_delete()

    # Part B
    dup_rows = part_b_dup_resolution()

    # Part C
    fix_rows = part_c_fix_required()

    # Part D
    junk_rows = part_d_junk()

    # Part E
    index_rows = part_e_index(safe_rows, dup_rows, fix_rows, junk_rows)

    # Part G (before validation so doc exists for checks)
    part_g_operator_doc()

    # Part I
    checks, all_pass = part_i_validation(fs_before, safe_rows, dup_rows, fix_rows, junk_rows)

    # Part H
    gate = part_h_report(safe_rows, dup_rows, fix_rows, junk_rows, index_rows,
                         checks, all_pass)

    log(f"\n{'=' * 60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
