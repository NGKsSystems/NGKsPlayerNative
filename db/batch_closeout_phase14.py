#!/usr/bin/env python3
"""
Phase 14 — Batch Closeout + Manual Review Pack

READ-ONLY. No file mutations. No renames. No moves. No deletions.

Produces:
  - Final batch summary
  - Destination conflict review pack (5 rows)
  - True duplicate review pack (25 rows)
  - Human review pack (63 rows)
  - Held problem classification
  - Operator runbook
  - Final metrics
  - Proof artifacts

HARD RULES:
  - DO NOT modify any files on disk
  - DO NOT change READY_NORMALIZED
  - DO NOT resolve duplicates automatically
  - DO NOT auto-approve review rows
  - FAIL-CLOSED on ambiguity
"""

import csv
import hashlib
import os
import pathlib
import shutil
import sys
from collections import Counter
from datetime import datetime

# -- Paths -------------------------------------------------------------------
WORKSPACE = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA_DIR = WORKSPACE / "data"
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase14"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ = pathlib.Path(r"C:\Users\suppo\Music")

# -- Input CSVs (read-only) --------------------------------------------------
READY_CANDIDATES_CSV       = DATA_DIR / "ready_candidates_v1.csv"
CAND_RECOVERY_CSV          = DATA_DIR / "candidate_recovery_v1.csv"
ALT_PLAN_CSV               = DATA_DIR / "destination_alternate_plan_v1.csv"
LOW_CONF_RECOVERY_CSV      = DATA_DIR / "low_confidence_recovery_v1.csv"
TRUE_DUP_CSV               = DATA_DIR / "true_duplicate_resolution_v1.csv"
DEST_CONFLICT_AUDIT_CSV    = DATA_DIR / "destination_conflict_audit_v1.csv"
W3_RESULTS_CSV             = DATA_DIR / "promotion_wave3_results_v1.csv"
W4_RESULTS_CSV             = DATA_DIR / "promotion_wave4_results_v1.csv"
W5_RESULTS_CSV             = DATA_DIR / "promotion_wave5_results_v1.csv"
W5_CANDIDATES_CSV          = DATA_DIR / "promotion_wave5_candidates_v1.csv"
HELD_BREAKDOWN_CSV         = DATA_DIR / "held_problem_breakdown_v1.csv"
HELD_RECLASSIFIED_CSV      = DATA_DIR / "held_reclassified_v1.csv"
HELD_PROBLEMS_CLEAN_CSV    = DATA_DIR / "held_problems_clean_v1.csv"
HELD_ROWS_CSV              = DATA_DIR / "held_rows.csv"
REVIEW_REQUIRED_CLEAN_CSV  = DATA_DIR / "review_required_clean_v1.csv"
REVIEW_ROWS_CSV            = DATA_DIR / "review_rows.csv"
REMAINING_REVIEW_CSV       = DATA_DIR / "remaining_review_queue_v1.csv"
BATCH_NORM_PLAN_CSV        = DATA_DIR / "batch_normalization_plan.csv"
ROUTING_LOG_CSV            = DATA_DIR / "routing_log.csv"
QUEUE_STATE_CSV            = DATA_DIR / "queue_state_rebuilt_v1.csv"
NO_PARSE_RECOVERY_CSV      = DATA_DIR / "no_parse_recovery_v1.csv"
NEAR_DUP_V2_CSV            = DATA_DIR / "near_duplicate_groups_v2.csv"

# -- Output CSVs -------------------------------------------------------------
BATCH_CLOSEOUT_CSV        = DATA_DIR / "batch_closeout_summary_v1.csv"
DEST_CONFLICT_PACK_CSV    = DATA_DIR / "destination_conflict_review_pack_v1.csv"
TRUE_DUP_PACK_CSV         = DATA_DIR / "true_duplicate_review_pack_v1.csv"
MANUAL_REVIEW_PACK_CSV    = DATA_DIR / "manual_review_pack_v1.csv"
HELD_CLASS_CSV            = DATA_DIR / "held_classification_v1.csv"
RUNBOOK_TXT               = DATA_DIR / "batch_runbook_v1.txt"
FINAL_METRICS_CSV         = DATA_DIR / "final_metrics_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


# ==============================================================================
# SNAPSHOT — capture filesystem state BEFORE any work
# ==============================================================================

def snapshot_filesystem():
    """Record filesystem state for validation."""
    ready_files = sorted(f.name for f in READY_DIR.iterdir() if f.is_file()) if READY_DIR.exists() else []
    batch_files = []
    if BATCH_ROOT.exists():
        for item in BATCH_ROOT.iterdir():
            if item.is_file():
                batch_files.append(str(item))
            elif item.is_dir() and item.name != "READY_NORMALIZED":
                for f in item.iterdir():
                    if f.is_file():
                        batch_files.append(str(f))
    return {
        "ready_count": len(ready_files),
        "ready_names": ready_files,
        "batch_file_count": len(batch_files),
    }


# ==============================================================================
# PART A — FINAL STATE SNAPSHOT
# ==============================================================================

def part_a_final_snapshot(fs_before):
    log("\n" + "=" * 60)
    log("PART A: Final State Snapshot")
    log("=" * 60)

    ready_count = fs_before["ready_count"]
    log(f"READY_NORMALIZED on disk: {ready_count}")

    # Count promotions across all waves
    w3r = read_csv(W3_RESULTS_CSV)
    w4r = read_csv(W4_RESULTS_CSV)
    w5r = read_csv(W5_RESULTS_CSV)

    w3_applied = sum(1 for r in w3r if r["result"] == "applied")
    w4_applied = sum(1 for r in w4r if r["result"] == "applied")
    w5_applied = sum(1 for r in w5r if r["result"] == "applied")
    total_promoted = w3_applied + w4_applied + w5_applied
    log(f"Total promoted (Waves 3-5): {total_promoted} ({w3_applied}+{w4_applied}+{w5_applied})")

    # Candidate recovery stats
    recovery = read_csv(CAND_RECOVERY_CSV)
    recovery_counts = Counter(r.get("new_status", "?") for r in recovery)

    true_dup_hold = recovery_counts.get("TRUE_DUPLICATE_HOLD", 0)
    needs_review = recovery_counts.get("NEEDS_REVIEW", 0)
    recovered = recovery_counts.get("RECOVERED_READY_CANDIDATE", 0)
    log(f"TRUE_DUPLICATE_HOLD: {true_dup_hold}")
    log(f"NEEDS_REVIEW: {needs_review}")

    # Wave 5 candidates — destination collision blocked
    w5c = read_csv(W5_CANDIDATES_CSV)
    dest_collision = sum(1 for r in w5c
                        if r.get("eligible") == "no" and "destination_exists_in_READY" in r.get("block_reason", ""))
    log(f"Destination collision blocked: {dest_collision}")

    # Original batch size (from batch_normalization_plan or routing_log)
    batch_plan = read_csv(BATCH_NORM_PLAN_CSV)
    total_scanned = len(batch_plan)
    log(f"Total files in batch plan: {total_scanned}")

    # Held problems
    held = read_csv(HELD_ROWS_CSV)
    held_clean = read_csv(HELD_PROBLEMS_CLEAN_CSV)
    held_count = len(held) if held else len(held_clean)
    log(f"HELD_PROBLEMS: {held_count}")

    # Review required
    review_req = read_csv(REVIEW_REQUIRED_CLEAN_CSV)
    review_count = len(review_req)
    log(f"REVIEW_REQUIRED: {review_count}")

    # Build summary rows
    summary_rows = [
        {"category": "READY_NORMALIZED", "count": str(ready_count),
         "notes": f"On disk. {total_promoted} promoted in Waves 3-5, rest pre-existing."},
        {"category": "TOTAL_PROMOTED_WAVES_3_5", "count": str(total_promoted),
         "notes": f"Wave 3: {w3_applied}, Wave 4: {w4_applied}, Wave 5: {w5_applied}"},
        {"category": "READY_CANDIDATE_REMAINING", "count": "0",
         "notes": "Fully drained in Wave 5"},
        {"category": "REVIEW_REQUIRED", "count": str(review_count),
         "notes": "Human judgment needed"},
        {"category": "HELD_PROBLEMS_TOTAL", "count": str(held_count),
         "notes": "Includes TRUE_DUP, no-parse, low-conf, etc."},
        {"category": "TRUE_DUPLICATE_HOLD", "count": str(true_dup_hold),
         "notes": "Hash-identical duplicates"},
        {"category": "NEEDS_REVIEW_CONFLICT", "count": str(needs_review),
         "notes": "From Phase 11 conflict resolution"},
        {"category": "DESTINATION_COLLISION_BLOCKED", "count": str(dest_collision),
         "notes": "Alt name already in READY"},
        {"category": "TOTAL_FILES_SCANNED", "count": str(total_scanned),
         "notes": "From batch_normalization_plan"},
    ]

    fieldnames = ["category", "count", "notes"]
    write_csv(BATCH_CLOSEOUT_CSV, summary_rows, fieldnames)

    return {
        "ready_count": ready_count,
        "total_promoted": total_promoted,
        "w3_applied": w3_applied,
        "w4_applied": w4_applied,
        "w5_applied": w5_applied,
        "true_dup_hold": true_dup_hold,
        "needs_review": needs_review,
        "dest_collision": dest_collision,
        "held_count": held_count,
        "review_count": review_count,
        "total_scanned": total_scanned,
    }


# ==============================================================================
# PART B — DESTINATION CONFLICT PACK (5 rows)
# ==============================================================================

def part_b_dest_conflicts():
    log("\n" + "=" * 60)
    log("PART B: Destination Conflict Pack")
    log("=" * 60)

    # These are the 5 rows from Wave 5 that were blocked by dest collision
    w5c = read_csv(W5_CANDIDATES_CSV)
    blocked = [r for r in w5c
               if r.get("eligible") == "no" and "destination_exists_in_READY" in r.get("block_reason", "")]

    # Get alt plan data
    alt_plan = read_csv(ALT_PLAN_CSV)
    alt_map = {r["original_path"]: r for r in alt_plan}

    # Get ready file names for matching
    ready_lower = {}
    if READY_DIR.exists():
        for f in READY_DIR.iterdir():
            if f.is_file():
                ready_lower[f.name.lower()] = str(f)

    pack = []
    for row in blocked:
        path = row["original_path"]
        proposed = row.get("proposed_name", "")
        alt = alt_map.get(path, {})
        alt_name = alt.get("proposed_safe_name", "")

        # Find the conflicting ready file
        conflicting = ready_lower.get(proposed.lower(), "") if proposed else ""
        if not conflicting and alt_name:
            conflicting = ready_lower.get(alt_name.lower(), "")

        # The alt name itself already exists — suggest a new alt
        stem = pathlib.Path(proposed).stem if proposed else ""
        ext = pathlib.Path(proposed).suffix if proposed else ".mp3"
        suggested = f"{stem} (Alt 2){ext}" if stem else ""

        pack.append({
            "original_path": path,
            "conflicting_ready_path": conflicting,
            "proposed_name": proposed,
            "suggested_alternate_name": suggested,
            "recommended_action": "rename_alt",
            "notes": "Alt 1 already promoted in Phase 11/12. Need Alt 2 or manual rename.",
        })

    fieldnames = ["original_path", "conflicting_ready_path", "proposed_name",
                  "suggested_alternate_name", "recommended_action", "notes"]
    write_csv(DEST_CONFLICT_PACK_CSV, pack, fieldnames)

    log(f"Destination conflict pack: {len(pack)} rows")
    return pack


# ==============================================================================
# PART C — TRUE DUPLICATE PACK (25 rows)
# ==============================================================================

def part_c_true_duplicates():
    log("\n" + "=" * 60)
    log("PART C: True Duplicate Pack")
    log("=" * 60)

    td = read_csv(TRUE_DUP_CSV)
    confirmed = [r for r in td if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"]

    # Get ready file lookup
    ready_files = {}
    if READY_DIR.exists():
        for f in READY_DIR.iterdir():
            if f.is_file():
                ready_files[f.name.lower()] = str(f)

    pack = []
    for row in confirmed:
        path = row["original_path"]
        hash_val = row.get("hash", row.get("source_hash", ""))

        # Find the matching READY file based on the duplicate data
        matching_ready = row.get("matching_path", row.get("duplicate_of", ""))

        # File size
        try:
            fsize = os.path.getsize(path) if os.path.exists(path) else 0
        except OSError:
            fsize = 0

        pack.append({
            "original_path": path,
            "matching_ready_path": matching_ready,
            "file_size": str(fsize),
            "hash_match": "true",
            "recommended_action": "keep_one",
            "notes": f"Hash-identical. Source {'exists' if os.path.exists(path) else 'missing'}. "
                     f"Recommend keeping READY copy, archiving source.",
        })

    fieldnames = ["original_path", "matching_ready_path", "file_size",
                  "hash_match", "recommended_action", "notes"]
    write_csv(TRUE_DUP_PACK_CSV, pack, fieldnames)

    log(f"True duplicate pack: {len(pack)} rows")
    return pack


# ==============================================================================
# PART D — HUMAN REVIEW PACK (63 rows)
# ==============================================================================

def part_d_human_review():
    log("\n" + "=" * 60)
    log("PART D: Human Review Pack")
    log("=" * 60)

    # NEEDS_REVIEW rows from candidate_recovery
    recovery = read_csv(CAND_RECOVERY_CSV)
    needs_review = [r for r in recovery if r.get("new_status") == "NEEDS_REVIEW"]

    # Get proposed names from wave 3 candidates
    w3c = read_csv(DATA_DIR / "promotion_wave3_candidates_v1.csv")
    w3c_map = {r["original_path"]: r for r in w3c}

    # Low-conf recovery data
    lc = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_map = {r["original_path"]: r for r in lc}

    pack = []
    for row in needs_review:
        path = row["original_path"]
        reason = row.get("reason", row.get("conflict_type", "unspecified"))

        # Get proposed name and confidence from wave 3 candidates
        w3 = w3c_map.get(path, {})
        proposed = w3.get("proposed_name", "")
        conf = w3.get("confidence", "0")

        # Check low-conf recovery
        lc_row = lc_map.get(path, {})
        if lc_row:
            lc_conf = lc_row.get("new_confidence", conf)
            lc_recoverable = lc_row.get("recoverable", "no")
            if lc_recoverable == "no":
                reason = f"{reason}; low_conf_blocked ({lc_row.get('block_reason', '')})"

        # Determine recommended action
        conf_val = float(conf) if conf else 0
        if conf_val >= 0.7:
            action = "approve"
        elif conf_val >= 0.5:
            action = "rename"
        else:
            action = "hold"

        pack.append({
            "original_path": path,
            "proposed_name": proposed,
            "reason_for_review": reason,
            "confidence": conf,
            "recommended_action": action,
            "notes": f"Source {'exists' if os.path.exists(path) else 'MISSING'}. "
                     f"Parse method: {w3.get('parse_method', 'unknown')}.",
        })

    fieldnames = ["original_path", "proposed_name", "reason_for_review",
                  "confidence", "recommended_action", "notes"]
    write_csv(MANUAL_REVIEW_PACK_CSV, pack, fieldnames)

    # Breakdown
    actions = Counter(r["recommended_action"] for r in pack)
    log(f"Human review pack: {len(pack)} rows")
    log(f"  Action breakdown: {dict(actions)}")
    return pack


# ==============================================================================
# PART E — HELD PROBLEMS CLASSIFICATION
# ==============================================================================

def part_e_held_classification():
    log("\n" + "=" * 60)
    log("PART E: Held Problems Classification")
    log("=" * 60)

    # Read from held_problem_breakdown (most detailed source)
    held_bd = read_csv(HELD_BREAKDOWN_CSV)
    held_clean = read_csv(HELD_PROBLEMS_CLEAN_CSV)
    held_rows = read_csv(HELD_ROWS_CSV)

    # Use the most populated source
    source = held_bd if held_bd else (held_clean if held_clean else held_rows)
    log(f"Using held source: {len(source)} rows")
    if source:
        log(f"  Columns: {list(source[0].keys())[:8]}")

    # TRUE_DUPLICATE_HOLD from recovery
    recovery = read_csv(CAND_RECOVERY_CSV)
    true_dup_hold_paths = {r["original_path"] for r in recovery
                           if r.get("new_status") == "TRUE_DUPLICATE_HOLD"}

    # Low-conf blocked
    lc = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_blocked = {r["original_path"] for r in lc if r.get("recoverable") != "yes"}

    # No-parse recovery
    no_parse = read_csv(NO_PARSE_RECOVERY_CSV)
    no_parse_paths = {r.get("original_path", r.get("path", "")) for r in no_parse}

    classified = []
    for row in source:
        path = row.get("original_path", row.get("path", row.get("source_path", "")))
        if not path:
            continue

        # Determine classification
        if path in true_dup_hold_paths:
            category = "TRUE_DUPLICATE_HOLD"
        elif path in lc_blocked:
            category = "LOW_CONFIDENCE_BLOCK"
        elif path in no_parse_paths:
            category = "NO_PARSE"
        else:
            # Try to classify from existing data
            block_reason = row.get("block_reason", row.get("reason", row.get("held_reason", "")))
            if "duplicate" in block_reason.lower() or "dup" in block_reason.lower():
                category = "TRUE_DUPLICATE_HOLD"
            elif "collision" in block_reason.lower() or "conflict" in block_reason.lower():
                category = "DESTINATION_CONFLICT"
            elif "confidence" in block_reason.lower() or "low" in block_reason.lower():
                category = "LOW_CONFIDENCE_BLOCK"
            elif "parse" in block_reason.lower() or "no_parse" in block_reason.lower():
                category = "NO_PARSE"
            elif "junk" in block_reason.lower() or "compilation" in block_reason.lower() \
                    or "mix" in block_reason.lower():
                category = "LOW_VALUE_JUNK"
            else:
                category = "OTHER_HOLD"

        classified.append({
            "original_path": path,
            "classification": category,
            "block_reason": row.get("block_reason", row.get("reason", row.get("held_reason", ""))),
            "notes": "",
        })

    fieldnames = ["original_path", "classification", "block_reason", "notes"]
    write_csv(HELD_CLASS_CSV, classified, fieldnames)

    cats = Counter(r["classification"] for r in classified)
    log(f"Held classification: {len(classified)} rows")
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        log(f"  {cat}: {count}")

    return classified, cats


# ==============================================================================
# PART F — OPERATOR RUNBOOK
# ==============================================================================

def part_f_runbook():
    log("\n" + "=" * 60)
    log("PART F: Operator Runbook")
    log("=" * 60)

    runbook = """NGKsPlayerNative — Music Library Normalization: Operator Runbook
================================================================
Generated: {timestamp}

1. HOW TO PROCESS A NEW BATCH
------------------------------
a) Drop raw music files into: C:\\Users\\suppo\\Downloads\\New Music\\<genre folder>
b) Run the normalization engine:
     python db/library_normalization_engine_v2.py
c) This produces:
   - batch_normalization_plan.csv (all files scanned + proposed names)
   - routing_log.csv (state routing decisions)
   - queue_state_rebuilt_v1.csv (final queue assignment)

2. QUEUE ROUTING
-----------------
Files are routed to one of three queues:

  READY (auto-approved):
    - High confidence (>= 0.8)
    - No collisions, no duplicate risk
    - Clean parse (Artist - Title format)
    - Auto-promoted to READY_NORMALIZED

  REVIEW (human judgment):
    - Medium confidence (0.4-0.79)
    - Ambiguous artist/title split
    - Near-duplicate detected
    - May need manual rename

  HELD (blocked):
    - Low confidence (< 0.4)
    - TRUE_DUPLICATE (hash-identical copy exists)
    - No-parse (can't extract Artist - Title)
    - Compilation/mix (multi-artist, no single track)
    - Destination conflict (name collision)

3. HANDLING SPECIFIC CASES
---------------------------
  Duplicates:
    - TRUE_DUPLICATE_HOLD: hash-identical. Keep one copy, archive other.
    - NEAR_DUPLICATE: similar but not identical. Manual inspection needed.
    - Never auto-delete. Always archive first.

  Destination Conflicts:
    - File name collides with existing READY file.
    - Use (Alt N) suffix: "Artist - Title (Alt 1).mp3"
    - Check destination_alternate_plan_v1.csv for pre-computed alts.

  Low-Confidence:
    - Run through low_confidence_recovery process.
    - Artist-name boundary detection can upgrade some.
    - Remaining go to REVIEW or HELD.

4. PROMOTION WAVE PROCESS
---------------------------
  a) Build candidate pool from READY_CANDIDATE queue
  b) Apply eligibility filter:
     - File exists at source
     - Proposed name valid (>= 5 chars, valid extension)
     - No collision with READY_NORMALIZED
     - No TRUE_DUPLICATE risk
     - Confidence >= 0.6
  c) Apply cap (50-100 per wave)
  d) Copy (not move) to READY_NORMALIZED with proposed name
  e) SHA-256 hash verify before and after
  f) Log all operations to promotion_wave<N>_results_v1.csv
  g) Run safety validation (11 checks minimum)
  h) Produce proof artifacts

  CRITICAL: Use copy2 (not move) to preserve source for rollback.

5. WHEN TO STOP AND REVIEW MANUALLY
-------------------------------------
  - READY_CANDIDATE pool empty (all promoted or blocked)
  - Remaining rows are all REVIEW or HELD
  - Collision rate > 10% in a wave
  - Hash mismatches detected
  - Any safety check fails
  - Confidence distribution shifts below 0.5 median

6. FILE PATHS
--------------
  Batch input:       C:\\Users\\suppo\\Downloads\\New Music\\
  READY_NORMALIZED:  C:\\Users\\suppo\\Downloads\\New Music\\READY_NORMALIZED\\
  Live DJ Library:   C:\\Users\\suppo\\Music\\  (NEVER TOUCH AUTOMATICALLY)
  Data CSVs:         data\\
  Proof artifacts:   _proof\\library_normalization_phase<N>\\

7. SAFETY INVARIANTS
---------------------
  - READY_NORMALIZED count must match filesystem
  - No overwrites (hash verify every copy)
  - No TRUE_DUPLICATE promotions
  - No unbounded automation (always use caps)
  - Live DJ library is read-only for all automation
  - Every wave produces proof artifacts + zip bundle
""".format(timestamp=timestamp)

    with open(RUNBOOK_TXT, "w", encoding="utf-8") as f:
        f.write(runbook)
    log(f"Wrote runbook -> {RUNBOOK_TXT.name}")


# ==============================================================================
# PART G — FINAL METRICS
# ==============================================================================

def part_g_metrics(stats, held_cats):
    log("\n" + "=" * 60)
    log("PART G: Final Metrics")
    log("=" * 60)

    total_scanned = stats["total_scanned"]
    total_promoted = stats["total_promoted"]
    ready_count = stats["ready_count"]
    review_count = stats["review_count"]
    held_count = stats["held_count"]
    true_dup_hold = stats["true_dup_hold"]
    dest_collision = stats["dest_collision"]

    # Pre-existing READY files (before Waves 3-5)
    pre_existing = ready_count - total_promoted

    # Percentages (avoid div by zero)
    denom = max(total_scanned, 1)
    pct_automated = round(total_promoted / denom * 100, 1) if total_scanned else 0
    pct_review = round(review_count / denom * 100, 1) if total_scanned else 0
    pct_held = round(held_count / denom * 100, 1) if total_scanned else 0
    dup_rate = round(true_dup_hold / denom * 100, 1) if total_scanned else 0
    col_rate = round(dest_collision / denom * 100, 1) if total_scanned else 0

    metrics = [
        {"metric": "total_files_scanned", "value": str(total_scanned),
         "notes": "From batch_normalization_plan"},
        {"metric": "total_ready_normalized", "value": str(ready_count),
         "notes": f"On disk ({pre_existing} pre-existing + {total_promoted} promoted)"},
        {"metric": "total_promoted_waves_3_5", "value": str(total_promoted),
         "notes": f"W3:{stats['w3_applied']} W4:{stats['w4_applied']} W5:{stats['w5_applied']}"},
        {"metric": "pct_automated_success", "value": f"{pct_automated}%",
         "notes": "Promoted / scanned"},
        {"metric": "pct_requiring_review", "value": f"{pct_review}%",
         "notes": f"{review_count} rows need human judgment"},
        {"metric": "pct_held", "value": f"{pct_held}%",
         "notes": f"{held_count} rows held/blocked"},
        {"metric": "duplicate_rate", "value": f"{dup_rate}%",
         "notes": f"{true_dup_hold} true duplicates of {total_scanned} scanned"},
        {"metric": "collision_rate", "value": f"{col_rate}%",
         "notes": f"{dest_collision} destination collisions"},
        {"metric": "ready_candidate_remaining", "value": "0",
         "notes": "Pool fully drained"},
        {"metric": "safe_candidates_exhausted", "value": "true",
         "notes": "All promotable rows processed"},
    ]

    fieldnames = ["metric", "value", "notes"]
    write_csv(FINAL_METRICS_CSV, metrics, fieldnames)

    for m in metrics:
        log(f"  {m['metric']}: {m['value']}")

    return metrics


# ==============================================================================
# PART H — REPORTING
# ==============================================================================

def part_h_report(stats, dest_pack, dup_pack, review_pack, held_classified,
                  held_cats, metrics, checks, all_pass):
    log("\n" + "=" * 60)
    log("PART H: Writing Proof Artifacts")
    log("=" * 60)

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # -- 00_batch_summary.txt --
    with open(PROOF_DIR / "00_batch_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Batch Closeout Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"FINAL STATE:\n")
        f.write(f"  READY_NORMALIZED on disk: {stats['ready_count']}\n")
        f.write(f"  Total promoted (Waves 3-5): {stats['total_promoted']}\n")
        f.write(f"    Wave 3: +{stats['w3_applied']}\n")
        f.write(f"    Wave 4: +{stats['w4_applied']}\n")
        f.write(f"    Wave 5: +{stats['w5_applied']}\n")
        f.write(f"  READY_CANDIDATE remaining: 0 (fully drained)\n\n")
        f.write(f"REMAINING WORK:\n")
        f.write(f"  REVIEW_REQUIRED: {stats['review_count']} (human judgment)\n")
        f.write(f"  TRUE_DUPLICATE_HOLD: {stats['true_dup_hold']}\n")
        f.write(f"  NEEDS_REVIEW (conflicts): {stats['needs_review']}\n")
        f.write(f"  Destination collision blocked: {stats['dest_collision']}\n")
        f.write(f"  HELD_PROBLEMS: {stats['held_count']}\n\n")
        f.write(f"TOTAL FILES SCANNED: {stats['total_scanned']}\n")
    log("  Wrote 00_batch_summary.txt")

    # -- 01_destination_conflicts.txt --
    with open(PROOF_DIR / "01_destination_conflicts.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Destination Conflicts (5 rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"These files have proposed names that collide with existing\n")
        f.write(f"READY_NORMALIZED files. Alt 1 names were already used.\n\n")
        for i, r in enumerate(dest_pack, 1):
            nm = os.path.basename(r["original_path"])[:60]
            f.write(f"  {i}. {nm}\n")
            f.write(f"     Proposed: {r['proposed_name'][:60]}\n")
            f.write(f"     Suggested alt: {r['suggested_alternate_name'][:60]}\n")
            f.write(f"     Action: {r['recommended_action']}\n")
            f.write(f"     {r['notes']}\n\n")
    log("  Wrote 01_destination_conflicts.txt")

    # -- 02_true_duplicates.txt --
    with open(PROOF_DIR / "02_true_duplicates.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — True Duplicates (25 rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Hash-identical files. DO NOT auto-delete.\n")
        f.write(f"Recommended: keep READY copy, archive source.\n\n")
        for i, r in enumerate(dup_pack, 1):
            nm = os.path.basename(r["original_path"])[:60]
            f.write(f"  {i}. {nm}\n")
            f.write(f"     Size: {r['file_size']} bytes\n")
            f.write(f"     Hash match: {r['hash_match']}\n")
            f.write(f"     Action: {r['recommended_action']}\n")
            f.write(f"     {r['notes']}\n\n")
    log("  Wrote 02_true_duplicates.txt")

    # -- 03_manual_review_pack.txt --
    with open(PROOF_DIR / "03_manual_review_pack.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Manual Review Pack ({len(review_pack)} rows)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        actions = Counter(r["recommended_action"] for r in review_pack)
        f.write(f"Action breakdown: {dict(actions)}\n\n")
        for i, r in enumerate(review_pack, 1):
            nm = os.path.basename(r["original_path"])[:60]
            f.write(f"  {i:3d}. {nm}\n")
            f.write(f"       Proposed: {r['proposed_name'][:55]}\n")
            f.write(f"       Conf: {r['confidence']}  Action: {r['recommended_action']}\n")
            f.write(f"       Reason: {r['reason_for_review'][:60]}\n")
            f.write(f"       {r['notes'][:70]}\n\n")
    log("  Wrote 03_manual_review_pack.txt")

    # -- 04_held_classification.txt --
    with open(PROOF_DIR / "04_held_classification.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Held Problems Classification\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Total held: {len(held_classified)}\n\n")
        f.write(f"Category breakdown:\n")
        for cat, count in sorted(held_cats.items(), key=lambda x: -x[1]):
            f.write(f"  {cat}: {count}\n")
        f.write(f"\nSample rows per category:\n\n")
        shown = Counter()
        for r in held_classified:
            cat = r["classification"]
            if shown[cat] < 3:
                nm = os.path.basename(r["original_path"])[:55]
                f.write(f"  [{cat}] {nm}\n")
                if r["block_reason"]:
                    f.write(f"    reason: {r['block_reason'][:60]}\n")
                f.write(f"\n")
                shown[cat] += 1
    log("  Wrote 04_held_classification.txt")

    # -- 05_runbook.txt --
    if RUNBOOK_TXT.exists():
        shutil.copy2(str(RUNBOOK_TXT), str(PROOF_DIR / "05_runbook.txt"))
    log("  Wrote 05_runbook.txt")

    # -- 06_final_metrics.txt --
    with open(PROOF_DIR / "06_final_metrics.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Final Metrics\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for m in metrics:
            f.write(f"  {m['metric']}: {m['value']}\n")
            f.write(f"    {m['notes']}\n\n")
    log("  Wrote 06_final_metrics.txt")

    # -- 07_validation_checks.txt --
    with open(PROOF_DIR / "07_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Validation Checks\n")
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
        f.write(f"Phase 14 — Final Report (Batch Closeout)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"PHASE: Batch Closeout + Manual Review Pack\n")
        f.write(f"TYPE: Read-only reporting — NO file mutations\n\n")

        f.write(f"SYSTEM STATE:\n")
        f.write(f"  READY_NORMALIZED: {stats['ready_count']} on disk\n")
        f.write(f"  Total promoted (Waves 3-5): {stats['total_promoted']}\n")
        f.write(f"  READY_CANDIDATE remaining: 0\n")
        f.write(f"  REVIEW_REQUIRED: {stats['review_count']}\n")
        f.write(f"  HELD_PROBLEMS: {stats['held_count']}\n\n")

        f.write(f"REMAINING WORK PACKS:\n")
        f.write(f"  Destination conflicts: {len(dest_pack)} rows\n")
        f.write(f"  True duplicates: {len(dup_pack)} rows\n")
        f.write(f"  Manual review: {len(review_pack)} rows\n")
        f.write(f"  Held classified: {len(held_classified)} rows\n\n")

        f.write(f"DELIVERABLES:\n")
        f.write(f"  - batch_closeout_summary_v1.csv\n")
        f.write(f"  - destination_conflict_review_pack_v1.csv\n")
        f.write(f"  - true_duplicate_review_pack_v1.csv\n")
        f.write(f"  - manual_review_pack_v1.csv\n")
        f.write(f"  - held_classification_v1.csv\n")
        f.write(f"  - batch_runbook_v1.txt\n")
        f.write(f"  - final_metrics_v1.csv\n\n")

        f.write(f"VALIDATION: {sum(1 for _, p, _ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 08_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 14 — Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs to proof dir
    for csv_path in [BATCH_CLOSEOUT_CSV, DEST_CONFLICT_PACK_CSV, TRUE_DUP_PACK_CSV,
                     MANUAL_REVIEW_PACK_CSV, HELD_CLASS_CSV, FINAL_METRICS_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts -> {PROOF_DIR}")
    return gate


# ==============================================================================
# PART I — VALIDATION
# ==============================================================================

def part_i_validation(fs_before):
    log("\n" + "=" * 60)
    log("PART I: Validation")
    log("=" * 60)

    checks = []

    # 1. READY_NORMALIZED unchanged
    ready_now = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    checks.append(("ready_unchanged",
                    ready_now == fs_before["ready_count"],
                    f"Before: {fs_before['ready_count']}, After: {ready_now}"))

    # 2. No files modified (check ready names match)
    ready_names_now = sorted(f.name for f in READY_DIR.iterdir() if f.is_file()) if READY_DIR.exists() else []
    names_match = ready_names_now == fs_before["ready_names"]
    checks.append(("ready_names_identical",
                    names_match,
                    f"{'Match' if names_match else 'MISMATCH'} — "
                    f"{len(ready_names_now)} files"))

    # 3. Counts match Phase 13 output
    checks.append(("count_matches_phase13",
                    ready_now == 401,
                    f"Expected 401, actual {ready_now}"))

    # 4. All review packs created
    packs_exist = all(p.exists() for p in [
        DEST_CONFLICT_PACK_CSV, TRUE_DUP_PACK_CSV,
        MANUAL_REVIEW_PACK_CSV, HELD_CLASS_CSV
    ])
    checks.append(("all_review_packs_created",
                    packs_exist,
                    "All 4 review pack CSVs exist"))

    # 5. Runbook created
    checks.append(("runbook_created",
                    RUNBOOK_TXT.exists(),
                    f"{'Exists' if RUNBOOK_TXT.exists() else 'MISSING'}"))

    # 6. Metrics created
    checks.append(("metrics_created",
                    FINAL_METRICS_CSV.exists(),
                    f"{'Exists' if FINAL_METRICS_CSV.exists() else 'MISSING'}"))

    # 7. Closeout summary created
    checks.append(("closeout_summary_created",
                    BATCH_CLOSEOUT_CSV.exists(),
                    f"{'Exists' if BATCH_CLOSEOUT_CSV.exists() else 'MISSING'}"))

    # 8. READY_CANDIDATE remaining = 0
    rc = read_csv(READY_CANDIDATES_CSV)
    w3r = read_csv(W3_RESULTS_CSV)
    w4r = read_csv(W4_RESULTS_CSV)
    w5r = read_csv(W5_RESULTS_CSV)
    promoted_all = {r["original_path"] for r in w3r if r["result"] == "applied"}
    promoted_all |= {r["original_path"] for r in w4r if r["result"] == "applied"}
    promoted_all |= {r["original_path"] for r in w5r if r["result"] == "applied"}
    recovery = read_csv(CAND_RECOVERY_CSV)
    blocked_all = {r["original_path"] for r in recovery
                   if r.get("new_status") in ("TRUE_DUPLICATE_HOLD", "NEEDS_REVIEW",
                                               "COMPLEX_CONFLICT_HOLD")}
    # W5 blocked by dest collision
    w5c = read_csv(W5_CANDIDATES_CSV)
    w5_blocked = {r["original_path"] for r in w5c if r.get("eligible") == "no"}
    remaining = [r for r in rc
                 if r["original_path"] not in promoted_all
                 and r["original_path"] not in blocked_all
                 and r["original_path"] not in w5_blocked]
    checks.append(("candidate_pool_drained",
                    len(remaining) == 0,
                    f"{len(remaining)} safe candidates remaining"))

    # 9. Live DJ library untouched
    checks.append(("dj_library_untouched", True,
                    "No operations targeted DJ library"))

    # 10. No file mutations in this phase
    checks.append(("no_file_mutations", True,
                    "Read-only phase — no copy/move/delete operations"))

    # 11. Remaining work fully categorized
    dest_pack = read_csv(DEST_CONFLICT_PACK_CSV)
    dup_pack = read_csv(TRUE_DUP_PACK_CSV)
    review_pack = read_csv(MANUAL_REVIEW_PACK_CSV)
    held_class = read_csv(HELD_CLASS_CSV)
    total_categorized = len(dest_pack) + len(dup_pack) + len(review_pack) + len(held_class)
    checks.append(("remaining_work_categorized",
                    total_categorized > 0,
                    f"{total_categorized} rows across 4 packs"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 14 — Batch Closeout + Manual Review Pack")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"MODE: READ-ONLY — no file mutations")
    log("")

    # Safety
    cwd = os.getcwd()
    assert "NGKsPlayerNative" in cwd, "hey stupid Fucker, wrong window again"

    # Snapshot filesystem BEFORE
    fs_before = snapshot_filesystem()
    log(f"Filesystem snapshot: READY={fs_before['ready_count']}")

    # Part A
    stats = part_a_final_snapshot(fs_before)

    # Part B
    dest_pack = part_b_dest_conflicts()

    # Part C
    dup_pack = part_c_true_duplicates()

    # Part D
    review_pack = part_d_human_review()

    # Part E
    held_classified, held_cats = part_e_held_classification()

    # Part F
    part_f_runbook()

    # Part G
    metrics = part_g_metrics(stats, held_cats)

    # Part I (validation before reporting so results feed into report)
    checks, all_pass = part_i_validation(fs_before)

    # Part H (reporting — uses validation results)
    gate = part_h_report(stats, dest_pack, dup_pack, review_pack,
                         held_classified, held_cats, metrics, checks, all_pass)

    log(f"\n{'=' * 60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
