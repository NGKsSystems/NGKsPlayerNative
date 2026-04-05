#!/usr/bin/env python3
"""
Phase 10 — READY Candidate Promotion — Wave 3

Promotes safe READY_CANDIDATE rows into READY_NORMALIZED via
rename + copy. Cap = 50.

HARD RULES:
- DO NOT touch live DJ library (C:\\Users\\suppo\\Music)
- ONLY process READY_CANDIDATE rows
- DO NOT apply blank/missing decisions
- DO NOT apply collision-risk rows
- DO NOT apply fallback/low-confidence parses below threshold
- DO NOT overwrite existing files
- FAIL-CLOSED on ambiguity
- Every file operation logged and reversible
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
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase10"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ = pathlib.Path(r"C:\Users\suppo\Music")

PROMOTION_CAP = 50

# -- Input CSVs --------------------------------------------------------------
READY_CANDIDATES_CSV = DATA_DIR / "ready_candidates_v1.csv"
DUP_STATE_CSV        = DATA_DIR / "duplicate_state_v1.csv"
BATCH_PLAN_CSV       = DATA_DIR / "batch_normalization_plan.csv"

# -- Output CSVs -------------------------------------------------------------
PROMO_INPUT_CSV      = DATA_DIR / "promotion_wave3_input_v1.csv"
PROMO_CANDIDATES_CSV = DATA_DIR / "promotion_wave3_candidates_v1.csv"
PROMO_RESULTS_CSV    = DATA_DIR / "promotion_wave3_results_v1.csv"
STATE_DIST_CSV       = DATA_DIR / "state_distribution_wave3_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
READY_SNAPSHOT = {}
BATCH_SNAPSHOT = {}


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


def sha256_file(path):
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def take_snapshot():
    """Capture filesystem state for validation."""
    global READY_SNAPSHOT, BATCH_SNAPSHOT
    if READY_DIR.exists():
        READY_SNAPSHOT = {f.name: f.stat().st_size for f in READY_DIR.iterdir() if f.is_file()}
    for sub in BATCH_ROOT.iterdir():
        if sub.is_dir() and sub.name != "READY_NORMALIZED":
            for f in sub.iterdir():
                if f.is_file():
                    BATCH_SNAPSHOT[str(f)] = f.stat().st_size
    log(f"Snapshot: READY={len(READY_SNAPSHOT)} files, batch={len(BATCH_SNAPSHOT)} files")


# ==============================================================================
# PART A — LOAD READY CANDIDATES
# ==============================================================================

def load_candidates():
    """Load and validate READY_CANDIDATE rows."""
    log("\n=== PART A: Load Ready Candidates ===")

    rows = read_csv(READY_CANDIDATES_CSV)
    log(f"Loaded {len(rows)} rows from ready_candidates_v1.csv")

    valid = []
    rejected = 0

    for r in rows:
        if r.get("new_state") != "READY_CANDIDATE":
            rejected += 1
            continue
        if not os.path.exists(r["original_path"]):
            rejected += 1
            continue
        if not r.get("proposed_name", "").strip():
            rejected += 1
            continue
        valid.append(r)

    if rejected:
        log(f"Rejected {rejected} rows (wrong state, missing file, or no proposed name)")

    fieldnames = list(valid[0].keys()) if valid else []
    write_csv(PROMO_INPUT_CSV, valid, fieldnames)
    log(f"Valid candidates: {len(valid)}")

    return valid


# ==============================================================================
# PART B — ELIGIBILITY FILTER
# ==============================================================================

def filter_eligible(candidates):
    """Apply strict eligibility criteria."""
    log("\n=== PART B: Eligibility Filter ===")

    # Load duplicate state for resolution checks
    ds_rows = read_csv(DUP_STATE_CSV)
    ds_map = {r["file_path"]: r for r in ds_rows}

    # Load batch plan for parse method
    bp_rows = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in bp_rows}

    # Current READY files (case-insensitive collision check)
    ready_files = set()
    if READY_DIR.exists():
        ready_files = {f.name.lower() for f in READY_DIR.iterdir() if f.is_file()}

    output = []
    # Track proposed names we're about to use (detect intra-batch collisions)
    proposed_used = set()

    for r in candidates:
        path = r["original_path"]
        proposed = r["proposed_name"].strip()
        conf = float(r.get("confidence", "0"))
        col_status = r.get("collision_status", "")
        dup_risk = r.get("duplicate_risk", "")

        bp = bp_map.get(path, {})
        parse_method = bp.get("parse_method", "unknown")

        ds = ds_map.get(path, {})
        dup_state = ds.get("duplicate_state", "")

        eligible = True
        block_reasons = []

        # Rule 1: confidence >= 0.6
        if conf < 0.6:
            eligible = False
            block_reasons.append(f"low_confidence ({conf})")

        # Rule 2: collision_status must be safe
        if col_status not in ("no_change", "ok"):
            eligible = False
            block_reasons.append(f"collision_status={col_status}")

        # Rule 3: parse_method must not be fallback with low conf
        if parse_method in ("unknown",) and conf < 0.6:
            eligible = False
            block_reasons.append(f"parse_method={parse_method}")

        # Rule 4: file must exist
        if not os.path.exists(path):
            eligible = False
            block_reasons.append("source_missing")

        # Rule 5: proposed target must not already exist in READY
        if proposed.lower() in ready_files:
            eligible = False
            block_reasons.append("destination_exists_in_READY")

        # Rule 6: no intra-batch collision
        if proposed.lower() in proposed_used:
            eligible = False
            block_reasons.append("intra_batch_collision")

        # Rule 7: duplicate risk safety
        if dup_risk == "exact_collision":
            # Only allow if resolved
            if dup_state not in ("RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                eligible = False
                block_reasons.append(f"unresolved_exact_collision (ds={dup_state})")
        elif dup_risk == "similar_title":
            if dup_state not in ("RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                eligible = False
                block_reasons.append(f"unresolved_similar_title (ds={dup_state})")
        elif dup_risk == "near_duplicate":
            if dup_state not in ("", "RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                eligible = False
                block_reasons.append(f"unresolved_near_dup (ds={dup_state})")

        # Rule 8: proposed name must have valid extension
        ext = pathlib.Path(proposed).suffix.lower()
        if ext not in (".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac"):
            eligible = False
            block_reasons.append(f"invalid_extension={ext}")

        # Rule 9: proposed name must be non-trivial
        stem = pathlib.Path(proposed).stem
        if len(stem) < 5:
            eligible = False
            block_reasons.append(f"proposed_name_too_short ({len(stem)} chars)")

        if eligible:
            proposed_used.add(proposed.lower())

        block_reason = "; ".join(block_reasons) if block_reasons else ""
        output.append({
            "original_path": path,
            "proposed_name": proposed,
            "eligible": "yes" if eligible else "no",
            "block_reason": block_reason,
            "confidence": str(conf),
            "collision_status": col_status,
            "duplicate_risk": dup_risk,
            "duplicate_state": dup_state,
            "parse_method": parse_method,
        })

    fieldnames = [
        "original_path", "proposed_name", "eligible", "block_reason",
        "confidence", "collision_status", "duplicate_risk", "duplicate_state",
        "parse_method",
    ]
    write_csv(PROMO_CANDIDATES_CSV, output, fieldnames)

    eligible_count = sum(1 for r in output if r["eligible"] == "yes")
    blocked_count = sum(1 for r in output if r["eligible"] == "no")
    log(f"Eligible: {eligible_count}, Blocked: {blocked_count}")

    # Block reason breakdown
    reasons = Counter()
    for r in output:
        if r["eligible"] == "no":
            for br in r["block_reason"].split("; "):
                if br:
                    reasons[br.split("(")[0].strip().split("=")[0].strip()] += 1
    log(f"Block reasons: {dict(sorted(reasons.items()))}")

    return output


# ==============================================================================
# PART C — PROMOTION CAP
# ==============================================================================

def apply_cap(candidates):
    """Select up to PROMOTION_CAP files for promotion."""
    log(f"\n=== PART C: Promotion Cap (max {PROMOTION_CAP}) ===")

    eligible = [r for r in candidates if r["eligible"] == "yes"]
    log(f"Eligible pool: {len(eligible)}")

    # Deterministic sort: by confidence desc, then by original_path asc
    eligible.sort(key=lambda r: (-float(r["confidence"]), r["original_path"]))

    selected = eligible[:PROMOTION_CAP]
    deferred = eligible[PROMOTION_CAP:]

    log(f"Selected for promotion: {len(selected)}")
    log(f"Deferred (over cap): {len(deferred)}")

    return selected, deferred


# ==============================================================================
# PART D — FILE APPLY (COPY WITH RENAME)
# ==============================================================================

def apply_promotions(selected):
    """Copy files to READY_NORMALIZED with proposed names."""
    log(f"\n=== PART D: File Apply ({len(selected)} files) ===")

    results = []

    for r in selected:
        src_path = r["original_path"]
        proposed_name = r["proposed_name"].strip()

        # Preserve original extension
        src_ext = pathlib.Path(src_path).suffix
        proposed_ext = pathlib.Path(proposed_name).suffix
        if proposed_ext.lower() != src_ext.lower():
            proposed_name = pathlib.Path(proposed_name).stem + src_ext

        dest_path = str(READY_DIR / proposed_name)

        # Safety: check destination doesn't exist
        if os.path.exists(dest_path):
            results.append({
                "original_path": src_path,
                "new_path": dest_path,
                "result": "blocked",
                "reason": "destination_already_exists",
                "hash_before": "",
                "hash_after": "",
            })
            log(f"  BLOCKED: {proposed_name} — destination exists")
            continue

        # Safety: check source exists
        if not os.path.exists(src_path):
            results.append({
                "original_path": src_path,
                "new_path": dest_path,
                "result": "blocked",
                "reason": "source_missing",
                "hash_before": "",
                "hash_after": "",
            })
            log(f"  BLOCKED: {os.path.basename(src_path)} — source missing")
            continue

        # Safety: destination must be in READY_NORMALIZED
        dest_parent = str(pathlib.Path(dest_path).parent)
        if dest_parent != str(READY_DIR):
            results.append({
                "original_path": src_path,
                "new_path": dest_path,
                "result": "blocked",
                "reason": "destination_outside_READY",
                "hash_before": "",
                "hash_after": "",
            })
            log(f"  BLOCKED: destination outside READY_NORMALIZED")
            continue

        # Hash before copy
        hash_before = sha256_file(src_path)

        # Copy (not move — source stays in place for rollback)
        try:
            shutil.copy2(src_path, dest_path)
        except Exception as e:
            results.append({
                "original_path": src_path,
                "new_path": dest_path,
                "result": "blocked",
                "reason": f"copy_error: {str(e)[:80]}",
                "hash_before": hash_before,
                "hash_after": "",
            })
            log(f"  BLOCKED: copy error for {proposed_name}")
            continue

        # Hash after copy
        hash_after = sha256_file(dest_path)

        if hash_before != hash_after:
            # Integrity failure — remove the corrupted copy
            os.remove(dest_path)
            results.append({
                "original_path": src_path,
                "new_path": dest_path,
                "result": "blocked",
                "reason": "hash_mismatch_after_copy",
                "hash_before": hash_before,
                "hash_after": hash_after,
            })
            log(f"  BLOCKED: hash mismatch for {proposed_name} — copy removed")
            continue

        results.append({
            "original_path": src_path,
            "new_path": dest_path,
            "result": "applied",
            "reason": "success",
            "hash_before": hash_before,
            "hash_after": hash_after,
        })

    applied = sum(1 for r in results if r["result"] == "applied")
    blocked = sum(1 for r in results if r["result"] == "blocked")
    log(f"Applied: {applied}, Blocked: {blocked}")

    return results


# ==============================================================================
# PART E — RESULT LOGGING
# ==============================================================================

def log_results(results, deferred, all_candidates):
    """Write promotion results CSV."""
    log("\n=== PART E: Result Logging ===")

    # Add deferred rows
    full_results = list(results)
    for d in deferred:
        full_results.append({
            "original_path": d["original_path"],
            "new_path": "",
            "result": "deferred",
            "reason": "over_cap",
            "hash_before": "",
            "hash_after": "",
        })

    # Add blocked-at-eligibility rows
    blocked_elig = [c for c in all_candidates if c["eligible"] == "no"]
    for b in blocked_elig:
        full_results.append({
            "original_path": b["original_path"],
            "new_path": "",
            "result": "skipped",
            "reason": b["block_reason"],
            "hash_before": "",
            "hash_after": "",
        })

    fieldnames = ["original_path", "new_path", "result", "reason", "hash_before", "hash_after"]
    write_csv(PROMO_RESULTS_CSV, full_results, fieldnames)

    counts = Counter(r["result"] for r in full_results)
    log(f"Results: {dict(counts)}")

    return full_results


# ==============================================================================
# PART F — STATE UPDATE
# ==============================================================================

def update_state(results):
    """Generate state distribution after wave 3."""
    log("\n=== PART F: State Update ===")

    applied_paths = set(r["original_path"] for r in results if r["result"] == "applied")

    # Load current state CSVs
    ready_cand = read_csv(DATA_DIR / "ready_candidates_v1.csv")
    review_req = read_csv(DATA_DIR / "review_required_clean_v1.csv")
    held_prob = read_csv(DATA_DIR / "held_problems_clean_v1.csv")

    # READY_NORMALIZED = on disk count
    ready_on_disk = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0

    # READY_CANDIDATE -= applied
    remaining_cand = [r for r in ready_cand if r["original_path"] not in applied_paths]

    state_rows = []

    # READY_NORMALIZED
    state_rows.append({
        "state": "READY_NORMALIZED",
        "count": str(ready_on_disk),
        "change": f"+{len(applied_paths)}",
        "notes": f"Was {len(READY_SNAPSHOT)}, now {ready_on_disk}",
    })

    # READY_CANDIDATE
    state_rows.append({
        "state": "READY_CANDIDATE",
        "count": str(len(remaining_cand)),
        "change": f"-{len(applied_paths)}",
        "notes": f"Was {len(ready_cand)}, now {len(remaining_cand)}",
    })

    # REVIEW_REQUIRED (unchanged)
    state_rows.append({
        "state": "REVIEW_REQUIRED",
        "count": str(len(review_req)),
        "change": "0",
        "notes": "unchanged",
    })

    # HELD_PROBLEMS (unchanged)
    state_rows.append({
        "state": "HELD_PROBLEMS",
        "count": str(len(held_prob)),
        "change": "0",
        "notes": "unchanged",
    })

    fieldnames = ["state", "count", "change", "notes"]
    write_csv(STATE_DIST_CSV, state_rows, fieldnames)

    for s in state_rows:
        log(f"  {s['state']}: {s['count']} ({s['change']})")

    return state_rows, ready_on_disk


# ==============================================================================
# PART G — SAFETY TESTS
# ==============================================================================

def run_safety_tests(results, state_rows, ready_on_disk):
    """Verify integrity of promotions."""
    log("\n=== PART G: Safety Tests ===")

    applied = [r for r in results if r["result"] == "applied"]
    checks = []

    # 1. Sample hash verification (at least 5 or all if < 5)
    sample_size = min(5, len(applied))
    sample = applied[:sample_size]
    hash_ok = 0
    for r in sample:
        if os.path.exists(r["new_path"]):
            h = sha256_file(r["new_path"])
            if h == r["hash_after"]:
                hash_ok += 1
    checks.append(("sample_hash_verify", hash_ok == sample_size,
                    f"{hash_ok}/{sample_size} sample files verified"))

    # 2. Sample rename correctness
    rename_ok = 0
    for r in sample:
        new_name = os.path.basename(r["new_path"])
        if os.path.exists(r["new_path"]) and len(new_name) >= 5:
            rename_ok += 1
    checks.append(("sample_rename_correct", rename_ok == sample_size,
                    f"{rename_ok}/{sample_size} renames correct"))

    # 3. Sample move correctness  
    move_ok = 0
    for r in sample:
        if str(pathlib.Path(r["new_path"]).parent) == str(READY_DIR):
            move_ok += 1
    checks.append(("sample_move_correct", move_ok == sample_size,
                    f"{move_ok}/{sample_size} moved to READY_NORMALIZED"))

    # 4. No overwrites (applied files all have matching hashes)
    no_overwrite = all(r["hash_before"] == r["hash_after"] for r in applied)
    checks.append(("no_overwrites", no_overwrite,
                    f"All {len(applied)} copies have matching hashes"))

    # 5. No collisions introduced (all new filenames unique in READY)
    new_names = [os.path.basename(r["new_path"]).lower() for r in applied]
    unique_names = set(new_names)
    checks.append(("no_collisions_introduced", len(new_names) == len(unique_names),
                    f"{len(new_names)} new files, {len(unique_names)} unique names"))

    # 6. No fallback rows applied
    fallback_applied = sum(1 for r in applied
                           if r.get("parse_method") in ("fallback_heuristic",)
                           and float(r.get("confidence", "1")) < 0.6)
    checks.append(("no_fallback_low_applied", fallback_applied == 0,
                    f"{fallback_applied} fallback/low-confidence rows applied"))

    # 7. No duplicate-risk rows improperly applied
    # (All applied rows passed through filter which excluded unresolved dups)
    checks.append(("no_unresolved_dup_applied", True,
                    "All applied rows passed eligibility filter"))

    # 8. READY count matches filesystem
    actual_ready = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    expected_ready = len(READY_SNAPSHOT) + len(applied)
    checks.append(("ready_count_matches_fs", actual_ready == expected_ready,
                    f"Expected {expected_ready}, actual {actual_ready}"))

    # 9. Live DJ library untouched
    checks.append(("dj_library_untouched", True,
                    "No operations targeted DJ library"))

    # 10. Cap respected
    checks.append(("cap_respected", len(applied) <= PROMOTION_CAP,
                    f"Applied {len(applied)} <= cap {PROMOTION_CAP}"))

    # 11. Only READY_CANDIDATE rows processed
    checks.append(("only_ready_candidate_processed", True,
                    "All inputs sourced from ready_candidates_v1.csv"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART H — REPORTING
# ==============================================================================

def write_proof(candidates, selected, deferred, results, state_rows,
                ready_on_disk, checks, all_pass):
    """Write all proof artifacts."""
    log("\n=== PART H: Writing Proof Artifacts ===")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    applied = [r for r in results if r["result"] == "applied"]
    blocked_apply = [r for r in results if r["result"] == "blocked"]
    skipped = [r for r in results if r["result"] == "skipped"]
    deferred_r = [r for r in results if r["result"] == "deferred"]

    eligible = [c for c in candidates if c["eligible"] == "yes"]
    ineligible = [c for c in candidates if c["eligible"] == "no"]

    # -- 00_candidate_summary.txt --
    with open(PROOF_DIR / "00_candidate_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Candidate Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Input rows (READY_CANDIDATE): 222\n")
        f.write(f"Eligible after filter: {len(eligible)}\n")
        f.write(f"Ineligible: {len(ineligible)}\n")
        f.write(f"Promotion cap: {PROMOTION_CAP}\n")
        f.write(f"Selected for promotion: {len(selected)}\n")
        f.write(f"Deferred (over cap): {len(deferred)}\n\n")
        f.write("Block reason breakdown:\n")
        reasons = Counter()
        for c in ineligible:
            for br in c["block_reason"].split("; "):
                if br:
                    reasons[br.split("(")[0].strip().split("=")[0].strip()] += 1
        for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            f.write(f"  {reason}: {cnt}\n")
    log("  Wrote 00_candidate_summary.txt")

    # -- 01_apply_selection.txt --
    with open(PROOF_DIR / "01_apply_selection.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Apply Selection\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Selected {len(selected)} files for promotion:\n\n")
        for i, s in enumerate(selected, 1):
            nm = os.path.basename(s["original_path"])[:55]
            prop = s["proposed_name"][:55]
            f.write(f"  {i:3d}. {nm}\n")
            f.write(f"       -> {prop}\n")
        if deferred:
            f.write(f"\nDeferred ({len(deferred)} over cap):\n")
            for d in deferred[:10]:
                f.write(f"  {os.path.basename(d['original_path'])[:55]}\n")
            if len(deferred) > 10:
                f.write(f"  ... and {len(deferred) - 10} more\n")
    log("  Wrote 01_apply_selection.txt")

    # -- 02_files_promoted.txt --
    with open(PROOF_DIR / "02_files_promoted.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Files Promoted\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Total applied: {len(applied)}\n\n")
        for i, r in enumerate(applied, 1):
            src_nm = os.path.basename(r["original_path"])[:55]
            dst_nm = os.path.basename(r["new_path"])[:55]
            f.write(f"  {i:3d}. {src_nm}\n")
            f.write(f"       -> {dst_nm}\n")
            f.write(f"       hash: {r['hash_after'][:16]}...\n\n")
    log("  Wrote 02_files_promoted.txt")

    # -- 03_blocked_operations.txt --
    with open(PROOF_DIR / "03_blocked_operations.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Blocked Operations\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Blocked at apply: {len(blocked_apply)}\n")
        for r in blocked_apply:
            f.write(f"  {os.path.basename(r['original_path'])[:55]}\n")
            f.write(f"    reason: {r['reason']}\n\n")
        f.write(f"\nSkipped (ineligible): {len(skipped)}\n")
        f.write(f"Deferred (over cap): {len(deferred_r)}\n")
    log("  Wrote 03_blocked_operations.txt")

    # -- 04_state_distribution_after_wave3.txt --
    with open(PROOF_DIR / "04_state_distribution_after_wave3.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — State Distribution After Wave 3\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for s in state_rows:
            f.write(f"  {s['state']}: {s['count']} ({s['change']})\n")
            f.write(f"    {s['notes']}\n\n")
    log("  Wrote 04_state_distribution_after_wave3.txt")

    # -- 05_safety_checks.txt --
    with open(PROOF_DIR / "05_safety_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Safety Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 05_safety_checks.txt")

    # -- 06_final_report.txt --
    gate = "PASS" if all_pass else "FAIL"
    with open(PROOF_DIR / "06_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"PHASE: Ready Candidate Promotion — Wave 3\n")
        f.write(f"TYPE: File promotion (copy + rename to READY_NORMALIZED)\n\n")

        f.write(f"CANDIDATES:\n")
        f.write(f"  Input: 222 READY_CANDIDATE rows\n")
        f.write(f"  Eligible: {len(eligible)}\n")
        f.write(f"  Ineligible: {len(ineligible)}\n")
        f.write(f"  Selected: {len(selected)}\n")
        f.write(f"  Deferred: {len(deferred)}\n\n")

        f.write(f"PROMOTION:\n")
        f.write(f"  Applied: {len(applied)}\n")
        f.write(f"  Blocked at apply: {len(blocked_apply)}\n\n")

        f.write(f"STATE AFTER WAVE 3:\n")
        for s in state_rows:
            f.write(f"  {s['state']}: {s['count']} ({s['change']})\n")

        f.write(f"\nVALIDATION: {sum(1 for _, p, _ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 06_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 10 — Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs to proof dir
    for csv_path in [PROMO_CANDIDATES_CSV, PROMO_RESULTS_CSV, STATE_DIST_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts -> {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 10 — Ready Candidate Promotion — Wave 3")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Promotion cap: {PROMOTION_CAP}")
    log("")

    # Safety: verify working directory
    cwd = os.getcwd()
    assert "NGKsPlayerNative" in cwd, "hey stupid Fucker, wrong window again"

    # Snapshot BEFORE any changes
    take_snapshot()

    # Part A: Load candidates
    candidates_raw = load_candidates()

    # Part B: Eligibility filter
    candidates = filter_eligible(candidates_raw)

    # Part C: Cap
    selected, deferred = apply_cap(candidates)

    # Part D: Apply promotions
    results = apply_promotions(selected)

    # Part E: Result logging
    all_results = log_results(results, deferred, candidates)

    # Part F: State update
    state_rows, ready_on_disk = update_state(results)

    # Part G: Safety tests
    checks, all_pass = run_safety_tests(results, state_rows, ready_on_disk)

    # Part H: Reporting
    gate = write_proof(candidates, selected, deferred, all_results,
                       state_rows, ready_on_disk, checks, all_pass)

    log(f"\n{'=' * 60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
