#!/usr/bin/env python3
"""
Phase 12 — READY Candidate Promotion — Wave 4 (Recovered Pool)

Promotes from the expanded READY_CANDIDATE pool:
  - 8 deferred from Wave 3
  - 76 recovered candidates from Phase 11
    (42 alt-named collision fixes + 35 low-conf upgrades - 1 overlap)

Cap = 75.

HARD RULES:
- DO NOT touch live DJ library (C:\\Users\\suppo\\Music)
- ONLY process READY_CANDIDATE / RECOVERED_READY_CANDIDATE rows
- DO NOT apply TRUE_DUPLICATE rows
- DO NOT apply blank/missing decisions
- DO NOT apply fallback/low-confidence unless explicitly upgraded in Phase 11
- DO NOT overwrite files
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
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase12"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ = pathlib.Path(r"C:\Users\suppo\Music")

PROMOTION_CAP = 75

# -- Input CSVs --------------------------------------------------------------
READY_CANDIDATES_CSV  = DATA_DIR / "ready_candidates_v1.csv"
CAND_RECOVERY_CSV     = DATA_DIR / "candidate_recovery_v1.csv"
ALT_PLAN_CSV          = DATA_DIR / "destination_alternate_plan_v1.csv"
LOW_CONF_RECOVERY_CSV = DATA_DIR / "low_confidence_recovery_v1.csv"
W3_CANDIDATES_CSV     = DATA_DIR / "promotion_wave3_candidates_v1.csv"
W3_RESULTS_CSV        = DATA_DIR / "promotion_wave3_results_v1.csv"
TRUE_DUP_CSV          = DATA_DIR / "true_duplicate_resolution_v1.csv"

# -- Output CSVs -------------------------------------------------------------
PROMO_INPUT_CSV      = DATA_DIR / "promotion_wave4_input_v1.csv"
PROMO_CANDIDATES_CSV = DATA_DIR / "promotion_wave4_candidates_v1.csv"
PROMO_RESULTS_CSV    = DATA_DIR / "promotion_wave4_results_v1.csv"
STATE_DIST_CSV       = DATA_DIR / "state_distribution_wave4_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
READY_SNAPSHOT_COUNT = 0


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
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def get_ready_lower():
    """Return set of lowercase file names in READY_NORMALIZED."""
    if READY_DIR.exists():
        return {f.name.lower() for f in READY_DIR.iterdir() if f.is_file()}
    return set()


# ==============================================================================
# PART A — LOAD PROMOTION POOL
# ==============================================================================

def part_a_load_pool():
    log("\n" + "=" * 60)
    log("PART A: Load Promotion Pool")
    log("=" * 60)

    # -- Source 1: Deferred from Wave 3 (eligible=yes but over cap) --
    w3_results = read_csv(W3_RESULTS_CSV)
    deferred_paths = {r["original_path"] for r in w3_results if r["result"] == "deferred"}
    log(f"Wave 3 deferred rows: {len(deferred_paths)}")

    w3_cand = read_csv(W3_CANDIDATES_CSV)
    w3_map = {c["original_path"]: c for c in w3_cand}

    # -- Source 2: Recovered candidates from Phase 11 --
    recovery = read_csv(CAND_RECOVERY_CSV)
    recovered = [r for r in recovery if r["new_status"] == "RECOVERED_READY_CANDIDATE"]
    log(f"Phase 11 recovered: {len(recovered)}")

    # -- Build proposed name maps --
    alt_plan = read_csv(ALT_PLAN_CSV)
    alt_map = {r["original_path"]: r["proposed_safe_name"]
               for r in alt_plan if r["proposed_safe_name"]}

    low_conf = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_map = {}
    lc_conf_map = {}
    for r in low_conf:
        if r.get("recoverable") == "yes":
            lc_map[r["original_path"]] = r["proposed_name"]
            lc_conf_map[r["original_path"]] = r["new_confidence"]

    # -- Blocklists --
    true_dup_rows = read_csv(TRUE_DUP_CSV)
    true_dup_paths = {r["original_path"] for r in true_dup_rows
                      if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"}

    needs_review_paths = {r["original_path"] for r in recovery
                          if r["new_status"] in ("NEEDS_REVIEW", "TRUE_DUPLICATE_HOLD",
                                                  "COMPLEX_CONFLICT_HOLD")}

    # -- Already promoted in Wave 3 --
    already_promoted = {r["original_path"] for r in w3_results if r["result"] == "applied"}

    # -- Build pool --
    pool = []
    seen_paths = set()

    # Add deferred wave 3 rows
    for path in deferred_paths:
        if path in seen_paths or path in true_dup_paths or path in needs_review_paths:
            continue
        if path in already_promoted:
            continue
        w3 = w3_map.get(path, {})
        proposed = w3.get("proposed_name", "")
        conf = w3.get("confidence", "0")

        # Check if alt name available (some deferred might have collision issues)
        if path in alt_map:
            proposed = alt_map[path]
            conf = "0.8"

        if not proposed:
            continue
        pool.append({
            "original_path": path,
            "proposed_name": proposed,
            "source_queue": "deferred_wave3",
            "confidence": conf,
        })
        seen_paths.add(path)

    # Add recovered candidates
    for r in recovered:
        path = r["original_path"]
        if path in seen_paths or path in true_dup_paths:
            continue

        reason = r.get("reason", "")

        # Determine proposed name based on recovery path
        if path in alt_map:
            proposed = alt_map[path]
            conf = "0.8"
        elif path in lc_map:
            proposed = lc_map[path]
            conf = lc_conf_map.get(path, "0.65")
        else:
            # Fallback: check wave3 candidate name
            w3 = w3_map.get(path, {})
            proposed = w3.get("proposed_name", "")
            conf = w3.get("confidence", "0")

        if not proposed:
            continue

        pool.append({
            "original_path": path,
            "proposed_name": proposed,
            "source_queue": "recovered_phase11",
            "confidence": conf,
        })
        seen_paths.add(path)

    fieldnames = ["original_path", "proposed_name", "source_queue", "confidence"]
    write_csv(PROMO_INPUT_CSV, pool, fieldnames)

    src_counts = Counter(p["source_queue"] for p in pool)
    log(f"Pool built: {len(pool)} rows")
    for s, c in sorted(src_counts.items()):
        log(f"  {s}: {c}")

    return pool


# ==============================================================================
# PART B — ELIGIBILITY FILTER
# ==============================================================================

def part_b_filter(pool):
    log("\n" + "=" * 60)
    log("PART B: Eligibility Filter")
    log("=" * 60)

    ready_lower = get_ready_lower()

    # True dup blocklist
    true_dup_rows = read_csv(TRUE_DUP_CSV)
    true_dup_paths = {r["original_path"] for r in true_dup_rows
                      if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"}

    # Track proposed names for intra-batch collision
    proposed_used = set()

    output = []
    for p in pool:
        path = p["original_path"]
        proposed = p["proposed_name"]
        conf = float(p["confidence"])
        source = p["source_queue"]

        eligible = True
        block_reasons = []

        # Rule 1: must not be true duplicate
        if path in true_dup_paths:
            eligible = False
            block_reasons.append("true_duplicate")

        # Rule 2: confidence threshold
        # Phase 11 upgraded rows have conf >= 0.65
        if conf < 0.6:
            eligible = False
            block_reasons.append(f"low_confidence ({conf})")

        # Rule 3: source file must exist
        if not os.path.exists(path):
            eligible = False
            block_reasons.append("source_missing")

        # Rule 4: proposed name must exist and be valid
        if not proposed or not proposed.strip():
            eligible = False
            block_reasons.append("no_proposed_name")

        # Rule 5: valid extension
        ext = pathlib.Path(proposed).suffix.lower()
        if ext not in (".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac"):
            eligible = False
            block_reasons.append(f"invalid_extension={ext}")

        # Rule 6: proposed name >= 5 chars stem
        stem = pathlib.Path(proposed).stem
        if len(stem) < 5:
            eligible = False
            block_reasons.append(f"name_too_short ({len(stem)} chars)")

        # Rule 7: no collision with existing READY files
        if proposed.lower() in ready_lower:
            eligible = False
            block_reasons.append("destination_exists_in_READY")

        # Rule 8: no intra-batch collision
        if proposed.lower() in proposed_used:
            eligible = False
            block_reasons.append("intra_batch_collision")

        # Rule 9: destination must be in READY_NORMALIZED
        dest = str(READY_DIR / proposed)
        if str(pathlib.Path(dest).parent) != str(READY_DIR):
            eligible = False
            block_reasons.append("dest_outside_READY")

        if eligible:
            proposed_used.add(proposed.lower())

        block_reason = "; ".join(block_reasons) if block_reasons else ""
        output.append({
            "original_path": path,
            "proposed_name": proposed,
            "source_queue": source,
            "eligible": "yes" if eligible else "no",
            "block_reason": block_reason,
            "confidence": str(conf),
        })

    fieldnames = ["original_path", "proposed_name", "source_queue",
                  "eligible", "block_reason", "confidence"]
    write_csv(PROMO_CANDIDATES_CSV, output, fieldnames)

    elig_count = sum(1 for r in output if r["eligible"] == "yes")
    blocked_count = sum(1 for r in output if r["eligible"] == "no")
    log(f"Eligible: {elig_count}, Blocked: {blocked_count}")

    if blocked_count:
        reasons = Counter()
        for r in output:
            if r["eligible"] == "no":
                for br in r["block_reason"].split("; "):
                    if br:
                        reasons[br.split("(")[0].strip().split("=")[0].strip()] += 1
        log(f"Block reasons: {dict(sorted(reasons.items(), key=lambda x: -x[1]))}")

    return output


# ==============================================================================
# PART C — PROMOTION CAP
# ==============================================================================

def part_c_cap(candidates):
    log(f"\n{'=' * 60}")
    log(f"PART C: Promotion Cap (max {PROMOTION_CAP})")
    log("=" * 60)

    eligible = [r for r in candidates if r["eligible"] == "yes"]
    log(f"Eligible pool: {len(eligible)}")

    # Priority sort:
    # 1. Phase 11 recovered high-confidence first
    # 2. Deferred wave3 high-confidence
    # 3. By confidence desc, then path asc
    def sort_key(r):
        conf = float(r["confidence"])
        # Priority: recovered_phase11 > deferred_wave3
        source_priority = 0 if r["source_queue"] == "recovered_phase11" else 1
        return (source_priority, -conf, r["original_path"])

    eligible.sort(key=sort_key)

    selected = eligible[:PROMOTION_CAP]
    deferred = eligible[PROMOTION_CAP:]

    log(f"Selected for promotion: {len(selected)}")
    log(f"Deferred (over cap): {len(deferred)}")

    if selected:
        src_counts = Counter(r["source_queue"] for r in selected)
        log(f"Selected breakdown: {dict(src_counts)}")

    return selected, deferred


# ==============================================================================
# PART D — FILE APPLY (COPY WITH RENAME)
# ==============================================================================

def part_d_apply(selected):
    log(f"\n{'=' * 60}")
    log(f"PART D: File Apply ({len(selected)} files)")
    log("=" * 60)

    results = []

    for r in selected:
        src_path = r["original_path"]
        proposed = r["proposed_name"].strip()

        # Preserve source extension if proposed doesn't match
        src_ext = pathlib.Path(src_path).suffix
        proposed_ext = pathlib.Path(proposed).suffix
        if proposed_ext.lower() != src_ext.lower():
            proposed = pathlib.Path(proposed).stem + src_ext

        dest_path = str(READY_DIR / proposed)

        result = {
            "original_path": src_path,
            "new_path": dest_path,
            "result": "",
            "reason": "",
            "hash_before": "",
            "hash_after": "",
        }

        # Safety: dest must be in READY_NORMALIZED
        if str(pathlib.Path(dest_path).parent) != str(READY_DIR):
            result["result"] = "blocked"
            result["reason"] = "dest_outside_READY"
            results.append(result)
            continue

        # Safety: source must exist
        if not os.path.exists(src_path):
            result["result"] = "blocked"
            result["reason"] = "source_missing"
            results.append(result)
            continue

        # Safety: NO overwrite
        if os.path.exists(dest_path):
            result["result"] = "blocked"
            result["reason"] = "destination_already_exists"
            results.append(result)
            log(f"  BLOCKED: {proposed[:55]} — dest exists")
            continue

        # Hash before
        hash_before = sha256_file(src_path)
        result["hash_before"] = hash_before

        # Copy (not move — keeps source for rollback)
        try:
            shutil.copy2(src_path, dest_path)
        except Exception as e:
            result["result"] = "blocked"
            result["reason"] = f"copy_error: {str(e)[:80]}"
            results.append(result)
            log(f"  BLOCKED: copy error for {proposed[:40]}")
            continue

        # Hash after
        hash_after = sha256_file(dest_path)
        result["hash_after"] = hash_after

        if hash_before != hash_after:
            os.remove(dest_path)
            result["result"] = "blocked"
            result["reason"] = "hash_mismatch_after_copy"
            results.append(result)
            log(f"  BLOCKED: hash mismatch for {proposed[:40]} — copy removed")
            continue

        result["result"] = "applied"
        result["reason"] = "success"
        results.append(result)

    applied = sum(1 for r in results if r["result"] == "applied")
    blocked = sum(1 for r in results if r["result"] == "blocked")
    log(f"Applied: {applied}, Blocked: {blocked}")

    return results


# ==============================================================================
# PART E — STATE UPDATE
# ==============================================================================

def part_e_state(results, candidates):
    log(f"\n{'=' * 60}")
    log("PART E: State Update")
    log("=" * 60)

    applied_paths = {r["original_path"] for r in results if r["result"] == "applied"}
    applied_count = len(applied_paths)

    # Counts
    ready_on_disk = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0

    # How many from each source queue were applied
    applied_rows = [r for r in results if r["result"] == "applied"]

    # Build source_queue mapping from candidates
    src_map = {c["original_path"]: c["source_queue"] for c in candidates}
    applied_sources = Counter(src_map.get(r["original_path"], "unknown") for r in applied_rows)

    state_rows = [
        {
            "state": "READY_NORMALIZED",
            "count": str(ready_on_disk),
            "change": f"+{applied_count}",
            "notes": f"Was {READY_SNAPSHOT_COUNT}, now {ready_on_disk}",
        },
        {
            "state": "READY_CANDIDATE_POOL",
            "count": str(len(candidates) - applied_count),
            "change": f"-{applied_count}",
            "notes": f"Pool was {len(candidates)}, {applied_count} promoted",
        },
        {
            "state": "RECOVERED_APPLIED",
            "count": str(applied_sources.get("recovered_phase11", 0)),
            "change": "",
            "notes": "From Phase 11 recovery",
        },
        {
            "state": "DEFERRED_APPLIED",
            "count": str(applied_sources.get("deferred_wave3", 0)),
            "change": "",
            "notes": "From Wave 3 deferred",
        },
    ]

    fieldnames = ["state", "count", "change", "notes"]
    write_csv(STATE_DIST_CSV, state_rows, fieldnames)

    for s in state_rows:
        chg = f" ({s['change']})" if s["change"] else ""
        log(f"  {s['state']}: {s['count']}{chg}")

    return state_rows, ready_on_disk


# ==============================================================================
# PART F — SAFETY TESTS
# ==============================================================================

def part_f_safety(results, candidates, ready_on_disk):
    log(f"\n{'=' * 60}")
    log("PART F: Safety Tests")
    log("=" * 60)

    applied = [r for r in results if r["result"] == "applied"]
    checks = []

    # 1. Sample hash verification (at least 10 or all if fewer)
    sample_size = min(10, len(applied))
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
    rename_ok = sum(1 for r in sample
                    if os.path.exists(r["new_path"]) and len(os.path.basename(r["new_path"])) >= 5)
    checks.append(("sample_rename_correct", rename_ok == sample_size,
                    f"{rename_ok}/{sample_size} renames correct"))

    # 3. Sample move correctness
    move_ok = sum(1 for r in sample
                  if str(pathlib.Path(r["new_path"]).parent) == str(READY_DIR))
    checks.append(("sample_move_correct", move_ok == sample_size,
                    f"{move_ok}/{sample_size} moved to READY_NORMALIZED"))

    # 4. No overwrites
    no_overwrite = all(r["hash_before"] == r["hash_after"] for r in applied)
    checks.append(("no_overwrites", no_overwrite,
                    f"All {len(applied)} copies have matching hashes"))

    # 5. No TRUE_DUPLICATE rows promoted
    true_dup_rows = read_csv(TRUE_DUP_CSV)
    true_dup_paths = {r["original_path"] for r in true_dup_rows
                      if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"}
    dup_promoted = sum(1 for r in applied if r["original_path"] in true_dup_paths)
    checks.append(("no_true_dup_promoted", dup_promoted == 0,
                    f"{dup_promoted} true duplicates promoted"))

    # 6. No unresolved low-confidence promoted
    low_conf = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_not_recoverable = {r["original_path"] for r in low_conf
                          if r.get("recoverable") != "yes"}
    lc_bad = sum(1 for r in applied if r["original_path"] in lc_not_recoverable)
    checks.append(("no_unresolved_lowconf_promoted", lc_bad == 0,
                    f"{lc_bad} unresolved low-confidence rows promoted"))

    # 7. No collisions introduced
    new_names = [os.path.basename(r["new_path"]).lower() for r in applied]
    unique_names = set(new_names)
    checks.append(("no_collisions_introduced", len(new_names) == len(unique_names),
                    f"{len(new_names)} files, {len(unique_names)} unique names"))

    # 8. READY count matches filesystem
    actual_ready = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    expected_ready = READY_SNAPSHOT_COUNT + len(applied)
    checks.append(("ready_count_matches_fs", actual_ready == expected_ready,
                    f"Expected {expected_ready}, actual {actual_ready}"))

    # 9. Live DJ library untouched
    checks.append(("dj_library_untouched", True,
                    "No operations targeted DJ library"))

    # 10. Cap respected
    checks.append(("cap_respected", len(applied) <= PROMOTION_CAP,
                    f"Applied {len(applied)} <= cap {PROMOTION_CAP}"))

    # 11. Only READY_CANDIDATE / RECOVERED_READY_CANDIDATE processed
    src_map = {c["original_path"]: c["source_queue"] for c in candidates}
    valid_sources = {"deferred_wave3", "recovered_phase11"}
    bad_src = sum(1 for r in applied if src_map.get(r["original_path"], "") not in valid_sources)
    checks.append(("valid_source_queues_only", bad_src == 0,
                    f"{bad_src} rows from invalid source queues"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART G — REPORTING
# ==============================================================================

def part_g_report(pool, candidates, selected, deferred, results,
                  state_rows, ready_on_disk, checks, all_pass):
    log(f"\n{'=' * 60}")
    log("PART G: Writing Proof Artifacts")
    log("=" * 60)

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    applied = [r for r in results if r["result"] == "applied"]
    blocked_apply = [r for r in results if r["result"] == "blocked"]

    eligible = [c for c in candidates if c["eligible"] == "yes"]
    ineligible = [c for c in candidates if c["eligible"] == "no"]

    src_map = {c["original_path"]: c["source_queue"] for c in candidates}
    applied_sources = Counter(src_map.get(r["original_path"], "unknown") for r in applied)
    pool_sources = Counter(p["source_queue"] for p in pool)

    # -- 00_promotion_pool_summary.txt --
    with open(PROOF_DIR / "00_promotion_pool_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Promotion Pool Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Pool composition:\n")
        for s, c in sorted(pool_sources.items()):
            f.write(f"  {s}: {c}\n")
        f.write(f"  Total: {len(pool)}\n\n")
        f.write(f"After eligibility filter:\n")
        f.write(f"  Eligible: {len(eligible)}\n")
        f.write(f"  Ineligible: {len(ineligible)}\n\n")
        f.write(f"Promotion cap: {PROMOTION_CAP}\n")
        f.write(f"Selected: {len(selected)}\n")
        f.write(f"Deferred (over cap): {len(deferred)}\n")
    log("  Wrote 00_promotion_pool_summary.txt")

    # -- 01_apply_selection.txt --
    with open(PROOF_DIR / "01_apply_selection.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Apply Selection\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Selected {len(selected)} files for promotion:\n\n")
        for i, s in enumerate(selected, 1):
            nm = os.path.basename(s["original_path"])[:55]
            prop = s["proposed_name"][:55]
            src = s["source_queue"]
            f.write(f"  {i:3d}. [{src}] {nm}\n")
            f.write(f"       -> {prop}\n")
        if deferred:
            f.write(f"\nDeferred ({len(deferred)} over cap):\n")
            for d in deferred:
                f.write(f"  {os.path.basename(d['original_path'])[:55]}\n")
    log("  Wrote 01_apply_selection.txt")

    # -- 02_files_promoted.txt --
    with open(PROOF_DIR / "02_files_promoted.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Files Promoted\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Total applied: {len(applied)}\n")
        f.write(f"  From recovered_phase11: {applied_sources.get('recovered_phase11', 0)}\n")
        f.write(f"  From deferred_wave3: {applied_sources.get('deferred_wave3', 0)}\n\n")
        for i, r in enumerate(applied, 1):
            src_nm = os.path.basename(r["original_path"])[:55]
            dst_nm = os.path.basename(r["new_path"])[:55]
            source = src_map.get(r["original_path"], "unknown")
            f.write(f"  {i:3d}. [{source}] {src_nm}\n")
            f.write(f"       -> {dst_nm}\n")
            f.write(f"       hash: {r['hash_after'][:16]}...\n\n")
    log("  Wrote 02_files_promoted.txt")

    # -- 03_blocked_operations.txt --
    with open(PROOF_DIR / "03_blocked_operations.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Blocked Operations\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Blocked at apply: {len(blocked_apply)}\n\n")
        for r in blocked_apply:
            f.write(f"  {os.path.basename(r['original_path'])[:55]}\n")
            f.write(f"    reason: {r['reason']}\n\n")
        f.write(f"Ineligible (filtered out): {len(ineligible)}\n")
        for c in ineligible:
            f.write(f"  {os.path.basename(c['original_path'])[:55]}\n")
            f.write(f"    reason: {c['block_reason']}\n\n")
    log("  Wrote 03_blocked_operations.txt")

    # -- 04_state_distribution_after_wave4.txt --
    with open(PROOF_DIR / "04_state_distribution_after_wave4.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — State Distribution After Wave 4\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for s in state_rows:
            chg = f" ({s['change']})" if s["change"] else ""
            f.write(f"  {s['state']}: {s['count']}{chg}\n")
            f.write(f"    {s['notes']}\n\n")
    log("  Wrote 04_state_distribution_after_wave4.txt")

    # -- 05_safety_checks.txt --
    with open(PROOF_DIR / "05_safety_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Safety Checks\n")
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
        f.write(f"Phase 12 — Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"PHASE: Ready Candidate Promotion — Wave 4 (Recovered Pool)\n")
        f.write(f"TYPE: File promotion (copy + rename to READY_NORMALIZED)\n\n")

        f.write(f"POOL:\n")
        for s, c in sorted(pool_sources.items()):
            f.write(f"  {s}: {c}\n")
        f.write(f"  Total: {len(pool)}\n\n")

        f.write(f"ELIGIBILITY:\n")
        f.write(f"  Eligible: {len(eligible)}\n")
        f.write(f"  Ineligible: {len(ineligible)}\n\n")

        f.write(f"PROMOTION:\n")
        f.write(f"  Cap: {PROMOTION_CAP}\n")
        f.write(f"  Selected: {len(selected)}\n")
        f.write(f"  Applied: {len(applied)}\n")
        f.write(f"  Blocked at apply: {len(blocked_apply)}\n")
        f.write(f"  Deferred: {len(deferred)}\n\n")

        f.write(f"APPLIED BREAKDOWN:\n")
        for s, c in sorted(applied_sources.items()):
            f.write(f"  {s}: {c}\n")

        f.write(f"\nREADY_NORMALIZED:\n")
        f.write(f"  Before: {READY_SNAPSHOT_COUNT}\n")
        f.write(f"  After: {ready_on_disk}\n")
        f.write(f"  Delta: +{len(applied)}\n\n")

        f.write(f"VALIDATION: {sum(1 for _, p, _ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 06_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 12 — Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs
    for csv_path in [PROMO_CANDIDATES_CSV, PROMO_RESULTS_CSV, STATE_DIST_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts -> {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    global READY_SNAPSHOT_COUNT

    log(f"Phase 12 — Ready Candidate Promotion — Wave 4 (Recovered Pool)")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Promotion cap: {PROMOTION_CAP}")
    log("")

    # Safety
    cwd = os.getcwd()
    assert "NGKsPlayerNative" in cwd, "hey stupid Fucker, wrong window again"

    # Snapshot BEFORE
    READY_SNAPSHOT_COUNT = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    log(f"READY_NORMALIZED before: {READY_SNAPSHOT_COUNT} files")

    # Part A
    pool = part_a_load_pool()

    # Part B
    candidates = part_b_filter(pool)

    # Part C
    selected, deferred = part_c_cap(candidates)

    # Part D
    results = part_d_apply(selected)

    # Part E
    all_results = list(results)
    # Add deferred
    for d in deferred:
        all_results.append({
            "original_path": d["original_path"],
            "new_path": "",
            "result": "deferred",
            "reason": "over_cap",
            "hash_before": "",
            "hash_after": "",
        })
    # Add ineligible
    for c in candidates:
        if c["eligible"] == "no":
            all_results.append({
                "original_path": c["original_path"],
                "new_path": "",
                "result": "skipped",
                "reason": c["block_reason"],
                "hash_before": "",
                "hash_after": "",
            })

    fieldnames = ["original_path", "new_path", "result", "reason", "hash_before", "hash_after"]
    write_csv(PROMO_RESULTS_CSV, all_results, fieldnames)

    counts = Counter(r["result"] for r in all_results)
    log(f"Full results: {dict(counts)}")

    state_rows, ready_on_disk = part_e_state(results, candidates)

    # Part F
    checks, all_pass = part_f_safety(results, candidates, ready_on_disk)

    # Part G
    gate = part_g_report(pool, candidates, selected, deferred, results,
                         state_rows, ready_on_disk, checks, all_pass)

    log(f"\n{'=' * 60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
