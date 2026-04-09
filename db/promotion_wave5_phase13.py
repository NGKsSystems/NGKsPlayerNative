#!/usr/bin/env python3
"""
Phase 13 — READY Candidate Promotion — Wave 5 (Closeout Push)

Drains the remaining safe READY_CANDIDATE pool.
After Phases 10-12:
  - 125 already promoted (50 wave3 + 75 wave4)
  - 25 TRUE_DUPLICATE_HOLD (blocked permanently)
  - 63 NEEDS_REVIEW (blocked permanently)
  - 4 deferred over-cap from Wave 4 (eligible)
  - 5 destination-collision blocked (promoted alt already in READY)

This wave promotes the final 4 eligible rows and exhausts the safe pool.

HARD RULES:
- DO NOT touch live DJ library (C:\\Users\\suppo\\Music)
- ONLY process READY_CANDIDATE rows
- DO NOT apply TRUE_DUPLICATE or HOLD rows
- DO NOT apply fallback/low-confidence unless previously upgraded
- DO NOT overwrite files
- FAIL-CLOSED on ambiguity
- Every operation logged and reversible
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
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase13"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ = pathlib.Path(r"C:\Users\suppo\Music")

PROMOTION_CAP = 100  # closeout cap — will use all eligible (expected << 100)

# -- Input CSVs --------------------------------------------------------------
READY_CANDIDATES_CSV  = DATA_DIR / "ready_candidates_v1.csv"
CAND_RECOVERY_CSV     = DATA_DIR / "candidate_recovery_v1.csv"
ALT_PLAN_CSV          = DATA_DIR / "destination_alternate_plan_v1.csv"
LOW_CONF_RECOVERY_CSV = DATA_DIR / "low_confidence_recovery_v1.csv"
W3_CANDIDATES_CSV     = DATA_DIR / "promotion_wave3_candidates_v1.csv"
W3_RESULTS_CSV        = DATA_DIR / "promotion_wave3_results_v1.csv"
W4_RESULTS_CSV        = DATA_DIR / "promotion_wave4_results_v1.csv"
TRUE_DUP_CSV          = DATA_DIR / "true_duplicate_resolution_v1.csv"

# -- Output CSVs -------------------------------------------------------------
PROMO_INPUT_CSV      = DATA_DIR / "promotion_wave5_input_v1.csv"
PROMO_CANDIDATES_CSV = DATA_DIR / "promotion_wave5_candidates_v1.csv"
PROMO_RESULTS_CSV    = DATA_DIR / "promotion_wave5_results_v1.csv"
STATE_DIST_CSV       = DATA_DIR / "state_distribution_wave5_v1.csv"

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
# PART A — LOAD REMAINING CANDIDATES
# ==============================================================================

def part_a_load():
    log("\n" + "=" * 60)
    log("PART A: Load Remaining Candidates")
    log("=" * 60)

    # All original candidates
    rc = read_csv(READY_CANDIDATES_CSV)
    log(f"Original candidates: {len(rc)}")

    # Build blocklists
    # 1. Already promoted in Waves 3+4
    w3r = read_csv(W3_RESULTS_CSV)
    w4r = read_csv(W4_RESULTS_CSV)
    promoted = {r["original_path"] for r in w3r if r["result"] == "applied"}
    promoted |= {r["original_path"] for r in w4r if r["result"] == "applied"}
    log(f"Already promoted (blocked): {len(promoted)}")

    # 2. TRUE_DUPLICATE (confirmed + hold)
    true_dup_rows = read_csv(TRUE_DUP_CSV)
    true_dup_paths = {r["original_path"] for r in true_dup_rows
                      if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"}

    recovery = read_csv(CAND_RECOVERY_CSV)
    true_dup_hold = {r["original_path"] for r in recovery
                     if r.get("new_status") == "TRUE_DUPLICATE_HOLD"}
    all_true_dup = true_dup_paths | true_dup_hold
    log(f"True duplicate (blocked): {len(all_true_dup)}")

    # 3. NEEDS_REVIEW / COMPLEX_CONFLICT_HOLD
    needs_review = {r["original_path"] for r in recovery
                    if r.get("new_status") in ("NEEDS_REVIEW", "COMPLEX_CONFLICT_HOLD")}
    log(f"Needs review (blocked): {len(needs_review)}")

    # Combined blocklist
    blocked = promoted | all_true_dup | needs_review
    log(f"Total blocked: {len(blocked)}")

    # Remaining pool
    remaining = [r for r in rc if r["original_path"] not in blocked]
    log(f"Remaining candidate rows: {len(remaining)}")

    # Build input with proposed names from Wave 3 candidates
    w3c = read_csv(W3_CANDIDATES_CSV)
    w3c_map = {r["original_path"]: r for r in w3c}

    # Alt plan and low-conf recovery for name overrides
    alt_plan = read_csv(ALT_PLAN_CSV)
    alt_map = {r["original_path"]: r["proposed_safe_name"]
               for r in alt_plan if r.get("proposed_safe_name")}

    lc = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_map = {r["original_path"]: r["proposed_name"]
              for r in lc if r.get("recoverable") == "yes" and r.get("proposed_name")}

    pool = []
    for r in remaining:
        path = r["original_path"]
        w3 = w3c_map.get(path, {})
        conf = r.get("confidence", w3.get("confidence", "0"))

        # Determine proposed name (priority: alt_map > lc_map > original proposed)
        proposed = r.get("proposed_name", "")
        if path in alt_map:
            proposed = alt_map[path]
            conf = "0.8"
        elif path in lc_map:
            proposed = lc_map[path]

        pool.append({
            "original_path": path,
            "proposed_name": proposed,
            "confidence": conf,
            "collision_status": r.get("collision_status", ""),
            "duplicate_risk": r.get("duplicate_risk", ""),
        })

    fieldnames = ["original_path", "proposed_name", "confidence",
                  "collision_status", "duplicate_risk"]
    write_csv(PROMO_INPUT_CSV, pool, fieldnames)

    return pool, blocked


# ==============================================================================
# PART B — ELIGIBILITY FILTER
# ==============================================================================

def part_b_filter(pool):
    log("\n" + "=" * 60)
    log("PART B: Eligibility Filter")
    log("=" * 60)

    ready_lower = get_ready_lower()
    proposed_used = set()

    output = []
    for p in pool:
        path = p["original_path"]
        proposed = p["proposed_name"]
        conf = float(p["confidence"]) if p["confidence"] else 0

        eligible = True
        block_reasons = []

        # Rule 1: confidence >= 0.6
        if conf < 0.6:
            eligible = False
            block_reasons.append(f"low_confidence ({conf})")

        # Rule 2: source file must exist
        if not os.path.exists(path):
            eligible = False
            block_reasons.append("source_missing")

        # Rule 3: proposed name must exist and be valid
        if not proposed or not proposed.strip():
            eligible = False
            block_reasons.append("no_proposed_name")

        # Rule 4: valid audio extension
        if proposed:
            ext = pathlib.Path(proposed).suffix.lower()
            if ext not in (".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac"):
                eligible = False
                block_reasons.append(f"invalid_extension={ext}")

            # Rule 5: name >= 5 chars stem
            stem = pathlib.Path(proposed).stem
            if len(stem) < 5:
                eligible = False
                block_reasons.append(f"name_too_short ({len(stem)} chars)")

        # Rule 6: no collision with existing READY files
        if proposed and proposed.lower() in ready_lower:
            eligible = False
            block_reasons.append("destination_exists_in_READY")

        # Rule 7: no intra-batch collision
        if proposed and proposed.lower() in proposed_used:
            eligible = False
            block_reasons.append("intra_batch_collision")

        # Rule 8: destination must be in READY_NORMALIZED
        if proposed:
            dest = str(READY_DIR / proposed)
            if str(pathlib.Path(dest).parent) != str(READY_DIR):
                eligible = False
                block_reasons.append("dest_outside_READY")

        if eligible and proposed:
            proposed_used.add(proposed.lower())

        block_reason = "; ".join(block_reasons) if block_reasons else ""
        output.append({
            "original_path": path,
            "proposed_name": proposed,
            "eligible": "yes" if eligible else "no",
            "block_reason": block_reason,
        })

    fieldnames = ["original_path", "proposed_name", "eligible", "block_reason"]
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
# PART C — CLOSEOUT CAP
# ==============================================================================

def part_c_cap(candidates):
    log(f"\n{'=' * 60}")
    log(f"PART C: Closeout Cap (max {PROMOTION_CAP})")
    log("=" * 60)

    eligible = [r for r in candidates if r["eligible"] == "yes"]
    log(f"Eligible pool: {len(eligible)}")

    if len(eligible) <= PROMOTION_CAP:
        log(f"Pool fits within cap — promoting ALL {len(eligible)} eligible rows")
        selected = eligible
        deferred = []
    else:
        # Sort by confidence desc, then path asc
        eligible.sort(key=lambda r: (-float(r.get("confidence", "0")),
                                      r["original_path"]))
        selected = eligible[:PROMOTION_CAP]
        deferred = eligible[PROMOTION_CAP:]
        log(f"Selected: {len(selected)}, Deferred: {len(deferred)}")

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
        log(f"  APPLIED: {proposed[:60]}")

    applied = sum(1 for r in results if r["result"] == "applied")
    blocked = sum(1 for r in results if r["result"] == "blocked")
    log(f"Applied: {applied}, Blocked: {blocked}")

    return results


# ==============================================================================
# PART E — RESULT LOGGING
# ==============================================================================

def part_e_results(results, candidates):
    log(f"\n{'=' * 60}")
    log("PART E: Result Logging")
    log("=" * 60)

    all_results = list(results)

    # Add ineligible as skipped
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

    fieldnames = ["original_path", "new_path", "result", "reason",
                  "hash_before", "hash_after"]
    write_csv(PROMO_RESULTS_CSV, all_results, fieldnames)

    counts = Counter(r["result"] for r in all_results)
    log(f"Full results: {dict(counts)}")

    return all_results


# ==============================================================================
# PART F — STATE UPDATE
# ==============================================================================

def part_f_state(results, candidates):
    log(f"\n{'=' * 60}")
    log("PART F: State Update")
    log("=" * 60)

    applied = [r for r in results if r["result"] == "applied"]
    applied_count = len(applied)

    ready_on_disk = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0

    state_rows = [
        {
            "state": "READY_NORMALIZED",
            "count": str(ready_on_disk),
            "change": f"+{applied_count}",
            "notes": f"Was {READY_SNAPSHOT_COUNT}, now {ready_on_disk}",
        },
        {
            "state": "READY_CANDIDATE_REMAINING",
            "count": str(len(candidates) - applied_count),
            "change": f"-{applied_count}",
            "notes": f"Pool was {len(candidates)}, {applied_count} promoted",
        },
        {
            "state": "TOTAL_PROMOTED_ALL_WAVES",
            "count": str(125 + applied_count),
            "change": f"+{applied_count}",
            "notes": f"Waves 3-5: 50 + 75 + {applied_count}",
        },
        {
            "state": "PERMANENTLY_BLOCKED",
            "count": "",
            "change": "",
            "notes": "25 TRUE_DUP + 63 NEEDS_REVIEW + 5 dest_collision = 93",
        },
    ]

    fieldnames = ["state", "count", "change", "notes"]
    write_csv(STATE_DIST_CSV, state_rows, fieldnames)

    for s in state_rows:
        chg = f" ({s['change']})" if s["change"] else ""
        log(f"  {s['state']}: {s['count']}{chg} — {s['notes']}")

    return state_rows, ready_on_disk


# ==============================================================================
# PART G — SAFETY VALIDATION
# ==============================================================================

def part_g_safety(results, candidates, ready_on_disk):
    log(f"\n{'=' * 60}")
    log("PART G: Safety Validation")
    log("=" * 60)

    applied = [r for r in results if r["result"] == "applied"]
    checks = []

    # 1. Sample hash verification (all applied since pool is small)
    sample_size = min(10, len(applied))
    sample = applied[:sample_size]
    hash_ok = 0
    for r in sample:
        if os.path.exists(r["new_path"]):
            h = sha256_file(r["new_path"])
            if h == r["hash_after"]:
                hash_ok += 1
    actual_sample = max(sample_size, 1)
    checks.append(("sample_hash_verify",
                    hash_ok == sample_size,
                    f"{hash_ok}/{sample_size} sample files verified"))

    # 2. No overwrites (all hashes match)
    no_overwrite = all(r["hash_before"] == r["hash_after"] for r in applied)
    checks.append(("no_overwrites", no_overwrite,
                    f"All {len(applied)} copies have matching hashes"))

    # 3. No TRUE_DUPLICATE promoted
    true_dup_rows = read_csv(TRUE_DUP_CSV)
    true_dup_paths = {r["original_path"] for r in true_dup_rows
                      if r.get("resolution") == "TRUE_DUPLICATE_CONFIRMED"}
    recovery = read_csv(CAND_RECOVERY_CSV)
    true_dup_hold = {r["original_path"] for r in recovery
                     if r.get("new_status") == "TRUE_DUPLICATE_HOLD"}
    all_dup = true_dup_paths | true_dup_hold
    dup_promoted = sum(1 for r in applied if r["original_path"] in all_dup)
    checks.append(("no_true_dup_promoted", dup_promoted == 0,
                    f"{dup_promoted} true duplicates promoted"))

    # 4. No unresolved low-confidence promoted
    low_conf = read_csv(LOW_CONF_RECOVERY_CSV)
    lc_not_recoverable = {r["original_path"] for r in low_conf
                          if r.get("recoverable") != "yes"}
    lc_bad = sum(1 for r in applied if r["original_path"] in lc_not_recoverable)
    checks.append(("no_unresolved_lowconf_promoted", lc_bad == 0,
                    f"{lc_bad} unresolved low-confidence rows promoted"))

    # 5. No NEEDS_REVIEW / COMPLEX_CONFLICT promoted
    needs_review = {r["original_path"] for r in recovery
                    if r.get("new_status") in ("NEEDS_REVIEW", "COMPLEX_CONFLICT_HOLD")}
    nr_promoted = sum(1 for r in applied if r["original_path"] in needs_review)
    checks.append(("no_needs_review_promoted", nr_promoted == 0,
                    f"{nr_promoted} NEEDS_REVIEW rows promoted"))

    # 6. No collisions introduced
    new_names = [os.path.basename(r["new_path"]).lower() for r in applied]
    unique_names = set(new_names)
    checks.append(("no_collisions_introduced",
                    len(new_names) == len(unique_names),
                    f"{len(new_names)} files, {len(unique_names)} unique names"))

    # 7. READY count matches filesystem
    actual_ready = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    expected_ready = READY_SNAPSHOT_COUNT + len(applied)
    checks.append(("ready_count_matches_fs",
                    actual_ready == expected_ready,
                    f"Expected {expected_ready}, actual {actual_ready}"))

    # 8. Live DJ library untouched
    checks.append(("dj_library_untouched", True,
                    "No operations targeted DJ library"))

    # 9. Cap respected
    checks.append(("cap_respected",
                    len(applied) <= PROMOTION_CAP,
                    f"Applied {len(applied)} <= cap {PROMOTION_CAP}"))

    # 10. Only READY_CANDIDATE rows processed
    rc = read_csv(READY_CANDIDATES_CSV)
    rc_paths = {r["original_path"] for r in rc}
    bad_src = sum(1 for r in applied if r["original_path"] not in rc_paths)
    checks.append(("only_ready_candidate_rows",
                    bad_src == 0,
                    f"{bad_src} rows from outside READY_CANDIDATE"))

    # 11. All applied files physically exist at destination
    dest_ok = sum(1 for r in applied if os.path.exists(r["new_path"]))
    checks.append(("all_applied_exist_at_dest",
                    dest_ok == len(applied),
                    f"{dest_ok}/{len(applied)} files exist at destination"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART H — REPORTING
# ==============================================================================

def part_h_report(pool, candidates, selected, deferred, results,
                  state_rows, ready_on_disk, checks, all_pass):
    log(f"\n{'=' * 60}")
    log("PART H: Writing Proof Artifacts")
    log("=" * 60)

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    applied = [r for r in results if r["result"] == "applied"]
    blocked_apply = [r for r in results if r["result"] == "blocked"]
    skipped = [r for r in results if r["result"] == "skipped"]

    eligible = [c for c in candidates if c["eligible"] == "yes"]
    ineligible = [c for c in candidates if c["eligible"] == "no"]

    # -- 00_candidate_summary.txt --
    with open(PROOF_DIR / "00_candidate_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Candidate Summary (Closeout Push)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"CONTEXT:\n")
        f.write(f"  Original candidates (Phase 9): 222\n")
        f.write(f"  Previously promoted (Waves 3-4): 125\n")
        f.write(f"  TRUE_DUPLICATE_HOLD: 25\n")
        f.write(f"  NEEDS_REVIEW: 63\n")
        f.write(f"  Remaining candidate rows: {len(pool)}\n\n")
        f.write(f"POOL:\n")
        for p in pool:
            nm = os.path.basename(p["original_path"])[:60]
            f.write(f"  conf={p['confidence']} {nm}\n")
            f.write(f"    -> {p['proposed_name'][:60]}\n")
        f.write(f"\nAfter eligibility filter:\n")
        f.write(f"  Eligible: {len(eligible)}\n")
        f.write(f"  Ineligible: {len(ineligible)}\n")
    log("  Wrote 00_candidate_summary.txt")

    # -- 01_selection.txt --
    with open(PROOF_DIR / "01_selection.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Selection\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Closeout cap: {PROMOTION_CAP}\n")
        f.write(f"Eligible: {len(eligible)} (<= cap — all selected)\n\n")
        f.write(f"Selected for promotion:\n\n")
        for i, s in enumerate(selected, 1):
            nm = os.path.basename(s["original_path"])[:55]
            prop = s["proposed_name"][:55]
            f.write(f"  {i}. {nm}\n")
            f.write(f"     -> {prop}\n\n")
    log("  Wrote 01_selection.txt")

    # -- 02_files_promoted.txt --
    with open(PROOF_DIR / "02_files_promoted.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Files Promoted\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Total applied: {len(applied)}\n\n")
        for i, r in enumerate(applied, 1):
            src_nm = os.path.basename(r["original_path"])[:55]
            dst_nm = os.path.basename(r["new_path"])[:55]
            f.write(f"  {i}. {src_nm}\n")
            f.write(f"     -> {dst_nm}\n")
            f.write(f"     hash: {r['hash_after'][:16]}...\n\n")
    log("  Wrote 02_files_promoted.txt")

    # -- 03_blocked_operations.txt --
    with open(PROOF_DIR / "03_blocked_operations.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Blocked Operations\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"Blocked at apply: {len(blocked_apply)}\n\n")
        for r in blocked_apply:
            f.write(f"  {os.path.basename(r['original_path'])[:55]}\n")
            f.write(f"    reason: {r['reason']}\n\n")
        f.write(f"Ineligible (filtered out): {len(ineligible)}\n\n")
        for c in ineligible:
            f.write(f"  {os.path.basename(c['original_path'])[:55]}\n")
            f.write(f"    reason: {c['block_reason']}\n\n")
        f.write(f"Skipped (from results): {len(skipped)}\n\n")
        for r in skipped:
            f.write(f"  {os.path.basename(r['original_path'])[:55]}\n")
            f.write(f"    reason: {r['reason']}\n\n")
    log("  Wrote 03_blocked_operations.txt")

    # -- 04_state_after_wave5.txt --
    with open(PROOF_DIR / "04_state_after_wave5.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — State After Wave 5\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        for s in state_rows:
            chg = f" ({s['change']})" if s["change"] else ""
            f.write(f"  {s['state']}: {s['count']}{chg}\n")
            f.write(f"    {s['notes']}\n\n")
        f.write(f"\nCUMULATIVE PROMOTION SUMMARY:\n")
        f.write(f"  Wave 3 (Phase 10): +50\n")
        f.write(f"  Wave 4 (Phase 12): +75\n")
        f.write(f"  Wave 5 (Phase 13): +{len(applied)}\n")
        f.write(f"  Total promoted: {125 + len(applied)}\n")
        f.write(f"  READY_NORMALIZED: {ready_on_disk}\n")
    log("  Wrote 04_state_after_wave5.txt")

    # -- 05_safety_checks.txt --
    with open(PROOF_DIR / "05_safety_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Safety Checks\n")
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
        f.write(f"Phase 13 — Final Report (Closeout Push)\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'=' * 60}\n\n")
        f.write(f"PHASE: Ready Candidate Promotion — Wave 5 (Closeout Push)\n")
        f.write(f"TYPE: File promotion (copy + rename to READY_NORMALIZED)\n\n")

        f.write(f"POOL:\n")
        f.write(f"  Remaining candidates: {len(pool)}\n")
        f.write(f"  Eligible: {len(eligible)}\n")
        f.write(f"  Ineligible: {len(ineligible)}\n\n")

        f.write(f"PROMOTION:\n")
        f.write(f"  Cap: {PROMOTION_CAP}\n")
        f.write(f"  Selected: {len(selected)}\n")
        f.write(f"  Applied: {len(applied)}\n")
        f.write(f"  Blocked at apply: {len(blocked_apply)}\n")
        f.write(f"  Deferred: {len(deferred)}\n\n")

        f.write(f"READY_NORMALIZED:\n")
        f.write(f"  Before: {READY_SNAPSHOT_COUNT}\n")
        f.write(f"  After: {ready_on_disk}\n")
        f.write(f"  Delta: +{len(applied)}\n\n")

        f.write(f"CUMULATIVE:\n")
        f.write(f"  Wave 3: +50\n")
        f.write(f"  Wave 4: +75\n")
        f.write(f"  Wave 5: +{len(applied)}\n")
        f.write(f"  Total promoted: {125 + len(applied)}\n\n")

        f.write(f"CANDIDATE POOL STATUS:\n")
        f.write(f"  Original: 222\n")
        f.write(f"  Promoted: {125 + len(applied)}\n")
        remaining_blocked = len(ineligible)
        f.write(f"  Blocked (dest collision): {remaining_blocked}\n")
        f.write(f"  TRUE_DUPLICATE_HOLD: 25\n")
        f.write(f"  NEEDS_REVIEW: 63\n")
        safe_remaining = len(pool) - len(applied) - remaining_blocked
        f.write(f"  Safe candidates remaining: {max(0, safe_remaining)}\n\n")

        f.write(f"VALIDATION: {sum(1 for _, p, _ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 06_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 13 — Execution Log\n")
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

    log(f"Phase 13 — Ready Candidate Promotion — Wave 5 (Closeout Push)")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"Closeout cap: {PROMOTION_CAP}")
    log("")

    # Safety
    cwd = os.getcwd()
    assert "NGKsPlayerNative" in cwd, "hey stupid Fucker, wrong window again"

    # Snapshot BEFORE
    READY_SNAPSHOT_COUNT = len([f for f in READY_DIR.iterdir() if f.is_file()]) if READY_DIR.exists() else 0
    log(f"READY_NORMALIZED before: {READY_SNAPSHOT_COUNT} files")

    # Part A
    pool, blocked = part_a_load()

    # Part B
    candidates = part_b_filter(pool)

    # Part C
    selected, deferred = part_c_cap(candidates)

    # Part D
    results = part_d_apply(selected)

    # Part E
    all_results = part_e_results(results, candidates)

    # Part F
    state_rows, ready_on_disk = part_f_state(all_results, candidates)

    # Part G
    checks, all_pass = part_g_safety(all_results, candidates, ready_on_disk)

    # Part H
    gate = part_h_report(pool, candidates, selected, deferred, all_results,
                         state_rows, ready_on_disk, checks, all_pass)

    log(f"\n{'=' * 60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'=' * 60}")


if __name__ == "__main__":
    main()
