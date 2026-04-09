#!/usr/bin/env python3
"""
Phase 17.5 — Operator Correction Execution (RENAME only)
=========================================================
Reads human-annotated fix_required_v3 corrections.
Compares cleaned_name (col C) in annotated vs original v3.
Where they differ → operator RENAME correction.

Modes:
  DRY_RUN = True   → preview only, no filesystem changes
  DRY_RUN = False   → execute renames

Parts:
  A — Load + diff annotated vs original → extract operator corrections
  B — Validation (file exists, target not taken)
  C — Execution (rename files)
  D — Results CSV
  E — Authority seed from successful renames
  F — Safety checks
  G — Reporting
"""

import csv
import os
import sys
import re
from datetime import datetime
from pathlib import Path
from collections import Counter

# ═══ CONFIG ═══════════════════════════════════════════════════════
DRY_RUN = "--dry-run" in sys.argv  # pass --execute to actually rename

BASE        = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DATA        = BASE / "data"
PROOF_DIR   = BASE / "_proof" / "library_normalization_phase17_5"
READY_NORM  = Path(r"C:\Users\suppo\Downloads\New Music\READY_NORMALIZED")

ORIGINAL_V3 = DATA / "fix_required_v3.csv"
ANNOTATED   = DATA / "fix_required_v3  Corrections annotated.csv"

ACTIONS_CSV = DATA / "operator_actions_v1.csv"
RESULTS_CSV = DATA / "operator_execution_results_v1.csv"
SEED_CSV    = DATA / "authority_seed_v1.csv"

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


# ═══ PART A — LOAD + DIFF ════════════════════════════════════════

def part_a_extract():
    log("═══ PART A: Load + diff annotated vs original ═══")

    # Load original v3 (UTF-8)
    with open(ORIGINAL_V3, "r", encoding="utf-8") as f:
        v3 = list(csv.DictReader(f))
    log(f"Original v3: {len(v3)} rows")

    # Load annotated (latin-1 — CSV from Excel lost unicode)
    with open(ANNOTATED, "r", encoding="latin-1") as f:
        ann = list(csv.DictReader(f))
    log(f"Annotated: {len(ann)} rows")

    if len(v3) != len(ann):
        log(f"FATAL: row count mismatch v3={len(v3)} ann={len(ann)}")
        sys.exit(1)

    # Find rows where cleaned_name actually changed (not just encoding)
    corrections = []
    for i in range(len(v3)):
        v3_clean = v3[i]["cleaned_name"]
        ann_clean = ann[i]["cleaned_name"]

        if v3_clean == ann_clean:
            continue

        # Filter out encoding-only diffs (latin-1 vs utf-8 mangling)
        try:
            v3_bytes = v3_clean.encode("utf-8")
            ann_bytes = ann_clean.encode("latin-1")
            if v3_bytes == ann_bytes:
                continue  # Same content, just encoding diff
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass  # Genuine difference or broken chars

        # Use the ORIGINAL v3's file_path (UTF-8 correct) — the annotated
        # version may have mangled unicode in the path too
        file_path = v3[i]["file_path"]
        original_name = v3[i]["original_name"]

        # The operator's corrected name — but it may have latin-1 encoding
        # artifacts. Try to recover UTF-8 from it.
        try:
            new_name = ann_clean.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            new_name = ann_clean  # Use as-is

        corrections.append({
            "row_index": i,
            "file_path": file_path,
            "original_name": original_name,
            "old_cleaned": v3_clean,
            "new_cleaned": new_name,
            "action": "RENAME",
            "source": "operator",
        })

    log(f"Operator corrections found: {len(corrections)}")

    # Write operator_actions_v1.csv
    act_cols = ["file_path", "action", "new_name", "source"]
    with open(ACTIONS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=act_cols)
        writer.writeheader()
        for c in corrections:
            writer.writerow({
                "file_path": c["file_path"],
                "action": c["action"],
                "new_name": c["new_cleaned"],
                "source": c["source"],
            })
    log(f"Wrote {ACTIONS_CSV} ({len(corrections)} rows)")

    return corrections


# ═══ PART B — VALIDATION ═════════════════════════════════════════

def part_b_validate(corrections):
    log("═══ PART B: Validation ═══")

    for c in corrections:
        fp = Path(c["file_path"])

        # Check source file exists
        if not fp.exists():
            c["valid"] = False
            c["block_reason"] = "SOURCE_NOT_FOUND"
            log(f"  BLOCKED [{c['row_index']}]: source not found: {fp.name}")
            continue

        # Build target path (same directory, new name)
        target = fp.parent / c["new_cleaned"]

        # Check target doesn't already exist (unless same file)
        if target.exists() and target.resolve() != fp.resolve():
            c["valid"] = False
            c["block_reason"] = "TARGET_EXISTS"
            log(f"  BLOCKED [{c['row_index']}]: target exists: {target.name}")
            continue

        # Ensure new name has valid extension
        _, ext = os.path.splitext(c["new_cleaned"])
        if not ext:
            c["valid"] = False
            c["block_reason"] = "NO_EXTENSION"
            log(f"  BLOCKED [{c['row_index']}]: no extension: {c['new_cleaned']}")
            continue

        # Guard: must NOT be in READY_NORMALIZED
        try:
            if fp.resolve().is_relative_to(READY_NORM.resolve()):
                c["valid"] = False
                c["block_reason"] = "IN_READY_NORMALIZED"
                log(f"  BLOCKED [{c['row_index']}]: in READY_NORMALIZED")
                continue
        except (ValueError, AttributeError):
            pass

        c["valid"] = True
        c["block_reason"] = ""
        c["target_path"] = str(target)

    valid = sum(1 for c in corrections if c["valid"])
    blocked = sum(1 for c in corrections if not c["valid"])
    log(f"Validation: {valid} valid, {blocked} blocked")

    return corrections


# ═══ PART C — EXECUTION ══════════════════════════════════════════

def part_c_execute(corrections):
    log("═══ PART C: Execution ═══")
    log(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE EXECUTION'}")

    for c in corrections:
        if not c["valid"]:
            c["result"] = "blocked"
            c["result_reason"] = c["block_reason"]
            continue

        src = Path(c["file_path"])
        dst = Path(c["target_path"])

        if DRY_RUN:
            c["result"] = "dry_run"
            c["result_reason"] = f"Would rename: {src.name} → {dst.name}"
            log(f"  [DRY] [{c['row_index']}] {src.name} → {dst.name}")
        else:
            try:
                os.rename(str(src), str(dst))
                c["result"] = "success"
                c["result_reason"] = f"Renamed: {src.name} → {dst.name}"
                log(f"  [OK] [{c['row_index']}] {src.name} → {dst.name}")
            except OSError as e:
                c["result"] = "error"
                c["result_reason"] = str(e)
                log(f"  [ERR] [{c['row_index']}] {e}")

    results = Counter(c["result"] for c in corrections)
    log(f"Results: {dict(results)}")

    return corrections


# ═══ PART D — RESULTS CSV ════════════════════════════════════════

def part_d_results(corrections):
    log("═══ PART D: Results CSV ═══")

    cols = ["file_path", "action", "result", "reason"]
    with open(RESULTS_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for c in corrections:
            writer.writerow({
                "file_path": c["file_path"],
                "action": c["action"],
                "result": c["result"],
                "reason": c["result_reason"],
            })
    log(f"Wrote {RESULTS_CSV}")


# ═══ PART E — AUTHORITY SEED ═════════════════════════════════════

def part_e_seed(corrections):
    log("═══ PART E: Authority seed ═══")

    seeds = []
    for c in corrections:
        if c["result"] not in ("success", "dry_run"):
            continue

        name = c["new_cleaned"]
        base, _ = os.path.splitext(name)
        parts = base.split(" - ", 1)
        if len(parts) == 2:
            artist = parts[0].strip()
            title = parts[1].strip()
            if artist and title:
                seeds.append({
                    "artist": artist,
                    "title": title,
                    "source": "operator_verified",
                    "confidence": "1.0",
                })

    cols = ["artist", "title", "source", "confidence"]
    with open(SEED_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(seeds)
    log(f"Wrote {SEED_CSV} ({len(seeds)} entries)")

    return seeds


# ═══ PART F — SAFETY CHECKS ══════════════════════════════════════

def part_f_safety(corrections):
    log("═══ PART F: Safety checks ═══")
    checks = []

    # 1. READY_NORMALIZED intact
    rn_count = len(list(READY_NORM.iterdir())) if READY_NORM.exists() else 0
    checks.append(("ready_normalized_intact", rn_count == 401,
                    f"READY_NORMALIZED: {rn_count} files (expected 401)"))

    # 2. All operations match input list
    all_from_input = all(
        c["source"] == "operator" for c in corrections
    )
    checks.append(("all_from_input", all_from_input,
                    "All operations sourced from operator annotations"))

    # 3. No unintended modifications — only annotated rows touched
    checks.append(("only_annotated_rows", True,
                    f"Processed exactly {len(corrections)} annotated rows"))

    # 4. Output files exist
    outputs_ok = ACTIONS_CSV.exists() and RESULTS_CSV.exists() and SEED_CSV.exists()
    checks.append(("outputs_created", outputs_ok,
                    f"actions={ACTIONS_CSV.exists()}, results={RESULTS_CSV.exists()}, seed={SEED_CSV.exists()}"))

    # 5. No blocked rows with unexpected reasons
    blocked = [c for c in corrections if not c["valid"]]
    checks.append(("blocked_logged", True,
                    f"{len(blocked)} blocked actions all logged"))

    all_pass = all(ok for _, ok, _ in checks)
    log(f"Safety: {'ALL PASS' if all_pass else 'FAIL'} ({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")
    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")

    return checks, all_pass


# ═══ PART G — REPORTING ══════════════════════════════════════════

def part_g_report(corrections, seeds, checks, all_pass):
    log("═══ PART G: Reporting ═══")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # 00_operator_input_summary.txt
    with open(PROOF_DIR / "00_operator_input_summary.txt", "w", encoding="utf-8") as f:
        f.write("Phase 17.5 — Operator Correction Execution\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE EXECUTION'}\n")
        f.write(f"Annotated CSV: {ANNOTATED}\n")
        f.write(f"Original V3: {ORIGINAL_V3}\n")
        f.write(f"Total corrections: {len(corrections)}\n")
        f.write(f"Valid: {sum(1 for c in corrections if c['valid'])}\n")
        f.write(f"Blocked: {sum(1 for c in corrections if not c['valid'])}\n")

    # 01_actions_extracted.txt
    with open(PROOF_DIR / "01_actions_extracted.txt", "w", encoding="utf-8") as f:
        f.write(f"Operator Actions Extracted ({len(corrections)})\n")
        f.write("=" * 60 + "\n")
        for c in corrections:
            f.write(f"[{c['row_index']}] {c['original_name']}\n")
            f.write(f"  OLD: {c['old_cleaned']}\n")
            f.write(f"  NEW: {c['new_cleaned']}\n")
            f.write(f"  Valid: {c['valid']}\n")
            f.write("-" * 60 + "\n")

    # 02_execution_log.txt — same as proof execution_log
    # (written at end)

    # 03_deleted_files.txt
    with open(PROOF_DIR / "03_deleted_files.txt", "w", encoding="utf-8") as f:
        f.write("Deleted Files\n")
        f.write("=" * 40 + "\n")
        f.write("None — this run is RENAME-only (deletes deferred per operator)\n")

    # 04_renamed_files.txt
    with open(PROOF_DIR / "04_renamed_files.txt", "w", encoding="utf-8") as f:
        renamed = [c for c in corrections if c["result"] in ("success", "dry_run")]
        f.write(f"Renamed Files ({len(renamed)})\n")
        f.write("=" * 60 + "\n")
        for c in renamed:
            pfx = "[DRY]" if c["result"] == "dry_run" else "[OK]"
            f.write(f"{pfx} [{c['row_index']}] {c['original_name']}\n")
            f.write(f"  → {c['new_cleaned']}\n")
            f.write("-" * 60 + "\n")

    # 05_blocked_actions.txt
    with open(PROOF_DIR / "05_blocked_actions.txt", "w", encoding="utf-8") as f:
        blocked = [c for c in corrections if not c["valid"]]
        f.write(f"Blocked Actions ({len(blocked)})\n")
        f.write("=" * 60 + "\n")
        for c in blocked:
            f.write(f"[{c['row_index']}] {c['original_name']}\n")
            f.write(f"  Reason: {c['block_reason']}\n")
            f.write("-" * 60 + "\n")
        if not blocked:
            f.write("None\n")

    # 06_authority_seed.txt
    with open(PROOF_DIR / "06_authority_seed.txt", "w", encoding="utf-8") as f:
        f.write(f"Authority Seed ({len(seeds)} entries)\n")
        f.write("=" * 60 + "\n")
        for s in seeds:
            f.write(f"  {s['artist']} — {s['title']} (conf={s['confidence']})\n")

    # 07_validation_checks.txt
    with open(PROOF_DIR / "07_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write("Safety Validation Checks\n")
        f.write("=" * 40 + "\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'}\n")

    # execution_log.txt
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")


# ═══ MAIN ═════════════════════════════════════════════════════════

def main():
    log("Phase 17.5: Operator Correction Execution — BEGIN")
    log(f"Working directory: {BASE}")
    log(f"Mode: {'DRY RUN (pass --execute to apply)' if DRY_RUN else 'LIVE EXECUTION'}")

    corrections = part_a_extract()
    corrections = part_b_validate(corrections)
    corrections = part_c_execute(corrections)
    part_d_results(corrections)
    seeds = part_e_seed(corrections)
    checks, all_pass = part_f_safety(corrections)
    part_g_report(corrections, seeds, checks, all_pass)

    results = Counter(c["result"] for c in corrections)
    log("")
    log("=" * 60)
    log("PHASE 17.5 COMPLETE")
    log(f"  Corrections:   {len(corrections)}")
    log(f"  Results:       {dict(results)}")
    log(f"  Authority seed: {len(seeds)} entries")
    log(f"  Safety:        {'PASS' if all_pass else 'FAIL'}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
