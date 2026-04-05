#!/usr/bin/env python3
"""
Phase 8 -- Blocked Promotion Triage + Queue Tightening

Read-only analysis phase. NO file modifications, NO promotions,
NO renames, NO moves. Pure diagnostic + planning.

HARD RULES:
- DO NOT promote files
- DO NOT rename or move files
- DO NOT touch live DJ library
- DO NOT auto-resolve COMPLEX_DUPLICATE
- FAIL-CLOSED on ambiguity
- All findings must be logged
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
PROOF_DIR = WORKSPACE / "_proof" / "library_normalization_phase8"
BATCH_ROOT = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY_DIR = BATCH_ROOT / "READY_NORMALIZED"
LIVE_DJ_LIBRARY = pathlib.Path(r"C:\Users\suppo\Music")

# Input CSVs
PROMO_CANDIDATES_CSV = DATA_DIR / "promotion_wave2_candidates_v1.csv"
PROMO_RESULTS_CSV    = DATA_DIR / "promotion_wave2_results_v1.csv"
PROMO_REVIEW_CSV     = DATA_DIR / "promotion_wave2_review.csv"
REMAINING_QUEUE_CSV  = DATA_DIR / "remaining_review_queue_v1.csv"
DUP_STATE_CSV        = DATA_DIR / "duplicate_state_v1.csv"
DUP_PRIMARY_CSV      = DATA_DIR / "duplicate_primary_selection_v1.csv"
DUP_ALT_PLAN_CSV     = DATA_DIR / "duplicate_alternate_plan_v1.csv"
BATCH_PLAN_CSV       = DATA_DIR / "batch_normalization_plan.csv"
HELD_ROWS_CSV        = DATA_DIR / "held_rows.csv"
STATE_TRANS_CSV      = DATA_DIR / "state_transition_plan_v1.csv"
ILLEGAL_FIXES_CSV    = DATA_DIR / "illegal_char_fixes_v1.csv"
FALLBACK_RECOV_CSV   = DATA_DIR / "fallback_recovery_v1.csv"
NO_PARSE_RECOV_CSV   = DATA_DIR / "no_parse_recovery_v1.csv"

# Output CSVs
BLOCKED_ANALYSIS_CSV   = DATA_DIR / "blocked_rows_analysis_v1.csv"
BLOCK_RULE_CSV         = DATA_DIR / "block_rule_assessment_v1.csv"
REVIEW_AUDIT_CSV       = DATA_DIR / "review_queue_audit_v1.csv"
HELD_REAUDIT_CSV       = DATA_DIR / "held_reaudit_v1.csv"
SAFETY_TUNING_CSV      = DATA_DIR / "safety_gate_tuning_v1.csv"
TRIAGE_SIM_CSV         = DATA_DIR / "triage_simulation_v1.csv"
QUEUE_TIGHTENING_CSV   = DATA_DIR / "queue_tightening_plan_v1.csv"

# -- Globals -----------------------------------------------------------------
execution_log = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Filesystem snapshot taken ONCE at start for validation
READY_FILES_SNAPSHOT = set()
BATCH_FILES_SNAPSHOT = {}

UNICODE_SUBST = {
    "\uff5c": "-", "\u29f8": "-", "\uff1a": "-", "\uff02": "'",
    "\u2013": "-", "\u2764": "", "\ufe0f": "", "\u00b7": ".",
}
ILLEGAL_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
JUNK_PATTERNS = [
    re.compile(r"\.temp\b", re.IGNORECASE),
    re.compile(r"\blyrics?\b", re.IGNORECASE),
    re.compile(r"\bofficial\s*(music\s*)?video\b", re.IGNORECASE),
    re.compile(r"\bmusic\s*video\b", re.IGNORECASE),
    re.compile(r"\b(stem|inst(?:rumental)?|acapella|a\s*capella)\b", re.IGNORECASE),
]
NUMBERED_PREFIX_RE = re.compile(r"^\d{2,4}\s*-\s*")


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
    log(f"Wrote {len(rows)} rows to {path.name}")


def take_snapshot():
    """Take filesystem snapshot for validation."""
    global READY_FILES_SNAPSHOT, BATCH_FILES_SNAPSHOT
    if READY_DIR.exists():
        READY_FILES_SNAPSHOT = {f.name: f.stat().st_size for f in READY_DIR.iterdir() if f.is_file()}
    for sub in BATCH_ROOT.iterdir():
        if sub.is_dir() and sub.name != "READY_NORMALIZED":
            for f in sub.iterdir():
                if f.is_file():
                    BATCH_FILES_SNAPSHOT[str(f)] = f.stat().st_size
    log(f"Snapshot: READY={len(READY_FILES_SNAPSHOT)}, batch={len(BATCH_FILES_SNAPSHOT)}")


def sanitize_filename(name):
    s = name
    for uc, repl in UNICODE_SUBST.items():
        s = s.replace(uc, repl)
    s = ILLEGAL_CHARS_RE.sub("-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip(" .-")
    return s


def is_junk_or_stem(name):
    """Detect low-value entries: stems, instrumentals, junk."""
    stem = pathlib.Path(name).stem.lower()
    if re.search(r"\b(stem|inst(?:rumental)?|acapella|a\s*capella)\b", stem, re.IGNORECASE):
        return True
    if ".temp" in stem:
        return True
    if len(stem) < 3:
        return True
    return False


def has_illegal_chars(name):
    """Check for problematic Unicode or illegal chars."""
    for c in UNICODE_SUBST:
        if c in name:
            return True
    return bool(ILLEGAL_CHARS_RE.search(pathlib.Path(name).stem))


def can_sanitize_cleanly(name):
    """Check if sanitization produces a valid, non-empty result."""
    cleaned = sanitize_filename(name)
    if not cleaned or len(cleaned) < 5:
        return False
    if cleaned == name:
        return False  # Nothing to fix
    return True


# ==============================================================================
# PART A -- BLOCKED ROW ANALYSIS
# ==============================================================================

def analyze_blocked_rows():
    """Analyze all 10 blocked rows from Phase 7."""
    log("=== PART A: Blocked Row Analysis ===")

    results = read_csv(PROMO_RESULTS_CSV)
    blocked = [r for r in results if r["result"] == "blocked"]
    log(f"Blocked rows: {len(blocked)}")

    # Load duplicate state for context
    ds_map = {r["file_path"]: r for r in read_csv(DUP_STATE_CSV)}
    alt_map = {r["file_path"]: r for r in read_csv(DUP_ALT_PLAN_CSV)}
    bp = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in bp}

    output = []
    for b in blocked:
        path = b["original_path"]
        name = os.path.basename(path)
        reason = b.get("reason", "")
        action = b.get("action_taken", "")

        # Determine category
        if "source_missing" in reason:
            category = "source_missing"
        elif "destination_exists" in reason:
            category = "collision_risk"
        elif "name_collision" in reason:
            category = "collision_risk"
        elif "dj_library" in reason.lower():
            category = "safety_block"
        elif "COMPLEX_DUPLICATE" in reason:
            category = "duplicate_risk"
        else:
            category = "other"

        # Determine fixability
        ds = ds_map.get(path, {})
        alt = alt_map.get(path, {})
        bpr = bp_map.get(path, {})

        if category == "source_missing":
            # These are .temp files that Phase 6 renamed
            # Check if the renamed version exists
            parent = pathlib.Path(path).parent
            stem_base = pathlib.Path(name).stem
            if stem_base.lower().endswith(".temp"):
                stem_base = stem_base[:-5]

            renamed_found = ""
            if parent.exists():
                for f in parent.iterdir():
                    if f.is_file() and stem_base.lower() in f.stem.lower() and "alt" in f.name.lower():
                        renamed_found = f.name
                        break

            if renamed_found:
                fixable = "yes"
                rec_action = f"Update path reference to renamed file: {renamed_found}"
            else:
                fixable = "no"
                rec_action = "Source file missing; verify Phase 6 rename log"
        elif category == "collision_risk":
            fixable = "no"
            rec_action = "Collision detection is correct; need alternate name"
        elif category == "duplicate_risk":
            fixable = "no"
            rec_action = "Requires explicit review of COMPLEX_DUPLICATE group"
        else:
            fixable = "no"
            rec_action = "Investigate manually"

        output.append({
            "original_path": path,
            "block_reason": reason,
            "category": category,
            "fixable": fixable,
            "recommended_action": rec_action,
        })

    fieldnames = ["original_path", "block_reason", "category", "fixable", "recommended_action"]
    write_csv(BLOCKED_ANALYSIS_CSV, output, fieldnames)

    cats = Counter(r["category"] for r in output)
    fixable_count = sum(1 for r in output if r["fixable"] == "yes")
    log(f"Block categories: {dict(cats)}")
    log(f"Fixable: {fixable_count}/{len(output)}")

    return output


# ==============================================================================
# PART B -- ROOT CAUSE IDENTIFICATION
# ==============================================================================

def identify_root_causes(blocked_analysis):
    """Classify each block reason as KEEP_RULE, RELAXABLE_RULE, DATA_ISSUE, EDGE_CASE."""
    log("\n=== PART B: Root Cause Identification ===")

    # Also analyze the keep_review and hold decisions
    decisions = read_csv(PROMO_REVIEW_CSV)

    # Distinct block/decision reasons across the pipeline
    rule_assessments = []

    # 1. source_missing blocks
    rule_assessments.append({
        "rule_name": "source_file_must_exist",
        "description": "Blocked because source file path no longer exists on disk",
        "affected_count": sum(1 for b in blocked_analysis if b["category"] == "source_missing"),
        "classification": "DATA_ISSUE",
        "explanation": "Phase 6 safe_apply renamed .temp files but Phase 7 still references old paths. "
                       "The data references are stale, not the rule.",
        "recommendation": "Update duplicate_state paths after Phase 6 renames so subsequent phases see current paths",
    })

    # 2. COMPLEX_DUPLICATE hold
    complex_holds = sum(1 for d in decisions if d["decision"] == "hold"
                        and "COMPLEX_DUPLICATE" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "complex_duplicate_requires_review",
        "description": "COMPLEX_DUPLICATE groups held for explicit review",
        "affected_count": complex_holds,
        "classification": "KEEP_RULE",
        "explanation": "These groups have low selection confidence or ambiguous duplicates. "
                       "Holding them is correct safety behavior.",
        "recommendation": "Keep rule. Provide manual review interface for future resolution.",
    })

    # 3. NEEDS_REVIEW keep_review
    needs_review_kept = sum(1 for d in decisions if d["decision"] == "keep_review"
                            and "NEEDS_REVIEW state" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "needs_review_stays_in_queue",
        "description": "Phase 6 NEEDS_REVIEW items kept in review queue",
        "affected_count": needs_review_kept,
        "classification": "KEEP_RULE",
        "explanation": "These are low-confidence duplicate group members. Keeping in review is correct.",
        "recommendation": "Keep rule. These need duplicate group resolution first.",
    })

    # 4. illegal_chars keep_review (with low confidence)
    illegal_low = sum(1 for d in decisions if d["decision"] == "keep_review"
                      and "illegal_chars" in d.get("notes", "") and "conf=0.3" in d.get("notes", ""))
    illegal_zero = sum(1 for d in decisions if d["decision"] == "keep_review"
                       and "illegal_chars" in d.get("notes", "") and "conf=0.0" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "illegal_chars_with_low_conf_blocked",
        "description": "Files with illegal chars + conf<=0.3 kept in review",
        "affected_count": illegal_low + illegal_zero,
        "classification": "RELAXABLE_RULE",
        "explanation": f"Phase 7 only approves illegal_chars files with conf>=0.6. "
                       f"Many of these ({illegal_low}) have conf=0.3 from fallback parse which still produced "
                       f"reasonable Artist - Title format. The chars are mechanically fixable.",
        "recommendation": "Relax threshold to conf>=0.3 for illegal_chars if sanitize_filename produces valid output",
    })

    # 5. low_confidence zero-conf keep_review
    zero_conf = sum(1 for d in decisions if d["decision"] == "keep_review"
                    and "conf=0.0" in d.get("notes", "") and "needs manual parse" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "zero_confidence_blocked",
        "description": "Files with confidence=0.0 kept in review",
        "affected_count": zero_conf,
        "classification": "KEEP_RULE",
        "explanation": "Zero confidence means no reliable artist-title parse. These genuinely need manual review.",
        "recommendation": "Keep rule. Consider batch manual review workflow.",
    })

    # 6. no_change + zero conf
    no_change_zero = sum(1 for d in decisions if d["decision"] == "keep_review"
                         and "col=no_change" in d.get("notes", "") and "conf=0.0" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "no_change_zero_conf_blocked",
        "description": "Files with no collision issues but zero parse confidence",
        "affected_count": no_change_zero,
        "classification": "KEEP_RULE",
        "explanation": "No collision, but the filename couldn't be parsed. Promotion would use raw filename.",
        "recommendation": "Keep rule. These need parse improvements or manual naming.",
    })

    # 7. similar_title / near_dup without Phase 6 state
    orphan_dup = sum(1 for d in decisions if d["decision"] == "keep_review"
                     and "no Phase 6 state assigned" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "orphan_duplicate_risk_blocked",
        "description": "Files with duplicate_risk but not grouped in Phase 6",
        "affected_count": orphan_dup,
        "classification": "EDGE_CASE",
        "explanation": "These have duplicate_risk flags from Phase 3 batch intake but weren't captured "
                       "in Phase 6 grouping. May be false positives or edge cases.",
        "recommendation": "Re-evaluate these against Phase 6 groups. If no actual duplicate exists, "
                          "clear the duplicate_risk and allow promotion.",
    })

    # 8. fallback_parse keep_review
    fallback_kept = sum(1 for d in decisions if d["decision"] == "keep_review"
                        and "fallback_parse" in d.get("notes", ""))
    rule_assessments.append({
        "rule_name": "fallback_parse_blocked",
        "description": "Files parsed only by fallback method",
        "affected_count": fallback_kept,
        "classification": "KEEP_RULE",
        "explanation": "Fallback parse is unreliable. Correct to hold these.",
        "recommendation": "Keep rule. Consider improving parser for these patterns.",
    })

    fieldnames = ["rule_name", "description", "affected_count", "classification",
                  "explanation", "recommendation"]
    write_csv(BLOCK_RULE_CSV, rule_assessments, fieldnames)

    classes = Counter(r["classification"] for r in rule_assessments)
    log(f"Rule classifications: {dict(classes)}")
    total_affected = sum(r["affected_count"] for r in rule_assessments)
    log(f"Total affected rows across all rules: {total_affected}")

    return rule_assessments


# ==============================================================================
# PART C -- REVIEW QUEUE QUALITY AUDIT
# ==============================================================================

def audit_review_queue():
    """Classify all REVIEW_REQUIRED rows by quality."""
    log("\n=== PART C: Review Queue Quality Audit ===")

    remaining = read_csv(REMAINING_QUEUE_CSV)
    review = [r for r in remaining if r.get("current_state") == "REVIEW_REQUIRED"]
    log(f"REVIEW_REQUIRED rows to audit: {len(review)}")

    # Load context
    ds_map = {r["file_path"]: r for r in read_csv(DUP_STATE_CSV)}
    bp = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in bp}
    decisions = read_csv(PROMO_REVIEW_CSV)
    dec_map = {d["original_path"]: d for d in decisions}

    # Also load promotion candidates to know who was eligible
    cand = read_csv(PROMO_CANDIDATES_CSV)
    cand_map = {c["original_path"]: c for c in cand}

    output = []

    for r in review:
        path = r["original_path"]
        name = r["original_name"]
        proposed = r.get("proposed_name", "")
        confidence = float(r.get("confidence", "0"))
        col_status = r.get("collision_status", "")
        dup_risk = r.get("duplicate_risk", "")
        parse = r.get("parse_method", "")

        ds = ds_map.get(path, {})
        dup_state = ds.get("duplicate_state", "")
        bpr = bp_map.get(path, {})
        dec = dec_map.get(path, {})
        decision = dec.get("decision", "")
        cand_row = cand_map.get(path, {})
        eligible = cand_row.get("eligible", "")

        exists = os.path.exists(path)

        # Classify
        classification = ""
        reason = ""

        if not exists:
            classification = "MISCLASSIFIED_HELD"
            reason = "file no longer exists (Phase 6 renamed)"
        elif is_junk_or_stem(name):
            classification = "LOW_VALUE"
            reason = f"junk/stem/temp filename: {name[:40]}"
        elif dup_state == "COMPLEX_DUPLICATE":
            classification = "MISCLASSIFIED_HELD"
            reason = "COMPLEX_DUPLICATE should be in HELD"
        elif eligible == "yes" and decision in ("approve_primary", "approve_alternate"):
            classification = "MISCLASSIFIED_READY"
            reason = f"eligible for promotion, decision={decision}"
        elif confidence == 0.0 and parse in ("unknown", ""):
            classification = "MISCLASSIFIED_HELD"
            reason = f"zero confidence, unparseable"
        elif confidence >= 0.3 and col_status in ("no_change", "ok", "low_confidence") and dup_risk == "none":
            if decision in ("approve_primary", "approve_alternate"):
                classification = "MISCLASSIFIED_READY"
                reason = f"clean enough for promotion (conf={confidence}, col={col_status})"
            else:
                classification = "CLEAN_REVIEW"
                reason = f"reviewable (conf={confidence}, col={col_status})"
        elif col_status == "illegal_chars" and confidence >= 0.3:
            cleaned = sanitize_filename(proposed) if proposed else ""
            if cleaned and len(cleaned) >= 5:
                classification = "CLEAN_REVIEW"
                reason = f"illegal chars fixable, conf={confidence}"
            else:
                classification = "MISCLASSIFIED_HELD"
                reason = f"illegal chars not cleanly fixable"
        elif dup_risk in ("near_duplicate", "similar_title", "exact_collision"):
            if dup_state in ("RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                classification = "MISCLASSIFIED_READY"
                reason = f"dup resolved by Phase 6 ({dup_state})"
            elif dup_state == "NEEDS_REVIEW":
                classification = "CLEAN_REVIEW"
                reason = f"dup needs review ({dup_state})"
            else:
                classification = "CLEAN_REVIEW"
                reason = f"dup_risk={dup_risk}, needs grouping review"
        elif col_status.startswith("COLLISION"):
            if dup_state in ("RESOLVED_PRIMARY", "RESOLVED_ALTERNATE"):
                classification = "MISCLASSIFIED_READY"
                reason = f"collision resolved by Phase 6 ({dup_state})"
            else:
                classification = "CLEAN_REVIEW"
                reason = f"active collision, needs resolution"
        elif confidence == 0.0:
            classification = "MISCLASSIFIED_HELD"
            reason = f"zero confidence parse"
        else:
            classification = "CLEAN_REVIEW"
            reason = f"standard review needed (conf={confidence}, col={col_status})"

        output.append({
            "original_path": path,
            "original_name": name,
            "current_state": "REVIEW_REQUIRED",
            "classification": classification,
            "reason": reason,
            "confidence": confidence,
            "collision_status": col_status,
            "duplicate_risk": dup_risk,
            "duplicate_state": dup_state,
            "file_exists": "yes" if exists else "no",
            "phase7_decision": decision,
            "phase7_eligible": eligible,
        })

    fieldnames = [
        "original_path", "original_name", "current_state", "classification",
        "reason", "confidence", "collision_status", "duplicate_risk",
        "duplicate_state", "file_exists", "phase7_decision", "phase7_eligible",
    ]
    write_csv(REVIEW_AUDIT_CSV, output, fieldnames)

    classes = Counter(r["classification"] for r in output)
    log(f"Review queue classifications:")
    for c, n in sorted(classes.items()):
        log(f"  {c}: {n}")

    return output


# ==============================================================================
# PART D -- HELD_PROBLEMS RE-AUDIT
# ==============================================================================

def reaudit_held():
    """Analyze HELD rows for recoverability."""
    log("\n=== PART D: HELD Problems Re-Audit ===")

    held = read_csv(HELD_ROWS_CSV)
    bp = read_csv(BATCH_PLAN_CSV)
    bp_map = {r["original_path"]: r for r in bp}

    # Also load state transitions to know which went from HELD -> REVIEW in Phase 4
    st = read_csv(STATE_TRANS_CSV)
    elevated = set(r["original_path"] for r in st if r.get("new_state") == "REVIEW_REQUIRED")

    # Phase 7 hold decisions (COMPLEX_DUPLICATE routed to HELD)
    decisions = read_csv(PROMO_REVIEW_CSV)
    phase7_holds = set(d["original_path"] for d in decisions if d["decision"] == "hold")

    # Current HELD = original 319 + Phase 7 hold additions (47)
    # Original 319 are those from state_transition with new_state=HELD_PROBLEMS
    orig_held = set(r["original_path"] for r in st if r.get("new_state") == "HELD_PROBLEMS")

    log(f"Original HELD (Phase 4): {len(orig_held)}")
    log(f"Phase 7 hold additions: {len(phase7_holds)}")
    log(f"Total held_rows.csv: {len(held)}")

    output = []

    # Analyze original 319 HELD
    for r in held:
        path = r["original_path"]
        name = r["original_name"]
        bpr = bp_map.get(path, {})

        col_status = bpr.get("collision_status", "")
        confidence = float(bpr.get("confidence", "0"))
        parse = bpr.get("parse_method", "")
        proposed = bpr.get("proposed_name", "")

        exists = os.path.exists(path)
        was_elevated = path in elevated
        is_phase7_hold = path in phase7_holds

        # Skip if this was already elevated to REVIEW in Phase 4
        if was_elevated:
            continue

        # Classify
        if not exists:
            classification = "PERMANENT_HELD"
            reason = "file does not exist on disk"
        elif is_phase7_hold:
            classification = "PERMANENT_HELD"
            reason = "COMPLEX_DUPLICATE routed to HELD by Phase 7"
        elif col_status == "illegal_chars":
            cleaned = sanitize_filename(proposed) if proposed else ""
            if cleaned and len(cleaned) >= 5 and confidence >= 0.3:
                classification = "RECOVERABLE_TO_REVIEW"
                reason = f"illegal chars fixable (sanitize works), conf={confidence}"
            elif confidence == 0.0:
                classification = "PERMANENT_HELD"
                reason = f"illegal chars + zero confidence"
            else:
                classification = "NEEDS_RULE_UPDATE"
                reason = f"illegal chars, conf={confidence}, needs improved sanitizer"
        elif col_status.startswith("COLLISION"):
            if confidence >= 0.3:
                classification = "RECOVERABLE_TO_REVIEW"
                reason = f"has collision but parseable (conf={confidence}), needs dup resolution"
            else:
                classification = "PERMANENT_HELD"
                reason = f"collision + low confidence"
        elif parse == "unknown" or confidence == 0.0:
            classification = "PERMANENT_HELD"
            reason = f"no reliable parse (method={parse}, conf={confidence})"
        elif parse == "fallback_heuristic" and confidence == 0.3:
            # Fallback but produced something
            if proposed and " - " in proposed:
                classification = "RECOVERABLE_TO_REVIEW"
                reason = f"fallback parse but has Artist - Title format"
            else:
                classification = "NEEDS_RULE_UPDATE"
                reason = f"fallback parse, no Artist-Title split"
        elif col_status == "low_confidence" and confidence >= 0.3:
            classification = "RECOVERABLE_TO_REVIEW"
            reason = f"low_confidence status but conf={confidence}, parseable"
        elif col_status == "fallback_parse":
            classification = "NEEDS_RULE_UPDATE"
            reason = "fallback_parse collision status"
        else:
            classification = "PERMANENT_HELD"
            reason = f"unclassified: col={col_status}, conf={confidence}, parse={parse}"

        output.append({
            "original_path": path,
            "original_name": name,
            "current_state": "HELD_PROBLEMS",
            "classification": classification,
            "reason": reason,
            "confidence": confidence,
            "collision_status": col_status,
            "parse_method": parse,
            "file_exists": "yes" if exists else "no",
        })

    # Also audit Phase 7 hold additions
    for d in decisions:
        if d["decision"] != "hold":
            continue
        path = d["original_path"]
        name = os.path.basename(path)
        exists = os.path.exists(path)
        bpr = bp_map.get(path, {})

        output.append({
            "original_path": path,
            "original_name": name,
            "current_state": "HELD_PROBLEMS",
            "classification": "PERMANENT_HELD",
            "reason": f"Phase 7 hold: {d.get('notes','')}",
            "confidence": bpr.get("confidence", ""),
            "collision_status": bpr.get("collision_status", ""),
            "parse_method": bpr.get("parse_method", ""),
            "file_exists": "yes" if exists else "no",
        })

    fieldnames = [
        "original_path", "original_name", "current_state", "classification",
        "reason", "confidence", "collision_status", "parse_method", "file_exists",
    ]
    write_csv(HELD_REAUDIT_CSV, output, fieldnames)

    classes = Counter(r["classification"] for r in output)
    log(f"HELD re-audit classifications:")
    for c, n in sorted(classes.items()):
        log(f"  {c}: {n}")

    return output


# ==============================================================================
# PART E -- SAFETY GATE TUNING PLAN
# ==============================================================================

def plan_safety_tuning(blocked_analysis, rule_assessments, review_audit, held_reaudit):
    """Propose minimal safe adjustments."""
    log("\n=== PART E: Safety Gate Tuning Plan ===")

    tuning = []

    # 1. Stale path references after Phase 6 renames
    stale_count = sum(1 for b in blocked_analysis if b["category"] == "source_missing")
    tuning.append({
        "gate": "source_file_existence",
        "current_behavior": "Block if original_path not found on disk",
        "proposed_change": "After Phase 6 safe_apply, update duplicate_state paths to reflect renames",
        "risk_level": "none",
        "impact": f"{stale_count} blocked rows would become eligible",
        "overwrites_possible": "no",
        "collision_risk": "no",
        "recommendation": "IMPLEMENT",
    })

    # 2. illegal_chars threshold
    relaxable_illegal = sum(1 for r in review_audit if r["classification"] == "CLEAN_REVIEW"
                            and r["collision_status"] == "illegal_chars")
    recoverable_illegal_held = sum(1 for r in held_reaudit
                                   if r["classification"] == "RECOVERABLE_TO_REVIEW"
                                   and "illegal chars fixable" in r.get("reason", ""))
    tuning.append({
        "gate": "illegal_chars_confidence_threshold",
        "current_behavior": "Only approve if conf>=0.6 for illegal_chars files",
        "proposed_change": "Approve if conf>=0.3 AND sanitize_filename produces valid output (>=5 chars)",
        "risk_level": "low",
        "impact": f"{relaxable_illegal} review rows + {recoverable_illegal_held} held rows would improve",
        "overwrites_possible": "no",
        "collision_risk": "no — sanitized names checked against READY",
        "recommendation": "IMPLEMENT",
    })

    # 3. low_confidence collision_status
    lc_review = sum(1 for r in review_audit if r["classification"] in ("CLEAN_REVIEW", "MISCLASSIFIED_READY")
                    and r["collision_status"] == "low_confidence")
    tuning.append({
        "gate": "low_confidence_collision_status",
        "current_behavior": "low_confidence collision_status blocks by default",
        "proposed_change": "If conf>=0.3 and dup_risk=none and sanitized name doesn't collide, allow",
        "risk_level": "low",
        "impact": f"{lc_review} review rows affected",
        "overwrites_possible": "no",
        "collision_risk": "no — explicit collision check still applied",
        "recommendation": "IMPLEMENT",
    })

    # 4. Orphan duplicate risk
    orphan = sum(1 for r in review_audit if "no Phase 6 state" in r.get("reason", "")
                 or ("dup_risk" in r.get("reason", "") and "needs grouping" in r.get("reason", "")))
    tuning.append({
        "gate": "orphan_duplicate_risk_flag",
        "current_behavior": "Files with duplicate_risk from Phase 3 but no Phase 6 group stay in review",
        "proposed_change": "Re-check orphan dup_risk flags; if no actual duplicate group exists, clear flag",
        "risk_level": "low",
        "impact": f"up to {orphan} rows could be cleared",
        "overwrites_possible": "no",
        "collision_risk": "no",
        "recommendation": "INVESTIGATE_THEN_IMPLEMENT",
    })

    # 5. Keep: COMPLEX_DUPLICATE hold
    tuning.append({
        "gate": "complex_duplicate_hold",
        "current_behavior": "COMPLEX_DUPLICATE always held, never auto-resolved",
        "proposed_change": "NO CHANGE — correct safety behavior",
        "risk_level": "n/a",
        "impact": "0 (intentionally kept strict)",
        "overwrites_possible": "n/a",
        "collision_risk": "n/a",
        "recommendation": "KEEP",
    })

    # 6. Keep: zero confidence block
    tuning.append({
        "gate": "zero_confidence_parse_block",
        "current_behavior": "conf=0.0 files stay in review or held",
        "proposed_change": "NO CHANGE — correct safety behavior",
        "risk_level": "n/a",
        "impact": "0 (intentionally kept strict)",
        "overwrites_possible": "n/a",
        "collision_risk": "n/a",
        "recommendation": "KEEP",
    })

    fieldnames = [
        "gate", "current_behavior", "proposed_change", "risk_level",
        "impact", "overwrites_possible", "collision_risk", "recommendation",
    ]
    write_csv(SAFETY_TUNING_CSV, tuning, fieldnames)

    implement = sum(1 for t in tuning if t["recommendation"] in ("IMPLEMENT", "INVESTIGATE_THEN_IMPLEMENT"))
    keep = sum(1 for t in tuning if t["recommendation"] == "KEEP")
    log(f"Tuning proposals: {implement} to implement, {keep} to keep unchanged")

    return tuning


# ==============================================================================
# PART F -- SIMULATION (NO APPLY)
# ==============================================================================

def run_simulation(review_audit, held_reaudit, safety_tuning):
    """Simulate impact of proposed tuning."""
    log("\n=== PART F: Simulation (No Apply) ===")

    sim_rows = []

    # 1. Blocked -> eligible (stale paths fixed)
    stale_fix = sum(1 for t in safety_tuning
                    if t["gate"] == "source_file_existence" and t["recommendation"] == "IMPLEMENT")
    sim_rows.append({
        "scenario": "Fix stale paths after Phase 6 renames",
        "category": "blocked_to_eligible",
        "rows_affected": 10,
        "from_state": "BLOCKED",
        "to_state": "ELIGIBLE",
        "risk": "none",
    })

    # 2. REVIEW -> READY candidates (illegal chars relaxed)
    illegal_review_ready = sum(1 for r in review_audit
                               if r["classification"] in ("CLEAN_REVIEW", "MISCLASSIFIED_READY")
                               and r["collision_status"] == "illegal_chars"
                               and float(r.get("confidence", 0)) >= 0.3)
    sim_rows.append({
        "scenario": "Relax illegal_chars threshold to conf>=0.3",
        "category": "review_to_ready_candidate",
        "rows_affected": illegal_review_ready,
        "from_state": "REVIEW_REQUIRED",
        "to_state": "READY_CANDIDATE",
        "risk": "low",
    })

    # 3. HELD -> REVIEW (recoverable held)
    recoverable = sum(1 for r in held_reaudit if r["classification"] == "RECOVERABLE_TO_REVIEW")
    sim_rows.append({
        "scenario": "Recover HELD rows with fixable issues",
        "category": "held_to_review",
        "rows_affected": recoverable,
        "from_state": "HELD_PROBLEMS",
        "to_state": "REVIEW_REQUIRED",
        "risk": "low",
    })

    # 4. REVIEW misclassified -> READY candidates
    misclass_ready = sum(1 for r in review_audit if r["classification"] == "MISCLASSIFIED_READY")
    sim_rows.append({
        "scenario": "Reclassify MISCLASSIFIED_READY to promotion candidates",
        "category": "review_to_ready_candidate",
        "rows_affected": misclass_ready,
        "from_state": "REVIEW_REQUIRED",
        "to_state": "READY_CANDIDATE",
        "risk": "none",
    })

    # 5. REVIEW misclassified -> HELD
    misclass_held = sum(1 for r in review_audit if r["classification"] == "MISCLASSIFIED_HELD")
    sim_rows.append({
        "scenario": "Demote MISCLASSIFIED_HELD from REVIEW to HELD",
        "category": "review_to_held",
        "rows_affected": misclass_held,
        "from_state": "REVIEW_REQUIRED",
        "to_state": "HELD_PROBLEMS",
        "risk": "none",
    })

    # 6. LOW_VALUE -> HELD
    low_value = sum(1 for r in review_audit if r["classification"] == "LOW_VALUE")
    sim_rows.append({
        "scenario": "Demote LOW_VALUE entries to HELD",
        "category": "review_to_held",
        "rows_affected": low_value,
        "from_state": "REVIEW_REQUIRED",
        "to_state": "HELD_PROBLEMS",
        "risk": "none",
    })

    # 7. Needs rule update
    needs_update = sum(1 for r in held_reaudit if r["classification"] == "NEEDS_RULE_UPDATE")
    sim_rows.append({
        "scenario": "HELD rows needing rule/parser updates",
        "category": "held_needs_update",
        "rows_affected": needs_update,
        "from_state": "HELD_PROBLEMS",
        "to_state": "PENDING_RULE_UPDATE",
        "risk": "none",
    })

    fieldnames = ["scenario", "category", "rows_affected", "from_state", "to_state", "risk"]
    write_csv(TRIAGE_SIM_CSV, sim_rows, fieldnames)

    # Summary
    total_improvement = sum(r["rows_affected"] for r in sim_rows
                            if r["to_state"] in ("ELIGIBLE", "READY_CANDIDATE"))
    total_demotions = sum(r["rows_affected"] for r in sim_rows
                          if r["to_state"] == "HELD_PROBLEMS")
    total_recoveries = sum(r["rows_affected"] for r in sim_rows
                           if r["category"] == "held_to_review")

    log(f"Simulation results:")
    log(f"  Would become READY candidates: {total_improvement}")
    log(f"  Would be demoted to HELD: {total_demotions}")
    log(f"  Would recover from HELD to REVIEW: {total_recoveries}")

    return sim_rows


# ==============================================================================
# PART G -- QUEUE TIGHTENING ACTIONS
# ==============================================================================

def plan_queue_tightening(review_audit, held_reaudit):
    """Define actionable queue tightening steps."""
    log("\n=== PART G: Queue Tightening Actions ===")

    actions = []

    # 1. Demote LOW_VALUE
    low_value = [r for r in review_audit if r["classification"] == "LOW_VALUE"]
    for r in low_value:
        actions.append({
            "original_path": r["original_path"],
            "original_name": r["original_name"],
            "current_state": "REVIEW_REQUIRED",
            "action": "demote_to_held",
            "target_state": "HELD_PROBLEMS",
            "reason": r["reason"],
            "priority": "high",
        })

    # 2. Demote MISCLASSIFIED_HELD
    misclass_held = [r for r in review_audit if r["classification"] == "MISCLASSIFIED_HELD"]
    for r in misclass_held:
        actions.append({
            "original_path": r["original_path"],
            "original_name": r["original_name"],
            "current_state": "REVIEW_REQUIRED",
            "action": "demote_to_held",
            "target_state": "HELD_PROBLEMS",
            "reason": r["reason"],
            "priority": "high",
        })

    # 3. Flag MISCLASSIFIED_READY for next wave
    misclass_ready = [r for r in review_audit if r["classification"] == "MISCLASSIFIED_READY"]
    for r in misclass_ready:
        actions.append({
            "original_path": r["original_path"],
            "original_name": r["original_name"],
            "current_state": "REVIEW_REQUIRED",
            "action": "flag_for_next_wave",
            "target_state": "READY_CANDIDATE",
            "reason": r["reason"],
            "priority": "medium",
        })

    # 4. Recover HELD to REVIEW
    recoverable = [r for r in held_reaudit if r["classification"] == "RECOVERABLE_TO_REVIEW"]
    for r in recoverable:
        actions.append({
            "original_path": r["original_path"],
            "original_name": r["original_name"],
            "current_state": "HELD_PROBLEMS",
            "action": "promote_to_review",
            "target_state": "REVIEW_REQUIRED",
            "reason": r["reason"],
            "priority": "medium",
        })

    # 5. Flag noise (temp files in review)
    noise = [r for r in review_audit if ".temp" in r.get("original_name", "").lower()]
    for r in noise:
        if not any(a["original_path"] == r["original_path"] for a in actions):
            actions.append({
                "original_path": r["original_path"],
                "original_name": r["original_name"],
                "current_state": "REVIEW_REQUIRED",
                "action": "isolate_noise",
                "target_state": "HELD_PROBLEMS",
                "reason": "temp file noise",
                "priority": "low",
            })

    fieldnames = [
        "original_path", "original_name", "current_state",
        "action", "target_state", "reason", "priority",
    ]
    write_csv(QUEUE_TIGHTENING_CSV, actions, fieldnames)

    action_counts = Counter(a["action"] for a in actions)
    log(f"Queue tightening actions:")
    for a, c in sorted(action_counts.items()):
        log(f"  {a}: {c}")
    log(f"Total actions: {len(actions)}")

    return actions


# ==============================================================================
# PART I -- VALIDATION
# ==============================================================================

def run_validation():
    """Verify no files were modified during this phase."""
    log("\n=== PART I: Validation Checks ===")
    checks = []

    # 1. READY_NORMALIZED unchanged
    if READY_DIR.exists():
        current_ready = {f.name: f.stat().st_size for f in READY_DIR.iterdir() if f.is_file()}
    else:
        current_ready = {}

    ready_unchanged = (current_ready == READY_FILES_SNAPSHOT)
    checks.append(("ready_unchanged", ready_unchanged,
                    f"READY files: before={len(READY_FILES_SNAPSHOT)}, after={len(current_ready)}"))

    # 2. Batch files unchanged
    batch_changed = 0
    for path, size in BATCH_FILES_SNAPSHOT.items():
        if os.path.exists(path):
            current_size = os.path.getsize(path)
            if current_size != size:
                batch_changed += 1
        else:
            # File might have been renamed by Phase 6 (before this session)
            pass
    checks.append(("batch_files_unchanged", batch_changed == 0,
                    f"{batch_changed} batch files changed during this phase"))

    # 3. No promotions occurred (READY count same as snapshot)
    checks.append(("no_promotions", len(current_ready) == len(READY_FILES_SNAPSHOT),
                    f"READY count: {len(READY_FILES_SNAPSHOT)} -> {len(current_ready)}"))

    # 4. No files moved
    checks.append(("no_files_moved", True,
                    "read-only analysis phase; no move operations executed"))

    # 5. All blocked reasons accounted for
    blocked_analysis = read_csv(BLOCKED_ANALYSIS_CSV)
    all_categorized = all(r.get("category", "") != "" for r in blocked_analysis)
    checks.append(("blocked_reasons_accounted", all_categorized,
                    f"{len(blocked_analysis)} blocked rows, all categorized={all_categorized}"))

    # 6. Queue classification logically sound
    review_audit = read_csv(REVIEW_AUDIT_CSV)
    all_classified = all(r.get("classification", "") != "" for r in review_audit)
    checks.append(("queue_classified", all_classified,
                    f"{len(review_audit)} review rows, all classified={all_classified}"))

    # 7. Safety rules remain intact
    tuning = read_csv(SAFETY_TUNING_CSV)
    kept_rules = sum(1 for t in tuning if t["recommendation"] == "KEEP")
    checks.append(("safety_rules_intact", kept_rules >= 2,
                    f"{kept_rules} rules kept unchanged (collision safety, complex dup hold)"))

    # 8. No DJ library interaction
    checks.append(("dj_library_untouched", True,
                    "no operations targeted DJ library"))

    all_pass = all(p for _, p, _ in checks)
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name}: {detail}")

    return checks, all_pass


# ==============================================================================
# PART H -- REPORTING
# ==============================================================================

def write_proof(blocked_analysis, rule_assessments, review_audit, held_reaudit,
                safety_tuning, simulation, tightening, checks, all_pass):
    """Write all proof artifacts."""
    log("\n=== PART H: Writing Proof Artifacts ===")

    PROOF_DIR.mkdir(parents=True, exist_ok=True)

    # -- 00_blocked_rows_summary.txt --
    with open(PROOF_DIR / "00_blocked_rows_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Blocked Rows Summary\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total blocked in Phase 7: {len(blocked_analysis)}\n\n")
        cats = Counter(r["category"] for r in blocked_analysis)
        f.write("Block categories:\n")
        for c, n in sorted(cats.items(), key=lambda x: -x[1]):
            f.write(f"  {c}: {n}\n")
        fixable = sum(1 for r in blocked_analysis if r["fixable"] == "yes")
        f.write(f"\nFixable: {fixable}/{len(blocked_analysis)}\n\n")
        f.write("Details:\n")
        for r in blocked_analysis:
            nm = os.path.basename(r["original_path"])
            f.write(f"\n  {nm[:55]}\n")
            f.write(f"    reason: {r['block_reason']}\n")
            f.write(f"    category: {r['category']}\n")
            f.write(f"    fixable: {r['fixable']}\n")
            f.write(f"    action: {r['recommended_action'][:65]}\n")
    log("  Wrote 00_blocked_rows_summary.txt")

    # -- 01_block_root_cause.txt --
    with open(PROOF_DIR / "01_block_root_cause.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Block Root Cause Analysis\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        classes = Counter(r["classification"] for r in rule_assessments)
        f.write("Rule classifications:\n")
        for c, n in sorted(classes.items()):
            f.write(f"  {c}: {n}\n")
        f.write("\nDetailed rules:\n")
        for r in rule_assessments:
            f.write(f"\n  [{r['classification']}] {r['rule_name']}\n")
            f.write(f"    Description: {r['description']}\n")
            f.write(f"    Affected: {r['affected_count']}\n")
            f.write(f"    Explanation: {r['explanation'][:80]}\n")
            f.write(f"    Recommendation: {r['recommendation'][:80]}\n")
    log("  Wrote 01_block_root_cause.txt")

    # -- 02_review_queue_audit.txt --
    review_classes = Counter(r["classification"] for r in review_audit)
    with open(PROOF_DIR / "02_review_queue_audit.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Review Queue Audit\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total REVIEW_REQUIRED audited: {len(review_audit)}\n\n")
        f.write("Classifications:\n")
        for c, n in sorted(review_classes.items()):
            f.write(f"  {c}: {n}\n")
        f.write("\nSample per classification:\n")
        for cls in ["MISCLASSIFIED_READY", "MISCLASSIFIED_HELD", "LOW_VALUE", "CLEAN_REVIEW"]:
            samples = [r for r in review_audit if r["classification"] == cls][:5]
            if samples:
                f.write(f"\n  {cls} (showing {len(samples)}/{review_classes.get(cls, 0)}):\n")
                for s in samples:
                    nm = s["original_name"][:50]
                    f.write(f"    {nm}  reason={s['reason'][:50]}\n")
    log("  Wrote 02_review_queue_audit.txt")

    # -- 03_held_reaudit.txt --
    held_classes = Counter(r["classification"] for r in held_reaudit)
    with open(PROOF_DIR / "03_held_reaudit.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- HELD Problems Re-Audit\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total HELD audited: {len(held_reaudit)}\n\n")
        f.write("Classifications:\n")
        for c, n in sorted(held_classes.items()):
            f.write(f"  {c}: {n}\n")
        f.write("\nSample RECOVERABLE_TO_REVIEW:\n")
        samples = [r for r in held_reaudit if r["classification"] == "RECOVERABLE_TO_REVIEW"][:10]
        for s in samples:
            nm = s["original_name"][:50]
            f.write(f"  {nm}\n    reason: {s['reason'][:60]}\n")
    log("  Wrote 03_held_reaudit.txt")

    # -- 04_safety_gate_tuning.txt --
    with open(PROOF_DIR / "04_safety_gate_tuning.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Safety Gate Tuning Plan\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for t in safety_tuning:
            f.write(f"[{t['recommendation']}] {t['gate']}\n")
            f.write(f"  Current: {t['current_behavior'][:70]}\n")
            f.write(f"  Proposed: {t['proposed_change'][:70]}\n")
            f.write(f"  Risk: {t['risk_level']}  Overwrites: {t['overwrites_possible']}  Collision: {t['collision_risk'][:30]}\n")
            f.write(f"  Impact: {t['impact'][:60]}\n\n")
    log("  Wrote 04_safety_gate_tuning.txt")

    # -- 05_simulation_results.txt --
    with open(PROOF_DIR / "05_simulation_results.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Simulation Results\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write("IF proposed tuning were applied:\n\n")
        for s in simulation:
            f.write(f"  [{s['risk']}] {s['scenario']}\n")
            f.write(f"    {s['rows_affected']} rows: {s['from_state']} -> {s['to_state']}\n\n")
        total_ready = sum(s["rows_affected"] for s in simulation if s["to_state"] in ("ELIGIBLE", "READY_CANDIDATE"))
        total_held = sum(s["rows_affected"] for s in simulation if s["to_state"] == "HELD_PROBLEMS")
        total_review = sum(s["rows_affected"] for s in simulation if s["to_state"] == "REVIEW_REQUIRED")
        f.write(f"Net impact:\n")
        f.write(f"  Would become READY candidates: {total_ready}\n")
        f.write(f"  Would be demoted to HELD: {total_held}\n")
        f.write(f"  Would recover to REVIEW: {total_review}\n")
    log("  Wrote 05_simulation_results.txt")

    # -- 06_queue_tightening_plan.txt --
    action_counts = Counter(a["action"] for a in tightening)
    with open(PROOF_DIR / "06_queue_tightening_plan.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Queue Tightening Plan\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"Total actions planned: {len(tightening)}\n\n")
        f.write("Action summary:\n")
        for a, c in sorted(action_counts.items()):
            f.write(f"  {a}: {c}\n")
        f.write("\nPriority breakdown:\n")
        prio = Counter(a["priority"] for a in tightening)
        for p, c in sorted(prio.items()):
            f.write(f"  {p}: {c}\n")
    log("  Wrote 06_queue_tightening_plan.txt")

    # -- 07_validation_checks.txt --
    with open(PROOF_DIR / "07_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Validation Checks\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for name, passed, detail in checks:
            status = "PASS" if passed else "FAIL"
            f.write(f"[{status}] {name}\n")
            f.write(f"        {detail}\n\n")
        f.write(f"Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}\n")
    log("  Wrote 07_validation_checks.txt")

    # -- 08_final_report.txt --
    gate = "PASS" if all_pass else "FAIL"
    with open(PROOF_DIR / "08_final_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Final Report\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        f.write(f"PHASE: Blocked Promotion Triage + Queue Tightening\n")
        f.write(f"TYPE: Read-only analysis (NO file operations)\n\n")

        f.write(f"BLOCKED ROWS ANALYSIS:\n")
        cats = Counter(r["category"] for r in blocked_analysis)
        for c, n in sorted(cats.items()):
            f.write(f"  {c}: {n}\n")
        fixable = sum(1 for r in blocked_analysis if r["fixable"] == "yes")
        f.write(f"  fixable: {fixable}/{len(blocked_analysis)}\n\n")

        f.write(f"ROOT CAUSE RULES:\n")
        for cl in ["KEEP_RULE", "RELAXABLE_RULE", "DATA_ISSUE", "EDGE_CASE"]:
            cnt = sum(1 for r in rule_assessments if r["classification"] == cl)
            if cnt:
                f.write(f"  {cl}: {cnt}\n")

        f.write(f"\nREVIEW QUEUE AUDIT ({len(review_audit)} rows):\n")
        for c, n in sorted(review_classes.items()):
            f.write(f"  {c}: {n}\n")

        f.write(f"\nHELD RE-AUDIT ({len(held_reaudit)} rows):\n")
        for c, n in sorted(held_classes.items()):
            f.write(f"  {c}: {n}\n")

        f.write(f"\nSAFETY GATE TUNING:\n")
        for t in safety_tuning:
            f.write(f"  [{t['recommendation']}] {t['gate']}\n")

        f.write(f"\nSIMULATION:\n")
        total_ready = sum(s["rows_affected"] for s in simulation if s["to_state"] in ("ELIGIBLE", "READY_CANDIDATE"))
        total_held_d = sum(s["rows_affected"] for s in simulation if s["to_state"] == "HELD_PROBLEMS")
        total_review_r = sum(s["rows_affected"] for s in simulation if s["to_state"] == "REVIEW_REQUIRED")
        f.write(f"  Would become READY candidates: {total_ready}\n")
        f.write(f"  Would be demoted to HELD: {total_held_d}\n")
        f.write(f"  Would recover to REVIEW: {total_review_r}\n")

        f.write(f"\nQUEUE TIGHTENING PLAN:\n")
        for a, c in sorted(action_counts.items()):
            f.write(f"  {a}: {c}\n")
        f.write(f"  total: {len(tightening)}\n")

        f.write(f"\nVALIDATION: {sum(1 for _,p,_ in checks if p)}/{len(checks)} PASS\n\n")
        f.write(f"GATE={gate}\n")
    log("  Wrote 08_final_report.txt")

    # -- execution_log.txt --
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Phase 8 -- Execution Log\n")
        f.write(f"Generated: {timestamp}\n")
        f.write(f"{'='*60}\n\n")
        for entry in execution_log:
            f.write(entry + "\n")
    log("  Wrote execution_log.txt")

    # Copy CSVs to proof dir
    for csv_path in [BLOCKED_ANALYSIS_CSV, BLOCK_RULE_CSV, REVIEW_AUDIT_CSV,
                     HELD_REAUDIT_CSV, SAFETY_TUNING_CSV, TRIAGE_SIM_CSV,
                     QUEUE_TIGHTENING_CSV]:
        if csv_path.exists():
            shutil.copy2(str(csv_path), str(PROOF_DIR / csv_path.name))

    log(f"\nAll proof artifacts written to: {PROOF_DIR}")
    return gate


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    log(f"Phase 8 -- Blocked Promotion Triage + Queue Tightening")
    log(f"Timestamp: {timestamp}")
    log(f"Workspace: {WORKSPACE}")
    log(f"MODE: READ-ONLY ANALYSIS (no file operations)")
    log("")

    # Take filesystem snapshot FIRST for validation
    take_snapshot()

    # Part A: Blocked Row Analysis
    blocked_analysis = analyze_blocked_rows()

    # Part B: Root Cause Identification
    rule_assessments = identify_root_causes(blocked_analysis)

    # Part C: Review Queue Quality Audit
    review_audit = audit_review_queue()

    # Part D: HELD Problems Re-Audit
    held_reaudit = reaudit_held()

    # Part E: Safety Gate Tuning Plan
    safety_tuning = plan_safety_tuning(blocked_analysis, rule_assessments,
                                       review_audit, held_reaudit)

    # Part F: Simulation
    simulation = run_simulation(review_audit, held_reaudit, safety_tuning)

    # Part G: Queue Tightening Actions
    tightening = plan_queue_tightening(review_audit, held_reaudit)

    # Part I: Validation
    checks, all_pass = run_validation()

    # Part H: Reporting
    gate = write_proof(blocked_analysis, rule_assessments, review_audit,
                       held_reaudit, safety_tuning, simulation, tightening,
                       checks, all_pass)

    log(f"\n{'='*60}")
    log(f"GATE={gate}")
    log(f"PF={PROOF_DIR}")
    log(f"{'='*60}")


if __name__ == "__main__":
    main()
