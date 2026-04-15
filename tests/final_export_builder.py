"""
NGKsPlayerNative — Final Export Builder
Merges analyzer outputs with calibrated BPM/Key results,
applies confidence system, produces production-ready export.

No BPM/key re-tuning. No per-song overrides. Deterministic.
"""

import csv
import os
import sys
import zipfile
from collections import Counter
from datetime import datetime

# ── Confidence resolver ──
sys.path.insert(0, os.path.dirname(__file__))
from confidence_resolver import (
    resolve_bpm_confidence,
    resolve_key_confidence,
    resolve_combined_quality,
    build_bpm_candidate_summary,
    build_key_candidate_summary,
    build_bpm_evidence_summary,
    build_key_evidence_summary,
    _safe_float,
)

# ──────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────
WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "final_export_schema")
EXPORT_VERSION = "1.0"

CAMELOT_MAP = {
    "C major": "8B",  "G major": "9B",  "D major": "10B", "A major": "11B",
    "E major": "12B", "B major": "1B",  "F# major": "2B", "Db major": "3B",
    "Ab major": "4B", "Eb major": "5B", "Bb major": "6B", "F major": "7B",
    "A minor": "8A",  "E minor": "9A",  "B minor": "10A", "F# minor": "11A",
    "C# minor": "12A","Ab minor": "1A", "Eb minor": "2A", "Bb minor": "3A",
    "F minor": "4A",  "C minor": "5A",  "G minor": "6A",  "D minor": "7A",
    "C# major": "3B",  "D# major": "5B", "D# minor": "2A",
    "G# major": "4B",  "G# minor": "1A",
    "A# major": "6B",  "A# minor": "3A",
}
REVERSE_CAMELOT = {}
for _kn, _cam in CAMELOT_MAP.items():
    if _cam not in REVERSE_CAMELOT:
        REVERSE_CAMELOT[_cam] = _kn

# ──────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────
_log_lines = []


def log(line: str):
    ts = datetime.now().strftime("%H:%M:%S")
    entry = f"[{ts}] {line}"
    _log_lines.append(entry)
    try:
        print(entry)
    except UnicodeEncodeError:
        print(entry.encode("ascii", "replace").decode())


# ──────────────────────────────────────────────────────────────────
# CSV loading
# ──────────────────────────────────────────────────────────────────
def load_csv(path, label):
    if not os.path.isfile(path):
        log(f"  {label}: NOT FOUND at {path}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    log(f"  {label}: {len(rows)} rows from {path}")
    return rows


def make_key(row):
    """Artist+Title lookup key, normalized."""
    a = (row.get("Artist", "") or "").strip().lower()
    t = (row.get("Title", "") or "").strip().lower()
    return (a, t)


# ──────────────────────────────────────────────────────────────────
# Key name normalization
# ──────────────────────────────────────────────────────────────────
def key_to_camelot(key_str):
    """Convert a key name like 'D major' to Camelot like '10B'."""
    if not key_str:
        return ""
    key_str = key_str.strip()
    # Already Camelot?
    if len(key_str) >= 2 and key_str[-1] in ("A", "B") and key_str[:-1].isdigit():
        return key_str
    return CAMELOT_MAP.get(key_str, "")


def camelot_to_name(cam):
    """Convert Camelot '10B' to key name 'D major'."""
    if not cam:
        return ""
    return REVERSE_CAMELOT.get(cam, cam)


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────
def main():
    os.chdir(WORKSPACE)
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("FINAL EXPORT BUILDER")
    log(f"Workspace: {WORKSPACE}")
    log(f"Date: {datetime.now().isoformat()}")
    log(f"Export Version: {EXPORT_VERSION}")
    log("")

    # ──────────────────────────────────────────────────────────────
    # LOAD INPUTS
    # ──────────────────────────────────────────────────────────────
    log("=== LOADING INPUTS ===")

    # Primary source: evidence CSV (907 rows)
    primary_path = os.path.join(WORKSPACE, "_proof", "analyzer_upgrade",
                                "03_analysis_with_evidence.csv")
    primary_rows = load_csv(primary_path, "Primary (evidence)")
    if not primary_rows:
        log("FATAL: Primary evidence CSV not found")
        sys.exit(1)

    # BPM finish eval (12 calibration rows)
    bpm_eval_path = os.path.join(WORKSPACE, "_proof", "bpm_finish", "02_finish_eval.csv")
    bpm_eval_rows = load_csv(bpm_eval_path, "BPM finish eval")
    bpm_eval_lookup = {make_key(r): r for r in bpm_eval_rows}

    # Key same-root eval (23 calibration rows — already incorporates K1+K2)
    key_sr_path = os.path.join(WORKSPACE, "_proof", "key_same_root_mode",
                               "02_same_root_eval.csv")
    key_sr_rows = load_csv(key_sr_path, "Key same-root eval")
    key_sr_lookup = {make_key(r): r for r in key_sr_rows}

    # Key phase2 eval (23 calibration rows — fallback if same-root missing)
    key_k2_path = os.path.join(WORKSPACE, "_proof", "key_phase2",
                               "01_key_phase2_eval.csv")
    key_k2_rows = load_csv(key_k2_path, "Key phase2 eval")
    key_k2_lookup = {make_key(r): r for r in key_k2_rows}

    log("")
    log(f"Primary rows: {len(primary_rows)}")
    log(f"BPM calibration rows: {len(bpm_eval_lookup)}")
    log(f"Key calibration rows (SR): {len(key_sr_lookup)}")
    log(f"Key calibration rows (K2): {len(key_k2_lookup)}")

    # ──────────────────────────────────────────────────────────────
    # BUILD EXPORT ROWS
    # ──────────────────────────────────────────────────────────────
    log("")
    log("=== BUILDING EXPORT ===")

    export_rows = []
    bpm_trusts = Counter()
    key_trusts = Counter()
    tiers = Counter()
    review_reasons_counter = Counter()

    for idx, src in enumerate(primary_rows):
        rk = make_key(src)
        bpm_cal = bpm_eval_lookup.get(rk)
        key_sr = key_sr_lookup.get(rk)
        key_k2 = key_k2_lookup.get(rk)

        row_num = idx + 1
        artist = src.get("Artist", "")
        title = src.get("Title", "")
        album = src.get("Album", "")
        filename = src.get("Filename", "")
        duration_s = _safe_float(src.get("Duration_s", 0))

        # ── FINAL BPM ──
        if bpm_cal:
            final_bpm = _safe_float(bpm_cal.get("FinalBPM", 0))
            final_bpm_conf = _safe_float(bpm_cal.get("FinalBPMConfidence", 0))
            final_bpm_family = bpm_cal.get("BPMFamilyLabel", "") or bpm_cal.get("FinalBPMFamily", "")
            final_bpm_source = "BPM_FINISH_EVAL"
            perceptual_applied = bpm_cal.get("PerceptualResolverApplied", "")
            perceptual_reason = bpm_cal.get("PerceptualResolverReason", "")
            bpm_cal_class = bpm_cal.get("FinalBPM_Class", "")
        else:
            # Use evidence-enhanced SelectedBPM if available, else base BPM
            sel_bpm = _safe_float(src.get("SelectedBPM", 0))
            sel_conf = _safe_float(src.get("SelectedBPMConfidence", 0))
            base_bpm = _safe_float(src.get("ResolvedBPM", 0)) or _safe_float(src.get("BPM", 0))
            base_conf = _safe_float(src.get("BPMConfidence", 0))

            if sel_bpm > 0:
                final_bpm = sel_bpm
                final_bpm_conf = sel_conf
                final_bpm_source = "EVIDENCE_SELECTED"
            elif base_bpm > 0:
                final_bpm = base_bpm
                final_bpm_conf = base_conf
                final_bpm_source = "BASE_ANALYZER"
            else:
                final_bpm = 0.0
                final_bpm_conf = 0.0
                final_bpm_source = "BASE_ANALYZER"

            final_bpm_family = src.get("BPMFamily", "")
            perceptual_applied = ""
            perceptual_reason = ""
            bpm_cal_class = ""

        # ── FINAL KEY ──
        if key_sr:
            final_key_cam = key_sr.get("FinalKey_SR", "")
            final_key_name = key_sr.get("FinalKeyName_SR", "") or camelot_to_name(final_key_cam)
            final_key_conf = _safe_float(key_sr.get("FinalKeyConfidence_SR", 0))
            final_key_source = key_sr.get("FinalKeyDecisionSource_SR", "KEY_SAME_ROOT_MODE")
            key_cal_relation = key_sr.get("FinalRelation_SR", "")
            sr_applied = key_sr.get("SameRootModeApplied", "")
            sr_decision = key_sr.get("FinalKeyDecisionSource_SR", "")
        elif key_k2:
            final_key_cam = key_k2.get("K2_Key", "")
            final_key_name = key_k2.get("K2_KeyName", "") or camelot_to_name(final_key_cam)
            final_key_conf = _safe_float(key_k2.get("K2_Confidence", 0))
            final_key_source = "KEY_K2_PHASE2"
            key_cal_relation = key_k2.get("K2_Relation", "")
            sr_applied = ""
            sr_decision = ""
        else:
            # Use evidence-enhanced SelectedKey if available, else base Key
            sel_key = src.get("SelectedKey", "") or ""
            sel_key_conf = _safe_float(src.get("SelectedKeyConfidence", 0))
            base_key = src.get("Key", "") or ""
            base_key_conf = _safe_float(src.get("KeyConfidence", 0))

            if sel_key:
                final_key_cam = key_to_camelot(sel_key)
                if not final_key_cam:
                    final_key_cam = sel_key
                final_key_name = camelot_to_name(final_key_cam) if final_key_cam else sel_key
                final_key_conf = sel_key_conf
                final_key_source = "EVIDENCE_SELECTED"
            elif base_key:
                final_key_cam = key_to_camelot(base_key)
                if not final_key_cam:
                    final_key_cam = base_key
                final_key_name = camelot_to_name(final_key_cam) if final_key_cam else base_key
                final_key_conf = base_key_conf
                final_key_source = "BASE_ANALYZER"
            else:
                final_key_cam = ""
                final_key_name = ""
                final_key_conf = 0.0
                final_key_source = "BASE_ANALYZER"

            key_cal_relation = ""
            sr_applied = ""
            sr_decision = ""

        # ── Build confidence input row ──
        conf_row = {
            "FinalBPM": final_bpm,
            "FinalBPMConfidence": final_bpm_conf,
            "FinalBPMDecisionSource": final_bpm_source,
            "BeatGridConfidence": _safe_float(src.get("BeatGridConfidence", 0)),
            "PerceptualResolverApplied": perceptual_applied,
            "PerceptualResolverReason": perceptual_reason,
            "_bpm_cal_class": bpm_cal_class,
            "FinalKey": final_key_cam,
            "FinalKeyConfidence": final_key_conf,
            "FinalKeyDecisionSource": final_key_source,
            "TonalClarity": _safe_float(src.get("TonalClarity", 0)),
            "_key_cal_relation": key_cal_relation,
            "SameRootModeApplied": sr_applied,
            "FinalKeyDecisionSource_SR": sr_decision,
        }

        bpm_trust, bpm_review, bpm_reason = resolve_bpm_confidence(conf_row)
        key_trust, key_review, key_reason = resolve_key_confidence(conf_row)
        analyzer_ready, review_required, review_reason, override_eligible, tier = \
            resolve_combined_quality(bpm_trust, bpm_review, key_trust, key_review)

        bpm_trusts[bpm_trust] += 1
        key_trusts[key_trust] += 1
        tiers[tier] += 1
        if review_required:
            if bpm_review:
                review_reasons_counter[f"BPM: {bpm_reason}"] += 1
            if key_review:
                review_reasons_counter[f"Key: {key_reason}"] += 1

        # ── Candidate/evidence summaries ──
        bpm_cand_summary = build_bpm_candidate_summary(src)
        key_cand_summary = build_key_candidate_summary(src)
        bpm_ev_summary = build_bpm_evidence_summary(conf_row)
        key_ev_summary = build_key_evidence_summary(conf_row)

        # ── Assemble export row ──
        export_row = {
            "Row": row_num,
            "Artist": artist,
            "Title": title,
            "Album": album,
            "Filename": filename,
            "Duration_s": round(duration_s, 2),
            # BPM
            "FinalBPM": round(final_bpm, 1) if final_bpm > 0 else "",
            "FinalBPMConfidence": round(final_bpm_conf, 4),
            "FinalBPMTrustLevel": bpm_trust,
            "FinalBPMFamily": final_bpm_family,
            "FinalBPMDecisionSource": final_bpm_source,
            "FinalBPMReviewFlag": bpm_review,
            "FinalBPMReason": bpm_reason,
            # Key
            "FinalKey": final_key_cam,
            "FinalKeyName": final_key_name,
            "FinalKeyConfidence": round(final_key_conf, 4),
            "FinalKeyTrustLevel": key_trust,
            "FinalKeyRelationClass": key_cal_relation if key_cal_relation else "N/A",
            "FinalKeyDecisionSource": final_key_source,
            "FinalKeyReviewFlag": key_review,
            "FinalKeyReason": key_reason,
            # Combined
            "AnalyzerReady": analyzer_ready,
            "ReviewRequired": review_required,
            "ReviewReason": review_reason,
            "ManualOverrideEligible": override_eligible,
            "ConfidenceTier": tier,
            "ExportVersion": EXPORT_VERSION,
            # Evidence
            "BPMCandidateSummary": bpm_cand_summary,
            "KeyCandidateSummary": key_cand_summary,
            "BPMEvidenceSummary": bpm_ev_summary,
            "KeyEvidenceSummary": key_ev_summary,
            # Carry-through
            "BPM": src.get("BPM", ""),
            "ResolvedBPM": src.get("ResolvedBPM", ""),
            "Tunebat_BPM": src.get("Tunebat BPM", ""),
            "Key": src.get("Key", ""),
            "Tunebat_Key": src.get("Tunebat Key", ""),
            "BPMConfidence_orig": src.get("BPMConfidence", ""),
            "KeyConfidence_orig": src.get("KeyConfidence", ""),
            "BeatGridConfidence": src.get("BeatGridConfidence", ""),
            "TonalClarity": src.get("TonalClarity", ""),
        }
        export_rows.append(export_row)

    log(f"Built {len(export_rows)} export rows")

    # ──────────────────────────────────────────────────────────────
    # WRITE OUTPUTS
    # ──────────────────────────────────────────────────────────────
    log("")
    log("=== WRITING OUTPUTS ===")

    # Full export CSV
    export_csv_path = os.path.join(PROOF_DIR, "NGKs_final_analyzer_export.csv")
    fields: list[str] = []
    if export_rows:
        fields = list(export_rows[0].keys())
        with open(export_csv_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(export_rows)
    log(f"Wrote {export_csv_path} ({len(export_rows)} rows)")

    # 00_export_build_summary.txt
    summary_path = os.path.join(PROOF_DIR, "00_export_build_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("FINAL EXPORT BUILD SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Export Version: {EXPORT_VERSION}\n\n")
        f.write(f"Primary source: {primary_path}\n")
        f.write(f"Primary rows: {len(primary_rows)}\n")
        f.write(f"BPM calibration rows merged: {len(bpm_eval_lookup)}\n")
        f.write(f"Key calibration rows merged (SR): {len(key_sr_lookup)}\n")
        f.write(f"Key calibration rows merged (K2): {len(key_k2_lookup)}\n\n")
        f.write(f"Export rows: {len(export_rows)}\n")
        f.write(f"Export fields: {len(fields)}\n\n")
        f.write("Fields:\n")
        for fld in fields:
            f.write(f"  {fld}\n")
    log(f"Wrote {summary_path}")

    # 01_final_export_preview.csv (first 25 rows)
    preview_path = os.path.join(PROOF_DIR, "01_final_export_preview.csv")
    preview_rows = export_rows[:25]
    with open(preview_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(preview_rows)
    log(f"Wrote {preview_path} ({len(preview_rows)} rows)")

    # 02_confidence_distribution.txt
    dist_path = os.path.join(PROOF_DIR, "02_confidence_distribution.txt")
    total = len(export_rows)
    with open(dist_path, "w", encoding="utf-8") as f:
        f.write("CONFIDENCE DISTRIBUTION\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Total rows: {total}\n\n")

        f.write("BPM Trust Levels:\n")
        for lvl in ("HIGH", "MEDIUM", "LOW"):
            cnt = bpm_trusts.get(lvl, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {lvl:8s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write("\nKey Trust Levels:\n")
        for lvl in ("HIGH", "MEDIUM", "LOW"):
            cnt = key_trusts.get(lvl, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {lvl:8s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write("\nConfidence Tiers:\n")
        for t in ("PRODUCTION", "USABLE_WITH_CAUTION", "REVIEW_REQUIRED"):
            cnt = tiers.get(t, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {t:24s}: {cnt:5d} ({pct:5.1f}%)\n")

        review_count = sum(1 for r in export_rows if r["ReviewRequired"])
        f.write(f"\nReview Required: {review_count} / {total} ({review_count/total*100:.1f}%)\n")

        f.write("\nTop Review Reasons:\n")
        for reason, cnt in review_reasons_counter.most_common(15):
            f.write(f"  {cnt:4d}  {reason}\n")
    log(f"Wrote {dist_path}")

    # 03_review_queue.csv
    review_path = os.path.join(PROOF_DIR, "03_review_queue.csv")
    review_rows = [r for r in export_rows if r["ReviewRequired"]]
    review_fields = ["Row", "Artist", "Title", "Filename",
                     "FinalBPM", "FinalBPMTrustLevel", "FinalBPMReviewFlag", "FinalBPMReason",
                     "FinalKey", "FinalKeyTrustLevel", "FinalKeyReviewFlag", "FinalKeyReason",
                     "ConfidenceTier", "ReviewReason", "ManualOverrideEligible"]
    with open(review_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=review_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(review_rows)
    log(f"Wrote {review_path} ({len(review_rows)} review rows)")

    # ──────────────────────────────────────────────────────────────
    # STEP 7 — SANITY CHECKS
    # ──────────────────────────────────────────────────────────────
    log("")
    log("=== SANITY CHECKS ===")

    checks = []
    gate = "PASS"

    # Row count preserved
    if len(export_rows) == len(primary_rows):
        checks.append(f"PASS  Row count: {len(export_rows)} == {len(primary_rows)} (primary)")
    else:
        checks.append(f"FAIL  Row count: {len(export_rows)} != {len(primary_rows)} (primary)")
        gate = "FAIL"

    # Row order preserved
    order_ok = True
    for i, (exp, src) in enumerate(zip(export_rows, primary_rows)):
        if exp["Artist"] != src.get("Artist", "") or exp["Title"] != src.get("Title", ""):
            order_ok = False
            checks.append(f"FAIL  Row order mismatch at row {i+1}: "
                          f"export=({exp['Artist']}, {exp['Title']}) != "
                          f"source=({src.get('Artist','')}, {src.get('Title','')})")
            gate = "FAIL"
            break
    if order_ok:
        checks.append("PASS  Row order preserved")

    # No blank Artist/Title
    blank_count = sum(1 for r in export_rows
                      if not r["Artist"].strip() and not r["Title"].strip())
    # Allow rows that were blank in source
    src_blank = sum(1 for r in primary_rows
                    if not r.get("Artist", "").strip() and not r.get("Title", "").strip())
    if blank_count <= src_blank:
        checks.append(f"PASS  No new blank Artist/Title (export={blank_count}, source={src_blank})")
    else:
        checks.append(f"FAIL  New blank Artist/Title introduced: {blank_count} > {src_blank}")
        gate = "FAIL"

    # Export file exists
    if os.path.isfile(export_csv_path):
        sz = os.path.getsize(export_csv_path)
        checks.append(f"PASS  Export file exists ({sz:,} bytes)")
    else:
        checks.append("FAIL  Export file not created")
        gate = "FAIL"

    # All required fields populated
    required_fields = ["Row", "Artist", "Title", "FinalBPM", "FinalBPMConfidence",
                       "FinalBPMTrustLevel", "FinalKey", "FinalKeyConfidence",
                       "FinalKeyTrustLevel", "AnalyzerReady", "ReviewRequired",
                       "ConfidenceTier", "ExportVersion"]
    for fld in required_fields:
        if fld not in fields:
            checks.append(f"FAIL  Required field missing: {fld}")
            gate = "FAIL"
        else:
            checks.append(f"PASS  Required field present: {fld}")

    # Confidence tiers assigned
    unassigned = sum(1 for r in export_rows if not r["ConfidenceTier"])
    if unassigned == 0:
        checks.append("PASS  All rows have ConfidenceTier assigned")
    else:
        checks.append(f"FAIL  {unassigned} rows missing ConfidenceTier")
        gate = "FAIL"

    sanity_path = os.path.join(PROOF_DIR, "04_sanity_checks.txt")
    with open(sanity_path, "w", encoding="utf-8") as f:
        f.write("SANITY CHECKS\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        for c in checks:
            f.write(f"  {c}\n")
        f.write(f"\nGATE: {gate}\n")
    log(f"Wrote {sanity_path}")
    for c in checks:
        log(f"  {c}")
    log(f"Sanity GATE: {gate}")

    # ──────────────────────────────────────────────────────────────
    # STEP 8 — FINAL REPORT
    # ──────────────────────────────────────────────────────────────
    log("")
    log("=== FINAL REPORT ===")

    report_path = os.path.join(PROOF_DIR, "05_final_report.txt")
    production_count = tiers.get("PRODUCTION", 0)
    caution_count = tiers.get("USABLE_WITH_CAUTION", 0)
    review_req_count = tiers.get("REVIEW_REQUIRED", 0)
    review_total = sum(1 for r in export_rows if r["ReviewRequired"])

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("NGKsPlayerNative — FINAL ANALYZER EXPORT REPORT\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"Export Version: {EXPORT_VERSION}\n\n")

        f.write("SCHEMA SUMMARY\n")
        f.write(f"  Total rows: {total}\n")
        f.write(f"  Total fields: {len(fields)}\n")
        f.write(f"  BPM calibration rows: {len(bpm_eval_lookup)}\n")
        f.write(f"  Key calibration rows: {len(key_sr_lookup)}\n\n")

        f.write("BPM TRUST DISTRIBUTION\n")
        for lvl in ("HIGH", "MEDIUM", "LOW"):
            cnt = bpm_trusts.get(lvl, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {lvl:8s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write("\nKEY TRUST DISTRIBUTION\n")
        for lvl in ("HIGH", "MEDIUM", "LOW"):
            cnt = key_trusts.get(lvl, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {lvl:8s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write("\nCONFIDENCE TIERS\n")
        for t in ("PRODUCTION", "USABLE_WITH_CAUTION", "REVIEW_REQUIRED"):
            cnt = tiers.get(t, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {t:24s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write(f"\nPRODUCTION READY: {production_count} / {total} "
                f"({production_count/total*100:.1f}%)\n")
        f.write(f"REVIEW REQUIRED:  {review_total} / {total} "
                f"({review_total/total*100:.1f}%)\n\n")

        f.write("TOP REVIEW REASONS\n")
        for reason, cnt in review_reasons_counter.most_common(10):
            f.write(f"  {cnt:4d}  {reason}\n")

        f.write("\nRECOMMENDATION\n")
        prod_pct = production_count / total * 100 if total else 0
        if prod_pct >= 80:
            f.write(f"  Export is ready for app integration.\n")
            f.write(f"  {prod_pct:.1f}% of rows are PRODUCTION quality.\n")
            f.write(f"  {review_total} rows flagged for optional manual review.\n")
        elif prod_pct >= 50:
            f.write(f"  Export is usable with caution.\n")
            f.write(f"  {prod_pct:.1f}% PRODUCTION, {review_total} rows need review.\n")
        else:
            f.write(f"  Export needs significant review.\n")
            f.write(f"  Only {prod_pct:.1f}% PRODUCTION.\n")

        f.write(f"\nGATE: {gate}\n")
    log(f"Wrote {report_path}")

    log(f"  PRODUCTION:          {production_count} ({production_count/total*100:.1f}%)")
    log(f"  USABLE_WITH_CAUTION: {caution_count} ({caution_count/total*100:.1f}%)")
    log(f"  REVIEW_REQUIRED:     {review_req_count} ({review_req_count/total*100:.1f}%)")
    log(f"  Review total:        {review_total}")

    # ──────────────────────────────────────────────────────────────
    # STEP 9 — PROOF PACKAGE
    # ──────────────────────────────────────────────────────────────
    log("")
    log("=== PROOF PACKAGE ===")

    # Write execution log
    log_path = os.path.join(PROOF_DIR, "execution_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
        f.write("\n")

    # Also copy schema spec into proof dir
    schema_src = os.path.join(WORKSPACE, "tools", "export_schema_spec.md")
    schema_dst = os.path.join(PROOF_DIR, "export_schema_spec.md")
    if os.path.isfile(schema_src):
        import shutil
        shutil.copy2(schema_src, schema_dst)
        log(f"Copied schema spec to proof dir")

    # Create ZIP
    zip_path = os.path.join(WORKSPACE, "_proof", "final_export_schema.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in sorted(os.listdir(PROOF_DIR)):
            fpath = os.path.join(PROOF_DIR, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, os.path.join("final_export_schema", fname))
    log(f"Wrote {zip_path}")

    # Re-write log with final entries
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(_log_lines))
        f.write("\n")

    # Final output
    log("")
    print("=" * 60)
    print(f"PF={PROOF_DIR}")
    print(f"ZIP={zip_path}")
    print(f"GATE={gate}")
    print("=" * 60)


if __name__ == "__main__":
    main()
