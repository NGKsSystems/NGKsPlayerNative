"""
NGKsPlayerNative — Confidence Resolver
Deterministic trust-level, review-flag, and confidence-tier logic.
No BPM/key tuning. No per-song overrides. Pure classification.
"""


def _safe_float(val, default=0.0):
    try:
        v = float(val)
        if v != v:  # NaN
            return default
        return v
    except (ValueError, TypeError):
        return default


# ──────────────────────────────────────────────────────────────────
# BPM CONFIDENCE
# ──────────────────────────────────────────────────────────────────
def resolve_bpm_confidence(row):
    """Return (FinalBPMTrustLevel, FinalBPMReviewFlag, FinalBPMReason)."""
    confidence = _safe_float(row.get("FinalBPMConfidence", 0))
    beat_grid = _safe_float(row.get("BeatGridConfidence", 0))
    source = row.get("FinalBPMDecisionSource", "")
    perceptual_applied = str(row.get("PerceptualResolverApplied", "")).upper() == "TRUE"
    perceptual_reason = row.get("PerceptualResolverReason", "") or ""
    bpm_val = _safe_float(row.get("FinalBPM", 0))

    # Calibration failure flag
    cal_class = row.get("_bpm_cal_class", "")  # injected by builder
    cal_failed = cal_class in ("BAD", "CLOSE")

    reasons = []

    # ── Trust level ──
    if bpm_val <= 0:
        trust = "LOW"
        reasons.append("no BPM detected")
    elif confidence >= 0.65 and beat_grid >= 0.6 and not cal_failed:
        trust = "HIGH"
        reasons.append("strong confidence and grid")
    elif confidence >= 0.35 or perceptual_applied:
        trust = "MEDIUM"
        if perceptual_applied:
            reasons.append("perceptual resolver applied")
        if confidence < 0.65:
            reasons.append("moderate confidence")
        if cal_failed:
            reasons.append(f"calibration class={cal_class}")
    else:
        trust = "LOW"
        reasons.append("low confidence")
        if beat_grid < 0.6:
            reasons.append("weak beat grid")

    # ── Review flag ──
    review = False
    if trust == "LOW":
        review = True
    if cal_failed:
        review = True
        if "calibration" not in " ".join(reasons):
            reasons.append(f"calibration class={cal_class}")
    if confidence < 0.30 and bpm_val > 0:
        review = True
        if "low confidence" not in " ".join(reasons):
            reasons.append("very low confidence")

    reason_str = "; ".join(reasons) if reasons else "stable"
    return trust, review, reason_str


# ──────────────────────────────────────────────────────────────────
# KEY CONFIDENCE
# ──────────────────────────────────────────────────────────────────
def resolve_key_confidence(row):
    """Return (FinalKeyTrustLevel, FinalKeyReviewFlag, FinalKeyReason)."""
    confidence = _safe_float(row.get("FinalKeyConfidence", 0))
    tonal_clarity = _safe_float(row.get("TonalClarity", 0))
    source = row.get("FinalKeyDecisionSource", "")
    key_val = row.get("FinalKey", "") or ""

    # Calibration-injected fields
    cal_relation = row.get("_key_cal_relation", "")  # EXACT/NEIGHBOR/RELATIVE/WRONG
    sr_applied = str(row.get("SameRootModeApplied", "")).upper() == "YES"
    sr_decision = row.get("FinalKeyDecisionSource_SR", "") or ""
    sr_ambiguous = "AMBIGUOUS" in sr_decision.upper()

    reasons = []

    # ── Trust level ──
    if not key_val:
        trust = "LOW"
        reasons.append("no key detected")
    elif confidence >= 0.70 and tonal_clarity >= 0.005 and cal_relation != "WRONG":
        trust = "HIGH"
        reasons.append("strong confidence and clarity")
    elif confidence >= 0.40:
        trust = "MEDIUM"
        if cal_relation == "WRONG":
            reasons.append("calibration mismatch")
        if tonal_clarity < 0.005:
            reasons.append("limited tonal clarity")
        if sr_ambiguous:
            reasons.append("same-root ambiguity")
        if not reasons:
            reasons.append("moderate confidence")
    else:
        trust = "LOW"
        reasons.append("low confidence")
        if tonal_clarity < 0.002:
            reasons.append("very low tonal clarity")

    # ── Review flag ──
    review = False
    if trust == "LOW":
        review = True
    if cal_relation == "WRONG":
        review = True
        if "calibration" not in " ".join(reasons):
            reasons.append("calibration mismatch")
    if sr_ambiguous:
        review = True
        if "same-root" not in " ".join(reasons):
            reasons.append("same-root ambiguity unresolved")
    if tonal_clarity < 0.002 and key_val:
        review = True
        if "tonal" not in " ".join(reasons):
            reasons.append("very low tonal clarity")

    reason_str = "; ".join(reasons) if reasons else "stable"
    return trust, review, reason_str


# ──────────────────────────────────────────────────────────────────
# COMBINED QUALITY
# ──────────────────────────────────────────────────────────────────
def resolve_combined_quality(bpm_trust, bpm_review, key_trust, key_review):
    """Return (AnalyzerReady, ReviewRequired, ReviewReason,
              ManualOverrideEligible, ConfidenceTier)."""
    review_required = bpm_review or key_review

    review_reasons = []
    if bpm_review:
        review_reasons.append("BPM")
    if key_review:
        review_reasons.append("Key")
    review_reason = "review: " + "+".join(review_reasons) if review_reasons else ""

    # Analyzer ready
    analyzer_ready = (
        bpm_trust in ("HIGH", "MEDIUM") and
        key_trust in ("HIGH", "MEDIUM") and
        not review_required
    )

    # Manual override eligible: issue is localized
    override_eligible = review_required and not (bpm_trust == "LOW" and key_trust == "LOW")

    # Confidence tier
    if bpm_trust == "LOW" or key_trust == "LOW":
        tier = "REVIEW_REQUIRED"
    elif review_required:
        tier = "USABLE_WITH_CAUTION"
    elif bpm_trust == "HIGH" and key_trust == "HIGH":
        tier = "PRODUCTION"
    else:
        # At least one MEDIUM, no review
        tier = "PRODUCTION"

    return analyzer_ready, review_required, review_reason, override_eligible, tier


# ──────────────────────────────────────────────────────────────────
# EVIDENCE SUMMARIES
# ──────────────────────────────────────────────────────────────────
def build_bpm_candidate_summary(row):
    """Build concise BPM candidate summary string."""
    parts = []
    for i in range(1, 4):
        cand = row.get(f"BPMCandidate{i}", "") or row.get(f"Cand{i}", "")
        score = row.get(f"BPMCandidateScore{i}", "") or row.get(f"Score{i}", "")
        if cand and str(cand).strip():
            c = _safe_float(cand)
            s = _safe_float(score)
            if c > 0:
                parts.append(f"{c:.1f}|{s:.2f}")
    return " ; ".join(parts) if parts else ""


def build_key_candidate_summary(row):
    """Build concise key candidate summary string."""
    parts = []
    for i in range(1, 4):
        cand = row.get(f"KeyCandidate{i}", "")
        score = row.get(f"KeyCandidateScore{i}", "")
        if cand and str(cand).strip():
            s = _safe_float(score)
            parts.append(f"{cand}|{s:.2f}")
    return " ; ".join(parts) if parts else ""


def build_bpm_evidence_summary(row):
    """Build human-readable BPM evidence summary."""
    parts = []
    beat_grid = _safe_float(row.get("BeatGridConfidence", 0))
    bisd = _safe_float(row.get("BeatIntervalStdDev", -1))
    perceptual = str(row.get("PerceptualResolverApplied", "")).upper() == "TRUE"
    source = row.get("FinalBPMDecisionSource", "")

    if beat_grid >= 0.7:
        parts.append("strong grid")
    elif beat_grid >= 0.4:
        parts.append("moderate grid")
    elif beat_grid > 0:
        parts.append("weak grid")

    if bisd >= 0 and bisd < 0.05:
        parts.append("stable intervals")
    elif bisd >= 0.05:
        parts.append("variable intervals")

    if perceptual:
        reason = row.get("PerceptualResolverReason", "") or ""
        parts.append(f"perceptual resolver: {reason}" if reason else "perceptual resolver applied")

    if source == "BPM_FINISH_EVAL":
        parts.append("calibrated")

    return "; ".join(parts) if parts else "base analyzer output"


def build_key_evidence_summary(row):
    """Build human-readable key evidence summary."""
    parts = []
    tonal = _safe_float(row.get("TonalClarity", 0))
    source = row.get("FinalKeyDecisionSource", "")
    sr_applied = str(row.get("SameRootModeApplied", "")).upper() == "YES"
    sr_decision = row.get("FinalKeyDecisionSource_SR", "") or ""
    cal_relation = row.get("_key_cal_relation", "")

    if tonal >= 0.01:
        parts.append("good tonal clarity")
    elif tonal >= 0.005:
        parts.append("moderate tonal clarity")
    elif tonal > 0:
        parts.append("low tonal clarity")

    if "SAME_ROOT" in source or sr_applied:
        if "AMBIGUOUS" in sr_decision.upper():
            parts.append("same-root ambiguity unresolved")
        else:
            parts.append("same-root mode evaluated")

    if cal_relation == "WRONG":
        parts.append("calibration mismatch")
    elif cal_relation in ("EXACT", "NEIGHBOR"):
        parts.append("calibration confirmed")

    if "K2" in source or "PHASE2" in source:
        parts.append("multi-evidence pipeline")

    return "; ".join(parts) if parts else "base analyzer output"
