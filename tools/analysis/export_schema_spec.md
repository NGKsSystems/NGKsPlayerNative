# NGKsPlayerNative — Final Analyzer Export Schema

**Version:** 1.0
**Date:** 2026-04-02

---

## Overview

The final export merges the analyzer's raw outputs with calibrated BPM/Key
results and a deterministic confidence system. Every row receives trust
levels, review flags, and a production-readiness classification.

---

## Field Definitions

### Identity / Source

| Field | Type | Description |
|---|---|---|
| Row | int | 1-based row index in export |
| Artist | str | Artist name from source |
| Title | str | Track title from source |
| Album | str | Album name from source |
| Filename | str | Audio filename |
| Duration_s | float | Track duration in seconds |

### Final BPM Section

| Field | Type | Description |
|---|---|---|
| FinalBPM | float | Best available BPM (calibrated if available, else analyzer output) |
| FinalBPMConfidence | float | 0–1 confidence score for the final BPM |
| FinalBPMTrustLevel | str | HIGH / MEDIUM / LOW |
| FinalBPMFamily | str | BPM family label (e.g. "129 family") |
| FinalBPMDecisionSource | str | Which pipeline stage produced this BPM |
| FinalBPMReviewFlag | bool | TRUE if BPM needs human review |
| FinalBPMReason | str | Human-readable explanation of BPM decision |

### Final Key Section

| Field | Type | Description |
|---|---|---|
| FinalKey | str | Best available Camelot key (e.g. "8B") |
| FinalKeyName | str | Western key name (e.g. "C major") |
| FinalKeyConfidence | float | 0–1 confidence score for the final key |
| FinalKeyTrustLevel | str | HIGH / MEDIUM / LOW |
| FinalKeyRelationClass | str | EXACT/NEIGHBOR/RELATIVE/WRONG vs ground truth (if available), else N/A |
| FinalKeyDecisionSource | str | Which pipeline stage produced this key |
| FinalKeyReviewFlag | bool | TRUE if key needs human review |
| FinalKeyReason | str | Human-readable explanation of key decision |

### Combined Quality Section

| Field | Type | Description |
|---|---|---|
| AnalyzerReady | bool | TRUE if row is production-ready |
| ReviewRequired | bool | TRUE if either BPM or Key needs review |
| ReviewReason | str | Concatenated review reasons |
| ManualOverrideEligible | bool | TRUE if issue is localized and overridable |
| ConfidenceTier | str | PRODUCTION / USABLE_WITH_CAUTION / REVIEW_REQUIRED |
| ExportVersion | str | Schema version identifier ("1.0") |

### Supporting Evidence Section

| Field | Type | Description |
|---|---|---|
| BPMCandidateSummary | str | Top candidates: "129.2\|0.90 ; 64.6\|0.71" |
| KeyCandidateSummary | str | Top candidates: "9A\|0.81 ; 9B\|0.77" |
| BPMEvidenceSummary | str | Human-readable BPM evidence notes |
| KeyEvidenceSummary | str | Human-readable key evidence notes |

### Original Carry-Through Fields

These fields are preserved from the source CSV for auditability:

| Field | Type | Description |
|---|---|---|
| BPM | float | Original analyzer BPM |
| ResolvedBPM | float | Resolver-adjusted BPM |
| Tunebat_BPM | float | Ground truth BPM (if available) |
| Key | str | Original analyzer key |
| Tunebat_Key | str | Ground truth key (if available) |
| BPMConfidence | float | Original BPM confidence |
| KeyConfidence | float | Original key confidence |
| BeatGridConfidence | float | Beat grid alignment score |
| TonalClarity | float | Tonal clarity from chroma analysis |

---

## Confidence System

### BPM Trust Levels

| Level | Criteria |
|---|---|
| HIGH | Confidence >= 0.65 AND beat grid >= 0.6 AND no perceptual ambiguity |
| MEDIUM | Confidence >= 0.35 OR perceptual resolver successfully applied |
| LOW | Confidence < 0.35 OR unresolved detection failure |

### Key Trust Levels

| Level | Criteria |
|---|---|
| HIGH | Confidence >= 0.70 AND tonal clarity >= 0.005 AND no unresolved same-root ambiguity |
| MEDIUM | Confidence >= 0.40 |
| LOW | Confidence < 0.40 OR tonal clarity < 0.002 |

### Confidence Tiers

| Tier | Criteria |
|---|---|
| PRODUCTION | Both BPM and Key trust HIGH or MEDIUM, ReviewRequired=FALSE |
| USABLE_WITH_CAUTION | At least one MEDIUM, no hard fail, ReviewRequired may be TRUE |
| REVIEW_REQUIRED | Any LOW trust or explicit unresolved issue |

---

## Review Flag Rules

### FinalBPMReviewFlag = TRUE when:
- BPM trust is LOW
- Perceptual ambiguity detected but unresolved
- BPM confidence below 0.30
- Known calibration failure (remained BAD/CLOSE in BPM eval)

### FinalKeyReviewFlag = TRUE when:
- Key trust is LOW
- Same-root ambiguity unresolved
- Tonal clarity below 0.002
- Known calibration failure (remained WRONG in key eval)
- Evidence channels conflict (confidence < 0.40)

### ReviewRequired = TRUE when either flag is TRUE

### ManualOverrideEligible = TRUE when:
- ReviewRequired is TRUE
- Issue is localized (one of BPM or Key, not both LOW)

---

## Decision Source Values

### BPM Decision Sources
- `BASE_ANALYZER` — Original analyzer output, no calibration data
- `BPM_FINISH_EVAL` — From calibrated BPM finish phase
- `EVIDENCE_SELECTED` — From evidence-enhanced selection

### Key Decision Sources
- `BASE_ANALYZER` — Original analyzer output, no calibration data
- `KEY_K1_TUNED` — From K1 tuned evaluation
- `KEY_K2_PHASE2` — From K2 phase2 evaluation
- `KEY_SAME_ROOT_MODE` — From same-root mode disambiguation
- `EVIDENCE_SELECTED` — From evidence-enhanced selection

---

## Data Flow

```
03_analysis_with_evidence.csv (907 rows)
    |
    +-- merge BPM finish eval (12 calibration rows by Artist+Title)
    +-- merge Key same-root eval (23 calibration rows by Artist+Title)
    |
    v
confidence_resolver.py   (compute trust, flags, tiers)
    |
    v
NGKs_final_analyzer_export.csv  (907 rows, all final fields)
```
