#include "TransitionValidationService.h"
#include <QDebug>
#include <cmath>
#include <algorithm>

// ════════════════════════════════════════════════════════════════════
//  CAMELOT PARSING + CLASSIFICATION
// ════════════════════════════════════════════════════════════════════

bool TransitionValidationService::parseCamelot(const QString& cam,
                                                int& outNum,
                                                bool& outMajor)
{
    if (cam.length() < 2) return false;
    QChar modeCh = cam.at(cam.length() - 1);
    outMajor = (modeCh == QLatin1Char('B') || modeCh == QLatin1Char('b'));
    bool isModeA = (modeCh == QLatin1Char('A') || modeCh == QLatin1Char('a'));
    if (!outMajor && !isModeA) return false;

    bool ok = false;
    outNum = QStringView(cam).left(cam.length() - 1).toInt(&ok);
    return ok && outNum >= 1 && outNum <= 12;
}

CamelotRelation TransitionValidationService::classifyCamelot(const QString& a,
                                                              const QString& b)
{
    int numA, numB;
    bool majA, majB;
    if (!parseCamelot(a, numA, majA) || !parseCamelot(b, numB, majB))
        return CamelotRelation::Distant;

    // Same key
    if (numA == numB && majA == majB)
        return CamelotRelation::Same;

    // Relative major/minor (same number, different letter)
    if (numA == numB && majA != majB)
        return CamelotRelation::Relative;

    // Adjacent: same letter, numbers differ by 1 on the wheel (wraps 12→1)
    if (majA == majB) {
        int diff = std::abs(numA - numB);
        if (diff == 1 || diff == 11) // 11 = wrap around 12-wheel
            return CamelotRelation::Adjacent;
    }

    return CamelotRelation::Distant;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 1 — HARMONIC COMPATIBILITY (weight: 40)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreHarmonic(
    const TransitionValidationInput& in,
    CamelotRelation& outRelation,
    QStringList& reasons)
{
    if (in.camelotA.isEmpty() || in.camelotB.isEmpty()) {
        reasons << QStringLiteral("harmonic: missing Camelot data");
        outRelation = CamelotRelation::Distant;
        return 25; // unknown = low confidence default
    }

    outRelation = classifyCamelot(in.camelotA, in.camelotB);

    switch (outRelation) {
    case CamelotRelation::Same:
        reasons << QStringLiteral("harmonic: SAME key — perfect harmonic match");
        return 100;
    case CamelotRelation::Adjacent:
        reasons << QString("harmonic: ADJACENT Camelot (%1 → %2) — smooth compatible transition")
                       .arg(in.camelotA, in.camelotB);
        return 85;
    case CamelotRelation::Relative:
        reasons << QString("harmonic: RELATIVE major/minor (%1 → %2) — mood shift, harmonically valid")
                       .arg(in.camelotA, in.camelotB);
        return 70;
    case CamelotRelation::Distant:
        // Check for 2-step distance (still somewhat workable)
        {
            int numA, numB;
            bool majA, majB;
            if (parseCamelot(in.camelotA, numA, majA) &&
                parseCamelot(in.camelotB, numB, majB) && majA == majB) {
                int diff = std::abs(numA - numB);
                if (diff > 6) diff = 12 - diff; // shortest path on wheel
                if (diff == 2) {
                    reasons << QString("harmonic: DISTANT but 2-step on Camelot wheel (%1 → %2) — use with care")
                                   .arg(in.camelotA, in.camelotB);
                    return 40;
                }
            }
        }
        reasons << QString("harmonic: DISTANT / incompatible keys (%1 → %2) — harmonic clash expected")
                       .arg(in.camelotA, in.camelotB);
        return 10;
    }
    return 10;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 2 — BPM COMPATIBILITY (weight: 20)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreBpm(
    const TransitionValidationInput& in,
    double& outDelta,
    double& outPitchShift,
    QStringList& reasons)
{
    outPitchShift = 0.0;

    if (in.bpmA <= 0.0 || in.bpmB <= 0.0) {
        reasons << QStringLiteral("bpm: missing BPM data for one or both tracks");
        outDelta = 0.0;
        return 25;
    }

    outDelta = std::abs(in.bpmA - in.bpmB);
    double pctDiff = (outDelta / std::max(in.bpmA, in.bpmB)) * 100.0;

    // Check half/double time compatibility
    double halfA  = in.bpmA / 2.0;
    double doubleA = in.bpmA * 2.0;
    double halfDelta  = std::abs(halfA - in.bpmB);
    double doubleDelta = std::abs(doubleA - in.bpmB);
    bool halfTimeMatch  = (halfDelta / std::max(halfA, in.bpmB)) < 0.03;
    bool doubleTimeMatch = (doubleDelta / std::max(doubleA, in.bpmB)) < 0.03;

    if (outDelta < 1.0) {
        reasons << QString("bpm: virtually identical (%1 → %2, delta=%3)")
                       .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1)
                       .arg(outDelta, 0, 'f', 1);
        return 100;
    }
    if (outDelta < 3.0) {
        reasons << QString("bpm: very close (%1 → %2, delta=%3)")
                       .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1)
                       .arg(outDelta, 0, 'f', 1);
        return 90;
    }
    if (outDelta < 6.0) {
        outPitchShift = ((in.bpmB - in.bpmA) / in.bpmA) * 100.0;
        reasons << QString("bpm: moderate gap (%1 → %2, delta=%3) — pitch shift %4%5% recommended")
                       .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1)
                       .arg(outDelta, 0, 'f', 1)
                       .arg(outPitchShift > 0 ? "+" : "")
                       .arg(outPitchShift, 0, 'f', 1);
        return 70;
    }
    if (halfTimeMatch || doubleTimeMatch) {
        reasons << QString("bpm: half/double time compatible (%1 → %2)")
                       .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1);
        return 65;
    }
    if (pctDiff < 8.0) {
        outPitchShift = ((in.bpmB - in.bpmA) / in.bpmA) * 100.0;
        reasons << QString("bpm: noticeable gap (%1 → %2, %3%) — significant pitch shift needed")
                       .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1)
                       .arg(pctDiff, 0, 'f', 1);
        return 45;
    }

    reasons << QString("bpm: large mismatch (%1 → %2, delta=%3) — smooth blend impossible")
                   .arg(in.bpmA, 0, 'f', 1).arg(in.bpmB, 0, 'f', 1)
                   .arg(outDelta, 0, 'f', 1);
    return 10;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 3 — ENERGY TRAJECTORY (weight: 15)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreEnergy(
    const TransitionValidationInput& in,
    double& outDelta,
    QStringList& reasons)
{
    if (in.energyA < 0.0 || in.energyB < 0.0) {
        reasons << QStringLiteral("energy: missing energy data");
        outDelta = 0.0;
        return 50; // unknown = neutral
    }

    outDelta = in.energyB - in.energyA; // positive = energy lift

    double absDelta = std::abs(outDelta);

    if (absDelta < 5.0) {
        reasons << QString("energy: smooth plateau (delta=%1) — seamless")
                       .arg(outDelta, 0, 'f', 1);
        return 100;
    }
    if (absDelta < 15.0) {
        if (outDelta > 0) {
            reasons << QString("energy: gentle lift (+%1) — builds momentum")
                           .arg(outDelta, 0, 'f', 1);
            return 90;
        } else {
            reasons << QString("energy: gentle drop (%1) — controlled cooldown")
                           .arg(outDelta, 0, 'f', 1);
            return 80;
        }
    }
    if (absDelta < 30.0) {
        reasons << QString("energy: moderate shift (%1%2) — noticeable but manageable")
                       .arg(outDelta > 0 ? "+" : "")
                       .arg(outDelta, 0, 'f', 1);
        return 55;
    }

    reasons << QString("energy: severe mismatch (%1%2) — jarring transition")
                   .arg(outDelta > 0 ? "+" : "")
                   .arg(outDelta, 0, 'f', 1);
    return 15;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 4 — LOUDNESS MATCH (weight: 10)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreLoudness(
    const TransitionValidationInput& in,
    double& outDelta,
    QStringList& reasons)
{
    // LUFS values are negative (e.g., -14 LUFS). 0 means missing.
    if (in.loudnessA == 0.0 && in.loudnessB == 0.0) {
        reasons << QStringLiteral("loudness: no LUFS data available");
        outDelta = 0.0;
        return 50;
    }

    outDelta = in.loudnessB - in.loudnessA; // LUFS difference
    double absDelta = std::abs(outDelta);

    if (absDelta < 1.5) {
        reasons << QString("loudness: well matched (delta=%1 LU)")
                       .arg(outDelta, 0, 'f', 1);
        return 100;
    }
    if (absDelta < 3.0) {
        reasons << QString("loudness: minor gap (%1 LU) — trim adjust may help")
                       .arg(outDelta, 0, 'f', 1);
        return 80;
    }
    if (absDelta < 6.0) {
        reasons << QString("loudness: noticeable gap (%1 LU) — gain compensation needed")
                       .arg(outDelta, 0, 'f', 1);
        return 50;
    }

    reasons << QString("loudness: large mismatch (%1 LU) — perceived volume jump")
                   .arg(outDelta, 0, 'f', 1);
    return 15;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 5 — CUE ALIGNMENT (weight: 10)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreCue(
    const TransitionValidationInput& in,
    QStringList& reasons)
{
    bool hasCueOut = (in.cueOutA > 0.001);
    bool hasCueIn  = (in.cueInB > 0.001);

    if (hasCueOut && hasCueIn) {
        reasons << QString("cue: cue points available (outA=%1s, inB=%2s) — mix point defined")
                       .arg(in.cueOutA, 0, 'f', 2).arg(in.cueInB, 0, 'f', 2);
        return 100;
    }
    if (hasCueOut || hasCueIn) {
        reasons << QStringLiteral("cue: partial cue data — one track missing cue point");
        return 60;
    }

    reasons << QStringLiteral("cue: no cue points — manual alignment required");
    return 30;
}

// ════════════════════════════════════════════════════════════════════
//  FACTOR 6 — DATA CONFIDENCE (weight: 5)
// ════════════════════════════════════════════════════════════════════

int TransitionValidationService::scoreConfidence(
    const TransitionValidationInput& in,
    QStringList& reasons)
{
    double avgConf = 0.0;
    int n = 0;

    if (in.keyConfidenceA > 0.0) { avgConf += in.keyConfidenceA; ++n; }
    if (in.keyConfidenceB > 0.0) { avgConf += in.keyConfidenceB; ++n; }

    if (n == 0) {
        reasons << QStringLiteral("confidence: no key confidence data — harmonic verdict less reliable");
        return 30;
    }

    avgConf /= n;

    if (avgConf > 0.8) {
        reasons << QString("confidence: high key confidence (avg=%1) — verdict reliable")
                       .arg(avgConf, 0, 'f', 2);
        return 100;
    }
    if (avgConf > 0.5) {
        reasons << QString("confidence: moderate key confidence (avg=%1) — treat harmonic verdict with care")
                       .arg(avgConf, 0, 'f', 2);
        return 65;
    }

    reasons << QString("confidence: low key confidence (avg=%1) — harmonic verdict unreliable")
                   .arg(avgConf, 0, 'f', 2);
    return 25;
}

// ════════════════════════════════════════════════════════════════════
//  VERDICT + RECOMMENDATION
// ════════════════════════════════════════════════════════════════════

TransitionVerdict TransitionValidationService::verdictFromScore(int score)
{
    if (score >= 75) return TransitionVerdict::Safe;
    if (score >= 55) return TransitionVerdict::Workable;
    if (score >= 35) return TransitionVerdict::Risky;
    return TransitionVerdict::Bad;
}

TransitionType TransitionValidationService::recommendType(
    int score,
    CamelotRelation rel,
    double bpmDelta,
    double energyDelta)
{
    if (score >= 75 && bpmDelta < 6.0)
        return TransitionType::PhraseBlend;

    if (score >= 55) {
        if (bpmDelta < 3.0)
            return TransitionType::PhraseBlend;
        return TransitionType::HardCut;
    }

    if (score >= 35) {
        if (rel == CamelotRelation::Distant)
            return TransitionType::EchoOut;
        return TransitionType::HardCut;
    }

    // BAD
    if (std::abs(energyDelta) > 30.0 || rel == CamelotRelation::Distant)
        return TransitionType::Avoid;
    return TransitionType::EchoOut;
}

// ════════════════════════════════════════════════════════════════════
//  MAIN VALIDATION ENTRY POINT
// ════════════════════════════════════════════════════════════════════

TransitionValidationResult TransitionValidationService::validate(
    const TransitionValidationInput& in)
{
    TransitionValidationResult r;

    // ── Collect missing inputs ──
    if (in.trackAPath.isEmpty()) r.missingInputs << QStringLiteral("trackAPath");
    if (in.trackBPath.isEmpty()) r.missingInputs << QStringLiteral("trackBPath");
    if (in.bpmA <= 0.0)  r.missingInputs << QStringLiteral("bpmA");
    if (in.bpmB <= 0.0)  r.missingInputs << QStringLiteral("bpmB");
    if (in.camelotA.isEmpty()) r.missingInputs << QStringLiteral("camelotA");
    if (in.camelotB.isEmpty()) r.missingInputs << QStringLiteral("camelotB");
    if (in.energyA < 0.0) r.missingInputs << QStringLiteral("energyA");
    if (in.energyB < 0.0) r.missingInputs << QStringLiteral("energyB");
    if (in.loudnessA == 0.0 && in.loudnessB == 0.0) r.missingInputs << QStringLiteral("loudness (both)");
    if (in.keyConfidenceA <= 0.0) r.missingInputs << QStringLiteral("keyConfidenceA");
    if (in.keyConfidenceB <= 0.0) r.missingInputs << QStringLiteral("keyConfidenceB");

    // ── Score each factor ──
    r.harmonicScore  = scoreHarmonic(in, r.camelotRelation, r.reasons);
    r.bpmScore       = scoreBpm(in, r.bpmDelta, r.recommendedPitchShift, r.reasons);
    r.energyScore    = scoreEnergy(in, r.energyDelta, r.reasons);
    r.loudnessScore  = scoreLoudness(in, r.loudnessDelta, r.reasons);
    r.cueScore       = scoreCue(in, r.reasons);
    r.confidenceScore = scoreConfidence(in, r.reasons);

    // ── Weighted total ──
    double total = 0.0;
    total += r.harmonicScore  * (kWeightHarmonic   / 100.0);
    total += r.bpmScore       * (kWeightBpm        / 100.0);
    total += r.energyScore    * (kWeightEnergy     / 100.0);
    total += r.loudnessScore  * (kWeightLoudness   / 100.0);
    total += r.cueScore       * (kWeightCue        / 100.0);
    total += r.confidenceScore * (kWeightConfidence / 100.0);

    r.score = std::clamp(static_cast<int>(std::round(total)), 0, 100);

    // ── Verdict + recommendation ──
    r.verdict = verdictFromScore(r.score);
    r.recommendedTransitionType = recommendType(r.score, r.camelotRelation,
                                                 r.bpmDelta, r.energyDelta);

    // ── Cue window recommendation ──
    if (in.cueOutA > 0.0 && in.cueInB > 0.0) {
        r.recommendedCueWindow = QString("mix from %1s (A out) to %2s (B in)")
            .arg(in.cueOutA, 0, 'f', 1).arg(in.cueInB, 0, 'f', 1);
    }

    // ── Log everything ──
    logValidation(in, r);

    return r;
}

// ════════════════════════════════════════════════════════════════════
//  MANDATORY LOGGING
// ════════════════════════════════════════════════════════════════════

void TransitionValidationService::logValidation(
    const TransitionValidationInput& in,
    const TransitionValidationResult& out)
{
    qInfo().noquote() << "═══════════════════════════════════════════════";
    qInfo().noquote() << "[TRANSITION_VALIDATE] START";
    qInfo().noquote() << QString("[TRANSITION_VALIDATE] WEIGHTS: harmonic=%1 bpm=%2 energy=%3 loudness=%4 cue=%5 confidence=%6")
        .arg(kWeightHarmonic).arg(kWeightBpm).arg(kWeightEnergy)
        .arg(kWeightLoudness).arg(kWeightCue).arg(kWeightConfidence);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] TRACK_A: path=%1 key=%2 camelot=%3 bpm=%4 energy=%5 loudness=%6 keyConf=%7")
        .arg(in.trackAPath)
        .arg(in.keyA).arg(in.camelotA)
        .arg(in.bpmA, 0, 'f', 1)
        .arg(in.energyA, 0, 'f', 1)
        .arg(in.loudnessA, 0, 'f', 1)
        .arg(in.keyConfidenceA, 0, 'f', 2);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] TRACK_B: path=%1 key=%2 camelot=%3 bpm=%4 energy=%5 loudness=%6 keyConf=%7")
        .arg(in.trackBPath)
        .arg(in.keyB).arg(in.camelotB)
        .arg(in.bpmB, 0, 'f', 1)
        .arg(in.energyB, 0, 'f', 1)
        .arg(in.loudnessB, 0, 'f', 1)
        .arg(in.keyConfidenceB, 0, 'f', 2);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] CAMELOT_RELATION=%1")
        .arg(camelotRelationToString(out.camelotRelation));

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] BPM_DELTA=%1")
        .arg(out.bpmDelta, 0, 'f', 1);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] ENERGY_DELTA=%1")
        .arg(out.energyDelta, 0, 'f', 1);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] LOUDNESS_DELTA=%1 LU")
        .arg(out.loudnessDelta, 0, 'f', 1);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] CUE_ALIGNMENT_STATUS: outA=%1 inB=%2")
        .arg(in.cueOutA > 0.001 ? "present" : "MISSING")
        .arg(in.cueInB  > 0.001 ? "present" : "MISSING");

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] FACTOR_SCORES: harmonic=%1/100 bpm=%2/100 energy=%3/100 loudness=%4/100 cue=%5/100 confidence=%6/100")
        .arg(out.harmonicScore).arg(out.bpmScore).arg(out.energyScore)
        .arg(out.loudnessScore).arg(out.cueScore).arg(out.confidenceScore);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] TRANSITION_SCORE=%1")
        .arg(out.score);

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] TRANSITION_VERDICT=%1")
        .arg(verdictToString(out.verdict));

    qInfo().noquote() << "[TRANSITION_VALIDATE] TRANSITION_REASONS:";
    for (const auto& reason : out.reasons) {
        qInfo().noquote() << QString("  - %1").arg(reason);
    }

    if (!out.missingInputs.isEmpty()) {
        qInfo().noquote() << "[TRANSITION_VALIDATE] MISSING_INPUTS:";
        for (const auto& mi : out.missingInputs) {
            qInfo().noquote() << QString("  ! %1").arg(mi);
        }
    }

    qInfo().noquote() << QString("[TRANSITION_VALIDATE] TRANSITION_RECOMMENDATION: type=%1 pitchShift=%2% cueWindow=%3")
        .arg(transitionTypeToString(out.recommendedTransitionType))
        .arg(out.recommendedPitchShift, 0, 'f', 1)
        .arg(out.recommendedCueWindow.isEmpty()
             ? QStringLiteral("N/A") : out.recommendedCueWindow);

    qInfo().noquote() << "[TRANSITION_VALIDATE] END";
    qInfo().noquote() << "═══════════════════════════════════════════════";
}
