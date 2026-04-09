#include "AnalysisQualityFlag.h"
#include <QDebug>
#include <cmath>

// ════════════════════════════════════════════════════════════════════
//  Thresholds
// ════════════════════════════════════════════════════════════════════

static constexpr double kBpmConfidenceLow  = 0.55;
static constexpr double kKeyConfidenceLow  = 0.40;
static constexpr double kBpmCandidateGapNarrow = 0.08;

// ════════════════════════════════════════════════════════════════════
//  evaluate()
// ════════════════════════════════════════════════════════════════════

AnalysisQualityStatus AnalysisQualityEvaluator::evaluate(
    const AnalysisQualityInput& input)
{
    AnalysisQualityStatus status;
    qDebug() << "[ANALYSIS_FLAG] ANALYSIS_FLAG_EVAL_START";

    auto addFlag = [&](const QString& name, const QString& reason, int severity) {
        status.flags.push_back({name, reason, severity});
        qInfo().noquote() << "[ANALYSIS_FLAG] FLAG_TRIGGERED"
                          << name << reason;
    };

    auto suppress = [](const QString& name, const QString& reason) {
        qDebug().noquote() << "[ANALYSIS_FLAG] FLAG_SUPPRESSED"
                           << name << reason;
    };

    // ── Rule 7: Missing critical analysis ──
    if (!input.hasBpm && !input.hasKey) {
        addFlag(QStringLiteral("Analysis Incomplete"),
                QStringLiteral("BPM and Key both missing"),
                3);
    } else if (!input.hasBpm) {
        addFlag(QStringLiteral("Analysis Incomplete"),
                QStringLiteral("BPM missing"),
                2);
    } else if (!input.hasKey) {
        addFlag(QStringLiteral("Analysis Incomplete"),
                QStringLiteral("Key missing"),
                2);
    }

    // Only evaluate detail flags if we have data
    if (input.hasBpm) {
        // ── Rule 1: BPM family flag ──
        if (std::abs(input.resolvedBpm - input.rawBpm) > 0.5 &&
            input.bpmFamily != QLatin1String("BASE")) {
            addFlag(QStringLiteral("Tempo Family Guess"),
                    QStringLiteral("Raw ") +
                    QString::number(input.rawBpm, 'f', 1) +
                    QStringLiteral(" resolved to ") +
                    QString::number(input.resolvedBpm, 'f', 1) +
                    QStringLiteral(" (") + input.bpmFamily + QStringLiteral(")"),
                    1);
        } else {
            suppress(QStringLiteral("Tempo Family Guess"),
                     QStringLiteral("raw==resolved or BASE family"));
        }

        // ── Rule 2: Low BPM confidence ──
        if (input.bpmConfidence < kBpmConfidenceLow) {
            addFlag(QStringLiteral("Low BPM Confidence"),
                    QStringLiteral("Confidence ") +
                    QString::number(input.bpmConfidence, 'f', 2) +
                    QStringLiteral(" below threshold ") +
                    QString::number(kBpmConfidenceLow, 'f', 2),
                    2);
        } else {
            suppress(QStringLiteral("Low BPM Confidence"),
                     QStringLiteral("confidence=") +
                     QString::number(input.bpmConfidence, 'f', 2));
        }

        // ── Optional: narrow BPM candidate gap ──
        if (input.bpmCandidateCount >= 2 &&
            input.bpmCandidateGap < kBpmCandidateGapNarrow) {
            addFlag(QStringLiteral("Competing BPM Candidates"),
                    QStringLiteral("Top two candidates separated by ") +
                    QString::number(input.bpmCandidateGap, 'f', 3),
                    1);
        }

        // ── Optional: family jump with medium confidence ──
        if (std::abs(input.resolvedBpm - input.rawBpm) > 0.5 &&
            input.bpmFamily != QLatin1String("BASE") &&
            input.bpmConfidence < 0.70 && input.bpmConfidence >= kBpmConfidenceLow) {
            addFlag(QStringLiteral("Review Tempo"),
                    QStringLiteral("Family jump with medium confidence ") +
                    QString::number(input.bpmConfidence, 'f', 2),
                    2);
        }
    }

    if (input.hasKey) {
        // ── Rule 3: Low key confidence ──
        if (input.keyConfidence < kKeyConfidenceLow) {
            addFlag(QStringLiteral("Low Key Confidence"),
                    QStringLiteral("Confidence ") +
                    QString::number(input.keyConfidence, 'f', 2) +
                    QStringLiteral(" below threshold ") +
                    QString::number(kKeyConfidenceLow, 'f', 2),
                    2);
        } else {
            suppress(QStringLiteral("Low Key Confidence"),
                     QStringLiteral("confidence=") +
                     QString::number(input.keyConfidence, 'f', 2));
        }

        // ── Rule 4: Key ambiguity ──
        if (input.keyAmbiguous) {
            QString reason = QStringLiteral("Key marked ambiguous");
            if (!input.keyRunnerUp.isEmpty()) {
                reason += QStringLiteral(", runner-up: ") + input.keyRunnerUp;
            }
            addFlag(QStringLiteral("Ambiguous Key"), reason, 2);
        } else {
            suppress(QStringLiteral("Ambiguous Key"),
                     QStringLiteral("keyAmbiguous=false"));
        }

        // ── Rule 5: Correction layer fired ──
        if (!input.keyCorrectionReason.isEmpty()) {
            addFlag(QStringLiteral("Corrected Key"),
                    input.keyCorrectionReason,
                    1);
        } else {
            suppress(QStringLiteral("Corrected Key"),
                     QStringLiteral("no correction"));
        }

        // ── Optional: low key confidence + correction ──
        if (input.keyConfidence < kKeyConfidenceLow &&
            !input.keyCorrectionReason.isEmpty()) {
            addFlag(QStringLiteral("Review Harmonics"),
                    QStringLiteral("Low confidence key was also corrected"),
                    2);
        }
    }

    // ── Rule 6: Raw vs final disagreement (composite) ──
    // Already covered by rules 1 and 5, but log the composite check
    {
        bool bpmDisagree = input.hasBpm &&
            std::abs(input.resolvedBpm - input.rawBpm) > 0.5;
        bool keyCorr = input.hasKey &&
            !input.keyCorrectionReason.isEmpty();
        if (bpmDisagree && keyCorr) {
            // Both BPM and key were adjusted — already flagged individually
            qDebug() << "[ANALYSIS_FLAG] RAW_VS_FINAL_BOTH_ADJUSTED"
                     << "bpm_raw=" << input.rawBpm
                     << "bpm_resolved=" << input.resolvedBpm
                     << "key_correction=" << input.keyCorrectionReason;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  Compute overall state from flags
    // ══════════════════════════════════════════════════════════════

    if (status.flags.empty()) {
        status.overallState = AnalysisQualityState::Clean;
        status.summaryText  = QStringLiteral("Analysis: Clean");
        status.tooltipText  = QStringLiteral("All analysis results look reliable.");
    } else {
        // Count severity levels
        int highCount = 0, medCount = 0, lowCount = 0;
        for (const auto& f : status.flags) {
            if (f.severity >= 3) ++highCount;
            else if (f.severity == 2) ++medCount;
            else ++lowCount;
        }

        // SUSPICIOUS: any high-severity, or 2+ medium, or 3+ total
        if (highCount > 0 || medCount >= 2 ||
            static_cast<int>(status.flags.size()) >= 3) {
            status.overallState = AnalysisQualityState::Suspicious;
        } else {
            status.overallState = AnalysisQualityState::Review;
        }

        int n = static_cast<int>(status.flags.size());
        if (n == 1) {
            status.summaryText = QStringLiteral("Analysis: ") +
                                 status.flags[0].name;
        } else {
            status.summaryText = QStringLiteral("Analysis: ") +
                                 QString::number(n) +
                                 QStringLiteral(" Flags");
        }

        // Build tooltip
        QStringList lines;
        for (const auto& f : status.flags) {
            lines << (QStringLiteral("- ") + f.name +
                      QStringLiteral(": ") + f.reason);
        }
        status.tooltipText = lines.join(QLatin1Char('\n'));
    }

    qInfo().noquote() << "[ANALYSIS_FLAG] ANALYSIS_QUALITY_STATE"
                      << (status.overallState == AnalysisQualityState::Clean
                            ? QStringLiteral("CLEAN")
                            : status.overallState == AnalysisQualityState::Review
                                ? QStringLiteral("REVIEW")
                                : QStringLiteral("SUSPICIOUS"));
    qInfo().noquote() << "[ANALYSIS_FLAG] ANALYSIS_QUALITY_SUMMARY"
                      << status.summaryText;

    return status;
}
