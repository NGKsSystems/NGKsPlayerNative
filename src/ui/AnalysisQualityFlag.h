#pragma once
#include <QString>
#include <QStringList>
#include <vector>

// ── Analysis quality flag model ────────────────────────────────────
//
// Compact trust/quality signal for analysis results.
// Evaluated from stored pipeline data — no recomputation.

enum class AnalysisQualityState
{
    Clean,          // no flags triggered
    Review,         // 1+ low-severity flags
    Suspicious      // multiple flags or high-severity combination
};

struct AnalysisFlag
{
    QString name;       // e.g. "Tempo Family Guess"
    QString reason;     // e.g. "Raw 85.2 resolved to 170.4 (DOUBLE)"
    int     severity;   // 1 = low, 2 = medium, 3 = high
};

struct AnalysisQualityStatus
{
    AnalysisQualityState overallState{AnalysisQualityState::Clean};
    std::vector<AnalysisFlag> flags;
    QString summaryText;    // e.g. "Analysis: Clean" or "Analysis: 2 Flags"
    QString tooltipText;    // multi-line detail for hover
};

// ── Input data for flag evaluation ─────────────────────────────────

struct AnalysisQualityInput
{
    // BPM
    double  rawBpm{0.0};
    double  resolvedBpm{0.0};
    double  bpmConfidence{0.0};
    QString bpmFamily;
    int     bpmCandidateCount{0};
    double  bpmCandidateGap{0.0};   // score gap between top two candidates

    // Key
    double  keyConfidence{0.0};
    bool    keyAmbiguous{false};
    QString keyRunnerUp;
    QString keyCorrectionReason;
    QString camelotKey;

    // Completeness
    bool    hasBpm{false};
    bool    hasKey{false};
};

// ── Evaluator ──────────────────────────────────────────────────────

class AnalysisQualityEvaluator
{
public:
    static AnalysisQualityStatus evaluate(const AnalysisQualityInput& input);
};
