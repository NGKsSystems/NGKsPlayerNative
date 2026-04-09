#pragma once

#include <QString>
#include <QStringList>

// ── Transition validation input ────────────────────────────────────

struct TransitionValidationInput
{
    QString trackAPath;
    QString trackBPath;

    double  bpmA{0.0};
    double  bpmB{0.0};
    QString keyA;           // e.g. "D Major"
    QString keyB;
    QString camelotA;       // e.g. "10B"
    QString camelotB;
    double  energyA{-1.0};
    double  energyB{-1.0};
    double  loudnessA{0.0}; // LUFS
    double  loudnessB{0.0};
    double  cueOutA{0.0};   // seconds
    double  cueInB{0.0};
    double  transitionDifficultyA{-1.0}; // [0..100]
    double  transitionDifficultyB{-1.0};
    double  keyConfidenceA{0.0}; // [0..1]
    double  keyConfidenceB{0.0};
};

// ── Enums ──────────────────────────────────────────────────────────

enum class TransitionVerdict { Safe, Workable, Risky, Bad };

enum class CamelotRelation { Same, Adjacent, Relative, Distant };

enum class TransitionType { PhraseBlend, HardCut, EchoOut, Avoid };

// ── Transition validation result ───────────────────────────────────

struct TransitionValidationResult
{
    int                 score{0};           // 0–100
    TransitionVerdict   verdict{TransitionVerdict::Bad};
    QStringList         reasons;

    double              bpmDelta{0.0};
    CamelotRelation     camelotRelation{CamelotRelation::Distant};
    double              energyDelta{0.0};
    double              loudnessDelta{0.0};

    double              recommendedPitchShift{0.0}; // percent
    TransitionType      recommendedTransitionType{TransitionType::Avoid};
    QString             recommendedCueWindow;       // human-readable

    // Per-factor scores (0–100 each, before weighting)
    int harmonicScore{0};
    int bpmScore{0};
    int energyScore{0};
    int loudnessScore{0};
    int cueScore{0};
    int confidenceScore{0};

    // Missing data flags
    QStringList         missingInputs;
};

// ── Service ────────────────────────────────────────────────────────

class TransitionValidationService
{
public:
    TransitionValidationResult validate(const TransitionValidationInput& input);

    // Scoring weights (public for transparency / logging)
    static constexpr int kWeightHarmonic   = 40;
    static constexpr int kWeightBpm        = 20;
    static constexpr int kWeightEnergy     = 15;
    static constexpr int kWeightLoudness   = 10;
    static constexpr int kWeightCue        = 10;
    static constexpr int kWeightConfidence = 5;

private:
    // Factor scorers — each returns 0–100
    int  scoreHarmonic(const TransitionValidationInput& in,
                       CamelotRelation& outRelation,
                       QStringList& reasons);
    int  scoreBpm(const TransitionValidationInput& in,
                  double& outDelta,
                  double& outPitchShift,
                  QStringList& reasons);
    int  scoreEnergy(const TransitionValidationInput& in,
                     double& outDelta,
                     QStringList& reasons);
    int  scoreLoudness(const TransitionValidationInput& in,
                       double& outDelta,
                       QStringList& reasons);
    int  scoreCue(const TransitionValidationInput& in,
                  QStringList& reasons);
    int  scoreConfidence(const TransitionValidationInput& in,
                         QStringList& reasons);

    // Camelot helpers
    static CamelotRelation classifyCamelot(const QString& a, const QString& b);
    static bool parseCamelot(const QString& cam, int& outNum, bool& outMajor);

    // Verdict from total score
    static TransitionVerdict verdictFromScore(int score);
    static TransitionType   recommendType(int score,
                                           CamelotRelation rel,
                                           double bpmDelta,
                                           double energyDelta);

    // Logging
    void logValidation(const TransitionValidationInput& in,
                       const TransitionValidationResult& out);
};

// ── String helpers ─────────────────────────────────────────────────
inline QString verdictToString(TransitionVerdict v) {
    switch (v) {
    case TransitionVerdict::Safe:     return QStringLiteral("SAFE");
    case TransitionVerdict::Workable: return QStringLiteral("WORKABLE");
    case TransitionVerdict::Risky:    return QStringLiteral("RISKY");
    case TransitionVerdict::Bad:      return QStringLiteral("BAD");
    }
    return QStringLiteral("UNKNOWN");
}

inline QString camelotRelationToString(CamelotRelation r) {
    switch (r) {
    case CamelotRelation::Same:     return QStringLiteral("SAME");
    case CamelotRelation::Adjacent: return QStringLiteral("ADJACENT");
    case CamelotRelation::Relative: return QStringLiteral("RELATIVE");
    case CamelotRelation::Distant:  return QStringLiteral("DISTANT");
    }
    return QStringLiteral("UNKNOWN");
}

inline QString transitionTypeToString(TransitionType t) {
    switch (t) {
    case TransitionType::PhraseBlend: return QStringLiteral("phrase blend");
    case TransitionType::HardCut:     return QStringLiteral("hard cut");
    case TransitionType::EchoOut:     return QStringLiteral("echo out / reset");
    case TransitionType::Avoid:       return QStringLiteral("avoid");
    }
    return QStringLiteral("unknown");
}
