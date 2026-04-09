#pragma once
#include "AnalysisResult.h"
#include <QString>
#include <vector>

// ── BPM Resolver — Serato-style post-detection tempo family selection ──
//
// Takes raw autocorrelation BPM and audio data, generates half/base/double
// candidates, scores them using onset density, IOI analysis, HF percussive
// energy, bar plausibility, and genre bias. Returns the best DJ-usable BPM.

struct BpmResolutionResult
{
    double  rawBpm{0.0};
    double  resolvedBpm{0.0};
    double  confidence{0.0};       // [0..1]
    QString selectedFamily;        // "HALF", "BASE", "DOUBLE"
    double  onsetDensity{0.0};     // onsets per second
    double  hfPercussiveScore{0.0};// [0..1]
    std::vector<BpmCandidate> candidates;
};

class BpmResolverService
{
public:
    BpmResolutionResult resolve(double rawBpm,
                                const float* monoData,
                                int64_t numSamples,
                                double sampleRate,
                                const QString& genre = {});

private:
    // Onset density (onsets per second)
    double computeOnsetDensity(const float* data, int64_t numSamples,
                               double sampleRate);

    // Inter-onset interval histogram peak period (seconds)
    double computeIOIPeakPeriod(const float* data, int64_t numSamples,
                                double sampleRate);

    // High-frequency percussive energy ratio [0..1]
    double computeHFPercussiveScore(const float* data, int64_t numSamples,
                                    double sampleRate);

    // Score a single candidate
    double scoreCandidate(double candidateBpm, double onsetDensity,
                          double ioiPeakPeriod, double hfPercussive,
                          const QString& genre);

    // Generate half/base/double candidates from raw BPM
    std::vector<BpmCandidate> generateCandidates(double rawBpm);

    // DJ-range plausibility [0..1]
    static double rangePlausibility(double bpm);

    // Genre-specific bias [0..1]
    static double genreBias(double bpm, const QString& genre);
};
