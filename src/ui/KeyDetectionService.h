#pragma once

#include <QString>
#include <cstdint>
#include <vector>

// ── Key detection result model ─────────────────────────────────────

struct KeyCandidate
{
    QString musicalKey;   // e.g. "D Major"
    QString camelot;      // e.g. "10B"
    double  score = 0.0;
};

struct KeyAnalysisResult
{
    QString finalKey;             // e.g. "D Major"
    QString finalCamelot;         // e.g. "10B"
    double  confidence = 0.0;     // [0..1]
    bool    ambiguous  = false;

    KeyCandidate topCandidates[5];
    int          candidateCount = 0;

    QString runnerUpKey;
    QString correctionReason;
};

// ── Pro-grade key detection service ────────────────────────────────
//
// Layered pipeline:
//   1. Preprocess (high-pass, normalize, harmonic emphasis)
//   2. HPCP / chroma frames (FFT → pitch class mapping)
//   3. Multi-window aggregation (early/mid/late, reject noise)
//   4. 24-key profile scoring (Krumhansl-Kessler + Temperley)
//   5. Ambiguity resolution (relative keys, fifths, brightness)
//   6. Style-aware correction heuristics
//   7. Confidence scoring
//   8. Camelot mapping

class KeyDetectionService
{
public:
    // Full pipeline.  spectralCentroid is an optional brightness hint
    // from the parent analysis service (Hz, 0 = unknown).
    KeyAnalysisResult detect(const float*  monoData,
                             int64_t       numSamples,
                             double        sampleRate,
                             double        spectralCentroid = 0.0);

private:
    // ── Layer 1 ──
    std::vector<float> preprocessForKey(const float* data, int64_t numSamples,
                                        double sampleRate);

    // ── Layer 2 ──
    struct ChromaFrame {
        double bins[12]     = {};
        double energy       = 0.0;
        double spectralFlux = 0.0;
    };
    std::vector<ChromaFrame> buildChromaFrames(const float* data,
                                               int64_t numSamples,
                                               double sampleRate);

    // ── Layer 3 ──
    struct WindowChroma {
        double bins[12]   = {};
        double confidence = 0.0;
        int    bestKey    = 0;
        bool   bestMajor  = true;
    };
    WindowChroma aggregateSection(const std::vector<ChromaFrame>& frames,
                                  size_t start, size_t end);
    void mergeWindows(const std::vector<WindowChroma>& windows,
                      double merged[12]);

    // ── Layer 4 ──
    struct KeyScore {
        int    root  = 0;
        bool   major = true;
        double score = 0.0;
    };
    std::vector<KeyScore> scoreKeyProfiles(const double chroma[12]);

    // ── Layer 5+6 ──
    void resolveAmbiguity(std::vector<KeyScore>& scores,
                          const double chroma[12],
                          double spectralCentroid,
                          bool& outAmbiguous,
                          QString& outCorrectionReason);

    // ── Layer 6b: Major vs Relative Minor Resolver ──
    void challengeMinorWithRelativeMajor(std::vector<KeyScore>& scores,
                                         const double chroma[12],
                                         bool& outAmbiguous,
                                         QString& outCorrectionReason);

    // ── Layer 6c: DJ Reinterpretation Layer ──
    //  Post-detection heuristic that biases results toward DJ-usable
    //  keys.  Handles same-root minor→major flip, window-majority
    //  override, and bright-signal major bias for rock/pop.
    void djReinterpret(std::vector<KeyScore>& scores,
                       const double chroma[12],
                       const std::vector<WindowChroma>& windows,
                       double spectralCentroid,
                       bool& outAmbiguous,
                       QString& outCorrectionReason);

    // ── Layer 7 ──
    double computeConfidence(const std::vector<KeyScore>& scores,
                             const std::vector<WindowChroma>& windows);

    // ── Layer 8 ──
    static QString mapToCamelot(int root, bool major);
    static QString keyName(int root, bool major);

    // ── FFT utility ──
    static void fftRadix2(double* data, int n);
};
