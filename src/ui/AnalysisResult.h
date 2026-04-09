#pragma once
#include <QString>
#include <vector>

// ── Analysis output from AudioAnalysisService ──────────────────────

struct BpmCandidate
{
    double  bpm{0.0};
    QString family;             // "HALF", "BASE", "DOUBLE"
    double  score{0.0};
    QString reason;
};

struct AnalysisResult
{
    bool    valid{false};       // true if analysis completed successfully
    QString errorMsg;           // non-empty if valid==false

    // ── CORE ──
    double  bpm{0.0};           // beats per minute (final DJ BPM)
    double  loudnessLUFS{0.0};  // integrated loudness (LUFS / LKFS)
    double  peakDBFS{0.0};      // true peak in dBFS
    double  energy{-1.0};       // normalized signal intensity [0..100]

    // ── BPM RESOLVER ──
    double  rawBpm{0.0};              // autocorrelation raw BPM
    double  resolvedBpm{0.0};         // post-resolver DJ BPM
    double  bpmConfidence{0.0};       // [0..1]
    QString bpmFamily;                // "HALF", "BASE", "DOUBLE"
    double  onsetDensity{0.0};        // onsets per second
    double  hfPercussiveScore{0.0};   // [0..1] high-freq percussive energy
    std::vector<BpmCandidate> bpmCandidates;

    // ── DJ USEFUL ──
    double  cueInSeconds{0.0};  // first strong transient (seconds)
    double  cueOutSeconds{0.0}; // last usable section (seconds)
    double  dynamicRangeLU{0.0};// loudness range (LU)

    // ── AUTO DJ FEATURES ──
    double  danceability{-1.0};       // [0..100]
    double  acousticness{-1.0};       // [0..100]
    double  instrumentalness{-1.0};   // [0..100]
    double  liveness{-1.0};           // [0..100]

    // ── PRO ANALYSIS ──
    QString camelotKey;               // e.g. "8B", "11A"
    double  lra{0.0};                 // loudness range approximation
    double  transitionDifficulty{-1.0}; // [0..100]

    // ── KEY DETECTION DETAIL ──
    double  keyConfidence{0.0};       // [0..1]
    bool    keyAmbiguous{false};
    QString keyRunnerUp;              // e.g. "B Minor"
    QString keyCorrectionReason;      // why key was corrected, if at all

    // ── EXTRA ──
    double  beatGridConfidence{0.0};  // [0..1]
    double  spectralCentroid{0.0};    // Hz — brightness proxy
    double  introDuration{0.0};       // seconds
    double  outroDuration{0.0};       // seconds
    double  durationSeconds{0.0};     // total file duration
    double  sampleRate{0.0};          // file sample rate
};
