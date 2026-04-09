#pragma once
#include <QObject>
#include <QString>
#include "AnalysisResult.h"

// ── Real audio analysis service ────────────────────────────────────
//
// Uses JUCE AudioFormatManager to decode audio, then runs DSP
// analysis to compute DJ-relevant metrics.  All values are computed
// from the actual signal — no hardcoded/demo values.

class AudioAnalysisService : public QObject
{
    Q_OBJECT
public:
    explicit AudioAnalysisService(QObject* parent = nullptr);

    // Run the full analysis pipeline on a file.
    // Blocking call — run from worker thread for large files.
    // genre hint is used by the BPM resolver for soft tempo-family bias.
    AnalysisResult analyzeFile(const QString& filePath,
                               const QString& genreHint = {});

    // Lightweight JUCE-based duration probe (seconds). Returns 0.0 on failure.
    // Does NOT run any DSP — just opens the file, reads frame count / sample rate.
    static double probeDurationSeconds(const QString& filePath);

private:
    // ── Individual analysis stages (operate on mono mixdown buffer) ──

    // Tempo detection via autocorrelation of onset envelope
    double detectBPM(const float* data, int64_t numSamples, double sampleRate);

    // Integrated loudness per ITU-R BS.1770 (simplified K-weighted)
    double detectLoudnessLUFS(const float* left, const float* right,
                              int64_t numSamples, double sampleRate);

    // True peak in dBFS
    double detectPeakDBFS(const float* data, int64_t numSamples);

    // Normalized signal energy [0..100]
    double detectEnergy(const float* data, int64_t numSamples);

    // First strong transient (seconds from start)
    double detectCueIn(const float* data, int64_t numSamples, double sampleRate);

    // Last usable section (seconds from start)
    double detectCueOut(const float* data, int64_t numSamples, double sampleRate);

    // Dynamic range approximation (loudness range in LU)
    double detectDynamicRange(const float* data, int64_t numSamples, double sampleRate);

    // Spectral centroid (brightness proxy, Hz)
    double detectSpectralCentroid(const float* data, int64_t numSamples, double sampleRate);

    // ── Derived features ──

    // Danceability from tempo stability + rhythm regularity
    double computeDanceability(double bpm, double energy,
                               double beatGridConfidence);

    // Acousticness from spectral centroid + dynamic range
    double computeAcousticness(double spectralCentroid, double dynamicRange,
                                double energy);

    // Instrumentalness from vocal presence heuristic
    double computeInstrumentalness(const float* data, int64_t numSamples,
                                    double sampleRate);

    // Liveness from dynamic variability
    double computeLiveness(const float* data, int64_t numSamples,
                            double sampleRate);

    // Transition difficulty heuristic
    double computeTransitionDifficulty(double bpm, double energy,
                                        double dynamicRange,
                                        double introDuration);
};
