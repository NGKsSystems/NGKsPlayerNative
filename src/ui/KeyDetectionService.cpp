#include "KeyDetectionService.h"

#include <QDebug>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// Note names for logging
static const char* kNoteNames[12] = {
    "C", "C#", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"
};

// ════════════════════════════════════════════════════════════════════
//  FFT — Radix-2 Cooley-Tukey (in-place)
// ════════════════════════════════════════════════════════════════════
//
//  data = interleaved complex: [re0, im0, re1, im1, ...]
//  n    = number of complex samples (must be power of 2)

void KeyDetectionService::fftRadix2(double* data, int n)
{
    // Bit-reversal permutation
    for (int i = 1, j = 0; i < n; ++i) {
        int bit = n >> 1;
        while (j & bit) { j ^= bit; bit >>= 1; }
        j ^= bit;
        if (i < j) {
            std::swap(data[2 * i],     data[2 * j]);
            std::swap(data[2 * i + 1], data[2 * j + 1]);
        }
    }

    // Butterfly stages
    for (int len = 2; len <= n; len <<= 1) {
        double angle = -2.0 * M_PI / len;
        double wRe = std::cos(angle);
        double wIm = std::sin(angle);
        for (int i = 0; i < n; i += len) {
            double curRe = 1.0, curIm = 0.0;
            for (int j = 0; j < len / 2; ++j) {
                int a = i + j;
                int b = i + j + len / 2;
                double tRe = curRe * data[2 * b]     - curIm * data[2 * b + 1];
                double tIm = curRe * data[2 * b + 1] + curIm * data[2 * b];
                data[2 * b]     = data[2 * a]     - tRe;
                data[2 * b + 1] = data[2 * a + 1] - tIm;
                data[2 * a]     += tRe;
                data[2 * a + 1] += tIm;
                double next = curRe * wRe - curIm * wIm;
                curIm       = curRe * wIm + curIm * wRe;
                curRe       = next;
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 1 — Preprocess (high-pass, normalize)
// ════════════════════════════════════════════════════════════════════

std::vector<float> KeyDetectionService::preprocessForKey(
    const float* data, int64_t numSamples, double sampleRate)
{
    std::vector<float> out(static_cast<size_t>(numSamples));

    // 1) High-pass filter at ~55 Hz  (1-pole IIR)
    //    Preserves D2 (73.4 Hz) and all useful bass fundamentals.
    //    alpha = 1 / (1 + 2*pi*fc/sr)
    const double fc = 55.0;
    const double alpha = 1.0 / (1.0 + 2.0 * M_PI * fc / sampleRate);
    double prevIn  = data[0];
    double prevOut = data[0];
    out[0] = data[0];
    for (int64_t i = 1; i < numSamples; ++i) {
        double filtered = alpha * (prevOut + data[i] - prevIn);
        out[static_cast<size_t>(i)] = static_cast<float>(filtered);
        prevIn  = data[i];
        prevOut = filtered;
    }

    // 2) RMS normalization — target ~-20 dBFS (0.1 amplitude)
    //    Removes loudness bias so chroma extraction is consistent.
    double sumSq = 0.0;
    for (int64_t i = 0; i < numSamples; ++i) {
        double s = out[static_cast<size_t>(i)];
        sumSq += s * s;
    }
    double rms = std::sqrt(sumSq / static_cast<double>(numSamples));
    if (rms > 1e-8) {
        float gain = static_cast<float>(0.1 / rms);
        if (gain > 100.0f) gain = 100.0f;  // clamp for near-silence
        for (int64_t i = 0; i < numSamples; ++i) {
            out[static_cast<size_t>(i)] *= gain;
        }
    }

    return out;
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 2 — Build HPCP / chroma frames
// ════════════════════════════════════════════════════════════════════
//
//  For each STFT frame:
//    • FFT (radix-2, 4096 points)
//    • Map magnitude-spectrum bins → 12 pitch classes
//    • Weight by harmonic-relevance band (200–1000 Hz emphasis)
//    • Compute spectral flux for transient detection
//  Then across all frames:
//    • Temporal smoothing (EMA) = harmonic emphasis
//    • Down-weight transient-heavy frames

std::vector<KeyDetectionService::ChromaFrame>
KeyDetectionService::buildChromaFrames(const float* data,
                                       int64_t numSamples,
                                       double sampleRate)
{
    constexpr int kFFTSize = 4096;
    constexpr int kHop     = 2048;
    const int64_t numFrames = (numSamples - kFFTSize) / kHop;
    if (numFrames < 1) return {};

    // Hann window
    std::vector<double> window(kFFTSize);
    for (int i = 0; i < kFFTSize; ++i) {
        window[i] = 0.5 * (1.0 - std::cos(2.0 * M_PI * i / (kFFTSize - 1)));
    }

    // Pre-compute bin → pitch-class mapping
    // Valid range: A1 (55 Hz) to C7 (~2093 Hz)
    const int halfBins = kFFTSize / 2 + 1;
    std::vector<int>    binToPc(halfBins, -1);
    std::vector<double> binWeight(halfBins, 0.0);

    for (int k = 1; k < halfBins; ++k) {
        double freq = static_cast<double>(k) * sampleRate / kFFTSize;
        if (freq < 55.0 || freq > 2100.0) continue;

        // MIDI note: A4(440)=69
        double midi = 12.0 * std::log2(freq / 440.0) + 69.0;
        int pc = static_cast<int>(std::round(midi)) % 12;
        if (pc < 0) pc += 12;
        binToPc[k] = pc;

        // Harmonic-relevance weight: favor 130–2000 Hz,
        // taper linearly outside that band.
        // Lower bound at 130 Hz (C3) to include bass fundamentals
        // that carry key information.
        double w = 1.0;
        if (freq < 130.0)       w = freq / 130.0;
        else if (freq > 2000.0) w = 2000.0 / freq;
        binWeight[k] = w;
    }

    // ── Process all frames ──
    std::vector<ChromaFrame> frames;
    frames.reserve(static_cast<size_t>(numFrames));

    std::vector<double> prevMag(halfBins, 0.0);
    std::vector<double> fftBuf(kFFTSize * 2, 0.0);  // interleaved complex

    for (int64_t f = 0; f < numFrames; ++f) {
        const float* frameStart = data + f * kHop;

        // Fill FFT buffer (real = windowed samples, imag = 0)
        for (int i = 0; i < kFFTSize; ++i) {
            fftBuf[2 * i]     = frameStart[i] * window[i];
            fftBuf[2 * i + 1] = 0.0;
        }

        fftRadix2(fftBuf.data(), kFFTSize);

        // Compute magnitudes, chroma, energy, flux
        ChromaFrame cf;
        double frameEnergy = 0.0;
        double flux = 0.0;

        for (int k = 1; k < halfBins; ++k) {
            double re  = fftBuf[2 * k];
            double im  = fftBuf[2 * k + 1];
            double mag = std::sqrt(re * re + im * im);

            frameEnergy += mag * mag;

            // Spectral flux (half-wave rectified)
            double diff = mag - prevMag[k];
            if (diff > 0.0) flux += diff;
            prevMag[k] = mag;

            // Accumulate to pitch class
            int pc = binToPc[k];
            if (pc >= 0) {
                cf.bins[pc] += mag * binWeight[k];
            }
        }

        cf.energy       = frameEnergy;
        cf.spectralFlux = flux;
        frames.push_back(cf);
    }

    // ── Harmonic emphasis: temporal smoothing of chroma (EMA) ──
    // Sustained harmonic content survives; transient spikes decay.
    constexpr double kSmoothAlpha = 0.3;
    if (frames.size() > 1) {
        for (size_t i = 1; i < frames.size(); ++i) {
            for (int pc = 0; pc < 12; ++pc) {
                frames[i].bins[pc] = kSmoothAlpha * frames[i].bins[pc]
                                   + (1.0 - kSmoothAlpha) * frames[i - 1].bins[pc];
            }
        }
    }

    // ── De-emphasize transient-heavy frames ──
    // Frames with spectral flux >> median are percussive.
    if (!frames.empty()) {
        std::vector<double> fluxes;
        fluxes.reserve(frames.size());
        for (const auto& cf : frames) fluxes.push_back(cf.spectralFlux);
        std::sort(fluxes.begin(), fluxes.end());
        double medianFlux = fluxes[fluxes.size() / 2];
        double fluxThreshold = medianFlux * 3.0;

        for (auto& cf : frames) {
            if (cf.spectralFlux > fluxThreshold && medianFlux > 0.0) {
                double scale = medianFlux / cf.spectralFlux;
                for (int pc = 0; pc < 12; ++pc) {
                    cf.bins[pc] *= scale;
                }
            }
        }
    }

    return frames;
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 3 — Multi-window aggregation
// ════════════════════════════════════════════════════════════════════

KeyDetectionService::WindowChroma
KeyDetectionService::aggregateSection(const std::vector<ChromaFrame>& frames,
                                      size_t start, size_t end)
{
    WindowChroma wc;
    if (start >= end || end > frames.size()) return wc;

    double totalEnergy = 0.0;
    int    liveFrames  = 0;

    for (size_t i = start; i < end; ++i) {
        // Skip near-silence frames
        if (frames[i].energy < 1e-10) continue;

        double weight = frames[i].energy;
        for (int pc = 0; pc < 12; ++pc) {
            wc.bins[pc] += frames[i].bins[pc] * weight;
        }
        totalEnergy += weight;
        ++liveFrames;
    }

    if (totalEnergy > 0.0) {
        for (int pc = 0; pc < 12; ++pc) {
            wc.bins[pc] /= totalEnergy;
        }
        // Confidence is proportional to how many frames had signal
        wc.confidence = static_cast<double>(liveFrames)
                      / static_cast<double>(end - start);
    }

    return wc;
}

void KeyDetectionService::mergeWindows(const std::vector<WindowChroma>& windows,
                                       double merged[12])
{
    for (int pc = 0; pc < 12; ++pc) merged[pc] = 0.0;

    double totalWeight = 0.0;
    for (const auto& w : windows) {
        if (w.confidence <= 0.0) continue;
        for (int pc = 0; pc < 12; ++pc) {
            merged[pc] += w.bins[pc] * w.confidence;
        }
        totalWeight += w.confidence;
    }

    if (totalWeight > 0.0) {
        for (int pc = 0; pc < 12; ++pc) {
            merged[pc] /= totalWeight;
        }
    }
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 4 — Score all 24 key profiles
// ════════════════════════════════════════════════════════════════════

std::vector<KeyDetectionService::KeyScore>
KeyDetectionService::scoreKeyProfiles(const double chroma[12])
{
    // ── Krumhansl-Kessler profiles ──
    static const double kkMajor[12] = {
        6.35, 2.23, 3.48, 2.33, 4.38, 4.09,
        2.52, 5.19, 2.39, 3.66, 2.29, 2.88
    };
    static const double kkMinor[12] = {
        6.33, 2.68, 3.52, 5.38, 2.60, 3.53,
        2.54, 4.75, 3.98, 2.69, 3.34, 3.17
    };

    // ── Temperley profiles (Temperley 1999) ──
    static const double tpMajor[12] = {
        5.0, 2.0, 3.5, 2.0, 4.5, 4.0,
        2.0, 4.5, 2.0, 3.5, 1.5, 4.0
    };
    static const double tpMinor[12] = {
        5.0, 2.0, 3.5, 4.5, 2.0, 4.0,
        2.0, 4.5, 3.5, 2.0, 1.5, 4.0
    };

    // Pearson correlation (removes mean bias)
    auto pearson = [](const double* x, const double* y) -> double {
        double mx = 0, my = 0;
        for (int i = 0; i < 12; ++i) { mx += x[i]; my += y[i]; }
        mx /= 12.0; my /= 12.0;
        double num = 0, dx2 = 0, dy2 = 0;
        for (int i = 0; i < 12; ++i) {
            double dx = x[i] - mx;
            double dy = y[i] - my;
            num += dx * dy;
            dx2 += dx * dx;
            dy2 += dy * dy;
        }
        double denom = std::sqrt(dx2 * dy2);
        return denom > 0 ? num / denom : 0.0;
    };

    std::vector<KeyScore> scores;
    scores.reserve(24);

    for (int root = 0; root < 12; ++root) {
        // Rotate chroma so root aligns with index 0
        double rotated[12];
        for (int i = 0; i < 12; ++i) {
            rotated[i] = chroma[(i + root) % 12];
        }

        // Major: average of KK + Temperley scores
        double kkMaj = pearson(rotated, kkMajor);
        double tpMaj = pearson(rotated, tpMajor);
        scores.push_back({root, true, (kkMaj + tpMaj) * 0.5});

        // Minor: average of KK + Temperley scores
        double kkMin = pearson(rotated, kkMinor);
        double tpMin = pearson(rotated, tpMinor);
        scores.push_back({root, false, (kkMin + tpMin) * 0.5});
    }

    // Sort descending by score
    std::sort(scores.begin(), scores.end(),
              [](const KeyScore& a, const KeyScore& b) { return a.score > b.score; });

    return scores;
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 5 + 6 — Ambiguity resolution + style-aware correction
// ════════════════════════════════════════════════════════════════════

void KeyDetectionService::resolveAmbiguity(
    std::vector<KeyScore>& scores,
    const double chroma[12],
    double spectralCentroid,
    bool& outAmbiguous,
    QString& outCorrectionReason)
{
    outAmbiguous = false;
    outCorrectionReason.clear();

    if (scores.size() < 4) return;

    auto best   = scores[0];
    auto runner = scores[1];
    double margin = best.score - runner.score;

    // ── Helper: find a specific key's score in the list ──
    auto findScore = [&](int root, bool major) -> double {
        for (const auto& ks : scores) {
            if (ks.root == root && ks.major == major) return ks.score;
        }
        return -1e9;
    };

    // ── Helper: promote a key to #1 ──
    auto promote = [&](int root, bool major) {
        for (auto& ks : scores) {
            if (ks.root == root && ks.major == major) {
                ks.score = scores[0].score + 0.001;
                break;
            }
        }
        std::sort(scores.begin(), scores.end(),
                  [](const KeyScore& a, const KeyScore& b) {
                      return a.score > b.score;
                  });
    };

    // ── Detect structural relationships ──
    bool isRelativePair = false;
    if (best.major && !runner.major)
        isRelativePair = (runner.root == (best.root + 9) % 12);
    else if (!best.major && runner.major)
        isRelativePair = (runner.root == (best.root + 3) % 12);

    bool isFifthPair = false;
    if (best.major == runner.major) {
        int diff = (runner.root - best.root + 12) % 12;
        isFifthPair = (diff == 7 || diff == 5);
    }

    // ────────────────────────────────────────────────────────────
    //  SUPERTONIC-MINOR CORRECTION (ii → I)
    //
    //  In rock/pop, the ii chord (e.g., Em in key of D) produces
    //  strong chroma that can outscore the real tonic major key.
    //  If best is minor, check if the major key 2 semitones below
    //  (the "parent tonic") is a strong candidate.
    //
    //  This runs with a wider threshold than relative-pair checks
    //  because the ii-chord artifact is a strong systematic bias.
    // ────────────────────────────────────────────────────────────
    if (!best.major && outCorrectionReason.isEmpty()) {
        int tonicMajRoot = (best.root + 10) % 12;  // root - 2 mod 12
        double tonicMajScore = findScore(tonicMajRoot, true);
        double supertonicMargin = best.score - tonicMajScore;

        qInfo().noquote() << QString("[KEY_DETECT] SUPERTONIC_CHECK: %1 (ii?) vs %2 (I?) "
                                      "minor_score=%3 major_score=%4 margin=%5")
            .arg(keyName(best.root, false))
            .arg(keyName(tonicMajRoot, true))
            .arg(best.score, 0, 'f', 4)
            .arg(tonicMajScore, 0, 'f', 4)
            .arg(supertonicMargin, 0, 'f', 4);

        // Check if parent tonic has strong evidence
        // The major key's tonic (root) and fifth should be prominent
        double tonicChroma = chroma[tonicMajRoot];
        double fifthChroma = chroma[(tonicMajRoot + 7) % 12];
        double majThird    = chroma[(tonicMajRoot + 4) % 12];  // major 3rd of tonic
        double minThird    = chroma[(best.root + 3) % 12];     // minor 3rd of best (ii)

        // Tonic triad strength: root + third + fifth of the major key
        double tonicTriad = tonicChroma + majThird + fifthChroma;
        // ii triad strength: root + third + fifth of the minor key
        double iiTriad = chroma[best.root] + minThird + chroma[(best.root + 7) % 12];

        qInfo().noquote() << QString("[KEY_DETECT] SUPERTONIC_TRIADS: I_triad=%1 "
                                      "(root=%2 3rd=%3 5th=%4) ii_triad=%5 "
                                      "(root=%6 3rd=%7 5th=%8)")
            .arg(tonicTriad, 0, 'f', 4)
            .arg(tonicChroma, 0, 'f', 4)
            .arg(majThird, 0, 'f', 4)
            .arg(fifthChroma, 0, 'f', 4)
            .arg(iiTriad, 0, 'f', 4)
            .arg(chroma[best.root], 0, 'f', 4)
            .arg(minThird, 0, 'f', 4)
            .arg(chroma[(best.root + 7) % 12], 0, 'f', 4);

        bool preferTonic = false;
        QString reason;

        // Case 1: tonicMaj is close in profile score (margin < 0.15)
        //         AND tonic triad is at least ~88% of ii triad
        if (supertonicMargin < 0.15 && tonicTriad > iiTriad * 0.88) {
            preferTonic = true;
            reason = QString("supertonic-minor: I_triad=%1 >= ii_triad*0.88=%2, "
                             "profile_margin=%3")
                .arg(tonicTriad, 0, 'f', 4)
                .arg(iiTriad * 0.88, 0, 'f', 4)
                .arg(supertonicMargin, 0, 'f', 4);
        }
        // Case 2: tonic root is among the strongest chroma bins
        //         (the real tonic note rings loudly)
        if (!preferTonic && supertonicMargin < 0.12) {
            double maxChroma = *std::max_element(chroma, chroma + 12);
            if (tonicChroma >= maxChroma * 0.92) {
                preferTonic = true;
                reason = QString("supertonic-minor: tonic_root=%1 is "
                                 "near_strongest_chroma=%2, margin=%3")
                    .arg(tonicChroma, 0, 'f', 4)
                    .arg(maxChroma, 0, 'f', 4)
                    .arg(supertonicMargin, 0, 'f', 4);
            }
        }
        // Case 3: bright signal + close scores = likely major
        if (!preferTonic && supertonicMargin < 0.10
            && spectralCentroid > 1500.0) {
            preferTonic = true;
            reason = QString("supertonic-minor: bright (centroid=%1Hz), margin=%2")
                .arg(spectralCentroid, 0, 'f', 0)
                .arg(supertonicMargin, 0, 'f', 4);
        }
        // Case 4: wide-net — tonic triad is strictly stronger than ii,
        //         and tonic root is in top-4 chroma bins
        if (!preferTonic && supertonicMargin < 0.20 && tonicTriad > iiTriad) {
            // Count how many chroma bins are >= tonicChroma
            int rank = 0;
            for (int pc = 0; pc < 12; ++pc)
                if (chroma[pc] > tonicChroma) ++rank;
            if (rank < 4) {
                preferTonic = true;
                reason = QString("supertonic-minor: I_triad=%1 > ii_triad=%2, "
                                 "tonic_rank=%3, margin=%4")
                    .arg(tonicTriad, 0, 'f', 4)
                    .arg(iiTriad, 0, 'f', 4)
                    .arg(rank + 1)
                    .arg(supertonicMargin, 0, 'f', 4);
            }
        }

        if (preferTonic) {
            qInfo().noquote() << "[KEY_DETECT] SUPERTONIC_CORRECTION:"
                << keyName(best.root, false) << "->"
                << keyName(tonicMajRoot, true) << reason;
            promote(tonicMajRoot, true);
            outAmbiguous = true;
            outCorrectionReason = reason;
        }
    }

    // Re-read best/runner after possible supertonic correction
    best   = scores[0];
    runner = scores[1];
    margin = best.score - runner.score;

    // ── Standard ambiguity zone ──
    constexpr double kAmbiguityThreshold = 0.05;

    if (margin < kAmbiguityThreshold && outCorrectionReason.isEmpty()) {
        outAmbiguous = true;

        qInfo().noquote() << QString("[KEY_DETECT] AMBIGUITY: %1 (%2) vs %3 (%4) margin=%5 relative=%6 fifth=%7")
            .arg(keyName(best.root, best.major))
            .arg(best.score, 0, 'f', 4)
            .arg(keyName(runner.root, runner.major))
            .arg(runner.score, 0, 'f', 4)
            .arg(margin, 0, 'f', 4)
            .arg(isRelativePair ? "yes" : "no")
            .arg(isFifthPair ? "yes" : "no");

        // ── Relative-pair correction (minor → major) ──
        if (isRelativePair && !best.major) {
            int relMajRoot = (best.root + 3) % 12;
            double majThirdEvidence = chroma[(relMajRoot + 4) % 12];
            double minThirdEvidence = chroma[(best.root + 3) % 12];
            bool brightTrack = (spectralCentroid > 2500.0);

            bool preferMajor = false;
            QString reason;

            if (margin < 0.03 && brightTrack) {
                preferMajor = true;
                reason = QString("bright track (centroid=%1Hz), margin=%2")
                    .arg(spectralCentroid, 0, 'f', 0)
                    .arg(margin, 0, 'f', 4);
            } else if (margin < 0.03
                       && majThirdEvidence > minThirdEvidence * 1.15) {
                preferMajor = true;
                reason = QString("major-3rd evidence (%1 > %2*1.15), margin=%3")
                    .arg(majThirdEvidence, 0, 'f', 4)
                    .arg(minThirdEvidence, 0, 'f', 4)
                    .arg(margin, 0, 'f', 4);
            } else if (margin < 0.01) {
                preferMajor = true;
                reason = QString("very tight margin=%1, relative major preferred")
                    .arg(margin, 0, 'f', 4);
            }

            if (preferMajor) {
                qInfo().noquote() << "[KEY_DETECT] RELATIVE_CORRECTION:"
                    << keyName(best.root, best.major) << "->"
                    << keyName(relMajRoot, true) << reason;
                promote(relMajRoot, true);
                outCorrectionReason = reason;
            }
        }

        // ── Fifth-pair flagging ──
        if (isFifthPair && margin < 0.02 && outCorrectionReason.isEmpty()) {
            outCorrectionReason = QString("fifth-pair ambiguity, margin=%1")
                .arg(margin, 0, 'f', 4);
        }

        // ── Power-chord / flat-7 rock check ──
        if (best.major && runner.major && margin < 0.02) {
            int diff = (runner.root - best.root + 12) % 12;
            if (diff == 10) {
                if (outCorrectionReason.isEmpty()) {
                    outCorrectionReason = QString("flat-7 rock bias, %1 preferred over %2")
                        .arg(keyName(best.root, true))
                        .arg(keyName(runner.root, true));
                }
            } else if (diff == 2) {
                double bestTonic  = chroma[best.root];
                double runnerTonic = chroma[runner.root];
                if (runnerTonic > bestTonic * 1.1 && margin < 0.01) {
                    promote(runner.root, true);
                    outCorrectionReason = QString("flat-7 rock: %1 tonic stronger (%2 > %3)")
                        .arg(keyName(runner.root, true))
                        .arg(runnerTonic, 0, 'f', 4)
                        .arg(bestTonic, 0, 'f', 4);
                }
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 6b — Major vs Relative Minor Resolver
//
//  If minor wins, challenge it against the relative major by comparing
//  3rd-interval chroma energy.  This is the same approach used by
//  Serato / Traktor to avoid locking onto energy-center instead of
//  musical key in guitar-driven rock and pop.
// ════════════════════════════════════════════════════════════════════

void KeyDetectionService::challengeMinorWithRelativeMajor(
    std::vector<KeyScore>& scores,
    const double chroma[12],
    bool& outAmbiguous,
    QString& outCorrectionReason)
{
    if (scores.size() < 2) return;

    const auto& best = scores[0];

    // Only challenge minor results
    if (best.major) return;

    // If a prior correction already flipped the result, don't override
    if (!outCorrectionReason.isEmpty()) return;

    int minPc = best.root;                  // e.g. 4 (E)
    int majPc = (minPc + 3) % 12;          // relative major: e.g. 7 (G)

    // Find scores for relative major
    double majorScore = -1e9;
    double minorScore = best.score;
    for (const auto& ks : scores) {
        if (ks.root == majPc && ks.major) {
            majorScore = ks.score;
            break;
        }
    }

    double ratio = majorScore / (minorScore + 1e-9);

    qInfo().noquote() << QString("[KEY_DETECT] RELATIVE_MINOR_RESOLVER: "
                                  "%1 (minor, score=%2) vs %3 (major, score=%4) "
                                  "ratio=%5")
        .arg(keyName(minPc, false))
        .arg(minorScore, 0, 'f', 4)
        .arg(keyName(majPc, true))
        .arg(majorScore, 0, 'f', 4)
        .arg(ratio, 0, 'f', 4);

    if (ratio <= 0.85) {
        qInfo().noquote() << "[KEY_DETECT] RELATIVE_MINOR_RESOLVER: "
                             "major too weak (ratio<=0.85), keeping minor";
        return;
    }

    // ── Resolve using 3rd-interval chroma energy ──
    // Major 3rd of the relative major key
    double majorThirdEnergy = chroma[majPc] + chroma[(majPc + 4) % 12];
    // Minor 3rd of the current minor key
    double minorThirdEnergy = chroma[minPc] + chroma[(minPc + 3) % 12];

    qInfo().noquote() << QString("[KEY_DETECT] RELATIVE_MINOR_RESOLVER: "
                                  "majorThirdEnergy=%1 (root=%2 + M3=%3) "
                                  "minorThirdEnergy=%4 (root=%5 + m3=%6)")
        .arg(majorThirdEnergy, 0, 'f', 4)
        .arg(chroma[majPc], 0, 'f', 4)
        .arg(chroma[(majPc + 4) % 12], 0, 'f', 4)
        .arg(minorThirdEnergy, 0, 'f', 4)
        .arg(chroma[minPc], 0, 'f', 4)
        .arg(chroma[(minPc + 3) % 12], 0, 'f', 4);

    bool preferMajor = false;
    QString reason;

    if (majorThirdEnergy > minorThirdEnergy * 1.15) {
        preferMajor = true;
        reason = QString("relative-major-resolver: M3_energy=%1 > m3_energy*1.15=%2")
            .arg(majorThirdEnergy, 0, 'f', 4)
            .arg(minorThirdEnergy * 1.15, 0, 'f', 4);
    } else if (minorThirdEnergy > majorThirdEnergy * 1.15) {
        // Minor 3rd clearly dominant → keep minor
        qInfo().noquote() << "[KEY_DETECT] RELATIVE_MINOR_RESOLVER: "
                             "minor 3rd dominant, keeping minor";
        return;
    } else {
        // Ambiguous → default to major for rock/pop
        preferMajor = true;
        reason = QString("relative-major-resolver: ambiguous 3rds "
                         "(M3=%1 m3=%2), defaulting to major")
            .arg(majorThirdEnergy, 0, 'f', 4)
            .arg(minorThirdEnergy, 0, 'f', 4);
    }

    if (preferMajor) {
        qInfo().noquote() << "[KEY_DETECT] RELATIVE_MINOR_CORRECTION:"
            << keyName(minPc, false) << "->"
            << keyName(majPc, true) << reason;

        // Promote relative major to #1
        for (auto& ks : scores) {
            if (ks.root == majPc && ks.major) {
                ks.score = scores[0].score + 0.001;
                break;
            }
        }
        std::sort(scores.begin(), scores.end(),
                  [](const KeyScore& a, const KeyScore& b) {
                      return a.score > b.score;
                  });

        outAmbiguous = true;
        outCorrectionReason = reason;
        return;
    }

    // ── Parent-tonic (supertonic) challenge ──
    // If the relative-major check didn't flip, also consider the
    // parent tonic major (e.g., Em is ii of D Major).  This is a
    // second chance for the supertonic correction if resolveAmbiguity()
    // thresholds were not met.
    int parentPc = (minPc + 10) % 12;  // root - 2 mod 12 → parent tonic
    double parentScore = -1e9;
    for (const auto& ks : scores) {
        if (ks.root == parentPc && ks.major) {
            parentScore = ks.score;
            break;
        }
    }
    double parentRatio = parentScore / (minorScore + 1e-9);

    // Parent-tonic chroma evidence: root + major 3rd + 5th
    double pRoot  = chroma[parentPc];
    double pThird = chroma[(parentPc + 4) % 12];
    double pFifth = chroma[(parentPc + 7) % 12];
    double parentTriad  = pRoot + pThird + pFifth;
    double iiTriad = chroma[minPc] + chroma[(minPc + 3) % 12]
                   + chroma[(minPc + 7) % 12];

    qInfo().noquote() << QString("[KEY_DETECT] PARENT_TONIC_RESOLVER: "
                                  "%1 (minor, score=%2) vs %3 (parent, score=%4) "
                                  "ratio=%5 parentTriad=%6 iiTriad=%7")
        .arg(keyName(minPc, false))
        .arg(minorScore, 0, 'f', 4)
        .arg(keyName(parentPc, true))
        .arg(parentScore, 0, 'f', 4)
        .arg(parentRatio, 0, 'f', 4)
        .arg(parentTriad, 0, 'f', 4)
        .arg(iiTriad, 0, 'f', 4);

    if (parentRatio > 0.85 && parentTriad > iiTriad * 0.90) {
        QString pReason = QString("parent-tonic-resolver: parentTriad=%1 > "
                                  "iiTriad*0.90=%2, ratio=%3")
            .arg(parentTriad, 0, 'f', 4)
            .arg(iiTriad * 0.90, 0, 'f', 4)
            .arg(parentRatio, 0, 'f', 4);

        qInfo().noquote() << "[KEY_DETECT] PARENT_TONIC_CORRECTION:"
            << keyName(minPc, false) << "->"
            << keyName(parentPc, true) << pReason;

        for (auto& ks : scores) {
            if (ks.root == parentPc && ks.major) {
                ks.score = scores[0].score + 0.001;
                break;
            }
        }
        std::sort(scores.begin(), scores.end(),
                  [](const KeyScore& a, const KeyScore& b) {
                      return a.score > b.score;
                  });

        outAmbiguous = true;
        outCorrectionReason = pReason;
    }
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 6c — DJ Reinterpretation Layer
//
//  Post-detection heuristic that adjusts the scientific key result
//  toward DJ-usable keys.  Standard chroma+profile correlation
//  produces musically accurate pitch-class analysis, but DJs need
//  the harmonic-mixing key — which for rock/pop/guitar-driven music
//  is often the parallel major rather than the minor mode the
//  algorithm "correctly" detects.
//
//  Rules:
//    1. Same-root minor→major flip (Em→E Major) when margin is small
//    2. Window-majority override (if 2/3+ windows disagree with merged)
//    3. Bright-signal major bias (high spectral centroid + minor winner)
// ════════════════════════════════════════════════════════════════════

void KeyDetectionService::djReinterpret(
    std::vector<KeyScore>& scores,
    const double chroma[12],
    const std::vector<WindowChroma>& windows,
    double spectralCentroid,
    bool& outAmbiguous,
    QString& outCorrectionReason)
{
    if (scores.size() < 2) return;

    // If a prior layer already corrected, don't override
    if (!outCorrectionReason.isEmpty()) {
        qInfo().noquote() << "[KEY_DETECT] DJ_REINTERPRET: skipped — prior correction active";
        return;
    }

    const auto& best   = scores[0];
    const auto& runner = scores[1];
    double margin = best.score - runner.score;

    qInfo().noquote() << QString("[KEY_DETECT] DJ_REINTERPRET: best=%1 (%2) "
                                  "runner=%3 (%4) margin=%5 centroid=%6Hz")
        .arg(keyName(best.root, best.major))
        .arg(best.score, 0, 'f', 6)
        .arg(keyName(runner.root, runner.major))
        .arg(runner.score, 0, 'f', 6)
        .arg(margin, 0, 'f', 6)
        .arg(spectralCentroid, 0, 'f', 0);

    auto promote = [&](int root, bool major) {
        for (auto& ks : scores) {
            if (ks.root == root && ks.major == major) {
                ks.score = scores[0].score + 0.001;
                break;
            }
        }
        std::sort(scores.begin(), scores.end(),
                  [](const KeyScore& a, const KeyScore& b) {
                      return a.score > b.score;
                  });
    };

    // ── Rule 1: Same-root minor→major flip ──────────────────────
    //  If #1 is minor and #2 is the SAME ROOT major, the signal has
    //  strong energy on that root but the minor template correlates
    //  slightly better.  In rock/pop this is almost always a parallel
    //  major — DJs should mix on the major wheel position.
    //
    //  Threshold: margin < 0.10 (conservative enough to avoid
    //  flipping genuinely minor tracks like Summertime Sadness).
    if (!best.major && runner.major && best.root == runner.root
        && margin < 0.10) {

        // Extra evidence: check window majority
        int windowsMajor = 0, windowsTotal = 0;
        for (const auto& w : windows) {
            if (w.confidence <= 0) continue;
            ++windowsTotal;
            if (w.bestKey == best.root && w.bestMajor) ++windowsMajor;
        }

        qInfo().noquote() << QString("[KEY_DETECT] DJ_REINTERPRET_RULE1: same-root minor→major "
                                      "%1→%2 margin=%3 windowsMajor=%4/%5")
            .arg(keyName(best.root, false))
            .arg(keyName(runner.root, true))
            .arg(margin, 0, 'f', 6)
            .arg(windowsMajor).arg(windowsTotal);

        // Fire if: margin is small, OR majority of windows agree on major
        if (margin < 0.06 || windowsMajor > windowsTotal / 2) {
            promote(best.root, true);
            outAmbiguous = true;
            outCorrectionReason = QString("dj-reinterpret-rule1: same-root minor→major "
                                          "(margin=%1, windows=%2/%3 major)")
                .arg(margin, 0, 'f', 4)
                .arg(windowsMajor).arg(windowsTotal);
            qInfo().noquote() << "[KEY_DETECT] DJ_REINTERPRET_CORRECTION:"
                << keyName(best.root, false) << "->"
                << keyName(best.root, true)
                << outCorrectionReason;
            return;
        }
    }

    // ── Rule 2: Window-majority override ─────────────────────────
    //  If 2/3+ windows agree on a key different from the merged
    //  winner, the merged result may be skewed by one dominant window.
    //  Trust the majority.
    if (windows.size() >= 3) {
        // Count votes for each (root, major) pair
        struct KeyVote { int root; bool major; int votes = 0; };
        std::vector<KeyVote> votes;

        for (const auto& w : windows) {
            if (w.confidence <= 0) continue;
            bool found = false;
            for (auto& v : votes) {
                if (v.root == w.bestKey && v.major == w.bestMajor) {
                    ++v.votes;
                    found = true;
                    break;
                }
            }
            if (!found) votes.push_back({w.bestKey, w.bestMajor, 1});
        }

        // Find the majority winner
        int maxVotes = 0;
        int majRoot = -1;
        bool majMajor = true;
        for (const auto& v : votes) {
            if (v.votes > maxVotes) {
                maxVotes = v.votes;
                majRoot  = v.root;
                majMajor = v.major;
            }
        }

        int validWindows = 0;
        for (const auto& w : windows)
            if (w.confidence > 0) ++validWindows;

        // Override if majority disagrees with merged winner
        if (maxVotes > validWindows / 2
            && (majRoot != best.root || majMajor != best.major)) {

            // Only override if the majority key has a decent profile score
            double majScore = -1e9;
            for (const auto& ks : scores) {
                if (ks.root == majRoot && ks.major == majMajor) {
                    majScore = ks.score;
                    break;
                }
            }

            qInfo().noquote() << QString("[KEY_DETECT] DJ_REINTERPRET_RULE2: window-majority "
                                          "%1 (%2/%3 windows) vs merged %4 (majScore=%5)")
                .arg(keyName(majRoot, majMajor))
                .arg(maxVotes).arg(validWindows)
                .arg(keyName(best.root, best.major))
                .arg(majScore, 0, 'f', 6);

            // Only flip if the window-majority key has a score within 0.15
            // of the merged winner (don't let noise windows override)
            if (best.score - majScore < 0.15) {
                promote(majRoot, majMajor);
                outAmbiguous = true;
                outCorrectionReason = QString("dj-reinterpret-rule2: window-majority "
                                              "%1 (%2/%3 windows, score=%4)")
                    .arg(keyName(majRoot, majMajor))
                    .arg(maxVotes).arg(validWindows)
                    .arg(majScore, 0, 'f', 4);
                qInfo().noquote() << "[KEY_DETECT] DJ_REINTERPRET_CORRECTION:"
                    << keyName(best.root, best.major) << "->"
                    << keyName(majRoot, majMajor)
                    << outCorrectionReason;
                return;
            }
        }
    }

    // ── Rule 3: Bright-signal major bias ─────────────────────────
    //  Rock/pop tracks with high spectral centroid (bright mix with
    //  guitars, cymbals) that land on minor are very often actually
    //  major.  If any major key with the same root OR relative major
    //  is close, prefer it.
    if (!best.major && spectralCentroid > 2500.0 && margin < 0.12) {
        // Check same-root major
        int sameRoot = best.root;
        double sameRootScore = -1e9;
        for (const auto& ks : scores) {
            if (ks.root == sameRoot && ks.major) {
                sameRootScore = ks.score;
                break;
            }
        }

        // Check relative major (minor root + 3 semitones)
        int relMajRoot = (best.root + 3) % 12;
        double relMajScore = -1e9;
        for (const auto& ks : scores) {
            if (ks.root == relMajRoot && ks.major) {
                relMajScore = ks.score;
                break;
            }
        }

        // Pick the best major alternative
        int pickRoot = -1;
        bool pickMajor = true;
        double pickScore = -1e9;

        if (sameRootScore > pickScore && best.score - sameRootScore < 0.12) {
            pickRoot = sameRoot;
            pickScore = sameRootScore;
        }
        if (relMajScore > pickScore && best.score - relMajScore < 0.12) {
            pickRoot = relMajRoot;
            pickScore = relMajScore;
        }

        qInfo().noquote() << QString("[KEY_DETECT] DJ_REINTERPRET_RULE3: bright-major-bias "
                                      "centroid=%1Hz minor=%2 sameRootMaj=%3(score=%4) "
                                      "relMaj=%5(score=%6)")
            .arg(spectralCentroid, 0, 'f', 0)
            .arg(keyName(best.root, false))
            .arg(keyName(sameRoot, true))
            .arg(sameRootScore, 0, 'f', 6)
            .arg(keyName(relMajRoot, true))
            .arg(relMajScore, 0, 'f', 6);

        if (pickRoot >= 0) {
            promote(pickRoot, pickMajor);
            outAmbiguous = true;
            outCorrectionReason = QString("dj-reinterpret-rule3: bright-major-bias "
                                          "(centroid=%1Hz, %2→%3, score=%4)")
                .arg(spectralCentroid, 0, 'f', 0)
                .arg(keyName(best.root, false))
                .arg(keyName(pickRoot, true))
                .arg(pickScore, 0, 'f', 4);
            qInfo().noquote() << "[KEY_DETECT] DJ_REINTERPRET_CORRECTION:"
                << keyName(best.root, false) << "->"
                << keyName(pickRoot, true)
                << outCorrectionReason;
            return;
        }
    }

    qInfo().noquote() << "[KEY_DETECT] DJ_REINTERPRET: no rule fired — keeping"
        << keyName(best.root, best.major);
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 7 — Confidence scoring
// ════════════════════════════════════════════════════════════════════

double KeyDetectionService::computeConfidence(
    const std::vector<KeyScore>& scores,
    const std::vector<WindowChroma>& windows)
{
    if (scores.size() < 2) return 0.0;

    // Component 1: score gap  (#1 vs #2)
    // A gap of 0.15+  = very clear;  0.0 = ambiguous
    double gap = scores[0].score - scores[1].score;
    double gapScore = std::min(1.0, gap / 0.15);

    // Component 2: cross-window agreement
    double agreementScore = 1.0;
    if (windows.size() >= 2) {
        int agree = 0;
        int valid = 0;
        for (const auto& w : windows) {
            if (w.confidence <= 0.0) continue;
            ++valid;
            if (w.bestKey == scores[0].root && w.bestMajor == scores[0].major) {
                ++agree;
            }
        }
        if (valid > 0) {
            agreementScore = static_cast<double>(agree)
                           / static_cast<double>(valid);
        }
    }

    // Component 3: absolute correlation quality
    // Good > 0.7,  weak < 0.3
    double qualityScore = std::min(1.0,
        std::max(0.0, (scores[0].score - 0.3) / 0.5));

    // Weighted combination
    double raw = gapScore * 0.40
               + agreementScore * 0.35
               + qualityScore * 0.25;
    return std::min(1.0, std::max(0.0, raw));
}

// ════════════════════════════════════════════════════════════════════
//  LAYER 8 — Camelot mapping + key naming
// ════════════════════════════════════════════════════════════════════

QString KeyDetectionService::keyName(int root, bool major)
{
    QString name = QLatin1String(kNoteNames[root % 12]);
    name += major ? QStringLiteral(" Major") : QStringLiteral(" Minor");
    return name;
}

QString KeyDetectionService::mapToCamelot(int root, bool major)
{
    // Verified correct: D Major=10B, B Minor=10A, E Minor=9A, G Major=9B
    static const int camelotMajor[12] = {
        8, 3, 10, 5, 12, 7, 2, 9, 4, 11, 6, 1
    }; // C C# D Eb E F F# G Ab A Bb B
    static const int camelotMinor[12] = {
        5, 12, 7, 2, 9, 4, 11, 6, 1, 8, 3, 10
    }; // Cm C#m Dm Ebm Em Fm F#m Gm Abm Am Bbm Bm

    int num = major ? camelotMajor[root % 12] : camelotMinor[root % 12];
    return QString::number(num) + (major ? QStringLiteral("B")
                                         : QStringLiteral("A"));
}

// ════════════════════════════════════════════════════════════════════
//  MAIN PIPELINE — Orchestrate all layers
// ════════════════════════════════════════════════════════════════════

KeyAnalysisResult KeyDetectionService::detect(const float* monoData,
                                              int64_t numSamples,
                                              double sampleRate,
                                              double spectralCentroid)
{
    KeyAnalysisResult result;

    if (numSamples < 4096) {
        result.finalKey     = QStringLiteral("--");
        result.finalCamelot = QStringLiteral("--");
        return result;
    }

    qDebug() << "[KEY_DETECT] START samples=" << numSamples
             << "sr=" << sampleRate
             << "centroid=" << spectralCentroid;

    // ── Layer 1: Preprocess ──
    auto processed = preprocessForKey(monoData, numSamples, sampleRate);
    qDebug() << "[KEY_DETECT] LAYER1_PREPROCESSED";

    // ── Layer 2: Build chroma frames ──
    auto frames = buildChromaFrames(processed.data(),
                                    static_cast<int64_t>(processed.size()),
                                    sampleRate);
    qDebug() << "[KEY_DETECT] LAYER2_CHROMA_FRAMES count=" << frames.size();

    if (frames.empty()) {
        result.finalKey     = QStringLiteral("--");
        result.finalCamelot = QStringLiteral("--");
        return result;
    }

    // ── Layer 3: Multi-window aggregation ──
    // Skip first 5% (intro) and last 10% (outro/fade)
    size_t n         = frames.size();
    size_t introEnd  = n / 20;
    size_t outroStart = n * 9 / 10;
    if (outroStart <= introEnd) {
        introEnd = 0;
        outroStart = n;
    }
    size_t usable = outroStart - introEnd;
    size_t third  = std::max(size_t(1), usable / 3);

    std::vector<WindowChroma> windows;
    windows.push_back(aggregateSection(frames, introEnd,
                                        introEnd + third));               // early
    windows.push_back(aggregateSection(frames, introEnd + third,
                                        introEnd + 2 * third));           // middle
    windows.push_back(aggregateSection(frames, introEnd + 2 * third,
                                        outroStart));                     // late

    // Score each window to get per-window best key
    for (size_t wi = 0; wi < windows.size(); ++wi) {
        auto& w = windows[wi];
        if (w.confidence <= 0.0) continue;
        auto wScores = scoreKeyProfiles(w.bins);
        if (!wScores.empty()) {
            w.bestKey   = wScores[0].root;
            w.bestMajor = wScores[0].major;
        }
        // ── FORENSIC: per-window detail ──
        {
            QString wChroma;
            for (int pc = 0; pc < 12; ++pc) {
                if (!wChroma.isEmpty()) wChroma += QStringLiteral(", ");
                wChroma += QString("%1=%2")
                    .arg(QLatin1String(kNoteNames[pc]))
                    .arg(w.bins[pc], 0, 'f', 6);
            }
            qInfo().noquote() << QString("[KEY_DETECT] WINDOW_%1_CHROMA: %2 (confidence=%3)")
                .arg(wi).arg(wChroma).arg(w.confidence, 0, 'f', 4);
            int topN = std::min(5, static_cast<int>(wScores.size()));
            for (int i = 0; i < topN; ++i) {
                qInfo().noquote() << QString("[KEY_DETECT] WINDOW_%1_CANDIDATE #%2: %3 (%4) score=%5")
                    .arg(wi).arg(i + 1)
                    .arg(keyName(wScores[i].root, wScores[i].major))
                    .arg(mapToCamelot(wScores[i].root, wScores[i].major))
                    .arg(wScores[i].score, 0, 'f', 6);
            }
        }
    }

    // Reject windows with no real chroma content
    for (auto& w : windows) {
        double sum = 0.0;
        for (int pc = 0; pc < 12; ++pc) sum += w.bins[pc];
        if (sum < 1e-15) w.confidence = 0.0;
    }

    // Merge surviving windows into final chroma
    double mergedChroma[12] = {};
    mergeWindows(windows, mergedChroma);

    int validWindows = static_cast<int>(
        std::count_if(windows.begin(), windows.end(),
                      [](const WindowChroma& w) { return w.confidence > 0; }));
    qDebug() << "[KEY_DETECT] LAYER3_WINDOWS valid=" << validWindows
             << "of" << windows.size();

    // Log merged chroma
    {
        QString chromaStr;
        for (int pc = 0; pc < 12; ++pc) {
            if (!chromaStr.isEmpty()) chromaStr += QStringLiteral(", ");
            chromaStr += QString("%1=%2")
                .arg(QLatin1String(kNoteNames[pc]))
                .arg(mergedChroma[pc], 0, 'f', 4);
        }
        qInfo().noquote() << "[KEY_DETECT] MERGED_CHROMA:" << chromaStr;
    }

    // ── Layer 4: Score all 24 keys ──
    auto scores = scoreKeyProfiles(mergedChroma);

    qInfo().noquote() << "[KEY_DETECT] LAYER4_RAW_CANDIDATES (all 24):";
    for (int i = 0; i < static_cast<int>(scores.size()); ++i) {
        qInfo().noquote() << QString("  #%1: %2 (%3) score=%4")
            .arg(i + 1, 2)
            .arg(keyName(scores[i].root, scores[i].major), -12)
            .arg(mapToCamelot(scores[i].root, scores[i].major), -4)
            .arg(scores[i].score, 0, 'f', 6);
    }

    // ── Capture raw key before resolution ──
    QString rawKey = keyName(scores[0].root, scores[0].major);
    QString rawCamelot = mapToCamelot(scores[0].root, scores[0].major);

    // ── Layer 5+6: Ambiguity resolution ──
    bool ambiguous = false;
    QString correctionReason;
    resolveAmbiguity(scores, mergedChroma, spectralCentroid,
                     ambiguous, correctionReason);

    // ── Layer 6b: Major vs Relative Minor Resolver ──
    // If minor still wins after ambiguity resolution, challenge it
    // against the relative major using 3rd-interval chroma energy.
    challengeMinorWithRelativeMajor(scores, mergedChroma,
                                    ambiguous, correctionReason);

    // ── Layer 6c: DJ Reinterpretation ──
    // Post-detection heuristic that biases toward DJ-usable keys.
    // Same-root minor→major, window-majority, bright-signal bias.
    djReinterpret(scores, mergedChroma, windows, spectralCentroid,
                  ambiguous, correctionReason);

    // ── Diagnostic: raw vs resolved ──
    QString resolvedKey = keyName(scores[0].root, scores[0].major);
    QString resolvedCamelot = mapToCamelot(scores[0].root, scores[0].major);
    qInfo().noquote() << QString("[KEY_DETECT] PIPELINE_STAGE: rawKey=%1 (%2) "
                                  "resolvedKey=%3 (%4) corrected=%5")
        .arg(rawKey).arg(rawCamelot)
        .arg(resolvedKey).arg(resolvedCamelot)
        .arg(rawKey != resolvedKey ? "YES" : "NO");

    // ── Layer 7: Confidence ──
    double confidence = computeConfidence(scores, windows);

    // ── Layer 8: Final mapping ──
    int  finalRoot  = scores[0].root;
    bool finalMajor = scores[0].major;

    result.finalKey          = keyName(finalRoot, finalMajor);
    result.finalCamelot      = mapToCamelot(finalRoot, finalMajor);
    result.confidence        = confidence;
    result.ambiguous         = ambiguous;
    result.correctionReason  = correctionReason;

    // Runner-up
    if (scores.size() >= 2) {
        result.runnerUpKey = keyName(scores[1].root, scores[1].major);
    }

    // Top candidates
    result.candidateCount = std::min(5, static_cast<int>(scores.size()));
    for (int i = 0; i < result.candidateCount; ++i) {
        result.topCandidates[i].musicalKey =
            keyName(scores[i].root, scores[i].major);
        result.topCandidates[i].camelot =
            mapToCamelot(scores[i].root, scores[i].major);
        result.topCandidates[i].score = scores[i].score;
    }

    // ── Final logging ──
    qInfo().noquote() << QString("[KEY_DETECT] RESULT: %1 (%2)"
                                 " confidence=%3 ambiguous=%4%5")
        .arg(result.finalKey)
        .arg(result.finalCamelot)
        .arg(confidence, 0, 'f', 2)
        .arg(ambiguous ? "yes" : "no")
        .arg(correctionReason.isEmpty()
             ? QString()
             : QString(" correction=%1").arg(correctionReason));

    if (scores.size() >= 2) {
        qInfo().noquote() << QString("[KEY_DETECT] RUNNER_UP: %1 (%2) score=%3")
            .arg(result.runnerUpKey)
            .arg(mapToCamelot(scores[1].root, scores[1].major))
            .arg(scores[1].score, 0, 'f', 4);
    }

    // Per-window key agreement
    {
        QString wLog;
        for (size_t i = 0; i < windows.size(); ++i) {
            const auto& w = windows[i];
            if (w.confidence <= 0) continue;
            if (!wLog.isEmpty()) wLog += QStringLiteral(", ");
            wLog += QString("w%1=%2").arg(i)
                        .arg(keyName(w.bestKey, w.bestMajor));
        }
        qInfo().noquote() << "[KEY_DETECT] WINDOW_KEYS:" << wLog;
    }

    // ── FORENSIC: explicit Em vs D Major comparison ──
    {
        // Find Em and D Major scores from raw (pre-resolution) list
        double emScore = -1e9, dMajScore = -1e9;
        for (const auto& ks : scores) {
            if (ks.root == 4 && !ks.major) emScore = ks.score;      // E=4, minor
            if (ks.root == 2 && ks.major)  dMajScore = ks.score;    // D=2, major
        }
        double eTonic   = mergedChroma[4];  // E
        double eMinor3  = mergedChroma[7];  // G (minor 3rd of Em)
        double eFifth   = mergedChroma[11]; // B (5th of Em)
        double dTonic   = mergedChroma[2];  // D
        double dMajor3  = mergedChroma[6];  // F# (major 3rd of D)
        double dFifth   = mergedChroma[9];  // A (5th of D)
        double emTriad  = eTonic + eMinor3 + eFifth;
        double dTriad   = dTonic + dMajor3 + dFifth;
        qInfo().noquote() << "════════════════════════════════════════════════════════════════";
        qInfo().noquote() << "[KEY_DETECT] FORENSIC: Em vs D Major";
        qInfo().noquote() << QString("  Em_score=%1  D_Major_score=%2  margin=%3")
            .arg(emScore, 0, 'f', 6).arg(dMajScore, 0, 'f', 6)
            .arg(emScore - dMajScore, 0, 'f', 6);
        qInfo().noquote() << QString("  Em_triad=%1  (E=%2 G=%3 B=%4)")
            .arg(emTriad, 0, 'f', 6).arg(eTonic, 0, 'f', 6)
            .arg(eMinor3, 0, 'f', 6).arg(eFifth, 0, 'f', 6);
        qInfo().noquote() << QString("  D_triad=%1   (D=%2 F#=%3 A=%4)")
            .arg(dTriad, 0, 'f', 6).arg(dTonic, 0, 'f', 6)
            .arg(dMajor3, 0, 'f', 6).arg(dFifth, 0, 'f', 6);
        qInfo().noquote() << QString("  triad_ratio(D/Em)=%1  D_triad>Em*0.88=%2  D_triad>Em=%3")
            .arg(dTriad / (emTriad + 1e-9), 0, 'f', 4)
            .arg(dTriad > emTriad * 0.88 ? "YES" : "NO")
            .arg(dTriad > emTriad ? "YES" : "NO");
        // Where does D tonic rank among the 12 chroma bins?
        int dRank = 0;
        for (int pc = 0; pc < 12; ++pc)
            if (mergedChroma[pc] > dTonic) ++dRank;
        qInfo().noquote() << QString("  D_tonic_rank=%1/12  (rank 1 = strongest)")
            .arg(dRank + 1);
        qInfo().noquote() << "════════════════════════════════════════════════════════════════";
    }

    return result;
}
