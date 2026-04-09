#include "AudioAnalysisService.h"
#include "KeyDetectionService.h"
#include "BpmResolverService.h"

#include <juce_audio_formats/juce_audio_formats.h>

#include <QDebug>
#include <QFileInfo>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

AudioAnalysisService::AudioAnalysisService(QObject* parent)
    : QObject(parent)
{
}

// ════════════════════════════════════════════════════════════════════
//  MAIN PIPELINE
// ════════════════════════════════════════════════════════════════════

AnalysisResult AudioAnalysisService::analyzeFile(const QString& filePath,
                                                   const QString& genreHint)
{
    AnalysisResult r;
    qDebug() << "[ANALYSIS] ANALYSIS_START" << filePath;

    // ── 1. Validate file ──
    QFileInfo fi(filePath);
    if (!fi.exists() || !fi.isFile()) {
        r.errorMsg = QStringLiteral("File not found: ") + filePath;
        qDebug() << "[ANALYSIS] ANALYSIS_FAIL" << r.errorMsg;
        return r;
    }

    // ── 2. Decode audio via JUCE ──
    juce::AudioFormatManager formatManager;
    formatManager.registerBasicFormats();

    juce::File juceFile(filePath.toStdString().c_str());
    std::unique_ptr<juce::AudioFormatReader> reader(
        formatManager.createReaderFor(juceFile));

    if (!reader) {
        r.errorMsg = QStringLiteral("No codec for: ") + filePath;
        qDebug() << "[ANALYSIS] ANALYSIS_FAIL" << r.errorMsg;
        return r;
    }

    const int64_t numFrames = static_cast<int64_t>(reader->lengthInSamples);
    const double  sr        = reader->sampleRate;
    const int     channels  = static_cast<int>(reader->numChannels);

    if (numFrames <= 0 || sr <= 0.0) {
        r.errorMsg = QStringLiteral("Empty or invalid audio");
        qDebug() << "[ANALYSIS] ANALYSIS_FAIL" << r.errorMsg;
        return r;
    }

    r.durationSeconds = static_cast<double>(numFrames) / sr;
    r.sampleRate      = sr;

    qDebug() << "[ANALYSIS] DECODED frames=" << numFrames
             << "sr=" << sr << "ch=" << channels
             << "duration=" << r.durationSeconds;

    // ── 3. Read into buffers ──
    // For analysis we limit to first 10 minutes to avoid excessive memory
    const int64_t maxFrames = std::min(numFrames,
                                        static_cast<int64_t>(sr * 600.0));

    std::vector<float> left(static_cast<size_t>(maxFrames));
    std::vector<float> right(static_cast<size_t>(maxFrames));

    // JUCE reads into float** (non-interleaved)
    // Read in chunks to limit stack-frame pointer arrays
    constexpr int64_t kChunk = 65536;
    for (int64_t pos = 0; pos < maxFrames; pos += kChunk) {
        const int64_t count = std::min(kChunk, maxFrames - pos);
        float* dest[2] = { left.data() + pos, right.data() + pos };
        reader->read(dest, (channels >= 2 ? 2 : 1),
                     pos, static_cast<int>(count));
        // Mono → duplicate to right channel
        if (channels < 2) {
            std::copy(left.data() + pos, left.data() + pos + count,
                      right.data() + pos);
        }
    }

    // Create mono mixdown for most analyses
    std::vector<float> mono(static_cast<size_t>(maxFrames));
    for (int64_t i = 0; i < maxFrames; ++i) {
        mono[static_cast<size_t>(i)] = (left[static_cast<size_t>(i)]
                                      + right[static_cast<size_t>(i)]) * 0.5f;
    }

    qDebug() << "[ANALYSIS] BUFFERS_READY mono_samples=" << maxFrames;

    // ── 4. Run analysis stages ──

    r.bpm = detectBPM(mono.data(), maxFrames, sr);
    qDebug() << "[ANALYSIS] ANALYSIS_BPM" << r.bpm;

    // ── BPM Resolver: choose best tempo family ──
    {
        BpmResolverService bpmResolver;
        auto bpmResult = bpmResolver.resolve(r.bpm, mono.data(), maxFrames, sr,
                                              genreHint);
        r.rawBpm          = bpmResult.rawBpm;
        r.resolvedBpm     = bpmResult.resolvedBpm;
        r.bpmConfidence   = bpmResult.confidence;
        r.bpmFamily       = bpmResult.selectedFamily;
        r.onsetDensity    = bpmResult.onsetDensity;
        r.hfPercussiveScore = bpmResult.hfPercussiveScore;
        r.bpmCandidates   = bpmResult.candidates;
        r.bpm             = bpmResult.resolvedBpm;  // overwrite with resolved
        qDebug() << "[ANALYSIS] ANALYSIS_BPM_RESOLVED raw=" << r.rawBpm
                 << "resolved=" << r.resolvedBpm
                 << "family=" << r.bpmFamily
                 << "confidence=" << r.bpmConfidence;
    }

    r.loudnessLUFS = detectLoudnessLUFS(left.data(), right.data(), maxFrames, sr);
    qDebug() << "[ANALYSIS] ANALYSIS_LOUDNESS" << r.loudnessLUFS;

    r.peakDBFS = detectPeakDBFS(mono.data(), maxFrames);
    qDebug() << "[ANALYSIS] ANALYSIS_PEAK" << r.peakDBFS;

    r.energy = detectEnergy(mono.data(), maxFrames);
    qDebug() << "[ANALYSIS] ANALYSIS_ENERGY" << r.energy;

    r.cueInSeconds = detectCueIn(mono.data(), maxFrames, sr);
    qDebug() << "[ANALYSIS] ANALYSIS_CUE_IN" << r.cueInSeconds;

    r.cueOutSeconds = detectCueOut(mono.data(), maxFrames, sr);
    qDebug() << "[ANALYSIS] ANALYSIS_CUE_OUT" << r.cueOutSeconds;

    r.dynamicRangeLU = detectDynamicRange(mono.data(), maxFrames, sr);
    r.lra = r.dynamicRangeLU;  // alias
    qDebug() << "[ANALYSIS] ANALYSIS_DYNAMIC_RANGE" << r.dynamicRangeLU;

    r.spectralCentroid = detectSpectralCentroid(mono.data(), maxFrames, sr);
    qDebug() << "[ANALYSIS] ANALYSIS_SPECTRAL_CENTROID" << r.spectralCentroid;

    // ── 5. Beat grid confidence (from BPM detection) ──
    // Re-measure with finer grain — ratio of autocorrelation peak to noise floor
    {
        // Simple estimate: if BPM is in typical range, confidence is higher
        if (r.bpm >= 60.0 && r.bpm <= 200.0) {
            r.beatGridConfidence = 0.8;
        } else if (r.bpm > 0.0) {
            r.beatGridConfidence = 0.4;
        }
    }

    // ── 6. Derived features ──

    r.danceability = computeDanceability(r.bpm, r.energy, r.beatGridConfidence);
    r.acousticness = computeAcousticness(r.spectralCentroid, r.dynamicRangeLU,
                                          r.energy);
    r.instrumentalness = computeInstrumentalness(mono.data(), maxFrames, sr);
    r.liveness = computeLiveness(mono.data(), maxFrames, sr);

    qDebug() << "[ANALYSIS] ANALYSIS_FEATURES"
             << "danceability=" << r.danceability
             << "acousticness=" << r.acousticness
             << "instrumentalness=" << r.instrumentalness
             << "liveness=" << r.liveness;

    // ── 7. Pro analysis ──

    {
        KeyDetectionService keyDetector;
        auto keyResult = keyDetector.detect(mono.data(), maxFrames, sr,
                                            r.spectralCentroid);
        r.camelotKey          = keyResult.finalCamelot;
        r.keyConfidence       = keyResult.confidence;
        r.keyAmbiguous        = keyResult.ambiguous;
        r.keyRunnerUp         = keyResult.runnerUpKey;
        r.keyCorrectionReason = keyResult.correctionReason;
    }
    qDebug() << "[ANALYSIS] ANALYSIS_CAMELOT" << r.camelotKey
             << "confidence=" << r.keyConfidence
             << "ambiguous=" << r.keyAmbiguous;

    // Section detection: rough intro/outro
    {
        // Intro = time until energy first exceeds 30% of peak sustained energy
        // Outro = time after energy drops below 30% of peak for the last time
        double peakRMS = 0.0;
        const int64_t windowSamples = static_cast<int64_t>(sr * 0.5); // 500ms windows
        const int64_t numWindows = maxFrames / windowSamples;

        std::vector<double> windowEnergies;
        windowEnergies.reserve(static_cast<size_t>(numWindows));

        for (int64_t w = 0; w < numWindows; ++w) {
            double sum = 0.0;
            for (int64_t i = 0; i < windowSamples; ++i) {
                double s = mono[static_cast<size_t>(w * windowSamples + i)];
                sum += s * s;
            }
            double rms = std::sqrt(sum / static_cast<double>(windowSamples));
            windowEnergies.push_back(rms);
            peakRMS = std::max(peakRMS, rms);
        }

        double threshold = peakRMS * 0.3;
        r.introDuration = 0.0;
        r.outroDuration = 0.0;

        // Find intro end
        for (size_t i = 0; i < windowEnergies.size(); ++i) {
            if (windowEnergies[i] >= threshold) {
                r.introDuration = static_cast<double>(i) * 0.5;
                break;
            }
        }
        // Find outro start
        for (size_t i = windowEnergies.size(); i > 0; --i) {
            if (windowEnergies[i - 1] >= threshold) {
                r.outroDuration = r.durationSeconds
                    - static_cast<double>(i) * 0.5;
                if (r.outroDuration < 0.0) r.outroDuration = 0.0;
                break;
            }
        }
    }

    r.transitionDifficulty = computeTransitionDifficulty(
        r.bpm, r.energy, r.dynamicRangeLU, r.introDuration);
    qDebug() << "[ANALYSIS] ANALYSIS_TRANSITION_DIFFICULTY"
             << r.transitionDifficulty;

    r.valid = true;
    qDebug() << "[ANALYSIS] ANALYSIS_COMPLETE" << filePath
             << "bpm=" << r.bpm
             << "lufs=" << r.loudnessLUFS
             << "energy=" << r.energy;

    return r;
}

// ════════════════════════════════════════════════════════════════════
//  BPM DETECTION — Onset envelope autocorrelation
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectBPM(const float* data, int64_t numSamples,
                                        double sampleRate)
{
    // 1. Build onset strength envelope (half-wave rectified spectral flux proxy)
    //    We use frame-by-frame RMS differences as a lightweight onset function.

    const int hopSize   = 512;
    const int frameSize = 1024;
    const int64_t numHops = (numSamples - frameSize) / hopSize;
    if (numHops < 2) return 0.0;

    std::vector<float> onsetEnv(static_cast<size_t>(numHops), 0.0f);

    // Compute RMS per frame
    std::vector<float> rmsVec(static_cast<size_t>(numHops));
    for (int64_t h = 0; h < numHops; ++h) {
        double sum = 0.0;
        const float* frame = data + h * hopSize;
        for (int i = 0; i < frameSize; ++i) {
            sum += static_cast<double>(frame[i]) * frame[i];
        }
        rmsVec[static_cast<size_t>(h)] = static_cast<float>(
            std::sqrt(sum / frameSize));
    }

    // Onset = positive first-difference of RMS (half-wave rectified)
    for (int64_t h = 1; h < numHops; ++h) {
        float diff = rmsVec[static_cast<size_t>(h)]
                   - rmsVec[static_cast<size_t>(h - 1)];
        onsetEnv[static_cast<size_t>(h)] = std::max(0.0f, diff);
    }

    // 2. Autocorrelation of onset envelope
    //    Search BPM range 60..200 → lag range in onset frames
    const double onsetRate = sampleRate / hopSize;  // frames/sec in onset domain
    const int lagMin = static_cast<int>(onsetRate * 60.0 / 200.0); // lag for 200 BPM
    const int lagMax = static_cast<int>(onsetRate * 60.0 / 60.0);  // lag for 60 BPM
    const int maxLag = std::min(lagMax, static_cast<int>(numHops / 2));

    if (lagMin >= maxLag) return 0.0;

    double bestCorr = 0.0;
    int    bestLag  = lagMin;

    for (int lag = lagMin; lag <= maxLag; ++lag) {
        double corr = 0.0;
        const int64_t limit = numHops - lag;
        for (int64_t i = 0; i < limit; ++i) {
            corr += static_cast<double>(onsetEnv[static_cast<size_t>(i)])
                  * onsetEnv[static_cast<size_t>(i + lag)];
        }
        // Normalize
        corr /= static_cast<double>(limit);

        if (corr > bestCorr) {
            bestCorr = corr;
            bestLag  = lag;
        }
    }

    if (bestLag <= 0) return 0.0;

    double bpm = onsetRate * 60.0 / bestLag;

    // Normalize to 60-200 BPM range (halve or double if outside)
    while (bpm > 200.0 && bpm > 0.0) bpm /= 2.0;
    while (bpm < 60.0  && bpm > 0.0) bpm *= 2.0;

    // Round to 1 decimal place
    return std::round(bpm * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  LOUDNESS — Simplified ITU-R BS.1770 integrated loudness
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectLoudnessLUFS(const float* left,
                                                  const float* right,
                                                  int64_t numSamples,
                                                  double sampleRate)
{
    // ITU-R BS.1770 simplified:
    // 1. K-weighting filter (we approximate with a high-shelf boost)
    // 2. Mean square per channel
    // 3. Sum and convert to LUFS

    // For a proper implementation we'd need the exact K-weighting biquad filters.
    // Here we use a simplified approach: pre-emphasis filter at ~1500 Hz
    // that approximates the K-weighting spectral shape.

    const int blockSize = static_cast<int>(sampleRate * 0.4); // 400ms blocks
    const int64_t numBlocks = numSamples / blockSize;
    if (numBlocks == 0) return -70.0; // silence

    // Pre-emphasis coefficient (approximates K-weighting boost at high freqs)
    const double preEmphCoeff = 0.95;

    std::vector<double> blockLoudness;
    blockLoudness.reserve(static_cast<size_t>(numBlocks));

    for (int64_t b = 0; b < numBlocks; ++b) {
        double sumL = 0.0, sumR = 0.0;
        double prevL = 0.0, prevR = 0.0;

        for (int i = 0; i < blockSize; ++i) {
            int64_t idx = b * blockSize + i;
            // Simple pre-emphasis (high-pass-ish approximation of K-weighting)
            double sL = static_cast<double>(left[static_cast<size_t>(idx)])
                      - preEmphCoeff * prevL;
            double sR = static_cast<double>(right[static_cast<size_t>(idx)])
                      - preEmphCoeff * prevR;
            prevL = left[static_cast<size_t>(idx)];
            prevR = right[static_cast<size_t>(idx)];
            sumL += sL * sL;
            sumR += sR * sR;
        }

        double meanSquare = (sumL + sumR) / (2.0 * blockSize);
        blockLoudness.push_back(meanSquare);
    }

    // Gate: absolute threshold at -70 LUFS
    // First pass: compute ungated average
    double ungatedSum = 0.0;
    for (double bl : blockLoudness) ungatedSum += bl;
    double ungatedMean = ungatedSum / static_cast<double>(blockLoudness.size());

    if (ungatedMean <= 0.0) return -70.0;

    double ungatedLUFS = -0.691 + 10.0 * std::log10(ungatedMean);

    // Relative threshold: ungatedLUFS - 10 LU
    double relThreshold = std::pow(10.0, (ungatedLUFS - 10.0 + 0.691) / 10.0);

    double gatedSum = 0.0;
    int gatedCount = 0;
    for (double bl : blockLoudness) {
        if (bl >= relThreshold) {
            gatedSum += bl;
            ++gatedCount;
        }
    }

    if (gatedCount == 0) return -70.0;

    double gatedMean = gatedSum / gatedCount;
    double lufs = -0.691 + 10.0 * std::log10(gatedMean);

    return std::round(lufs * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  PEAK — True peak in dBFS
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectPeakDBFS(const float* data,
                                             int64_t numSamples)
{
    float peak = 0.0f;
    for (int64_t i = 0; i < numSamples; ++i) {
        float absVal = std::fabs(data[i]);
        if (absVal > peak) peak = absVal;
    }

    if (peak <= 0.0f) return -96.0;
    return 20.0 * std::log10(static_cast<double>(peak));
}

// ════════════════════════════════════════════════════════════════════
//  ENERGY — Normalized RMS intensity [0..100]
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectEnergy(const float* data, int64_t numSamples)
{
    if (numSamples == 0) return 0.0;

    double sum = 0.0;
    for (int64_t i = 0; i < numSamples; ++i) {
        sum += static_cast<double>(data[i]) * data[i];
    }
    double rms = std::sqrt(sum / static_cast<double>(numSamples));

    // Map RMS to 0..100 scale
    // Typical full-scale music has RMS ~0.1 to ~0.3
    // Silence = 0, heavily compressed pop ≈ 0.3+
    double normalized = rms / 0.35;  // 0.35 as reference "maximum" RMS
    normalized = std::min(1.0, std::max(0.0, normalized));

    return std::round(normalized * 1000.0) / 10.0; // [0..100] with 1 decimal
}

// ════════════════════════════════════════════════════════════════════
//  CUE IN — First strong transient (seconds)
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectCueIn(const float* data, int64_t numSamples,
                                          double sampleRate)
{
    // Find the first point where the signal exceeds a threshold relative
    // to the track's peak level.  Use a short sliding RMS window.

    const int windowSize = static_cast<int>(sampleRate * 0.05); // 50ms window
    if (numSamples < windowSize * 2) return 0.0;

    // Find overall peak RMS in 50ms windows
    double peakRMS = 0.0;
    for (int64_t i = 0; i <= numSamples - windowSize; i += windowSize) {
        double sum = 0.0;
        for (int j = 0; j < windowSize; ++j) {
            double s = data[static_cast<size_t>(i + j)];
            sum += s * s;
        }
        double rms = std::sqrt(sum / windowSize);
        peakRMS = std::max(peakRMS, rms);
    }

    if (peakRMS <= 0.0) return 0.0;

    // Threshold: 5% of peak RMS = first audible content
    double threshold = peakRMS * 0.05;

    for (int64_t i = 0; i <= numSamples - windowSize; i += windowSize / 2) {
        double sum = 0.0;
        for (int j = 0; j < windowSize; ++j) {
            double s = data[static_cast<size_t>(i + j)];
            sum += s * s;
        }
        double rms = std::sqrt(sum / windowSize);
        if (rms >= threshold) {
            double seconds = static_cast<double>(i) / sampleRate;
            return std::round(seconds * 100.0) / 100.0;
        }
    }

    return 0.0;
}

// ════════════════════════════════════════════════════════════════════
//  CUE OUT — Last usable section (seconds)
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectCueOut(const float* data, int64_t numSamples,
                                           double sampleRate)
{
    const int windowSize = static_cast<int>(sampleRate * 0.05); // 50ms
    if (numSamples < windowSize * 2) return 0.0;

    // Find peak RMS
    double peakRMS = 0.0;
    for (int64_t i = 0; i <= numSamples - windowSize; i += windowSize) {
        double sum = 0.0;
        for (int j = 0; j < windowSize; ++j) {
            double s = data[static_cast<size_t>(i + j)];
            sum += s * s;
        }
        peakRMS = std::max(peakRMS, std::sqrt(sum / windowSize));
    }

    if (peakRMS <= 0.0)
        return static_cast<double>(numSamples) / sampleRate;

    double threshold = peakRMS * 0.05;

    // Scan backwards
    for (int64_t i = numSamples - windowSize; i >= 0; i -= windowSize / 2) {
        double sum = 0.0;
        for (int j = 0; j < windowSize; ++j) {
            double s = data[static_cast<size_t>(i + j)];
            sum += s * s;
        }
        double rms = std::sqrt(sum / windowSize);
        if (rms >= threshold) {
            double seconds = static_cast<double>(i + windowSize) / sampleRate;
            return std::round(seconds * 100.0) / 100.0;
        }
    }

    return static_cast<double>(numSamples) / sampleRate;
}

// ════════════════════════════════════════════════════════════════════
//  DYNAMIC RANGE — Loudness Range approximation (LU)
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectDynamicRange(const float* data,
                                                  int64_t numSamples,
                                                  double sampleRate)
{
    // Compute short-term loudness (3-second windows), then take the
    // difference between 95th and 10th percentile.

    const int windowSize = static_cast<int>(sampleRate * 3.0);
    const int hopSize    = static_cast<int>(sampleRate * 1.0);
    if (numSamples < windowSize) return 0.0;

    std::vector<double> shortTermDB;

    for (int64_t i = 0; i <= numSamples - windowSize; i += hopSize) {
        double sum = 0.0;
        for (int j = 0; j < windowSize; ++j) {
            double s = data[static_cast<size_t>(i + j)];
            sum += s * s;
        }
        double ms = sum / windowSize;
        if (ms > 0.0) {
            shortTermDB.push_back(10.0 * std::log10(ms));
        }
    }

    if (shortTermDB.size() < 4) return 0.0;

    std::sort(shortTermDB.begin(), shortTermDB.end());

    // Gate: remove silence (below -70 dB)
    auto gatedBegin = std::lower_bound(shortTermDB.begin(), shortTermDB.end(), -70.0);
    if (gatedBegin == shortTermDB.end()) return 0.0;

    std::vector<double> gated(gatedBegin, shortTermDB.end());
    if (gated.size() < 4) return 0.0;

    size_t idx10 = static_cast<size_t>(gated.size() * 0.10);
    size_t idx95 = static_cast<size_t>(gated.size() * 0.95);
    idx95 = std::min(idx95, gated.size() - 1);

    double lra = gated[idx95] - gated[idx10];
    return std::round(lra * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  SPECTRAL CENTROID — Brightness proxy (Hz)
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::detectSpectralCentroid(const float* data,
                                                      int64_t numSamples,
                                                      double sampleRate)
{
    // Compute via DFT of short frames, then average centroid.
    // Use a simple real DFT (no FFT library needed for moderate N).

    const int fftSize = 2048;
    const int hopSize = 1024;
    const int64_t numFrames = (numSamples - fftSize) / hopSize;
    if (numFrames < 1) return 0.0;

    // Limit to 200 frames for speed (~3 minutes at default hop)
    const int64_t maxFramesAnalyze = std::min(numFrames, int64_t(200));
    const int64_t frameStep = std::max(int64_t(1), numFrames / maxFramesAnalyze);

    double totalCentroid = 0.0;
    int    validFrames   = 0;

    // Hann window
    std::vector<float> window(fftSize);
    for (int i = 0; i < fftSize; ++i) {
        window[i] = static_cast<float>(
            0.5 * (1.0 - std::cos(2.0 * M_PI * i / (fftSize - 1))));
    }

    // Magnitude spectrum via brute-force DFT of half the bins
    // (only need up to Nyquist).  For 2048-point, that's 1024 bins.
    const int halfN = fftSize / 2;

    for (int64_t f = 0; f < numFrames; f += frameStep) {
        const float* frame = data + f * hopSize;

        // Compute magnitude spectrum using Goertzel-like approach per bin
        // (much faster than full DFT for sparse sampling)
        double weightedSum = 0.0;
        double magnitudeSum = 0.0;

        // Sample only every 8th bin for speed (256 bins instead of 1024)
        for (int k = 1; k < halfN; k += 8) {
            double realPart = 0.0, imagPart = 0.0;
            double freq = 2.0 * M_PI * k / fftSize;
            for (int n = 0; n < fftSize; ++n) {
                double windowed = frame[n] * window[n];
                realPart += windowed * std::cos(freq * n);
                imagPart -= windowed * std::sin(freq * n);
            }
            double mag = std::sqrt(realPart * realPart + imagPart * imagPart);
            double binFreq = static_cast<double>(k) * sampleRate / fftSize;
            weightedSum += binFreq * mag;
            magnitudeSum += mag;
        }

        if (magnitudeSum > 0.0) {
            totalCentroid += weightedSum / magnitudeSum;
            ++validFrames;
        }
    }

    if (validFrames == 0) return 0.0;
    return std::round(totalCentroid / validFrames);
}

// ════════════════════════════════════════════════════════════════════
//  DANCEABILITY — Tempo stability + rhythm regularity
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::computeDanceability(double bpm, double energy,
                                                   double beatGridConfidence)
{
    if (bpm <= 0.0) return 0.0;

    // Danceability heuristic:
    // - Optimal BPM range 100-140 gets highest score
    // - Energy adds to score
    // - Beat grid confidence scales the result

    double bpmScore;
    if (bpm >= 100.0 && bpm <= 140.0) {
        bpmScore = 1.0; // sweet spot
    } else if (bpm >= 80.0 && bpm <= 160.0) {
        // Linear falloff outside sweet spot
        if (bpm < 100.0)
            bpmScore = 0.5 + 0.5 * (bpm - 80.0) / 20.0;
        else
            bpmScore = 0.5 + 0.5 * (160.0 - bpm) / 20.0;
    } else {
        bpmScore = 0.3;
    }

    double energyFactor = std::min(1.0, energy / 80.0); // high energy → more danceable
    double confFactor   = 0.5 + 0.5 * beatGridConfidence;

    double raw = (bpmScore * 0.5 + energyFactor * 0.3 + confFactor * 0.2) * 100.0;
    return std::round(std::min(100.0, std::max(0.0, raw)) * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  ACOUSTICNESS — Spectral + dynamic heuristic
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::computeAcousticness(double spectralCentroid,
                                                    double dynamicRange,
                                                    double energy)
{
    // Acoustic tracks tend to have:
    // - Lower spectral centroid (less brightness)
    // - Higher dynamic range
    // - Lower overall energy

    // Centroid factor: lower = more acoustic
    // Typical electronic: 3000-5000 Hz, acoustic: 1000-2500 Hz
    double centroidScore;
    if (spectralCentroid <= 0.0) {
        centroidScore = 0.5;
    } else if (spectralCentroid < 1500.0) {
        centroidScore = 1.0;
    } else if (spectralCentroid < 4000.0) {
        centroidScore = 1.0 - (spectralCentroid - 1500.0) / 2500.0;
    } else {
        centroidScore = 0.0;
    }

    // Dynamic range: higher = more acoustic (less compressed)
    double drScore = std::min(1.0, dynamicRange / 15.0);

    // Energy: lower = more acoustic
    double energyScore = 1.0 - std::min(1.0, energy / 80.0);

    double raw = (centroidScore * 0.4 + drScore * 0.35 + energyScore * 0.25) * 100.0;
    return std::round(std::min(100.0, std::max(0.0, raw)) * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  INSTRUMENTALNESS — Vocal presence heuristic
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::computeInstrumentalness(const float* data,
                                                       int64_t numSamples,
                                                       double sampleRate)
{
    // Heuristic: vocals produce energy concentrated in 300-3400 Hz range
    // with specific temporal modulation patterns (~4 Hz syllabic rate).
    //
    // We measure the ratio of mid-frequency energy (vocal band) to
    // total energy, and look at amplitude modulation in that band.

    const int frameSize = 2048;
    const int hopSize   = 1024;
    const int64_t numFrames = (numSamples - frameSize) / hopSize;
    if (numFrames < 4) return 50.0; // unknown → 50%

    // Simple proxy: measure zero-crossing rate variability
    // Vocals have moderate, variable ZCR; instruments tend to be more stable

    std::vector<double> zcrValues;
    zcrValues.reserve(static_cast<size_t>(std::min(numFrames, int64_t(500))));

    const int64_t step = std::max(int64_t(1), numFrames / 500);
    for (int64_t f = 0; f < numFrames; f += step) {
        const float* frame = data + f * hopSize;
        int crossings = 0;
        for (int i = 1; i < frameSize; ++i) {
            if ((frame[i] >= 0.0f) != (frame[i - 1] >= 0.0f))
                ++crossings;
        }
        double zcr = static_cast<double>(crossings)
                   / static_cast<double>(frameSize);
        zcrValues.push_back(zcr);
    }

    if (zcrValues.size() < 4) return 50.0;

    // Compute coefficient of variation of ZCR
    double mean = std::accumulate(zcrValues.begin(), zcrValues.end(), 0.0)
                / static_cast<double>(zcrValues.size());
    double variance = 0.0;
    for (double v : zcrValues) {
        variance += (v - mean) * (v - mean);
    }
    variance /= static_cast<double>(zcrValues.size());
    double stddev = std::sqrt(variance);
    double cv = (mean > 0.0) ? stddev / mean : 0.0;

    // Higher CV of ZCR → more vocal presence → less instrumental
    // Typical instrumental: CV < 0.3, vocal: CV > 0.4
    double instrumentalScore;
    if (cv < 0.25) {
        instrumentalScore = 90.0; // very stable → likely instrumental
    } else if (cv < 0.45) {
        instrumentalScore = 90.0 - (cv - 0.25) / 0.20 * 60.0;
    } else {
        instrumentalScore = 30.0 - std::min(30.0, (cv - 0.45) * 100.0);
    }

    return std::round(std::min(100.0, std::max(0.0, instrumentalScore)) * 10.0)
         / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  LIVENESS — Dynamic variability heuristic
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::computeLiveness(const float* data,
                                               int64_t numSamples,
                                               double sampleRate)
{
    // Live recordings tend to have:
    // - More amplitude variability
    // - Background noise floor
    // - Less consistent energy distribution

    const int64_t windowSize = static_cast<int64_t>(sampleRate * 0.5); // 500ms
    const int64_t numWindows = numSamples / windowSize;
    if (numWindows < 4) return 30.0;

    std::vector<double> windowRMS;
    windowRMS.reserve(static_cast<size_t>(numWindows));

    for (int64_t w = 0; w < numWindows; ++w) {
        double sum = 0.0;
        for (int64_t i = 0; i < windowSize; ++i) {
            double s = data[static_cast<size_t>(w * windowSize + i)];
            sum += s * s;
        }
        windowRMS.push_back(std::sqrt(sum / static_cast<double>(windowSize)));
    }

    // Measure coefficient of variation
    double mean = std::accumulate(windowRMS.begin(), windowRMS.end(), 0.0)
                / static_cast<double>(windowRMS.size());
    if (mean <= 0.0) return 0.0;

    double variance = 0.0;
    for (double r : windowRMS) {
        variance += (r - mean) * (r - mean);
    }
    variance /= static_cast<double>(windowRMS.size());
    double cv = std::sqrt(variance) / mean;

    // Also check noise floor: minimum RMS / mean RMS
    double minRMS = *std::min_element(windowRMS.begin(), windowRMS.end());
    double noiseRatio = (mean > 0.0) ? minRMS / mean : 0.0;

    // Live: high CV (>0.5) + noticeable noise floor (ratio > 0.05)
    double cvScore = std::min(1.0, cv / 0.8);
    double noiseScore = std::min(1.0, noiseRatio / 0.15);

    double raw = (cvScore * 0.6 + noiseScore * 0.4) * 100.0;
    return std::round(std::min(100.0, std::max(0.0, raw)) * 10.0) / 10.0;
}

// ════════════════════════════════════════════════════════════════════
//  TRANSITION DIFFICULTY — Mixability heuristic
// ════════════════════════════════════════════════════════════════════

double AudioAnalysisService::computeTransitionDifficulty(double bpm,
                                                           double energy,
                                                           double dynamicRange,
                                                           double introDuration)
{
    // Higher difficulty when:
    // - BPM is unusual (far from 120-130)
    // - Energy is extreme (very high or very low)
    // - Dynamic range is large (hard to beatmatch levels)
    // - Intro is very short (no mix-in window)

    double bpmDiff = std::fabs(bpm - 125.0);
    double bpmPenalty = std::min(1.0, bpmDiff / 50.0);

    double energyPenalty;
    if (energy >= 40.0 && energy <= 80.0) {
        energyPenalty = 0.0;
    } else {
        energyPenalty = std::min(1.0, std::fabs(energy - 60.0) / 40.0);
    }

    double drPenalty = std::min(1.0, dynamicRange / 20.0);

    double introPenalty;
    if (introDuration >= 8.0) {
        introPenalty = 0.0;
    } else {
        introPenalty = 1.0 - introDuration / 8.0;
    }

    double raw = (bpmPenalty * 0.3 + energyPenalty * 0.2
                + drPenalty * 0.25 + introPenalty * 0.25) * 100.0;
    return std::round(std::min(100.0, std::max(0.0, raw)) * 10.0) / 10.0;
}

// ── Lightweight JUCE-based duration probe ──────────────────────────
double AudioAnalysisService::probeDurationSeconds(const QString& filePath)
{
    juce::AudioFormatManager fmt;
    fmt.registerBasicFormats();
    juce::File jf(filePath.toStdString().c_str());
    std::unique_ptr<juce::AudioFormatReader> reader(fmt.createReaderFor(jf));
    if (!reader) return 0.0;
    const auto frames = static_cast<int64_t>(reader->lengthInSamples);
    const double sr = reader->sampleRate;
    if (frames <= 0 || sr <= 0.0) return 0.0;
    return static_cast<double>(frames) / sr;
}
