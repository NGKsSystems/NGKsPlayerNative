#include "BpmResolverService.h"

#include <QDebug>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// ════════════════════════════════════════════════════════════════════
//  PUBLIC — resolve()
// ════════════════════════════════════════════════════════════════════

BpmResolutionResult BpmResolverService::resolve(double rawBpm,
                                                 const float* monoData,
                                                 int64_t numSamples,
                                                 double sampleRate,
                                                 const QString& genre)
{
    BpmResolutionResult res;
    res.rawBpm = rawBpm;

    if (rawBpm <= 0.0 || numSamples <= 0 || sampleRate <= 0.0) {
        res.resolvedBpm = rawBpm;
        res.confidence  = 0.0;
        res.selectedFamily = QStringLiteral("BASE");
        return res;
    }

    qDebug() << "[BPM_RESOLVER] RAW_BPM=" << rawBpm << "genre=" << genre;

    // ── Compute signal features ──
    double onsetDens   = computeOnsetDensity(monoData, numSamples, sampleRate);
    double ioiPeak     = computeIOIPeakPeriod(monoData, numSamples, sampleRate);
    double hfPerc      = computeHFPercussiveScore(monoData, numSamples, sampleRate);

    res.onsetDensity      = onsetDens;
    res.hfPercussiveScore = hfPerc;

    qDebug() << "[BPM_RESOLVER] ONSET_DENSITY=" << onsetDens
             << "IOI_PEAK_PERIOD=" << ioiPeak
             << "HF_PERCUSSIVE=" << hfPerc;

    // ── Generate and score candidates ──
    auto candidates = generateCandidates(rawBpm);

    for (auto& c : candidates) {
        c.score = scoreCandidate(c.bpm, onsetDens, ioiPeak, hfPerc, genre);
        qDebug() << "[BPM_RESOLVER] CANDIDATE"
                 << c.family << "bpm=" << c.bpm
                 << "score=" << c.score;
    }

    // Sort by score descending
    std::sort(candidates.begin(), candidates.end(),
              [](const BpmCandidate& a, const BpmCandidate& b) {
                  return a.score > b.score;
              });

    // Winner
    const auto& winner = candidates.front();
    res.resolvedBpm    = winner.bpm;
    res.selectedFamily = winner.family;
    res.candidates     = candidates;

    // ── Confidence ──
    // Based on margin between best and second-best score
    if (candidates.size() >= 2) {
        double margin = candidates[0].score - candidates[1].score;
        // Map margin [0..0.4] → confidence [0.5..1.0]
        res.confidence = std::min(1.0, 0.5 + margin * 1.25);
    } else {
        res.confidence = 0.7;
    }

    // Boost confidence if winner is in sweet spot (90-160)
    if (res.resolvedBpm >= 90.0 && res.resolvedBpm <= 160.0) {
        res.confidence = std::min(1.0, res.confidence + 0.05);
    }

    qDebug() << "[BPM_RESOLVER] RESOLVED=" << res.resolvedBpm
             << "family=" << res.selectedFamily
             << "confidence=" << res.confidence;

    return res;
}

// ════════════════════════════════════════════════════════════════════
//  Onset density — number of detected onsets per second
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::computeOnsetDensity(const float* data,
                                                int64_t numSamples,
                                                double sampleRate)
{
    const int hopSize   = 512;
    const int frameSize = 1024;
    const int64_t numHops = (numSamples - frameSize) / hopSize;
    if (numHops < 4) return 0.0;

    // RMS per frame
    std::vector<float> rms(static_cast<size_t>(numHops));
    for (int64_t h = 0; h < numHops; ++h) {
        double sum = 0.0;
        const float* frame = data + h * hopSize;
        for (int i = 0; i < frameSize; ++i)
            sum += static_cast<double>(frame[i]) * frame[i];
        rms[static_cast<size_t>(h)] = static_cast<float>(
            std::sqrt(sum / frameSize));
    }

    // Onset envelope (half-wave rectified first-difference)
    std::vector<float> onset(static_cast<size_t>(numHops), 0.0f);
    for (int64_t h = 1; h < numHops; ++h) {
        float diff = rms[static_cast<size_t>(h)] - rms[static_cast<size_t>(h - 1)];
        onset[static_cast<size_t>(h)] = std::max(0.0f, diff);
    }

    // Adaptive threshold: mean + 1.5 * stddev
    double oSum = 0.0;
    for (auto v : onset) oSum += v;
    double oMean = oSum / static_cast<double>(numHops);

    double oVar = 0.0;
    for (auto v : onset) {
        double d = v - oMean;
        oVar += d * d;
    }
    double oStd = std::sqrt(oVar / static_cast<double>(numHops));
    double threshold = oMean + 1.5 * oStd;

    // Count onsets (peaks above threshold with minimum spacing)
    int onsetCount = 0;
    const int minSpacing = static_cast<int>(sampleRate / hopSize * 0.05); // 50ms min gap
    int lastOnset = -minSpacing;

    for (int64_t h = 1; h < numHops - 1; ++h) {
        if (onset[static_cast<size_t>(h)] > threshold &&
            onset[static_cast<size_t>(h)] >= onset[static_cast<size_t>(h - 1)] &&
            onset[static_cast<size_t>(h)] >= onset[static_cast<size_t>(h + 1)] &&
            (static_cast<int>(h) - lastOnset) >= minSpacing) {
            ++onsetCount;
            lastOnset = static_cast<int>(h);
        }
    }

    double durationSec = static_cast<double>(numSamples) / sampleRate;
    return (durationSec > 0.0) ? onsetCount / durationSec : 0.0;
}

// ════════════════════════════════════════════════════════════════════
//  IOI — Inter-onset interval histogram peak period
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::computeIOIPeakPeriod(const float* data,
                                                  int64_t numSamples,
                                                  double sampleRate)
{
    const int hopSize   = 512;
    const int frameSize = 1024;
    const int64_t numHops = (numSamples - frameSize) / hopSize;
    if (numHops < 4) return 0.0;

    // Build onset positions
    std::vector<float> rms(static_cast<size_t>(numHops));
    for (int64_t h = 0; h < numHops; ++h) {
        double sum = 0.0;
        const float* frame = data + h * hopSize;
        for (int i = 0; i < frameSize; ++i)
            sum += static_cast<double>(frame[i]) * frame[i];
        rms[static_cast<size_t>(h)] = static_cast<float>(
            std::sqrt(sum / frameSize));
    }

    std::vector<float> onset(static_cast<size_t>(numHops), 0.0f);
    for (int64_t h = 1; h < numHops; ++h) {
        float diff = rms[static_cast<size_t>(h)] - rms[static_cast<size_t>(h - 1)];
        onset[static_cast<size_t>(h)] = std::max(0.0f, diff);
    }

    // Threshold
    double oSum = 0.0;
    for (auto v : onset) oSum += v;
    double oMean = oSum / static_cast<double>(numHops);
    double oVar = 0.0;
    for (auto v : onset) { double d = v - oMean; oVar += d * d; }
    double oStd = std::sqrt(oVar / static_cast<double>(numHops));
    double threshold = oMean + 1.0 * oStd;

    // Collect onset frame positions
    std::vector<int64_t> onsetPositions;
    const int minSpacing = static_cast<int>(sampleRate / hopSize * 0.05);
    int64_t lastPos = -minSpacing;

    for (int64_t h = 1; h < numHops - 1; ++h) {
        if (onset[static_cast<size_t>(h)] > threshold &&
            onset[static_cast<size_t>(h)] >= onset[static_cast<size_t>(h - 1)] &&
            onset[static_cast<size_t>(h)] >= onset[static_cast<size_t>(h + 1)] &&
            (h - lastPos) >= minSpacing) {
            onsetPositions.push_back(h);
            lastPos = h;
        }
    }

    if (onsetPositions.size() < 3) return 0.0;

    // Build IOI histogram (in seconds)
    // Bin IOIs into 5ms bins from 0.1s to 1.5s (20-600 BPM range)
    const double binWidth = 0.005; // 5ms
    const double minIOI   = 0.1;   // 600 BPM
    const double maxIOI   = 1.5;   // 40 BPM
    const int numBins = static_cast<int>((maxIOI - minIOI) / binWidth) + 1;
    std::vector<int> histogram(static_cast<size_t>(numBins), 0);

    const double hopSec = static_cast<double>(hopSize) / sampleRate;
    for (size_t i = 1; i < onsetPositions.size(); ++i) {
        double ioi = static_cast<double>(onsetPositions[i] - onsetPositions[i - 1]) * hopSec;
        if (ioi >= minIOI && ioi <= maxIOI) {
            int bin = static_cast<int>((ioi - minIOI) / binWidth);
            if (bin >= 0 && bin < numBins)
                histogram[static_cast<size_t>(bin)]++;
        }
    }

    // Find histogram peak
    int bestBin = 0;
    int bestCount = 0;
    for (int b = 0; b < numBins; ++b) {
        if (histogram[static_cast<size_t>(b)] > bestCount) {
            bestCount = histogram[static_cast<size_t>(b)];
            bestBin = b;
        }
    }

    double peakIOI = minIOI + (bestBin + 0.5) * binWidth;
    return peakIOI; // seconds per beat
}

// ════════════════════════════════════════════════════════════════════
//  HF Percussive Score — high-frequency energy ratio
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::computeHFPercussiveScore(const float* data,
                                                     int64_t numSamples,
                                                     double sampleRate)
{
    // Simple high-pass energy ratio:
    // 1-pole high-pass at ~4000Hz, measure ratio of HP energy to total energy

    if (numSamples <= 0 || sampleRate <= 0.0) return 0.0;

    // HPF coefficient: y[n] = alpha * (y[n-1] + x[n] - x[n-1])
    // alpha = RC / (RC + dt), where RC = 1/(2*pi*fc)
    const double fc = 4000.0;
    const double dt = 1.0 / sampleRate;
    const double RC = 1.0 / (2.0 * M_PI * fc);
    const double alpha = RC / (RC + dt);

    double totalEnergy = 0.0;
    double hfEnergy    = 0.0;
    double prevX = 0.0;
    double prevY = 0.0;

    // Process in blocks to reduce cache pressure
    for (int64_t i = 0; i < numSamples; ++i) {
        double x = static_cast<double>(data[static_cast<size_t>(i)]);
        double y = alpha * (prevY + x - prevX);
        prevX = x;
        prevY = y;

        totalEnergy += x * x;
        hfEnergy    += y * y;
    }

    if (totalEnergy <= 0.0) return 0.0;
    return std::min(1.0, hfEnergy / totalEnergy);
}

// ════════════════════════════════════════════════════════════════════
//  Candidate generation — half / base / double
// ════════════════════════════════════════════════════════════════════

std::vector<BpmCandidate> BpmResolverService::generateCandidates(double rawBpm)
{
    std::vector<BpmCandidate> cands;

    // Hard bounds: 40-220 BPM
    auto addIf = [&](double bpm, const QString& family, const QString& reason) {
        if (bpm >= 40.0 && bpm <= 220.0) {
            cands.push_back({bpm, family, 0.0, reason});
        }
    };

    addIf(rawBpm / 2.0, QStringLiteral("HALF"),
          QStringLiteral("raw/2"));
    addIf(rawBpm,        QStringLiteral("BASE"),
          QStringLiteral("raw"));
    addIf(rawBpm * 2.0, QStringLiteral("DOUBLE"),
          QStringLiteral("raw*2"));

    return cands;
}

// ════════════════════════════════════════════════════════════════════
//  Candidate scoring
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::scoreCandidate(double candidateBpm,
                                           double onsetDens,
                                           double ioiPeakPeriod,
                                           double hfPercussive,
                                           const QString& genre)
{
    double score = 0.0;

    // ── 1. Range plausibility (0.25 weight) ──
    score += 0.25 * rangePlausibility(candidateBpm);

    // ── 2. Onset density alignment (0.25 weight) ──
    // Expected onsets/sec for BPM: at minimum ~BPM/60 (one per beat),
    // typically 2-4× that for kick+snare+hats
    {
        double expectedBeatsPerSec = candidateBpm / 60.0;
        // Onset density should be between 1× and 6× beats per second
        double ratio = (onsetDens > 0.0 && expectedBeatsPerSec > 0.0)
                       ? onsetDens / expectedBeatsPerSec : 0.0;
        // Ideal ratio is 2-4 (kick+snare at minimum)
        double alignment;
        if (ratio >= 1.5 && ratio <= 5.0)
            alignment = 1.0;
        else if (ratio >= 0.8 && ratio < 1.5)
            alignment = 0.6;
        else if (ratio > 5.0 && ratio <= 8.0)
            alignment = 0.5;
        else
            alignment = 0.2;
        score += 0.25 * alignment;
    }

    // ── 3. IOI consistency (0.20 weight) ──
    // How well does IOI peak period match this candidate's beat period?
    {
        double candidatePeriod = 60.0 / candidateBpm; // seconds per beat
        double ioiScore = 0.0;
        if (ioiPeakPeriod > 0.0 && candidatePeriod > 0.0) {
            // Check match at period, period/2, period*2
            auto matchQuality = [](double a, double b) -> double {
                if (a <= 0.0 || b <= 0.0) return 0.0;
                double ratio = a / b;
                double deviation = std::abs(ratio - 1.0);
                return std::max(0.0, 1.0 - deviation * 4.0);
            };
            double m1 = matchQuality(ioiPeakPeriod, candidatePeriod);
            double m2 = matchQuality(ioiPeakPeriod, candidatePeriod * 2.0);
            double m3 = matchQuality(ioiPeakPeriod, candidatePeriod / 2.0);
            ioiScore = std::max({m1, m2 * 0.7, m3 * 0.7});
        }
        score += 0.20 * ioiScore;
    }

    // ── 4. HF percussive energy (0.15 weight) ──
    // High HF percussive energy suggests faster tempo (more hi-hats = double-time)
    {
        double hfBias = 0.5; // neutral
        if (hfPercussive > 0.15) {
            // High HF: favors higher BPM (double-time has more hi-hat activity)
            if (candidateBpm >= 120.0)
                hfBias = 0.7 + hfPercussive;
            else
                hfBias = 0.4;
        } else {
            // Low HF: neutral to slight preference for lower BPM
            if (candidateBpm <= 120.0)
                hfBias = 0.6;
        }
        score += 0.15 * std::min(1.0, hfBias);
    }

    // ── 5. Genre bias (0.15 weight) ──
    score += 0.15 * genreBias(candidateBpm, genre);

    return score;
}

// ════════════════════════════════════════════════════════════════════
//  Range plausibility — DJ-friendly tempo ranges
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::rangePlausibility(double bpm)
{
    // Sweet spot: 90-160 BPM (most DJ music lives here)
    if (bpm >= 90.0 && bpm <= 160.0) return 1.0;

    // Acceptable: 70-90 or 160-180
    if (bpm >= 70.0 && bpm < 90.0) return 0.7;
    if (bpm > 160.0 && bpm <= 180.0) return 0.8;

    // Marginal: 60-70 or 180-200
    if (bpm >= 60.0 && bpm < 70.0) return 0.4;
    if (bpm > 180.0 && bpm <= 200.0) return 0.5;

    // Outside 60-200: low plausibility
    if (bpm >= 40.0 && bpm < 60.0) return 0.2;
    if (bpm > 200.0 && bpm <= 220.0) return 0.3;

    return 0.0;
}

// ════════════════════════════════════════════════════════════════════
//  Genre bias — soft preferences per genre
// ════════════════════════════════════════════════════════════════════

double BpmResolverService::genreBias(double bpm, const QString& genre)
{
    if (genre.isEmpty()) return 0.5; // neutral

    QString g = genre.toLower().trimmed();

    // Hip-Hop / R&B: 70-105
    if (g.contains(QLatin1String("hip")) || g.contains(QLatin1String("r&b")) ||
        g.contains(QLatin1String("rap"))) {
        if (bpm >= 70.0 && bpm <= 105.0) return 1.0;
        if (bpm >= 55.0 && bpm < 70.0)   return 0.5;
        return 0.3;
    }

    // EDM / House / Techno: 115-150
    if (g.contains(QLatin1String("house")) || g.contains(QLatin1String("techno")) ||
        g.contains(QLatin1String("edm")) || g.contains(QLatin1String("trance")) ||
        g.contains(QLatin1String("electro"))) {
        if (bpm >= 115.0 && bpm <= 150.0) return 1.0;
        if (bpm >= 100.0 && bpm < 115.0)  return 0.5;
        return 0.3;
    }

    // DnB / Jungle: 160-180
    if (g.contains(QLatin1String("drum")) || g.contains(QLatin1String("jungle")) ||
        g.contains(QLatin1String("dnb")) || g.contains(QLatin1String("d&b"))) {
        if (bpm >= 160.0 && bpm <= 180.0) return 1.0;
        if (bpm >= 140.0 && bpm < 160.0)  return 0.5;
        return 0.2;
    }

    // Rock / Metal / Punk: 90-180, strong double-time bias
    if (g.contains(QLatin1String("rock")) || g.contains(QLatin1String("metal")) ||
        g.contains(QLatin1String("punk")) || g.contains(QLatin1String("hard"))) {
        // Rock/Metal: DJs and listeners perceive double-time kicks
        if (bpm >= 130.0 && bpm <= 180.0) return 1.0;
        if (bpm >= 100.0 && bpm < 130.0)  return 0.7;
        if (bpm >= 90.0 && bpm < 100.0)   return 0.5;
        return 0.3;
    }

    // Pop / Dance: 90-130
    if (g.contains(QLatin1String("pop")) || g.contains(QLatin1String("dance"))) {
        if (bpm >= 90.0 && bpm <= 130.0) return 1.0;
        if (bpm >= 130.0 && bpm <= 150.0) return 0.6;
        return 0.4;
    }

    // Reggae / Dub: 60-90
    if (g.contains(QLatin1String("reggae")) || g.contains(QLatin1String("dub")) ||
        g.contains(QLatin1String("ska"))) {
        if (bpm >= 60.0 && bpm <= 90.0) return 1.0;
        return 0.3;
    }

    return 0.5; // unknown genre, neutral
}
