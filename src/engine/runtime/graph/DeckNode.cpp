#include "engine/runtime/graph/DeckNode.h"

#include "engine/DiagLog.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>

namespace ngks {

DeckNode::DeckNode()
{
    formatManager_.registerBasicFormats(); // WAV, AIFF, FLAC, OGG, MP3 (via juce_audio_formats)
}

DeckNode::~DeckNode()
{
    cancelStreamDecode();
}

void DeckNode::cancelStreamDecode()
{
    streamCancelled_.store(true, std::memory_order_release);
    if (streamDecodeThread_.joinable()) {
        streamDecodeThread_.join();
    }
    streamCancelled_.store(false, std::memory_order_relaxed);
}

void DeckNode::prepare(double sampleRate)
{
    std::unique_lock<std::shared_mutex> lock(bufferMutex_);
    deviceSampleRate_ = (sampleRate > 0.0) ? sampleRate : 48000.0;
    stopFadeSamplesRemaining = 0;
    stopFadeSamplesTotal = std::max(1, static_cast<int>(deviceSampleRate_ * 0.2));

    if (fileSampleRate_ > 0.0) {
        resampleRatio_ = fileSampleRate_ / deviceSampleRate_;
    }
}

void DeckNode::beginStopFade(int fadeSamples) noexcept
{
    std::unique_lock<std::shared_mutex> lock(bufferMutex_);
    stopFadeSamplesTotal = std::max(1, fadeSamples);
    stopFadeSamplesRemaining = stopFadeSamplesTotal;
}

bool DeckNode::isStopFadeActive() const noexcept
{
    std::shared_lock<std::shared_mutex> lock(bufferMutex_);
    return stopFadeSamplesRemaining > 0;
}

bool DeckNode::loadFile(const std::string& path, double& outDurationSeconds)
{
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto elapsedMs = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    };
    auto elapsedUs = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
    };

    outDurationSeconds = 0.0;
    loadedFilePath_ = path;
    ngks::audioTrace("TRACK_LOAD_BEGIN", "path=%s elapsedUs=0", path.c_str());

    // Cancel any in-progress background decode from a previous load
    {
        const auto tCancel = Clock::now();
        cancelStreamDecode();
        const auto cancelUs = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - tCancel).count();
        ngks::audioTrace("TRACK_LOAD_CANCEL_PREV", "cancelUs=%lld elapsedUs=%lld",
                         cancelUs, elapsedUs());
    }

    juce::String jucePath(path.c_str());
    juce::File file(jucePath);
    if (!file.existsAsFile()) {
        std::cerr << "DeckNode::loadFile: file not found: " << path << std::endl;
        return false;
    }
    ngks::audioTrace("TRACK_LOAD_FILE_CHECK", "elapsedUs=%lld", elapsedUs());

    std::unique_ptr<juce::AudioFormatReader> reader(
        formatManager_.createReaderFor(file));

    if (!reader) {
        std::cerr << "DeckNode::loadFile: no codec for: " << path << std::endl;
        return false;
    }
    ngks::audioTrace("TRACK_LOAD_READER_CREATED", "elapsedUs=%lld", elapsedUs());

    const int64_t numFrames = static_cast<int64_t>(reader->lengthInSamples);
    const int numChannels = static_cast<int>(reader->numChannels);
    const double sr = reader->sampleRate;

    if (numFrames <= 0 || sr <= 0.0) {
        std::cerr << "DeckNode::loadFile: empty or invalid: " << path << std::endl;
        return false;
    }
    ngks::audioTrace("TRACK_LOAD_HEADER", "frames=%lld sr=%.0f ch=%d elapsedUs=%lld",
                     (long long)numFrames, sr, numChannels, elapsedUs());

    // Only allocate the preload portion (~2MB) on the hot path — NOT the
    // full file (~100MB).  Background thread handles full allocation.
    constexpr int64_t kPreloadSeconds = 5;
    const int64_t preloadFrames = std::min(numFrames, static_cast<int64_t>(sr * kPreloadSeconds));

    std::vector<float> newL(static_cast<size_t>(preloadFrames), 0.0f);
    std::vector<float> newR(static_cast<size_t>(preloadFrames), 0.0f);
    ngks::audioTrace("TRACK_LOAD_ALLOC_DONE", "preloadFrames=%lld bytes=%lld elapsedUs=%lld",
                     (long long)preloadFrames,
                     (long long)(preloadFrames * 2 * sizeof(float)),
                     elapsedUs());

    // Decode first 5 seconds — enough for instant playback
    constexpr int64_t chunkSize = 65536;
    for (int64_t pos = 0; pos < preloadFrames; pos += chunkSize) {
        const int remaining = static_cast<int>(std::min(chunkSize, preloadFrames - pos));
        float* chunkPtrs[2] = { newL.data() + pos, newR.data() + pos };
        reader->read(chunkPtrs, numChannels >= 2 ? 2 : 1, pos, remaining);
    }

    // If mono, copy L to R for preloaded region
    if (numChannels == 1) {
        std::memcpy(newR.data(), newL.data(), static_cast<size_t>(preloadFrames) * sizeof(float));
    }
    ngks::audioTrace("TRACK_LOAD_DECODE_PRELOAD", "preloadFrames=%lld preloadMs=%lld elapsedUs=%lld",
                     (long long)preloadFrames, elapsedMs(), elapsedUs());

    const double duration = static_cast<double>(numFrames) / sr;

    // Swap preload-sized buffers into RT — deck becomes playable NOW
    {
        const auto tSwap = Clock::now();
        std::unique_lock<std::shared_mutex> lock(bufferMutex_);
        decodedL_ = std::move(newL);
        decodedR_ = std::move(newR);
        totalDecodedFrames_ = numFrames;
        streamDecodedFrames_.store(preloadFrames, std::memory_order_release);
        fileSampleRate_ = sr;
        fileDurationSeconds_ = duration;
        hasAudioData_ = true;
        diagFirstNonzero_ = false;
        readPosition_.store(0, std::memory_order_release);
        fractionalReadPos_ = 0.0;

        if (deviceSampleRate_ > 0.0) {
            resampleRatio_ = sr / deviceSampleRate_;
        }
        const auto swapUs = std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now() - tSwap).count();
        ngks::audioTrace("TRACK_LOAD_SWAP_DONE", "swapUs=%lld elapsedUs=%lld",
                         swapUs, elapsedUs());
    }

    outDurationSeconds = duration;
    ngks::audioTrace("TRACK_LOAD_PRELOAD_DONE", "path=%s frames=%lld preloadFrames=%lld "
                     "dur=%.2fs ch=%d preloadMs=%lld totalMs=%lld",
                     path.c_str(), (long long)numFrames, (long long)preloadFrames,
                     duration, numChannels,
                     elapsedMs(), elapsedMs());

    // Background thread: allocate full buffer, decode entire file, swap in
    if (preloadFrames < numFrames) {
        streamDecodeThread_ = std::thread(
            [this, reader = std::move(reader), preloadFrames, numFrames, numChannels]() mutable {
                const auto bgT0 = Clock::now();
                // Full allocation happens here, OFF the hot path
                std::vector<float> fullL(static_cast<size_t>(numFrames), 0.0f);
                std::vector<float> fullR(static_cast<size_t>(numFrames), 0.0f);
                const auto allocMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                    Clock::now() - bgT0).count();
                ngks::audioTrace("TRACK_LOAD_BG_ALLOC", "frames=%lld allocMs=%lld",
                                 (long long)numFrames, allocMs);

                // Decode entire file from the beginning
                constexpr int64_t bgChunk = 65536;
                int64_t decodedUpTo = 0;
                for (int64_t pos = 0; pos < numFrames; pos += bgChunk) {
                    if (streamCancelled_.load(std::memory_order_acquire))
                        break;
                    const int remaining = static_cast<int>(std::min(bgChunk, numFrames - pos));
                    float* ptrs[2] = { fullL.data() + pos, fullR.data() + pos };
                    reader->read(ptrs, numChannels >= 2 ? 2 : 1, pos, remaining);
                    if (numChannels == 1) {
                        std::memcpy(fullR.data() + pos, fullL.data() + pos,
                                    static_cast<size_t>(remaining) * sizeof(float));
                    }
                    decodedUpTo = pos + remaining;
                }
                reader.reset();

                if (!streamCancelled_.load(std::memory_order_acquire)) {
                    // Atomic swap: brief write-lock, just pointer moves
                    std::unique_lock<std::shared_mutex> lock(bufferMutex_);
                    decodedL_ = std::move(fullL);
                    decodedR_ = std::move(fullR);
                    streamDecodedFrames_.store(decodedUpTo, std::memory_order_release);
                }

                const auto bgTotalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                    Clock::now() - bgT0).count();
                ngks::audioTrace("TRACK_LOAD_STREAM_DONE", "decodedFrames=%lld cancelled=%d bgTotalMs=%lld",
                                 (long long)streamDecodedFrames_.load(std::memory_order_relaxed),
                                 streamCancelled_.load(std::memory_order_relaxed) ? 1 : 0,
                                 bgTotalMs);
            });
    }

    ngks::audioTrace("TRACK_LOAD_TOTAL", "path=%s totalMs=%lld", path.c_str(), elapsedMs());
    std::cout << "DeckNode::loadFile OK: " << path
              << " frames=" << numFrames
              << " sr=" << sr
              << " dur=" << duration << "s"
              << " ch=" << numChannels
              << " totalMs=" << elapsedMs() << std::endl;
    return true;
}

void DeckNode::unloadFile() noexcept
{
    cancelStreamDecode();
    std::unique_lock<std::shared_mutex> lock(bufferMutex_);
    decodedL_.clear();
    decodedR_.clear();
    totalDecodedFrames_ = 0;
    streamDecodedFrames_.store(0, std::memory_order_relaxed);
    fileSampleRate_ = 0.0;
    fileDurationSeconds_ = 0.0;
    hasAudioData_ = false;
    diagFirstNonzero_ = false;
    readPosition_.store(0, std::memory_order_relaxed);
    fractionalReadPos_ = 0.0;
    loadedFilePath_.clear();
}

void DeckNode::seekTo(double seconds) noexcept
{
    std::unique_lock<std::shared_mutex> lock(bufferMutex_);
    if (!hasAudioData_ || fileSampleRate_ <= 0.0) return;
    const double clampedSec = std::max(0.0, std::min(seconds, fileDurationSeconds_));
    const int64_t frame = static_cast<int64_t>(clampedSec * fileSampleRate_);
    const int64_t clampedFrame = std::min(frame, totalDecodedFrames_);
    readPosition_.store(clampedFrame, std::memory_order_release);
    fractionalReadPos_ = static_cast<double>(clampedFrame);
}

double DeckNode::getPlayheadSeconds() const noexcept
{
    std::shared_lock<std::shared_mutex> lock(bufferMutex_);
    if (!hasAudioData_ || fileSampleRate_ <= 0.0) return 0.0;
    return static_cast<double>(readPosition_.load(std::memory_order_relaxed)) / fileSampleRate_;
}

bool DeckNode::isFullyDecoded() const noexcept
{
    const int64_t decoded = streamDecodedFrames_.load(std::memory_order_acquire);
    return hasAudioData_ && totalDecodedFrames_ > 0 && decoded >= totalDecodedFrames_;
}

std::string DeckNode::loadedFilePath() const
{
    std::shared_lock<std::shared_mutex> lock(bufferMutex_);
    return loadedFilePath_;
}

std::vector<WaveMinMax> DeckNode::generateWaveformOverview(int numBins) const
{
    if (numBins <= 0) return {};
    std::shared_lock<std::shared_mutex> lock(bufferMutex_);
    const int64_t frames = streamDecodedFrames_.load(std::memory_order_acquire);
    if (!hasAudioData_ || frames <= 0 || decodedL_.empty()) {
        return std::vector<WaveMinMax>(static_cast<size_t>(numBins), {0.0f, 0.0f});
    }

    std::vector<WaveMinMax> bins(static_cast<size_t>(numBins));
    const float* srcL = decodedL_.data();
    const float* srcR = decodedR_.empty() ? nullptr : decodedR_.data();
    const int64_t usableFrames = std::min(frames, static_cast<int64_t>(decodedL_.size()));

    // True min/max + RMS per bucket.
    // min/max captures transient peaks; RMS captures energy envelope.
    // At overview zoom (~4K samples/bin), RMS shows honest loudness.
    // At zoomed-in views (~few hundred samples/bin), min/max shows real shape.
    for (int b = 0; b < numBins; ++b) {
        const int64_t startF = (usableFrames * b) / numBins;
        const int64_t endF = (usableFrames * (b + 1)) / numBins;
        const int64_t count = endF - startF;
        if (count <= 0) {
            bins[static_cast<size_t>(b)] = {0.0f, 0.0f, 0.0f};
            continue;
        }

        float lo = srcL[startF];
        float hi = srcL[startF];
        double sumSq = 0.0;
        for (int64_t f = startF; f < endF; ++f) {
            const float vL = srcL[f];
            lo = std::min(lo, vL);
            hi = std::max(hi, vL);
            float absV = std::abs(vL);
            if (srcR) {
                const float vR = srcR[f];
                lo = std::min(lo, vR);
                hi = std::max(hi, vR);
                absV = std::max(absV, std::abs(vR));
            }
            sumSq += static_cast<double>(absV) * static_cast<double>(absV);
        }
        const float rms = static_cast<float>(std::sqrt(sumSq / static_cast<double>(count)));
        bins[static_cast<size_t>(b)] = {lo, hi, rms};
    }
    return bins;
}

void DeckNode::render(const DeckSnapshot& deck,
                      int numSamples,
                      float* outLeft,
                      float* outRight,
                      float& outRms,
                      float& outPeak) noexcept
{
    outRms = 0.0f;
    outPeak = 0.0f;

    if (numSamples <= 0 || outLeft == nullptr || outRight == nullptr) {
        return;
    }

    std::shared_lock<std::shared_mutex> lock(bufferMutex_);

    if (!hasAudioData_ || totalDecodedFrames_ <= 0) {
        std::memset(outLeft, 0, static_cast<size_t>(numSamples) * sizeof(float));
        std::memset(outRight, 0, static_cast<size_t>(numSamples) * sizeof(float));
        return;
    }

    float sumSquares = 0.0f;
    const float gain = deck.deckGain;
    // Use streaming boundary — only read up to what's been decoded so far
    const int64_t totalFrames = streamDecodedFrames_.load(std::memory_order_acquire);
    const float* srcL = decodedL_.data();
    const float* srcR = decodedR_.data();

    for (int sample = 0; sample < numSamples; ++sample) {
        float envelope = 0.0f;
        if (deck.transport == TransportState::Playing || deck.transport == TransportState::Starting) {
            envelope = 1.0f;
        } else if (deck.transport == TransportState::Stopping && stopFadeSamplesRemaining > 0) {
            envelope = static_cast<float>(stopFadeSamplesRemaining) / static_cast<float>(stopFadeSamplesTotal);
            --stopFadeSamplesRemaining;
        }

        float valueL = 0.0f;
        float valueR = 0.0f;

        if (deck.hasTrack && envelope > 0.0f) {
            const int64_t intPos = static_cast<int64_t>(fractionalReadPos_);
            if (intPos < totalFrames) {
                const double frac = fractionalReadPos_ - static_cast<double>(intPos);
                const int64_t nextPos = std::min(intPos + 1, totalFrames - 1);

                valueL = static_cast<float>(
                    srcL[intPos] * (1.0 - frac) + srcL[nextPos] * frac) * gain * envelope;
                valueR = static_cast<float>(
                    srcR[intPos] * (1.0 - frac) + srcR[nextPos] * frac) * gain * envelope;

                fractionalReadPos_ += resampleRatio_;
            }
        }

        outLeft[sample] = valueL;
        outRight[sample] = valueR;
        const float mono = 0.5f * (std::abs(valueL) + std::abs(valueR));
        sumSquares += mono * mono;
        outPeak = std::max(outPeak, mono);
    }

    readPosition_.store(static_cast<int64_t>(fractionalReadPos_), std::memory_order_relaxed);
    outRms = std::sqrt(sumSquares / static_cast<float>(numSamples));

    if (!diagFirstNonzero_ && outPeak > 0.001f) {
        diagFirstNonzero_ = true;
        ngks::diagLog("DIAG: DeckNode::render FIRST_NONZERO_OUTPUT deck=%d rms=%.6f peak=%.6f transport=%d hasTrack=%d readPos=%lld frames=%lld",
                     static_cast<int>(deck.id), outRms, outPeak, static_cast<int>(deck.transport), deck.hasTrack,
                     (long long)readPosition_.load(), (long long)totalDecodedFrames_);
    }
}

}