#include "engine/runtime/graph/DeckNode.h"

#include "engine/DiagLog.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <iostream>

namespace ngks {

DeckNode::DeckNode()
{
    formatManager_.registerBasicFormats(); // WAV, AIFF, FLAC, OGG, MP3 (via juce_audio_formats)
}

DeckNode::~DeckNode() = default;

void DeckNode::prepare(double sampleRate)
{
    deviceSampleRate_ = (sampleRate > 0.0) ? sampleRate : 48000.0;
    stopFadeSamplesRemaining = 0;
    stopFadeSamplesTotal = std::max(1, static_cast<int>(deviceSampleRate_ * 0.2));

    // Update resample ratio if file is loaded
    if (fileSampleRate_ > 0.0) {
        resampleRatio_ = fileSampleRate_ / deviceSampleRate_;
    }
}

void DeckNode::beginStopFade(int fadeSamples) noexcept
{
    stopFadeSamplesTotal = std::max(1, fadeSamples);
    stopFadeSamplesRemaining = stopFadeSamplesTotal;
}

bool DeckNode::isStopFadeActive() const noexcept
{
    return stopFadeSamplesRemaining > 0;
}

bool DeckNode::loadFile(const std::string& path, double& outDurationSeconds)
{
    outDurationSeconds = 0.0;

    juce::String jucePath(path.c_str());
    juce::File file(jucePath);
    if (!file.existsAsFile()) {
        std::cerr << "DeckNode::loadFile: file not found: " << path << std::endl;
        return false;
    }

    std::unique_ptr<juce::AudioFormatReader> reader(
        formatManager_.createReaderFor(file));

    if (!reader) {
        std::cerr << "DeckNode::loadFile: no codec for: " << path << std::endl;
        return false;
    }

    const int64_t numFrames = static_cast<int64_t>(reader->lengthInSamples);
    const int numChannels = static_cast<int>(reader->numChannels);
    const double sr = reader->sampleRate;

    if (numFrames <= 0 || sr <= 0.0) {
        std::cerr << "DeckNode::loadFile: empty or invalid: " << path << std::endl;
        return false;
    }

    // Decode into separate L/R buffers
    std::vector<float> newL(static_cast<size_t>(numFrames), 0.0f);
    std::vector<float> newR(static_cast<size_t>(numFrames), 0.0f);

    // Read in chunks to handle large files
    constexpr int64_t chunkSize = 65536;
    for (int64_t pos = 0; pos < numFrames; pos += chunkSize) {
        const int remaining = static_cast<int>(std::min(chunkSize, numFrames - pos));
        float* chunkPtrs[2] = { newL.data() + pos, newR.data() + pos };
        reader->read(chunkPtrs, numChannels >= 2 ? 2 : 1,
                     pos, remaining);
    }

    // If mono, copy L to R
    if (numChannels == 1) {
        std::memcpy(newR.data(), newL.data(), static_cast<size_t>(numFrames) * sizeof(float));
    }

    const double duration = static_cast<double>(numFrames) / sr;

    // Swap into RT-accessible buffers under lock
    {
        std::lock_guard<std::mutex> lock(bufferMutex_);
        decodedL_ = std::move(newL);
        decodedR_ = std::move(newR);
        totalDecodedFrames_ = numFrames;
        fileSampleRate_ = sr;
        fileDurationSeconds_ = duration;
        hasAudioData_ = true;
        readPosition_.store(0, std::memory_order_release);
        fractionalReadPos_ = 0.0;

        if (deviceSampleRate_ > 0.0) {
            resampleRatio_ = sr / deviceSampleRate_;
        }
    }

    outDurationSeconds = duration;
    std::cout << "DeckNode::loadFile OK: " << path
              << " frames=" << numFrames
              << " sr=" << sr
              << " dur=" << duration << "s"
              << " ch=" << numChannels << std::endl;
    return true;
}

void DeckNode::unloadFile() noexcept
{
    std::lock_guard<std::mutex> lock(bufferMutex_);
    decodedL_.clear();
    decodedR_.clear();
    totalDecodedFrames_ = 0;
    fileSampleRate_ = 0.0;
    fileDurationSeconds_ = 0.0;
    hasAudioData_ = false;
    readPosition_.store(0, std::memory_order_relaxed);
    fractionalReadPos_ = 0.0;
}

void DeckNode::seekTo(double seconds) noexcept
{
    if (!hasAudioData_ || fileSampleRate_ <= 0.0) return;
    const double clampedSec = std::max(0.0, std::min(seconds, fileDurationSeconds_));
    const int64_t frame = static_cast<int64_t>(clampedSec * fileSampleRate_);
    readPosition_.store(std::min(frame, totalDecodedFrames_), std::memory_order_release);
    fractionalReadPos_ = static_cast<double>(frame);
}

double DeckNode::getPlayheadSeconds() const noexcept
{
    if (!hasAudioData_ || fileSampleRate_ <= 0.0) return 0.0;
    return static_cast<double>(readPosition_.load(std::memory_order_relaxed)) / fileSampleRate_;
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

    // If no audio data loaded, output silence
    if (!hasAudioData_ || totalDecodedFrames_ <= 0) {
        std::memset(outLeft, 0, static_cast<size_t>(numSamples) * sizeof(float));
        std::memset(outRight, 0, static_cast<size_t>(numSamples) * sizeof(float));
        // One-shot diagnostic
        static int silenceLogCount = 0;
        if (silenceLogCount < 3 && deck.hasTrack) {
            ++silenceLogCount;
            ngks::diagLog("DIAG: DeckNode::render SILENCE hasAudioData=%d frames=%lld hasTrack=%d transport=%d",
                         (int)hasAudioData_, (long long)totalDecodedFrames_, deck.hasTrack, static_cast<int>(deck.transport));
        }
        return;
    }

    float sumSquares = 0.0f;
    const float gain = deck.deckGain;
    const int64_t totalFrames = totalDecodedFrames_;
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
            // Linear interpolation with resample ratio
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
            // else: past end, output silence (valueL/R remain 0)
        }

        outLeft[sample] = valueL;
        outRight[sample] = valueR;
        const float mono = 0.5f * (std::abs(valueL) + std::abs(valueR));
        sumSquares += mono * mono;
        outPeak = std::max(outPeak, mono);
    }

    // Update the atomic read position for snapshot reporting
    readPosition_.store(static_cast<int64_t>(fractionalReadPos_), std::memory_order_relaxed);

    outRms = std::sqrt(sumSquares / static_cast<float>(numSamples));

    if (!diagFirstNonzero_ && outPeak > 0.001f) {
        diagFirstNonzero_ = true;
        ngks::diagLog("DIAG: DeckNode::render FIRST_NONZERO_OUTPUT rms=%.6f peak=%.6f transport=%d hasTrack=%d readPos=%lld frames=%lld",
                     outRms, outPeak, static_cast<int>(deck.transport), deck.hasTrack,
                     (long long)readPosition_.load(), (long long)totalDecodedFrames_);
    }
}

}