#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>
#include <cmath>

namespace
{
constexpr float twoPi = 6.28318530717958647692f;
}

EngineCore::EngineCore()
    : audioIO(std::make_unique<AudioIOJuce>(*this))
{
}

EngineCore::~EngineCore()
{
    if (audioIO != nullptr) {
        audioIO->stop();
    }
}

bool EngineCore::startAudioIfNeeded()
{
    if (!audioOpened.load(std::memory_order_acquire)) {
        auto result = audioIO->start();
        if (!result.ok) {
            return false;
        }

        actualBufferSize = result.actualBufferSize;
        sampleRateHz = result.sampleRate;
        audioOpened.store(true, std::memory_order_release);
    }

    fadeSamplesRemaining.store(0, std::memory_order_release);
    running.store(true, std::memory_order_release);
    return true;
}

void EngineCore::stopWithFade()
{
    if (!running.load(std::memory_order_acquire)) {
        return;
    }

    fadeSamplesRemaining.store(fadeSamplesTotal, std::memory_order_release);
    running.store(false, std::memory_order_release);
}

void EngineCore::setMasterGain(double linear01) noexcept
{
    const auto clamped = static_cast<float>(std::clamp(linear01, 0.0, 1.0));
    masterGain.store(clamped, std::memory_order_release);
}

MeterSnapshot EngineCore::getSnapshot() noexcept
{
    const MeterValues values = meter.consumeAndReset();
    return MeterSnapshot{ values.leftPeak, values.rightPeak };
}

bool EngineCore::isRunning() const noexcept
{
    return running.load(std::memory_order_acquire)
        || fadeSamplesRemaining.load(std::memory_order_acquire) > 0;
}

int EngineCore::getRequestedBufferSize() const noexcept
{
    return requestedBufferSize;
}

int EngineCore::getActualBufferSize() const noexcept
{
    return actualBufferSize;
}

double EngineCore::getSampleRate() const noexcept
{
    return sampleRateHz;
}

void EngineCore::prepare(double sampleRate, int)
{
    sampleRateHz = (sampleRate > 0.0) ? sampleRate : 48000.0;
    phase = 0.0f;
    phaseIncrement = (twoPi * 440.0f) / static_cast<float>(sampleRateHz);
    fadeSamplesTotal = static_cast<int>(sampleRateHz * 0.2);
    if (fadeSamplesTotal < 1) {
        fadeSamplesTotal = 1;
    }
}

void EngineCore::process(float* left, float* right, int numSamples) noexcept
{
    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        return;
    }

    const bool runningNow = running.load(std::memory_order_relaxed);
    int fadeRemaining = fadeSamplesRemaining.load(std::memory_order_relaxed);
    const float gainNow = masterGain.load(std::memory_order_relaxed);

    for (int sample = 0; sample < numSamples; ++sample) {
        float envelope = 0.0f;
        if (runningNow) {
            envelope = 1.0f;
        } else if (fadeRemaining > 0) {
            envelope = static_cast<float>(fadeRemaining) / static_cast<float>(fadeSamplesTotal);
            --fadeRemaining;
        }

        const float tone = std::sin(phase) * 0.1f * gainNow * envelope;
        phase += phaseIncrement;
        if (phase >= twoPi) {
            phase -= twoPi;
        }

        const float limited = limiter.processSample(tone);
        left[sample] = limited;
        right[sample] = limited;
    }

    fadeSamplesRemaining.store(fadeRemaining, std::memory_order_relaxed);

    meter.updateBlock(left, right, numSamples);
}