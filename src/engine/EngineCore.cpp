#include "engine/EngineCore.h"

#include <algorithm>
#include <cmath>

namespace
{
constexpr float twoPi = 6.28318530717958647692f;
}

void EngineCore::prepare(double sampleRate, int)
{
    sampleRateHz = (sampleRate > 0.0) ? sampleRate : 48000.0;
    phase = 0.0f;
    phaseIncrement = (twoPi * 440.0f) / static_cast<float>(sampleRateHz);
}

void EngineCore::process(float* left, float* right, int numSamples) noexcept
{
    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        const float tone = std::sin(phase) * 0.1f;
        phase += phaseIncrement;
        if (phase >= twoPi) {
            phase -= twoPi;
        }

        const float limited = limiter.processSample(tone);
        left[sample] = limited;
        right[sample] = limited;
    }

    meter.updateBlock(left, right, numSamples);
}

MeterSnapshot EngineCore::consumeMeterSnapshot() noexcept
{
    const MeterValues values = meter.consumeAndReset();
    return MeterSnapshot{ values.leftPeak, values.rightPeak };
}