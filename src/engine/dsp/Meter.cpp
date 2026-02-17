#include "engine/dsp/Meter.h"

#include <algorithm>
#include <cmath>

void Meter::updateAtomicPeak(std::atomic<float>& target, float value) noexcept
{
    float current = target.load(std::memory_order_relaxed);
    while (value > current && !target.compare_exchange_weak(current, value, std::memory_order_relaxed)) {
    }
}

void Meter::updateBlock(const float* left, const float* right, int numSamples) noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    float localLeft = 0.0f;
    float localRight = 0.0f;

    for (int sample = 0; sample < numSamples; ++sample) {
        localLeft = std::max(localLeft, std::abs(left[sample]));
        localRight = std::max(localRight, std::abs(right[sample]));
    }

    updateAtomicPeak(peakLeft, localLeft);
    updateAtomicPeak(peakRight, localRight);
}

MeterValues Meter::consumeAndReset() noexcept
{
    MeterValues values;
    values.leftPeak = peakLeft.exchange(0.0f, std::memory_order_relaxed);
    values.rightPeak = peakRight.exchange(0.0f, std::memory_order_relaxed);
    return values;
}