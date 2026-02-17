#pragma once

#include <atomic>

struct MeterValues
{
    float leftPeak = 0.0f;
    float rightPeak = 0.0f;
};

class Meter
{
public:
    void updateBlock(const float* left, const float* right, int numSamples) noexcept;
    MeterValues consumeAndReset() noexcept;

private:
    static void updateAtomicPeak(std::atomic<float>& target, float value) noexcept;

    std::atomic<float> peakLeft { 0.0f };
    std::atomic<float> peakRight { 0.0f };
};