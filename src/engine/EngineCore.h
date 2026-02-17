#pragma once

#include <cstddef>

#include "engine/dsp/Limiter.h"
#include "engine/dsp/Meter.h"

struct MeterSnapshot
{
    float left = 0.0f;
    float right = 0.0f;
};

class EngineCore
{
public:
    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;
    MeterSnapshot consumeMeterSnapshot() noexcept;

private:
    double sampleRateHz = 48000.0;
    float phase = 0.0f;
    float phaseIncrement = 0.0f;
    Limiter limiter;
    Meter meter;
};