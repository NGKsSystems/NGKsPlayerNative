#pragma once

#include <atomic>
#include <cstddef>
#include <memory>

#include "engine/dsp/Limiter.h"
#include "engine/dsp/Meter.h"

class AudioIOJuce;

struct MeterSnapshot
{
    float left = 0.0f;
    float right = 0.0f;
};

class EngineCore
{
public:
    EngineCore();
    ~EngineCore();

    bool startAudioIfNeeded();
    void stopWithFade();
    void setMasterGain(double linear01) noexcept;
    MeterSnapshot getSnapshot() noexcept;
    bool isRunning() const noexcept;

    int getRequestedBufferSize() const noexcept;
    int getActualBufferSize() const noexcept;
    double getSampleRate() const noexcept;

    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;

private:
    std::unique_ptr<AudioIOJuce> audioIO;
    std::atomic<bool> audioOpened { false };
    std::atomic<bool> running { false };
    std::atomic<float> masterGain { 1.0f };
    std::atomic<int> fadeSamplesRemaining { 0 };

    double sampleRateHz = 48000.0;
    float phase = 0.0f;
    float phaseIncrement = 0.0f;
    int fadeSamplesTotal = 9600;
    int requestedBufferSize = 128;
    int actualBufferSize = 0;

    Limiter limiter;
    Meter meter;
};