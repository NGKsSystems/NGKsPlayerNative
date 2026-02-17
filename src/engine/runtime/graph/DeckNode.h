#pragma once

#include <cstdint>

#include "engine/runtime/EngineSnapshot.h"

namespace ngks {

class DeckNode {
public:
    void prepare(double sampleRate);
    void setFrequency(float hz) noexcept;
    void beginStopFade(int fadeSamples) noexcept;
    bool isStopFadeActive() const noexcept;

    void render(const DeckSnapshot& deck,
                int numSamples,
                float* outLeft,
                float* outRight,
                float& outRms,
                float& outPeak) noexcept;

private:
    float phase = 0.0f;
    float frequencyHz = 220.0f;
    float phaseIncrement = 0.0f;
    int stopFadeSamplesRemaining = 0;
    int stopFadeSamplesTotal = 1;
};

}