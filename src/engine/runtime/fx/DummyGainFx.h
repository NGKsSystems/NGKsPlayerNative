#pragma once

#include "engine/runtime/fx/FxProcessor.h"

namespace ngks {

class DummyGainFx final : public FxProcessor {
public:
    void setGain(float linear) noexcept;
    float gain() const noexcept;
    void process(float* left, float* right, int numSamples) noexcept override;

private:
    float gainLinear = 1.0f;
};

}