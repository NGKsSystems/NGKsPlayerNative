#include "engine/runtime/fx/DummyGainFx.h"

#include <algorithm>

namespace ngks {

void DummyGainFx::setGain(float linear) noexcept
{
    gainLinear = std::clamp(linear, 0.0f, 2.0f);
}

float DummyGainFx::gain() const noexcept
{
    return gainLinear;
}

void DummyGainFx::process(float* left, float* right, int numSamples) noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        left[sample] *= gainLinear;
        right[sample] *= gainLinear;
    }
}

}