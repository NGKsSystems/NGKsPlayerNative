#include "engine/dsp/Limiter.h"

#include <algorithm>

float Limiter::processSample(float sample) const noexcept
{
    return std::clamp(sample, -0.98f, 0.98f);
}