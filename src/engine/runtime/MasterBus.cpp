#include "engine/runtime/MasterBus.h"

#include <algorithm>
#include <cmath>

namespace ngks {

void MasterBus::setGainTrim(float gainTrim) noexcept
{
    gainTrim_ = std::clamp(gainTrim, 0.0f, 12.0f);
}

MasterBusMeters MasterBus::process(float* left, float* right, int numSamples) noexcept
{
    MasterBusMeters meters;
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return meters;
    }

    float sumSquaresL = 0.0f;
    float sumSquaresR = 0.0f;

    for (int sample = 0; sample < numSamples; ++sample) {
        float l = left[sample] * gainTrim_;
        float r = right[sample] * gainTrim_;

        const float absL = std::abs(l);
        const float absR = std::abs(r);

        if (absL > kLimiterThreshold) {
            l = (l >= 0.0f) ? kLimiterThreshold : -kLimiterThreshold;
            meters.limiterEngaged = true;
        }

        if (absR > kLimiterThreshold) {
            r = (r >= 0.0f) ? kLimiterThreshold : -kLimiterThreshold;
            meters.limiterEngaged = true;
        }

        left[sample] = l;
        right[sample] = r;

        sumSquaresL += l * l;
        sumSquaresR += r * r;
        meters.masterPeakL = std::max(meters.masterPeakL, std::abs(l));
        meters.masterPeakR = std::max(meters.masterPeakR, std::abs(r));
    }

    const float denom = static_cast<float>(numSamples);
    meters.masterRmsL = std::sqrt(sumSquaresL / denom);
    meters.masterRmsR = std::sqrt(sumSquaresR / denom);
    return meters;
}

}