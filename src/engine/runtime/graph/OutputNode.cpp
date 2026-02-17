#include "engine/runtime/graph/OutputNode.h"

#include <algorithm>

namespace ngks {

void OutputNode::renderToDevice(const float* masterLeft,
                                const float* masterRight,
                                int numSamples,
                                float masterGain,
                                float* outLeft,
                                float* outRight) const noexcept
{
    if (masterLeft == nullptr || masterRight == nullptr || outLeft == nullptr || outRight == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        const float scaledL = std::clamp(masterLeft[sample] * masterGain, -0.98f, 0.98f);
        const float scaledR = std::clamp(masterRight[sample] * masterGain, -0.98f, 0.98f);
        outLeft[sample] = scaledL;
        outRight[sample] = scaledR;
    }
}

}