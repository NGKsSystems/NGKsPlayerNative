#include "engine/runtime/graph/CueMixNode.h"

namespace ngks {

void CueMixNode::clear(float* left, float* right, int numSamples) const noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        left[sample] = 0.0f;
        right[sample] = 0.0f;
    }
}

void CueMixNode::accumulate(const float* inLeft,
                            const float* inRight,
                            float* cueLeft,
                            float* cueRight,
                            int numSamples,
                            float weight) const noexcept
{
    if (inLeft == nullptr || inRight == nullptr || cueLeft == nullptr || cueRight == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        cueLeft[sample] += inLeft[sample] * weight;
        cueRight[sample] += inRight[sample] * weight;
    }
}

}