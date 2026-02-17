#include "engine/runtime/graph/MasterMixNode.h"

namespace ngks {

void MasterMixNode::clear(float* left, float* right, int numSamples) const noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        left[sample] = 0.0f;
        right[sample] = 0.0f;
    }
}

void MasterMixNode::accumulate(const float* inLeft,
                               const float* inRight,
                               float* outLeft,
                               float* outRight,
                               int numSamples,
                               float weight) const noexcept
{
    if (inLeft == nullptr || inRight == nullptr || outLeft == nullptr || outRight == nullptr || numSamples <= 0) {
        return;
    }

    for (int sample = 0; sample < numSamples; ++sample) {
        outLeft[sample] += inLeft[sample] * weight;
        outRight[sample] += inRight[sample] * weight;
    }
}

}