#pragma once

namespace ngks {

class MasterMixNode {
public:
    void clear(float* left, float* right, int numSamples) const noexcept;
    void accumulate(const float* inLeft,
                    const float* inRight,
                    float* outLeft,
                    float* outRight,
                    int numSamples,
                    float weight) const noexcept;
};

}