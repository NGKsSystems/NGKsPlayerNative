#pragma once

namespace ngks {

class CueMixNode {
public:
    void clear(float* left, float* right, int numSamples) const noexcept;
    void accumulate(const float* inLeft,
                    const float* inRight,
                    float* cueLeft,
                    float* cueRight,
                    int numSamples,
                    float weight) const noexcept;
};

}