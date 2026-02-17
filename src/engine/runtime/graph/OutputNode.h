#pragma once

namespace ngks {

class OutputNode {
public:
    void renderToDevice(const float* masterLeft,
                        const float* masterRight,
                        int numSamples,
                        float masterGain,
                        float* outLeft,
                        float* outRight) const noexcept;
};

}