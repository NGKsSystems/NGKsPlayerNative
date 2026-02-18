#pragma once

namespace ngks {

class FxProcessor {
public:
    virtual ~FxProcessor() = default;
    virtual void process(float* left, float* right, int numSamples) noexcept = 0;
};

}