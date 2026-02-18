#pragma once

namespace ngks {

struct MasterBusMeters {
    float masterRmsL{0.0f};
    float masterRmsR{0.0f};
    float masterPeakL{0.0f};
    float masterPeakR{0.0f};
    bool limiterEngaged{false};
};

class MasterBus {
public:
    static constexpr float kLimiterThreshold = 0.95f;

    void setGainTrim(float gainTrim) noexcept;
    MasterBusMeters process(float* left, float* right, int numSamples) noexcept;

private:
    float gainTrim_ = 1.0f;
};

}