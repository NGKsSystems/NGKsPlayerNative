#pragma once

#include <array>
#include <cmath>
#include <cstdint>

namespace ngks {

/// 16-band parametric EQ using cascaded biquad filters (peaking EQ).
/// Each band is a second-order IIR peaking filter (bell curve).
/// Thread-safe for RT: all state is plain floats, no allocations.
class ParametricEQ16 {
public:
    static constexpr int kBandCount = 16;
    static constexpr float kMinGainDb = -6.0f;
    static constexpr float kMaxGainDb =  6.0f;
    static constexpr float kOutputCeiling = 0.98f;

    static constexpr float kCenterFreqs[kBandCount] = {
        20.0f,   32.0f,   50.0f,   80.0f,
        125.0f,  200.0f,  315.0f,  500.0f,
        800.0f,  1250.0f, 2000.0f, 3150.0f,
        5000.0f, 8000.0f, 12500.0f, 16000.0f
    };

    void prepare(double sampleRate) noexcept;

    /// Set gain for one band in dB. Clamped to [-12, +12].
    void setBandGain(int band, float gainDb) noexcept;

    /// Get current gain for a band in dB.
    float getBandGain(int band) const noexcept;

    /// Set bypass state. When bypassed, process() is a no-op.
    void setBypass(bool bypassed) noexcept { bypassed_ = bypassed; }
    bool isBypassed() const noexcept { return bypassed_; }

    /// Process stereo buffers in-place. RT-safe, no allocations.
    void process(float* left, float* right, int numSamples) noexcept;

    /// Reset all filter states (call on seek / track load to avoid transients).
    void reset() noexcept;

private:
    // Biquad coefficients for one band
    struct BiquadCoeffs {
        float b0{1.0f}, b1{0.0f}, b2{0.0f};
        float a1{0.0f}, a2{0.0f};
    };

    // Biquad state for one channel of one band
    struct BiquadState {
        float z1{0.0f}, z2{0.0f};
    };

    void recalcCoeffs(int band) noexcept;

    static float clampGain(float db) noexcept {
        return db < kMinGainDb ? kMinGainDb : (db > kMaxGainDb ? kMaxGainDb : db);
    }

    double sampleRate_{48000.0};
    bool bypassed_{false};

    std::array<float, kBandCount> bandGainDb_{};           // dB per band (default 0)
    std::array<BiquadCoeffs, kBandCount> coeffs_{};        // filter coefficients
    std::array<BiquadState, kBandCount> stateL_{};         // left channel state
    std::array<BiquadState, kBandCount> stateR_{};         // right channel state
};

} // namespace ngks
