#include "engine/dsp/ParametricEQ16.h"

#include <algorithm>
#include <cmath>

namespace ngks {

// ── constexpr definition (C++17 inline, but some MSVC builds want it) ──
constexpr float ParametricEQ16::kCenterFreqs[kBandCount];

static constexpr float kPi = 3.14159265358979323846f;

// Default Q for each band — wider at extremes, tighter in mids.
// This gives a musical response across the 16-band range.
static constexpr float kBandQ[ParametricEQ16::kBandCount] = {
    0.8f,  0.9f,  1.0f,  1.1f,
    1.2f,  1.3f,  1.4f,  1.4f,
    1.4f,  1.3f,  1.2f,  1.1f,
    1.0f,  0.9f,  0.8f,  0.7f
};

void ParametricEQ16::prepare(double sampleRate) noexcept
{
    sampleRate_ = sampleRate > 0.0 ? sampleRate : 48000.0;
    bandGainDb_.fill(0.0f);
    for (int i = 0; i < kBandCount; ++i)
        recalcCoeffs(i);
    reset();
}

void ParametricEQ16::setBandGain(int band, float gainDb) noexcept
{
    if (band < 0 || band >= kBandCount) return;
    gainDb = clampGain(gainDb);
    bandGainDb_[static_cast<size_t>(band)] = gainDb;
    recalcCoeffs(band);
}

float ParametricEQ16::getBandGain(int band) const noexcept
{
    if (band < 0 || band >= kBandCount) return 0.0f;
    return bandGainDb_[static_cast<size_t>(band)];
}

void ParametricEQ16::process(float* left, float* right, int numSamples) noexcept
{
    if (bypassed_ || left == nullptr || right == nullptr || numSamples <= 0)
        return;

    for (int band = 0; band < kBandCount; ++band) {
        // Skip bands that are flat (0 dB) — their coeffs are identity
        if (bandGainDb_[static_cast<size_t>(band)] == 0.0f)
            continue;

        const auto& c = coeffs_[static_cast<size_t>(band)];
        auto& sL = stateL_[static_cast<size_t>(band)];
        auto& sR = stateR_[static_cast<size_t>(band)];

        // Direct Form II Transposed biquad
        for (int i = 0; i < numSamples; ++i) {
            // Left channel
            {
                const float x = left[i];
                const float y = c.b0 * x + sL.z1;
                sL.z1 = c.b1 * x - c.a1 * y + sL.z2;
                sL.z2 = c.b2 * x - c.a2 * y;
                left[i] = y;
            }
            // Right channel
            {
                const float x = right[i];
                const float y = c.b0 * x + sR.z1;
                sR.z1 = c.b1 * x - c.a1 * y + sR.z2;
                sR.z2 = c.b2 * x - c.a2 * y;
                right[i] = y;
            }
        }
    }

    // Soft clamp output to prevent downstream clipping
    for (int i = 0; i < numSamples; ++i) {
        if (left[i] > kOutputCeiling)       left[i] = kOutputCeiling;
        else if (left[i] < -kOutputCeiling) left[i] = -kOutputCeiling;
        if (right[i] > kOutputCeiling)       right[i] = kOutputCeiling;
        else if (right[i] < -kOutputCeiling) right[i] = -kOutputCeiling;
    }
}

void ParametricEQ16::reset() noexcept
{
    for (auto& s : stateL_) { s.z1 = 0.0f; s.z2 = 0.0f; }
    for (auto& s : stateR_) { s.z1 = 0.0f; s.z2 = 0.0f; }
}

// Peaking EQ biquad coefficient calculation (Audio EQ Cookbook, Robert Bristow-Johnson)
void ParametricEQ16::recalcCoeffs(int band) noexcept
{
    auto& c = coeffs_[static_cast<size_t>(band)];
    const float gainDb = bandGainDb_[static_cast<size_t>(band)];

    // Identity passthrough for flat bands
    if (gainDb == 0.0f) {
        c.b0 = 1.0f; c.b1 = 0.0f; c.b2 = 0.0f;
        c.a1 = 0.0f; c.a2 = 0.0f;
        return;
    }

    const float freq = kCenterFreqs[band];
    const float Q = kBandQ[band];
    const float A = std::pow(10.0f, gainDb / 40.0f);  // amplitude = 10^(dB/40) for peaking
    const float w0 = 2.0f * kPi * freq / static_cast<float>(sampleRate_);
    const float sinW0 = std::sin(w0);
    const float cosW0 = std::cos(w0);
    const float alpha = sinW0 / (2.0f * Q);

    const float b0 =  1.0f + alpha * A;
    const float b1 = -2.0f * cosW0;
    const float b2 =  1.0f - alpha * A;
    const float a0 =  1.0f + alpha / A;
    const float a1 = -2.0f * cosW0;
    const float a2 =  1.0f - alpha / A;

    // Normalize by a0
    const float invA0 = 1.0f / a0;
    c.b0 = b0 * invA0;
    c.b1 = b1 * invA0;
    c.b2 = b2 * invA0;
    c.a1 = a1 * invA0;
    c.a2 = a2 * invA0;
}

} // namespace ngks
