#include "engine/runtime/fx/FxChain.h"

#include <algorithm>
#include <cmath>

namespace ngks {

namespace {

float clamp01(float value) noexcept
{
    return std::clamp(value, 0.0f, 1.0f);
}

float applyFxSample(FxSlot& slot, float input, bool rightChannel) noexcept
{
    const auto type = static_cast<FxType>(slot.state.type);
    float wet = input;

    switch (type) {
    case FxType::Gain: {
        const float gain = std::clamp(slot.param0, 0.0f, 2.0f);
        wet = input * gain;
        break;
    }
    case FxType::SoftClip: {
        const float drive = std::clamp(slot.param0, 0.25f, 8.0f);
        const float x = input * drive;
        wet = x / (1.0f + std::abs(x));
        break;
    }
    case FxType::SimpleFilter: {
        const float alpha = std::clamp(slot.param0, 0.01f, 0.5f);
        float& state = rightChannel ? slot.filterStateR : slot.filterStateL;
        state = state + alpha * (input - state);
        wet = state;
        break;
    }
    case FxType::DjFilter: {
        // DJ filter: param0 0.0=full LPF, 0.5=neutral, 1.0=full HPF
        const float pos = std::clamp(slot.param0, 0.0f, 1.0f);
        float& state = rightChannel ? slot.filterStateR : slot.filterStateL;
        constexpr float deadLo = 0.47f;
        constexpr float deadHi = 0.53f;
        if (pos < deadLo) {
            // LPF: pos 0.0→0.47 maps alpha 0.01→0.50
            const float t = pos / deadLo;
            const float alpha = 0.01f + t * 0.49f;
            state = state + alpha * (input - state);
            wet = state;
        } else if (pos > deadHi) {
            // HPF: pos 0.53→1.0 maps alpha 0.50→0.01
            const float t = (pos - deadHi) / (1.0f - deadHi);
            const float alpha = 0.50f - t * 0.49f;
            state = state + alpha * (input - state);
            wet = input - state;
        } else {
            // Dead zone around center = neutral passthrough
            state = input;
            wet = input;
        }
        break;
    }
    case FxType::None:
    default:
        wet = input;
        break;
    }

    const float mix = clamp01(slot.state.dryWet);
    return input + (wet - input) * mix;
}

}

bool FxChain::setSlotEnabled(int slotIndex, bool enabled) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    slotStates_[slotIndex].state.enabled = enabled;
    return true;
}

bool FxChain::setSlotType(int slotIndex, uint32_t fxType) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    auto& slot = slotStates_[slotIndex];
    const auto type = static_cast<FxType>(fxType);
    switch (type) {
    case FxType::None:
    case FxType::Gain:
    case FxType::SoftClip:
    case FxType::SimpleFilter:
    case FxType::DjFilter:
        slot.state.type = fxType;
        slot.filterStateL = 0.0f;
        slot.filterStateR = 0.0f;
        return true;
    default:
        return false;
    }
}

bool FxChain::setSlotDryWet(int slotIndex, float dryWet) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    slotStates_[slotIndex].state.dryWet = clamp01(dryWet);
    return true;
}

bool FxChain::setSlotParam0(int slotIndex, float value) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    slotStates_[slotIndex].param0 = value;
    return true;
}

bool FxChain::isSlotEnabled(int slotIndex) const noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    return slotStates_[slotIndex].state.enabled;
}

FxSlotState FxChain::getSlotState(int slotIndex) const noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return {};
    }

    return slotStates_[slotIndex].state;
}

void FxChain::process(float* left, float* right, int numSamples) noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    for (auto& slot : slotStates_) {
        if (!slot.state.enabled || slot.state.type == static_cast<uint32_t>(FxType::None)) {
            continue;
        }

        for (int sample = 0; sample < numSamples; ++sample) {
            left[sample] = applyFxSample(slot, left[sample], false);
            right[sample] = applyFxSample(slot, right[sample], true);
        }
    }
}

}