#include "engine/runtime/fx/FxChain.h"

namespace ngks {

bool FxChain::setSlotEnabled(int slotIndex, bool enabled) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    auto& slot = slotStates[slotIndex];
    slot.enabled = enabled ? 1 : 0;
    if (slot.type == ProcessorType::None && enabled) {
        slot.type = ProcessorType::DummyGain;
        slot.param0 = 1.0f;
        slot.dummyGain.setGain(slot.param0);
    }
    return true;
}

bool FxChain::setSlotGain(int slotIndex, float gainLinear) noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    auto& slot = slotStates[slotIndex];
    if (slot.type == ProcessorType::None) {
        slot.type = ProcessorType::DummyGain;
    }

    slot.param0 = gainLinear;
    slot.dummyGain.setGain(slot.param0);
    return true;
}

bool FxChain::isSlotEnabled(int slotIndex) const noexcept
{
    if (slotIndex < 0 || slotIndex >= kMaxSlots) {
        return false;
    }

    return slotStates[slotIndex].enabled != 0;
}

void FxChain::process(float* left, float* right, int numSamples) noexcept
{
    if (left == nullptr || right == nullptr || numSamples <= 0) {
        return;
    }

    for (auto& slot : slotStates) {
        if (!slot.enabled) {
            continue;
        }

        switch (slot.type) {
        case ProcessorType::DummyGain:
            slot.dummyGain.process(left, right, numSamples);
            break;
        case ProcessorType::None:
        default:
            break;
        }
    }
}

}