#pragma once

#include <array>
#include <cstdint>

#include "engine/runtime/fx/FxSlot.h"

namespace ngks {

class FxChain {
public:
    static constexpr int kMaxSlots = 4;

    bool setSlotEnabled(int slotIndex, bool enabled) noexcept;
    bool setSlotType(int slotIndex, uint32_t fxType) noexcept;
    bool setSlotDryWet(int slotIndex, float dryWet) noexcept;
    bool setSlotParam0(int slotIndex, float value) noexcept;
    bool isSlotEnabled(int slotIndex) const noexcept;
    FxSlotState getSlotState(int slotIndex) const noexcept;
    void process(float* left, float* right, int numSamples) noexcept;

private:
    std::array<FxSlot, kMaxSlots> slotStates_ {};
};

}