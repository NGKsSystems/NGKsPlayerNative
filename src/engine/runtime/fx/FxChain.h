#pragma once

#include <array>
#include <cstdint>

#include "engine/runtime/fx/DummyGainFx.h"

namespace ngks {

class FxChain {
public:
    static constexpr int kMaxSlots = 8;

    enum class ProcessorType : uint8_t {
        None = 0,
        DummyGain = 1
    };

    struct Slot {
        uint8_t enabled = 0;
        ProcessorType type = ProcessorType::None;
        float param0 = 1.0f;
        DummyGainFx dummyGain;
    };

    bool setSlotEnabled(int slotIndex, bool enabled) noexcept;
    bool setSlotGain(int slotIndex, float gainLinear) noexcept;
    bool isSlotEnabled(int slotIndex) const noexcept;
    void process(float* left, float* right, int numSamples) noexcept;

private:
    std::array<Slot, kMaxSlots> slotStates {};
};

}