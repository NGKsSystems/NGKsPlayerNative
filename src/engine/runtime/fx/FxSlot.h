#pragma once

#include <cstdint>

#include "engine/runtime/fx/FxTypes.h"

namespace ngks {

struct FxSlotState {
    bool enabled{false};
    float dryWet{0.0f};
    uint32_t type{static_cast<uint32_t>(FxType::None)};
};

struct FxSlot {
    FxSlotState state{};
    float param0{1.0f};
    float filterStateL{0.0f};
    float filterStateR{0.0f};
};

}