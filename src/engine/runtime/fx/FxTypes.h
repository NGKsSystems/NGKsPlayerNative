#pragma once

#include <cstdint>

namespace ngks {

enum class FxType : uint32_t {
    None = 0,
    Gain = 1,
    SoftClip = 2,
    SimpleFilter = 3
};

}