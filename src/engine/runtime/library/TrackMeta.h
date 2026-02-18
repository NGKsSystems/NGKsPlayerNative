#pragma once

#include <cstdint>

namespace ngks {

struct TrackMeta {
    uint64_t trackId{0};
    char label[64]{};
    uint32_t durationMs{0};
    uint32_t flags{0};
};

}
