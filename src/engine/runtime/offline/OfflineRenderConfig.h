#pragma once

#include <cstdint>

namespace ngks {

struct OfflineRenderConfig {
    uint32_t sampleRate{48000};
    uint32_t blockSize{256};
    uint32_t channels{2};
    float secondsToRender{5.0f};
    float masterGain{1.0f};
    uint32_t seed{0};
};

}
