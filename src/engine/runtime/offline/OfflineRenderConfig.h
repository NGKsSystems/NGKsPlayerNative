#pragma once

#include <cstdint>

namespace ngks {

enum class OfflineWavFormat : uint32_t {
    Pcm16 = 1,
    Float32 = 3
};

struct OfflineRenderConfig {
    uint32_t sampleRate{48000};
    uint32_t blockSize{256};
    uint32_t channels{2};
    float secondsToRender{5.0f};
    float masterGain{1.0f};
    uint32_t seed{0};
    OfflineWavFormat wavFormat{OfflineWavFormat::Pcm16};
};

}
