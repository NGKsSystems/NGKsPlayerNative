#pragma once

#include <cstdint>
#include <string>

#include "engine/runtime/offline/OfflineRenderConfig.h"

namespace ngks {

struct OfflineRenderResult {
    bool success{false};
    uint32_t renderedFrames{0};
    float peakAbs{0.0f};
    uint16_t wavFormatCode{0};
    uint16_t bitsPerSample{0};
    uint16_t blockAlign{0};
    uint32_t sampleRate{0};
    uint16_t channels{0};
};

class OfflineRenderer {
public:
    static std::string deterministicFileName(const OfflineRenderConfig& config);

    bool renderToWav(const OfflineRenderConfig& config,
                     const std::string& outputPath,
                     OfflineRenderResult& result);
};

}
