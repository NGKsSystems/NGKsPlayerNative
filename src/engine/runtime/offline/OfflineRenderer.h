#pragma once

#include <cstdint>
#include <string>

#include "engine/runtime/offline/OfflineRenderConfig.h"

namespace ngks {

struct OfflineRenderResult {
    bool success{false};
    uint32_t renderedFrames{0};
    float peakAbs{0.0f};
};

class OfflineRenderer {
public:
    bool renderToWav(const OfflineRenderConfig& config,
                     const std::string& outputPath,
                     OfflineRenderResult& result);
};

}
