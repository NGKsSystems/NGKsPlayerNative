#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "engine/runtime/offline/OfflineRenderConfig.h"

namespace ngks {

class WavWriter {
public:
    bool open(const std::string& path, uint32_t sampleRate, uint16_t channels, OfflineWavFormat format);
    bool writeInterleaved(const float* interleaved, uint32_t frames);
    bool finalize();

    uint16_t formatCode() const noexcept;
    uint16_t bitsPerSample() const noexcept;
    uint16_t blockAlign() const noexcept;

private:
    std::ofstream stream_;
    uint32_t dataBytesWritten_ = 0;
    uint16_t channels_ = 2;
    uint32_t sampleRate_ = 48000;
    OfflineWavFormat format_ = OfflineWavFormat::Pcm16;
    uint16_t bitsPerSample_ = 16;
};

}
