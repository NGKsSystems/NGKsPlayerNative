#pragma once

#include <cstdint>
#include <fstream>
#include <string>

namespace ngks {

class WavWriter {
public:
    bool open(const std::string& path, uint32_t sampleRate, uint16_t channels);
    bool writeInterleaved(const float* interleaved, uint32_t frames);
    bool finalize();

private:
    std::ofstream stream_;
    uint32_t dataBytesWritten_ = 0;
    uint16_t channels_ = 2;
    uint32_t sampleRate_ = 48000;
};

}
