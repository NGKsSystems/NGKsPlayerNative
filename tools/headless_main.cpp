#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "engine/runtime/MasterBus.h"
#include "engine/runtime/offline/OfflineRenderConfig.h"
#include "engine/runtime/offline/OfflineRenderer.h"

namespace {
constexpr float kSecondsToRender = 2.0f;
constexpr uint32_t kSampleRate = 48000u;
constexpr uint32_t kBlockSize = 256u;

bool readWavDataSize(const std::string& path, uint32_t& outDataBytes)
{
    std::ifstream stream(path, std::ios::binary);
    if (!stream.is_open()) {
        return false;
    }

    stream.seekg(40, std::ios::beg);
    stream.read(reinterpret_cast<char*>(&outDataBytes), sizeof(outDataBytes));
    return stream.good();
}

} // namespace

int main()
{
    std::filesystem::create_directories("_artifacts/exports");

    ngks::OfflineRenderConfig config {};
    config.sampleRate = kSampleRate;
    config.blockSize = kBlockSize;
    config.channels = 2;
    config.secondsToRender = kSecondsToRender;
    config.masterGain = 1.0f;
    config.seed = 123u;

    const std::string outputPath = "_artifacts/exports/offline_render_test.wav";
    ngks::OfflineRenderResult result {};
    ngks::OfflineRenderer renderer;
    const bool rendered = renderer.renderToWav(config, outputPath, result);

    const bool fileExists = std::filesystem::exists(outputPath);
    const uint32_t expectedFrames = static_cast<uint32_t>(kSampleRate * kSecondsToRender);
    uint32_t dataBytes = 0;
    const bool headerRead = fileExists && readWavDataSize(outputPath, dataBytes);
    const bool dataSizeValid = headerRead && dataBytes > 44u;
    const uint32_t actualFrames = headerRead ? (dataBytes / 4u) : 0u;
    const bool frameCountOk = (actualFrames == expectedFrames) && (result.renderedFrames == expectedFrames);
    const bool limiterPeakOk = result.peakAbs <= (ngks::MasterBus::kLimiterThreshold + 0.0001f);

    const bool offlinePass = rendered && result.success && fileExists && dataSizeValid && frameCountOk && limiterPeakOk;

    std::cout << "OfflineRenderTest=" << (offlinePass ? "PASS" : "FAIL") << std::endl;
    const bool pass = offlinePass;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
