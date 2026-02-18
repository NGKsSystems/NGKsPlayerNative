#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "engine/runtime/MasterBus.h"
#include "engine/runtime/offline/OfflineRenderConfig.h"
#include "engine/runtime/offline/OfflineRenderer.h"

namespace {
constexpr float kSecondsToRender = 2.0f;
constexpr uint32_t kSampleRate = 48000u;
constexpr uint32_t kBlockSize = 256u;

struct WavHeaderInfo {
    uint16_t formatCode = 0;
    uint16_t channels = 0;
    uint32_t sampleRate = 0;
    uint16_t blockAlign = 0;
    uint16_t bitsPerSample = 0;
    uint32_t dataBytes = 0;
};

bool readWavHeader(const std::string& path, WavHeaderInfo& outHeader)
{
    std::ifstream stream(path, std::ios::binary);
    if (!stream.is_open()) {
        return false;
    }

    char header[44] {};
    stream.read(header, sizeof(header));
    if (stream.gcount() != static_cast<std::streamsize>(sizeof(header))) {
        return false;
    }

    if (std::memcmp(header + 0, "RIFF", 4) != 0) {
        return false;
    }
    if (std::memcmp(header + 8, "WAVE", 4) != 0) {
        return false;
    }
    if (std::memcmp(header + 12, "fmt ", 4) != 0) {
        return false;
    }
    if (std::memcmp(header + 36, "data", 4) != 0) {
        return false;
    }

    std::memcpy(&outHeader.formatCode, header + 20, sizeof(outHeader.formatCode));
    std::memcpy(&outHeader.channels, header + 22, sizeof(outHeader.channels));
    std::memcpy(&outHeader.sampleRate, header + 24, sizeof(outHeader.sampleRate));
    std::memcpy(&outHeader.blockAlign, header + 32, sizeof(outHeader.blockAlign));
    std::memcpy(&outHeader.bitsPerSample, header + 34, sizeof(outHeader.bitsPerSample));
    std::memcpy(&outHeader.dataBytes, header + 40, sizeof(outHeader.dataBytes));
    return true;
}

bool runFormatCase(ngks::OfflineWavFormat format,
                   ngks::OfflineRenderer& renderer,
                   const std::filesystem::path& outputDir,
                   uint32_t expectedFrames)
{
    ngks::OfflineRenderConfig config {};
    config.sampleRate = kSampleRate;
    config.blockSize = kBlockSize;
    config.channels = 2;
    config.secondsToRender = kSecondsToRender;
    config.masterGain = 1.0f;
    config.seed = 123u;
    config.wavFormat = format;

    const std::string fileName = renderer.deterministicFileName(config);
    const std::filesystem::path outputPath = outputDir / fileName;

    ngks::OfflineRenderResult result {};
    const bool rendered = renderer.renderToWav(config, outputPath.string(), result);
    if (!rendered || !result.success) {
        return false;
    }

    if (!std::filesystem::exists(outputPath)) {
        return false;
    }

    WavHeaderInfo header {};
    if (!readWavHeader(outputPath.string(), header)) {
        return false;
    }

    const uint16_t expectedFormatCode = (format == ngks::OfflineWavFormat::Float32) ? 3u : 1u;
    const uint16_t expectedBits = (format == ngks::OfflineWavFormat::Float32) ? 32u : 16u;
    const uint16_t expectedBlockAlign = static_cast<uint16_t>(config.channels * (expectedBits / 8u));
    const bool headerOk =
        (header.formatCode == expectedFormatCode)
        && (header.channels == static_cast<uint16_t>(config.channels))
        && (header.sampleRate == config.sampleRate)
        && (header.bitsPerSample == expectedBits)
        && (header.blockAlign == expectedBlockAlign)
        && (header.dataBytes > 0u);

    if (!headerOk) {
        return false;
    }

    const uint32_t actualFrames = (header.blockAlign == 0u) ? 0u : (header.dataBytes / static_cast<uint32_t>(header.blockAlign));
    const bool frameCountOk = (actualFrames == expectedFrames) && (result.renderedFrames == expectedFrames);
    if (!frameCountOk) {
        return false;
    }

    const bool resultMetaOk =
        (result.wavFormatCode == expectedFormatCode)
        && (result.bitsPerSample == expectedBits)
        && (result.blockAlign == expectedBlockAlign)
        && (result.sampleRate == config.sampleRate)
        && (result.channels == static_cast<uint16_t>(config.channels));
    if (!resultMetaOk) {
        return false;
    }

    const bool limiterPeakOk = result.peakAbs <= (ngks::MasterBus::kLimiterThreshold + 0.0001f);
    return limiterPeakOk;
}

} // namespace

int main()
{
    const std::filesystem::path outputDir = "_proof/milestone_S/render_out";
    std::filesystem::create_directories(outputDir);

    ngks::OfflineRenderer renderer;
    const uint32_t expectedFrames = static_cast<uint32_t>(kSampleRate * kSecondsToRender);
    const bool pcm16Ok = runFormatCase(ngks::OfflineWavFormat::Pcm16, renderer, outputDir, expectedFrames);
    const bool float32Ok = runFormatCase(ngks::OfflineWavFormat::Float32, renderer, outputDir, expectedFrames);

    const bool offlinePass = pcm16Ok && float32Ok;

    std::cout << "OfflineRenderTest=" << (offlinePass ? "PASS" : "FAIL") << std::endl;
    const bool pass = offlinePass;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
