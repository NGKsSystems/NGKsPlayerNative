#include <cstdint>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "engine/EngineCore.h"
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

struct CliOptions {
    std::string telemetryCsvPath;
    int telemetrySeconds = 3;
};

bool parseCliOptions(int argc, char* argv[], CliOptions& options)
{
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--telemetry_csv") {
            if (i + 1 >= argc) {
                return false;
            }
            options.telemetryCsvPath = argv[++i];
            continue;
        }

        if (arg == "--telemetry_seconds") {
            if (i + 1 >= argc) {
                return false;
            }

            try {
                options.telemetrySeconds = std::stoi(argv[++i]);
            } catch (...) {
                return false;
            }

            if (options.telemetrySeconds <= 0) {
                return false;
            }
            continue;
        }

        return false;
    }

    return true;
}

int runTelemetryCsvMode(const CliOptions& options)
{
    const std::filesystem::path csvPath(options.telemetryCsvPath);
    if (csvPath.empty()) {
        std::cerr << "TelemetryCsvMode=FAIL reason=missing_path" << std::endl;
        return 1;
    }

    if (csvPath.has_parent_path()) {
        std::filesystem::create_directories(csvPath.parent_path());
    }

    std::ofstream csv(csvPath, std::ios::trunc);
    if (!csv.is_open()) {
        std::cerr << "TelemetryCsvMode=FAIL reason=open_failed path=" << csvPath.string() << std::endl;
        return 1;
    }

    csv << "elapsed_ms,render_cycles,audio_callbacks,xruns,last_render_us,max_render_us,last_callback_us,max_callback_us,window_count,window_last_us\n";

    EngineCore telemetryProbe(true);
    telemetryProbe.prepare(static_cast<double>(kSampleRate), static_cast<int>(kBlockSize));
    std::vector<float> interleaved(static_cast<size_t>(kBlockSize) * 2u, 0.0f);

    const int ticks = options.telemetrySeconds * 4;
    const double callbackMs = (1000.0 * static_cast<double>(kBlockSize)) / static_cast<double>(kSampleRate);
    int callbacksPerTick = static_cast<int>(250.0 / callbackMs + 0.5);
    if (callbacksPerTick < 1) {
        callbacksPerTick = 1;
    }

    for (int tick = 0; tick <= ticks; ++tick) {
        for (int cb = 0; cb < callbacksPerTick; ++cb) {
            telemetryProbe.renderOfflineBlock(interleaved.data(), kBlockSize);
        }

        const auto telemetry = telemetryProbe.getTelemetrySnapshot();
        const uint32_t count = telemetry.renderDurationWindowCount;
        const uint32_t lastWindowUs = (count > 0u) ? telemetry.renderDurationWindowUs[count - 1u] : 0u;

        csv << (tick * 250)
            << ',' << telemetry.renderCycles
            << ',' << telemetry.audioCallbacks
            << ',' << telemetry.xruns
            << ',' << telemetry.lastRenderDurationUs
            << ',' << telemetry.maxRenderDurationUs
            << ',' << telemetry.lastCallbackDurationUs
            << ',' << telemetry.maxCallbackDurationUs
            << ',' << telemetry.renderDurationWindowCount
            << ',' << lastWindowUs
            << '\n';
    }

    csv.flush();

    std::cout << "TelemetryCsvMode=PASS" << std::endl;
    std::cout << "TelemetryCsvPath=" << csvPath.string() << std::endl;
    std::cout << "TelemetryCsvRows=" << (ticks + 1) << std::endl;
    std::cout << "RunResult=PASS" << std::endl;
    return 0;
}

} // namespace

int main(int argc, char* argv[])
{
    CliOptions options {};
    if (!parseCliOptions(argc, argv, options)) {
        std::cerr << "Usage: NGKsPlayerHeadless [--telemetry_csv <path>] [--telemetry_seconds <int>]" << std::endl;
        return 1;
    }

    if (!options.telemetryCsvPath.empty()) {
        return runTelemetryCsvMode(options);
    }

    const std::filesystem::path outputDir = "_proof/milestone_S/render_out";
    std::filesystem::create_directories(outputDir);

    ngks::OfflineRenderer renderer;
    const uint32_t expectedFrames = static_cast<uint32_t>(kSampleRate * kSecondsToRender);
    const bool pcm16Ok = runFormatCase(ngks::OfflineWavFormat::Pcm16, renderer, outputDir, expectedFrames);
    const bool float32Ok = runFormatCase(ngks::OfflineWavFormat::Float32, renderer, outputDir, expectedFrames);

    const bool offlinePass = pcm16Ok && float32Ok;

    EngineCore telemetryProbe(true);
    telemetryProbe.prepare(static_cast<double>(kSampleRate), static_cast<int>(kBlockSize));
    float telemetryInterleaved[kBlockSize * 2u] {};
    telemetryProbe.renderOfflineBlock(telemetryInterleaved, kBlockSize);
    telemetryProbe.renderOfflineBlock(telemetryInterleaved, kBlockSize);
    telemetryProbe.renderOfflineBlock(telemetryInterleaved, kBlockSize);
    const auto telemetry = telemetryProbe.getTelemetrySnapshot();
    const bool telemetryPass = telemetry.renderCycles >= 3u;
    std::cout << "TelemetryRenderCycles>=3=" << (telemetryPass ? "PASS" : "FAIL")
              << " value=" << telemetry.renderCycles << std::endl;

    std::cout << "OfflineRenderTest=" << (offlinePass ? "PASS" : "FAIL") << std::endl;
    const bool pass = offlinePass && telemetryPass;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
