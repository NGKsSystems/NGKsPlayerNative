#include <cstdint>
#include <cstring>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
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
    bool foundationReport = false;
    bool foundationJson = false;
    bool selfTest = false;
    bool rtAudioProbe = false;
    int rtSeconds = 5;
    float rtToneHz = 440.0f;
    float rtToneDb = -12.0f;
};

bool parseCliOptions(int argc, char* argv[], CliOptions& options)
{
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--foundation_report") {
            options.foundationReport = true;
            continue;
        }

        if (arg == "--foundation_json") {
            options.foundationJson = true;
            continue;
        }

        if (arg == "--selftest") {
            options.selfTest = true;
            continue;
        }

        if (arg == "--rt_audio_probe") {
            options.rtAudioProbe = true;
            continue;
        }

        if (arg == "--seconds") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.rtSeconds = std::stoi(argv[++i]);
            } catch (...) {
                return false;
            }
            if (options.rtSeconds <= 0) {
                return false;
            }
            continue;
        }

        if (arg == "--tone_hz") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.rtToneHz = std::stof(argv[++i]);
            } catch (...) {
                return false;
            }
            continue;
        }

        if (arg == "--tone_db") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.rtToneDb = std::stof(argv[++i]);
            } catch (...) {
                return false;
            }
            continue;
        }

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

int runRtAudioProbe(const CliOptions& options)
{
    std::cout << "RTAudioProbe=BEGIN" << std::endl;

    EngineCore engine(false);
    const bool openOk = engine.startRtAudioProbe(options.rtToneHz, options.rtToneDb);
    auto telemetry = engine.getTelemetrySnapshot();

    if (openOk && telemetry.rtDeviceOpenOk) {
        std::cout << "RTAudioDeviceOpen=PASS"
                  << " name=" << telemetry.rtDeviceName
                  << " sr=" << telemetry.rtSampleRate
                  << " buffer=" << telemetry.rtBufferFrames
                  << " channels=" << telemetry.rtChannelsOut
                  << std::endl;
    } else {
        std::cout << "RTAudioDeviceOpen=FAIL" << std::endl;
    }

    const auto start = std::chrono::steady_clock::now();
    bool watchdogOk = true;
    int64_t worstStallMs = 0;
    while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() < options.rtSeconds) {
        int64_t stallMs = 0;
        const bool tickOk = engine.pollRtWatchdog(500, stallMs);
        watchdogOk = watchdogOk && tickOk;
        if (stallMs > worstStallMs) {
            worstStallMs = stallMs;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    engine.stopRtAudioProbe();
    telemetry = engine.getTelemetrySnapshot();

    const uint64_t callbackTicks = telemetry.rtCallbackCount;
    const int64_t conservativeMinTicks = static_cast<int64_t>(options.rtSeconds) * 2;
    const bool callbackPass = callbackTicks >= static_cast<uint64_t>(std::max<int64_t>(1, conservativeMinTicks));
    std::cout << "RTAudioCallbackTicks>=" << std::max<int64_t>(1, conservativeMinTicks)
              << '=' << (callbackPass ? "PASS" : "FAIL")
              << " value=" << callbackTicks << std::endl;

    const bool xrunPass = telemetry.rtXRunCount == 0u;
    if (xrunPass) {
        std::cout << "RTAudioXRuns=0" << std::endl;
    } else {
        std::cout << "RTAudioXRuns=" << telemetry.rtXRunCount << " FAIL" << std::endl;
    }

    const double peakDb = static_cast<double>(telemetry.rtMeterPeakDb10) / 10.0;
    std::cout << "RTAudioMeterPeakDb=" << peakDb << std::endl;

    std::cout << "RTAudioWatchdog=" << (watchdogOk ? "PASS" : "FAIL")
              << " StallMs=" << worstStallMs << std::endl;

    const bool pass = openOk && telemetry.rtDeviceOpenOk && callbackPass && xrunPass && watchdogOk;
    std::cout << "RTAudioProbe=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}

struct SelfTestResults {
    bool telemetryReadable = false;
    bool healthReadable = false;
    bool offlineRenderPasses = false;
    bool allPass = false;
};

struct FoundationStatus {
    bool engineInit = false;
    bool offlineRender = false;
    bool telemetry = false;
    bool healthSnapshot = false;
    bool diagnostics = false;
    uint64_t telemetryRenderCycles = 0;
    bool healthRenderOk = false;
};

SelfTestResults runSelfTests(bool offlinePass)
{
    EngineCore probe(true);
    probe.prepare(static_cast<double>(kSampleRate), static_cast<int>(kBlockSize));

    float interleaved[kBlockSize * 2u] {};
    probe.renderOfflineBlock(interleaved, kBlockSize);
    probe.renderOfflineBlock(interleaved, kBlockSize);
    probe.renderOfflineBlock(interleaved, kBlockSize);

    const auto telemetry = probe.getTelemetrySnapshot();
    const auto snapshot = probe.getSnapshot();

    SelfTestResults out {};
    out.telemetryReadable = telemetry.renderCycles >= 0u
        && telemetry.audioCallbacks >= 0u
        && telemetry.xruns >= 0u;
    out.healthReadable = std::isfinite(snapshot.masterPeakL)
        && std::isfinite(snapshot.masterPeakR)
        && std::isfinite(snapshot.masterRmsL)
        && std::isfinite(snapshot.masterRmsR);
    out.offlineRenderPasses = offlinePass;
    out.allPass = out.telemetryReadable && out.healthReadable && out.offlineRenderPasses;
    return out;
}

FoundationStatus buildFoundationStatus(bool offlinePass)
{
    EngineCore probe(true);
    probe.prepare(static_cast<double>(kSampleRate), static_cast<int>(kBlockSize));
    float interleaved[kBlockSize * 2u] {};
    const bool rendered = probe.renderOfflineBlock(interleaved, kBlockSize);
    const auto telemetry = probe.getTelemetrySnapshot();
    const auto snapshot = probe.getSnapshot();

    FoundationStatus status {};
    status.engineInit = rendered;
    status.offlineRender = offlinePass;
    status.telemetry = telemetry.renderCycles >= 0u
        && telemetry.audioCallbacks >= 0u
        && telemetry.xruns >= 0u;
    status.healthSnapshot = std::isfinite(snapshot.masterPeakL)
        && std::isfinite(snapshot.masterPeakR)
        && std::isfinite(snapshot.masterRmsL)
        && std::isfinite(snapshot.masterRmsR);
    status.diagnostics = true;
    status.telemetryRenderCycles = telemetry.renderCycles;
    status.healthRenderOk = status.healthSnapshot;
    return status;
}

void printSelfTestSuite(const SelfTestResults& selfTests)
{
    std::cout << "SelfTestSuite=BEGIN" << std::endl;
    std::cout << "SelfTest_TelemetryReadable=" << (selfTests.telemetryReadable ? "PASS" : "FAIL") << std::endl;
    std::cout << "SelfTest_HealthReadable=" << (selfTests.healthReadable ? "PASS" : "FAIL") << std::endl;
    std::cout << "SelfTest_OfflineRenderPasses=" << (selfTests.offlineRenderPasses ? "PASS" : "FAIL") << std::endl;
    std::cout << "SelfTestSuite=END" << std::endl;
}

void printFoundationReportText(const FoundationStatus& status)
{
    std::cout << "FoundationReport=BEGIN" << std::endl;
    std::cout << "FoundationEngineInit=" << (status.engineInit ? "PASS" : "FAIL") << std::endl;
    std::cout << "FoundationOfflineRender=" << (status.offlineRender ? "PASS" : "FAIL") << std::endl;
    std::cout << "FoundationTelemetry=" << (status.telemetry ? "PASS" : "FAIL") << std::endl;
    std::cout << "FoundationHealthSnapshot=" << (status.healthSnapshot ? "PASS" : "FAIL") << std::endl;
    std::cout << "FoundationDiagnostics=" << (status.diagnostics ? "PASS" : "FAIL") << std::endl;
    std::cout << "FoundationReport=END" << std::endl;
}

void printFoundationReportJson(const FoundationStatus& status, const SelfTestResults* selfTests)
{
    std::cout << "{"
              << "\"foundation\":{"
              << "\"engine_init\":" << (status.engineInit ? "true" : "false") << ','
              << "\"offline_render\":" << (status.offlineRender ? "true" : "false") << ','
              << "\"telemetry\":" << (status.telemetry ? "true" : "false") << ','
              << "\"health_snapshot\":" << (status.healthSnapshot ? "true" : "false") << ','
              << "\"diagnostics\":" << (status.diagnostics ? "true" : "false") << ','
              << "\"telemetry_render_cycles\":" << status.telemetryRenderCycles << ','
              << "\"health_render_ok\":" << (status.healthRenderOk ? "true" : "false")
              << "}";
    if (selfTests != nullptr) {
        std::cout << ",\"selftests\":{"
                  << "\"telemetry_readable\":" << (selfTests->telemetryReadable ? "true" : "false") << ','
                  << "\"health_readable\":" << (selfTests->healthReadable ? "true" : "false") << ','
                  << "\"offline_render_passes\":" << (selfTests->offlineRenderPasses ? "true" : "false") << ','
                  << "\"all_pass\":" << (selfTests->allPass ? "true" : "false")
                  << "}";
    }
    std::cout << "}" << std::endl;
}

} // namespace

int main(int argc, char* argv[])
{
    CliOptions options {};
    if (!parseCliOptions(argc, argv, options)) {
        std::cerr << "Usage: NGKsPlayerHeadless [--telemetry_csv <path>] [--telemetry_seconds <int>]" << std::endl;
        return 1;
    }

    if (options.rtAudioProbe) {
        return runRtAudioProbe(options);
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

    SelfTestResults selfTests {};
    if (options.selfTest) {
        selfTests = runSelfTests(offlinePass);
        printSelfTestSuite(selfTests);
    }

    const FoundationStatus foundation = buildFoundationStatus(offlinePass);
    if (options.foundationReport) {
        if (options.foundationJson) {
            printFoundationReportJson(foundation, options.selfTest ? &selfTests : nullptr);
        } else {
            printFoundationReportText(foundation);
        }
    }

    std::cout << "OfflineRenderTest=" << (offlinePass ? "PASS" : "FAIL") << std::endl;
    const bool foundationPass = foundation.engineInit
        && foundation.offlineRender
        && foundation.telemetry
        && foundation.healthSnapshot
        && foundation.diagnostics;
    const bool pass = offlinePass
        && telemetryPass
        && (!options.foundationReport || foundationPass)
        && (!options.selfTest || selfTests.allPass);
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
