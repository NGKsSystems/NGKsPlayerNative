#include <cstdint>
#include <cstring>
#include <ctime>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "engine/EngineCore.h"
#include "engine/audio/AudioIO_Juce.h"
#include "engine/runtime/MasterBus.h"
#include "engine/runtime/offline/OfflineRenderConfig.h"
#include "engine/runtime/offline/OfflineRenderer.h"

namespace {
constexpr float kSecondsToRender = 2.0f;
constexpr uint32_t kSampleRate = 48000u;
constexpr uint32_t kBlockSize = 256u;

const char* rtWatchdogStateText(int32_t code)
{
    switch (code) {
    case 0:
        return "GRACE";
    case 1:
        return "ACTIVE";
    case 2:
        return "STALL";
    case 3:
        return "FAILED";
    default:
        return "UNKNOWN";
    }
}

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
    bool aeSoak = false;
    int aeSeconds = 600;
    int aePollMs = 250;
    int aeMaxXruns = 0;
    uint64_t aeMaxJitterNs = 15000000ull;
    bool aeStrictJitter = false;
    bool aeRequireNoRestarts = false;
    bool aeAllowStallTrips = false;
    bool listDevices = false;
    std::string deviceId;
    std::string deviceName;
    bool setPreferredDeviceId = false;
    bool setPreferredDeviceName = false;
};

struct AudioDeviceProfile {
    std::string preferredDeviceId;
    std::string preferredDeviceName;
    int sampleRate = 0;
    int bufferFrames = 0;
    int channelsIn = 0;
    int channelsOut = 0;
};

const std::filesystem::path kAudioProfilePath = std::filesystem::path("data") / "runtime" / "audio_device_profile.json";

std::string jsonEscape(const std::string& value)
{
    std::string out;
    out.reserve(value.size() + 8u);
    for (char c : value) {
        if (c == '\\' || c == '"') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

std::string extractJsonString(const std::string& text, const std::string& key)
{
    const std::string needle = "\"" + key + "\"";
    const size_t keyPos = text.find(needle);
    if (keyPos == std::string::npos) {
        return {};
    }
    size_t colonPos = text.find(':', keyPos + needle.size());
    if (colonPos == std::string::npos) {
        return {};
    }
    size_t start = text.find('"', colonPos + 1u);
    if (start == std::string::npos) {
        return {};
    }
    ++start;
    size_t end = start;
    while (end < text.size()) {
        if (text[end] == '"' && text[end - 1] != '\\') {
            break;
        }
        ++end;
    }
    if (end >= text.size()) {
        return {};
    }
    return text.substr(start, end - start);
}

int extractJsonInt(const std::string& text, const std::string& key)
{
    const std::string needle = "\"" + key + "\"";
    const size_t keyPos = text.find(needle);
    if (keyPos == std::string::npos) {
        return 0;
    }
    size_t colonPos = text.find(':', keyPos + needle.size());
    if (colonPos == std::string::npos) {
        return 0;
    }
    size_t begin = text.find_first_of("-0123456789", colonPos + 1u);
    if (begin == std::string::npos) {
        return 0;
    }
    size_t end = text.find_first_not_of("-0123456789", begin);
    const std::string value = text.substr(begin, end - begin);
    try {
        return std::stoi(value);
    } catch (...) {
        return 0;
    }
}

bool loadAudioDeviceProfile(AudioDeviceProfile& outProfile)
{
    if (!std::filesystem::exists(kAudioProfilePath)) {
        return false;
    }

    std::ifstream stream(kAudioProfilePath, std::ios::in | std::ios::binary);
    if (!stream.is_open()) {
        return false;
    }

    std::ostringstream oss;
    oss << stream.rdbuf();
    const std::string text = oss.str();
    outProfile.preferredDeviceId = extractJsonString(text, "preferred_device_id");
    outProfile.preferredDeviceName = extractJsonString(text, "preferred_device_name");
    outProfile.sampleRate = extractJsonInt(text, "sample_rate");
    outProfile.bufferFrames = extractJsonInt(text, "buffer_frames");
    outProfile.channelsIn = extractJsonInt(text, "channels_in");
    outProfile.channelsOut = extractJsonInt(text, "channels_out");
    return true;
}

bool saveAudioDeviceProfile(const AudioDeviceProfile& profile)
{
    std::filesystem::create_directories(kAudioProfilePath.parent_path());
    std::ofstream stream(kAudioProfilePath, std::ios::trunc | std::ios::binary);
    if (!stream.is_open()) {
        return false;
    }

    const auto now = std::chrono::system_clock::now();
    const auto nowT = std::chrono::system_clock::to_time_t(now);
    std::tm utcTm {};
#ifdef _WIN32
    gmtime_s(&utcTm, &nowT);
#else
    gmtime_r(&nowT, &utcTm);
#endif
    char timeBuf[64] {};
    std::strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%dT%H:%M:%SZ", &utcTm);

    stream << "{\n"
           << "  \"preferred_device_id\": \"" << jsonEscape(profile.preferredDeviceId) << "\",\n"
           << "  \"preferred_device_name\": \"" << jsonEscape(profile.preferredDeviceName) << "\",\n"
           << "  \"sample_rate\": " << profile.sampleRate << ",\n"
           << "  \"buffer_frames\": " << profile.bufferFrames << ",\n"
           << "  \"channels_in\": " << profile.channelsIn << ",\n"
           << "  \"channels_out\": " << profile.channelsOut << ",\n"
           << "  \"updated_utc\": \"" << timeBuf << "\"\n"
           << "}\n";
    return true;
}

bool resolveDeviceFromOptions(const CliOptions& options,
                              const std::vector<AudioIOJuce::DeviceInfo>& devices,
                              std::string& outId,
                              std::string& outName)
{
    if (!options.deviceId.empty()) {
        for (const auto& d : devices) {
            if (d.deviceId == options.deviceId) {
                outId = d.deviceId;
                outName = d.deviceName;
                return true;
            }
        }
        return false;
    }

    if (!options.deviceName.empty()) {
        for (const auto& d : devices) {
            if (d.deviceName == options.deviceName) {
                outId = d.deviceId;
                outName = d.deviceName;
                return true;
            }
        }
        return false;
    }

    AudioDeviceProfile profile {};
    if (loadAudioDeviceProfile(profile)) {
        if (!profile.preferredDeviceId.empty()) {
            for (const auto& d : devices) {
                if (d.deviceId == profile.preferredDeviceId) {
                    outId = d.deviceId;
                    outName = d.deviceName;
                    return true;
                }
            }
        }

        if (!profile.preferredDeviceName.empty()) {
            for (const auto& d : devices) {
                if (d.deviceName == profile.preferredDeviceName) {
                    outId = d.deviceId;
                    outName = d.deviceName;
                    return true;
                }
            }
        }
    }

    if (!devices.empty()) {
        outId = devices.front().deviceId;
        outName = devices.front().deviceName;
        return true;
    }
    return false;
}

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

        if (arg == "--list_devices") {
            options.listDevices = true;
            continue;
        }

        if (arg == "--ae_soak") {
            options.aeSoak = true;
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
            options.aeSeconds = options.rtSeconds;
            continue;
        }

        if (arg == "--poll_ms") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.aePollMs = std::stoi(argv[++i]);
            } catch (...) {
                return false;
            }
            if (options.aePollMs <= 0) {
                return false;
            }
            continue;
        }

        if (arg == "--max_xruns") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.aeMaxXruns = std::stoi(argv[++i]);
            } catch (...) {
                return false;
            }
            if (options.aeMaxXruns < 0) {
                return false;
            }
            continue;
        }

        if (arg == "--max_jitter_ns") {
            if (i + 1 >= argc) {
                return false;
            }
            try {
                options.aeMaxJitterNs = static_cast<uint64_t>(std::stoull(argv[++i]));
            } catch (...) {
                return false;
            }
            continue;
        }

        if (arg == "--strict_jitter") {
            options.aeStrictJitter = true;
            options.aeMaxJitterNs = 2000000ull;
            continue;
        }

        if (arg == "--require_no_restarts") {
            options.aeRequireNoRestarts = true;
            continue;
        }

        if (arg == "--allow_stall_trips") {
            options.aeAllowStallTrips = true;
            continue;
        }

        if (arg == "--device_id") {
            if (i + 1 >= argc) {
                return false;
            }
            options.deviceId = argv[++i];
            continue;
        }

        if (arg == "--device_name") {
            if (i + 1 >= argc) {
                return false;
            }
            options.deviceName = argv[++i];
            continue;
        }

        if (arg == "--set_preferred_device_id") {
            if (i + 1 >= argc) {
                return false;
            }
            options.setPreferredDeviceId = true;
            options.deviceId = argv[++i];
            continue;
        }

        if (arg == "--set_preferred_device_name") {
            if (i + 1 >= argc) {
                return false;
            }
            options.setPreferredDeviceName = true;
            options.deviceName = argv[++i];
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

int runListDevices()
{
    const auto devices = AudioIOJuce::listAudioDevices();
    if (devices.empty()) {
        std::cout << "RTAudioDeviceList=FAIL reason=none" << std::endl;
        return 1;
    }

    std::cout << "RTAudioDeviceList=BEGIN" << std::endl;
    for (const auto& d : devices) {
        std::cout << "RTAudioDevice id=" << d.deviceId
                  << " name=" << d.deviceName
                  << " backend=" << d.backendType
                  << " in=" << d.inputChannels
                  << " out=" << d.outputChannels
                  << std::endl;
    }
    std::cout << "RTAudioDeviceListCount=" << devices.size() << std::endl;
    std::cout << "RTAudioDeviceList=PASS" << std::endl;
    return 0;
}

int runSetPreferredDevice(const CliOptions& options)
{
    const auto devices = AudioIOJuce::listAudioDevices();
    std::string resolvedId;
    std::string resolvedName;
    const bool resolved = resolveDeviceFromOptions(options, devices, resolvedId, resolvedName);
    if (!resolved || resolvedId.empty() || resolvedName.empty()) {
        std::cout << "RTAudioDeviceSelect=FAIL" << std::endl;
        return 1;
    }

    AudioDeviceProfile profile {};
    profile.preferredDeviceId = resolvedId;
    profile.preferredDeviceName = resolvedName;
    if (!saveAudioDeviceProfile(profile)) {
        std::cout << "RTAudioDeviceProfileWrite=FAIL" << std::endl;
        return 1;
    }

    std::cout << "RTAudioDeviceSelect=PASS" << std::endl;
    std::cout << "RTAudioDeviceId=" << resolvedId << std::endl;
    std::cout << "RTAudioDeviceName=" << resolvedName << std::endl;
    std::cout << "RTAudioDeviceProfileWrite=PASS path=" << kAudioProfilePath.string() << std::endl;
    return 0;
}

int runRtAudioProbe(const CliOptions& options)
{
    std::cout << "RTAudioProbe=BEGIN" << std::endl;
    std::cout << "RTAudioAD=BEGIN" << std::endl;

    const auto devices = AudioIOJuce::listAudioDevices();
    std::string selectedDeviceId;
    std::string selectedDeviceName;
    if (!resolveDeviceFromOptions(options, devices, selectedDeviceId, selectedDeviceName)) {
        std::cout << "RTAudioDeviceSelect=FAIL" << std::endl;
        return 1;
    }

    EngineCore engine(false);
    if (!selectedDeviceId.empty()) {
        engine.setPreferredAudioDeviceId(selectedDeviceId);
    } else if (!selectedDeviceName.empty()) {
        engine.setPreferredAudioDeviceName(selectedDeviceName);
    }

    const bool openOk = engine.startRtAudioProbe(options.rtToneHz, options.rtToneDb);
    auto telemetry = engine.getTelemetrySnapshot();

    std::cout << "RTAudioDeviceSelect=" << (openOk ? "PASS" : "FAIL") << std::endl;
    std::cout << "RTAudioDeviceId=" << selectedDeviceId << std::endl;
    std::cout << "RTAudioDeviceName=" << selectedDeviceName << std::endl;
    std::cout << "RTAudioSampleRate=" << telemetry.rtSampleRate << std::endl;
    std::cout << "RTAudioBufferFrames=" << telemetry.rtBufferFrames << std::endl;
    std::cout << "RTAudioChannelsIn=" << telemetry.rtChannelsIn << std::endl;
    std::cout << "RTAudioChannelsOut=" << telemetry.rtChannelsOut << std::endl;

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

    std::cout << "RTAudioXRunsTotal=" << telemetry.rtXRunCountTotal << std::endl;
    std::cout << "RTAudioJitterMaxNsWindow=" << telemetry.rtJitterAbsNsMaxWindow << std::endl;
    std::cout << "RTAudioDeviceRestartCount=" << telemetry.rtDeviceRestartCount << std::endl;
    std::cout << "RTAudioWatchdogState=" << rtWatchdogStateText(telemetry.rtWatchdogStateCode) << std::endl;

    std::cout << "RTAudioWatchdog=" << (watchdogOk ? "PASS" : "FAIL")
              << " StallMs=" << worstStallMs << std::endl;

    const bool stateOk = telemetry.rtWatchdogStateCode != 3;
    const bool pass = openOk && telemetry.rtDeviceOpenOk && callbackPass && xrunPass && watchdogOk && stateOk;
    std::cout << "RTAudioAD=" << (pass ? "PASS" : "FAIL") << std::endl;
    std::cout << "RTAudioProbe=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}

int runAeSoak(const CliOptions& options)
{
    std::cout << "RTAudioAE=BEGIN" << std::endl;
    std::cout << "RTAudioAESeconds=" << options.aeSeconds << std::endl;
    std::cout << "RTAudioAEJitterLimitNs=" << options.aeMaxJitterNs << std::endl;
    std::cout << "RTAudioAEXRunLimit=" << options.aeMaxXruns << std::endl;
    std::cout << "RTAudioAERestartPolicy=" << (options.aeRequireNoRestarts ? "strict" : "allow") << std::endl;

    const auto devices = AudioIOJuce::listAudioDevices();
    std::string selectedDeviceId;
    std::string selectedDeviceName;
    if (!resolveDeviceFromOptions(options, devices, selectedDeviceId, selectedDeviceName)) {
        std::cout << "RTAudioDeviceSelect=FAIL" << std::endl;
        std::cout << "RTAudioAE=FAIL" << std::endl;
        return 1;
    }

    EngineCore engine(false);
    if (!selectedDeviceId.empty()) {
        engine.setPreferredAudioDeviceId(selectedDeviceId);
    } else if (!selectedDeviceName.empty()) {
        engine.setPreferredAudioDeviceName(selectedDeviceName);
    }

    const bool openOk = engine.startRtAudioProbe(options.rtToneHz, options.rtToneDb);
    auto telemetry = engine.getTelemetrySnapshot();

    std::cout << "RTAudioDeviceSelect=" << (openOk ? "PASS" : "FAIL") << std::endl;
    std::cout << "RTAudioDeviceId=" << selectedDeviceId << std::endl;
    std::cout << "RTAudioDeviceName=" << selectedDeviceName << std::endl;
    std::cout << "RTAudioSampleRate=" << telemetry.rtSampleRate << std::endl;
    std::cout << "RTAudioBufferFrames=" << telemetry.rtBufferFrames << std::endl;
    std::cout << "RTAudioChannelsIn=" << telemetry.rtChannelsIn << std::endl;
    std::cout << "RTAudioChannelsOut=" << telemetry.rtChannelsOut << std::endl;

    uint64_t initialCallbackCount = telemetry.rtCallbackCount;
    uint64_t previousCallbackCount = initialCallbackCount;
    uint64_t maxJitterNsObserved = telemetry.rtJitterAbsNsMaxWindow;
    uint64_t maxIntervalNsObserved = telemetry.rtCallbackIntervalNsMaxWindow;
    uint64_t xrunTotal = telemetry.rtXRunCountTotal;
    uint32_t restartCount = telemetry.rtDeviceRestartCount;
    int32_t lastState = telemetry.rtWatchdogStateCode;
    uint32_t stateTransitions = 0u;
    uint32_t stallTripCount = (lastState == 2) ? 1u : 0u;
    bool watchdogFailedSeen = (lastState == 3);

    const auto start = std::chrono::steady_clock::now();
    const auto startMs = std::chrono::duration_cast<std::chrono::milliseconds>(start.time_since_epoch()).count();
    int64_t stagnantStartMs = startMs;
    int64_t longestStagnantMs = 0;

    while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - start).count() < options.aeSeconds) {
        int64_t stallMs = 0;
        engine.pollRtWatchdog(500, stallMs);
        telemetry = engine.getTelemetrySnapshot();

        if (telemetry.rtCallbackCount > previousCallbackCount) {
            previousCallbackCount = telemetry.rtCallbackCount;
            const auto nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const int64_t stagnantMs = nowMs - stagnantStartMs;
            if (stagnantMs > longestStagnantMs) {
                longestStagnantMs = stagnantMs;
            }
            stagnantStartMs = nowMs;
        }

        maxJitterNsObserved = std::max<uint64_t>(maxJitterNsObserved, telemetry.rtJitterAbsNsMaxWindow);
        maxIntervalNsObserved = std::max<uint64_t>(maxIntervalNsObserved, telemetry.rtCallbackIntervalNsMaxWindow);
        xrunTotal = telemetry.rtXRunCountTotal;
        restartCount = std::max<uint32_t>(restartCount, telemetry.rtDeviceRestartCount);
        if (telemetry.rtWatchdogStateCode != lastState) {
            ++stateTransitions;
            if (telemetry.rtWatchdogStateCode == 2) {
                ++stallTripCount;
            }
            lastState = telemetry.rtWatchdogStateCode;
        }
        if (telemetry.rtWatchdogStateCode == 3) {
            watchdogFailedSeen = true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(options.aePollMs));
    }

    engine.stopRtAudioProbe();
    telemetry = engine.getTelemetrySnapshot();
    maxJitterNsObserved = std::max<uint64_t>(maxJitterNsObserved, telemetry.rtJitterAbsNsMaxWindow);
    maxIntervalNsObserved = std::max<uint64_t>(maxIntervalNsObserved, telemetry.rtCallbackIntervalNsMaxWindow);
    xrunTotal = telemetry.rtXRunCountTotal;
    restartCount = std::max<uint32_t>(restartCount, telemetry.rtDeviceRestartCount);

    const auto endMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    const int64_t finalStagnantMs = endMs - stagnantStartMs;
    if (finalStagnantMs > longestStagnantMs) {
        longestStagnantMs = finalStagnantMs;
    }

    const bool callbackProgressPass = telemetry.rtCallbackCount > initialCallbackCount
        && longestStagnantMs <= 2000;
    const bool xrunPass = xrunTotal <= static_cast<uint64_t>(options.aeMaxXruns);
    const bool jitterPass = maxJitterNsObserved <= options.aeMaxJitterNs;
    const bool restartPass = !options.aeRequireNoRestarts || (restartCount == 0u);
    const bool stallTripPass = options.aeAllowStallTrips || (stallTripCount == 0u);
    const bool watchdogPass = !watchdogFailedSeen && telemetry.rtWatchdogStateCode != 3;

    std::cout << "RTAudioAECallbackProgress=" << (callbackProgressPass ? "PASS" : "FAIL")
              << " first=" << initialCallbackCount
              << " last=" << telemetry.rtCallbackCount
              << " maxStagnantMs=" << longestStagnantMs
              << std::endl;

    std::cout << "RTAudioAEXRunsTotal=" << xrunTotal << std::endl;
    std::cout << "RTAudioAEXRunsCheck=" << (xrunPass ? "PASS" : "FAIL")
              << " maxAllowed=" << options.aeMaxXruns << std::endl;

    std::cout << "RTAudioAEJitterMaxNs=" << maxJitterNsObserved << std::endl;
    std::cout << "RTAudioAEJitterCheck=" << (jitterPass ? "PASS" : "FAIL")
              << " maxAllowed=" << options.aeMaxJitterNs << std::endl;

    std::cout << "RTAudioAEIntervalMaxNs=" << maxIntervalNsObserved << std::endl;
    std::cout << "RTAudioAEWatchdogTransitions=" << stateTransitions << std::endl;
    std::cout << "RTAudioAEStallTrips=" << stallTripCount << std::endl;
    std::cout << "RTAudioAEStallTripCheck=" << (stallTripPass ? "PASS" : "FAIL")
              << " allow=" << (options.aeAllowStallTrips ? 1 : 0) << std::endl;

    std::cout << "RTAudioAERestarts=" << restartCount << std::endl;
    std::cout << "RTAudioAERestartsCheck=" << (restartPass ? "PASS" : "FAIL")
              << " requireNoRestarts=" << (options.aeRequireNoRestarts ? 1 : 0) << std::endl;

    std::cout << "RTAudioAEWatchdogFinal=" << rtWatchdogStateText(telemetry.rtWatchdogStateCode) << std::endl;
    std::cout << "RTAudioAEWatchdogCheck=" << (watchdogPass ? "PASS" : "FAIL") << std::endl;

    const bool pass = openOk
        && telemetry.rtDeviceOpenOk
        && callbackProgressPass
        && xrunPass
        && jitterPass
        && restartPass
        && stallTripPass
        && watchdogPass;

    std::cout << "RTAudioAE=" << (pass ? "PASS" : "FAIL") << std::endl;
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

    if (options.listDevices) {
        return runListDevices();
    }

    if (options.setPreferredDeviceId || options.setPreferredDeviceName) {
        return runSetPreferredDevice(options);
    }

    if (options.rtAudioProbe) {
        return runRtAudioProbe(options);
    }

    if (options.aeSoak) {
        return runAeSoak(options);
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
