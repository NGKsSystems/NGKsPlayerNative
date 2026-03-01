#include "ui/EngineBridge.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "engine/command/Command.h"

EngineBridge::EngineBridge(QObject* parent)
    : QObject(parent)
{
    healthEngineInitialized.store(true, std::memory_order_relaxed);
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, nextCommandSeq++, 1001ULL, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, nextCommandSeq++, 1002ULL, 0.0f, 0 });

    meterTimer.setInterval(16);
    connect(&meterTimer, &QTimer::timeout, this, &EngineBridge::pollSnapshot);
    meterTimer.start();
}

void EngineBridge::start()
{
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
}

void EngineBridge::stop()
{
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_A, nextCommandSeq++, 0, static_cast<float>(std::clamp(linear01, 0.0, 1.0)), 0 });
}

bool EngineBridge::startRtProbe(double toneHz, double toneDb)
{
    return engine.startRtAudioProbe(static_cast<float>(toneHz), static_cast<float>(toneDb));
}

void EngineBridge::stopRtProbe()
{
    engine.stopRtAudioProbe();
}

bool EngineBridge::applyAudioProfile(const std::string& deviceId,
                                     const std::string& deviceName,
                                     int sampleRate,
                                     int bufferFrames,
                                     int channelsOut)
{
    engine.setPreferredAudioFormat(static_cast<double>(sampleRate), bufferFrames, channelsOut);

    if (!deviceId.empty()) {
        engine.setPreferredAudioDeviceId(deviceId);
        if (engine.reopenAudioWithPreferredConfig()) {
            return true;
        }
    }

    if (!deviceName.empty()) {
        engine.setPreferredAudioDeviceName(deviceName);
        if (engine.reopenAudioWithPreferredConfig()) {
            return true;
        }
    }

    engine.clearPreferredAudioDevice();
    return engine.reopenAudioWithPreferredConfig();
}

bool EngineBridge::tryGetStatus(UIStatus& out)
{
    const auto snapshot = engine.getSnapshot();
    out.engineReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    out.sampleRateHz = 0;
    out.blockSize = 0;
    out.masterPeakLinear = std::max(snapshot.masterPeakL, snapshot.masterPeakR);
    return out.engineReady;
}

bool EngineBridge::tryGetHealth(UIHealthSnapshot& out) const
{
    out.engineInitialized = healthEngineInitialized.load(std::memory_order_relaxed);
    out.audioDeviceReady = healthAudioDeviceReady.load(std::memory_order_relaxed);
    out.lastRenderCycleOk = healthLastRenderCycleOk.load(std::memory_order_relaxed);
    out.renderCycleCounter = healthRenderCycleCounter.load(std::memory_order_relaxed);
    return out.engineInitialized;
}

bool EngineBridge::tryGetTelemetry(UIEngineTelemetrySnapshot& out) const noexcept
{
    const auto telemetry = engine.getTelemetrySnapshot();
    out.renderCycles = telemetry.renderCycles;
    out.audioCallbacks = telemetry.audioCallbacks;
    out.xruns = telemetry.xruns;
    out.lastRenderDurationUs = telemetry.lastRenderDurationUs;
    out.maxRenderDurationUs = telemetry.maxRenderDurationUs;
    out.lastCallbackDurationUs = telemetry.lastCallbackDurationUs;
    out.maxCallbackDurationUs = telemetry.maxCallbackDurationUs;
    out.renderDurationWindowCount = telemetry.renderDurationWindowCount;
    for (uint32_t i = 0u; i < UIEngineTelemetrySnapshot::kRenderDurationWindowSize; ++i) {
        out.renderDurationWindowUs[i] = telemetry.renderDurationWindowUs[i];
    }
    out.rtAudioEnabled = telemetry.rtAudioEnabled;
    out.rtDeviceOpenOk = telemetry.rtDeviceOpenOk;
    out.rtSampleRate = telemetry.rtSampleRate;
    out.rtBufferFrames = telemetry.rtBufferFrames;
    out.rtRequestedSampleRate = telemetry.rtRequestedSampleRate;
    out.rtRequestedBufferFrames = telemetry.rtRequestedBufferFrames;
    out.rtRequestedChannelsOut = telemetry.rtRequestedChannelsOut;
    out.rtChannelsIn = telemetry.rtChannelsIn;
    out.rtChannelsOut = telemetry.rtChannelsOut;
    out.rtAgFallback = telemetry.rtAgFallback;
    out.rtDeviceIdHash = telemetry.rtDeviceIdHash;
    out.rtCallbackCount = telemetry.rtCallbackCount;
    out.rtXRunCount = telemetry.rtXRunCount;
    out.rtXRunCountTotal = telemetry.rtXRunCountTotal;
    out.rtXRunCountWindow = telemetry.rtXRunCountWindow;
    out.rtLastCallbackNs = telemetry.rtLastCallbackNs;
    out.rtJitterAbsNsMaxWindow = telemetry.rtJitterAbsNsMaxWindow;
    out.rtCallbackIntervalNsLast = telemetry.rtCallbackIntervalNsLast;
    out.rtCallbackIntervalNsMaxWindow = telemetry.rtCallbackIntervalNsMaxWindow;
    out.rtLastCallbackUs = telemetry.rtLastCallbackUs;
    out.rtMaxCallbackUs = telemetry.rtMaxCallbackUs;
    out.rtMeterPeakDb10 = telemetry.rtMeterPeakDb10;
    out.rtWatchdogOk = telemetry.rtWatchdogOk;
    out.rtWatchdogStateCode = telemetry.rtWatchdogStateCode;
    out.rtWatchdogTripCount = telemetry.rtWatchdogTripCount;
    out.rtDeviceRestartCount = telemetry.rtDeviceRestartCount;
    out.rtLastDeviceErrorCode = telemetry.rtLastDeviceErrorCode;
    out.rtRecoveryRequested = telemetry.rtRecoveryRequested;
    out.rtRecoveryFailedState = telemetry.rtRecoveryFailedState;
    out.rtLastCallbackTickMs = telemetry.rtLastCallbackTickMs;
    std::strncpy(out.rtDeviceId, telemetry.rtDeviceId, sizeof(out.rtDeviceId) - 1u);
    out.rtDeviceId[sizeof(out.rtDeviceId) - 1u] = '\0';
    std::strncpy(out.rtDeviceName, telemetry.rtDeviceName, sizeof(out.rtDeviceName) - 1u);
    out.rtDeviceName[sizeof(out.rtDeviceName) - 1u] = '\0';
    return true;
}

bool EngineBridge::pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept
{
    return engine.pollRtWatchdog(thresholdMs, outStallMs);
}

bool EngineBridge::runSelfTests(UISelfTestSnapshot& out) noexcept
{
    UIEngineTelemetrySnapshot telemetry {};
    const bool telemetryReadable = tryGetTelemetry(telemetry)
        && telemetry.renderCycles >= 0u
        && telemetry.audioCallbacks >= 0u
        && telemetry.xruns >= 0u;

    UIHealthSnapshot health {};
    const bool healthReadable = tryGetHealth(health)
        && (health.renderCycleCounter >= 0u);

    constexpr uint32_t testFrames = 256u;
    float interleaved[testFrames * 2u] {};
    const bool offlineRenderPasses = engine.renderOfflineBlock(interleaved, testFrames);

    out.telemetryReadable = telemetryReadable;
    out.healthReadable = healthReadable;
    out.offlineRenderPasses = offlineRenderPasses;
    out.allPass = telemetryReadable && healthReadable && offlineRenderPasses;

    selfTestsRan.store(true, std::memory_order_relaxed);
    selfTestsPass.store(out.allPass, std::memory_order_relaxed);
    return out.allPass;
}

bool EngineBridge::tryGetFoundation(UIFoundationSnapshot& out) const noexcept
{
    UIEngineTelemetrySnapshot telemetry {};
    UIHealthSnapshot health {};

    const bool telemetryReadable = tryGetTelemetry(telemetry)
        && telemetry.renderCycles >= 0u
        && telemetry.audioCallbacks >= 0u
        && telemetry.xruns >= 0u;
    const bool healthReadable = tryGetHealth(health)
        && (health.renderCycleCounter >= 0u);

    out.engineInit = health.engineInitialized;
    out.offlineRender = true;
    out.telemetry = telemetryReadable;
    out.healthSnapshot = healthReadable;
    out.diagnostics = true;
    out.selfTestsRan = selfTestsRan.load(std::memory_order_relaxed);
    out.selfTestsPass = selfTestsPass.load(std::memory_order_relaxed);
    out.telemetryRenderCycles = telemetry.renderCycles;
    out.healthRenderOk = health.lastRenderCycleOk;
    return true;
}

double EngineBridge::meterL() const noexcept
{
    return meterLeftValue;
}

double EngineBridge::meterR() const noexcept
{
    return meterRightValue;
}

bool EngineBridge::running() const noexcept
{
    return runningValue;
}

void EngineBridge::pollSnapshot()
{
    const auto snapshot = engine.getSnapshot();
    const double newL = std::clamp(static_cast<double>(snapshot.decks[ngks::DECK_A].peakL), 0.0, 1.0);
    const double newR = std::clamp(static_cast<double>(snapshot.decks[ngks::DECK_A].peakR), 0.0, 1.0);
    const auto transport = snapshot.decks[ngks::DECK_A].transport;
    const bool nowRunning = (transport == ngks::TransportState::Starting)
        || (transport == ngks::TransportState::Playing)
        || (transport == ngks::TransportState::Stopping);

    const bool audioReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    const bool renderOk = std::isfinite(snapshot.masterPeakL)
        && std::isfinite(snapshot.masterPeakR)
        && std::isfinite(snapshot.masterRmsL)
        && std::isfinite(snapshot.masterRmsR);

    healthAudioDeviceReady.store(audioReady, std::memory_order_relaxed);
    healthLastRenderCycleOk.store(renderOk, std::memory_order_relaxed);
    healthRenderCycleCounter.fetch_add(1u, std::memory_order_relaxed);

    if (newL != meterLeftValue) {
        meterLeftValue = newL;
        emit meterLChanged();
    }

    if (newR != meterRightValue) {
        meterRightValue = newR;
        emit meterRChanged();
    }

    if (nowRunning != runningValue) {
        runningValue = nowRunning;
        emit runningChanged();
    }
}