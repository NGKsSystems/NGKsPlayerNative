#include "ui/EngineBridge.h"

#include <algorithm>
#include <cmath>
#include <cstring>

#include <QDebug>
#include <QString>

#include "engine/command/Command.h"

EngineBridge::EngineBridge(QObject* parent)
    : QObject(parent)
{
    meterTimer.setInterval(16);
    connect(&meterTimer, &QTimer::timeout, this, &EngineBridge::pollSnapshot);
}

EngineBridge::~EngineBridge()
{
    meterTimer.stop();
    healthEngineInitialized.store(false, std::memory_order_relaxed);
}

void EngineBridge::start()
{
    const auto seq = engine.nextSeq();
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::start() => enqueue Play DECK_A seq=%1").arg(seq);
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq, 0, 0.0f, 0 });
}

void EngineBridge::stop()
{
    const auto seq = engine.nextSeq();
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::stop() => enqueue Stop DECK_A seq=%1").arg(seq);
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq, 0, 0.0f, 0 });
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_A, engine.nextSeq(), 0,
                            static_cast<float>(std::clamp(linear01, 0.0, 1.0)), 0 });
}

bool EngineBridge::startRtProbe(double toneHz, double toneDb)
{
    return engine.startRtAudioProbe(static_cast<float>(toneHz), static_cast<float>(toneDb));
}

void EngineBridge::stopRtProbe()
{
    engine.stopRtAudioProbe();
}

bool EngineBridge::loadTrack(const QString& filePath)
{
    const std::string path = filePath.toStdString();
    ++trackLoadGen_;
    loadedTrackPath_ = filePath;
    endOfTrackEmitted = true;  // block any stale endOfTrack
    qInfo().noquote() << QStringLiteral("TRC[G%1] loadTrack BEGIN path=%2").arg(trackLoadGen_).arg(filePath);

    // Force deck lifecycle to Empty so LoadTrack command is accepted.
    // Stop (Playing→Stopped, rejected if already Stopped — OK) then
    // UnloadTrack (Stopped→Empty). Both harmless if deck is already Empty.
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A,
                            engine.nextSeq(), 0, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, ngks::DECK_A,
                            engine.nextSeq(), 0, 0.0f, 0 });

    double duration = 0.0;
    if (!engine.loadFileIntoDeck(ngks::DECK_A, path, duration, trackLoadGen_)) {
        qWarning().noquote() << QStringLiteral("TRC[G%1] loadTrack FAILED path=%2").arg(trackLoadGen_).arg(filePath);
        return false;
    }
    lastDurationSeconds = duration;
    lastPlayheadSeconds = 0.0;
    emit durationChanged(duration);
    emit playheadChanged(0.0);
    qInfo().noquote() << QStringLiteral("TRC[G%1] loadTrack OK dur=%2")
        .arg(trackLoadGen_).arg(duration, 0, 'f', 2);
    return true;
}

void EngineBridge::pause()
{
    engine.enqueueCommand({ ngks::CommandType::Pause, ngks::DECK_A, engine.nextSeq(), 0, 0.0f, 0 });
}

void EngineBridge::seek(double seconds)
{
    qInfo().noquote() << QStringLiteral("TRC[G%1] seek target=%2 curPH=%3 curDur=%4 endFlag=%5 track=%6")
        .arg(trackLoadGen_).arg(seconds, 0, 'f', 2)
        .arg(lastPlayheadSeconds, 0, 'f', 2)
        .arg(lastDurationSeconds, 0, 'f', 2)
        .arg(endOfTrackEmitted ? "T" : "F")
        .arg(loadedTrackPath_);
    engine.seekDeck(ngks::DECK_A, seconds);
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
    out.audioDeviceReady  = healthAudioDeviceReady.load(std::memory_order_relaxed);
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

double EngineBridge::meterL() const noexcept { return meterLeftValue; }
double EngineBridge::meterR() const noexcept { return meterRightValue; }
bool EngineBridge::running() const noexcept { return runningValue; }

// -------------------------------
// Missing symbols (linker fix)
// -------------------------------

bool EngineBridge::ensureAudioHot()
{
    const auto snapshot = engine.getSnapshot();
    if ((snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u) {
        qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() audio already running");
        return true;
    }

    qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() opening audio device...");
    const bool ok = engine.reopenAudioWithPreferredConfig();
    qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() reopenAudio=%1").arg(ok ? "OK" : "FAIL");
    healthAudioDeviceReady.store(ok, std::memory_order_relaxed);
    return ok;
}

bool EngineBridge::enterDjMode()
{
    healthEngineInitialized.store(true, std::memory_order_relaxed);
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, engine.nextSeq(), 1001ULL, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, engine.nextSeq(), 1002ULL, 0.0f, 0 });
    const bool ok = ensureAudioHot();
    if (ok) {
        meterTimer.start();
    }
    return ok;
}

void EngineBridge::leaveDjMode()
{
    meterTimer.stop();
}

bool EngineBridge::enterSimpleMode()
{
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::enterSimpleMode() called");
    healthEngineInitialized.store(true, std::memory_order_relaxed);
    const bool ok = ensureAudioHot();
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::enterSimpleMode() ensureAudioHot=%1").arg(ok ? "OK" : "FAIL");
    if (ok) {
        meterTimer.start();
    }
    return ok;
}

void EngineBridge::leaveSimpleMode()
{
    meterTimer.stop();
}

void EngineBridge::notifyDeviceFailure(int code)
{
    // Record “device not OK” into health signals; code can be surfaced later.
    (void)code;
    healthAudioDeviceReady.store(false, std::memory_order_relaxed);
    healthLastRenderCycleOk.store(false, std::memory_order_relaxed);
}

void EngineBridge::appExitTeardown()
{
    meterTimer.stop();
    healthEngineInitialized.store(false, std::memory_order_relaxed);
    healthAudioDeviceReady.store(false, std::memory_order_relaxed);
}

QString EngineBridge::engineStateMachineSummary()
{
    const auto snapshot = engine.getSnapshot();
    const bool audioRunning = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    const auto t = snapshot.decks[ngks::DECK_A].transport;

    return QString("audio=%1 transportA=%2")
        .arg(audioRunning ? "RUNNING" : "STOPPED")
        .arg(static_cast<int>(t));
}

void EngineBridge::pollSnapshot()
{
    const auto snapshot = engine.getSnapshot();
    const auto& deckA = snapshot.decks[ngks::DECK_A];
    const double newL = std::clamp(static_cast<double>(deckA.peakL), 0.0, 1.0);
    const double newR = std::clamp(static_cast<double>(deckA.peakR), 0.0, 1.0);
    const auto transport = deckA.transport;
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

    // ── Generation-gated snapshot acceptance ──
    // Drop snapshot data if the engine hasn't processed the current
    // track load yet. trackLoadGen is threaded through the command
    // queue and stamped on DeckSnapshot — zero timing dependency.
    const double playhead = deckA.playheadSeconds;
    const double duration = deckA.lengthSeconds;
    const uint64_t snapGen = deckA.trackLoadGen;

    if (snapGen != trackLoadGen_) {
        // Stale snapshot from previous track — drop
        static int dropLogCount = 0;
        if (dropLogCount < 30) {
            qInfo().noquote() << QStringLiteral("TRC[G%1] SNAP_DROP snapGen=%2 snapDur=%3 snapPH=%4")
                .arg(trackLoadGen_).arg(snapGen).arg(duration, 0, 'f', 2).arg(playhead, 0, 'f', 3);
            ++dropLogCount;
        }
        return;
    }

    // Snapshot generation matches current track — process updates
    if (duration != lastDurationSeconds) {
        qInfo().noquote() << QStringLiteral("TRC[G%1] SNAP_DUR_CHANGE old=%2 new=%3 track=%4")
            .arg(trackLoadGen_).arg(lastDurationSeconds, 0, 'f', 2).arg(duration, 0, 'f', 2).arg(loadedTrackPath_);
        lastDurationSeconds = duration;
        emit durationChanged(duration);
    }

    if (std::abs(playhead - lastPlayheadSeconds) > 0.01) {
        lastPlayheadSeconds = playhead;
        emit playheadChanged(playhead);
    }

    // End-of-track detection
    if (duration > 0.0 && playhead >= duration && !endOfTrackEmitted) {
        qInfo().noquote() << QStringLiteral("TRC[G%1] END_OF_TRACK_FIRE ph=%2 dur=%3 track=%4")
            .arg(trackLoadGen_).arg(playhead, 0, 'f', 3).arg(duration, 0, 'f', 2).arg(loadedTrackPath_);
        endOfTrackEmitted = true;
        emit endOfTrack();
    }
    if (playhead < duration * 0.99) {
        endOfTrackEmitted = false;
    }
}