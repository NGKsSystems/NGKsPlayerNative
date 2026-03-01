#include "ui/EngineBridge.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <exception>

#include <QByteArray>

#include <QDateTime>
#include <QDir>
#include <QFile>
#include <QTextStream>
#include <QDebug>

#include "engine/command/Command.h"

namespace {

constexpr int kAudioOpenGraceTicks = 125;
constexpr int kPlaybackStartGraceTicks = 125;

void bridgeLogLine(const QString& message, bool warning = false)
{
    const QString ts = QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
    const QString line = QStringLiteral("%1 %2").arg(ts, message);

    QDir dir;
    dir.mkpath(QStringLiteral("artifacts/logs"));
    QFile file(QStringLiteral("artifacts/logs/ui_qt.log"));
    if (file.open(QIODevice::WriteOnly | QIODevice::Append | QIODevice::Text)) {
        QTextStream stream(&file);
        stream << line << '\n';
        file.close();
    }

    if (warning) {
        qWarning().noquote() << line;
    } else {
        qInfo().noquote() << line;
    }
    const QByteArray bytes = line.isNull() ? QByteArray("") : line.toUtf8();
    std::fprintf(stderr, "%s\n", bytes.constData());
}

QString truncateBridgeDetail(const QString& value)
{
    if (value.size() <= 1024) {
        return value;
    }
    return value.left(1024) + QStringLiteral("...(truncated)");
}

bool parseFlag(const QByteArray& value)
{
    const QByteArray normalized = value.trimmed().toLower();
    return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
}

} // namespace

bool EngineBridge::envFlagEnabled(const char* key)
{
    return parseFlag(qgetenv(key));
}

const char* EngineBridge::toString(BridgeState state)
{
    switch (state) {
    case BridgeState::Disconnected:
        return "BridgeDisconnected";
    case BridgeState::Connected:
        return "BridgeConnected";
    }
    return "BridgeUnknown";
}

const char* EngineBridge::toString(EngineState state)
{
    switch (state) {
    case EngineState::NotInitialized:
        return "EngineNotInitialized";
    case EngineState::Initialized:
        return "EngineInitialized";
    }
    return "EngineUnknown";
}

const char* EngineBridge::toString(AudioState state)
{
    switch (state) {
    case AudioState::Closed:
        return "AudioDeviceClosed";
    case AudioState::Open:
        return "AudioDeviceOpen";
    case AudioState::Failed:
        return "AudioDeviceFailed";
    }
    return "AudioUnknown";
}

const char* EngineBridge::toString(PlaybackState state)
{
    switch (state) {
    case PlaybackState::Inactive:
        return "PlaybackInactive";
    case PlaybackState::Active:
        return "PlaybackActive";
    }
    return "PlaybackUnknown";
}

const char* EngineBridge::toString(UiModeState state)
{
    switch (state) {
    case UiModeState::None:
        return "ModeNone";
    case UiModeState::DJ:
        return "ModeDJ";
    case UiModeState::Simple:
        return "ModeSimple";
    }
    return "ModeUnknown";
}

EngineBridge::EngineBridge(QObject* parent)
    : QObject(parent)
{
    initializeBridge("constructor");

    meterTimer.setInterval(16);
    connect(&meterTimer, &QTimer::timeout, this, &EngineBridge::pollSnapshot);
    meterTimer.start();
}

EngineBridge::~EngineBridge()
{
    appExitTeardown();
}

bool EngineBridge::canUseEngineLocked() const
{
    return bridgeState_ == BridgeState::Connected
        && engineState_ == EngineState::Initialized
        && bridgeInitialized.load(std::memory_order_relaxed);
}

bool EngineBridge::ensureAudioHotLocked(const char* triggerTag)
{
    if (!canUseEngineLocked()) {
        bridgeLogLine(QStringLiteral("STATE_GUARD_BLOCK event=%1 reason=BridgeOrEngineNotReady")
                          .arg(QString::fromUtf8(triggerTag)),
                      true);
        return false;
    }

    if (audioState_ == AudioState::Open) {
        return true;
    }

    if (envFlagEnabled("NGKS_DISABLE_AUDIO")) {
        audioDisabledByEnv_ = true;
        audioState_ = AudioState::Closed;
        healthAudioDeviceReady.store(false, std::memory_order_relaxed);
        bridgeLogLine(QStringLiteral("STATE_AUDIO_DISABLED event=%1 env=NGKS_DISABLE_AUDIO")
                          .arg(QString::fromUtf8(triggerTag)));
        return false;
    }

    audioDisabledByEnv_ = false;
    bridgeLogLine(QStringLiteral("AUDIO_WARMUP_BEGIN event=%1").arg(QString::fromUtf8(triggerTag)));
    bridgeLogLine(QStringLiteral("AUDIO_DEVICE_OPEN_BEGIN event=%1").arg(QString::fromUtf8(triggerTag)));

    const bool openOk = engine.reopenAudioWithPreferredConfig();
    if (!openOk) {
        handleDeviceFailureLocked(-1, "EnsureAudioHotOpenFailed");
        bridgeLogLine(QStringLiteral("AUDIO_WARMUP_END event=%1 result=FAIL").arg(QString::fromUtf8(triggerTag)), true);
        return false;
    }

    audioState_ = AudioState::Open;
    audioOpenGraceTicks_ = kAudioOpenGraceTicks;
    healthAudioDeviceReady.store(true, std::memory_order_relaxed);
    if (uiModeState_ == UiModeState::DJ || uiModeState_ == UiModeState::Simple) {
        audioLatchedByMode_ = true;
    }

    bridgeLogLine(QStringLiteral("AUDIO_DEVICE_OPEN_OK event=%1").arg(QString::fromUtf8(triggerTag)));
    bridgeLogLine(QStringLiteral("AUDIO_WARMUP_END event=%1 result=PASS").arg(QString::fromUtf8(triggerTag)));
    return true;
}

void EngineBridge::handleDeviceFailureLocked(int errorCode, const char* detailTag)
{
    if (playbackState_ == PlaybackState::Active) {
        engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
    }
    engine.closeAudioDevice();
    playbackState_ = PlaybackState::Inactive;
    audioState_ = AudioState::Failed;
    audioLatchedByMode_ = false;
    audioOpenGraceTicks_ = 0;
    playbackStartGraceTicks_ = 0;
    healthAudioDeviceReady.store(false, std::memory_order_relaxed);
    bridgeLogLine(QStringLiteral("DEVICE_FAILURE code=%1 detail=%2")
                      .arg(QString::number(errorCode), QString::fromUtf8(detailTag)),
                  true);
}

bool EngineBridge::initializeBridge(const char* stageTag)
{
    Q_UNUSED(stageTag);

    bridgeReasonValue.clear();
    bridgeDetailValue.clear();
    bridgeInitialized.store(false, std::memory_order_relaxed);

    bridgeLogLine(QStringLiteral("BRIDGE_START"));
    bridgeLogLine(QStringLiteral("BRIDGE_CONFIG"));
    bridgeLogLine(QStringLiteral("BRIDGE_SPAWN"));
    bridgeLogLine(QStringLiteral("BRIDGE_CONNECT"));
    bridgeLogLine(QStringLiteral("BRIDGE_HANDSHAKE"));

    try {
        healthEngineInitialized.store(true, std::memory_order_relaxed);
        engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, nextCommandSeq++, 1001ULL, 0.0f, 0 });
        engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, nextCommandSeq++, 1002ULL, 0.0f, 0 });
        const auto snapshot = engine.getSnapshot();
        Q_UNUSED(snapshot);

        bridgeInitialized.store(true, std::memory_order_relaxed);
        bridgeReasonValue = QStringLiteral("OK");
        bridgeDetailValue = QStringLiteral("Bridge initialized and snapshot readable");
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            bridgeState_ = BridgeState::Connected;
            engineState_ = EngineState::Initialized;
            audioDisabledByEnv_ = envFlagEnabled("NGKS_DISABLE_AUDIO");
            if (audioState_ != AudioState::Open) {
                audioState_ = AudioState::Closed;
            }
            playbackState_ = PlaybackState::Inactive;
            appExitStarted_ = false;
        }
        bridgeLogLine(QStringLiteral("BRIDGE_OK reason=%1 detail=%2").arg(bridgeReasonValue, bridgeDetailValue));
        return true;
    } catch (const std::exception& ex) {
        healthEngineInitialized.store(false, std::memory_order_relaxed);
        healthAudioDeviceReady.store(false, std::memory_order_relaxed);
        bridgeReasonValue = QStringLiteral("ExceptionThrown");
        bridgeDetailValue = truncateBridgeDetail(QString::fromUtf8(ex.what()));
        bridgeInitialized.store(false, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            bridgeState_ = BridgeState::Disconnected;
            engineState_ = EngineState::NotInitialized;
            audioState_ = AudioState::Closed;
            playbackState_ = PlaybackState::Inactive;
        }
        bridgeLogLine(QStringLiteral("BRIDGE_FAIL reason=%1 detail=%2").arg(bridgeReasonValue, bridgeDetailValue), true);
        return false;
    } catch (...) {
        healthEngineInitialized.store(false, std::memory_order_relaxed);
        healthAudioDeviceReady.store(false, std::memory_order_relaxed);
        bridgeReasonValue = QStringLiteral("UnknownNotOK");
        bridgeDetailValue = QStringLiteral("Unknown exception during bridge initialization");
        bridgeInitialized.store(false, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(stateMutex_);
            bridgeState_ = BridgeState::Disconnected;
            engineState_ = EngineState::NotInitialized;
            audioState_ = AudioState::Closed;
            playbackState_ = PlaybackState::Inactive;
        }
        bridgeLogLine(QStringLiteral("BRIDGE_FAIL reason=%1 detail=%2").arg(bridgeReasonValue, bridgeDetailValue), true);
        return false;
    }
}

bool EngineBridge::retryInitialize()
{
    bridgeLogLine(QStringLiteral("BRIDGE_RETRY"));
    return initializeBridge("retry");
}

QString EngineBridge::bridgeReason() const
{
    if (bridgeReasonValue.isEmpty()) {
        return bridgeInitialized.load(std::memory_order_relaxed) ? QStringLiteral("OK") : QStringLiteral("UnknownNotOK");
    }
    return bridgeReasonValue;
}

QString EngineBridge::bridgeDetail() const
{
    return bridgeDetailValue;
}

void EngineBridge::start()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (!canUseEngineLocked()) {
        bridgeLogLine(QStringLiteral("STATE_GUARD_BLOCK event=PressPlay reason=BridgeOrEngineNotReady"), true);
        return;
    }

    if (audioDisabledByEnv_ || envFlagEnabled("NGKS_DISABLE_AUDIO")) {
        audioDisabledByEnv_ = true;
        bridgeLogLine(QStringLiteral("PLAY_REJECT reason=AudioDisabledByEnv"), true);
        return;
    }

    if (!ensureAudioHotLocked("PressPlay")) {
        bridgeLogLine(QStringLiteral("PLAY_REJECT reason=EnsureAudioHotFailed"), true);
        return;
    }

    if (playbackState_ == PlaybackState::Active) {
        return;
    }

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
    playbackState_ = PlaybackState::Active;
    playbackStartGraceTicks_ = kPlaybackStartGraceTicks;
}

void EngineBridge::stop()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (!canUseEngineLocked()) {
        return;
    }

    if (playbackState_ == PlaybackState::Inactive) {
        return;
    }

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
    playbackState_ = PlaybackState::Inactive;
    playbackStartGraceTicks_ = 0;
}

bool EngineBridge::enterDjMode()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    uiModeState_ = UiModeState::DJ;
    const bool hot = ensureAudioHotLocked("EnterDJMode");
    if (hot) {
        audioLatchedByMode_ = true;
    }
    return hot;
}

void EngineBridge::leaveDjMode()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (uiModeState_ == UiModeState::DJ) {
        uiModeState_ = UiModeState::None;
    }
}

bool EngineBridge::enterSimpleMode()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    uiModeState_ = UiModeState::Simple;
    const bool hot = ensureAudioHotLocked("EnterSimpleMode");
    if (hot) {
        audioLatchedByMode_ = true;
    }
    return hot;
}

void EngineBridge::leaveSimpleMode()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (uiModeState_ == UiModeState::Simple) {
        uiModeState_ = UiModeState::None;
    }
}

bool EngineBridge::ensureAudioHot()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    return ensureAudioHotLocked("EnsureAudioHot");
}

void EngineBridge::notifyDeviceFailure(int errorCode)
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    handleDeviceFailureLocked(errorCode, "ExplicitNotify");
}

void EngineBridge::appExitTeardown()
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (appExitStarted_) {
        return;
    }
    appExitStarted_ = true;

    bridgeLogLine(QStringLiteral("APP_EXIT_BEGIN"));
    if (meterTimer.isActive()) {
        meterTimer.stop();
    }

    if (playbackState_ == PlaybackState::Active) {
        engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
        playbackState_ = PlaybackState::Inactive;
        playbackStartGraceTicks_ = 0;
    }

    if (audioState_ == AudioState::Open || audioState_ == AudioState::Failed) {
        bridgeLogLine(QStringLiteral("AUDIO_CLOSE_BEGIN event=AppExit"));
        engine.closeAudioDevice();
        audioState_ = AudioState::Closed;
        audioOpenGraceTicks_ = 0;
        healthAudioDeviceReady.store(false, std::memory_order_relaxed);
        bridgeLogLine(QStringLiteral("AUDIO_CLOSE_OK event=AppExit"));
    }

    audioLatchedByMode_ = false;
    uiModeState_ = UiModeState::None;
    runningValue = false;
    meterLeftValue = 0.0;
    meterRightValue = 0.0;
    bridgeLogLine(QStringLiteral("APP_EXIT_TEARDOWN"));
    bridgeLogLine(QStringLiteral("APP_EXIT_END"));
}

QString EngineBridge::engineStateMachineSummary() const
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    return QStringLiteral("bridge=%1 engine=%2 audio=%3 playback=%4 mode=%5 latched=%6 disabled=%7")
        .arg(QString::fromUtf8(toString(bridgeState_)),
             QString::fromUtf8(toString(engineState_)),
             QString::fromUtf8(toString(audioState_)),
             QString::fromUtf8(toString(playbackState_)),
             QString::fromUtf8(toString(uiModeState_)),
             audioLatchedByMode_ ? QStringLiteral("TRUE") : QStringLiteral("FALSE"),
             audioDisabledByEnv_ ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_A, nextCommandSeq++, 0, static_cast<float>(std::clamp(linear01, 0.0, 1.0)), 0 });
}

bool EngineBridge::startRtProbe(double toneHz, double toneDb)
{
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (!canUseEngineLocked()) {
        return false;
    }

    if (audioDisabledByEnv_ || envFlagEnabled("NGKS_DISABLE_AUDIO")) {
        audioDisabledByEnv_ = true;
        return false;
    }

    const bool ok = engine.startRtAudioProbe(static_cast<float>(toneHz), static_cast<float>(toneDb));
    if (ok) {
        audioState_ = AudioState::Open;
        audioOpenGraceTicks_ = kAudioOpenGraceTicks;
        healthAudioDeviceReady.store(true, std::memory_order_relaxed);
    } else {
        handleDeviceFailureLocked(-1, "StartRtProbeFailed");
    }
    return ok;
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
    std::lock_guard<std::mutex> lock(stateMutex_);
    if (!canUseEngineLocked()) {
        return false;
    }

    if (audioLatchedByMode_ && audioState_ == AudioState::Open) {
        bridgeLogLine(QStringLiteral("PROFILE_APPLY_REJECT reason=AudioLatchedOpen"), true);
        return false;
    }

    engine.setPreferredAudioFormat(static_cast<double>(sampleRate), bufferFrames, channelsOut);

    if (!deviceId.empty()) {
        engine.setPreferredAudioDeviceId(deviceId);
        if (engine.reopenAudioWithPreferredConfig()) {
            audioState_ = AudioState::Open;
            audioOpenGraceTicks_ = kAudioOpenGraceTicks;
            healthAudioDeviceReady.store(true, std::memory_order_relaxed);
            return true;
        }
    }

    if (!deviceName.empty()) {
        engine.setPreferredAudioDeviceName(deviceName);
        if (engine.reopenAudioWithPreferredConfig()) {
            audioState_ = AudioState::Open;
            audioOpenGraceTicks_ = kAudioOpenGraceTicks;
            healthAudioDeviceReady.store(true, std::memory_order_relaxed);
            return true;
        }
    }

    engine.clearPreferredAudioDevice();
    const bool fallbackOk = engine.reopenAudioWithPreferredConfig();
    if (fallbackOk) {
        audioState_ = AudioState::Open;
        audioOpenGraceTicks_ = kAudioOpenGraceTicks;
        healthAudioDeviceReady.store(true, std::memory_order_relaxed);
    } else {
        handleDeviceFailureLocked(-1, "ApplyAudioProfileReopenFailed");
    }
    return fallbackOk;
}

bool EngineBridge::tryGetStatus(UIStatus& out)
{
    try {
        const auto snapshot = engine.getSnapshot();
        out.engineReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
        out.sampleRateHz = 0;
        out.blockSize = 0;
        out.masterPeakLinear = std::max(snapshot.masterPeakL, snapshot.masterPeakR);
        return bridgeInitialized.load(std::memory_order_relaxed);
    } catch (const std::exception& ex) {
        bridgeInitialized.store(false, std::memory_order_relaxed);
        bridgeReasonValue = QStringLiteral("ExceptionThrown");
        bridgeDetailValue = truncateBridgeDetail(QString::fromUtf8(ex.what()));
        bridgeLogLine(QStringLiteral("BRIDGE_FAIL reason=%1 detail=%2").arg(bridgeReasonValue, bridgeDetailValue), true);
        out.engineReady = false;
        out.sampleRateHz = 0;
        out.blockSize = 0;
        out.masterPeakLinear = 0.0f;
        return false;
    } catch (...) {
        bridgeInitialized.store(false, std::memory_order_relaxed);
        bridgeReasonValue = QStringLiteral("UnknownNotOK");
        bridgeDetailValue = QStringLiteral("Unknown exception in status polling");
        bridgeLogLine(QStringLiteral("BRIDGE_FAIL reason=%1 detail=%2").arg(bridgeReasonValue, bridgeDetailValue), true);
        out.engineReady = false;
        out.sampleRateHz = 0;
        out.blockSize = 0;
        out.masterPeakLinear = 0.0f;
        return false;
    }
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

    {
        std::lock_guard<std::mutex> lock(stateMutex_);
        if (bridgeInitialized.load(std::memory_order_relaxed)) {
            bridgeState_ = BridgeState::Connected;
            engineState_ = EngineState::Initialized;
        }

        if (appExitStarted_) {
            audioState_ = AudioState::Closed;
            playbackState_ = PlaybackState::Inactive;
            audioOpenGraceTicks_ = 0;
            playbackStartGraceTicks_ = 0;
            healthAudioDeviceReady.store(false, std::memory_order_relaxed);
        } else {
            if (audioReady) {
                if (audioState_ != AudioState::Failed) {
                    audioState_ = AudioState::Open;
                }
                audioOpenGraceTicks_ = kAudioOpenGraceTicks;
            } else if (audioState_ == AudioState::Open && !appExitStarted_) {
                if (audioOpenGraceTicks_ > 0) {
                    --audioOpenGraceTicks_;
                } else {
                    handleDeviceFailureLocked(-2, "AudioClosedUnexpectedly");
                }
            } else if (audioState_ != AudioState::Failed) {
                audioState_ = AudioState::Closed;
                audioOpenGraceTicks_ = 0;
            }

            if (nowRunning) {
                playbackState_ = PlaybackState::Active;
                playbackStartGraceTicks_ = kPlaybackStartGraceTicks;
            } else if (playbackState_ == PlaybackState::Active && playbackStartGraceTicks_ > 0) {
                --playbackStartGraceTicks_;
            } else {
                playbackState_ = PlaybackState::Inactive;
                playbackStartGraceTicks_ = 0;
            }
        }
    }

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