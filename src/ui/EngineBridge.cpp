#include "ui/EngineBridge.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <thread>

#include <QDebug>
#include <QString>

#include "engine/audio/AudioIO_Juce.h"
#include "engine/command/Command.h"
#include "engine/DiagLog.h"

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <objbase.h>
#endif

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
    if (engine.isDjMode() && engine.isDjDeviceLost()) {
        ngks::audioTrace("DJ_GATE_BLOCK_PLAY",
            "fn=start djMode=1 djDeviceLost=1");
        qWarning().noquote() << QStringLiteral("DJ_GATE_BLOCK_PLAY: start() blocked — device-lost active");
        return;
    }
    const auto seq = engine.nextSeq();
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::start() => enqueue Play DECK_S seq=%1").arg(seq);
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_S, seq, 0, 0.0f, 0 });
}

void EngineBridge::stop()
{
    const auto seq = engine.nextSeq();
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::stop() => enqueue Stop DECK_S seq=%1").arg(seq);
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_S, seq, 0, 0.0f, 0 });
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_S, engine.nextSeq(), 0,
                            static_cast<float>(std::clamp(linear01, 0.0, 1.0)), 0 });
}

void EngineBridge::setEqBandGain(int band, double gainDb)
{
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetEqBandGain;
    cmd.deck = ngks::DECK_S;
    cmd.seq = engine.nextSeq();
    cmd.slotIndex = static_cast<uint8_t>(std::clamp(band, 0, 15));
    cmd.floatValue = static_cast<float>(std::clamp(gainDb, -12.0, 12.0));
    engine.enqueueCommand(cmd);
}

void EngineBridge::setEqBypass(bool bypassed)
{
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetEqBypass;
    cmd.deck = ngks::DECK_S;
    cmd.seq = engine.nextSeq();
    cmd.boolValue = bypassed ? 1 : 0;
    engine.enqueueCommand(cmd);
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
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_S,
                            engine.nextSeq(), 0, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, ngks::DECK_S,
                            engine.nextSeq(), 0, 0.0f, 0 });

    double duration = 0.0;
    if (!engine.loadFileIntoDeck(ngks::DECK_S, path, duration, trackLoadGen_)) {
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
    engine.enqueueCommand({ ngks::CommandType::Pause, ngks::DECK_S, engine.nextSeq(), 0, 0.0f, 0 });
}

void EngineBridge::seek(double seconds)
{
    qInfo().noquote() << QStringLiteral("TRC[G%1] seek target=%2 curPH=%3 curDur=%4 endFlag=%5 track=%6")
        .arg(trackLoadGen_).arg(seconds, 0, 'f', 2)
        .arg(lastPlayheadSeconds, 0, 'f', 2)
        .arg(lastDurationSeconds, 0, 'f', 2)
        .arg(endOfTrackEmitted ? "T" : "F")
        .arg(loadedTrackPath_);
    engine.seekDeck(ngks::DECK_S, seconds);
}

// ── DJ deck-aware methods ──

bool EngineBridge::loadTrackToDeck(int deckIndex, const QString& filePath)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    qInfo().noquote() << QStringLiteral("DJ loadTrack(deckIndex=%1, path=%2)").arg(deckIndex).arg(filePath);
    const auto did = static_cast<ngks::DeckId>(deckIndex);

    engine.enqueueCommand({ ngks::CommandType::Stop, did, engine.nextSeq(), 0, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, did, engine.nextSeq(), 0, 0.0f, 0 });

    const auto tDispatch = std::chrono::steady_clock::now();
    ngks::audioTrace("TRACK_LOAD_DISPATCH", "deck=%d path=%s tid=%lu",
                     deckIndex, filePath.toStdString().c_str(), (unsigned long)GetCurrentThreadId());

    // Decode on background thread to avoid freezing the UI
    std::thread([this, did, deckIndex, path = filePath.toStdString(), tDispatch]() {
        const auto tThread = std::chrono::steady_clock::now();
        const auto dispatchToThreadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            tThread - tDispatch).count();
        ngks::audioTrace("TRACK_LOAD_THREAD_START", "deck=%d dispatchToThreadMs=%lld tid=%lu",
                         deckIndex, dispatchToThreadMs, (unsigned long)GetCurrentThreadId());

        double duration = 0.0;
        if (!engine.loadFileIntoDeck(did, path, duration, 0)) {
            qWarning().noquote() << QStringLiteral("DJ loadTrackToDeck FAIL deck=%1").arg(deckIndex);
            return;
        }
        const auto threadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - tThread).count();
        const auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - tDispatch).count();
        ngks::audioTrace("TRACK_LOAD_READY", "deck=%d dur=%.2f threadMs=%lld totalMs=%lld",
                         deckIndex, duration, threadMs, totalMs);
        qInfo().noquote() << QStringLiteral("DJ loadTrackToDeck OK deck=%1 dur=%2 threadMs=%3 totalMs=%4")
            .arg(deckIndex).arg(duration, 0, 'f', 2).arg(threadMs).arg(totalMs);
    }).detach();

    return true;
}

void EngineBridge::playDeck(int deckIndex)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    if (engine.isDjMode() && engine.isDjDeviceLost()) {
        ngks::audioTrace("DJ_GATE_BLOCK_PLAY",
            "fn=playDeck deck=%d djMode=1 djDeviceLost=1", deckIndex);
        qWarning().noquote() << QStringLiteral("DJ_GATE_BLOCK_PLAY: playDeck(%1) blocked — device-lost active").arg(deckIndex);
        return;
    }
    const auto did = static_cast<ngks::DeckId>(deckIndex);
    engine.enqueueCommand({ ngks::CommandType::Play, did, engine.nextSeq(), 0, 0.0f, 0 });
    qInfo().noquote() << QStringLiteral("DJ playDeck(deckIndex=%1)").arg(deckIndex);
}

void EngineBridge::stopDeck(int deckIndex)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    const auto did = static_cast<ngks::DeckId>(deckIndex);
    engine.enqueueCommand({ ngks::CommandType::Stop, did, engine.nextSeq(), 0, 0.0f, 0 });
    qInfo().noquote() << QStringLiteral("DJ stopDeck(deckIndex=%1)").arg(deckIndex);
}

void EngineBridge::unloadDeck(int deckIndex)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    const auto did = static_cast<ngks::DeckId>(deckIndex);
    engine.enqueueCommand({ ngks::CommandType::Stop, did, engine.nextSeq(), 0, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, did, engine.nextSeq(), 0, 0.0f, 0 });
    qInfo().noquote() << QStringLiteral("DJ unloadDeck(deckIndex=%1)").arg(deckIndex);
}

void EngineBridge::pauseDeck(int deckIndex)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    const auto did = static_cast<ngks::DeckId>(deckIndex);
    engine.enqueueCommand({ ngks::CommandType::Pause, did, engine.nextSeq(), 0, 0.0f, 0 });
    qInfo().noquote() << QStringLiteral("DJ pauseDeck(deckIndex=%1)").arg(deckIndex);
}

void EngineBridge::seekDeck(int deckIndex, double seconds)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    engine.seekDeck(static_cast<ngks::DeckId>(deckIndex), seconds);
    qInfo().noquote() << QStringLiteral("DJ seekDeck(deckIndex=%1, seconds=%2)")
        .arg(deckIndex).arg(seconds, 0, 'f', 3);
}

void EngineBridge::setDeckGain(int deckIndex, double linearGain)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    const auto did = static_cast<ngks::DeckId>(deckIndex);
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetDeckGain;
    cmd.deck = did;
    cmd.seq = engine.nextSeq();
    cmd.floatValue = static_cast<float>(std::clamp(linearGain, 0.0, 2.0));
    engine.enqueueCommand(cmd);
}

void EngineBridge::setCrossfader(double position)
{
    engine.updateCrossfader(static_cast<float>(std::clamp(position, 0.0, 1.0)));
}

void EngineBridge::setDeckEqBandGain(int deckIndex, int band, double gainDb)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetEqBandGain;
    cmd.deck = static_cast<ngks::DeckId>(deckIndex);
    cmd.seq = engine.nextSeq();
    cmd.slotIndex = static_cast<uint8_t>(std::clamp(band, 0, 15));
    cmd.floatValue = static_cast<float>(std::clamp(gainDb, -6.0, 6.0));
    engine.enqueueCommand(cmd);
}

void EngineBridge::setDeckEqBypass(int deckIndex, bool bypassed)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetEqBypass;
    cmd.deck = static_cast<ngks::DeckId>(deckIndex);
    cmd.seq = engine.nextSeq();
    cmd.boolValue = bypassed ? 1 : 0;
    engine.enqueueCommand(cmd);
}

void EngineBridge::setDeckMute(int deckIndex, bool muted)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetDeckMute;
    cmd.deck = static_cast<ngks::DeckId>(deckIndex);
    cmd.seq = engine.nextSeq();
    cmd.boolValue = muted ? 1 : 0;
    engine.enqueueCommand(cmd);
}

void EngineBridge::setDeckCueMonitor(int deckIndex, bool enabled)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetDeckCueMonitor;
    cmd.deck = static_cast<ngks::DeckId>(deckIndex);
    cmd.seq = engine.nextSeq();
    cmd.boolValue = enabled ? 1 : 0;
    engine.enqueueCommand(cmd);
}

void EngineBridge::setDeckFilter(int deckIndex, double position)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return;
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::SetDeckFilter;
    cmd.deck = static_cast<ngks::DeckId>(deckIndex);
    cmd.seq = engine.nextSeq();
    cmd.floatValue = static_cast<float>(std::clamp(position, 0.0, 1.0));
    engine.enqueueCommand(cmd);
}

void EngineBridge::setCueMix(double ratio)
{
    cueMixValue_ = std::clamp(ratio, 0.0, 1.0);
    engine.setCueMixRatio(static_cast<float>(cueMixValue_));
}

void EngineBridge::setCueVolume(double linear)
{
    cueVolumeValue_ = std::clamp(linear, 0.0, 1.0);
    engine.setCueVolume(static_cast<float>(cueVolumeValue_));
}

void EngineBridge::setOutputMode(int mode)
{
    engine.setOutputMode(mode);
}

int EngineBridge::outputMode() const
{
    return engine.outputMode();
}

QStringList EngineBridge::listAudioDeviceNames() const
{
    QStringList names;
    const auto devices = AudioIOJuce::listAudioDevices();
    for (const auto& d : devices) {
        if (d.outputChannels >= 2) {
            names.append(QString::fromStdString(d.deviceName));
        }
    }
    return names;
}

QStringList EngineBridge::listMidiDeviceNames() const
{
    QStringList names;
    const auto devices = juce::MidiInput::getAvailableDevices();
    for (const auto& d : devices)
        names.append(QString::fromStdString(d.name.toStdString()));
    return names;
}

bool EngineBridge::switchAudioDevice(const QString& deviceName)
{
    ngks::audioTrace("BRIDGE_ENTER", "fn=switchAudioDevice device=\"%s\"", deviceName.toUtf8().constData());
    qInfo().noquote() << QStringLiteral("DEVICE_SWITCH_UI: requested device='%1' — dispatching to background thread").arg(deviceName);
    engine.setPreferredAudioDeviceName(deviceName.toStdString());
    std::thread([this, deviceName]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        const unsigned long bgTid = GetCurrentThreadId();
        qInfo().noquote() << QStringLiteral("DEVICE_SWITCH_UI: bg thread tid=%1 starting controlled restart")
            .arg(bgTid);
        const auto switchResult = engine.reopenAudioControlled();
        const QString activeDev = QString::fromStdString(switchResult.activeDevice);
        const auto ms = switchResult.elapsedMs;
        qInfo().noquote() << QStringLiteral("DEVICE_SWITCH_UI: result=%1 active='%2' rollback=%3 tid=%4 [%5ms]")
            .arg(switchResult.ok ? QStringLiteral("OK") : QStringLiteral("FAIL"),
                 activeDev,
                 switchResult.rollbackUsed
                     ? (switchResult.rollbackOk ? QStringLiteral("OK") : QStringLiteral("FAIL"))
                     : QStringLiteral("N/A"),
                 QString::number(bgTid), QString::number(ms));
        QMetaObject::invokeMethod(this, [this, ok = switchResult.ok, activeDev, ms]() {
            emit deviceSwitchFinished(ok, activeDev, static_cast<long long>(ms));
        }, Qt::QueuedConnection);
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
    return true;
}

QString EngineBridge::activeAudioDeviceName() const
{
    return QString::fromStdString(engine.getActiveDeviceName());
}

bool EngineBridge::deckHasTrack(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    return engine.getSnapshot().decks[deckIndex].hasTrack != 0;
}

int EngineBridge::deckLifecycle(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0;
    return static_cast<int>(engine.getSnapshot().decks[deckIndex].lifecycle);
}

double EngineBridge::deckLengthSeconds(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    return engine.getSnapshot().decks[deckIndex].lengthSeconds;
}

double EngineBridge::deckPlayhead(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    return engine.getSnapshot().decks[deckIndex].playheadSeconds;
}

double EngineBridge::deckDuration(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    return engine.getSnapshot().decks[deckIndex].lengthSeconds;
}

bool EngineBridge::deckIsPlaying(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    const auto t = engine.getSnapshot().decks[deckIndex].transport;
    return t == ngks::TransportState::Starting || t == ngks::TransportState::Playing;
}

bool EngineBridge::deckIsMuted(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    return engine.getSnapshot().decks[deckIndex].muted;
}

bool EngineBridge::deckCueEnabled(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    return engine.getSnapshot().decks[deckIndex].cueEnabled;
}

QString EngineBridge::deckTrackLabel(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return {};
    return QString::fromUtf8(engine.getSnapshot().decks[deckIndex].currentTrackLabel);
}

double EngineBridge::deckPeakL(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    return static_cast<double>(engine.getSnapshot().decks[deckIndex].peakL);
}

double EngineBridge::deckPeakR(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    return static_cast<double>(engine.getSnapshot().decks[deckIndex].peakR);
}

std::vector<ngks::WaveMinMax> EngineBridge::getWaveformOverview(int deckIndex, int numBins)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return {};
    return engine.getWaveformOverview(static_cast<ngks::DeckId>(deckIndex), numBins);
}

std::vector<ngks::BandEnergy> EngineBridge::getBandEnergyOverview(int deckIndex, int numBins)
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return {};
    return engine.getBandEnergyOverview(static_cast<ngks::DeckId>(deckIndex), numBins);
}

bool EngineBridge::isDeckFullyDecoded(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return false;
    return engine.isDeckFullyDecoded(static_cast<ngks::DeckId>(deckIndex));
}

QString EngineBridge::deckFilePath(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return {};
    return QString::fromStdString(engine.getDeckFilePath(static_cast<ngks::DeckId>(deckIndex)));
}

double EngineBridge::deckBpmFixed(int deckIndex) const
{
    if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) return 0.0;
    const auto& snap = engine.getSnapshot().decks[deckIndex];
    return static_cast<double>(snap.cachedBpmFixed) / 100.0;
}

bool EngineBridge::applyAudioProfile(const std::string& deviceId,
                                     const std::string& deviceName,
                                     int sampleRate,
                                     int bufferFrames,
                                     int channelsOut)
{
    ngks::audioTrace("BRIDGE_ENTER", "fn=applyAudioProfile device=\"%s\" sr=%d buf=%d", deviceName.c_str(), sampleRate, bufferFrames);
    if (engine.isDeviceSwitchInFlight()) {
        qInfo().noquote() << QStringLiteral("DIAG: applyAudioProfile() skipped — switch in flight");
        QMetaObject::invokeMethod(this, [this]() { emit audioProfileApplied(false); }, Qt::QueuedConnection);
        return false;
    }

    std::thread([this, deviceId, deviceName, sampleRate, bufferFrames, channelsOut]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        engine.setPreferredAudioFormat(static_cast<double>(sampleRate), bufferFrames, channelsOut);

        bool ok = false;
        if (!deviceId.empty()) {
            engine.setPreferredAudioDeviceId(deviceId);
            ok = engine.reopenAudioWithPreferredConfig();
        }

        if (!ok && !deviceName.empty()) {
            engine.setPreferredAudioDeviceName(deviceName);
            ok = engine.reopenAudioWithPreferredConfig();
        }

        if (!ok) {
            engine.clearPreferredAudioDevice();
            ok = engine.reopenAudioWithPreferredConfig();
        }

        QMetaObject::invokeMethod(this, [this, ok]() {
            emit audioProfileApplied(ok);
        }, Qt::QueuedConnection);
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
    return true;
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
    ngks::audioTrace("BRIDGE_ENTER", "fn=ensureAudioHot");

    // ── DJ device-lost hard gate: block audio reopen ──
    if (engine.isDjMode() && engine.isDjDeviceLost()) {
        ngks::audioTrace("DJ_GATE_BLOCK_AUDIO_OPEN",
            "fn=ensureAudioHot djMode=1 djDeviceLost=1");
        qWarning().noquote() << QStringLiteral("DJ_GATE_BLOCK_AUDIO_OPEN: ensureAudioHot() blocked — device-lost active");
        QMetaObject::invokeMethod(this, [this]() { emit audioHotReady(false); }, Qt::QueuedConnection);
        return false;
    }

    const auto snapshot = engine.getSnapshot();
    if ((snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u) {
        qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() audio already running");
        ngks::audioTrace("BRIDGE_EXIT", "fn=ensureAudioHot result=already_running");
        QMetaObject::invokeMethod(this, [this]() { emit audioHotReady(true); }, Qt::QueuedConnection);
        return true;
    }

    if (engine.isDeviceSwitchInFlight()) {
        qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() skipped — switch in flight");
        return true;
    }

    qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() dispatching to background thread...");
    std::thread([this]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        const bool ok = engine.reopenAudioWithPreferredConfig();
        qInfo().noquote() << QStringLiteral("DIAG: ensureAudioHot() bg result=%1").arg(ok ? "OK" : "FAIL");
        healthAudioDeviceReady.store(ok, std::memory_order_relaxed);
        QMetaObject::invokeMethod(this, [this, ok]() {
            emit audioHotReady(ok);
        }, Qt::QueuedConnection);
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
    return true;
}

bool EngineBridge::enterDjMode()
{
    ngks::audioTrace("BRIDGE_ENTER", "fn=enterDjMode");
    healthEngineInitialized.store(true, std::memory_order_relaxed);
    engine.setDjMode(true);
    djDeviceLostEmitted_ = false;
    // No dummy track loads — DJ UI will load tracks to specific decks on demand

    // ── DJ device-lost hard gate: block audio open on re-enter ──
    if (engine.isDjDeviceLost()) {
        ngks::audioTrace("DJ_GATE_BLOCK_AUDIO_OPEN",
            "fn=enterDjMode djDeviceLost=1 — audio open blocked until explicit recovery");
        qWarning().noquote() << QStringLiteral("DJ_GATE_BLOCK_AUDIO_OPEN: enterDjMode() blocked audio open — device-lost still active");
        meterTimer.start(); // keep polling so UI sees device-lost flag
        return false;
    }

    const auto snapshot = engine.getSnapshot();
    if ((snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u) {
        meterTimer.start();
        return true;
    }

    if (engine.isDeviceSwitchInFlight()) {
        qInfo().noquote() << QStringLiteral("DIAG: enterDjMode() skipped — switch in flight");
        return true;
    }

    qInfo().noquote() << QStringLiteral("DIAG: enterDjMode() dispatching audio open to background thread...");
    std::thread([this]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        const bool ok = engine.reopenAudioWithPreferredConfig();
        healthAudioDeviceReady.store(ok, std::memory_order_relaxed);
        QMetaObject::invokeMethod(this, [this, ok]() {
            if (ok) {
                meterTimer.start();
            }
            emit audioHotReady(ok);
        }, Qt::QueuedConnection);
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
    return true;
}

void EngineBridge::leaveDjMode()
{
    engine.setDjMode(false);
    djDeviceLostEmitted_ = false;
    meterTimer.stop();
}

bool EngineBridge::isDjDeviceLost() const
{
    return engine.isDjDeviceLost();
}

void EngineBridge::clearDjDeviceLost()
{
    engine.clearDjDeviceLost();
    djDeviceLostEmitted_ = false;
    qInfo().noquote() << QStringLiteral("DIAG: clearDjDeviceLost() — user cleared device-lost state");
}

void EngineBridge::attemptDjRecovery()
{
    ngks::audioTrace("DJ_RECOVERY_REQUESTED", "fn=attemptDjRecovery source=bridge");
    qInfo().noquote() << QStringLiteral("DIAG: attemptDjRecovery() — user triggered explicit recovery");

    std::thread([this]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        const auto result = engine.attemptDjRecovery();

        if (result.ok) {
            djDeviceLostEmitted_ = false;
            QMetaObject::invokeMethod(this, [this, dev = QString::fromStdString(result.activeDevice)]() {
                emit djRecoverySuccess(dev);
            }, Qt::QueuedConnection);
        } else {
            QMetaObject::invokeMethod(this, [this, reason = QString::fromStdString(result.reason)]() {
                emit djRecoveryFailed(reason);
            }, Qt::QueuedConnection);
        }
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
}

bool EngineBridge::enterSimpleMode()
{
    ngks::audioTrace("BRIDGE_ENTER", "fn=enterSimpleMode");
    qInfo().noquote() << QStringLiteral("DIAG: EngineBridge::enterSimpleMode() called");
    healthEngineInitialized.store(true, std::memory_order_relaxed);

    const auto snapshot = engine.getSnapshot();
    if ((snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u) {
        qInfo().noquote() << QStringLiteral("DIAG: enterSimpleMode() audio already running");
        meterTimer.start();
        return true;
    }

    if (engine.isDeviceSwitchInFlight()) {
        qInfo().noquote() << QStringLiteral("DIAG: enterSimpleMode() skipped — switch in flight");
        return true;
    }

    qInfo().noquote() << QStringLiteral("DIAG: enterSimpleMode() dispatching audio open to background thread...");
    std::thread([this]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        const bool ok = engine.reopenAudioWithPreferredConfig();
        qInfo().noquote() << QStringLiteral("DIAG: enterSimpleMode() bg result=%1").arg(ok ? "OK" : "FAIL");
        healthAudioDeviceReady.store(ok, std::memory_order_relaxed);
        QMetaObject::invokeMethod(this, [this, ok]() {
            if (ok) {
                meterTimer.start();
            }
            emit audioHotReady(ok);
        }, Qt::QueuedConnection);
#ifdef _WIN32
        CoUninitialize();
#endif
    }).detach();
    return true;
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
    const auto t = snapshot.decks[ngks::DECK_S].transport;

    return QString("audio=%1 transportA=%2")
        .arg(audioRunning ? "RUNNING" : "STOPPED")
        .arg(static_cast<int>(t));
}

void EngineBridge::pollSnapshot()
{
    // ── DJ output validity enforcer (runs before snapshot read) ──
    engine.pollDjOutputEnforcer();

    // ── DJ auto-recovery probe (runs while device-lost is active) ──
    {
        const auto autoResult = engine.pollDjAutoRecovery();
        if (autoResult.ok) {
            djDeviceLostEmitted_ = false;

            // Auto-resume decks that were playing before device loss
            bool anyWasPlaying = false;
            for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
                if (autoResult.deckWasPlaying[d]) {
                    anyWasPlaying = true;
                    const auto did = static_cast<ngks::DeckId>(d);
                    engine.enqueueCommand({ ngks::CommandType::Play, did, engine.nextSeq(), 0, 0.0f, 0 });
                    ngks::audioTrace("DJ_AUTO_RESUME_PLAYBACK",
                        "deck=%u source=auto wasPlayingBeforeLoss=1", d);
                } else {
                    ngks::audioTrace("DJ_AUTO_RESUME_SKIPPED",
                        "deck=%u source=auto wasPlayingBeforeLoss=0", d);
                }
            }

            QMetaObject::invokeMethod(this, [this, dev = QString::fromStdString(autoResult.activeDevice), anyWasPlaying]() {
                emit djAutoRecoverySuccess(dev, anyWasPlaying);
            }, Qt::QueuedConnection);
        }
    }

    const auto snapshot = engine.getSnapshot();

    // ── DJ device-lost detection ──
    if ((snapshot.flags & ngks::SNAP_DJ_DEVICE_LOST) != 0u && !djDeviceLostEmitted_) {
        djDeviceLostEmitted_ = true;
        qWarning().noquote() << QStringLiteral("DJ_DEVICE_LOST: audio endpoint lost in DJ mode — all playback stopped");
        emit djDeviceLost();
    }

    const auto& deckA = snapshot.decks[ngks::DECK_A];
    const auto& deckB = snapshot.decks[ngks::DECK_B];
    const auto& deckS = snapshot.decks[ngks::DECK_S];

    static uint8_t lastHasTrackA = 2;
    if (deckA.hasTrack != lastHasTrackA) {
        lastHasTrackA = deckA.hasTrack;
        qWarning().noquote() << QString("SNAPSHOT_BRIDGE deck=A hasTrack=%1 lifecycle=%2 duration=%3").arg(deckA.hasTrack).arg(static_cast<int>(deckA.lifecycle)).arg(deckA.lengthSeconds);
    }
    static uint8_t lastHasTrackB = 2;
    if (deckB.hasTrack != lastHasTrackB) {
        lastHasTrackB = deckB.hasTrack;
        qWarning().noquote() << QString("SNAPSHOT_BRIDGE deck=B hasTrack=%1 lifecycle=%2 duration=%3").arg(deckB.hasTrack).arg(static_cast<int>(deckB.lifecycle)).arg(deckB.lengthSeconds);
    }

    const double newL = std::clamp(static_cast<double>(deckS.peakL), 0.0, 1.0);
    const double newR = std::clamp(static_cast<double>(deckS.peakR), 0.0, 1.0);
    const auto transportA = deckA.transport;
    const auto transportB = deckB.transport;
    const auto transportS = deckS.transport;
    const auto isActive = [](ngks::TransportState t) {
        return t == ngks::TransportState::Starting
            || t == ngks::TransportState::Playing
            || t == ngks::TransportState::Stopping;
    };
    const bool nowRunning = isActive(transportA) || isActive(transportB) || isActive(transportS);

    const bool audioReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    const bool renderOk = std::isfinite(snapshot.masterPeakL)
        && std::isfinite(snapshot.masterPeakR)
        && std::isfinite(snapshot.masterRmsL)
        && std::isfinite(snapshot.masterRmsR);

    healthAudioDeviceReady.store(audioReady, std::memory_order_relaxed);
    healthLastRenderCycleOk.store(renderOk, std::memory_order_relaxed);
    healthRenderCycleCounter.fetch_add(1u, std::memory_order_relaxed);

    masterPeakLeftValue_  = std::clamp(static_cast<double>(snapshot.masterPeakL), 0.0, 1.2);
    masterPeakRightValue_ = std::clamp(static_cast<double>(snapshot.masterPeakR), 0.0, 1.2);

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

    // DJ mode: emit UNCONDITIONALLY so deck strips can update meters / playheads.
    // DJ decks read their own snapshot data directly (deckPlayhead, deckPeakL, etc.)
    // and are NOT dependent on the simple-mode generation tracking below.
    emit djSnapshotUpdated();

    // ── Generation-gated snapshot acceptance (simple mode only) ──
    // Drop snapshot data if the engine hasn't processed the current
    // track load yet. trackLoadGen is threaded through the command
    // queue and stamped on DeckSnapshot — zero timing dependency.
    const double playhead = deckS.playheadSeconds;
    const double duration = deckS.lengthSeconds;
    const uint64_t snapGen = deckS.trackLoadGen;

    if (snapGen != trackLoadGen_) {
        // Stale snapshot from previous track — drop simple-mode updates
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

    // DJ diagnostics: log A/B deck telemetry when active or when values change.
    {
        static int logTick = 0;
        static double prevPlayhead[2] { -1.0, -1.0 };
        static double prevDuration[2] { -1.0, -1.0 };
        static int prevTransport[2] { -1, -1 };
        static QString prevLabel[2];

        const ngks::DeckSnapshot* decks[2] { &deckA, &deckB };
        const bool periodic = (++logTick % 30) == 0;
        for (int i = 0; i < 2; ++i) {
            const auto& d = *decks[i];
            const QString label = QString::fromUtf8(d.currentTrackLabel);
            const bool isPlaying = d.transport == ngks::TransportState::Starting
                || d.transport == ngks::TransportState::Playing;
            const bool changed = (prevLabel[i] != label)
                || (std::abs(prevDuration[i] - d.lengthSeconds) > 0.05)
                || (std::abs(prevPlayhead[i] - d.playheadSeconds) > 0.20)
                || (prevTransport[i] != static_cast<int>(d.transport));
            const bool active = !label.isEmpty() || d.hasTrack != 0 || isPlaying;

            if (active && (periodic || changed)) {
                qInfo().noquote() << QStringLiteral("DJ SNAP deck=%1 label=%2 duration=%3 isPlaying=%4 playhead=%5 peakL=%6 peakR=%7 transport=%8 hasTrack=%9")
                    .arg(i)
                    .arg(label)
                    .arg(d.lengthSeconds, 0, 'f', 2)
                    .arg(isPlaying ? QStringLiteral("true") : QStringLiteral("false"))
                    .arg(d.playheadSeconds, 0, 'f', 3)
                    .arg(d.peakL, 0, 'f', 4)
                    .arg(d.peakR, 0, 'f', 4)
                    .arg(static_cast<int>(d.transport))
                    .arg(static_cast<int>(d.hasTrack));
            }

            prevLabel[i] = label;
            prevDuration[i] = d.lengthSeconds;
            prevPlayhead[i] = d.playheadSeconds;
            prevTransport[i] = static_cast<int>(d.transport);
        }
    }

    // (djSnapshotUpdated already emitted above, unconditionally)
}
