#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include <QObject>
#include <QString>
#include <QTimer>

#include "engine/EngineCore.h"

struct UIStatus {
    std::string buildStamp;
    std::string gitSha;
    std::string buildTimestamp{__DATE__ " " __TIME__};
    bool engineReady{false};
    int sampleRateHz{0};
    int blockSize{0};
    float masterPeakLinear{0.0f};
    std::string lastUpdateUtc;
};

struct UIHealthSnapshot {
    bool engineInitialized{false};
    bool audioDeviceReady{false};
    bool lastRenderCycleOk{false};
    uint64_t renderCycleCounter{0};
};

struct UIEngineTelemetrySnapshot {
    static constexpr uint32_t kRenderDurationWindowSize = 64u;

    uint64_t renderCycles;
    uint64_t audioCallbacks;
    uint64_t xruns;
    uint32_t lastRenderDurationUs;
    uint32_t maxRenderDurationUs;
    uint32_t lastCallbackDurationUs;
    uint32_t maxCallbackDurationUs;
    uint32_t renderDurationWindowCount;
    uint32_t renderDurationWindowUs[kRenderDurationWindowSize] {};

    bool rtAudioEnabled{false};
    bool rtDeviceOpenOk{false};
    int32_t rtSampleRate{0};
    int32_t rtBufferFrames{0};
    int32_t rtRequestedSampleRate{0};
    int32_t rtRequestedBufferFrames{0};
    int32_t rtRequestedChannelsOut{2};
    int32_t rtChannelsIn{0};
    int32_t rtChannelsOut{0};
    bool rtAgFallback{false};
    uint64_t rtDeviceIdHash{0};
    uint64_t rtCallbackCount{0};
    uint64_t rtXRunCount{0};
    uint64_t rtXRunCountTotal{0};
    uint64_t rtXRunCountWindow{0};
    uint64_t rtLastCallbackNs{0};
    uint64_t rtJitterAbsNsMaxWindow{0};
    uint64_t rtCallbackIntervalNsLast{0};
    uint64_t rtCallbackIntervalNsMaxWindow{0};
    int32_t rtLastCallbackUs{0};
    int32_t rtMaxCallbackUs{0};
    int32_t rtMeterPeakDb10{-1200};
    bool rtWatchdogOk{true};
    int32_t rtWatchdogStateCode{0};
    uint32_t rtWatchdogTripCount{0};
    uint32_t rtDeviceRestartCount{0};
    int32_t rtLastDeviceErrorCode{0};
    bool rtRecoveryRequested{false};
    bool rtRecoveryFailedState{false};
    int64_t rtLastCallbackTickMs{0};
    char rtDeviceId[160] {};
    char rtDeviceName[96] {};
};

struct UISelfTestSnapshot {
    bool telemetryReadable{false};
    bool healthReadable{false};
    bool offlineRenderPasses{false};
    bool allPass{false};
};

struct UIFoundationSnapshot {
    bool engineInit{false};
    bool offlineRender{false};
    bool telemetry{false};
    bool healthSnapshot{false};
    bool diagnostics{false};
    bool selfTestsRan{false};
    bool selfTestsPass{false};
    uint64_t telemetryRenderCycles{0};
    bool healthRenderOk{false};
};

class EngineBridge final : public QObject
{
    Q_OBJECT
    Q_PROPERTY(double meterL READ meterL NOTIFY meterLChanged)
    Q_PROPERTY(double meterR READ meterR NOTIFY meterRChanged)
    Q_PROPERTY(bool running READ running NOTIFY runningChanged)

public:
    explicit EngineBridge(QObject* parent = nullptr);
    ~EngineBridge() override;

    Q_INVOKABLE void start();
    Q_INVOKABLE void stop();
    Q_INVOKABLE bool enterDjMode();
    Q_INVOKABLE void leaveDjMode();
    Q_INVOKABLE bool enterSimpleMode();
    Q_INVOKABLE void leaveSimpleMode();
    Q_INVOKABLE bool ensureAudioHot();
    Q_INVOKABLE void notifyDeviceFailure(int errorCode = -1);
    Q_INVOKABLE void appExitTeardown();
    Q_INVOKABLE QString engineStateMachineSummary();
    Q_INVOKABLE void setMasterGain(double linear01);
    Q_INVOKABLE void setEqBandGain(int band, double gainDb);
    Q_INVOKABLE void setEqBypass(bool bypassed);
    Q_INVOKABLE bool startRtProbe(double toneHz, double toneDb);
    Q_INVOKABLE void stopRtProbe();
    Q_INVOKABLE bool loadTrack(const QString& filePath);
    Q_INVOKABLE void pause();
    Q_INVOKABLE void seek(double seconds);

    // ── DJ deck-aware methods ──
    Q_INVOKABLE bool loadTrackToDeck(int deckIndex, const QString& filePath);
    Q_INVOKABLE void playDeck(int deckIndex);
    Q_INVOKABLE void stopDeck(int deckIndex);
    Q_INVOKABLE void unloadDeck(int deckIndex);
    Q_INVOKABLE void pauseDeck(int deckIndex);
    Q_INVOKABLE void seekDeck(int deckIndex, double seconds);
    Q_INVOKABLE void setDeckGain(int deckIndex, double linearGain);
    Q_INVOKABLE void setCrossfader(double position);
    Q_INVOKABLE void setDeckEqBandGain(int deckIndex, int band, double gainDb);
    Q_INVOKABLE void setDeckEqBypass(int deckIndex, bool bypassed);
    Q_INVOKABLE void setDeckMute(int deckIndex, bool muted);
    Q_INVOKABLE void setDeckCueMonitor(int deckIndex, bool enabled);
    Q_INVOKABLE void setDeckFilter(int deckIndex, double position);
    Q_INVOKABLE void setCueMix(double ratio);
    Q_INVOKABLE void setCueVolume(double linear);
    Q_INVOKABLE void setOutputMode(int mode);
    Q_INVOKABLE int outputMode() const;
    Q_INVOKABLE QStringList listAudioDeviceNames() const;
    Q_INVOKABLE bool switchAudioDevice(const QString& deviceName);
    Q_INVOKABLE QStringList listMidiDeviceNames() const;
    Q_INVOKABLE QString activeAudioDeviceName() const;

    // ── DJ device-loss ──
    Q_INVOKABLE bool isDjDeviceLost() const;
    Q_INVOKABLE void clearDjDeviceLost();
    Q_INVOKABLE void attemptDjRecovery();

    // ── DJ snapshot access ──
    Q_INVOKABLE double masterPeakL() const noexcept { return masterPeakLeftValue_; }
    Q_INVOKABLE double masterPeakR() const noexcept { return masterPeakRightValue_; }
    Q_INVOKABLE double cueMix() const noexcept { return cueMixValue_; }
    Q_INVOKABLE double cueVolume() const noexcept { return cueVolumeValue_; }
    Q_INVOKABLE double deckPlayhead(int deckIndex) const;
    Q_INVOKABLE double deckDuration(int deckIndex) const;
    Q_INVOKABLE bool deckIsPlaying(int deckIndex) const;
    Q_INVOKABLE bool deckIsMuted(int deckIndex) const;
    Q_INVOKABLE bool deckCueEnabled(int deckIndex) const;
    Q_INVOKABLE QString deckTrackLabel(int deckIndex) const;
    Q_INVOKABLE double deckPeakL(int deckIndex) const;
    Q_INVOKABLE double deckPeakR(int deckIndex) const;

    /// Get downsampled waveform overview for a deck (true min/max buckets).
    std::vector<ngks::WaveMinMax> getWaveformOverview(int deckIndex, int numBins);

    /// Get broad frequency-band energy overview for a deck.
    std::vector<ngks::BandEnergy> getBandEnergyOverview(int deckIndex, int numBins);

    /// Returns true when the full file decode (not just preload) is complete.
    bool isDeckFullyDecoded(int deckIndex) const;

    /// Returns the file path currently loaded in a deck.
    QString deckFilePath(int deckIndex) const;

    /// Returns the engine's cached/analyzed BPM for a deck (fixed-point / 100).
    /// Returns 0 if unavailable or no track loaded.
    double deckBpmFixed(int deckIndex) const;

    uint64_t currentLoadGen() const noexcept { return trackLoadGen_; }
    bool applyAudioProfile(const std::string& deviceId,
                           const std::string& deviceName,
                           int sampleRate,
                           int bufferFrames,
                           int channelsOut);
    bool retryInitialize();
    QString bridgeReason() const;
    QString bridgeDetail() const;

    bool tryGetStatus(UIStatus& out);
    bool tryGetHealth(UIHealthSnapshot& out) const;
    bool tryGetTelemetry(UIEngineTelemetrySnapshot& out) const noexcept;
    bool pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept;
    bool runSelfTests(UISelfTestSnapshot& out) noexcept;
    bool tryGetFoundation(UIFoundationSnapshot& out) const noexcept;

    double meterL() const noexcept;
    double meterR() const noexcept;
    bool running() const noexcept;

signals:
    void meterLChanged();
    void meterRChanged();
    void runningChanged();
    void playheadChanged(double seconds);
    void durationChanged(double seconds);
    void endOfTrack();
    void djSnapshotUpdated();  // emitted per poll so DJ page can refresh
    void deviceSwitchFinished(bool ok, const QString& activeDevice, long long elapsedMs);
    void audioHotReady(bool ok);
    void audioProfileApplied(bool ok);
    void djDeviceLost();
    void djRecoverySuccess(const QString& activeDevice);
    void djRecoveryFailed(const QString& reason);
    void djAutoRecoverySuccess(const QString& activeDevice, bool wasPlaying);

private:
    enum class BridgeState {
        Disconnected,
        Connected
    };

    enum class EngineState {
        NotInitialized,
        Initialized
    };

    enum class AudioState {
        Closed,
        Open,
        Failed
    };

    enum class PlaybackState {
        Inactive,
        Active
    };

    enum class UiModeState {
        None,
        DJ,
        Simple
    };

    static bool envFlagEnabled(const char* key);
    static const char* toString(BridgeState state);
    static const char* toString(EngineState state);
    static const char* toString(AudioState state);
    static const char* toString(PlaybackState state);
    static const char* toString(UiModeState state);

    bool initializeBridge(const char* stageTag);
    bool canUseEngineLocked() const;
    bool ensureAudioHotLocked(const char* triggerTag);
    void handleDeviceFailureLocked(int errorCode, const char* detailTag);
    void pollSnapshot();

    EngineCore engine;
    QTimer meterTimer;
    double meterLeftValue = 0.0;
    double meterRightValue = 0.0;
    double masterPeakLeftValue_ = 0.0;
    double masterPeakRightValue_ = 0.0;
    double cueMixValue_ = 0.5;
    double cueVolumeValue_ = 1.0;
    bool runningValue = false;
    double lastPlayheadSeconds = -1.0;
    double lastDurationSeconds = -1.0;
    bool endOfTrackEmitted = false;
    uint64_t trackLoadGen_{0};      // monotonic load generation counter
    QString loadedTrackPath_;       // path of last loaded track

    std::atomic<bool> healthEngineInitialized { false };
    std::atomic<bool> healthAudioDeviceReady { false };
    std::atomic<bool> healthLastRenderCycleOk { false };
    std::atomic<uint64_t> healthRenderCycleCounter { 0 };
    std::atomic<bool> selfTestsRan { false };
    std::atomic<bool> selfTestsPass { false };
    std::atomic<bool> bridgeInitialized { false };
    QString bridgeReasonValue {};
    QString bridgeDetailValue {};

    mutable std::mutex stateMutex_;
    BridgeState bridgeState_ { BridgeState::Disconnected };
    EngineState engineState_ { EngineState::NotInitialized };
    AudioState audioState_ { AudioState::Closed };
    PlaybackState playbackState_ { PlaybackState::Inactive };
    UiModeState uiModeState_ { UiModeState::None };
    bool audioLatchedByMode_ { false };
    bool audioDisabledByEnv_ { false };
    bool appExitStarted_ { false };
    bool djDeviceLostEmitted_ { false };
    int audioOpenGraceTicks_ { 0 };
    int playbackStartGraceTicks_ { 0 };
};