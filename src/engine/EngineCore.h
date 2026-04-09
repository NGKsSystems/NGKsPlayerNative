#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "engine/command/Command.h"
#include "engine/runtime/DeckAuthorityState.h"
#include "engine/runtime/EngineSnapshot.h"
#include "engine/runtime/MasterBus.h"
#include "engine/runtime/MixMatrix.h"
#include "engine/runtime/SPSCCommandRing.h"
#include "engine/runtime/graph/AudioGraph.h"
#include "engine/runtime/jobs/JobSystem.h"
#include "engine/runtime/library/RegistryStore.h"
#include "engine/runtime/library/TrackRegistry.h"

class AudioIOJuce;

struct EngineTelemetrySnapshot
{
    static constexpr uint32_t kRenderDurationWindowSize = 64u;

    uint64_t renderCycles{0};
    uint64_t audioCallbacks{0};
    uint64_t xruns{0};
    uint32_t lastRenderDurationUs{0};
    uint32_t maxRenderDurationUs{0};
    uint32_t lastCallbackDurationUs{0};
    uint32_t maxCallbackDurationUs{0};
    uint32_t renderDurationWindowCount{0};
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
    uint64_t cmdQueued{0};
    uint64_t cmdDropped{0};
    uint64_t cmdCoalesced{0};
    uint32_t cmdHighWaterMark{0};
    uint64_t snapshotPublishes{0};
    uint32_t engineRunState{0};

    char rtDeviceId[160] {};
    char rtDeviceName[96] {};
};

enum class EngineRunState : uint8_t
{
    Cold = 0,
    Ready,
    RtStarting,
    RtRunning,
    RtStopping,
    RtFailed
};

struct PlaybackStateCapture {
    struct DeckState {
        ngks::TransportState transport{ngks::TransportState::Stopped};
        double playheadSeconds{0.0};
        double lengthSeconds{0.0};
        bool hasTrack{false};
        bool muted{false};
        bool cueEnabled{false};
        char trackLabel[64]{};
    };
    DeckState decks[ngks::MAX_DECKS]{};
};

struct DeviceSwitchResult {
    bool ok{false};
    bool rollbackUsed{false};
    bool rollbackOk{false};
    std::string requestedDevice;
    std::string activeDevice;
    std::string previousDevice;
    PlaybackStateCapture capturedState;
    PlaybackStateCapture restoredState;
    long long elapsedMs{0};
};

class EngineCore
{
    friend class AudioIOJuce;
public:
    explicit EngineCore(bool offlineMode = false);
    ~EngineCore();

    ngks::EngineSnapshot getSnapshot() const;
    EngineRunState getRunState() const noexcept;
    void enqueueCommand(const ngks::Command& command);
    uint32_t nextSeq() noexcept { return internalCommandSeq_.fetch_add(1u, std::memory_order_relaxed); }
    void updateCrossfader(float x);
    void setOutputMode(int mode) noexcept { outputMode_.store(mode, std::memory_order_relaxed); }
    int outputMode() const noexcept { return outputMode_.load(std::memory_order_relaxed); }
    void setCueVolume(float linear) noexcept { cueVolume_.store(std::clamp(linear, 0.0f, 1.0f), std::memory_order_relaxed); }
    void setCueMixRatio(float ratio) noexcept { cueMixRatio_.store(std::clamp(ratio, 0.0f, 1.0f), std::memory_order_relaxed); }
    bool renderOfflineBlock(float* outInterleavedLR, uint32_t frames);
    EngineTelemetrySnapshot getTelemetrySnapshot() const noexcept;
    bool startRtAudioProbe(float toneHz, float toneDb) noexcept;
    void stopRtAudioProbe() noexcept;
    bool pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept;
    void setPreferredAudioDeviceId(const std::string& deviceId);
    void setPreferredAudioDeviceName(const std::string& deviceName);
    void setPreferredAudioFormat(double sampleRate, int bufferFrames, int channelsOut);
    void clearPreferredAudioDevice();
    bool reopenAudioWithPreferredConfig() noexcept;
    DeviceSwitchResult reopenAudioControlled() noexcept;
    PlaybackStateCapture capturePlaybackState() const noexcept;
    bool isDeviceSwitchInFlight() const noexcept { return deviceSwitchInFlight_.load(std::memory_order_acquire); }
    std::string getActiveDeviceName() const noexcept;

    // ── DJ mode device-loss ──
    void setDjMode(bool enabled) noexcept;
    bool isDjMode() const noexcept;
    bool isDjDeviceLost() const noexcept;
    void clearDjDeviceLost() noexcept;
    void forceStopAllDecks() noexcept;
    void forceDjDeviceLost() noexcept;

    /// Explicit DJ audio recovery — verifies device presence, reopens audio,
    /// clears djDeviceLost_ only on success.  No auto-play.
    struct DjRecoveryResult {
        bool ok{false};
        std::string activeDevice;
        std::string reason;
        std::string matchType;                  // "exact"/"normalized"/"class" or empty
        std::string expectedDevice;             // what we were looking for
        std::vector<std::string> available;     // devices enumerated during recovery
        bool deckWasPlaying[ngks::MAX_DECKS]{};  // per-deck transport state at time of loss
    };
    DjRecoveryResult attemptDjRecovery() noexcept;

    /// Periodic DJ output validity enforcer.
    /// Call from UI/message thread (~16ms poll).  Internally throttled.
    /// Returns true if it just forced a device-lost event.
    bool pollDjOutputEnforcer() noexcept;

    /// Periodic DJ auto-recovery probe.  Call from UI/message thread while
    /// djDeviceLost_ is active.  Checks if the intended DJ output has
    /// reappeared and, if a stable match persists, triggers recovery via
    /// the existing hardened pipeline.  Returns a valid DjRecoveryResult
    /// when auto-recovery fires (ok=true on success).  Returns ok=false
    /// with an empty reason when no action was taken (normal idle case).
    DjRecoveryResult pollDjAutoRecovery() noexcept;

    // Load a real audio file into a deck (called from UI thread, NOT RT).
    // Returns true on success; fills outDurationSeconds.
    bool loadFileIntoDeck(ngks::DeckId deckId, const std::string& filePath, double& outDurationSeconds, uint64_t trackLoadGen = 0);

    // Seek a deck to a position in seconds.
    void seekDeck(ngks::DeckId deckId, double seconds);

    /// Get downsampled waveform overview for a deck (thread-safe, true min/max).
    std::vector<ngks::WaveMinMax> getWaveformOverview(ngks::DeckId deckId, int numBins);

    /// Get broad frequency-band energy overview for a deck (time-domain).
    std::vector<ngks::BandEnergy> getBandEnergyOverview(ngks::DeckId deckId, int numBins);

    /// Returns true once the full file (not just preload) has been decoded.
    bool isDeckFullyDecoded(ngks::DeckId deckId) const;

    /// Returns the file path currently loaded in a deck (empty if none).
    std::string getDeckFilePath(ngks::DeckId deckId) const;

    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;

    struct EngineTelemetry
    {
        static constexpr uint32_t kRenderDurationHistorySize = 64u;

        std::atomic<uint64_t> renderCycles { 0 };
        std::atomic<uint64_t> audioCallbacks { 0 };
        std::atomic<uint64_t> xruns { 0 };
        std::atomic<uint32_t> lastRenderDurationUs { 0 };
        std::atomic<uint32_t> maxRenderDurationUs { 0 };
        std::atomic<uint32_t> lastCallbackDurationUs { 0 };
        std::atomic<uint32_t> maxCallbackDurationUs { 0 };
        std::atomic<uint32_t> renderDurationHistoryWriteIndex { 0 };
        std::atomic<uint32_t> renderDurationHistoryCount { 0 };
        std::atomic<uint32_t> renderDurationHistoryUs[kRenderDurationHistorySize] {};

        std::atomic<uint8_t> rtAudioEnabled { 0 };
        std::atomic<uint8_t> rtDeviceOpenOk { 0 };
        std::atomic<int32_t> rtSampleRate { 0 };
        std::atomic<int32_t> rtBufferFrames { 0 };
        std::atomic<int32_t> rtRequestedSampleRate { 0 };
        std::atomic<int32_t> rtRequestedBufferFrames { 0 };
        std::atomic<int32_t> rtRequestedChannelsOut { 2 };
        std::atomic<int32_t> rtChannelsIn { 0 };
        std::atomic<int32_t> rtChannelsOut { 0 };
        std::atomic<uint8_t> rtAgFallback { 0 };
        std::atomic<uint64_t> rtDeviceIdHash { 0 };
        std::atomic<uint64_t> rtCallbackCount { 0 };
        std::atomic<uint64_t> rtXRunCount { 0 };
        std::atomic<uint64_t> rtXRunCountWindow { 0 };
        std::atomic<uint64_t> rtLastCallbackNs { 0 };
        std::atomic<uint64_t> rtJitterAbsNsMaxWindow { 0 };
        std::atomic<uint64_t> rtCallbackIntervalNsLast { 0 };
        std::atomic<uint64_t> rtCallbackIntervalNsMaxWindow { 0 };
        std::atomic<int32_t> rtLastCallbackUs { 0 };
        std::atomic<int32_t> rtMaxCallbackUs { 0 };
        std::atomic<int32_t> rtMeterPeakDb10 { -1200 };
        std::atomic<uint8_t> rtWatchdogOk { 1 };
        std::atomic<int32_t> rtWatchdogStateCode { 0 };
        std::atomic<uint32_t> rtWatchdogTripCount { 0 };
        std::atomic<uint32_t> rtDeviceRestartCount { 0 };
        std::atomic<int32_t> rtLastDeviceErrorCode { 0 };
        std::atomic<uint8_t> rtRecoveryRequested { 0 };
        std::atomic<uint8_t> rtRecoveryFailedState { 0 };
        std::atomic<int64_t> rtLastCallbackTickMs { 0 };

        std::atomic<uint64_t> cmdQueued { 0 };
        std::atomic<uint64_t> cmdDropped { 0 };
        std::atomic<uint64_t> cmdCoalesced { 0 };
        std::atomic<uint32_t> cmdHighWaterMark { 0 };
        std::atomic<uint64_t> snapshotPublishes { 0 };
        std::atomic<uint32_t> engineRunState { static_cast<uint32_t>(EngineRunState::Cold) };
    };

private:
    bool startAudioIfNeeded(bool forceReopen = false);
    ngks::CommandResult applyCommand(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept;
    ngks::CommandResult submitJobCommand(const ngks::Command& command) noexcept;
    void appendJobResults(ngks::EngineSnapshot& snapshot) noexcept;
    void publishCommandOutcome(const ngks::Command& command, ngks::CommandResult result) noexcept;
    ngks::CommandResult applySetDeckTrack(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept;
    void applyCachedAnalysisToDeck(ngks::DeckSnapshot& deck, const ngks::AnalysisMeta& analysis) noexcept;
    void persistRegistryIfNeeded(bool force);
    bool validateTransition(DeckLifecycleState from, DeckLifecycleState to);
    bool isCriticalMutationCommand(const ngks::Command& c);
    bool isDeckMutationCommand(const ngks::Command& c);
    void pushRenderDurationSample(uint32_t durationUs) noexcept;
    void requestRtRecovery(int32_t errorCode) noexcept;
    bool performRtRecoveryIfNeeded(int64_t nowMs) noexcept;
    void sanitizeSnapshot(ngks::EngineSnapshot& snapshot) const noexcept;
    void publishSnapshot(const ngks::EngineSnapshot& snapshot) noexcept;
    void setRunState(EngineRunState state) noexcept;
    void notifyDeviceStopped() noexcept;

    std::unique_ptr<AudioIOJuce> audioIO;
    bool offlineMode_ = false;
    std::atomic<bool> audioOpened { false };
    std::atomic<bool> rtRecoveryInFlight_ { false };
    std::atomic<bool> deviceSwitchInFlight_ { false };  // suppress watchdog/recovery during intentional switch
    std::atomic<bool> djMode_ { false };
    std::atomic<bool> djDeviceLost_ { false };
    std::atomic<bool> djRecoveryInFlight_ { false };  // serializes recovery vs enforcer

    // ── DJ output validity enforcer state ──
    struct DjEnforcerState {
        int64_t firstInvalidTickMs{0};   // steady_clock ms when invalidity first detected
        uint64_t lastCallbackCount{0};   // last observed callback counter
        int64_t lastPollTickMs{0};       // last enforcer poll time (throttle)
        bool armed{false};               // true = invalid output detected, waiting threshold
    };
    DjEnforcerState djEnforcer_;

    // ── DJ auto-recovery probe state (active while djDeviceLost_ == true) ──
    struct DjAutoRecoveryState {
        int64_t lastProbeTickMs{0};           // throttle: don't probe more often than every 1s
        int64_t firstMatchTickMs{0};          // steady_clock ms when a safe match was first seen
        int64_t lastAttemptTickMs{0};          // when last recovery attempt was made (cooldown)
        std::string lastMatchedDevice;        // device name seen on previous probe
        bool matchStable{false};              // true = same match persisted across probes
    };
    DjAutoRecoveryState djAutoRecovery_;

    // ── Per-deck transport state captured at device-loss time ──
    bool deckWasPlayingBeforeLoss_[ngks::MAX_DECKS]{};

    std::atomic<uint32_t> frontSnapshotIndex { 0 };

    ngks::EngineSnapshot snapshots[2] {};
    DeckAuthorityState authority_[ngks::MAX_DECKS] {};
    ngks::SPSCCommandRing<1024> commandRing;
    std::atomic<uint32_t> internalCommandSeq_{1000000u}; // internal seq counter, starts high to avoid bridge collisions
    MixMatrix mixMatrix_ {};
    float crossfaderPosition_ = 0.5f;
    std::atomic<int> outputMode_ { 0 };  // 0=Stereo, 1=FullMono (master→L, cue→R)
    std::atomic<float> cueVolume_ { 1.0f };
    std::atomic<float> cueMixRatio_ { 0.5f };  // 0=cue only, 0.5=balanced, 1=master only
    ngks::MasterBus masterBus_ {};
    ngks::AudioGraph audioGraph;
    ngks::JobSystem jobSystem;
    ngks::TrackRegistry trackRegistry;
    ngks::RegistryStore registryStore;
    bool registryDirty = false;
    std::chrono::steady_clock::time_point lastRegistryPersist {};

    double sampleRateHz = 48000.0;
    int fadeSamplesTotal = 9600;
    float deckRmsSmoothing[ngks::MAX_DECKS] {};
    float deckPeakSmoothingL[ngks::MAX_DECKS] {};
    float deckPeakSmoothingR[ngks::MAX_DECKS] {};
    int deckPeakHoldBlocksL[ngks::MAX_DECKS] {};
    int deckPeakHoldBlocksR[ngks::MAX_DECKS] {};
    float masterRmsSmoothing = 0.0f;
    float masterPeakSmoothing = 0.0f;
    int masterPeakHoldBlocks = 0;
    EngineTelemetry telemetry_ {};
    std::atomic<float> rtToneHz_ { 440.0f };
    std::atomic<float> rtToneLinear_ { 0.25f };
    float rtTonePhase_ = 0.0f;
    std::string preferredAudioDeviceId_;
    std::string preferredAudioDeviceName_;
    double preferredAudioSampleRate_ = 0.0;
    int preferredAudioBufferFrames_ = 128;
    int preferredAudioOutputChannels_ = 2;
    uint64_t rtWindowLastXRunTotal_ = 0;
    uint64_t rtLastObservedCallbackCount_ = 0;
    int64_t rtProbeStartTickMs_ = 0;
    int64_t rtLastProgressTickMs_ = 0;
    int64_t rtLastRecoveryAttemptMs_ = 0;
    uint32_t rtConsecutiveRecoveryFailures_ = 0;
    char rtDeviceId_[160] {};
    char rtDeviceName_[96] {};
    mutable std::mutex controlMutex_;
    mutable std::mutex outcomeMutex_;
    ngks::EngineSnapshot pendingOutcome_ {};
    bool hasPendingOutcome_ = false;
};