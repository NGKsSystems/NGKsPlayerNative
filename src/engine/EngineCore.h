#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>

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
    int32_t rtChannelsOut{0};
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
    char rtDeviceName[96] {};
};

class EngineCore
{
public:
    explicit EngineCore(bool offlineMode = false);
    ~EngineCore();

    ngks::EngineSnapshot getSnapshot();
    void enqueueCommand(const ngks::Command& command);
    void updateCrossfader(float x);
    bool renderOfflineBlock(float* outInterleavedLR, uint32_t frames);
    EngineTelemetrySnapshot getTelemetrySnapshot() const noexcept;
    bool startRtAudioProbe(float toneHz, float toneDb) noexcept;
    void stopRtAudioProbe() noexcept;
    bool pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept;

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
        std::atomic<int32_t> rtChannelsOut { 0 };
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

    std::unique_ptr<AudioIOJuce> audioIO;
    bool offlineMode_ = false;
    std::atomic<bool> audioOpened { false };
    std::atomic<uint32_t> frontSnapshotIndex { 0 };

    ngks::EngineSnapshot snapshots[2] {};
    DeckAuthorityState authority_[ngks::MAX_DECKS] {};
    ngks::SPSCCommandRing<1024> commandRing;
    MixMatrix mixMatrix_ {};
    float crossfaderPosition_ = 0.5f;
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
    float deckPeakSmoothing[ngks::MAX_DECKS] {};
    int deckPeakHoldBlocks[ngks::MAX_DECKS] {};
    float masterRmsSmoothing = 0.0f;
    float masterPeakSmoothing = 0.0f;
    int masterPeakHoldBlocks = 0;
    EngineTelemetry telemetry_ {};
    std::atomic<float> rtToneHz_ { 440.0f };
    std::atomic<float> rtToneLinear_ { 0.25f };
    float rtTonePhase_ = 0.0f;
    uint64_t rtWindowLastXRunTotal_ = 0;
    uint64_t rtLastObservedCallbackCount_ = 0;
    int64_t rtProbeStartTickMs_ = 0;
    int64_t rtLastProgressTickMs_ = 0;
    int64_t rtLastRecoveryAttemptMs_ = 0;
    uint32_t rtConsecutiveRecoveryFailures_ = 0;
    char rtDeviceName_[96] {};
};