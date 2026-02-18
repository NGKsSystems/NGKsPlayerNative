#pragma once

#include <atomic>
#include <chrono>
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
    };

private:
    void startAudioIfNeeded();
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
};