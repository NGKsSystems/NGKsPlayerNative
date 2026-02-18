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

class EngineCore
{
public:
    EngineCore();
    ~EngineCore();

    ngks::EngineSnapshot getSnapshot();
    void enqueueCommand(const ngks::Command& command);
    void updateCrossfader(float x);

    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;

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

    std::unique_ptr<AudioIOJuce> audioIO;
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
};