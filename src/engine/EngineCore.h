#pragma once

#include <atomic>
#include <memory>

#include "engine/command/Command.h"
#include "engine/runtime/EngineSnapshot.h"
#include "engine/runtime/RoutingMatrix.h"
#include "engine/runtime/SPSCCommandRing.h"
#include "engine/runtime/graph/AudioGraph.h"
#include "engine/runtime/jobs/JobSystem.h"

class AudioIOJuce;

class EngineCore
{
public:
    EngineCore();
    ~EngineCore();

    ngks::EngineSnapshot getSnapshot();
    void enqueueCommand(const ngks::Command& command);

    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;

private:
    void startAudioIfNeeded();
    ngks::CommandResult applyCommand(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept;
    ngks::CommandResult submitJobCommand(const ngks::Command& command) noexcept;
    void appendJobResults(ngks::EngineSnapshot& snapshot) noexcept;
    void publishCommandOutcome(const ngks::Command& command, ngks::CommandResult result) noexcept;

    std::unique_ptr<AudioIOJuce> audioIO;
    std::atomic<bool> audioOpened { false };
    std::atomic<uint32_t> frontSnapshotIndex { 0 };

    ngks::EngineSnapshot snapshots[2] {};
    ngks::SPSCCommandRing<1024> commandRing;
    ngks::RoutingMatrix routingMatrix;
    ngks::AudioGraph audioGraph;
    ngks::JobSystem jobSystem;

    double sampleRateHz = 48000.0;
    int fadeSamplesTotal = 9600;
    float deckRmsSmoothing[ngks::MAX_DECKS] { 0.0f, 0.0f };
    float deckPeakSmoothing[ngks::MAX_DECKS] { 0.0f, 0.0f };
    int deckPeakHoldBlocks[ngks::MAX_DECKS] { 0, 0 };
    float masterRmsSmoothing = 0.0f;
    float masterPeakSmoothing = 0.0f;
    int masterPeakHoldBlocks = 0;
};