#pragma once

#include <memory>
#include <mutex>

#include "engine/domain/EngineState.h"
#include "engine/runtime/CommandQueue.h"
#include "engine/runtime/RoutingMatrix.h"
#include "engine/runtime/graph/AudioGraph.h"

class AudioIOJuce;

class EngineCore
{
public:
    EngineCore();
    ~EngineCore();

    ngks::EngineState getSnapshot() const;
    void enqueueCommand(const ngks::Command& command);

    void prepare(double sampleRate, int blockSize);
    void process(float* left, float* right, int numSamples) noexcept;

private:
    void startAudioIfNeeded();
    void applyCommand(const ngks::Command& command);

    std::unique_ptr<AudioIOJuce> audioIO;
    bool audioOpened = false;
    mutable std::mutex stateMutex;

    ngks::EngineState state;
    ngks::CommandQueue commandQueue;
    ngks::RoutingMatrix routingMatrix;
    ngks::AudioGraph audioGraph;

    double sampleRateHz = 48000.0;
    int fadeSamplesTotal = 9600;
};