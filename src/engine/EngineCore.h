#pragma once

#include <memory>
#include <mutex>

#include "engine/dsp/Limiter.h"
#include "engine/domain/EngineState.h"
#include "engine/runtime/CommandQueue.h"

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

    double sampleRateHz = 48000.0;
    float deckPhases[ngks::MAX_DECKS] { 0.0f, 0.0f };
    float deckPhaseIncrements[ngks::MAX_DECKS] { 0.0f, 0.0f };
    int deckFadeSamplesRemaining[ngks::MAX_DECKS] { 0, 0 };
    int fadeSamplesTotal = 9600;
    int requestedBufferSize = 128;
    int actualBufferSize = 0;

    Limiter limiter;
};