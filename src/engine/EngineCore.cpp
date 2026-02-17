#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>

namespace
{
constexpr float audibleRmsThresholdLinear = 0.0316227766f;
}

EngineCore::EngineCore()
    : audioIO(std::make_unique<AudioIOJuce>(*this))
{
    state.decks[ngks::DECK_A].id = ngks::DECK_A;
    state.decks[ngks::DECK_B].id = ngks::DECK_B;
}

EngineCore::~EngineCore()
{
    if (audioIO != nullptr) {
        audioIO->stop();
    }
}

ngks::EngineState EngineCore::getSnapshot() const
{
    std::lock_guard<std::mutex> lock(stateMutex);
    return state;
}

void EngineCore::enqueueCommand(const ngks::Command& command)
{
    if (command.type == ngks::CommandType::Play) {
        std::lock_guard<std::mutex> lock(stateMutex);
        startAudioIfNeeded();
    }

    commandQueue.enqueue(command);
}

void EngineCore::startAudioIfNeeded()
{
    if (audioOpened) {
        return;
    }

    const auto result = audioIO->start();
    if (!result.ok) {
        return;
    }

    sampleRateHz = result.sampleRate;
    audioOpened = true;
}

void EngineCore::prepare(double sampleRate, int)
{
    sampleRateHz = (sampleRate > 0.0) ? sampleRate : 48000.0;
    fadeSamplesTotal = static_cast<int>(sampleRateHz * 0.2);
    if (fadeSamplesTotal < 1) {
        fadeSamplesTotal = 1;
    }

    audioGraph.prepare(sampleRateHz, 2048);
}

void EngineCore::applyCommand(const ngks::Command& command)
{
    if (command.deck >= ngks::MAX_DECKS) {
        return;
    }

    auto& deck = state.decks[command.deck];
    switch (command.type) {
    case ngks::CommandType::LoadTrack:
        deck.hasTrack = true;
        deck.trackUid = command.stringValue.empty() ? "track" : command.stringValue;
        deck.lengthSeconds = 240.0;
        break;
    case ngks::CommandType::UnloadTrack:
        deck.hasTrack = false;
        deck.trackUid.clear();
        deck.transport = ngks::TransportState::Stopped;
        deck.playheadSeconds = 0.0;
        deck.cueEnabled = false;
        deck.publicFacing = false;
        break;
    case ngks::CommandType::Play:
        if (deck.hasTrack) {
            startAudioIfNeeded();
            deck.transport = ngks::TransportState::Starting;
        }
        break;
    case ngks::CommandType::Stop:
        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Stopping;
            audioGraph.beginDeckStopFade(command.deck, fadeSamplesTotal);
        }
        break;
    case ngks::CommandType::SetDeckGain:
        deck.deckGain = std::clamp(command.floatValue, 0.0f, 1.5f);
        break;
    case ngks::CommandType::SetMasterGain:
        state.masterGain = std::clamp(static_cast<double>(command.floatValue), 0.0, 1.5);
        break;
    case ngks::CommandType::SetCue:
        if (deck.publicFacing) {
            break;
        }
        deck.cueEnabled = command.floatValue >= 0.5f;
        break;
    }
}

void EngineCore::process(float* left, float* right, int numSamples) noexcept
{
    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(stateMutex);

    ngks::Command command { ngks::CommandType::Stop };
    while (commandQueue.tryDequeue(command)) {
        applyCommand(command);
    }

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        auto& deck = state.decks[deckIndex];
        if (deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Playing;
        }
    }

    const auto graphStats = audioGraph.render(state, routingMatrix, numSamples, left, right);

    state.masterRmsL = graphStats.masterRms;
    state.masterRmsR = graphStats.masterRms;

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        auto& deck = state.decks[deckIndex];
        deck.rmsL = graphStats.decks[deckIndex].rms;
        deck.rmsR = deck.rmsL;
        deck.peakL = graphStats.decks[deckIndex].peak;
        deck.peakR = deck.peakL;

        if (deck.transport == ngks::TransportState::Stopping && !audioGraph.isDeckStopFadeActive(deckIndex)) {
            deck.transport = ngks::TransportState::Stopped;
        }

        const bool isPlayingOrStopping = (deck.transport == ngks::TransportState::Playing)
            || (deck.transport == ngks::TransportState::Stopping);
        const bool routedToMaster = routingMatrix.get(deckIndex).toMasterWeight > 0.0f;
        const bool audible = std::max(deck.rmsL, deck.rmsR) > audibleRmsThresholdLinear;
        deck.publicFacing = isPlayingOrStopping && routedToMaster && audible;

        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Stopping) {
            deck.playheadSeconds += (static_cast<double>(numSamples) / sampleRateHz);
            if (deck.lengthSeconds > 0.0 && deck.playheadSeconds > deck.lengthSeconds) {
                deck.playheadSeconds = deck.lengthSeconds;
            }
        }
    }
}