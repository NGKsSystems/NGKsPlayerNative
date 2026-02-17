#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>
#include <cmath>

namespace
{
constexpr float twoPi = 6.28318530717958647692f;
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

    actualBufferSize = result.actualBufferSize;
    sampleRateHz = result.sampleRate;
    audioOpened = true;
}

void EngineCore::prepare(double sampleRate, int)
{
    sampleRateHz = (sampleRate > 0.0) ? sampleRate : 48000.0;
    deckPhases[ngks::DECK_A] = 0.0f;
    deckPhases[ngks::DECK_B] = 0.0f;
    deckPhaseIncrements[ngks::DECK_A] = (twoPi * 220.0f) / static_cast<float>(sampleRateHz);
    deckPhaseIncrements[ngks::DECK_B] = (twoPi * 330.0f) / static_cast<float>(sampleRateHz);
    fadeSamplesTotal = static_cast<int>(sampleRateHz * 0.2);
    if (fadeSamplesTotal < 1) {
        fadeSamplesTotal = 1;
    }
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
        deckFadeSamplesRemaining[command.deck] = 0;
        break;
    case ngks::CommandType::Play:
        if (deck.hasTrack) {
            startAudioIfNeeded();
            deck.transport = ngks::TransportState::Starting;
            deckFadeSamplesRemaining[command.deck] = 0;
        }
        break;
    case ngks::CommandType::Stop:
        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Stopping;
            deckFadeSamplesRemaining[command.deck] = fadeSamplesTotal;
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

    float deckSumSquares[ngks::MAX_DECKS] { 0.0f, 0.0f };
    float deckPeaks[ngks::MAX_DECKS] { 0.0f, 0.0f };
    float masterSumSquares = 0.0f;

    for (int sample = 0; sample < numSamples; ++sample) {
        float mix = 0.0f;

        for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
            auto& deck = state.decks[deckIndex];
            float deckEnvelope = 0.0f;

            if (deck.transport == ngks::TransportState::Starting) {
                deck.transport = ngks::TransportState::Playing;
                deckEnvelope = 1.0f;
            } else if (deck.transport == ngks::TransportState::Playing) {
                deckEnvelope = 1.0f;
            } else if (deck.transport == ngks::TransportState::Stopping && deckFadeSamplesRemaining[deckIndex] > 0) {
                deckEnvelope = static_cast<float>(deckFadeSamplesRemaining[deckIndex]) / static_cast<float>(fadeSamplesTotal);
                --deckFadeSamplesRemaining[deckIndex];
                if (deckFadeSamplesRemaining[deckIndex] <= 0) {
                    deck.transport = ngks::TransportState::Stopped;
                }
            }

            float deckSample = 0.0f;
            if (deck.hasTrack && deckEnvelope > 0.0f) {
                deckSample = std::sin(deckPhases[deckIndex]) * 0.1f * deck.deckGain * deckEnvelope;
                deckPhases[deckIndex] += deckPhaseIncrements[deckIndex];
                if (deckPhases[deckIndex] >= twoPi) {
                    deckPhases[deckIndex] -= twoPi;
                }

                deck.playheadSeconds += (1.0 / sampleRateHz);
                if (deck.lengthSeconds > 0.0 && deck.playheadSeconds > deck.lengthSeconds) {
                    deck.playheadSeconds = deck.lengthSeconds;
                }
            }

            mix += deckSample;
            deckSumSquares[deckIndex] += deckSample * deckSample;
            deckPeaks[deckIndex] = std::max(deckPeaks[deckIndex], std::abs(deckSample));
        }

        const float limited = limiter.processSample(mix * static_cast<float>(state.masterGain));
        left[sample] = limited;
        right[sample] = limited;
        masterSumSquares += limited * limited;
    }

    state.masterRmsL = std::sqrt(masterSumSquares / static_cast<float>(numSamples));
    state.masterRmsR = state.masterRmsL;

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        auto& deck = state.decks[deckIndex];
        deck.rmsL = std::sqrt(deckSumSquares[deckIndex] / static_cast<float>(numSamples));
        deck.rmsR = deck.rmsL;
        deck.peakL = deckPeaks[deckIndex];
        deck.peakR = deckPeaks[deckIndex];

        const bool isPlaying = (deck.transport == ngks::TransportState::Playing);
        const bool audible = std::max(deck.rmsL, deck.rmsR) > audibleRmsThresholdLinear;
        deck.publicFacing = isPlaying && audible;
    }
}