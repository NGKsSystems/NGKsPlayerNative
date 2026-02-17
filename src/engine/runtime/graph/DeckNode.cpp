#include "engine/runtime/graph/DeckNode.h"

#include <algorithm>
#include <cmath>

namespace
{
constexpr float twoPi = 6.28318530717958647692f;
}

namespace ngks {

void DeckNode::prepare(double sampleRate)
{
    phase = 0.0f;
    const auto sampleRateSafe = static_cast<float>((sampleRate > 0.0) ? sampleRate : 48000.0);
    phaseIncrement = (twoPi * frequencyHz) / sampleRateSafe;
    stopFadeSamplesRemaining = 0;
    stopFadeSamplesTotal = std::max(1, static_cast<int>(sampleRateSafe * 0.2f));
}

void DeckNode::setFrequency(float hz) noexcept
{
    frequencyHz = std::max(hz, 10.0f);
}

void DeckNode::beginStopFade(int fadeSamples) noexcept
{
    stopFadeSamplesTotal = std::max(1, fadeSamples);
    stopFadeSamplesRemaining = stopFadeSamplesTotal;
}

bool DeckNode::isStopFadeActive() const noexcept
{
    return stopFadeSamplesRemaining > 0;
}

void DeckNode::render(const DeckState& deck,
                      int numSamples,
                      float* outLeft,
                      float* outRight,
                      float& outRms,
                      float& outPeak) noexcept
{
    outRms = 0.0f;
    outPeak = 0.0f;

    if (numSamples <= 0 || outLeft == nullptr || outRight == nullptr) {
        return;
    }

    float sumSquares = 0.0f;

    for (int sample = 0; sample < numSamples; ++sample) {
        float envelope = 0.0f;
        if (deck.transport == TransportState::Playing || deck.transport == TransportState::Starting) {
            envelope = 1.0f;
        } else if (deck.transport == TransportState::Stopping && stopFadeSamplesRemaining > 0) {
            envelope = static_cast<float>(stopFadeSamplesRemaining) / static_cast<float>(stopFadeSamplesTotal);
            --stopFadeSamplesRemaining;
        }

        float value = 0.0f;
        if (deck.hasTrack && envelope > 0.0f) {
            value = std::sin(phase) * 0.1f * deck.deckGain * envelope;
            phase += phaseIncrement;
            if (phase >= twoPi) {
                phase -= twoPi;
            }
        }

        outLeft[sample] = value;
        outRight[sample] = value;
        sumSquares += value * value;
        outPeak = std::max(outPeak, std::abs(value));
    }

    outRms = std::sqrt(sumSquares / static_cast<float>(numSamples));
}

}