#include "engine/runtime/graph/AudioGraph.h"

#include <algorithm>
#include <cmath>

namespace ngks {

void AudioGraph::prepare(double sampleRate, int)
{
    deckNodes[DECK_A].setFrequency(220.0f);
    deckNodes[DECK_B].setFrequency(330.0f);

    for (auto& node : deckNodes) {
        node.prepare(sampleRate);
    }
}

void AudioGraph::beginDeckStopFade(DeckId deckId, int fadeSamples)
{
    if (deckId >= MAX_DECKS) {
        return;
    }

    deckNodes[deckId].beginStopFade(fadeSamples);
}

bool AudioGraph::isDeckStopFadeActive(DeckId deckId) const noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckNodes[deckId].isStopFadeActive();
}

GraphRenderStats AudioGraph::render(const EngineSnapshot& state,
                                    const RoutingMatrix& routing,
                                    int numSamples,
                                    float* outLeft,
                                    float* outRight) noexcept
{
    GraphRenderStats stats;

    if (numSamples <= 0 || outLeft == nullptr || outRight == nullptr) {
        return stats;
    }

    const int safeSamples = std::min(numSamples, maxGraphBlock);

    masterMixNode.clear(masterBusL.data(), masterBusR.data(), safeSamples);
    cueMixNode.clear(cueBusL.data(), cueBusR.data(), safeSamples);

    for (uint8_t deckIndex = 0; deckIndex < MAX_DECKS; ++deckIndex) {
        float rms = 0.0f;
        float peak = 0.0f;

        deckNodes[deckIndex].render(state.decks[deckIndex],
                                    safeSamples,
                                    deckBufferL[deckIndex].data(),
                                    deckBufferR[deckIndex].data(),
                                    rms,
                                    peak);

        stats.decks[deckIndex].rms = rms;
        stats.decks[deckIndex].peak = peak;

        const auto route = routing.get(deckIndex);
        masterMixNode.accumulate(deckBufferL[deckIndex].data(),
                                 deckBufferR[deckIndex].data(),
                                 masterBusL.data(),
                                 masterBusR.data(),
                                 safeSamples,
                                 route.toMasterWeight);

        cueMixNode.accumulate(deckBufferL[deckIndex].data(),
                              deckBufferR[deckIndex].data(),
                              cueBusL.data(),
                              cueBusR.data(),
                              safeSamples,
                              route.toCueWeight);
    }

    float sumSquares = 0.0f;
    for (int sample = 0; sample < safeSamples; ++sample) {
        const float mono = 0.5f * (masterBusL[sample] + masterBusR[sample]);
        sumSquares += mono * mono;
    }

    stats.masterRms = std::sqrt(sumSquares / static_cast<float>(safeSamples));

    outputNode.renderToDevice(masterBusL.data(),
                              masterBusR.data(),
                              safeSamples,
                              static_cast<float>(state.masterGain),
                              outLeft,
                              outRight);

    if (safeSamples < numSamples) {
        for (int sample = safeSamples; sample < numSamples; ++sample) {
            outLeft[sample] = 0.0f;
            outRight[sample] = 0.0f;
        }
    }

    return stats;
}

}