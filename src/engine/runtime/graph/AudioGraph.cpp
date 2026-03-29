#include "engine/runtime/graph/AudioGraph.h"
#include "engine/DiagLog.h"

#include <algorithm>
#include <cmath>

namespace ngks {

void AudioGraph::prepare(double sampleRate, int)
{
    for (auto& node : deckNodes) {
        node.prepare(sampleRate);
    }
    for (auto& eq : deckEqs) {
        eq.prepare(sampleRate);
    }
    diagLog("[AudioGraph] ParametricEQ16 ready  bands=%d  decks=%d  sr=%.0f",
            ParametricEQ16::kBandCount, static_cast<int>(MAX_DECKS), sampleRate);
}

DeckNode& AudioGraph::getDeckNode(DeckId deckId) noexcept
{
    return deckNodes[std::min(static_cast<uint8_t>(deckId), static_cast<uint8_t>(MAX_DECKS - 1))];
}

const DeckNode& AudioGraph::getDeckNode(DeckId deckId) const noexcept
{
    return deckNodes[std::min(static_cast<uint8_t>(deckId), static_cast<uint8_t>(MAX_DECKS - 1))];
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

bool AudioGraph::setDeckFxSlotEnabled(DeckId deckId, int slotIndex, bool enabled) noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckFxChains[deckId].setSlotEnabled(slotIndex, enabled);
}

bool AudioGraph::setDeckFxSlotType(DeckId deckId, int slotIndex, uint32_t fxType) noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckFxChains[deckId].setSlotType(slotIndex, fxType);
}

bool AudioGraph::setDeckFxSlotDryWet(DeckId deckId, int slotIndex, float dryWet) noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckFxChains[deckId].setSlotDryWet(slotIndex, dryWet);
}

bool AudioGraph::setDeckFxGain(DeckId deckId, int slotIndex, float gainLinear) noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckFxChains[deckId].setSlotParam0(slotIndex, gainLinear);
}

bool AudioGraph::setMasterFxSlotEnabled(int slotIndex, bool enabled) noexcept
{
    return masterFxChain.setSlotEnabled(slotIndex, enabled);
}

bool AudioGraph::setMasterFxGain(int slotIndex, float gainLinear) noexcept
{
    return masterFxChain.setSlotParam0(slotIndex, gainLinear);
}

bool AudioGraph::isDeckFxSlotEnabled(DeckId deckId, int slotIndex) const noexcept
{
    if (deckId >= MAX_DECKS) {
        return false;
    }

    return deckFxChains[deckId].isSlotEnabled(slotIndex);
}

FxSlotState AudioGraph::getDeckFxSlotState(DeckId deckId, int slotIndex) const noexcept
{
    if (deckId >= MAX_DECKS) {
        return {};
    }

    return deckFxChains[deckId].getSlotState(slotIndex);
}

bool AudioGraph::isMasterFxSlotEnabled(int slotIndex) const noexcept
{
    return masterFxChain.isSlotEnabled(slotIndex);
}

// ── 16-band Parametric EQ per deck ──

bool AudioGraph::setEqBandGain(DeckId deckId, int band, float gainDb) noexcept
{
    if (deckId >= MAX_DECKS || band < 0 || band >= ParametricEQ16::kBandCount)
        return false;
    deckEqs[deckId].setBandGain(band, gainDb);
    return true;
}

float AudioGraph::getEqBandGain(DeckId deckId, int band) const noexcept
{
    if (deckId >= MAX_DECKS || band < 0 || band >= ParametricEQ16::kBandCount)
        return 0.0f;
    return deckEqs[deckId].getBandGain(band);
}

void AudioGraph::setEqBypass(DeckId deckId, bool bypassed) noexcept
{
    if (deckId < MAX_DECKS)
        deckEqs[deckId].setBypass(bypassed);
}

bool AudioGraph::isEqBypassed(DeckId deckId) const noexcept
{
    if (deckId >= MAX_DECKS) return true;
    return deckEqs[deckId].isBypassed();
}

void AudioGraph::resetEq(DeckId deckId) noexcept
{
    if (deckId < MAX_DECKS)
        deckEqs[deckId].reset();
}

GraphRenderStats AudioGraph::render(const EngineSnapshot& state,
                                    const MixMatrix& mixMatrix,
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

        // 16-band parametric EQ (after decode, before FX chain)
        deckEqs[deckIndex].process(deckBufferL[deckIndex].data(),
                                   deckBufferR[deckIndex].data(),
                                   safeSamples);

        deckFxChains[deckIndex].process(deckBufferL[deckIndex].data(),
                                        deckBufferR[deckIndex].data(),
                                        safeSamples);

        float postFxSumSquares = 0.0f;
        float postFxPeakL = 0.0f;
        float postFxPeakR = 0.0f;
        for (int sample = 0; sample < safeSamples; ++sample) {
            const float sL = deckBufferL[deckIndex][sample];
            const float sR = deckBufferR[deckIndex][sample];
            const float mono = 0.5f * (sL + sR);
            postFxSumSquares += mono * mono;
            postFxPeakL = std::max(postFxPeakL, std::abs(sL));
            postFxPeakR = std::max(postFxPeakR, std::abs(sR));
        }

        rms = std::sqrt(postFxSumSquares / static_cast<float>(safeSamples));

        stats.decks[deckIndex].rms = rms;
        stats.decks[deckIndex].peakL = postFxPeakL;
        stats.decks[deckIndex].peakR = postFxPeakR;
        stats.decks[deckIndex].peak = std::max(postFxPeakL, postFxPeakR);

        const float masterWeight = mixMatrix.decks[deckIndex].masterWeight;
        const float cueWeight = mixMatrix.decks[deckIndex].cueWeight;
        for (int sample = 0; sample < safeSamples; ++sample) {
            masterBusL[sample] += deckBufferL[deckIndex][sample] * masterWeight;
            masterBusR[sample] += deckBufferR[deckIndex][sample] * masterWeight;
            cueBusL[sample] += deckBufferL[deckIndex][sample] * cueWeight;
            cueBusR[sample] += deckBufferR[deckIndex][sample] * cueWeight;
        }
    }

    masterFxChain.process(masterBusL.data(), masterBusR.data(), safeSamples);

    stats.cueBusL = cueBusL.data();
    stats.cueBusR = cueBusR.data();
    stats.cueBusSamples = safeSamples;

    for (int sample = 0; sample < safeSamples; ++sample) {
        outLeft[sample] = masterBusL[sample];
        outRight[sample] = masterBusR[sample];
    }

    if (safeSamples < numSamples) {
        for (int sample = safeSamples; sample < numSamples; ++sample) {
            outLeft[sample] = 0.0f;
            outRight[sample] = 0.0f;
        }
    }

    return stats;
}

}