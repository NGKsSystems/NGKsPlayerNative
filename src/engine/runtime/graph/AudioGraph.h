#pragma once

#include <array>

#include "engine/dsp/ParametricEQ16.h"
#include "engine/runtime/EngineSnapshot.h"
#include "engine/runtime/fx/FxChain.h"
#include "engine/runtime/MixMatrix.h"
#include "engine/runtime/graph/CueMixNode.h"
#include "engine/runtime/graph/DeckNode.h"
#include "engine/runtime/graph/MasterMixNode.h"

namespace ngks {

struct GraphDeckStats {
    float rms = 0.0f;
    float peak = 0.0f;
    float peakL = 0.0f;
    float peakR = 0.0f;
};

struct GraphRenderStats {
    std::array<GraphDeckStats, MAX_DECKS> decks {};
    const float* cueBusL{nullptr};
    const float* cueBusR{nullptr};
    int cueBusSamples{0};
};

class AudioGraph {
public:
    void prepare(double sampleRate, int maxBlockSize);
    void beginDeckStopFade(DeckId deckId, int fadeSamples);
    bool isDeckStopFadeActive(DeckId deckId) const noexcept;

    // Access deck node for file load/unload/seek (called from UI thread)
    DeckNode& getDeckNode(DeckId deckId) noexcept;
    const DeckNode& getDeckNode(DeckId deckId) const noexcept;

    bool setDeckFxSlotType(DeckId deckId, int slotIndex, uint32_t fxType) noexcept;
    bool setDeckFxSlotEnabled(DeckId deckId, int slotIndex, bool enabled) noexcept;
    bool setDeckFxSlotDryWet(DeckId deckId, int slotIndex, float dryWet) noexcept;
    bool setDeckFxGain(DeckId deckId, int slotIndex, float gainLinear) noexcept;
    bool setMasterFxSlotEnabled(int slotIndex, bool enabled) noexcept;
    bool setMasterFxGain(int slotIndex, float gainLinear) noexcept;
    bool isDeckFxSlotEnabled(DeckId deckId, int slotIndex) const noexcept;
    FxSlotState getDeckFxSlotState(DeckId deckId, int slotIndex) const noexcept;
    bool isMasterFxSlotEnabled(int slotIndex) const noexcept;

    // 16-band Parametric EQ per deck
    bool setEqBandGain(DeckId deckId, int band, float gainDb) noexcept;
    float getEqBandGain(DeckId deckId, int band) const noexcept;
    void setEqBypass(DeckId deckId, bool bypassed) noexcept;
    bool isEqBypassed(DeckId deckId) const noexcept;
    void resetEq(DeckId deckId) noexcept;

    GraphRenderStats render(const EngineSnapshot& state,
                            const MixMatrix& mixMatrix,
                            int numSamples,
                            float* outLeft,
                            float* outRight) noexcept;

private:
    static constexpr int maxGraphBlock = 2048;

    std::array<DeckNode, MAX_DECKS> deckNodes {};
    std::array<FxChain, MAX_DECKS> deckFxChains {};
    std::array<ParametricEQ16, MAX_DECKS> deckEqs {};
    FxChain masterFxChain;
    MasterMixNode masterMixNode;
    CueMixNode cueMixNode;

    std::array<std::array<float, maxGraphBlock>, MAX_DECKS> deckBufferL {};
    std::array<std::array<float, maxGraphBlock>, MAX_DECKS> deckBufferR {};
    std::array<float, maxGraphBlock> masterBusL {};
    std::array<float, maxGraphBlock> masterBusR {};
    std::array<float, maxGraphBlock> cueBusL {};
    std::array<float, maxGraphBlock> cueBusR {};
};

}