#pragma once

#include <array>

#include "engine/runtime/EngineSnapshot.h"
#include "engine/runtime/fx/FxChain.h"
#include "engine/runtime/MixMatrix.h"
#include "engine/runtime/graph/CueMixNode.h"
#include "engine/runtime/graph/DeckNode.h"
#include "engine/runtime/graph/MasterMixNode.h"
#include "engine/runtime/graph/OutputNode.h"

namespace ngks {

struct GraphDeckStats {
    float rms = 0.0f;
    float peak = 0.0f;
};

struct GraphRenderStats {
    std::array<GraphDeckStats, MAX_DECKS> decks {};
    float masterRms = 0.0f;
};

class AudioGraph {
public:
    void prepare(double sampleRate, int maxBlockSize);
    void beginDeckStopFade(DeckId deckId, int fadeSamples);
    bool isDeckStopFadeActive(DeckId deckId) const noexcept;
    bool setDeckFxSlotEnabled(DeckId deckId, int slotIndex, bool enabled) noexcept;
    bool setDeckFxGain(DeckId deckId, int slotIndex, float gainLinear) noexcept;
    bool setMasterFxSlotEnabled(int slotIndex, bool enabled) noexcept;
    bool setMasterFxGain(int slotIndex, float gainLinear) noexcept;
    bool isDeckFxSlotEnabled(DeckId deckId, int slotIndex) const noexcept;
    bool isMasterFxSlotEnabled(int slotIndex) const noexcept;

    GraphRenderStats render(const EngineSnapshot& state,
                            const MixMatrix& mixMatrix,
                            int numSamples,
                            float* outLeft,
                            float* outRight) noexcept;

private:
    static constexpr int maxGraphBlock = 2048;

    std::array<DeckNode, MAX_DECKS> deckNodes {};
    std::array<FxChain, MAX_DECKS> deckFxChains {};
    FxChain masterFxChain;
    MasterMixNode masterMixNode;
    CueMixNode cueMixNode;
    OutputNode outputNode;

    std::array<std::array<float, maxGraphBlock>, MAX_DECKS> deckBufferL {};
    std::array<std::array<float, maxGraphBlock>, MAX_DECKS> deckBufferR {};
    std::array<float, maxGraphBlock> masterBusL {};
    std::array<float, maxGraphBlock> masterBusR {};
    std::array<float, maxGraphBlock> cueBusL {};
    std::array<float, maxGraphBlock> cueBusR {};
};

}