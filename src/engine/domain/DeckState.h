#pragma once
#include <cstdint>
#include "DeckId.h"
#include "TransportState.h"

namespace ngks {

struct DeckState {
    DeckId id{};
    bool hasTrack{false};
    uint64_t trackId{0};
    char trackLabel[64]{};

    TransportState transport{TransportState::Stopped};

    double playheadSeconds{0.0};
    double lengthSeconds{0.0};

    float deckGain{1.0f};
    float rmsL{0.0f};
    float rmsR{0.0f};
    float peakL{0.0f};
    float peakR{0.0f};

    bool cueEnabled{false};
    bool publicFacing{false};
};

}