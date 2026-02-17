#pragma once

#include <cstdint>

#include "engine/domain/DeckId.h"
#include "engine/domain/TransportState.h"

namespace ngks {

enum class CommandResult : uint8_t {
    None = 0,
    Applied = 1,
    RejectedPublicFacing = 2,
    RejectedNoTrack = 3,
    RejectedInvalidDeck = 4,
    RejectedQueueFull = 5
};

struct DeckSnapshot {
    DeckId id{};
    uint8_t hasTrack{0};
    uint64_t trackUidHash{0};

    TransportState transport{TransportState::Stopped};

    double playheadSeconds{0.0};
    double lengthSeconds{0.0};

    float deckGain{1.0f};
    float rmsL{0.0f};
    float rmsR{0.0f};
    float peakL{0.0f};
    float peakR{0.0f};

    uint8_t cueEnabled{0};
    uint8_t publicFacing{0};
};

struct EngineSnapshot {
    double masterGain{1.0};
    float masterRmsL{0.0f};
    float masterRmsR{0.0f};
    float masterPeakL{0.0f};
    float masterPeakR{0.0f};

    DeckSnapshot decks[MAX_DECKS] {};

    uint32_t lastProcessedCommandSeq{0};
    CommandResult lastCommandResult[MAX_DECKS] {
        CommandResult::None,
        CommandResult::None
    };
};

}