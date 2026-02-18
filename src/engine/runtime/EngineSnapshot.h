#pragma once

#include <cstdint>

#include "engine/domain/DeckId.h"
#include "engine/domain/TransportState.h"
#include "engine/runtime/jobs/JobResult.h"

namespace ngks {

constexpr uint32_t SNAP_AUDIO_RUNNING = 1u << 0;
constexpr uint32_t SNAP_WARMUP_COMPLETE = 1u << 1;

enum class CommandResult : uint8_t {
    None = 0,
    Applied = 1,
    RejectedPublicFacing = 2,
    RejectedNoTrack = 3,
    RejectedInvalidDeck = 4,
    RejectedQueueFull = 5,
    RejectedInvalidSlot = 6
};

struct DeckSnapshot {
    DeckId id{};
    uint8_t hasTrack{0};
    uint64_t trackUidHash{0};
    uint64_t currentTrackId{0};
    char currentTrackLabel[64]{};
    int32_t cachedBpmFixed{0};
    int32_t cachedLoudnessCentiDb{0};
    uint32_t cachedDeadAirMs{0};
    uint8_t cachedStemsReady{0};
    uint32_t cachedAnalysisStatus{0};

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
    uint8_t fxSlotEnabled[8]{};
};

struct EngineSnapshot {
    static constexpr int kMaxJobResults = 16;

    uint32_t flags{0};
    uint32_t warmupCounter{0};

    double masterGain{1.0};
    float masterRmsL{0.0f};
    float masterRmsR{0.0f};
    float masterPeakL{0.0f};
    float masterPeakR{0.0f};

    DeckSnapshot decks[MAX_DECKS] {};
    uint8_t masterFxSlotEnabled[8]{};
    JobResult jobResults[kMaxJobResults] {};
    uint32_t jobResultsWriteSeq{0};

    uint32_t lastProcessedCommandSeq{0};
    CommandResult lastCommandResult[MAX_DECKS] {
        CommandResult::None,
        CommandResult::None
    };
};

}