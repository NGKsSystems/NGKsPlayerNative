#pragma once
#include <cstdint>
#include "../domain/DeckId.h"

namespace ngks {

enum class CommandType {
    LoadTrack,
    UnloadTrack,
    Play,
    Stop,
    SetDeckGain,
    SetMasterGain,
    SetCue
};

struct Command {
    CommandType type;
    DeckId deck{0};
    uint32_t seq{0};
    uint64_t trackUidHash{0};
    float floatValue{0.0f};
    uint8_t boolValue{0};
};

}