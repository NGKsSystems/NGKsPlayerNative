#pragma once
#include <string>
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
    std::string stringValue{};
    float floatValue{0.0f};
};

}