#pragma once
#include "DeckState.h"

namespace ngks {

struct EngineState {
    double masterGain{1.0};
    float masterRmsL{0.0f};
    float masterRmsR{0.0f};

    DeckState decks[MAX_DECKS];
};

}