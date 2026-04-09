#pragma once
#include "engine/domain/DeckId.h"

struct DeckMixWeights {
    float masterWeight;
    float cueWeight;
};

struct MixMatrix {
    DeckMixWeights decks[ngks::MAX_DECKS];
};
