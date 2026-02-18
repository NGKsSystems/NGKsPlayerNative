#pragma once

constexpr int MAX_DECKS = 4;

struct DeckMixWeights {
    float masterWeight;
    float cueWeight;
};

struct MixMatrix {
    DeckMixWeights decks[4];
};
