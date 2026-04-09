#pragma once
#include <cstdint>

namespace ngks {

using DeckId = uint8_t;

constexpr DeckId DECK_A = 0;
constexpr DeckId DECK_B = 1;
constexpr DeckId DECK_C = 2;
constexpr DeckId DECK_D = 3;
constexpr DeckId DECK_S = 4;   // Simple Player (isolated from DJ decks)
constexpr uint8_t MAX_DECKS = 5;

}