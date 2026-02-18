#pragma once
#include <cstdint>

struct DeckAuthorityState {
    uint64_t lastAcceptedSeq{0};
    bool commandInFlight{false};
    bool locked{false};
};
