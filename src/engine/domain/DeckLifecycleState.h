#pragma once
#include <cstdint>

enum class DeckLifecycleState : uint8_t {
    Empty = 0,
    Loading,
    Loaded,
    Analyzed,
    Armed,
    Playing,
    Stopped
};
