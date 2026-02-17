#pragma once

#include <array>
#include <algorithm>

#include "engine/domain/DeckId.h"

namespace ngks {

struct DeckRouting {
    float toMasterWeight { 1.0f };
    float toCueWeight { 0.0f };
};

class RoutingMatrix {
public:
    RoutingMatrix()
    {
        for (auto& route : routes) {
            route.toMasterWeight = 1.0f;
            route.toCueWeight = 0.0f;
        }
    }

    const DeckRouting& get(DeckId deckId) const noexcept
    {
        return routes[deckId];
    }

    void setMasterWeight(DeckId deckId, float value) noexcept
    {
        routes[deckId].toMasterWeight = std::clamp(value, 0.0f, 1.0f);
    }

    void setCueWeight(DeckId deckId, float value) noexcept
    {
        routes[deckId].toCueWeight = std::clamp(value, 0.0f, 1.0f);
    }

private:
    std::array<DeckRouting, MAX_DECKS> routes {};
};

}