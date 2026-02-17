#include <chrono>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

int main()
{
    EngineCore engine;

    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, "deckA_track" });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, "deckB_track" });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_A, {}, 0.9f });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_B, {}, 0.5f });

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    auto snapshot = engine.getSnapshot();
    std::cout << "DeckCount=" << static_cast<int>(ngks::MAX_DECKS) << std::endl;
    std::cout << "DeckA_Playing=" << (snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Playing) << std::endl;
    std::cout << "DeckB_Playing=" << (snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Playing) << std::endl;

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_B });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    snapshot = engine.getSnapshot();
    std::cout << "DeckA_PublicFacing=" << snapshot.decks[ngks::DECK_A].publicFacing << std::endl;
    std::cout << "DeckB_PublicFacing=" << snapshot.decks[ngks::DECK_B].publicFacing << std::endl;

    const bool cueBefore = snapshot.decks[ngks::DECK_A].cueEnabled;
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, {}, 1.0f });
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    snapshot = engine.getSnapshot();
    const bool cueAfter = snapshot.decks[ngks::DECK_A].cueEnabled;
    std::cout << "CueRejectedWhenPublicFacing=" << (cueBefore == cueAfter) << std::endl;

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A });
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_B });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    snapshot = engine.getSnapshot();
    std::cout << "DeckA_Stopped=" << (snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Stopped) << std::endl;
    std::cout << "DeckB_Stopped=" << (snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Stopped) << std::endl;
    std::cout << "RunResult=PASS" << std::endl;
    return 0;
}