#include <chrono>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

int main()
{
    EngineCore engine;
    bool pass = true;

    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, "deckA_track" });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, "deckB_track" });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_A, {}, 0.9f });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_B, {}, 0.5f });

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    auto snapshot = engine.getSnapshot();
    std::cout << "DeckCount=" << static_cast<int>(ngks::MAX_DECKS) << std::endl;
    const bool deckAPlaying = snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Playing;
    const bool deckBStopped = snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Stopped;
    std::cout << "DeckA_Playing=" << deckAPlaying << std::endl;
    std::cout << "DeckB_StoppedBeforePlay=" << deckBStopped << std::endl;
    pass = pass && deckAPlaying && deckBStopped;

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_B });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    snapshot = engine.getSnapshot();
    const bool deckAPublicFacing = snapshot.decks[ngks::DECK_A].publicFacing;
    const bool deckBPublicFacing = snapshot.decks[ngks::DECK_B].publicFacing;
    const bool deckBActive = snapshot.decks[ngks::DECK_B].rmsL > 0.0f;
    std::cout << "DeckA_PublicFacing=" << deckAPublicFacing << std::endl;
    std::cout << "DeckB_PublicFacing=" << deckBPublicFacing << std::endl;
    std::cout << "DeckB_ActiveRms=" << deckBActive << std::endl;
    pass = pass && deckAPublicFacing && deckBPublicFacing && deckBActive;

    const bool cueBefore = snapshot.decks[ngks::DECK_A].cueEnabled;
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, {}, 1.0f });
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    snapshot = engine.getSnapshot();
    const bool cueAfter = snapshot.decks[ngks::DECK_A].cueEnabled;
    const bool cueRejected = (cueBefore == cueAfter);
    std::cout << "CueRejectedWhenPublicFacing=" << cueRejected << std::endl;
    pass = pass && cueRejected;

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A });
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_B });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    snapshot = engine.getSnapshot();
    const bool deckAStopped = snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Stopped;
    const bool deckBStoppedNow = snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Stopped;
    std::cout << "DeckA_Stopped=" << deckAStopped << std::endl;
    std::cout << "DeckB_Stopped=" << deckBStoppedNow << std::endl;
    pass = pass && deckAStopped && deckBStoppedNow;

    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}