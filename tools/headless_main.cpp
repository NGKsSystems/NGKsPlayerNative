#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

int main()
{
    EngineCore engine;
    bool pass = true;
    uint32_t seq = 1;

    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, seq++, 1111ULL, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, seq++, 2222ULL, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_A, seq++, 0, 0.9f, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_B, seq++, 0, 0.5f, 0 });

    const uint32_t seqPlayA = seq;
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    auto snapshot = engine.getSnapshot();
    std::cout << "DeckCount=" << static_cast<int>(ngks::MAX_DECKS) << std::endl;
    const bool deckAPlaying = snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Playing;
    const bool deckBStopped = snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Stopped;
    std::cout << "DeckA_Playing=" << deckAPlaying << std::endl;
    std::cout << "DeckB_StoppedBeforePlay=" << deckBStopped << std::endl;
    std::cout << "LastProcessedSeq_AfterPlayA=" << snapshot.lastProcessedCommandSeq << std::endl;
    pass = pass && deckAPlaying && deckBStopped;
    pass = pass && snapshot.lastProcessedCommandSeq >= seqPlayA;

    const uint32_t seqPlayB = seq;
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_B, seq++, 0, 0.0f, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));

    snapshot = engine.getSnapshot();
    const bool deckAPublicFacing = snapshot.decks[ngks::DECK_A].publicFacing;
    const bool deckBPublicFacing = snapshot.decks[ngks::DECK_B].publicFacing;
    const bool deckBActive = snapshot.decks[ngks::DECK_B].rmsL > 0.0f;
    std::cout << "DeckA_PublicFacing=" << deckAPublicFacing << std::endl;
    std::cout << "DeckB_PublicFacing=" << deckBPublicFacing << std::endl;
    std::cout << "DeckB_ActiveRms=" << deckBActive << std::endl;
    pass = pass && deckAPublicFacing && deckBPublicFacing && deckBActive;
    pass = pass && snapshot.lastProcessedCommandSeq >= seqPlayB;

    const uint32_t seqCueA = seq;
    const bool cueBefore = snapshot.decks[ngks::DECK_A].cueEnabled != 0;
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, seq++, 0, 0.0f, 1 });
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    snapshot = engine.getSnapshot();
    const bool cueAfter = snapshot.decks[ngks::DECK_A].cueEnabled != 0;
    const bool cueRejected = (cueBefore == cueAfter);
    const bool cueRejectedByResult = snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::RejectedPublicFacing;
    std::cout << "CueRejectedWhenPublicFacing=" << cueRejected << std::endl;
    std::cout << "CueRejectedByResultCode=" << cueRejectedByResult << std::endl;
    std::cout << "LastProcessedSeq_AfterCue=" << snapshot.lastProcessedCommandSeq << std::endl;
    pass = pass && cueRejected && cueRejectedByResult;
    pass = pass && snapshot.lastProcessedCommandSeq >= seqCueA;

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq++, 0, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_B, seq++, 0, 0.0f, 0 });
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