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

    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, seq++, 1111ULL, 0.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, seq++, 2222ULL, 0.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_A, seq++, 0, 0.9f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_B, seq++, 0, 0.5f, 0, 0 });

    const uint32_t seqPlayA = seq;
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });

    bool warmupComplete = false;
    ngks::EngineSnapshot warmupSnapshot = engine.getSnapshot();
    for (int attempt = 0; attempt < 300; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        warmupSnapshot = engine.getSnapshot();
        if ((warmupSnapshot.flags & ngks::SNAP_WARMUP_COMPLETE) != 0u) {
            warmupComplete = true;
            break;
        }
    }

    if (!warmupComplete) {
        const bool audioRunning = (warmupSnapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
        std::cout << "WarmupTimeout: audioRunning=" << (audioRunning ? 1 : 0)
                  << " masterRms=" << warmupSnapshot.masterRmsL
                  << " warmupCounter=" << warmupSnapshot.warmupCounter
                  << std::endl;
        std::cout << "RunResult=FAIL" << std::endl;
        return 1;
    }

    auto snapshot = engine.getSnapshot();
    std::cout << "DeckCount=" << static_cast<int>(ngks::MAX_DECKS) << std::endl;
    const bool deckAPlaying = snapshot.decks[ngks::DECK_A].transport == ngks::TransportState::Playing;
    const bool deckBStopped = snapshot.decks[ngks::DECK_B].transport == ngks::TransportState::Stopped;
    std::cout << "DeckA_Playing=" << deckAPlaying << std::endl;
    std::cout << "DeckB_StoppedBeforePlay=" << deckBStopped << std::endl;
    std::cout << "LastProcessedSeq_AfterPlayA=" << snapshot.lastProcessedCommandSeq << std::endl;
    pass = pass && deckAPlaying && deckBStopped;
    pass = pass && snapshot.lastProcessedCommandSeq >= seqPlayA;

    float baselineAccum = 0.0f;
    for (int i = 0; i < 6; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        baselineAccum += engine.getSnapshot().masterRmsL;
    }
    const float baselineMasterRms = baselineAccum / 6.0f;
    std::cout << "BaselineMasterRms=" << baselineMasterRms << std::endl;

    const uint32_t seqDeckFxGain = seq;
    engine.enqueueCommand({ ngks::CommandType::SetDeckFxGain, ngks::DECK_A, seq++, 0, 0.5f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::EnableDeckFxSlot, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    float postFxAccum = 0.0f;
    for (int i = 0; i < 6; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        postFxAccum += engine.getSnapshot().masterRmsL;
    }
    const float postFxMasterRms = postFxAccum / 6.0f;
    const bool fxReducedRms = postFxMasterRms < (baselineMasterRms * 0.75f);
    std::cout << "PostFxMasterRms=" << postFxMasterRms << std::endl;
    std::cout << "FxReducedMasterRms=" << fxReducedRms << std::endl;
    pass = pass && fxReducedRms;

    snapshot = engine.getSnapshot();
    pass = pass && snapshot.lastProcessedCommandSeq >= seqDeckFxGain;

    engine.enqueueCommand({ ngks::CommandType::EnableDeckFxSlot, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const uint32_t seqPlayB = seq;
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_B, seq++, 0, 0.0f, 0, 0 });
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
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });
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

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_B, seq++, 0, 0.0f, 0, 0 });
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