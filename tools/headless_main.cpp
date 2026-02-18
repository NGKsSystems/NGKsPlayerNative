#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

namespace {

constexpr uint64_t kTrackA = 1001ULL;
constexpr uint64_t kTrackB = 1002ULL;
constexpr int kPolls = 300;
constexpr int kPollSleepMs = 10;

void sendSetDeckTrack(EngineCore& engine, ngks::DeckId deck, uint32_t seq, uint64_t trackId, const char* label)
{
    ngks::Command command {};
    command.type = ngks::CommandType::SetDeckTrack;
    command.deck = deck;
    command.seq = seq;
    command.trackUidHash = trackId;
    std::memcpy(command.trackLabel, label, std::strlen(label));
    engine.enqueueCommand(command);
}

bool waitAnalyzeDone(EngineCore& engine, uint32_t jobId)
{
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        for (int slot = 0; slot < ngks::EngineSnapshot::kMaxJobResults; ++slot) {
            const auto& result = snapshot.jobResults[slot];
            if (result.jobId == jobId && result.type == ngks::JobType::AnalyzeTrack && result.status == ngks::JobStatus::Complete) {
                return true;
            }
        }
    }
    return false;
}

bool waitDeckPlaying(EngineCore& engine, ngks::DeckId deck)
{
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deck].lifecycle == DeckLifecycleState::Playing) {
            return true;
        }
    }
    return false;
}

ngks::EngineSnapshot settleSnapshot(EngineCore& engine)
{
    ngks::EngineSnapshot snapshot = engine.getSnapshot();
    for (int i = 0; i < 40; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        snapshot = engine.getSnapshot();
    }
    return snapshot;
}

} // namespace

int main()
{
    EngineCore engine;
    bool pass = true;
    uint32_t seq = 1;

    sendSetDeckTrack(engine, ngks::DECK_A, seq++, kTrackA, "DeckA");
    sendSetDeckTrack(engine, ngks::DECK_B, seq++, kTrackB, "DeckB");

    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, kTrackA, 0.0f, 0, 0, 1u });
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_B, seq++, kTrackB, 0.0f, 0, 0, 2u });

    const bool analyzeA = waitAnalyzeDone(engine, 1u);
    const bool analyzeB = waitAnalyzeDone(engine, 2u);
    pass = pass && analyzeA && analyzeB;

    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_B, seq++, 0, 0.0f, 1, 0 });

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_B, seq++, 0, 0.0f, 0, 0 });

    const bool playingA = waitDeckPlaying(engine, ngks::DECK_A);
    const bool playingB = waitDeckPlaying(engine, ngks::DECK_B);
    pass = pass && playingA && playingB;

    engine.updateCrossfader(0.0f);
    auto snapshot = settleSnapshot(engine);
    const bool leftAaudible = snapshot.decks[ngks::DECK_A].audible;
    const bool leftBsilent = !snapshot.decks[ngks::DECK_B].audible;
    const bool leftPublic = snapshot.decks[ngks::DECK_A].publicFacing && !snapshot.decks[ngks::DECK_B].publicFacing;
    pass = pass && leftAaudible && leftBsilent && leftPublic;

    engine.updateCrossfader(1.0f);
    snapshot = settleSnapshot(engine);
    const bool rightBaudible = snapshot.decks[ngks::DECK_B].audible;
    const bool rightAsilent = !snapshot.decks[ngks::DECK_A].audible;
    const bool rightPublic = snapshot.decks[ngks::DECK_B].publicFacing && !snapshot.decks[ngks::DECK_A].publicFacing;
    pass = pass && rightBaudible && rightAsilent && rightPublic;

    engine.updateCrossfader(0.5f);
    snapshot = settleSnapshot(engine);
    const float aWeight = snapshot.decks[ngks::DECK_A].masterWeight;
    const float bWeight = snapshot.decks[ngks::DECK_B].masterWeight;
    const float energy = (aWeight * aWeight) + (bWeight * bWeight);
    const bool bothRmsPositive = snapshot.decks[ngks::DECK_A].rmsL > 0.0f && snapshot.decks[ngks::DECK_B].rmsL > 0.0f;
    const bool bothBelowFullScale = snapshot.decks[ngks::DECK_A].rmsL < 1.0f && snapshot.decks[ngks::DECK_B].rmsL < 1.0f;
    const bool equalPower = std::abs(energy - 1.0f) < 0.02f;
    pass = pass && bothRmsPositive && bothBelowFullScale && equalPower;

    if (pass) {
        std::cout << "CrossfadeTest=PASS" << std::endl;
    }

    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
