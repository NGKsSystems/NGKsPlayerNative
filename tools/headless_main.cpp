#include <chrono>
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

bool waitAnalyzeComplete(EngineCore& engine, uint32_t jobId)
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

bool waitPublicFacing(EngineCore& engine, ngks::DeckId deck)
{
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deck].publicFacing) {
            return true;
        }
    }
    return false;
}

bool waitUnlockedStopped(EngineCore& engine, ngks::DeckId deck)
{
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deck].lifecycle == DeckLifecycleState::Stopped
            && !snapshot.decks[deck].publicFacing
            && !snapshot.decks[deck].commandLocked) {
            return true;
        }
    }
    return false;
}

} // namespace

int main()
{
    EngineCore engine;
    bool pass = true;

    sendSetDeckTrack(engine, ngks::DECK_A, 10u, kTrackA, "DeckA");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sendSetDeckTrack(engine, ngks::DECK_A, 9u, kTrackB, "DeckB");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto snapshot = engine.getSnapshot();
    const bool outOfOrderPass =
        snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::OutOfOrderSeq
        && snapshot.decks[ngks::DECK_A].lastAcceptedCommandSeq == 10u;
    std::cout << "OutOfOrderObservedResult=" << static_cast<int>(snapshot.lastCommandResult[ngks::DECK_A]) << std::endl;
    std::cout << "OutOfOrderObservedLastAcceptedSeq=" << snapshot.decks[ngks::DECK_A].lastAcceptedCommandSeq << std::endl;
    std::cout << "OutOfOrderSeqCheck: " << (outOfOrderPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && outOfOrderPass;

    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, 11u, kTrackA, 0.0f, 0, 0, 1u });
    const bool analyzed = waitAnalyzeComplete(engine, 1u);
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, 12u, 0, 0.0f, 1, 0 });
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, 13u, 0, 0.0f, 0, 0 });

    const bool publicFacingReady = analyzed && waitPublicFacing(engine, ngks::DECK_A);
    snapshot = engine.getSnapshot();
    const uint64_t trackBeforeLock = snapshot.decks[ngks::DECK_A].currentTrackId;

    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, ngks::DECK_A, 14u, 0, 0.0f, 0, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    snapshot = engine.getSnapshot();
    const bool lockPass = publicFacingReady
        && snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::DeckLocked
        && snapshot.decks[ngks::DECK_A].lifecycle == DeckLifecycleState::Playing
        && snapshot.decks[ngks::DECK_A].currentTrackId == trackBeforeLock;
    std::cout << "PublicFacingLockCheck: " << (lockPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && lockPass;

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, 15u, 0, 0.0f, 0, 0 });
    const bool stoppedUnlocked = waitUnlockedStopped(engine, ngks::DECK_A);

    sendSetDeckTrack(engine, ngks::DECK_A, 16u, 2002ULL, "DeckC");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    snapshot = engine.getSnapshot();
    const bool unlockPass = stoppedUnlocked
        && snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::Applied
        && snapshot.decks[ngks::DECK_A].currentTrackId == 2002ULL;
    std::cout << "UnlockAfterStopCheck: " << (unlockPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && unlockPass;

    std::cout << "AuthorityTest=" << (pass ? "PASS" : "FAIL") << std::endl;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
