#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

namespace {

constexpr uint64_t kTrackId = 123ULL;
constexpr int kWarmupPolls = 300;
constexpr int kWarmupSleepMs = 10;
constexpr int kPolls = 300;
constexpr int kPollSleepMs = 10;

bool waitWarmup(EngineCore& engine)
{
    auto snapshot = engine.getSnapshot();
    for (int i = 0; i < kWarmupPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kWarmupSleepMs));
        snapshot = engine.getSnapshot();
        if ((snapshot.flags & ngks::SNAP_WARMUP_COMPLETE) != 0u) {
            return true;
        }
    }

    const bool audioRunning = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    std::cout << "WarmupTimeout: audioRunning=" << (audioRunning ? 1 : 0)
              << " masterRms=" << snapshot.masterRmsL
              << " warmupCounter=" << snapshot.warmupCounter
              << std::endl;
    return false;
}

void sendSetDeckTrack(EngineCore& engine, uint32_t seq, uint64_t trackId)
{
    ngks::Command command {};
    command.type = ngks::CommandType::SetDeckTrack;
    command.deck = ngks::DECK_A;
    command.seq = seq;
    command.trackUidHash = trackId;
    const char* label = "TestTrack";
    std::memcpy(command.trackLabel, label, std::strlen(label));
    engine.enqueueCommand(command);
}

bool waitForAnalyzeComplete(EngineCore& engine, uint32_t jobId)
{
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        for (int slot = 0; slot < ngks::EngineSnapshot::kMaxJobResults; ++slot) {
            const auto& result = snapshot.jobResults[slot];
            if (result.jobId == jobId
                && result.type == ngks::JobType::AnalyzeTrack
                && result.status == ngks::JobStatus::Complete) {
                return true;
            }
        }
    }

    return false;
}

} // namespace

int main()
{
    EngineCore engine;
    bool pass = true;
    uint32_t seq = 1;

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto snapshot = engine.getSnapshot();
    const bool illegalTransitionPass =
        snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::IllegalTransition;
    std::cout << "IllegalTransitionCheck: " << (illegalTransitionPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && illegalTransitionPass;

    sendSetDeckTrack(engine, seq++, kTrackId);
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, kTrackId, 0.0f, 0, 0, 1u });

    const bool analyzeDone = waitForAnalyzeComplete(engine, 1u);
    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });

    if (!waitWarmup(engine)) {
        std::cout << "RunResult=FAIL" << std::endl;
        return 1;
    }

    bool publicFacingPass = false;
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        snapshot = engine.getSnapshot();
        if (snapshot.decks[ngks::DECK_A].audible) {
            publicFacingPass =
                snapshot.decks[ngks::DECK_A].publicFacing
                && !snapshot.decks[ngks::DECK_A].cueEnabled
                && (snapshot.decks[ngks::DECK_A].lifecycle == DeckLifecycleState::Playing);
            break;
        }
    }

    publicFacingPass = publicFacingPass && analyzeDone;
    std::cout << "PublicFacingCheck: " << (publicFacingPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && publicFacingPass;

    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    bool stopPass = false;
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        snapshot = engine.getSnapshot();
        if (snapshot.decks[ngks::DECK_A].lifecycle == DeckLifecycleState::Stopped) {
            stopPass = !snapshot.decks[ngks::DECK_A].publicFacing
                && snapshot.decks[ngks::DECK_A].cueEnabled;
            break;
        }
    }
    std::cout << "StopCheck: " << (stopPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && stopPass;

    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    for (int i = 0; i < kPolls; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        snapshot = engine.getSnapshot();
        if (snapshot.decks[ngks::DECK_A].lifecycle == DeckLifecycleState::Playing) {
            break;
        }
    }
    engine.enqueueCommand({ ngks::CommandType::UnloadTrack, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    snapshot = engine.getSnapshot();
    const bool unloadProtectionPass =
        snapshot.lastCommandResult[ngks::DECK_A] == ngks::CommandResult::IllegalTransition;
    std::cout << "UnloadProtectionCheck: " << (unloadProtectionPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && unloadProtectionPass;

    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
