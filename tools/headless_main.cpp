#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

namespace {

constexpr int kPolls = 800;
constexpr int kPollSleepMs = 10;
constexpr float kEpsilon = 0.0001f;

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

void sendSetCue(EngineCore& engine, ngks::DeckId deck, uint32_t seq)
{
    engine.enqueueCommand({ ngks::CommandType::SetCue, deck, seq, 0, 0.0f, 1, 0 });
}

void sendPlay(EngineCore& engine, ngks::DeckId deck, uint32_t seq)
{
    engine.enqueueCommand({ ngks::CommandType::Play, deck, seq, 0, 0.0f, 0, 0 });
}

void sendStop(EngineCore& engine, ngks::DeckId deck, uint32_t seq)
{
    engine.enqueueCommand({ ngks::CommandType::Stop, deck, seq, 0, 0.0f, 0, 0 });
}

bool waitForAllPlaying(EngineCore& engine)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        bool allPlaying = true;
        for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
            if (snapshot.decks[deck].transport != ngks::TransportState::Playing) {
                allPlaying = false;
                break;
            }
        }
        if (allPlaying) {
            return true;
        }
    }
    return false;
}

bool waitForAllAnalyzed(EngineCore& engine)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        bool allAnalyzed = true;
        for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
            if (snapshot.decks[deck].lifecycle != DeckLifecycleState::Analyzed) {
                allAnalyzed = false;
                break;
            }
        }
        if (allAnalyzed) {
            return true;
        }
    }
    return false;
}

bool waitForDeckAnalyzed(EngineCore& engine, ngks::DeckId deckId)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deckId].lifecycle == DeckLifecycleState::Analyzed) {
            return true;
        }
    }
    return false;
}

bool waitForStopped(EngineCore& engine, ngks::DeckId deckA, ngks::DeckId deckB)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        const bool aStopped = snapshot.decks[deckA].transport == ngks::TransportState::Stopped;
        const bool bStopped = snapshot.decks[deckB].transport == ngks::TransportState::Stopped;
        if (aStopped && bStopped) {
            return true;
        }
    }
    return false;
}

bool waitForTransportState(EngineCore& engine, ngks::DeckId deck, ngks::TransportState state)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deck].transport == state) {
            return true;
        }
    }
    return false;
}

float weightSumSq(const ngks::EngineSnapshot& snapshot)
{
    float sumSq = 0.0f;
    for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
        const float w = snapshot.decks[deck].masterWeight;
        sumSq += w * w;
    }
    return sumSq;
}

bool allNoIllegalTransitions(const ngks::EngineSnapshot& snapshot)
{
    for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
        if (snapshot.lastCommandResult[deck] == ngks::CommandResult::IllegalTransition) {
            return false;
        }
    }
    return true;
}

bool publicFacingAtMostOne(const ngks::EngineSnapshot& snapshot)
{
    int count = 0;
    for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
        if (snapshot.decks[deck].publicFacing) {
            ++count;
        }
    }
    return count <= 1;
}

} // namespace

int main()
{
    EngineCore engine;
    bool pass = true;
    uint32_t seq = 10u;

    sendSetDeckTrack(engine, ngks::DECK_A, seq++, 1001ULL, "DeckA");
    sendSetDeckTrack(engine, ngks::DECK_B, seq++, 1002ULL, "DeckB");
    sendSetDeckTrack(engine, ngks::DECK_C, seq++, 1003ULL, "DeckC");
    sendSetDeckTrack(engine, ngks::DECK_D, seq++, 1004ULL, "DeckD");

    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, 1001ULL, 0.0f, 0, 0, 101u });
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_B, seq++, 1002ULL, 0.0f, 0, 0, 102u });
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_C, seq++, 1003ULL, 0.0f, 0, 0, 103u });
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_D, seq++, 1004ULL, 0.0f, 0, 0, 104u });
    const bool allAnalyzed = waitForAllAnalyzed(engine);

    sendSetCue(engine, ngks::DECK_A, seq++);
    sendSetCue(engine, ngks::DECK_B, seq++);
    sendSetCue(engine, ngks::DECK_C, seq++);
    sendSetCue(engine, ngks::DECK_D, seq++);

    sendPlay(engine, ngks::DECK_A, seq++);
    sendPlay(engine, ngks::DECK_B, seq++);
    sendPlay(engine, ngks::DECK_C, seq++);
    sendPlay(engine, ngks::DECK_D, seq++);

    const bool allPlaying = allAnalyzed && waitForAllPlaying(engine);

    engine.updateCrossfader(0.5f);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    auto snapshot = engine.getSnapshot();
    const float centerSumSq = weightSumSq(snapshot);
    const bool centerWeightsPositive =
        snapshot.decks[ngks::DECK_A].masterWeight > 0.0f
        && snapshot.decks[ngks::DECK_B].masterWeight > 0.0f
        && snapshot.decks[ngks::DECK_C].masterWeight > 0.0f
        && snapshot.decks[ngks::DECK_D].masterWeight > 0.0f;
    const bool centerConserved = centerSumSq <= 1.0001f;
    const bool centerPass = allPlaying && centerWeightsPositive && centerConserved;
    std::cout << "FourDeckEqualCenterCheck: " << (centerPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && centerPass;

    engine.updateCrossfader(0.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    snapshot = engine.getSnapshot();
    const bool hardLeftPass =
        snapshot.decks[ngks::DECK_A].masterWeight > 0.0f
        && snapshot.decks[ngks::DECK_B].masterWeight > 0.0f
        && std::abs(snapshot.decks[ngks::DECK_C].masterWeight) <= kEpsilon
        && std::abs(snapshot.decks[ngks::DECK_D].masterWeight) <= kEpsilon;
    std::cout << "HardLeftCheck: " << (hardLeftPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && hardLeftPass;

    engine.updateCrossfader(1.0f);
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    snapshot = engine.getSnapshot();
    const bool hardRightPass =
        std::abs(snapshot.decks[ngks::DECK_A].masterWeight) <= kEpsilon
        && std::abs(snapshot.decks[ngks::DECK_B].masterWeight) <= kEpsilon
        && snapshot.decks[ngks::DECK_C].masterWeight > 0.0f
        && snapshot.decks[ngks::DECK_D].masterWeight > 0.0f;
    std::cout << "HardRightCheck: " << (hardRightPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && hardRightPass;

    sendStop(engine, ngks::DECK_C, seq++);
    sendStop(engine, ngks::DECK_D, seq++);
    const bool rightStopped = waitForStopped(engine, ngks::DECK_C, ngks::DECK_D);

    engine.updateCrossfader(0.5f);
    bool conflictPass = false;
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        snapshot = engine.getSnapshot();

        const bool twoAudible = snapshot.decks[ngks::DECK_A].audible && snapshot.decks[ngks::DECK_B].audible;
        const bool aboveThreshold = snapshot.decks[ngks::DECK_A].masterWeight > 0.15f
            && snapshot.decks[ngks::DECK_B].masterWeight > 0.15f;
        int publicCount = 0;
        int publicDeck = -1;
        for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
            if (snapshot.decks[deck].publicFacing) {
                ++publicCount;
                publicDeck = static_cast<int>(deck);
            }
        }

        if (rightStopped && twoAudible && aboveThreshold && publicCount == 1 && publicDeck == ngks::DECK_A) {
            conflictPass = true;
            break;
        }
    }
    std::cout << "PublicConflictCheck: " << (conflictPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && conflictPass;

    snapshot = engine.getSnapshot();
    const bool illegalTransitionLeakPass = allNoIllegalTransitions(snapshot);
    std::cout << "IllegalTransitionLeakCheck: " << (illegalTransitionLeakPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && illegalTransitionLeakPass;

    sendStop(engine, ngks::DECK_A, seq++);
    sendStop(engine, ngks::DECK_B, seq++);
    const bool allStopped =
        waitForTransportState(engine, ngks::DECK_A, ngks::TransportState::Stopped)
        && waitForTransportState(engine, ngks::DECK_B, ngks::TransportState::Stopped)
        && waitForTransportState(engine, ngks::DECK_C, ngks::TransportState::Stopped)
        && waitForTransportState(engine, ngks::DECK_D, ngks::TransportState::Stopped);

    sendSetDeckTrack(engine, ngks::DECK_A, seq++, 2001ULL, "ReloadA");
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, 2001ULL, 0.0f, 0, 0, 201u });
    const bool reanalyzed = waitForDeckAnalyzed(engine, ngks::DECK_A);
    sendSetCue(engine, ngks::DECK_A, seq++);
    sendPlay(engine, ngks::DECK_A, seq++);
    const bool replayingA = waitForTransportState(engine, ngks::DECK_A, ngks::TransportState::Playing);

    snapshot = engine.getSnapshot();
    const bool restartReloadCleanPass = allStopped
        && reanalyzed
        && replayingA
        && publicFacingAtMostOne(snapshot)
        && allNoIllegalTransitions(snapshot)
        && snapshot.decks[ngks::DECK_A].currentTrackId == 2001ULL;
    std::cout << "RestartReloadStateCleanCheck: " << (restartReloadCleanPass ? "PASS" : "FAIL") << std::endl;
    pass = pass && restartReloadCleanPass;

    std::cout << "StabilitySweep=" << (pass ? "PASS" : "FAIL") << std::endl;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
