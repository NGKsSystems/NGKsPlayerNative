#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"
#include "engine/runtime/fx/FxTypes.h"

namespace {

constexpr int kPolls = 500;
constexpr int kPollSleepMs = 10;
constexpr int kRmsSamples = 40;

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

bool waitWarmup(EngineCore& engine)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if ((snapshot.flags & ngks::SNAP_WARMUP_COMPLETE) != 0u) {
            return true;
        }
    }
    return false;
}

bool waitDeckAnalyzed(EngineCore& engine, ngks::DeckId deckId)
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

bool waitDeckPlaying(EngineCore& engine, ngks::DeckId deckId)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (snapshot.decks[deckId].transport == ngks::TransportState::Playing) {
            return true;
        }
    }
    return false;
}

float sampleDeckRms(EngineCore& engine, ngks::DeckId deckId)
{
    float sum = 0.0f;
    for (int i = 0; i < kRmsSamples; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        sum += snapshot.decks[deckId].rmsL;
    }
    return sum / static_cast<float>(kRmsSamples);
}

bool waitStableFxState(EngineCore& engine, ngks::DeckId deckId, int slotIndex, bool enabled, float dryWet, uint32_t type)
{
    for (int poll = 0; poll < kPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        const auto& slot = snapshot.decks[deckId].fxSlots[slotIndex];
        const bool dryWetOk = std::abs(slot.dryWet - dryWet) < 0.001f;
        if (slot.enabled == enabled && dryWetOk && slot.type == type) {
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
    uint32_t seq = 10u;

    sendSetDeckTrack(engine, ngks::DECK_A, seq++, 3001ULL, "DeckA");
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, 3001ULL, 0.0f, 0, 0, 301u });
    const bool analyzed = waitDeckAnalyzed(engine, ngks::DECK_A);

    sendSetCue(engine, ngks::DECK_A, seq++);
    sendPlay(engine, ngks::DECK_A, seq++);
    const bool playing = waitDeckPlaying(engine, ngks::DECK_A);
    const bool warmup = waitWarmup(engine);

    const float baselineRms = sampleDeckRms(engine, ngks::DECK_A);

    engine.enqueueCommand({ ngks::CommandType::SetFxSlotType,
                            ngks::DECK_A,
                            seq++,
                            0,
                            0.0f,
                            0,
                            0,
                            static_cast<uint32_t>(ngks::FxType::Gain) });
    engine.enqueueCommand({ ngks::CommandType::SetDeckFxGain, ngks::DECK_A, seq++, 0, 0.5f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetFxSlotDryWet, ngks::DECK_A, seq++, 0, 1.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetFxSlotEnabled, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });

    const bool fxEnabledState = waitStableFxState(
        engine,
        ngks::DECK_A,
        0,
        true,
        1.0f,
        static_cast<uint32_t>(ngks::FxType::Gain));
    const float fxRms = sampleDeckRms(engine, ngks::DECK_A);

    engine.enqueueCommand({ ngks::CommandType::SetFxSlotEnabled, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
    const bool fxDisabledState = waitStableFxState(
        engine,
        ngks::DECK_A,
        0,
        false,
        1.0f,
        static_cast<uint32_t>(ngks::FxType::Gain));
    const float restoredRms = sampleDeckRms(engine, ngks::DECK_A);

    const float ratio = (baselineRms > 0.0001f) ? (fxRms / baselineRms) : 0.0f;
    const bool reducedExpected = ratio > 0.40f && ratio < 0.60f;
    const bool restoredExpected = (restoredRms > 0.0f) && (std::abs(restoredRms - baselineRms) / baselineRms < 0.25f);
    const auto snapshot = engine.getSnapshot();
    const bool noIllegalTransition = snapshot.lastCommandResult[ngks::DECK_A] != ngks::CommandResult::IllegalTransition;

    const bool fxPass = analyzed
        && playing
        && warmup
        && fxEnabledState
        && reducedExpected
        && fxDisabledState
        && restoredExpected
        && noIllegalTransition;

    pass = pass && fxPass;

    sendStop(engine, ngks::DECK_A, seq++);

    std::cout << "FxChainTest=" << (pass ? "PASS" : "FAIL") << std::endl;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
