#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"

namespace {

constexpr int kPolls = 400;
constexpr int kPollSleepMs = 10;
constexpr int kMetricSamples = 60;
constexpr float kLimiterThreshold = 0.95f;
constexpr float kPeakTolerance = 0.0001f;

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

void sendSetDeckGain(EngineCore& engine, ngks::DeckId deck, uint32_t seq, float gain)
{
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, deck, seq, 0, gain, 0, 0 });
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

struct MasterMetricWindow {
    float avgRmsL{0.0f};
    float maxPeakL{0.0f};
    bool limiterSeen{false};
};

MasterMetricWindow sampleMasterMetrics(EngineCore& engine)
{
    MasterMetricWindow metrics;
    float rmsAccum = 0.0f;
    for (int i = 0; i < kMetricSamples; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        rmsAccum += snapshot.masterRmsL;
        metrics.maxPeakL = std::max(metrics.maxPeakL, snapshot.masterPeakL);
        metrics.limiterSeen = metrics.limiterSeen || snapshot.masterLimiterActive;
    }
    metrics.avgRmsL = rmsAccum / static_cast<float>(kMetricSamples);
    return metrics;
}

} // namespace

int main()
{
    EngineCore engine;
    bool pass = true;
    uint32_t seq = 10u;

    engine.updateCrossfader(0.0f);
    sendSetDeckTrack(engine, ngks::DECK_A, seq++, 3001ULL, "DeckA");
    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, 3001ULL, 0.0f, 0, 0, 301u });
    const bool analyzed = waitDeckAnalyzed(engine, ngks::DECK_A);

    sendSetCue(engine, ngks::DECK_A, seq++);
    sendPlay(engine, ngks::DECK_A, seq++);
    const bool playing = waitDeckPlaying(engine, ngks::DECK_A);
    const bool warmup = waitWarmup(engine);

    const auto baseline = sampleMasterMetrics(engine);
    const auto snapshot = engine.getSnapshot();
    const bool baselinePass = analyzed
        && playing
        && warmup
        && baseline.avgRmsL > 0.0f
        && baseline.maxPeakL > 0.0f
        && !baseline.limiterSeen
        && !snapshot.masterLimiterActive;

    sendSetDeckGain(engine, ngks::DECK_A, seq++, 12.0f);
    const auto clipped = sampleMasterMetrics(engine);

    const bool limiterPass = clipped.limiterSeen
        && clipped.maxPeakL <= (kLimiterThreshold + kPeakTolerance)
        && clipped.avgRmsL > 0.0f;

    pass = baselinePass && limiterPass;
    std::cout << "MasterBusTest=" << (baselinePass ? "PASS" : "FAIL") << std::endl;
    std::cout << "LimiterClampTest=" << (limiterPass ? "PASS" : "FAIL") << std::endl;
    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
