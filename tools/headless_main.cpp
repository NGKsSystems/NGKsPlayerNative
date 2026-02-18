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
constexpr int kWarmupPollSleepMs = 10;
constexpr int kJobPolls = 300;
constexpr int kJobPollSleepMs = 10;
constexpr int kCacheHitPolls = 20;

bool waitWarmup(EngineCore& engine)
{
    ngks::EngineSnapshot snapshot = engine.getSnapshot();
    for (int attempt = 0; attempt < kWarmupPolls; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kWarmupPollSleepMs));
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

void enqueueSetDeckTrack(EngineCore& engine, uint32_t seq, uint64_t trackId)
{
    ngks::Command cmd {};
    cmd.type = ngks::CommandType::SetDeckTrack;
    cmd.deck = ngks::DECK_A;
    cmd.seq = seq;
    cmd.trackUidHash = trackId;
    const char* label = "TestTrack";
    std::memcpy(cmd.trackLabel, label, std::strlen(label));
    engine.enqueueCommand(cmd);
}

bool findCompletedJob(const ngks::EngineSnapshot& snapshot, uint32_t jobId, ngks::JobResult& out)
{
    for (int i = 0; i < ngks::EngineSnapshot::kMaxJobResults; ++i) {
        const auto& candidate = snapshot.jobResults[i];
        if (candidate.jobId == jobId && candidate.status == ngks::JobStatus::Complete) {
            out = candidate;
            return true;
        }
    }
    return false;
}

bool waitForJob(EngineCore& engine, uint32_t jobId, int maxPolls, ngks::JobResult& out)
{
    for (int poll = 0; poll < maxPolls; ++poll) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kJobPollSleepMs));
        const auto snapshot = engine.getSnapshot();
        if (findCompletedJob(snapshot, jobId, out)) {
            return true;
        }
    }
    return false;
}

} // namespace

int main()
{
    bool pass = true;

    {
        EngineCore engine;
        uint32_t seq = 1;

        enqueueSetDeckTrack(engine, seq++, kTrackId);
        engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });

        if (!waitWarmup(engine)) {
            std::cout << "RunResult=FAIL" << std::endl;
            return 1;
        }

        engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, kTrackId, 0.0f, 0, 0, 1u });
        ngks::JobResult analyzeResult {};
        const bool analyzeCompleted = waitForJob(engine, 1u, kJobPolls, analyzeResult);
        std::cout << "AnalyzeJob1Completed=" << analyzeCompleted << std::endl;
        std::cout << "AnalyzeJob1_cacheHit=" << static_cast<int>(analyzeResult.cacheHit) << std::endl;
        std::cout << "AnalyzeJob1_bpmFixed=" << analyzeResult.bpmFixed << std::endl;

        const auto postAnalyzeSnapshot = engine.getSnapshot();
        std::cout << "DeckA_cachedBpmAfterJob1=" << postAnalyzeSnapshot.decks[ngks::DECK_A].cachedBpmFixed << std::endl;

        pass = pass && analyzeCompleted;
        pass = pass && (analyzeResult.bpmFixed == 12800);
        pass = pass && (postAnalyzeSnapshot.decks[ngks::DECK_A].cachedBpmFixed == 12800);

        engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    {
        EngineCore engine;
        uint32_t seq = 100;

        enqueueSetDeckTrack(engine, seq++, kTrackId);
        engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });

        if (!waitWarmup(engine)) {
            std::cout << "RunResult=FAIL" << std::endl;
            return 1;
        }

        engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, kTrackId, 0.0f, 0, 0, 2u });

        ngks::JobResult cacheHitResult {};
        const bool cacheHitCompleted = waitForJob(engine, 2u, kCacheHitPolls, cacheHitResult);
        std::cout << "AnalyzeJob2Completed=" << cacheHitCompleted << std::endl;
        std::cout << "AnalyzeJob2_cacheHit=" << static_cast<int>(cacheHitResult.cacheHit) << std::endl;
        std::cout << "AnalyzeJob2_bpmFixed=" << cacheHitResult.bpmFixed << std::endl;

        if (cacheHitCompleted && cacheHitResult.cacheHit == 1) {
            std::cout << "CACHE_HIT_ANALYZE trackId=123" << std::endl;
        }

        const auto postRestartSnapshot = engine.getSnapshot();
        std::cout << "DeckA_cachedBpmAfterRestart=" << postRestartSnapshot.decks[ngks::DECK_A].cachedBpmFixed << std::endl;

        pass = pass && cacheHitCompleted;
        pass = pass && (cacheHitResult.cacheHit == 1);
        pass = pass && (cacheHitResult.bpmFixed == 12800);
        pass = pass && (postRestartSnapshot.decks[ngks::DECK_A].cachedBpmFixed == 12800);

        engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }

    std::cout << "RunResult=" << (pass ? "PASS" : "FAIL") << std::endl;
    return pass ? 0 : 1;
}
