#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"
#include "engine/domain/CrossfadeAssignment.h"

#include <algorithm>
#include <cmath>
#include <chrono>
#include <iostream>

namespace
{
constexpr float warmupAudibleRmsThreshold = 0.005f;
constexpr uint32_t warmupConsecutiveBlocksRequired = 50u;
constexpr float rmsSmoothingAlpha = 0.2f;
constexpr float peakDecayFactor = 0.96f;
constexpr int peakHoldBlocks = 8;
constexpr std::chrono::seconds registryPersistInterval(1);
constexpr float kPublicFacingWeightThreshold = 0.15f;
constexpr CrossfadeAssignment kDefaultCrossfadeAssignment {
    { 0, 1 },
    { 2, 3 },
    2,
    2
};

void updateMaxRelaxed(std::atomic<uint32_t>& target, uint32_t value) noexcept
{
    uint32_t previous = target.load(std::memory_order_relaxed);
    while (value > previous && !target.compare_exchange_weak(previous, value, std::memory_order_relaxed)) {
    }
}

bool isDeckRoutingActive(const ngks::DeckSnapshot& deck) noexcept
{
    if (deck.hasTrack == 0) {
        return false;
    }

    return deck.transport == ngks::TransportState::Starting
        || deck.transport == ngks::TransportState::Playing
        || deck.transport == ngks::TransportState::Stopping;
}

void computeCrossfadeWeights(const ngks::EngineSnapshot& snapshot, float x, MixMatrix& mixMatrix) noexcept
{
    x = std::clamp(x, 0.0f, 1.0f);
    const float leftGain = std::cos(x * 1.57079632679f);
    const float rightGain = std::sin(x * 1.57079632679f);

    for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
        mixMatrix.decks[i].masterWeight = 0.0f;
        mixMatrix.decks[i].cueWeight = 1.0f;
    }

    int leftActiveCount = 0;
    int rightActiveCount = 0;
    bool leftActive[2] { false, false };
    bool rightActive[2] { false, false };

    for (int i = 0; i < kDefaultCrossfadeAssignment.leftCount; ++i) {
        const int deckIndex = kDefaultCrossfadeAssignment.leftDecks[i];
        if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) {
            continue;
        }

        leftActive[i] = isDeckRoutingActive(snapshot.decks[deckIndex]);
        if (leftActive[i]) {
            ++leftActiveCount;
        }
    }

    for (int i = 0; i < kDefaultCrossfadeAssignment.rightCount; ++i) {
        const int deckIndex = kDefaultCrossfadeAssignment.rightDecks[i];
        if (deckIndex < 0 || deckIndex >= ngks::MAX_DECKS) {
            continue;
        }

        rightActive[i] = isDeckRoutingActive(snapshot.decks[deckIndex]);
        if (rightActive[i]) {
            ++rightActiveCount;
        }
    }

    if (leftActiveCount > 0) {
        const float perDeckLeft = leftGain / static_cast<float>(leftActiveCount);
        for (int i = 0; i < kDefaultCrossfadeAssignment.leftCount; ++i) {
            if (!leftActive[i]) {
                continue;
            }

            const int deckIndex = kDefaultCrossfadeAssignment.leftDecks[i];
            if (deckIndex >= 0 && deckIndex < ngks::MAX_DECKS) {
                mixMatrix.decks[deckIndex].masterWeight = perDeckLeft;
            }
        }
    }

    if (rightActiveCount > 0) {
        const float perDeckRight = rightGain / static_cast<float>(rightActiveCount);
        for (int i = 0; i < kDefaultCrossfadeAssignment.rightCount; ++i) {
            if (!rightActive[i]) {
                continue;
            }

            const int deckIndex = kDefaultCrossfadeAssignment.rightDecks[i];
            if (deckIndex >= 0 && deckIndex < ngks::MAX_DECKS) {
                mixMatrix.decks[deckIndex].masterWeight = perDeckRight;
            }
        }
    }

    float sumSq = 0.0f;
    for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
        const float w = mixMatrix.decks[i].masterWeight;
        sumSq += (w * w);
    }

    if (sumSq > 1.0001f) {
        const float scale = 1.0f / std::sqrt(sumSq);
        for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
            mixMatrix.decks[i].masterWeight *= scale;
        }
    }
}
}

EngineCore::EngineCore(bool offlineMode)
    : offlineMode_(offlineMode)
{
    if (!offlineMode_) {
        audioIO = std::make_unique<AudioIOJuce>(*this);
    }

    for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
        snapshots[0].decks[deck].id = deck;
        snapshots[1].decks[deck].id = deck;
        snapshots[0].decks[deck].lastAcceptedCommandSeq = authority_[deck].lastAcceptedSeq;
        snapshots[1].decks[deck].lastAcceptedCommandSeq = authority_[deck].lastAcceptedSeq;
        snapshots[0].decks[deck].commandLocked = authority_[deck].locked;
        snapshots[1].decks[deck].commandLocked = authority_[deck].locked;
    }

    const size_t loadedCount = registryStore.load(trackRegistry);
    std::cout << "CACHE_LOAD_OK count=" << loadedCount << std::endl;

    updateCrossfader(0.5f);

    jobSystem.start();
    lastRegistryPersist = std::chrono::steady_clock::now();
}

EngineCore::~EngineCore()
{
    if (audioIO != nullptr) {
        audioIO->stop();
    }

    persistRegistryIfNeeded(true);
    jobSystem.stop();
}

ngks::EngineSnapshot EngineCore::getSnapshot()
{
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    const uint32_t back = front ^ 1u;

    ngks::EngineSnapshot working = snapshots[front];
    appendJobResults(working);
    snapshots[back] = working;
    frontSnapshotIndex.store(back, std::memory_order_release);

    persistRegistryIfNeeded(false);
    return snapshots[back];
}

void EngineCore::enqueueCommand(const ngks::Command& command)
{
    if (isDeckMutationCommand(command)) {
        if (command.deck >= ngks::MAX_DECKS) {
            publishCommandOutcome(command, ngks::CommandResult::RejectedInvalidDeck);
            return;
        }

        auto& authority = authority_[command.deck];
        if (command.seq <= authority.lastAcceptedSeq) {
            publishCommandOutcome(command, ngks::CommandResult::OutOfOrderSeq);
            return;
        }

        if (authority.locked && isCriticalMutationCommand(command)) {
            publishCommandOutcome(command, ngks::CommandResult::DeckLocked);
            return;
        }

        authority.commandInFlight = true;
    }

    if (command.type == ngks::CommandType::SetDeckTrack) {
        publishCommandOutcome(command, ngks::CommandResult::Applied);
        return;
    }

    if (command.type == ngks::CommandType::SetCue) {
        publishCommandOutcome(command, ngks::CommandResult::Applied);
        return;
    }

    if (command.type == ngks::CommandType::RequestAnalyzeTrack
        || command.type == ngks::CommandType::RequestStemsOffline
        || command.type == ngks::CommandType::CancelJob) {
        const auto result = submitJobCommand(command);
        publishCommandOutcome(command, result);
        return;
    }

    if (command.type == ngks::CommandType::Play) {
        if (command.deck >= ngks::MAX_DECKS) {
            publishCommandOutcome(command, ngks::CommandResult::RejectedInvalidDeck);
            return;
        }

        const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
        const auto& currentDeck = snapshots[front].decks[command.deck];
        if (!validateTransition(currentDeck.lifecycle, DeckLifecycleState::Playing)) {
            publishCommandOutcome(command, ngks::CommandResult::IllegalTransition);
            return;
        }
        if (!currentDeck.hasTrack) {
            publishCommandOutcome(command, ngks::CommandResult::RejectedNoTrack);
            return;
        }

        startAudioIfNeeded();
    }

    if (!commandRing.push(command)) {
        const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
        ngks::EngineSnapshot dropped = snapshots[front];
        if (command.deck < ngks::MAX_DECKS) {
            dropped.lastCommandResult[command.deck] = ngks::CommandResult::RejectedQueueFull;
            dropped.lastProcessedCommandSeq = command.seq;
            if (isDeckMutationCommand(command)) {
                authority_[command.deck].commandInFlight = false;
                dropped.decks[command.deck].lastAcceptedCommandSeq = authority_[command.deck].lastAcceptedSeq;
                dropped.decks[command.deck].commandLocked = authority_[command.deck].locked;
            }
            const uint32_t back = front ^ 1u;
            snapshots[back] = dropped;
            frontSnapshotIndex.store(back, std::memory_order_release);
        }
    }
}

bool EngineCore::isCriticalMutationCommand(const ngks::Command& c)
{
    return c.type == ngks::CommandType::SetDeckTrack
        || c.type == ngks::CommandType::LoadTrack
        || c.type == ngks::CommandType::UnloadTrack;
}

bool EngineCore::isDeckMutationCommand(const ngks::Command& c)
{
    switch (c.type) {
    case ngks::CommandType::SetDeckTrack:
    case ngks::CommandType::LoadTrack:
    case ngks::CommandType::UnloadTrack:
    case ngks::CommandType::Play:
    case ngks::CommandType::Stop:
    case ngks::CommandType::SetDeckGain:
    case ngks::CommandType::SetCue:
    case ngks::CommandType::SetFxSlotType:
    case ngks::CommandType::SetFxSlotEnabled:
    case ngks::CommandType::SetFxSlotDryWet:
    case ngks::CommandType::SetDeckFxGain:
    case ngks::CommandType::EnableDeckFxSlot:
    case ngks::CommandType::RequestAnalyzeTrack:
    case ngks::CommandType::RequestStemsOffline:
    case ngks::CommandType::CancelJob:
        return true;
    default:
        return false;
    }
}

bool EngineCore::validateTransition(DeckLifecycleState from, DeckLifecycleState to)
{
    switch (from) {
    case DeckLifecycleState::Empty:
        return to == DeckLifecycleState::Loading;
    case DeckLifecycleState::Loading:
        return to == DeckLifecycleState::Loaded;
    case DeckLifecycleState::Loaded:
        return to == DeckLifecycleState::Analyzed;
    case DeckLifecycleState::Analyzed:
        return to == DeckLifecycleState::Armed;
    case DeckLifecycleState::Armed:
        return to == DeckLifecycleState::Playing;
    case DeckLifecycleState::Playing:
        return to == DeckLifecycleState::Stopped;
    case DeckLifecycleState::Stopped:
        return to == DeckLifecycleState::Playing || to == DeckLifecycleState::Empty;
    }

    return false;
}

void EngineCore::startAudioIfNeeded()
{
    if (offlineMode_) {
        audioOpened.store(true, std::memory_order_release);
        return;
    }

    if (audioOpened.load(std::memory_order_acquire)) {
        return;
    }

    const auto result = audioIO->start();
    if (!result.ok) {
        return;
    }

    sampleRateHz = result.sampleRate;
    audioOpened.store(true, std::memory_order_release);
}

bool EngineCore::renderOfflineBlock(float* outInterleavedLR, uint32_t frames)
{
    if (outInterleavedLR == nullptr || frames == 0u) {
        return false;
    }

    constexpr uint32_t kMaxChunkFrames = 2048u;
    float left[kMaxChunkFrames] {};
    float right[kMaxChunkFrames] {};

    uint32_t rendered = 0u;
    while (rendered < frames) {
        const uint32_t remaining = frames - rendered;
        const uint32_t chunk = std::min(remaining, kMaxChunkFrames);
        process(left, right, static_cast<int>(chunk));

        for (uint32_t i = 0u; i < chunk; ++i) {
            const uint32_t outIndex = (rendered + i) * 2u;
            outInterleavedLR[outIndex] = left[i];
            outInterleavedLR[outIndex + 1u] = right[i];
        }

        rendered += chunk;
    }

    return true;
}

EngineTelemetrySnapshot EngineCore::getTelemetrySnapshot() const noexcept
{
    EngineTelemetrySnapshot snapshot {};
    snapshot.renderCycles = telemetry_.renderCycles.load(std::memory_order_relaxed);
    snapshot.audioCallbacks = telemetry_.audioCallbacks.load(std::memory_order_relaxed);
    snapshot.xruns = telemetry_.xruns.load(std::memory_order_relaxed);
    snapshot.lastRenderDurationUs = telemetry_.lastRenderDurationUs.load(std::memory_order_relaxed);
    snapshot.maxRenderDurationUs = telemetry_.maxRenderDurationUs.load(std::memory_order_relaxed);
    snapshot.lastCallbackDurationUs = telemetry_.lastCallbackDurationUs.load(std::memory_order_relaxed);
    snapshot.maxCallbackDurationUs = telemetry_.maxCallbackDurationUs.load(std::memory_order_relaxed);

    uint32_t count = telemetry_.renderDurationHistoryCount.load(std::memory_order_acquire);
    if (count > EngineTelemetrySnapshot::kRenderDurationWindowSize) {
        count = EngineTelemetrySnapshot::kRenderDurationWindowSize;
    }

    const uint32_t writeIndex = telemetry_.renderDurationHistoryWriteIndex.load(std::memory_order_acquire);
    snapshot.renderDurationWindowCount = count;
    if (count > 0u) {
        const uint32_t windowSize = EngineTelemetrySnapshot::kRenderDurationWindowSize;
        const uint32_t oldest = (writeIndex + windowSize - count) % windowSize;
        for (uint32_t i = 0u; i < count; ++i) {
            const uint32_t sourceIndex = (oldest + i) % windowSize;
            snapshot.renderDurationWindowUs[i] = telemetry_.renderDurationHistoryUs[sourceIndex].load(std::memory_order_relaxed);
        }
    }

    return snapshot;
}

void EngineCore::pushRenderDurationSample(uint32_t durationUs) noexcept
{
    const uint32_t writeIndex = telemetry_.renderDurationHistoryWriteIndex.load(std::memory_order_relaxed);
    const uint32_t slot = writeIndex % EngineTelemetry::kRenderDurationHistorySize;
    telemetry_.renderDurationHistoryUs[slot].store(durationUs, std::memory_order_relaxed);
    telemetry_.renderDurationHistoryWriteIndex.store(writeIndex + 1u, std::memory_order_release);

    const uint32_t count = telemetry_.renderDurationHistoryCount.load(std::memory_order_relaxed);
    if (count < EngineTelemetry::kRenderDurationHistorySize) {
        telemetry_.renderDurationHistoryCount.store(count + 1u, std::memory_order_release);
    }
}

void EngineCore::prepare(double sampleRate, int)
{
    sampleRateHz = (sampleRate > 0.0) ? sampleRate : 48000.0;
    fadeSamplesTotal = static_cast<int>(sampleRateHz * 0.2);
    if (fadeSamplesTotal < 1) {
        fadeSamplesTotal = 1;
    }

    audioGraph.prepare(sampleRateHz, 2048);
}

void EngineCore::updateCrossfader(float x)
{
    crossfaderPosition_ = std::clamp(x, 0.0f, 1.0f);
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    computeCrossfadeWeights(snapshots[front], crossfaderPosition_, mixMatrix_);
}

ngks::CommandResult EngineCore::submitJobCommand(const ngks::Command& command) noexcept
{
    if (command.type == ngks::CommandType::CancelJob) {
        jobSystem.cancel(command.jobId);
        return ngks::CommandResult::Applied;
    }

    if (command.deck >= ngks::MAX_DECKS) {
        return ngks::CommandResult::RejectedInvalidDeck;
    }

    const uint64_t trackId = command.trackUidHash;
    ngks::AnalysisMeta cachedAnalysis {};
    const bool hasCached = (trackId != 0) && trackRegistry.getAnalysis(trackId, cachedAnalysis);

    bool cacheSatisfied = false;
    if (hasCached) {
        if (command.type == ngks::CommandType::RequestAnalyzeTrack) {
            cacheSatisfied = (cachedAnalysis.bpmFixed != 0) && (cachedAnalysis.loudnessCentiDb != 0 || cachedAnalysis.status != 0);
        } else {
            cacheSatisfied = (cachedAnalysis.stemsReady != 0);
        }
    }

    if (cacheSatisfied) {
        ngks::JobResult result {};
        result.jobId = command.jobId;
        result.deckId = command.deck;
        result.trackId = trackId;
        result.type = (command.type == ngks::CommandType::RequestAnalyzeTrack)
            ? ngks::JobType::AnalyzeTrack
            : ngks::JobType::StemsOffline;
        result.status = ngks::JobStatus::Complete;
        result.progress0_100 = 100;
        result.bpmFixed = cachedAnalysis.bpmFixed;
        result.loudness = cachedAnalysis.loudnessCentiDb;
        result.deadAirMs = static_cast<int32_t>(cachedAnalysis.deadAirMs);
        result.stemsReady = cachedAnalysis.stemsReady;
        result.cacheHit = 1;
        jobSystem.publishSyntheticResult(result);

        if (command.type == ngks::CommandType::RequestAnalyzeTrack) {
            std::cout << "CACHE_HIT_ANALYZE trackId=" << trackId << std::endl;
        }
        return ngks::CommandResult::Applied;
    }

    if (command.type == ngks::CommandType::RequestAnalyzeTrack) {
        std::cout << "CACHE_MISS_ANALYZE trackId=" << trackId << std::endl;
    }

    ngks::JobRequest request {};
    request.jobId = command.jobId;
    request.deckId = command.deck;
    request.trackId = trackId;
    request.type = (command.type == ngks::CommandType::RequestAnalyzeTrack)
        ? ngks::JobType::AnalyzeTrack
        : ngks::JobType::StemsOffline;

    return jobSystem.enqueue(request)
        ? ngks::CommandResult::Applied
        : ngks::CommandResult::RejectedQueueFull;
}

ngks::CommandResult EngineCore::applySetDeckTrack(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept
{
    if (command.deck >= ngks::MAX_DECKS) {
        return ngks::CommandResult::RejectedInvalidDeck;
    }

    auto& deck = snapshot.decks[command.deck];

    auto state = deck.lifecycle;
    if (state == DeckLifecycleState::Stopped) {
        if (!validateTransition(state, DeckLifecycleState::Empty)) {
            return ngks::CommandResult::IllegalTransition;
        }
        state = DeckLifecycleState::Empty;
    }
    if (!validateTransition(state, DeckLifecycleState::Loading)) {
        return ngks::CommandResult::IllegalTransition;
    }
    state = DeckLifecycleState::Loading;
    if (!validateTransition(state, DeckLifecycleState::Loaded)) {
        return ngks::CommandResult::IllegalTransition;
    }

    deck.hasTrack = 1;
    deck.trackUidHash = command.trackUidHash;
    deck.currentTrackId = command.trackUidHash;
    deck.lengthSeconds = 240.0;

    for (size_t i = 0; i < sizeof(deck.currentTrackLabel); ++i) {
        deck.currentTrackLabel[i] = command.trackLabel[i];
    }

    ngks::TrackMeta trackMeta {};
    trackMeta.trackId = command.trackUidHash;
    trackMeta.durationMs = 240000;
    for (size_t i = 0; i < sizeof(trackMeta.label); ++i) {
        trackMeta.label[i] = command.trackLabel[i];
    }
    trackRegistry.upsertTrackMeta(command.trackUidHash, trackMeta);

    ngks::AnalysisMeta analysis {};
    if (trackRegistry.getAnalysis(command.trackUidHash, analysis)) {
        applyCachedAnalysisToDeck(deck, analysis);
    } else {
        deck.cachedBpmFixed = 0;
        deck.cachedLoudnessCentiDb = 0;
        deck.cachedDeadAirMs = 0;
        deck.cachedStemsReady = 0;
        deck.cachedAnalysisStatus = 0;
    }

    deck.lifecycle = DeckLifecycleState::Loaded;

    registryDirty = true;
    return ngks::CommandResult::Applied;
}

void EngineCore::applyCachedAnalysisToDeck(ngks::DeckSnapshot& deck, const ngks::AnalysisMeta& analysis) noexcept
{
    deck.cachedBpmFixed = analysis.bpmFixed;
    deck.cachedLoudnessCentiDb = analysis.loudnessCentiDb;
    deck.cachedDeadAirMs = analysis.deadAirMs;
    deck.cachedStemsReady = analysis.stemsReady;
    deck.cachedAnalysisStatus = analysis.status;
}

void EngineCore::appendJobResults(ngks::EngineSnapshot& snapshot) noexcept
{
    ngks::JobResult result {};
    while (jobSystem.tryPopResult(result)) {
        const uint32_t writeSeq = snapshot.jobResultsWriteSeq;
        const uint32_t slot = writeSeq % static_cast<uint32_t>(ngks::EngineSnapshot::kMaxJobResults);
        snapshot.jobResults[slot] = result;
        snapshot.jobResultsWriteSeq = writeSeq + 1u;

        if (result.status == ngks::JobStatus::Complete && result.trackId != 0) {
            ngks::AnalysisMeta analysis {};
            trackRegistry.getAnalysis(result.trackId, analysis);
            analysis.lastJobId = result.jobId;
            analysis.status = 1;
            if (result.type == ngks::JobType::AnalyzeTrack) {
                analysis.bpmFixed = result.bpmFixed;
                analysis.loudnessCentiDb = result.loudness;
                analysis.deadAirMs = static_cast<uint32_t>(std::max(result.deadAirMs, 0));
            }
            if (result.type == ngks::JobType::StemsOffline) {
                analysis.stemsReady = result.stemsReady;
            }
            trackRegistry.updateAnalysis(result.trackId, analysis);
            registryDirty = true;

            for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
                if (snapshot.decks[deckIndex].currentTrackId == result.trackId) {
                    applyCachedAnalysisToDeck(snapshot.decks[deckIndex], analysis);
                    if (result.type == ngks::JobType::AnalyzeTrack
                        && validateTransition(snapshot.decks[deckIndex].lifecycle, DeckLifecycleState::Analyzed)) {
                        snapshot.decks[deckIndex].lifecycle = DeckLifecycleState::Analyzed;
                    }
                }
            }
        }
    }
}

void EngineCore::persistRegistryIfNeeded(bool force)
{
    if (!registryDirty) {
        return;
    }

    const auto now = std::chrono::steady_clock::now();
    if (!force && (now - lastRegistryPersist) < registryPersistInterval) {
        return;
    }

    if (registryStore.save(trackRegistry)) {
        std::cout << "CACHE_PERSIST_OK path=" << registryStore.pathString() << std::endl;
        registryDirty = false;
        lastRegistryPersist = now;
    }
}

void EngineCore::publishCommandOutcome(const ngks::Command& command, ngks::CommandResult result) noexcept
{
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    const uint32_t back = front ^ 1u;

    ngks::EngineSnapshot updated = snapshots[front];
    if (result == ngks::CommandResult::Applied && command.type == ngks::CommandType::SetDeckTrack) {
        result = applySetDeckTrack(updated, command);
    } else if (result == ngks::CommandResult::Applied && command.type == ngks::CommandType::SetCue) {
        result = applyCommand(updated, command);
    }
    appendJobResults(updated);
    updated.lastProcessedCommandSeq = command.seq;
    if (command.deck < ngks::MAX_DECKS) {
        updated.lastCommandResult[command.deck] = result;
        if (isDeckMutationCommand(command)) {
            if (result == ngks::CommandResult::Applied) {
                authority_[command.deck].lastAcceptedSeq = command.seq;
            }
            authority_[command.deck].commandInFlight = false;
            updated.decks[command.deck].lastAcceptedCommandSeq = authority_[command.deck].lastAcceptedSeq;
            updated.decks[command.deck].commandLocked = authority_[command.deck].locked;
        }
    }

    snapshots[back] = updated;
    frontSnapshotIndex.store(back, std::memory_order_release);
}

ngks::CommandResult EngineCore::applyCommand(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept
{
    if (command.deck >= ngks::MAX_DECKS) {
        return ngks::CommandResult::RejectedInvalidDeck;
    }

    auto& deck = snapshot.decks[command.deck];
    const bool fxTransitionAllowed = deck.lifecycle != DeckLifecycleState::Empty;
    switch (command.type) {
    case ngks::CommandType::SetDeckTrack:
        return ngks::CommandResult::Applied;
    case ngks::CommandType::LoadTrack:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Loading)) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!validateTransition(DeckLifecycleState::Loading, DeckLifecycleState::Loaded)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.hasTrack = 1;
        deck.trackUidHash = command.trackUidHash;
        deck.lengthSeconds = 240.0;
        deck.lifecycle = DeckLifecycleState::Loaded;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::UnloadTrack:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Empty)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.hasTrack = 0;
        deck.trackUidHash = 0;
        deck.currentTrackId = 0;
        deck.lifecycle = DeckLifecycleState::Empty;
        deck.transport = ngks::TransportState::Stopped;
        deck.playheadSeconds = 0.0;
        deck.cueEnabled = true;
        deck.publicFacing = false;
        deck.audible = false;
        deck.cachedBpmFixed = 0;
        deck.cachedLoudnessCentiDb = 0;
        deck.cachedDeadAirMs = 0;
        deck.cachedStemsReady = 0;
        deck.cachedAnalysisStatus = 0;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Play:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Playing)) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!deck.hasTrack) {
            return ngks::CommandResult::RejectedNoTrack;
        }
        startAudioIfNeeded();
        deck.lifecycle = DeckLifecycleState::Playing;
        deck.transport = ngks::TransportState::Starting;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Stop:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Stopped)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.lifecycle = DeckLifecycleState::Stopped;
        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Stopping;
            audioGraph.beginDeckStopFade(command.deck, fadeSamplesTotal);
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetDeckGain:
        deck.deckGain = std::clamp(command.floatValue, 0.0f, 12.0f);
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetMasterGain:
        snapshot.masterGain = std::clamp(static_cast<double>(command.floatValue), 0.0, 1.5);
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetCue:
        if (command.boolValue == 0) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Armed)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.lifecycle = DeckLifecycleState::Armed;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetFxSlotType:
        if (!fxTransitionAllowed) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!audioGraph.setDeckFxSlotType(command.deck, command.slotIndex, command.jobId)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetFxSlotEnabled:
        if (!fxTransitionAllowed) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!audioGraph.setDeckFxSlotEnabled(command.deck, command.slotIndex, command.boolValue != 0)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetFxSlotDryWet:
        if (!fxTransitionAllowed) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!audioGraph.setDeckFxSlotDryWet(command.deck, command.slotIndex, command.floatValue)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetDeckFxGain:
        if (!fxTransitionAllowed) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!audioGraph.setDeckFxGain(command.deck, command.slotIndex, command.floatValue)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::EnableDeckFxSlot:
        if (!fxTransitionAllowed) {
            return ngks::CommandResult::IllegalTransition;
        }
        if (!audioGraph.setDeckFxSlotEnabled(command.deck, command.slotIndex, command.boolValue != 0)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetMasterFxGain:
        if (!audioGraph.setMasterFxGain(command.slotIndex, command.floatValue)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::EnableMasterFxSlot:
        if (!audioGraph.setMasterFxSlotEnabled(command.slotIndex, command.boolValue != 0)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::RequestAnalyzeTrack:
    case ngks::CommandType::RequestStemsOffline:
    case ngks::CommandType::CancelJob:
        return ngks::CommandResult::Applied;
    }

    return ngks::CommandResult::None;
}

void EngineCore::process(float* left, float* right, int numSamples) noexcept
{
    const auto callbackStart = std::chrono::high_resolution_clock::now();
    telemetry_.audioCallbacks.fetch_add(1u, std::memory_order_relaxed);

    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        telemetry_.xruns.fetch_add(1u, std::memory_order_relaxed);
        telemetry_.lastRenderDurationUs.store(0u, std::memory_order_relaxed);

        const auto callbackEnd = std::chrono::high_resolution_clock::now();
        const auto callbackDurationUs = static_cast<uint32_t>(std::max<int64_t>(0,
            std::chrono::duration_cast<std::chrono::microseconds>(callbackEnd - callbackStart).count()));
        telemetry_.lastCallbackDurationUs.store(callbackDurationUs, std::memory_order_relaxed);
        updateMaxRelaxed(telemetry_.maxCallbackDurationUs, callbackDurationUs);
        pushRenderDurationSample(0u);
        return;
    }

    const auto renderStart = std::chrono::high_resolution_clock::now();

    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    const uint32_t back = front ^ 1u;

    ngks::EngineSnapshot working = snapshots[front];

    if (audioOpened.load(std::memory_order_acquire)) {
        working.flags |= ngks::SNAP_AUDIO_RUNNING;
    }

    ngks::Command command { ngks::CommandType::Stop };
    while (commandRing.pop(command)) {
        const auto result = applyCommand(working, command);
        if (command.deck < ngks::MAX_DECKS) {
            working.lastCommandResult[command.deck] = result;
            if (isDeckMutationCommand(command)) {
                if (result == ngks::CommandResult::Applied) {
                    authority_[command.deck].lastAcceptedSeq = command.seq;
                }
                authority_[command.deck].commandInFlight = false;
                working.decks[command.deck].lastAcceptedCommandSeq = authority_[command.deck].lastAcceptedSeq;
                working.decks[command.deck].commandLocked = authority_[command.deck].locked;
            }
        }
        working.lastProcessedCommandSeq = command.seq;
    }

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        if (working.decks[deckIndex].transport == ngks::TransportState::Starting) {
            working.decks[deckIndex].transport = ngks::TransportState::Playing;
        }
    }

    computeCrossfadeWeights(working, crossfaderPosition_, mixMatrix_);

    const auto graphStats = audioGraph.render(working, mixMatrix_, numSamples, left, right);

    masterBus_.setGainTrim(static_cast<float>(working.masterGain));
    const auto masterMeters = masterBus_.process(left, right, numSamples);
    working.masterRmsL = masterMeters.masterRmsL;
    working.masterRmsR = masterMeters.masterRmsR;
    working.masterPeakL = masterMeters.masterPeakL;
    working.masterPeakR = masterMeters.masterPeakR;
    working.masterLimiterActive = masterMeters.limiterEngaged;

    float instantaneousMasterPeak = 0.0f;
    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        auto& deck = working.decks[deckIndex];

        deckRmsSmoothing[deckIndex] = deckRmsSmoothing[deckIndex]
            + rmsSmoothingAlpha * (graphStats.decks[deckIndex].rms - deckRmsSmoothing[deckIndex]);
        deck.rmsL = deckRmsSmoothing[deckIndex];
        deck.rmsR = deck.rmsL;

        if (graphStats.decks[deckIndex].peak >= deckPeakSmoothing[deckIndex]) {
            deckPeakSmoothing[deckIndex] = graphStats.decks[deckIndex].peak;
            deckPeakHoldBlocks[deckIndex] = peakHoldBlocks;
        } else if (deckPeakHoldBlocks[deckIndex] > 0) {
            --deckPeakHoldBlocks[deckIndex];
        } else {
            deckPeakSmoothing[deckIndex] *= peakDecayFactor;
        }

        deck.peakL = deckPeakSmoothing[deckIndex];
        deck.peakR = deck.peakL;
        instantaneousMasterPeak = std::max(instantaneousMasterPeak, deck.peakL);

        if (deck.transport == ngks::TransportState::Stopping && !audioGraph.isDeckStopFadeActive(deckIndex)) {
            deck.transport = ngks::TransportState::Stopped;
        }

        const float masterWeight = mixMatrix_.decks[deckIndex].masterWeight;
        const float cueWeight = mixMatrix_.decks[deckIndex].cueWeight;
        deck.masterWeight = masterWeight;
        deck.cueWeight = cueWeight;
        deck.routingActive = (masterWeight > 0.001f) && isDeckRoutingActive(deck);
        const bool deckAudible = deck.routingActive && (deck.lifecycle == DeckLifecycleState::Playing);
        deck.audible = deckAudible;
        deck.publicFacing = false;
        deck.lastAcceptedCommandSeq = authority_[deckIndex].lastAcceptedSeq;

        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Stopping) {
            deck.playheadSeconds += (static_cast<double>(numSamples) / sampleRateHz);
            if (deck.lengthSeconds > 0.0 && deck.playheadSeconds > deck.lengthSeconds) {
                deck.playheadSeconds = deck.lengthSeconds;
            }
        }

        for (int slot = 0; slot < 4; ++slot) {
            deck.fxSlots[slot] = audioGraph.getDeckFxSlotState(deckIndex, slot);
        }
    }

    int publicFacingDeck = -1;
    float publicFacingWeight = -1.0f;
    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        const auto& deck = working.decks[deckIndex];
        const bool lifecycleActive = (deck.lifecycle == DeckLifecycleState::Playing);
        const bool qualifies = lifecycleActive
            && deck.routingActive
            && (deck.masterWeight > kPublicFacingWeightThreshold)
            && !authority_[deckIndex].commandInFlight;
        if (!qualifies) {
            continue;
        }

        if (deck.masterWeight > publicFacingWeight) {
            publicFacingWeight = deck.masterWeight;
            publicFacingDeck = static_cast<int>(deckIndex);
            continue;
        }

        if (deck.masterWeight == publicFacingWeight
            && (publicFacingDeck < 0 || deckIndex < static_cast<uint8_t>(publicFacingDeck))) {
            publicFacingDeck = static_cast<int>(deckIndex);
        }
    }

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        auto& deck = working.decks[deckIndex];
        deck.publicFacing = (publicFacingDeck >= 0) && (deckIndex == static_cast<uint8_t>(publicFacingDeck));
        deck.cueEnabled = !deck.publicFacing;
        authority_[deckIndex].locked = deck.publicFacing;
        deck.commandLocked = authority_[deckIndex].locked;
    }

    for (int slot = 0; slot < 8; ++slot) {
        working.masterFxSlotEnabled[slot] = audioGraph.isMasterFxSlotEnabled(slot) ? 1 : 0;
    }

    if (instantaneousMasterPeak >= masterPeakSmoothing) {
        masterPeakSmoothing = instantaneousMasterPeak;
        masterPeakHoldBlocks = peakHoldBlocks;
    } else if (masterPeakHoldBlocks > 0) {
        --masterPeakHoldBlocks;
    } else {
        masterPeakSmoothing *= peakDecayFactor;
    }

    if ((working.flags & ngks::SNAP_AUDIO_RUNNING) != 0u
        && (working.flags & ngks::SNAP_WARMUP_COMPLETE) == 0u) {
        const float warmupRms = std::max(working.masterRmsL, working.masterRmsR);
        if (warmupRms > warmupAudibleRmsThreshold) {
            if (working.warmupCounter < warmupConsecutiveBlocksRequired) {
                ++working.warmupCounter;
            }
        } else {
            working.warmupCounter = 0;
        }

        if (working.warmupCounter >= warmupConsecutiveBlocksRequired) {
            working.flags |= ngks::SNAP_WARMUP_COMPLETE;
        }
    }

    snapshots[back] = working;
    frontSnapshotIndex.store(back, std::memory_order_release);

    const auto renderEnd = std::chrono::high_resolution_clock::now();
    const auto durationUs = std::chrono::duration_cast<std::chrono::microseconds>(renderEnd - renderStart).count();
    const uint32_t renderDurationUs = static_cast<uint32_t>(std::max<int64_t>(0, durationUs));
    telemetry_.renderCycles.fetch_add(1u, std::memory_order_relaxed);
    telemetry_.lastRenderDurationUs.store(renderDurationUs, std::memory_order_relaxed);
    updateMaxRelaxed(telemetry_.maxRenderDurationUs, renderDurationUs);
    pushRenderDurationSample(renderDurationUs);

    const auto callbackEnd = std::chrono::high_resolution_clock::now();
    const auto callbackDurationUs = static_cast<uint32_t>(std::max<int64_t>(0,
        std::chrono::duration_cast<std::chrono::microseconds>(callbackEnd - callbackStart).count()));
    telemetry_.lastCallbackDurationUs.store(callbackDurationUs, std::memory_order_relaxed);
    updateMaxRelaxed(telemetry_.maxCallbackDurationUs, callbackDurationUs);
}
