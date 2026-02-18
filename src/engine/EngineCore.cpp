#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>
#include <cmath>
#include <iostream>

namespace
{
constexpr float audibleRmsThresholdLinear = 0.0316227766f;
constexpr float warmupAudibleRmsThreshold = 0.005f;
constexpr uint32_t warmupConsecutiveBlocksRequired = 50u;
constexpr float rmsSmoothingAlpha = 0.2f;
constexpr float peakDecayFactor = 0.96f;
constexpr int peakHoldBlocks = 8;
constexpr std::chrono::seconds registryPersistInterval(1);
}

EngineCore::EngineCore()
    : audioIO(std::make_unique<AudioIOJuce>(*this))
{
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
        if (command.deck >= ngks::MAX_DECKS || command.deck >= 4) {
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
    if (x < 0.0f) {
        x = 0.0f;
    }
    if (x > 1.0f) {
        x = 1.0f;
    }

    const float a = std::cos(x * 1.57079632679f);
    const float b = std::sin(x * 1.57079632679f);

    mixMatrix_.decks[0].masterWeight = a;
    mixMatrix_.decks[1].masterWeight = b;

    mixMatrix_.decks[0].cueWeight = 1.0f;
    mixMatrix_.decks[1].cueWeight = 1.0f;

    for (int i = 2; i < MAX_DECKS; ++i) {
        mixMatrix_.decks[i].masterWeight = 0.0f;
        mixMatrix_.decks[i].cueWeight = 0.0f;
    }
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
        deck.deckGain = std::clamp(command.floatValue, 0.0f, 1.5f);
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
    case ngks::CommandType::SetDeckFxGain:
        if (!audioGraph.setDeckFxGain(command.deck, command.slotIndex, command.floatValue)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::EnableDeckFxSlot:
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
    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        return;
    }

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

    const auto graphStats = audioGraph.render(working, mixMatrix_, numSamples, left, right);

    masterRmsSmoothing = masterRmsSmoothing + rmsSmoothingAlpha * (graphStats.masterRms - masterRmsSmoothing);
    working.masterRmsL = masterRmsSmoothing;
    working.masterRmsR = masterRmsSmoothing;

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
        const bool deckAudible =
            (deck.lifecycle == DeckLifecycleState::Playing)
            && (masterWeight > 0.001f)
            && (std::max(deck.rmsL, deck.rmsR) > audibleRmsThresholdLinear);
        deck.audible = deckAudible;
        deck.publicFacing = deckAudible;
        deck.cueEnabled = !deck.publicFacing;
        authority_[deckIndex].locked = deck.publicFacing;
        deck.commandLocked = authority_[deckIndex].locked;
        deck.lastAcceptedCommandSeq = authority_[deckIndex].lastAcceptedSeq;

        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Stopping) {
            deck.playheadSeconds += (static_cast<double>(numSamples) / sampleRateHz);
            if (deck.lengthSeconds > 0.0 && deck.playheadSeconds > deck.lengthSeconds) {
                deck.playheadSeconds = deck.lengthSeconds;
            }
        }

        for (int slot = 0; slot < 8; ++slot) {
            deck.fxSlotEnabled[slot] = audioGraph.isDeckFxSlotEnabled(deckIndex, slot) ? 1 : 0;
        }
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

    working.masterPeakL = masterPeakSmoothing;
    working.masterPeakR = masterPeakSmoothing;

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
}
