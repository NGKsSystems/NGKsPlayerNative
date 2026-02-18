#include "engine/EngineCore.h"

#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>

namespace
{
constexpr float audibleRmsThresholdLinear = 0.0316227766f;
constexpr float warmupAudibleRmsThreshold = 0.005f;
constexpr uint32_t warmupConsecutiveBlocksRequired = 50u;
constexpr float rmsSmoothingAlpha = 0.2f;
constexpr float peakDecayFactor = 0.96f;
constexpr int peakHoldBlocks = 8;
}

EngineCore::EngineCore()
    : audioIO(std::make_unique<AudioIOJuce>(*this))
{
    for (uint8_t deck = 0; deck < ngks::MAX_DECKS; ++deck) {
        snapshots[0].decks[deck].id = deck;
        snapshots[1].decks[deck].id = deck;
    }
}

EngineCore::~EngineCore()
{
    if (audioIO != nullptr) {
        audioIO->stop();
    }
}

ngks::EngineSnapshot EngineCore::getSnapshot() const
{
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    return snapshots[front];
}

void EngineCore::enqueueCommand(const ngks::Command& command)
{
    if (command.type == ngks::CommandType::Play) {
        startAudioIfNeeded();
    }

    if (!commandRing.push(command)) {
        const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
        ngks::EngineSnapshot dropped = snapshots[front];
        if (command.deck < ngks::MAX_DECKS) {
            dropped.lastCommandResult[command.deck] = ngks::CommandResult::RejectedQueueFull;
            dropped.lastProcessedCommandSeq = command.seq;
            const uint32_t back = front ^ 1u;
            snapshots[back] = dropped;
            frontSnapshotIndex.store(back, std::memory_order_release);
        }
    }
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

ngks::CommandResult EngineCore::applyCommand(ngks::EngineSnapshot& snapshot, const ngks::Command& command) noexcept
{
    if (command.deck >= ngks::MAX_DECKS) {
        return ngks::CommandResult::RejectedInvalidDeck;
    }

    auto& deck = snapshot.decks[command.deck];
    switch (command.type) {
    case ngks::CommandType::LoadTrack:
        deck.hasTrack = 1;
        deck.trackUidHash = command.trackUidHash;
        deck.lengthSeconds = 240.0;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::UnloadTrack:
        deck.hasTrack = 0;
        deck.trackUidHash = 0;
        deck.transport = ngks::TransportState::Stopped;
        deck.playheadSeconds = 0.0;
        deck.cueEnabled = 0;
        deck.publicFacing = 0;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Play:
        if (!deck.hasTrack) {
            return ngks::CommandResult::RejectedNoTrack;
        }
        startAudioIfNeeded();
        deck.transport = ngks::TransportState::Starting;
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Stop:
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
        if (deck.publicFacing) {
            return ngks::CommandResult::RejectedPublicFacing;
        }
        deck.cueEnabled = command.boolValue ? 1 : 0;
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
        }
        working.lastProcessedCommandSeq = command.seq;
    }

    for (uint8_t deckIndex = 0; deckIndex < ngks::MAX_DECKS; ++deckIndex) {
        if (working.decks[deckIndex].transport == ngks::TransportState::Starting) {
            working.decks[deckIndex].transport = ngks::TransportState::Playing;
        }
    }

    const auto graphStats = audioGraph.render(working, routingMatrix, numSamples, left, right);

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

        const bool isPlayingOrStopping = (deck.transport == ngks::TransportState::Playing)
            || (deck.transport == ngks::TransportState::Stopping);
        const bool routedToMaster = routingMatrix.get(deckIndex).toMasterWeight > 0.0f;
        const bool audible = std::max(deck.rmsL, deck.rmsR) > audibleRmsThresholdLinear;
        deck.publicFacing = (isPlayingOrStopping && routedToMaster && audible) ? 1 : 0;

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