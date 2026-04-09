#include "engine/EngineCore.h"
// MIXER_PANEL_BUILDOUT_V1A
#include "engine/audio/AudioIO_Juce.h"
#include "engine/DiagLog.h"
#include "engine/domain/CrossfadeAssignment.h"

#include <algorithm>
#include <cmath>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <thread>

namespace
{
constexpr float warmupAudibleRmsThreshold = 0.005f;
constexpr uint32_t warmupConsecutiveBlocksRequired = 50u;
constexpr float rmsSmoothingAlpha = 0.2f;
constexpr float peakDecayFactor = 0.96f;
constexpr int peakHoldBlocks = 8;
constexpr std::chrono::seconds registryPersistInterval(1);
constexpr float kPublicFacingWeightThreshold = 0.15f;
constexpr float kTwoPi = 6.28318530718f;
constexpr int64_t kNsPerMs = 1000000;
constexpr int64_t kWatchdogGraceMs = 500;
constexpr uint64_t kWatchdogGraceCallbacks = 3u;
constexpr int64_t kRecoveryCooldownMs = 2000;
constexpr uint32_t kMaxRecoveryFailures = 3u;
constexpr int32_t kWatchdogStateGrace = 0;
constexpr int32_t kWatchdogStateActive = 1;
constexpr int32_t kWatchdogStateStall = 2;
constexpr int32_t kWatchdogStateFailed = 3;
constexpr CrossfadeAssignment kDefaultCrossfadeAssignment {
    { 0, -1 },
    { 1, -1 },
    1,
    1
};

void updateMaxRelaxed(std::atomic<uint32_t>& target, uint32_t value) noexcept
{
    uint32_t previous = target.load(std::memory_order_relaxed);
    while (value > previous && !target.compare_exchange_weak(previous, value, std::memory_order_relaxed)) {
    }
}

void updateMaxRelaxedInt(std::atomic<int32_t>& target, int32_t value) noexcept
{
    int32_t previous = target.load(std::memory_order_relaxed);
    while (value > previous && !target.compare_exchange_weak(previous, value, std::memory_order_relaxed)) {
    }
}

void updateMaxRelaxedU64(std::atomic<uint64_t>& target, uint64_t value) noexcept
{
    uint64_t previous = target.load(std::memory_order_relaxed);
    while (value > previous && !target.compare_exchange_weak(previous, value, std::memory_order_relaxed)) {
    }
}

float sanitizeFiniteNonNegative(float v) noexcept
{
    if (!std::isfinite(v) || v < 0.0f) {
        return 0.0f;
    }
    return v;
}

double sanitizeFiniteNonNegative(double v) noexcept
{
    if (!std::isfinite(v) || v < 0.0) {
        return 0.0;
    }
    return v;
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

// ── DJ device-name normalization: lowercase, trim whitespace, strip trailing
//    parenthetical index like " (2)", " (3)" that Windows appends on
//    re-enumeration ──
std::string normalize(const std::string& raw)
{
    std::string s = raw;
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.erase(s.begin());
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.pop_back();
    if (s.size() >= 4 && s.back() == ')') {
        auto parenPos = s.rfind('(');
        if (parenPos != std::string::npos && parenPos > 0) {
            bool allDigits = true;
            for (size_t i = parenPos + 1; i < s.size() - 1; ++i) {
                if (s[i] < '0' || s[i] > '9') { allDigits = false; break; }
            }
            if (allDigits) {
                s = s.substr(0, parenPos);
                while (!s.empty() && s.back() == ' ') s.pop_back();
            }
        }
    }
    for (auto& c : s) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + ('a' - 'A'));
    }
    return s;
}

bool isBuiltInSpeaker(const std::string& name)
{
    std::string lower = name;
    for (auto& c : lower)
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c + ('a' - 'A'));
    if (lower.find("speakers") != std::string::npos &&
        (lower.find("realtek") != std::string::npos ||
         lower.find("intel") != std::string::npos ||
         lower.find("high definition") != std::string::npos ||
         lower.find("hd audio") != std::string::npos))
        return true;
    return false;
}

void computeCrossfadeWeights(const ngks::EngineSnapshot& snapshot, float x, MixMatrix& mixMatrix, int outputMode) noexcept
{
    x = std::clamp(x, 0.0f, 1.0f);
    const float leftGain = std::cos(x * 1.57079632679f);
    const float rightGain = std::sin(x * 1.57079632679f);

    for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
        mixMatrix.decks[i].masterWeight = 0.0f;
        mixMatrix.decks[i].cueWeight = snapshot.decks[i].cueEnabled ? 1.0f : 0.0f;
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

    // DECK_S is the Simple Player — always route to master at full weight,
    // independent of crossfader. Applied after DJ normalization so it
    // never attenuates DJ decks and vice versa.
    mixMatrix.decks[ngks::DECK_S].masterWeight = 1.0f;

    // Apply per-deck mute: zero master contribution for muted decks
    for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
        if (snapshot.decks[i].muted) {
            mixMatrix.decks[i].masterWeight = 0.0f;
        }
        // In split mono mode, CUE MON isolates deck to cue bus (right ear only)
        if (outputMode == 1 && snapshot.decks[i].cueEnabled) {
            mixMatrix.decks[i].masterWeight = 0.0f;
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

    setRunState(EngineRunState::Ready);
}

EngineCore::~EngineCore()
{
    setRunState(EngineRunState::RtStopping);

    if (audioIO != nullptr) {
        audioIO->stop();
    }

    audioOpened.store(false, std::memory_order_release);
    telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
    telemetry_.rtAudioEnabled.store(0u, std::memory_order_relaxed);

    persistRegistryIfNeeded(true);
    jobSystem.stop();

    setRunState(EngineRunState::Cold);
}

ngks::EngineSnapshot EngineCore::getSnapshot() const
{
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    ngks::EngineSnapshot copy = snapshots[front];
    sanitizeSnapshot(copy);
    // Overlay live audioOpened state so UI never sees stale SNAP_AUDIO_RUNNING
    if (!audioOpened.load(std::memory_order_acquire)) {
        copy.flags &= ~ngks::SNAP_AUDIO_RUNNING;
    }
    // DJ device-lost: force SNAP_DJ_DEVICE_LOST, strip SNAP_AUDIO_RUNNING
    if (djDeviceLost_.load(std::memory_order_acquire)) {
        copy.flags |= ngks::SNAP_DJ_DEVICE_LOST;
        copy.flags &= ~ngks::SNAP_AUDIO_RUNNING;
    }
    return copy;
}

void EngineCore::enqueueCommand(const ngks::Command& command)
{
    telemetry_.cmdQueued.fetch_add(1u, std::memory_order_relaxed);

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
        // NOTE: Do NOT validate lifecycle here — the Play command may arrive right after
        // a LoadTrack command that is still in the SPSC ring waiting for the RT callback.
        // Let applyCommand() in process() handle validation where the snapshot is up-to-date.
        startAudioIfNeeded();
    }

    if (!commandRing.push(command)) {
        telemetry_.cmdDropped.fetch_add(1u, std::memory_order_relaxed);

        if (command.deck < ngks::MAX_DECKS) {
            std::lock_guard<std::mutex> lock(outcomeMutex_);
            ngks::EngineSnapshot dropped = hasPendingOutcome_
                ? pendingOutcome_
                : snapshots[frontSnapshotIndex.load(std::memory_order_acquire)];
            dropped.lastCommandResult[command.deck] = ngks::CommandResult::RejectedQueueFull;
            dropped.lastProcessedCommandSeq = command.seq;
            if (isDeckMutationCommand(command)) {
                authority_[command.deck].commandInFlight = false;
                dropped.decks[command.deck].lastAcceptedCommandSeq = authority_[command.deck].lastAcceptedSeq;
                dropped.decks[command.deck].commandLocked = authority_[command.deck].locked;
            }
            pendingOutcome_ = dropped;
            hasPendingOutcome_ = true;
        }
    } else {
        const uint64_t queued = telemetry_.cmdQueued.load(std::memory_order_relaxed);
        const uint64_t dropped = telemetry_.cmdDropped.load(std::memory_order_relaxed);
        const uint32_t approxDepth = static_cast<uint32_t>(queued - dropped);
        updateMaxRelaxed(telemetry_.cmdHighWaterMark, approxDepth);
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
    case ngks::CommandType::Pause:
    case ngks::CommandType::Seek:
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
    case ngks::CommandType::SetEqBandGain:
    case ngks::CommandType::SetEqBypass:
    case ngks::CommandType::SetDeckMute:
    case ngks::CommandType::SetDeckCueMonitor:
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
        return to == DeckLifecycleState::Analyzed || to == DeckLifecycleState::Playing
            || to == DeckLifecycleState::Stopped || to == DeckLifecycleState::Empty;
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

bool EngineCore::startAudioIfNeeded(bool forceReopen)
{
    using Clock = std::chrono::steady_clock;
    const auto tFunc = Clock::now();
    auto funcElapsed = [&tFunc]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tFunc).count();
    };

    ngks::audioTrace("START_AUDIO_ENTER", "forceReopen=%d audioOpened=%d preferredName=\"%s\"",
                     forceReopen ? 1 : 0,
                     audioOpened.load(std::memory_order_acquire) ? 1 : 0,
                     preferredAudioDeviceName_.c_str());

    // ── DJ device-lost hard gate: block all audio opens ──
    //    Bypassed when djRecoveryInFlight_ is set — recovery owns this path.
    if (djMode_.load(std::memory_order_acquire)
        && djDeviceLost_.load(std::memory_order_acquire)
        && !djRecoveryInFlight_.load(std::memory_order_acquire)) {
        ngks::audioTrace("DJ_GATE_BLOCK_AUDIO_OPEN",
            "fn=startAudioIfNeeded djMode=1 djDeviceLost=1 preferred=\"%s\"",
            preferredAudioDeviceName_.c_str());
        ngks::diagLog("DJ_GATE_BLOCK_AUDIO_OPEN: startAudioIfNeeded blocked — device-lost active");
        return false;
    }

    ngks::audioTrace("MUTEX_WAIT", "mutex=controlMutex_ caller=startAudioIfNeeded");
    const auto tLock = Clock::now();
    std::lock_guard<std::mutex> lock(controlMutex_);
    const auto lockMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tLock).count();
    ngks::audioTrace("MUTEX_ACQUIRED", "mutex=controlMutex_ caller=startAudioIfNeeded waitMs=%lld", lockMs);
    if (lockMs > 500) ngks::audioTrace("STALL_WARNING", "step=controlMutex_acquire caller=startAudioIfNeeded waitMs=%lld", lockMs);
    if (lockMs > 1500) ngks::audioTrace("STALL_CRITICAL", "step=controlMutex_acquire caller=startAudioIfNeeded waitMs=%lld", lockMs);

    if (offlineMode_) {
        audioOpened.store(true, std::memory_order_release);
        telemetry_.rtDeviceOpenOk.store(1u, std::memory_order_relaxed);
        telemetry_.rtLastDeviceErrorCode.store(0, std::memory_order_relaxed);
        setRunState(EngineRunState::Ready);
        return true;
    }

    if (audioIO == nullptr) {
        telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
        telemetry_.rtLastDeviceErrorCode.store(-10, std::memory_order_relaxed);
        setRunState(EngineRunState::RtFailed);
        return false;
    }

    if (forceReopen && audioIO != nullptr) {
        setRunState(EngineRunState::RtStopping);
        audioIO->stop();
        audioOpened.store(false, std::memory_order_release);
        telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
    }

    if (!forceReopen && audioOpened.load(std::memory_order_acquire)) {
        setRunState(EngineRunState::RtRunning);
        return true;
    }

    setRunState(EngineRunState::RtStarting);

    AudioIOJuce::StartRequest request {};
    request.preferredDeviceId = preferredAudioDeviceId_;
    request.preferredDeviceName = preferredAudioDeviceName_;
    request.preferredSampleRate = preferredAudioSampleRate_;
    request.preferredBufferSize = preferredAudioBufferFrames_;
    request.preferredOutputChannels = preferredAudioOutputChannels_;

    const auto result = audioIO->start(request);
    if (!result.ok) {
        ngks::diagLog("startAudioIfNeeded: audioIO->start FAILED — %s", result.message.c_str());
        telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
        telemetry_.rtLastDeviceErrorCode.store(-1, std::memory_order_relaxed);
        setRunState(EngineRunState::RtFailed);
        return false;
    }

    sampleRateHz = result.sampleRate;
    audioOpened.store(true, std::memory_order_release);
    ngks::audioTrace("START_AUDIO_OPENED", "device=\"%s\" sr=%.0f buf=%d",
                     result.deviceName.c_str(), result.sampleRate, result.actualBufferSize);
    telemetry_.rtDeviceOpenOk.store(1u, std::memory_order_relaxed);
    telemetry_.rtSampleRate.store(static_cast<int32_t>(std::max(0.0, result.sampleRate)), std::memory_order_relaxed);
    telemetry_.rtBufferFrames.store(result.actualBufferSize, std::memory_order_relaxed);
    telemetry_.rtRequestedSampleRate.store(static_cast<int32_t>(std::max(0.0, result.requestedSampleRate)), std::memory_order_relaxed);
    telemetry_.rtRequestedBufferFrames.store(result.requestedBufferSize, std::memory_order_relaxed);
    telemetry_.rtRequestedChannelsOut.store(result.requestedOutputChannels, std::memory_order_relaxed);
    telemetry_.rtChannelsIn.store(result.inputChannels, std::memory_order_relaxed);
    telemetry_.rtChannelsOut.store(result.outputChannels, std::memory_order_relaxed);
    telemetry_.rtAgFallback.store(result.fallbackUsed ? 1u : 0u, std::memory_order_relaxed);
    telemetry_.rtDeviceIdHash.store(result.deviceIdHash, std::memory_order_relaxed);
    telemetry_.rtLastDeviceErrorCode.store(0, std::memory_order_relaxed);
    std::strncpy(rtDeviceId_, result.deviceId.c_str(), sizeof(rtDeviceId_) - 1u);
    rtDeviceId_[sizeof(rtDeviceId_) - 1u] = '\\0';
    std::strncpy(rtDeviceName_, result.deviceName.c_str(), sizeof(rtDeviceName_) - 1u);
    rtDeviceName_[sizeof(rtDeviceName_) - 1u] = '\\0';

    telemetry_.rtLastCallbackTickMs.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count(),
        std::memory_order_relaxed);

    telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);
    telemetry_.rtWatchdogStateCode.store(kWatchdogStateGrace, std::memory_order_relaxed);
    setRunState(EngineRunState::RtRunning);
    {
        const auto totalMs = funcElapsed();
        ngks::audioTrace("START_AUDIO_EXIT", "ok=1 totalMs=%lld", totalMs);
        if (totalMs > 500) ngks::audioTrace("STALL_WARNING", "step=startAudioIfNeeded totalMs=%lld", totalMs);
        if (totalMs > 1500) ngks::audioTrace("STALL_CRITICAL", "step=startAudioIfNeeded totalMs=%lld", totalMs);
    }
    return true;
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
    snapshot.rtAudioEnabled = telemetry_.rtAudioEnabled.load(std::memory_order_relaxed) != 0u;
    snapshot.rtDeviceOpenOk = telemetry_.rtDeviceOpenOk.load(std::memory_order_relaxed) != 0u;
    snapshot.rtSampleRate = telemetry_.rtSampleRate.load(std::memory_order_relaxed);
    snapshot.rtBufferFrames = telemetry_.rtBufferFrames.load(std::memory_order_relaxed);
    snapshot.rtRequestedSampleRate = telemetry_.rtRequestedSampleRate.load(std::memory_order_relaxed);
    snapshot.rtRequestedBufferFrames = telemetry_.rtRequestedBufferFrames.load(std::memory_order_relaxed);
    snapshot.rtRequestedChannelsOut = telemetry_.rtRequestedChannelsOut.load(std::memory_order_relaxed);
    snapshot.rtChannelsIn = telemetry_.rtChannelsIn.load(std::memory_order_relaxed);
    snapshot.rtChannelsOut = telemetry_.rtChannelsOut.load(std::memory_order_relaxed);
    snapshot.rtAgFallback = telemetry_.rtAgFallback.load(std::memory_order_relaxed) != 0u;
    snapshot.rtDeviceIdHash = telemetry_.rtDeviceIdHash.load(std::memory_order_relaxed);
    snapshot.rtCallbackCount = telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
    snapshot.rtXRunCount = telemetry_.rtXRunCount.load(std::memory_order_relaxed);
    snapshot.rtXRunCountTotal = telemetry_.rtXRunCount.load(std::memory_order_relaxed);
    snapshot.rtXRunCountWindow = telemetry_.rtXRunCountWindow.load(std::memory_order_relaxed);
    snapshot.rtLastCallbackNs = telemetry_.rtLastCallbackNs.load(std::memory_order_relaxed);
    snapshot.rtJitterAbsNsMaxWindow = telemetry_.rtJitterAbsNsMaxWindow.load(std::memory_order_relaxed);
    snapshot.rtCallbackIntervalNsLast = telemetry_.rtCallbackIntervalNsLast.load(std::memory_order_relaxed);
    snapshot.rtCallbackIntervalNsMaxWindow = telemetry_.rtCallbackIntervalNsMaxWindow.load(std::memory_order_relaxed);
    snapshot.rtLastCallbackUs = telemetry_.rtLastCallbackUs.load(std::memory_order_relaxed);
    snapshot.rtMaxCallbackUs = telemetry_.rtMaxCallbackUs.load(std::memory_order_relaxed);
    snapshot.rtMeterPeakDb10 = telemetry_.rtMeterPeakDb10.load(std::memory_order_relaxed);
    snapshot.rtWatchdogOk = telemetry_.rtWatchdogOk.load(std::memory_order_relaxed) != 0u;
    snapshot.rtWatchdogStateCode = telemetry_.rtWatchdogStateCode.load(std::memory_order_relaxed);
    snapshot.rtWatchdogTripCount = telemetry_.rtWatchdogTripCount.load(std::memory_order_relaxed);
    snapshot.rtDeviceRestartCount = telemetry_.rtDeviceRestartCount.load(std::memory_order_relaxed);
    snapshot.rtLastDeviceErrorCode = telemetry_.rtLastDeviceErrorCode.load(std::memory_order_relaxed);
    snapshot.rtRecoveryRequested = telemetry_.rtRecoveryRequested.load(std::memory_order_relaxed) != 0u;
    snapshot.rtRecoveryFailedState = telemetry_.rtRecoveryFailedState.load(std::memory_order_relaxed) != 0u;
    snapshot.rtLastCallbackTickMs = telemetry_.rtLastCallbackTickMs.load(std::memory_order_relaxed);
    snapshot.cmdQueued = telemetry_.cmdQueued.load(std::memory_order_relaxed);
    snapshot.cmdDropped = telemetry_.cmdDropped.load(std::memory_order_relaxed);
    snapshot.cmdCoalesced = telemetry_.cmdCoalesced.load(std::memory_order_relaxed);
    snapshot.cmdHighWaterMark = telemetry_.cmdHighWaterMark.load(std::memory_order_relaxed);
    snapshot.snapshotPublishes = telemetry_.snapshotPublishes.load(std::memory_order_relaxed);
    snapshot.engineRunState = telemetry_.engineRunState.load(std::memory_order_relaxed);
    std::strncpy(snapshot.rtDeviceId, rtDeviceId_, sizeof(snapshot.rtDeviceId) - 1u);
    snapshot.rtDeviceId[sizeof(snapshot.rtDeviceId) - 1u] = '\0';
    std::strncpy(snapshot.rtDeviceName, rtDeviceName_, sizeof(snapshot.rtDeviceName) - 1u);
    snapshot.rtDeviceName[sizeof(snapshot.rtDeviceName) - 1u] = '\0';

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

bool EngineCore::startRtAudioProbe(float toneHz, float toneDb) noexcept
{
    if (toneHz < 20.0f) {
        toneHz = 20.0f;
    }
    if (toneHz > 20000.0f) {
        toneHz = 20000.0f;
    }

    const float toneLinear = std::pow(10.0f, toneDb / 20.0f);
    rtToneHz_.store(toneHz, std::memory_order_relaxed);
    rtToneLinear_.store(toneLinear, std::memory_order_relaxed);
    rtTonePhase_ = 0.0f;
    rtWindowLastXRunTotal_ = 0u;
    rtLastObservedCallbackCount_ = 0u;
    rtConsecutiveRecoveryFailures_ = 0u;
    rtLastRecoveryAttemptMs_ = 0;
    telemetry_.rtCallbackCount.store(0u, std::memory_order_relaxed);
    telemetry_.rtXRunCount.store(0u, std::memory_order_relaxed);
    telemetry_.rtXRunCountWindow.store(0u, std::memory_order_relaxed);
    telemetry_.rtLastCallbackNs.store(0u, std::memory_order_relaxed);
    telemetry_.rtJitterAbsNsMaxWindow.store(0u, std::memory_order_relaxed);
    telemetry_.rtCallbackIntervalNsLast.store(0u, std::memory_order_relaxed);
    telemetry_.rtCallbackIntervalNsMaxWindow.store(0u, std::memory_order_relaxed);
    telemetry_.rtWatchdogStateCode.store(kWatchdogStateGrace, std::memory_order_relaxed);
    telemetry_.rtWatchdogTripCount.store(0u, std::memory_order_relaxed);
    telemetry_.rtDeviceRestartCount.store(0u, std::memory_order_relaxed);
    telemetry_.rtRecoveryRequested.store(0u, std::memory_order_relaxed);
    telemetry_.rtRecoveryFailedState.store(0u, std::memory_order_relaxed);
    telemetry_.rtAudioEnabled.store(1u, std::memory_order_relaxed);
    telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);
    rtProbeStartTickMs_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    rtLastProgressTickMs_ = rtProbeStartTickMs_;
    setRunState(EngineRunState::RtStarting);
    return startAudioIfNeeded();
}

void EngineCore::setPreferredAudioDeviceId(const std::string& deviceId)
{
    preferredAudioDeviceId_ = deviceId;
    preferredAudioDeviceName_.clear();
}

void EngineCore::setPreferredAudioDeviceName(const std::string& deviceName)
{
    preferredAudioDeviceName_ = deviceName;
    preferredAudioDeviceId_.clear();
}

void EngineCore::setPreferredAudioFormat(double sampleRate, int bufferFrames, int channelsOut)
{
    preferredAudioSampleRate_ = sampleRate;
    preferredAudioBufferFrames_ = bufferFrames;
    preferredAudioOutputChannels_ = channelsOut;
}

void EngineCore::clearPreferredAudioDevice()
{
    preferredAudioDeviceId_.clear();
    preferredAudioDeviceName_.clear();
}

bool EngineCore::reopenAudioWithPreferredConfig() noexcept
{
    // Thin wrapper for callers that don't need DeviceSwitchResult.
    const auto result = reopenAudioControlled();
    return result.ok;
}

static const char* transportName(ngks::TransportState t) {
    switch (t) {
        case ngks::TransportState::Stopped:  return "Stopped";
        case ngks::TransportState::Starting: return "Starting";
        case ngks::TransportState::Playing:  return "Playing";
        case ngks::TransportState::Stopping: return "Stopping";
        case ngks::TransportState::Paused:   return "Paused";
        default: return "?";
    }
}

PlaybackStateCapture EngineCore::capturePlaybackState() const noexcept
{
    PlaybackStateCapture cap{};
    const auto snap = getSnapshot();
    for (int i = 0; i < ngks::MAX_DECKS; ++i) {
        auto& cs = cap.decks[i];
        const auto& ds = snap.decks[i];
        cs.transport = ds.transport;
        cs.playheadSeconds = ds.playheadSeconds;
        cs.lengthSeconds = ds.lengthSeconds;
        cs.hasTrack = ds.hasTrack != 0;
        cs.muted = ds.muted;
        cs.cueEnabled = ds.cueEnabled;
        std::strncpy(cs.trackLabel, ds.currentTrackLabel, sizeof(cs.trackLabel) - 1);
        cs.trackLabel[sizeof(cs.trackLabel) - 1] = '\0';
    }
    return cap;
}

DeviceSwitchResult EngineCore::reopenAudioControlled() noexcept
{
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto elapsed = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    };

    DeviceSwitchResult result{};
    result.requestedDevice = preferredAudioDeviceName_;

    ngks::audioTrace("REOPEN_ENTER", "requested=\"%s\" previous=\"%s\" audioOpened=%d switchInFlight=%d",
                     preferredAudioDeviceName_.c_str(), rtDeviceName_,
                     audioOpened.load(std::memory_order_acquire) ? 1 : 0,
                     deviceSwitchInFlight_.load(std::memory_order_acquire) ? 1 : 0);

    // ── DJ device-lost hard gate: block all audio reopens ──
    if (djMode_.load(std::memory_order_acquire) && djDeviceLost_.load(std::memory_order_acquire)) {
        ngks::audioTrace("DJ_GATE_BLOCK_AUDIO_OPEN",
            "fn=reopenAudioControlled djMode=1 djDeviceLost=1 requested=\"%s\"",
            preferredAudioDeviceName_.c_str());
        ngks::diagLog("DJ_GATE_BLOCK_AUDIO_OPEN: reopenAudioControlled blocked — device-lost active");
        result.ok = false;
        result.activeDevice = std::string(rtDeviceName_);
        result.elapsedMs = elapsed();
        return result;
    }

    if (offlineMode_) {
        result.ok = startAudioIfNeeded(true);
        result.activeDevice = std::string(rtDeviceName_);
        result.elapsedMs = elapsed();
        return result;
    }

    // ── 1. Guard: suppress watchdog/recovery during switch ──
    deviceSwitchInFlight_.store(true, std::memory_order_release);
    ngks::audioTrace("REOPEN_STEP1", "deviceSwitchInFlight=true [t=%lldms]", elapsed());

    // ── 2. Capture playback state BEFORE anything changes ──
    result.previousDevice = std::string(rtDeviceName_);
    ngks::audioTrace("REOPEN_CAPTURE_BEGIN", "[t=%lldms]", elapsed());
    result.capturedState = capturePlaybackState();
    ngks::audioTrace("REOPEN_CAPTURE_END", "[t=%lldms]", elapsed());

    ngks::diagLog("DEVICE_SWITCH: begin [t=0ms] — requested='%s' previous='%s'",
                  result.requestedDevice.c_str(), result.previousDevice.c_str());

    for (int i = 0; i < ngks::MAX_DECKS; ++i) {
        const auto& ds = result.capturedState.decks[i];
        if (ds.hasTrack) {
            ngks::diagLog("DEVICE_SWITCH: captured deck=%d transport=%s playhead=%.3f dur=%.1f muted=%d cue=%d label='%s'",
                          i, transportName(ds.transport), ds.playheadSeconds, ds.lengthSeconds,
                          ds.muted ? 1 : 0, ds.cueEnabled ? 1 : 0, ds.trackLabel);
        }
    }

    // ── 3. Controlled shutdown: remove callback, close device ──
    // ALWAYS force-close.  Even if audioOpened is already false
    // (e.g., audioDeviceStopped() fired before us), the JUCE
    // AudioDeviceManager may still hold a zombie device internally
    // that would interfere with opening a new one.
    {
        ngks::audioTrace("MUTEX_WAIT", "mutex=controlMutex_ caller=reopenAudioControlled_step3");
        const auto tLock = Clock::now();
        std::lock_guard<std::mutex> lock(controlMutex_);
        const auto lockMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tLock).count();
        ngks::audioTrace("MUTEX_ACQUIRED", "mutex=controlMutex_ caller=reopenAudioControlled_step3 waitMs=%lld", lockMs);
        if (lockMs > 500) ngks::audioTrace("STALL_WARNING", "step=controlMutex_acquire caller=reopenAudioControlled waitMs=%lld", lockMs);
        if (lockMs > 1500) ngks::audioTrace("STALL_CRITICAL", "step=controlMutex_acquire caller=reopenAudioControlled waitMs=%lld", lockMs);
        if (audioIO != nullptr) {
            ngks::audioTrace("REOPEN_STOP_BEGIN", "audioOpened=%d [t=%lldms]",
                             audioOpened.load(std::memory_order_acquire) ? 1 : 0, elapsed());
            ngks::diagLog("DEVICE_SWITCH: force-close begin [t=%lldms]", elapsed());
            const auto tStop = Clock::now();
            setRunState(EngineRunState::RtStopping);
            audioIO->stop();
            const auto stopMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tStop).count();
            audioOpened.store(false, std::memory_order_release);
            telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
            ngks::audioTrace("REOPEN_STOP_END", "stopMs=%lld [t=%lldms]", stopMs, elapsed());
            if (stopMs > 1000) ngks::audioTrace("STALL_WARNING", "step=reopenStop elapsedMs=%lld", stopMs);
            if (stopMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=reopenStop elapsedMs=%lld", stopMs);
            ngks::diagLog("DEVICE_SWITCH: device released [t=%lldms]", elapsed());
        }
    }

    // Clear any pending recovery — we are doing our own reopen
    telemetry_.rtRecoveryRequested.store(0u, std::memory_order_relaxed);
    telemetry_.rtRecoveryFailedState.store(0u, std::memory_order_relaxed);
    rtConsecutiveRecoveryFailures_ = 0u;

    // ── 4. Open new device ──
    ngks::audioTrace("REOPEN_OPEN_BEGIN", "[t=%lldms]", elapsed());
    ngks::diagLog("DEVICE_SWITCH: opening new device… [t=%lldms]", elapsed());
    const auto tOpen = Clock::now();
    const bool openOk = startAudioIfNeeded(false);
    const auto openMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tOpen).count();
    ngks::audioTrace("REOPEN_OPEN_END", "ok=%d openMs=%lld [t=%lldms]", openOk ? 1 : 0, openMs, elapsed());
    if (openMs > 1000) ngks::audioTrace("STALL_WARNING", "step=startAudioIfNeeded elapsedMs=%lld", openMs);
    if (openMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=startAudioIfNeeded elapsedMs=%lld", openMs);
    ngks::diagLog("DEVICE_SWITCH: startAudioIfNeeded=%s [t=%lldms]",
                  openOk ? "OK" : "FAIL", elapsed());

    if (openOk) {
        // ── 5. Verify active device ──
        result.activeDevice = std::string(rtDeviceName_);
        ngks::diagLog("DEVICE_SWITCH: active='%s' sr=%.0f buf=%d [t=%lldms]",
                      result.activeDevice.c_str(), sampleRateHz,
                      telemetry_.rtBufferFrames.load(std::memory_order_relaxed), elapsed());

        // Reset watchdog to grace period
        const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now().time_since_epoch()).count();
        rtProbeStartTickMs_ = nowMs;
        rtLastProgressTickMs_ = nowMs;
        rtLastObservedCallbackCount_ = telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
        telemetry_.rtWatchdogStateCode.store(kWatchdogStateGrace, std::memory_order_relaxed);
        telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);

        // ── 6. Verify playback state survived the restart ──
        ngks::diagLog("DEVICE_SWITCH: verifying playback state… [t=%lldms]", elapsed());
        result.restoredState = capturePlaybackState();
        bool stateMismatch = false;
        for (int i = 0; i < ngks::MAX_DECKS; ++i) {
            const auto& before = result.capturedState.decks[i];
            const auto& after = result.restoredState.decks[i];
            if (!before.hasTrack) continue;
            const bool transportMatch = (before.transport == after.transport);
            const bool playheadClose = std::abs(before.playheadSeconds - after.playheadSeconds) < 0.5;
            if (!transportMatch || !playheadClose) {
                stateMismatch = true;
                ngks::diagLog("DEVICE_SWITCH: STATE_MISMATCH deck=%d transport=%s->%s playhead=%.3f->%.3f",
                              i, transportName(before.transport), transportName(after.transport),
                              before.playheadSeconds, after.playheadSeconds);
            } else {
                ngks::diagLog("DEVICE_SWITCH: deck=%d state OK transport=%s playhead=%.3f",
                              i, transportName(after.transport), after.playheadSeconds);
            }
        }

        if (stateMismatch) {
            ngks::diagLog("DEVICE_SWITCH: WARNING — playback state drift detected (non-fatal)");
        }

        result.ok = true;
        ngks::diagLog("DEVICE_SWITCH: SUCCESS — device='%s' [t=%lldms]",
                      result.activeDevice.c_str(), elapsed());

        // Clear preferred device so subsequent reopens (mode changes,
        // ensureAudioHot) re-resolve to the current Windows default
        // instead of pinning this device name forever.
        preferredAudioDeviceName_.clear();
        preferredAudioDeviceId_.clear();
    } else {
        // ── 7. Rollback / recovery ──
        result.rollbackUsed = true;

        if (!result.previousDevice.empty() &&
            result.previousDevice != result.requestedDevice) {
            // Previous device differs from requested — try rolling back
            ngks::diagLog("DEVICE_SWITCH: FAILED — rollback to '%s' [t=%lldms]",
                          result.previousDevice.c_str(), elapsed());
            preferredAudioDeviceName_ = result.previousDevice;
            preferredAudioDeviceId_.clear();
            result.rollbackOk = startAudioIfNeeded(false);
            result.activeDevice = std::string(rtDeviceName_);
            ngks::diagLog("DEVICE_SWITCH: rollback %s — device='%s' [t=%lldms]",
                          result.rollbackOk ? "OK" : "FAIL", result.activeDevice.c_str(), elapsed());

            if (result.rollbackOk) {
                result.restoredState = capturePlaybackState();
                for (int i = 0; i < ngks::MAX_DECKS; ++i) {
                    const auto& before = result.capturedState.decks[i];
                    const auto& after = result.restoredState.decks[i];
                    if (!before.hasTrack) continue;
                    ngks::diagLog("DEVICE_SWITCH: rollback deck=%d transport=%s->%s playhead=%.3f->%.3f",
                                  i, transportName(before.transport), transportName(after.transport),
                                  before.playheadSeconds, after.playheadSeconds);
                }
            }
        }

        if (!result.rollbackOk) {
            // Rollback failed or same device — try system default as last resort
            ngks::diagLog("DEVICE_SWITCH: trying system default fallback [t=%lldms]", elapsed());
            preferredAudioDeviceName_.clear();
            preferredAudioDeviceId_.clear();
            result.rollbackOk = startAudioIfNeeded(false);
            result.activeDevice = std::string(rtDeviceName_);
            ngks::diagLog("DEVICE_SWITCH: default fallback %s — device='%s' [t=%lldms]",
                          result.rollbackOk ? "OK" : "FAIL", result.activeDevice.c_str(), elapsed());
        }
    }

    deviceSwitchInFlight_.store(false, std::memory_order_release);
    result.elapsedMs = elapsed();
    ngks::audioTrace("REOPEN_EXIT", "result=%s active=\"%s\" rollback=%s totalMs=%lld",
                     result.ok ? "OK" : "FAIL", result.activeDevice.c_str(),
                     result.rollbackUsed ? (result.rollbackOk ? "OK" : "FAIL") : "N/A",
                     result.elapsedMs);
    ngks::diagLog("DEVICE_SWITCH: complete — result=%s active='%s' rollback=%s totalMs=%lld",
                  result.ok ? "OK" : "FAIL", result.activeDevice.c_str(),
                  result.rollbackUsed ? (result.rollbackOk ? "OK" : "FAIL") : "N/A",
                  result.elapsedMs);
    return result;
}

std::string EngineCore::getActiveDeviceName() const noexcept
{
    return std::string(rtDeviceName_);
}

void EngineCore::stopRtAudioProbe() noexcept
{
    telemetry_.rtAudioEnabled.store(0u, std::memory_order_relaxed);
    if (audioOpened.load(std::memory_order_acquire)) {
        setRunState(EngineRunState::RtRunning);
    } else {
        setRunState(EngineRunState::Ready);
    }
}

bool EngineCore::pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept
{
    outStallMs = 0;

    // Suppress watchdog entirely during intentional device switch
    if (deviceSwitchInFlight_.load(std::memory_order_acquire)) {
        telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);
        return true;
    }

    // Suppress watchdog recovery when DJ mode device is lost
    if (djDeviceLost_.load(std::memory_order_acquire)) {
        telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);
        telemetry_.rtWatchdogStateCode.store(kWatchdogStateGrace, std::memory_order_relaxed);
        return true;
    }

    if (telemetry_.rtAudioEnabled.load(std::memory_order_relaxed) == 0u
        || telemetry_.rtDeviceOpenOk.load(std::memory_order_relaxed) == 0u) {
        telemetry_.rtWatchdogOk.store(1u, std::memory_order_relaxed);
        telemetry_.rtWatchdogStateCode.store(kWatchdogStateGrace, std::memory_order_relaxed);
        return true;
    }

    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    const uint64_t xrunTotal = telemetry_.rtXRunCount.load(std::memory_order_relaxed);
    const uint64_t xrunWindow = xrunTotal - rtWindowLastXRunTotal_;
    rtWindowLastXRunTotal_ = xrunTotal;
    telemetry_.rtXRunCountWindow.store(xrunWindow, std::memory_order_relaxed);

    const uint64_t jitterWindow = telemetry_.rtJitterAbsNsMaxWindow.exchange(0u, std::memory_order_relaxed);
    telemetry_.rtJitterAbsNsMaxWindow.store(jitterWindow, std::memory_order_relaxed);

    const uint64_t intervalWindow = telemetry_.rtCallbackIntervalNsMaxWindow.exchange(0u, std::memory_order_relaxed);
    telemetry_.rtCallbackIntervalNsMaxWindow.store(intervalWindow, std::memory_order_relaxed);

    const uint64_t callbackCount = telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
    if (callbackCount != rtLastObservedCallbackCount_) {
        rtLastObservedCallbackCount_ = callbackCount;
        rtLastProgressTickMs_ = nowMs;
    }

    int32_t state = telemetry_.rtWatchdogStateCode.load(std::memory_order_relaxed);
    if (state == kWatchdogStateFailed) {
        telemetry_.rtWatchdogOk.store(0u, std::memory_order_relaxed);
        telemetry_.rtRecoveryFailedState.store(1u, std::memory_order_relaxed);
        setRunState(EngineRunState::RtFailed);
        return false;
    }

    const bool graceExpired = (nowMs - rtProbeStartTickMs_) >= kWatchdogGraceMs;
    if (state == kWatchdogStateGrace) {
        if (callbackCount >= kWatchdogGraceCallbacks) {
            state = kWatchdogStateActive;
        } else if (graceExpired) {
            state = kWatchdogStateStall;
            telemetry_.rtWatchdogTripCount.fetch_add(1u, std::memory_order_relaxed);
            requestRtRecovery(-2);
        }
    }

    outStallMs = std::max<int64_t>(0, nowMs - rtLastProgressTickMs_);
    if (state == kWatchdogStateActive && outStallMs > thresholdMs) {
        state = kWatchdogStateStall;
        telemetry_.rtWatchdogTripCount.fetch_add(1u, std::memory_order_relaxed);
        requestRtRecovery(-3);
    }

    if (state == kWatchdogStateStall) {
        performRtRecoveryIfNeeded(nowMs);
        const uint64_t latestCallbackCount = telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
        if (latestCallbackCount >= kWatchdogGraceCallbacks && outStallMs <= thresholdMs) {
            state = kWatchdogStateActive;
            rtConsecutiveRecoveryFailures_ = 0u;
            telemetry_.rtRecoveryRequested.store(0u, std::memory_order_relaxed);
            telemetry_.rtRecoveryFailedState.store(0u, std::memory_order_relaxed);
        }
    }

    if (rtConsecutiveRecoveryFailures_ >= kMaxRecoveryFailures) {
        state = kWatchdogStateFailed;
        telemetry_.rtRecoveryFailedState.store(1u, std::memory_order_relaxed);
    }

    telemetry_.rtWatchdogStateCode.store(state, std::memory_order_relaxed);
    const bool ok = (state != kWatchdogStateStall && state != kWatchdogStateFailed);
    telemetry_.rtWatchdogOk.store(ok ? 1u : 0u, std::memory_order_relaxed);

    if (ok && audioOpened.load(std::memory_order_acquire)) {
        setRunState(EngineRunState::RtRunning);
    }

    return ok;
}

void EngineCore::requestRtRecovery(int32_t errorCode) noexcept
{
    telemetry_.rtRecoveryRequested.store(1u, std::memory_order_relaxed);
    telemetry_.rtLastDeviceErrorCode.store(errorCode, std::memory_order_relaxed);
}

bool EngineCore::performRtRecoveryIfNeeded(int64_t nowMs) noexcept
{
    if (telemetry_.rtRecoveryRequested.load(std::memory_order_relaxed) == 0u) {
        return false;
    }

    // DJ mode: never auto-recover — device-lost state requires explicit user action
    if (djDeviceLost_.load(std::memory_order_acquire)) {
        ngks::audioTrace("RT_RECOVERY_DJ_BLOCK", "djDeviceLost=1 — recovery suppressed");
        telemetry_.rtRecoveryRequested.store(0u, std::memory_order_relaxed);
        return false;
    }

    ngks::audioTrace("RT_RECOVERY_CHECK", "switchInFlight=%d cooldownRemMs=%lld rtRecoveryInFlight=%d",
                     deviceSwitchInFlight_.load(std::memory_order_acquire) ? 1 : 0,
                     kRecoveryCooldownMs - (nowMs - rtLastRecoveryAttemptMs_),
                     rtRecoveryInFlight_.load(std::memory_order_acquire) ? 1 : 0);

    // Skip recovery if an intentional device switch is in progress
    if (deviceSwitchInFlight_.load(std::memory_order_acquire)) {
        ngks::audioTrace("RT_RECOVERY_SKIP", "reason=deviceSwitchInFlight");
        ngks::diagLog("RT_RECOVERY: skipped — deviceSwitchInFlight");
        return false;
    }

    if ((nowMs - rtLastRecoveryAttemptMs_) < kRecoveryCooldownMs) {
        return false;
    }

    // Prevent re-entry while a recovery thread is already running
    bool expected = false;
    if (!rtRecoveryInFlight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return false;
    }

    rtLastRecoveryAttemptMs_ = nowMs;
    telemetry_.rtDeviceRestartCount.fetch_add(1u, std::memory_order_relaxed);

    ngks::diagLog("RT_RECOVERY: dispatching recovery to background thread (attempt %u)",
                  rtConsecutiveRecoveryFailures_ + 1u);
    ngks::audioTrace("RT_RECOVERY_DISPATCH", "attempt=%u",
                     rtConsecutiveRecoveryFailures_ + 1u);

    // Dispatch to background thread so UI poll never blocks on WASAPI
    std::thread([this]() {
        const bool reopenOk = startAudioIfNeeded(true);
        if (reopenOk) {
            rtConsecutiveRecoveryFailures_ = 0u;
            telemetry_.rtRecoveryRequested.store(0u, std::memory_order_relaxed);
            telemetry_.rtLastDeviceErrorCode.store(0, std::memory_order_relaxed);
            setRunState(EngineRunState::RtRunning);
            ngks::diagLog("RT_RECOVERY: success — device reopened");
        } else {
            ++rtConsecutiveRecoveryFailures_;
            telemetry_.rtLastDeviceErrorCode.store(-4, std::memory_order_relaxed);
            ngks::diagLog("RT_RECOVERY: FAILED — consecutive_failures=%u", rtConsecutiveRecoveryFailures_);
        }
        rtRecoveryInFlight_.store(false, std::memory_order_release);
    }).detach();

    return false;  // recovery is in-flight, not yet complete
}

void EngineCore::setRunState(EngineRunState state) noexcept
{
    telemetry_.engineRunState.store(static_cast<uint32_t>(state), std::memory_order_relaxed);
}

void EngineCore::notifyDeviceStopped() noexcept
{
    ngks::audioTrace("NOTIFY_STOPPED", "audioOpened=%d rtDeviceOpenOk=%d djMode=%d",
                     audioOpened.load(std::memory_order_acquire) ? 1 : 0,
                     telemetry_.rtDeviceOpenOk.load(std::memory_order_relaxed) != 0 ? 1 : 0,
                     djMode_.load(std::memory_order_acquire) ? 1 : 0);

    if (djMode_.load(std::memory_order_acquire)) {
        // Delegate to forceDjDeviceLost() which captures rtDeviceName_
        // for auto-recovery and properly resets all state.
        forceDjDeviceLost();
    } else {
        audioOpened.store(false, std::memory_order_release);
        telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
        setRunState(EngineRunState::Ready);
    }
}

// ── DJ mode device-loss ──

void EngineCore::setDjMode(bool enabled) noexcept
{
    djMode_.store(enabled, std::memory_order_release);
    if (!enabled) {
        djDeviceLost_.store(false, std::memory_order_release);
    }
    // Reset enforcer state on mode change
    djEnforcer_ = DjEnforcerState{};
    ngks::audioTrace("DJ_MODE_SET", "enabled=%d", enabled ? 1 : 0);
    ngks::diagLog("DJ_MODE: %s", enabled ? "ENABLED" : "DISABLED");
}

bool EngineCore::isDjMode() const noexcept
{
    return djMode_.load(std::memory_order_acquire);
}

bool EngineCore::isDjDeviceLost() const noexcept
{
    return djDeviceLost_.load(std::memory_order_acquire);
}

void EngineCore::clearDjDeviceLost() noexcept
{
    djDeviceLost_.store(false, std::memory_order_release);
    djEnforcer_ = DjEnforcerState{}; // reset enforcer for fresh monitoring
    djAutoRecovery_ = DjAutoRecoveryState{}; // reset auto-recovery probe
    ngks::audioTrace("DJ_DEVICE_LOST_CLEARED", "");
    ngks::diagLog("DJ_DEVICE_LOST: cleared by user action");
}

EngineCore::DjRecoveryResult EngineCore::attemptDjRecovery() noexcept
{
    DjRecoveryResult result;
    result.expectedDevice = preferredAudioDeviceName_;

    const auto tid = static_cast<unsigned long>(GetCurrentThreadId());
    ngks::audioTrace("DJ_RECOVERY_ENTER",
        "tid=%lu djMode=%d djDeviceLost=%d preferred=\"%s\" rtDevice=\"%s\" audioOpened=%d",
        tid,
        djMode_.load(std::memory_order_acquire) ? 1 : 0,
        djDeviceLost_.load(std::memory_order_acquire) ? 1 : 0,
        preferredAudioDeviceName_.c_str(),
        rtDeviceName_,
        audioOpened.load(std::memory_order_acquire) ? 1 : 0);

    // ── Pre-checks ──
    if (!djMode_.load(std::memory_order_acquire)) {
        result.reason = "not in DJ mode";
        ngks::audioTrace("DJ_RECOVERY_EXIT", "tid=%lu reason=not_dj_mode", tid);
        return result;
    }
    if (!djDeviceLost_.load(std::memory_order_acquire)) {
        result.reason = "device-lost not active";
        ngks::audioTrace("DJ_RECOVERY_EXIT", "tid=%lu reason=not_device_lost", tid);
        return result;
    }

    if (audioIO == nullptr) {
        result.reason = "no audio backend";
        ngks::audioTrace("DJ_RECOVERY_EXIT", "tid=%lu reason=no_audio_backend", tid);
        return result;
    }

    // ── Set recovery-in-flight flag — blocks enforcer from touching JUCE ──
    djRecoveryInFlight_.store(true, std::memory_order_release);

    // Everything below is wrapped in a fail-closed try/catch.
    // If anything throws or crashes, djDeviceLost_ stays true, recovery flag is cleared.
    try {

    // ── STEP 1: Enumerate available output devices ──
    ngks::audioTrace("DJ_RECOVERY_ENUM_BEGIN", "tid=%lu", tid);
    std::vector<std::string> available;
    try {
        available = audioIO->listOutputDeviceNames();
    } catch (...) {
        ngks::audioTrace("DJ_RECOVERY_ENUM_CRASH", "tid=%lu exception_in_listOutputDeviceNames", tid);
        djRecoveryInFlight_.store(false, std::memory_order_release);
        result.reason = "crash during device enumeration";
        return result;
    }
    result.available = available;
    ngks::audioTrace("DJ_RECOVERY_ENUM_END", "tid=%lu count=%zu", tid, available.size());
    for (size_t i = 0; i < available.size(); ++i) {
        ngks::audioTrace("DJ_RECOVERY_ENUM", "  [%zu] \"%s\"",
                         i, available[i].c_str());
    }

    ngks::audioTrace("DJ_RECOVERY_EXPECTED",
        "tid=%lu preferredName=\"%s\" rtDeviceName=\"%s\"",
        tid, preferredAudioDeviceName_.c_str(), rtDeviceName_);

    // normalize() and isBuiltInSpeaker() are file-scoped static helpers above.

    // ── STEP 2: Layered matching ──
    ngks::audioTrace("DJ_RECOVERY_MATCH_BEGIN", "tid=%lu", tid);

    std::string matchedDevice;
    std::string matchType;

    const std::string expectedName = preferredAudioDeviceName_;
    const std::string lastActiveName(rtDeviceName_);
    const std::string normExpected = normalize(expectedName);
    const std::string normLastActive = normalize(lastActiveName);

    // Priority 1: exact name match (preferredAudioDeviceName_)
    for (const auto& dev : available) {
        if (dev == expectedName) {
            matchedDevice = dev;
            matchType = "exact_preferred";
            ngks::audioTrace("DJ_RECOVERY_MATCH",
                "tid=%lu type=exact_preferred device=\"%s\"", tid, dev.c_str());
            break;
        }
    }

    // Priority 2: exact match to rtDeviceName_ (last active device)
    if (matchedDevice.empty() && !lastActiveName.empty() && lastActiveName != expectedName) {
        for (const auto& dev : available) {
            if (dev == lastActiveName) {
                matchedDevice = dev;
                matchType = "exact_lastActive";
                ngks::audioTrace("DJ_RECOVERY_MATCH",
                    "tid=%lu type=exact_lastActive device=\"%s\"", tid, dev.c_str());
                break;
            }
        }
    }

    // Priority 3: normalized match to preferred name
    if (matchedDevice.empty() && !normExpected.empty()) {
        for (const auto& dev : available) {
            if (normalize(dev) == normExpected) {
                matchedDevice = dev;
                matchType = "normalized_preferred";
                ngks::audioTrace("DJ_RECOVERY_MATCH",
                    "tid=%lu type=normalized_preferred device=\"%s\"", tid, dev.c_str());
                break;
            }
        }
    }

    // Priority 4: normalized match to last active device
    if (matchedDevice.empty() && !normLastActive.empty() && normLastActive != normExpected) {
        for (const auto& dev : available) {
            if (normalize(dev) == normLastActive) {
                matchedDevice = dev;
                matchType = "normalized_lastActive";
                ngks::audioTrace("DJ_RECOVERY_MATCH",
                    "tid=%lu type=normalized_lastActive device=\"%s\"", tid, dev.c_str());
                break;
            }
        }
    }

    // Priority 5: same-class reattach — sole non-speaker output
    if (matchedDevice.empty() && !isBuiltInSpeaker(expectedName)) {
        std::vector<std::string> nonSpeakerCandidates;
        for (const auto& dev : available) {
            if (!isBuiltInSpeaker(dev)) {
                nonSpeakerCandidates.push_back(dev);
            }
        }
        if (nonSpeakerCandidates.size() == 1) {
            matchedDevice = nonSpeakerCandidates[0];
            matchType = "class_reattach";
            ngks::audioTrace("DJ_RECOVERY_MATCH",
                "tid=%lu type=class_reattach device=\"%s\"", tid, matchedDevice.c_str());
        } else {
            ngks::audioTrace("DJ_RECOVERY_NO_MATCH",
                "tid=%lu candidates=%zu", tid, nonSpeakerCandidates.size());
        }
    }

    ngks::audioTrace("DJ_RECOVERY_MATCH_END",
        "tid=%lu matched=\"%s\" matchType=%s",
        tid, matchedDevice.c_str(), matchType.c_str());

    // ── No match → fail cleanly ──
    if (matchedDevice.empty()) {
        for (const auto& dev : available) {
            const char* rejectReason = isBuiltInSpeaker(dev) ? "built_in_speaker" : "no_name_match";
            ngks::audioTrace("DJ_RECOVERY_CANDIDATE",
                "tid=%lu device=\"%s\" rejected=%s", tid, dev.c_str(), rejectReason);
        }

        std::string failReason = "no safe match found";
        failReason += "\nExpected: " + expectedName;
        if (!lastActiveName.empty() && lastActiveName != expectedName)
            failReason += "\nLast active: " + lastActiveName;
        failReason += "\nAvailable (" + std::to_string(available.size()) + "):";
        for (const auto& dev : available) {
            failReason += "\n  - " + dev;
            if (isBuiltInSpeaker(dev)) failReason += " [built-in speaker]";
        }
        result.reason = failReason;
        ngks::audioTrace("DJ_RECOVERY_EXIT",
            "tid=%lu reason=no_safe_match djDeviceLost=1", tid);
        djRecoveryInFlight_.store(false, std::memory_order_release);
        return result;
    }

    // ── STEP 3: Update preferred name to matched device ──
    const std::string originalPreferred = preferredAudioDeviceName_;
    if (matchedDevice != preferredAudioDeviceName_) {
        ngks::audioTrace("DJ_RECOVERY_REATTACH",
            "tid=%lu updating preferred \"%s\" -> \"%s\" matchType=%s",
            tid, preferredAudioDeviceName_.c_str(), matchedDevice.c_str(), matchType.c_str());
        preferredAudioDeviceName_ = matchedDevice;
    }

    // ── STEP 4: Attempt audio reopen ──
    // djDeviceLost_ stays TRUE — gate is bypassed via djRecoveryInFlight_ flag.
    // This prevents the enforcer from touching JUCE and prevents accidental
    // state leaks if the reopen crashes.
    ngks::audioTrace("DJ_RECOVERY_REOPEN_BEGIN",
        "tid=%lu device=\"%s\" matchType=%s djDeviceLost=1 djRecoveryInFlight=1",
        tid, matchedDevice.c_str(), matchType.c_str());

    bool reopenOk = false;
    try {
        reopenOk = startAudioIfNeeded(true /* forceReopen */);
    } catch (...) {
        ngks::audioTrace("DJ_RECOVERY_REOPEN_CRASH",
            "tid=%lu exception_in_startAudioIfNeeded device=\"%s\"",
            tid, matchedDevice.c_str());
        // Restore original preferred name, keep djDeviceLost_ true
        preferredAudioDeviceName_ = originalPreferred;
        result.reason = "crash during audio reopen on \"" + matchedDevice + "\"";
        result.matchType = matchType;
        djRecoveryInFlight_.store(false, std::memory_order_release);
        ngks::audioTrace("DJ_RECOVERY_EXIT",
            "tid=%lu reason=reopen_crash djDeviceLost=1", tid);
        return result;
    }

    ngks::audioTrace("DJ_RECOVERY_REOPEN_END",
        "tid=%lu reopenOk=%d device=\"%s\"", tid, reopenOk ? 1 : 0, matchedDevice.c_str());

    if (!reopenOk) {
        // Reopen failed — restore original preferred name, djDeviceLost_ stays true
        preferredAudioDeviceName_ = originalPreferred;
        result.reason = "audio reopen failed on \"" + matchedDevice + "\"";
        result.activeDevice = std::string(rtDeviceName_);
        result.matchType = matchType;
        ngks::audioTrace("DJ_RECOVERY_EXIT",
            "tid=%lu reason=reopen_failed djDeviceLost=1 device=\"%s\"",
            tid, matchedDevice.c_str());
        djRecoveryInFlight_.store(false, std::memory_order_release);
        return result;
    }

    // ── STEP 5: Reopen succeeded — NOW clear device-lost ──
    ngks::audioTrace("DJ_RECOVERY_CLEAR_LOST_BEGIN",
        "tid=%lu openedDevice=\"%s\" matchType=%s",
        tid, rtDeviceName_, matchType.c_str());

    djDeviceLost_.store(false, std::memory_order_release);
    djEnforcer_ = DjEnforcerState{};
    for (int slot = 0; slot < 2; ++slot) {
        snapshots[slot].flags &= ~ngks::SNAP_DJ_DEVICE_LOST;
    }

    ngks::audioTrace("DJ_RECOVERY_CLEAR_LOST_END", "tid=%lu djDeviceLost=0", tid);

    // Log deck state after recovery — media binding must survive
    for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
        const auto& ds = snapshots[0].decks[d];
        ngks::audioTrace("DJ_DECK_STATE_AFTER_RECOVERY",
            "deck=%u hasTrack=%d lifecycle=%d transport=%d label=\"%.32s\"",
            d, ds.hasTrack, static_cast<int>(ds.lifecycle),
            static_cast<int>(ds.transport), ds.currentTrackLabel);
    }

    // ── STEP 6: Signal success ──
    ngks::audioTrace("DJ_RECOVERY_SIGNAL_BEGIN", "tid=%lu", tid);
    const std::string openedDevice(rtDeviceName_);

    result.ok = true;
    result.activeDevice = openedDevice;
    result.matchType = matchType;
    for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d)
        result.deckWasPlaying[d] = deckWasPlayingBeforeLoss_[d];
    ngks::audioTrace("DJ_RECOVERY_SIGNAL_END",
        "tid=%lu device=\"%s\" matchType=%s audioOpened=%d djDeviceLost=0",
        tid, openedDevice.c_str(), matchType.c_str(),
        audioOpened.load(std::memory_order_acquire) ? 1 : 0);
    ngks::diagLog("DJ_RECOVERY: succeeded — device=\"%s\" matchType=%s, playback remains stopped",
                  openedDevice.c_str(), matchType.c_str());

    djRecoveryInFlight_.store(false, std::memory_order_release);
    ngks::audioTrace("DJ_RECOVERY_EXIT", "tid=%lu ok=1", tid);
    return result;

    } catch (...) {
        // Catch-all: if anything above threw, fail closed.
        ngks::audioTrace("DJ_RECOVERY_UNHANDLED_EXCEPTION",
            "tid=%lu — fail-closed, djDeviceLost stays true", tid);
        djRecoveryInFlight_.store(false, std::memory_order_release);
        result.ok = false;
        result.reason = "unhandled exception during recovery — device-lost preserved";
        return result;
    }
}

void EngineCore::forceStopAllDecks() noexcept
{
    // Force-stop all deck transports in both snapshot slots to prevent
    // any stale Playing/Starting state from surviving to the UI.
    for (int slot = 0; slot < 2; ++slot) {
        for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
            auto& deck = snapshots[slot].decks[d];
            if (deck.transport == ngks::TransportState::Starting
                || deck.transport == ngks::TransportState::Playing) {
                ngks::audioTrace("DJ_PLAYBACK_FORCED_STOP", "deck=%u transport=%d lifecycle=%d slot=%d",
                                 d, static_cast<int>(deck.transport),
                                 static_cast<int>(deck.lifecycle), slot);
                deck.transport = ngks::TransportState::Stopped;
            }
            // Lifecycle must track transport — a Playing deck forced to
            // Stopped transport must move lifecycle to Stopped so the
            // FSM allows Play again after recovery (Stopped→Playing).
            if (deck.lifecycle == DeckLifecycleState::Playing) {
                deck.lifecycle = DeckLifecycleState::Stopped;
            }
            deck.rmsL = 0.0f;
            deck.rmsR = 0.0f;
            deck.peakL = 0.0f;
            deck.peakR = 0.0f;
            deck.audible = false;
        }
        snapshots[slot].masterRmsL = 0.0f;
        snapshots[slot].masterRmsR = 0.0f;
        snapshots[slot].masterPeakL = 0.0f;
        snapshots[slot].masterPeakR = 0.0f;
        snapshots[slot].flags &= ~ngks::SNAP_AUDIO_RUNNING;
        snapshots[slot].flags |= ngks::SNAP_DJ_DEVICE_LOST;
    }
    // Log post-stop deck state so we can verify media binding survives
    for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
        const auto& ds = snapshots[0].decks[d];
        ngks::diagLog("DJ_DECK_STATE_AFTER_LOSS: deck=%u hasTrack=%d lifecycle=%d transport=%d label=\"%.32s\"",
                      d, ds.hasTrack, static_cast<int>(ds.lifecycle),
                      static_cast<int>(ds.transport), ds.currentTrackLabel);
    }
    ngks::diagLog("DJ_PLAYBACK_FORCED_STOP: all decks stopped, lifecycle→Stopped, meters zeroed, SNAP_DJ_DEVICE_LOST set");
}

void EngineCore::forceDjDeviceLost() noexcept
{
    if (djDeviceLost_.load(std::memory_order_acquire))
        return; // already in device-lost state

    // Capture the active device name BEFORE shutting down,
    // so auto-recovery knows what device to look for after replug.
    if (preferredAudioDeviceName_.empty() && rtDeviceName_[0] != '\0') {
        preferredAudioDeviceName_ = std::string(rtDeviceName_);
        ngks::audioTrace("DJ_DEVICE_LOST_CAPTURE",
            "saved rtDeviceName=\"%s\" → preferredAudioDeviceName_",
            rtDeviceName_);
    }

    audioOpened.store(false, std::memory_order_release);
    telemetry_.rtDeviceOpenOk.store(0u, std::memory_order_relaxed);
    djDeviceLost_.store(true, std::memory_order_release);
    djAutoRecovery_ = DjAutoRecoveryState{}; // fresh auto-recovery state

    // Capture per-deck transport state BEFORE stopping — for auto-resume after recovery
    for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
        const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
        const auto t = snapshots[front].decks[d].transport;
        deckWasPlayingBeforeLoss_[d] = (t == ngks::TransportState::Playing
                                     || t == ngks::TransportState::Starting);
    }

    forceStopAllDecks();
    setRunState(EngineRunState::Ready);
    ngks::audioTrace("DJ_DEVICE_LOST",
        "source=enforcer allDecksForceStop=1 expected=\"%s\"",
        preferredAudioDeviceName_.c_str());
    ngks::diagLog("DJ_DEVICE_LOST: enforcer forced device-lost — all decks stopped, expected=\"%s\"",
                  preferredAudioDeviceName_.c_str());
}

bool EngineCore::pollDjOutputEnforcer() noexcept
{
    // ── Gate: only runs in DJ mode while audio is supposedly open ──
    if (!djMode_.load(std::memory_order_acquire))
        return false;
    if (djDeviceLost_.load(std::memory_order_acquire))
        return false;
    if (djRecoveryInFlight_.load(std::memory_order_acquire))
        return false;   // recovery thread owns JUCE right now — stay out
    if (!audioOpened.load(std::memory_order_acquire))
        return false;
    if (deviceSwitchInFlight_.load(std::memory_order_acquire))
        return false;

    // ── Throttle: check every ~300ms, not every 16ms poll ──
    const auto now = std::chrono::steady_clock::now();
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    if (djEnforcer_.lastPollTickMs != 0 && (nowMs - djEnforcer_.lastPollTickMs) < 300)
        return false;
    djEnforcer_.lastPollTickMs = nowMs;

    // ── Gather truth signals ──
    const std::string activeName = getActiveDeviceName();
    if (activeName.empty())
        return false; // no device was ever set

    // Cheap checks first
    const bool juceDeviceOpen = audioIO ? audioIO->isCurrentDeviceOpen() : false;
    const bool cbFlowing = audioIO ? audioIO->isCallbackFlowing() : false;
    const uint64_t cbCount = audioIO ? audioIO->callbackCount() : 0;
    const uint64_t cbDelta = cbCount - djEnforcer_.lastCallbackCount;
    djEnforcer_.lastCallbackCount = cbCount;

    // Check if any deck is supposed to be playing
    bool anyDeckPlaying = false;
    for (uint8_t d = 0; d < ngks::MAX_DECKS; ++d) {
        const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
        const auto t = snapshots[front].decks[d].transport;
        if (t == ngks::TransportState::Playing || t == ngks::TransportState::Starting) {
            anyDeckPlaying = true;
            break;
        }
    }

    // Expensive check: rescan device list to see if active device still exists
    // This calls scanForDevices() on the current WASAPI backend.
    const bool devicePresent = audioIO ? audioIO->isOutputDevicePresent(activeName) : false;

    // ── Determine if output is invalid ──
    // Invalid if device is gone from enumeration
    // OR if JUCE says device is not open anymore
    // OR if callbacks stopped flowing (cbDelta == 0 on two consecutive checks means stalled)
    const bool deviceGone = !devicePresent;
    const bool juceDeviceDead = !juceDeviceOpen;

    // Device enumeration miss is the strongest signal.
    // JUCE device pointer being null/closed is next.
    // Callback stall alone is weaker (could be legitimate pause), so combine
    // with other signals.
    const bool outputInvalid = deviceGone || juceDeviceDead;

    // ── Log every enforcer check ──
    ngks::audioTrace("DJ_ENFORCER_CHECK",
        "active=\"%s\" present=%d juceOpen=%d cbFlowing=%d cbDelta=%llu "
        "anyPlaying=%d audioOpened=%d invalid=%d armed=%d",
        activeName.c_str(),
        devicePresent ? 1 : 0,
        juceDeviceOpen ? 1 : 0,
        cbFlowing ? 1 : 0,
        static_cast<unsigned long long>(cbDelta),
        anyDeckPlaying ? 1 : 0,
        audioOpened.load(std::memory_order_acquire) ? 1 : 0,
        outputInvalid ? 1 : 0,
        djEnforcer_.armed ? 1 : 0);

    if (outputInvalid) {
        if (!djEnforcer_.armed) {
            // First detection — arm and record time
            djEnforcer_.armed = true;
            djEnforcer_.firstInvalidTickMs = nowMs;
            ngks::audioTrace("DJ_ENFORCER_INVALID_OUTPUT",
                "reason=%s%s active=\"%s\" firstDetectMs=%lld",
                deviceGone ? "deviceGone" : "",
                juceDeviceDead ? (deviceGone ? "+juceDeviceDead" : "juceDeviceDead") : "",
                activeName.c_str(),
                static_cast<long long>(nowMs));
            return false; // wait for sustained threshold
        }

        // Already armed — check threshold (500ms sustained invalidity)
        const int64_t invalidDurationMs = nowMs - djEnforcer_.firstInvalidTickMs;
        if (invalidDurationMs >= 500) {
            // ── FORCE DEVICE LOST ──
            ngks::audioTrace("DJ_ENFORCER_FORCE_DEVICE_LOST",
                "active=\"%s\" present=%d juceOpen=%d cbFlowing=%d "
                "invalidDurationMs=%lld anyPlaying=%d",
                activeName.c_str(),
                devicePresent ? 1 : 0,
                juceDeviceOpen ? 1 : 0,
                cbFlowing ? 1 : 0,
                static_cast<long long>(invalidDurationMs),
                anyDeckPlaying ? 1 : 0);

            ngks::audioTrace("DJ_ENFORCER_REASON",
                "deviceGone=%d juceDeviceDead=%d cbStalled=%d "
                "cbDelta=%llu activeName=\"%s\"",
                deviceGone ? 1 : 0,
                juceDeviceDead ? 1 : 0,
                (cbDelta == 0) ? 1 : 0,
                static_cast<unsigned long long>(cbDelta),
                activeName.c_str());

            // Force the device-lost state
            forceDjDeviceLost();

            // Also shut down the JUCE device to prevent zombie callbacks
            if (audioIO) {
                audioIO->stop();
            }

            // Reset enforcer state
            djEnforcer_.armed = false;
            djEnforcer_.firstInvalidTickMs = 0;
            return true;
        }
    } else {
        // Output is valid — disarm if previously armed
        if (djEnforcer_.armed) {
            ngks::audioTrace("DJ_ENFORCER_RECOVERED",
                "active=\"%s\" armedDurationMs=%lld",
                activeName.c_str(),
                static_cast<long long>(nowMs - djEnforcer_.firstInvalidTickMs));
            djEnforcer_.armed = false;
            djEnforcer_.firstInvalidTickMs = 0;
        }
    }
    return false;
}

// ── DJ auto-recovery: probe for safe device reattachment while device-lost ──
EngineCore::DjRecoveryResult EngineCore::pollDjAutoRecovery() noexcept
{
    DjRecoveryResult noAction;

    // ── Gate: only probe when device-lost is active in DJ mode ──
    if (!djMode_.load(std::memory_order_acquire)) {
        return noAction;
    }
    if (!djDeviceLost_.load(std::memory_order_acquire)) {
        return noAction;
    }
    if (djRecoveryInFlight_.load(std::memory_order_acquire)) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_GATE",
            "blocked=djRecoveryInFlight tid=%lu", static_cast<unsigned long>(GetCurrentThreadId()));
        return noAction;  // manual or previous auto-recovery already in flight
    }
    if (audioIO == nullptr) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_GATE",
            "blocked=audioIO_null tid=%lu", static_cast<unsigned long>(GetCurrentThreadId()));
        return noAction;
    }

    // ── Throttle: probe every 1000ms, not every 16ms ──
    const auto now = std::chrono::steady_clock::now();
    const int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    if (djAutoRecovery_.lastProbeTickMs != 0
        && (nowMs - djAutoRecovery_.lastProbeTickMs) < 1000)
        return noAction;
    djAutoRecovery_.lastProbeTickMs = nowMs;

    // ── Cooldown: don't retry within 5s of a failed attempt ──
    if (djAutoRecovery_.lastAttemptTickMs != 0
        && (nowMs - djAutoRecovery_.lastAttemptTickMs) < 5000) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_GATE",
            "blocked=cooldown remainMs=%lld tid=%lu",
            static_cast<long long>(5000 - (nowMs - djAutoRecovery_.lastAttemptTickMs)),
            static_cast<unsigned long>(GetCurrentThreadId()));
        return noAction;
    }

    ngks::audioTrace("DJ_AUTO_RECOVERY_CHECK",
        "expected=\"%s\" djDeviceLost=1 audioOpened=%d tid=%lu",
        preferredAudioDeviceName_.c_str(),
        audioOpened.load(std::memory_order_acquire) ? 1 : 0,
        static_cast<unsigned long>(GetCurrentThreadId()));

    // ── Enumerate devices ──
    const auto available = audioIO->listOutputDeviceNames();
    if (available.empty()) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_ENUM",
            "count=0 — no devices found tid=%lu",
            static_cast<unsigned long>(GetCurrentThreadId()));
        return noAction;
    }

    // Log all enumerated devices every probe
    ngks::audioTrace("DJ_AUTO_RECOVERY_ENUM",
        "count=%zu tid=%lu", available.size(),
        static_cast<unsigned long>(GetCurrentThreadId()));
    for (size_t i = 0; i < available.size(); ++i) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_ENUM",
            "  [%zu] \"%s\"", i, available[i].c_str());
    }

    // ── Quick safe-match check (reuse the same matching logic as recovery) ──
    // Only accept Priority 1-3: exact/normalized match to intended device.
    // NO class_reattach — auto-recovery must be conservative.
    const std::string& expected = preferredAudioDeviceName_;
    const std::string normExpected = normalize(expected);

    std::string candidate;
    std::string candidateMatchType;

    // Priority 1: exact preferred name
    for (const auto& dev : available) {
        if (dev == expected) {
            candidate = dev;
            candidateMatchType = "exact_preferred";
            break;
        }
    }

    // Priority 2: normalized preferred name
    if (candidate.empty() && !normExpected.empty()) {
        for (const auto& dev : available) {
            if (normalize(dev) == normExpected) {
                candidate = dev;
                candidateMatchType = "normalized_preferred";
                break;
            }
        }
    }

    // ── Reject built-in speakers ──
    if (!candidate.empty() && isBuiltInSpeaker(candidate)) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_REJECTED",
            "device=\"%s\" reason=built_in_speaker",
            candidate.c_str());
        candidate.clear();
    }

    // ── No match → reset stability tracking ──
    if (candidate.empty()) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_NO_MATCH",
            "expected=\"%s\" normExpected=\"%s\" availCount=%zu tid=%lu",
            expected.c_str(), normExpected.c_str(), available.size(),
            static_cast<unsigned long>(GetCurrentThreadId()));
        if (djAutoRecovery_.matchStable) {
            ngks::audioTrace("DJ_AUTO_RECOVERY_MATCH_LOST",
                "prev=\"%s\"", djAutoRecovery_.lastMatchedDevice.c_str());
        }
        djAutoRecovery_.matchStable = false;
        djAutoRecovery_.firstMatchTickMs = 0;
        djAutoRecovery_.lastMatchedDevice.clear();
        return noAction;
    }

    // ── Match found — check stability (same device for 1500ms) ──
    ngks::audioTrace("DJ_AUTO_RECOVERY_MATCH",
        "device=\"%s\" matchType=%s expected=\"%s\"",
        candidate.c_str(), candidateMatchType.c_str(), expected.c_str());

    if (candidate != djAutoRecovery_.lastMatchedDevice) {
        // New or changed match — start stability timer
        djAutoRecovery_.lastMatchedDevice = candidate;
        djAutoRecovery_.firstMatchTickMs = nowMs;
        djAutoRecovery_.matchStable = false;
        ngks::audioTrace("DJ_AUTO_RECOVERY_ARMED",
            "device=\"%s\" matchType=%s waitingStabilityMs=1500",
            candidate.c_str(), candidateMatchType.c_str());
        return noAction;
    }

    // Same match as before — check duration
    const int64_t stableMs = nowMs - djAutoRecovery_.firstMatchTickMs;
    if (stableMs < 1500) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_DEBOUNCE",
            "device=\"%s\" stableMs=%lld remaining=%lld",
            candidate.c_str(), static_cast<long long>(stableMs),
            static_cast<long long>(1500 - stableMs));
        return noAction;  // not stable long enough
    }

    // ── Stable match confirmed — trigger auto-recovery ──
    ngks::audioTrace("DJ_AUTO_RECOVERY_BEGIN",
        "device=\"%s\" matchType=%s stableMs=%lld source=auto_safe_reattach",
        candidate.c_str(), candidateMatchType.c_str(),
        static_cast<long long>(stableMs));
    ngks::diagLog("DJ_AUTO_RECOVERY: triggering — device=\"%s\" matchType=%s stableMs=%lld",
                  candidate.c_str(), candidateMatchType.c_str(),
                  static_cast<long long>(stableMs));

    djAutoRecovery_.lastAttemptTickMs = nowMs;
    djAutoRecovery_.matchStable = false;
    djAutoRecovery_.firstMatchTickMs = 0;
    djAutoRecovery_.lastMatchedDevice.clear();

    // Reuse the existing hardened recovery pipeline
    auto result = attemptDjRecovery();

    if (result.ok) {
        ngks::audioTrace("DJ_AUTO_RECOVERY_SUCCESS",
            "device=\"%s\" matchType=%s source=auto_safe_reattach",
            result.activeDevice.c_str(), result.matchType.c_str());
        ngks::diagLog("DJ_AUTO_RECOVERY: succeeded — device=\"%s\" matchType=%s",
                      result.activeDevice.c_str(), result.matchType.c_str());
    } else {
        ngks::audioTrace("DJ_AUTO_RECOVERY_FAIL",
            "reason=\"%s\" source=auto_safe_reattach",
            result.reason.c_str());
        ngks::diagLog("DJ_AUTO_RECOVERY: failed — reason=\"%s\"",
                      result.reason.c_str());
    }

    return result;
}

EngineRunState EngineCore::getRunState() const noexcept
{
    return static_cast<EngineRunState>(
        telemetry_.engineRunState.load(std::memory_order_relaxed));
}

void EngineCore::sanitizeSnapshot(ngks::EngineSnapshot& snapshot) const noexcept
{
    snapshot.masterRmsL  = sanitizeFiniteNonNegative(snapshot.masterRmsL);
    snapshot.masterRmsR  = sanitizeFiniteNonNegative(snapshot.masterRmsR);
    snapshot.masterPeakL = sanitizeFiniteNonNegative(snapshot.masterPeakL);
    snapshot.masterPeakR = sanitizeFiniteNonNegative(snapshot.masterPeakR);

    for (uint8_t i = 0; i < ngks::MAX_DECKS; ++i) {
        auto& deck = snapshot.decks[i];
        deck.rmsL = sanitizeFiniteNonNegative(deck.rmsL);
        deck.rmsR = sanitizeFiniteNonNegative(deck.rmsR);
        deck.peakL = sanitizeFiniteNonNegative(deck.peakL);
        deck.peakR = sanitizeFiniteNonNegative(deck.peakR);
        deck.playheadSeconds = sanitizeFiniteNonNegative(deck.playheadSeconds);
        deck.lengthSeconds = sanitizeFiniteNonNegative(deck.lengthSeconds);
        deck.deckGain = std::clamp(deck.deckGain, 0.0f, 12.0f);
        deck.masterWeight = std::clamp(deck.masterWeight, 0.0f, 1.0f);
        deck.cueWeight = std::clamp(deck.cueWeight, 0.0f, 1.0f);
    }
}

void EngineCore::publishSnapshot(const ngks::EngineSnapshot& snapshot) noexcept
{
    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);
    const uint32_t back = front ^ 1u;
    snapshots[back] = snapshot;
    frontSnapshotIndex.store(back, std::memory_order_release);
    telemetry_.snapshotPublishes.fetch_add(1u, std::memory_order_relaxed);
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
    computeCrossfadeWeights(snapshots[front], crossfaderPosition_, mixMatrix_,
                           outputMode_.load(std::memory_order_relaxed));
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
    deck.lengthSeconds = (command.seekSeconds > 0.0) ? command.seekSeconds : 240.0;

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
    std::lock_guard<std::mutex> lock(outcomeMutex_);

    ngks::EngineSnapshot updated = hasPendingOutcome_
        ? pendingOutcome_
        : snapshots[frontSnapshotIndex.load(std::memory_order_acquire)];

    if (result == ngks::CommandResult::Applied && command.type == ngks::CommandType::SetDeckTrack) {
        result = applySetDeckTrack(updated, command);
    } else if (result == ngks::CommandResult::Applied && command.type == ngks::CommandType::SetCue) {
        result = applyCommand(updated, command);
    }
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

    pendingOutcome_ = updated;
    hasPendingOutcome_ = true;
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
            ngks::diagLog("DIAG: applyCommand LoadTrack REJECTED deck=%d lifecycle=%d (need Empty)",
                          static_cast<int>(command.deck), static_cast<int>(deck.lifecycle));
            return ngks::CommandResult::IllegalTransition;
        }
        if (!validateTransition(DeckLifecycleState::Loading, DeckLifecycleState::Loaded)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.hasTrack = 1;
        deck.trackUidHash = command.trackUidHash;
        deck.trackLoadGen = command.trackLoadGen;
        deck.lengthSeconds = (command.seekSeconds > 0.0) ? command.seekSeconds : 240.0;
        deck.playheadSeconds = 0.0;
        // Copy track label into snapshot so UI can display it
        for (size_t i = 0; i < sizeof(deck.currentTrackLabel); ++i)
            deck.currentTrackLabel[i] = command.trackLabel[i];
        deck.lifecycle = DeckLifecycleState::Loaded;
        ngks::diagLog("DIAG: applyCommand LoadTrack APPLIED deck=%d hasTrack=1 dur=%.3f gen=%llu",
                  static_cast<int>(command.deck), deck.lengthSeconds, (unsigned long long)deck.trackLoadGen);
        return ngks::CommandResult::Applied;
    case ngks::CommandType::UnloadTrack:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Empty)) {
            return ngks::CommandResult::IllegalTransition;
        }
        deck.hasTrack = 0;
        deck.trackUidHash = 0;
        deck.currentTrackId = 0;
        std::memset(deck.currentTrackLabel, 0, sizeof(deck.currentTrackLabel));
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
        // ── DJ device-lost hard gate: block all transport starts ──
        if (djMode_.load(std::memory_order_acquire) && djDeviceLost_.load(std::memory_order_acquire)) {
            ngks::audioTrace("DJ_GATE_BLOCK_PLAY",
                "deck=%d djMode=1 djDeviceLost=1",
                static_cast<int>(command.deck));
            ngks::diagLog("DJ_GATE_BLOCK_PLAY: Play REJECTED deck=%d — device-lost active",
                          static_cast<int>(command.deck));
            return ngks::CommandResult::RejectedPublicFacing;
        }
        // Allow resume from Paused: lifecycle stays Playing, just update transport
        if (deck.lifecycle == DeckLifecycleState::Playing
            && deck.transport == ngks::TransportState::Paused) {
            deck.transport = ngks::TransportState::Playing;
            ngks::diagLog("DIAG: applyCommand Play RESUME deck=%d from Paused transport=Playing",
                          static_cast<int>(command.deck));
            return ngks::CommandResult::Applied;
        }
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Playing)) {
            ngks::diagLog("DIAG: applyCommand Play REJECTED deck=%d lifecycle=%d",
                          static_cast<int>(command.deck), static_cast<int>(deck.lifecycle));
            return ngks::CommandResult::IllegalTransition;
        }
        if (!deck.hasTrack) {
            ngks::diagLog("DIAG: applyCommand Play REJECTED deck=%d hasTrack=0", static_cast<int>(command.deck));
            return ngks::CommandResult::RejectedNoTrack;
        }
        startAudioIfNeeded();
        deck.lifecycle = DeckLifecycleState::Playing;
        deck.transport = ngks::TransportState::Starting;
        ngks::diagLog("DIAG: applyCommand Play APPLIED deck=%d transport=Starting",
                      static_cast<int>(command.deck));
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Stop:
        if (!validateTransition(deck.lifecycle, DeckLifecycleState::Stopped)) {
            ngks::diagLog("DIAG: applyCommand Stop REJECTED deck=%d lifecycle=%d",
                          static_cast<int>(command.deck), static_cast<int>(deck.lifecycle));
            return ngks::CommandResult::IllegalTransition;
        }
        ngks::diagLog("DIAG: applyCommand Stop APPLIED deck=%d", static_cast<int>(command.deck));
        deck.lifecycle = DeckLifecycleState::Stopped;
        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Stopping;
            audioGraph.beginDeckStopFade(command.deck, fadeSamplesTotal);
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Pause:
        ngks::diagLog("DIAG: applyCommand Pause deck=%d transport_before=%d",
                      static_cast<int>(command.deck), static_cast<int>(deck.transport));
        if (deck.transport == ngks::TransportState::Playing || deck.transport == ngks::TransportState::Starting) {
            deck.transport = ngks::TransportState::Paused;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::Seek:
        ngks::diagLog("DIAG: applyCommand Seek deck=%d target=%.3f hasTrack=%d len=%.3f",
                      static_cast<int>(command.deck), command.seekSeconds, deck.hasTrack, deck.lengthSeconds);
        if (deck.hasTrack) {
            deck.playheadSeconds = std::clamp(command.seekSeconds, 0.0, deck.lengthSeconds);
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
    case ngks::CommandType::SetDeckFilter:
        if (!audioGraph.setDeckFilter(command.deck, command.floatValue)) {
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
    case ngks::CommandType::SetEqBandGain:
        if (!audioGraph.setEqBandGain(command.deck, command.slotIndex, command.floatValue)) {
            return ngks::CommandResult::RejectedInvalidSlot;
        }
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetEqBypass:
        audioGraph.setEqBypass(command.deck, command.boolValue != 0);
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetDeckMute:
        deck.muted = (command.boolValue != 0);
        return ngks::CommandResult::Applied;
    case ngks::CommandType::SetDeckCueMonitor:
        deck.cueEnabled = (command.boolValue != 0);
        return ngks::CommandResult::Applied;
    }

    return ngks::CommandResult::None;
}

void EngineCore::process(float* left, float* right, int numSamples) noexcept
{
    const auto callbackStart = std::chrono::high_resolution_clock::now();
    const auto callbackSteadyNow = std::chrono::steady_clock::now();
    telemetry_.audioCallbacks.fetch_add(1u, std::memory_order_relaxed);
    telemetry_.rtCallbackCount.fetch_add(1u, std::memory_order_relaxed);

    const uint64_t callbackNowNs = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        callbackSteadyNow.time_since_epoch()).count());
    const uint64_t previousCallbackNs = telemetry_.rtLastCallbackNs.exchange(callbackNowNs, std::memory_order_relaxed);
    if (previousCallbackNs > 0u && callbackNowNs > previousCallbackNs) {
        const uint64_t intervalNs = callbackNowNs - previousCallbackNs;
        telemetry_.rtCallbackIntervalNsLast.store(intervalNs, std::memory_order_relaxed);
        updateMaxRelaxedU64(telemetry_.rtCallbackIntervalNsMaxWindow, intervalNs);

        const int32_t sampleRate = telemetry_.rtSampleRate.load(std::memory_order_relaxed);
        const int32_t bufferFrames = telemetry_.rtBufferFrames.load(std::memory_order_relaxed);
        uint64_t expectedIntervalNs = intervalNs;
        if (sampleRate > 0 && bufferFrames > 0) {
            expectedIntervalNs = static_cast<uint64_t>((static_cast<double>(bufferFrames) * 1000000000.0)
                / static_cast<double>(sampleRate));
        }

        const uint64_t jitterAbsNs = (intervalNs >= expectedIntervalNs)
            ? (intervalNs - expectedIntervalNs)
            : (expectedIntervalNs - intervalNs);
        updateMaxRelaxedU64(telemetry_.rtJitterAbsNsMaxWindow, jitterAbsNs);
    }

    if (numSamples <= 0 || left == nullptr || right == nullptr) {
        telemetry_.xruns.fetch_add(1u, std::memory_order_relaxed);
        telemetry_.rtXRunCount.fetch_add(1u, std::memory_order_relaxed);
        telemetry_.lastRenderDurationUs.store(0u, std::memory_order_relaxed);

        const auto callbackEnd = std::chrono::high_resolution_clock::now();
        const auto callbackDurationUs = static_cast<uint32_t>(std::max<int64_t>(0,
            std::chrono::duration_cast<std::chrono::microseconds>(callbackEnd - callbackStart).count()));
        telemetry_.lastCallbackDurationUs.store(callbackDurationUs, std::memory_order_relaxed);
        updateMaxRelaxed(telemetry_.maxCallbackDurationUs, callbackDurationUs);
        telemetry_.rtLastCallbackUs.store(static_cast<int32_t>(callbackDurationUs), std::memory_order_relaxed);
        updateMaxRelaxedInt(telemetry_.rtMaxCallbackUs, static_cast<int32_t>(callbackDurationUs));
        pushRenderDurationSample(0u);
        return;
    }

    const int64_t callbackTickMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    telemetry_.rtLastCallbackTickMs.store(callbackTickMs, std::memory_order_relaxed);

    if (audioOpened.load(std::memory_order_acquire)) {
        setRunState(EngineRunState::RtRunning);
    }

    const auto renderStart = std::chrono::high_resolution_clock::now();

    const uint32_t front = frontSnapshotIndex.load(std::memory_order_acquire);

    ngks::EngineSnapshot working = snapshots[front];

    {
        std::unique_lock<std::mutex> lock(outcomeMutex_, std::try_to_lock);
        if (lock.owns_lock() && hasPendingOutcome_) {
            working = pendingOutcome_;
            hasPendingOutcome_ = false;
        }
    }

    if (audioOpened.load(std::memory_order_acquire)) {
        working.flags |= ngks::SNAP_AUDIO_RUNNING;
    }

    // DJ device-lost: override flags so UI cannot see stale alive state
    if (djDeviceLost_.load(std::memory_order_acquire)) {
        working.flags &= ~ngks::SNAP_AUDIO_RUNNING;
        working.flags |= ngks::SNAP_DJ_DEVICE_LOST;
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

    appendJobResults(working);

    computeCrossfadeWeights(working, crossfaderPosition_, mixMatrix_,
                           outputMode_.load(std::memory_order_relaxed));

    const auto graphStats = audioGraph.render(working, mixMatrix_, numSamples, left, right);

    // (Split Mono routing moved to after masterBus_.process())

    if (telemetry_.rtAudioEnabled.load(std::memory_order_relaxed) != 0u) {
        const float toneHz = rtToneHz_.load(std::memory_order_relaxed);
        const float toneLinear = rtToneLinear_.load(std::memory_order_relaxed);
        const float phaseStep = (sampleRateHz > 1.0) ? (kTwoPi * toneHz / static_cast<float>(sampleRateHz)) : 0.0f;
        for (int i = 0; i < numSamples; ++i) {
            const float sample = std::sin(rtTonePhase_) * toneLinear;
            rtTonePhase_ += phaseStep;
            if (rtTonePhase_ >= kTwoPi) {
                rtTonePhase_ -= kTwoPi;
            }
            left[i] += sample;
            right[i] += sample;
        }
    }

    masterBus_.setGainTrim(static_cast<float>(working.masterGain));
    const auto masterMeters = masterBus_.process(left, right, numSamples);

    // Full Mono mode (after master bus gain/limiting):
    // master summed to mono → LEFT, cue summed to mono → RIGHT (with cue volume + cue/master blend)
    if (outputMode_.load(std::memory_order_relaxed) == 1
        && graphStats.cueBusL != nullptr
        && graphStats.cueBusR != nullptr) {
        const int cueSamples = std::min(numSamples, graphStats.cueBusSamples);
        const float cueVol = cueVolume_.load(std::memory_order_relaxed);
        const float cueMix = cueMixRatio_.load(std::memory_order_relaxed);
        // cueMix: 0.0=cue only, 0.5=balanced, 1.0=master only (in headphone channel)
        for (int i = 0; i < cueSamples; ++i) {
            const float masterMono = 0.5f * (left[i] + right[i]);
            const float cueMono = 0.5f * (graphStats.cueBusL[i] + graphStats.cueBusR[i]) * cueVol;
            left[i] = masterMono;
            right[i] = cueMono * (1.0f - cueMix) + masterMono * cueMix;
        }
    }
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

        // Left channel peak smoothing
        if (graphStats.decks[deckIndex].peakL >= deckPeakSmoothingL[deckIndex]) {
            deckPeakSmoothingL[deckIndex] = graphStats.decks[deckIndex].peakL;
            deckPeakHoldBlocksL[deckIndex] = peakHoldBlocks;
        } else if (deckPeakHoldBlocksL[deckIndex] > 0) {
            --deckPeakHoldBlocksL[deckIndex];
        } else {
            deckPeakSmoothingL[deckIndex] *= peakDecayFactor;
        }

        // Right channel peak smoothing
        if (graphStats.decks[deckIndex].peakR >= deckPeakSmoothingR[deckIndex]) {
            deckPeakSmoothingR[deckIndex] = graphStats.decks[deckIndex].peakR;
            deckPeakHoldBlocksR[deckIndex] = peakHoldBlocks;
        } else if (deckPeakHoldBlocksR[deckIndex] > 0) {
            --deckPeakHoldBlocksR[deckIndex];
        } else {
            deckPeakSmoothingR[deckIndex] *= peakDecayFactor;
        }

        deck.peakL = deckPeakSmoothingL[deckIndex];
        deck.peakR = deckPeakSmoothingR[deckIndex];
        instantaneousMasterPeak = std::max(instantaneousMasterPeak, std::max(deck.peakL, deck.peakR));

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
            deck.playheadSeconds = audioGraph.getDeckNode(deckIndex).getPlayheadSeconds();
            if (deck.lengthSeconds > 0.0 && deck.playheadSeconds >= deck.lengthSeconds) {
                deck.playheadSeconds = deck.lengthSeconds;
                if (deck.transport == ngks::TransportState::Playing) {
                    deck.transport = ngks::TransportState::Stopped;
                    deck.lifecycle = DeckLifecycleState::Stopped;
                }
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

    sanitizeSnapshot(working);
    publishSnapshot(working);

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
    telemetry_.rtLastCallbackUs.store(static_cast<int32_t>(callbackDurationUs), std::memory_order_relaxed);
    updateMaxRelaxedInt(telemetry_.rtMaxCallbackUs, static_cast<int32_t>(callbackDurationUs));

    const float peak = std::max(std::abs(masterMeters.masterPeakL), std::abs(masterMeters.masterPeakR));
    const float safePeak = std::max(peak, 0.0000001f);
    const int32_t peakDb10 = static_cast<int32_t>(std::lround(20.0f * std::log10(safePeak) * 10.0f));
    telemetry_.rtMeterPeakDb10.store(peakDb10, std::memory_order_relaxed);
}

bool EngineCore::loadFileIntoDeck(ngks::DeckId deckId, const std::string& filePath, double& outDurationSeconds, uint64_t trackLoadGen)
{
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto elapsedMs = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    };

    outDurationSeconds = 0.0;
    if (deckId >= ngks::MAX_DECKS) return false;

    ngks::audioTrace("LOAD_INTO_DECK_BEGIN", "deck=%d path=%s gen=%llu",
                     (int)deckId, filePath.c_str(), (unsigned long long)trackLoadGen);
    ngks::diagLog("DIAG: EngineCore::loadFileIntoDeck deck=%d path=%s gen=%llu", (int)deckId, filePath.c_str(), (unsigned long long)trackLoadGen);
    auto& deckNode = audioGraph.getDeckNode(deckId);
    if (!deckNode.loadFile(filePath, outDurationSeconds)) {
        ngks::diagLog("DIAG: EngineCore::loadFileIntoDeck DECODE_FAIL");
        return false;
    }
    ngks::audioTrace("LOAD_INTO_DECK_DECODE_OK", "deck=%d dur=%.3fs decodeMs=%lld",
                     (int)deckId, outDurationSeconds, elapsedMs());
    ngks::diagLog("DIAG: EngineCore::loadFileIntoDeck DECODE_OK dur=%.3fs", outDurationSeconds);

    // Enqueue LoadTrack command with real duration passed via seekSeconds
    ngks::Command cmd{};
    cmd.type = ngks::CommandType::LoadTrack;
    cmd.deck = deckId;
    cmd.seq = internalCommandSeq_.fetch_add(1u, std::memory_order_relaxed);
    cmd.trackUidHash = std::hash<std::string>{}(filePath);
    cmd.seekSeconds = outDurationSeconds;
    cmd.trackLoadGen = trackLoadGen;

    // Extract filename stem as track label for the snapshot
    {
        auto slash = filePath.find_last_of("/\\");
        std::string stem = (slash != std::string::npos) ? filePath.substr(slash + 1) : filePath;
        auto dot = stem.find_last_of('.');
        if (dot != std::string::npos) stem = stem.substr(0, dot);
        const size_t maxLen = sizeof(cmd.trackLabel) - 1;
        const size_t copyLen = (stem.size() < maxLen) ? stem.size() : maxLen;
        for (size_t i = 0; i < copyLen; ++i)
            cmd.trackLabel[i] = stem[i];
        cmd.trackLabel[copyLen] = '\0';
    }

    enqueueCommand(cmd);
    ngks::audioTrace("LOAD_INTO_DECK_DONE", "deck=%d seq=%u totalMs=%lld",
                     (int)deckId, cmd.seq, elapsedMs());
    ngks::diagLog("DIAG: EngineCore::loadFileIntoDeck LoadTrack cmd enqueued seq=%u gen=%llu", cmd.seq, (unsigned long long)trackLoadGen);
    return true;
}

void EngineCore::seekDeck(ngks::DeckId deckId, double seconds)
{
    if (deckId >= ngks::MAX_DECKS) return;

    audioGraph.getDeckNode(deckId).seekTo(seconds);

    ngks::Command cmd{};
    cmd.type = ngks::CommandType::Seek;
    cmd.deck = deckId;
    cmd.seq = internalCommandSeq_.fetch_add(1u, std::memory_order_relaxed);
    cmd.seekSeconds = seconds;
    enqueueCommand(cmd);
}

std::vector<ngks::WaveMinMax> EngineCore::getWaveformOverview(ngks::DeckId deckId, int numBins)
{
    if (deckId >= ngks::MAX_DECKS) return {};
    return audioGraph.getDeckNode(deckId).generateWaveformOverview(numBins);
}

std::vector<ngks::BandEnergy> EngineCore::getBandEnergyOverview(ngks::DeckId deckId, int numBins)
{
    if (deckId >= ngks::MAX_DECKS) return {};
    return audioGraph.getDeckNode(deckId).generateBandEnergyOverview(numBins);
}

bool EngineCore::isDeckFullyDecoded(ngks::DeckId deckId) const
{
    if (deckId >= ngks::MAX_DECKS) return false;
    return audioGraph.getDeckNode(deckId).isFullyDecoded();
}

std::string EngineCore::getDeckFilePath(ngks::DeckId deckId) const
{
    if (deckId >= ngks::MAX_DECKS) return {};
    return audioGraph.getDeckNode(deckId).loadedFilePath();
}
