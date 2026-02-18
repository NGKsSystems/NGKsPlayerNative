#include "ui/EngineBridge.h"

#include <algorithm>
#include <cmath>

#include "engine/command/Command.h"

EngineBridge::EngineBridge(QObject* parent)
    : QObject(parent)
{
    healthEngineInitialized.store(true, std::memory_order_relaxed);
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_A, nextCommandSeq++, 1001ULL, 0.0f, 0 });
    engine.enqueueCommand({ ngks::CommandType::LoadTrack, ngks::DECK_B, nextCommandSeq++, 1002ULL, 0.0f, 0 });

    meterTimer.setInterval(16);
    connect(&meterTimer, &QTimer::timeout, this, &EngineBridge::pollSnapshot);
    meterTimer.start();
}

void EngineBridge::start()
{
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
}

void EngineBridge::stop()
{
    engine.enqueueCommand({ ngks::CommandType::Stop, ngks::DECK_A, nextCommandSeq++, 0, 0.0f, 0 });
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_A, nextCommandSeq++, 0, static_cast<float>(std::clamp(linear01, 0.0, 1.0)), 0 });
}

bool EngineBridge::tryGetStatus(UIStatus& out)
{
    const auto snapshot = engine.getSnapshot();
    out.engineReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    out.sampleRateHz = 0;
    out.blockSize = 0;
    out.masterPeakLinear = std::max(snapshot.masterPeakL, snapshot.masterPeakR);
    return out.engineReady;
}

bool EngineBridge::tryGetHealth(UIHealthSnapshot& out) const
{
    out.engineInitialized = healthEngineInitialized.load(std::memory_order_relaxed);
    out.audioDeviceReady = healthAudioDeviceReady.load(std::memory_order_relaxed);
    out.lastRenderCycleOk = healthLastRenderCycleOk.load(std::memory_order_relaxed);
    out.renderCycleCounter = healthRenderCycleCounter.load(std::memory_order_relaxed);
    return out.engineInitialized;
}

double EngineBridge::meterL() const noexcept
{
    return meterLeftValue;
}

double EngineBridge::meterR() const noexcept
{
    return meterRightValue;
}

bool EngineBridge::running() const noexcept
{
    return runningValue;
}

void EngineBridge::pollSnapshot()
{
    const auto snapshot = engine.getSnapshot();
    const double newL = std::clamp(static_cast<double>(snapshot.decks[ngks::DECK_A].peakL), 0.0, 1.0);
    const double newR = std::clamp(static_cast<double>(snapshot.decks[ngks::DECK_A].peakR), 0.0, 1.0);
    const auto transport = snapshot.decks[ngks::DECK_A].transport;
    const bool nowRunning = (transport == ngks::TransportState::Starting)
        || (transport == ngks::TransportState::Playing)
        || (transport == ngks::TransportState::Stopping);

    const bool audioReady = (snapshot.flags & ngks::SNAP_AUDIO_RUNNING) != 0u;
    const bool renderOk = std::isfinite(snapshot.masterPeakL)
        && std::isfinite(snapshot.masterPeakR)
        && std::isfinite(snapshot.masterRmsL)
        && std::isfinite(snapshot.masterRmsR);

    healthAudioDeviceReady.store(audioReady, std::memory_order_relaxed);
    healthLastRenderCycleOk.store(renderOk, std::memory_order_relaxed);
    healthRenderCycleCounter.fetch_add(1u, std::memory_order_relaxed);

    if (newL != meterLeftValue) {
        meterLeftValue = newL;
        emit meterLChanged();
    }

    if (newR != meterRightValue) {
        meterRightValue = newR;
        emit meterRChanged();
    }

    if (nowRunning != runningValue) {
        runningValue = nowRunning;
        emit runningChanged();
    }
}