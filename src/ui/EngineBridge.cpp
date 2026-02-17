#include "ui/EngineBridge.h"

#include <algorithm>

EngineBridge::EngineBridge(QObject* parent)
    : QObject(parent)
{
    meterTimer.setInterval(16);
    connect(&meterTimer, &QTimer::timeout, this, &EngineBridge::pollSnapshot);
    meterTimer.start();
}

void EngineBridge::start()
{
    const bool wasRunning = runningValue;
    runningValue = engine.startAudioIfNeeded();
    if (runningValue != wasRunning) {
        emit runningChanged();
    }
}

void EngineBridge::stop()
{
    engine.stopWithFade();
}

void EngineBridge::setMasterGain(double linear01)
{
    engine.setMasterGain(std::clamp(linear01, 0.0, 1.0));
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
    const bool nowRunning = engine.isRunning();

    const double newL = std::clamp(static_cast<double>(snapshot.left), 0.0, 1.0);
    const double newR = std::clamp(static_cast<double>(snapshot.right), 0.0, 1.0);

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