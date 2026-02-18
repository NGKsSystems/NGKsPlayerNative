#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include <QObject>
#include <QTimer>

#include "engine/EngineCore.h"

struct UIStatus {
    std::string buildStamp;
    std::string gitSha;
    bool engineReady{false};
    int sampleRateHz{0};
    int blockSize{0};
    float masterPeakLinear{0.0f};
    std::string lastUpdateUtc;
};

struct UIHealthSnapshot {
    bool engineInitialized{false};
    bool audioDeviceReady{false};
    bool lastRenderCycleOk{false};
    uint64_t renderCycleCounter{0};
};

struct UIEngineTelemetrySnapshot {
    static constexpr uint32_t kRenderDurationWindowSize = 64u;

    uint64_t renderCycles;
    uint64_t audioCallbacks;
    uint64_t xruns;
    uint32_t lastRenderDurationUs;
    uint32_t maxRenderDurationUs;
    uint32_t lastCallbackDurationUs;
    uint32_t maxCallbackDurationUs;
    uint32_t renderDurationWindowCount;
    uint32_t renderDurationWindowUs[kRenderDurationWindowSize] {};
};

class EngineBridge final : public QObject
{
    Q_OBJECT
    Q_PROPERTY(double meterL READ meterL NOTIFY meterLChanged)
    Q_PROPERTY(double meterR READ meterR NOTIFY meterRChanged)
    Q_PROPERTY(bool running READ running NOTIFY runningChanged)

public:
    explicit EngineBridge(QObject* parent = nullptr);

    Q_INVOKABLE void start();
    Q_INVOKABLE void stop();
    Q_INVOKABLE void setMasterGain(double linear01);

    bool tryGetStatus(UIStatus& out);
    bool tryGetHealth(UIHealthSnapshot& out) const;
    bool tryGetTelemetry(UIEngineTelemetrySnapshot& out) const noexcept;

    double meterL() const noexcept;
    double meterR() const noexcept;
    bool running() const noexcept;

signals:
    void meterLChanged();
    void meterRChanged();
    void runningChanged();

private:
    void pollSnapshot();

    EngineCore engine;
    QTimer meterTimer;
    double meterLeftValue = 0.0;
    double meterRightValue = 0.0;
    bool runningValue = false;
    uint32_t nextCommandSeq = 1;

    std::atomic<bool> healthEngineInitialized { false };
    std::atomic<bool> healthAudioDeviceReady { false };
    std::atomic<bool> healthLastRenderCycleOk { false };
    std::atomic<uint64_t> healthRenderCycleCounter { 0 };
};