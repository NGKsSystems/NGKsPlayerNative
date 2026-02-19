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

    bool rtAudioEnabled{false};
    bool rtDeviceOpenOk{false};
    int32_t rtSampleRate{0};
    int32_t rtBufferFrames{0};
    int32_t rtRequestedSampleRate{0};
    int32_t rtRequestedBufferFrames{0};
    int32_t rtRequestedChannelsOut{2};
    int32_t rtChannelsIn{0};
    int32_t rtChannelsOut{0};
    bool rtAgFallback{false};
    uint64_t rtDeviceIdHash{0};
    uint64_t rtCallbackCount{0};
    uint64_t rtXRunCount{0};
    uint64_t rtXRunCountTotal{0};
    uint64_t rtXRunCountWindow{0};
    uint64_t rtLastCallbackNs{0};
    uint64_t rtJitterAbsNsMaxWindow{0};
    uint64_t rtCallbackIntervalNsLast{0};
    uint64_t rtCallbackIntervalNsMaxWindow{0};
    int32_t rtLastCallbackUs{0};
    int32_t rtMaxCallbackUs{0};
    int32_t rtMeterPeakDb10{-1200};
    bool rtWatchdogOk{true};
    int32_t rtWatchdogStateCode{0};
    uint32_t rtWatchdogTripCount{0};
    uint32_t rtDeviceRestartCount{0};
    int32_t rtLastDeviceErrorCode{0};
    bool rtRecoveryRequested{false};
    bool rtRecoveryFailedState{false};
    int64_t rtLastCallbackTickMs{0};
    char rtDeviceId[160] {};
    char rtDeviceName[96] {};
};

struct UISelfTestSnapshot {
    bool telemetryReadable{false};
    bool healthReadable{false};
    bool offlineRenderPasses{false};
    bool allPass{false};
};

struct UIFoundationSnapshot {
    bool engineInit{false};
    bool offlineRender{false};
    bool telemetry{false};
    bool healthSnapshot{false};
    bool diagnostics{false};
    bool selfTestsRan{false};
    bool selfTestsPass{false};
    uint64_t telemetryRenderCycles{0};
    bool healthRenderOk{false};
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
    Q_INVOKABLE bool startRtProbe(double toneHz, double toneDb);
    Q_INVOKABLE void stopRtProbe();

    bool tryGetStatus(UIStatus& out);
    bool tryGetHealth(UIHealthSnapshot& out) const;
    bool tryGetTelemetry(UIEngineTelemetrySnapshot& out) const noexcept;
    bool pollRtWatchdog(int64_t thresholdMs, int64_t& outStallMs) noexcept;
    bool runSelfTests(UISelfTestSnapshot& out) noexcept;
    bool tryGetFoundation(UIFoundationSnapshot& out) const noexcept;

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
    std::atomic<bool> selfTestsRan { false };
    std::atomic<bool> selfTestsPass { false };
};