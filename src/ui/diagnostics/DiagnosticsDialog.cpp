#include "ui/diagnostics/DiagnosticsDialog.h"

#include <QClipboard>
#include <QGuiApplication>
#include <QLabel>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QTimer>
#include <QVBoxLayout>
#include <QHBoxLayout>

#include <algorithm>
#include <fstream>
#include <string>
#include <vector>

// ── Formatting helpers ────────────────────────────────────────────────────────
QString utcNowIso()
{
    return QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
}

QString statusSummaryLine(const UIStatus& status)
{
    return QStringLiteral(
        "StatusReady=%1 peakLinear=%2 sampleRateHz=%3 blockSize=%4 limiterActive=%5 lastUpdateUtc=%6")
        .arg(status.engineReady ? QStringLiteral("TRUE") : QStringLiteral("FALSE"),
             QString::number(status.masterPeakLinear, 'f', 6),
             QString::number(status.sampleRateHz),
             QString::number(status.blockSize),
             QStringLiteral("N/A"),
             QString::fromStdString(status.lastUpdateUtc));
}

QString rtWatchdogStateText(int32_t code)
{
    switch (code) {
    case 0: return QStringLiteral("GRACE");
    case 1: return QStringLiteral("ACTIVE");
    case 2: return QStringLiteral("STALL");
    case 3: return QStringLiteral("FAILED");
    default: return QStringLiteral("UNKNOWN");
    }
}

QString healthSummaryLine(const UIHealthSnapshot& health)
{
    return QStringLiteral(
        "HealthEngineInit=%1 HealthAudioReady=%2 HealthRenderOK=%3 RenderCycleCounter=%4")
        .arg(boolToFlag(health.engineInitialized),
             boolToFlag(health.audioDeviceReady),
             boolToFlag(health.lastRenderCycleOk),
             QString::number(static_cast<qulonglong>(health.renderCycleCounter)));
}

QString telemetrySummaryLine(const UIEngineTelemetrySnapshot& telemetry)
{
    return QStringLiteral(
        "TelemetryRenderCycles=%1 TelemetryAudioCallbacks=%2 TelemetryXRuns=%3 "
        "TelemetryLastRenderUs=%4 TelemetryMaxRenderUs=%5 "
        "TelemetryLastCallbackUs=%6 TelemetryMaxCallbackUs=%7")
        .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
             QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)),
             QString::number(static_cast<qulonglong>(telemetry.xruns)),
             QString::number(telemetry.lastRenderDurationUs),
             QString::number(telemetry.maxRenderDurationUs),
             QString::number(telemetry.lastCallbackDurationUs),
             QString::number(telemetry.maxCallbackDurationUs));
}

QString agSummaryLine(const UIEngineTelemetrySnapshot& telemetry)
{
    return QStringLiteral(
        "RTAudioDeviceId=%1 RTAudioDeviceName=%2 "
        "RTAudioAGRequestedSR=%3 RTAudioAGRequestedBufferFrames=%4 RTAudioAGRequestedChOut=%5 "
        "RTAudioAGAppliedSR=%6 RTAudioAGAppliedBufferFrames=%7 RTAudioAGAppliedChOut=%8 RTAudioAGFallback=%9")
        .arg(QString::fromUtf8(telemetry.rtDeviceId),
             QString::fromUtf8(telemetry.rtDeviceName),
             QString::number(telemetry.rtRequestedSampleRate),
             QString::number(telemetry.rtRequestedBufferFrames),
             QString::number(telemetry.rtRequestedChannelsOut),
             QString::number(telemetry.rtSampleRate),
             QString::number(telemetry.rtBufferFrames),
             QString::number(telemetry.rtChannelsOut),
             telemetry.rtAgFallback ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));
}

QString telemetrySparkline(const UIEngineTelemetrySnapshot& telemetry)
{
    static const char* levels = " .:-=+*#%@";
    constexpr int levelCount = 10;

    uint32_t count = telemetry.renderDurationWindowCount;
    if (count > UIEngineTelemetrySnapshot::kRenderDurationWindowSize)
        count = UIEngineTelemetrySnapshot::kRenderDurationWindowSize;
    if (count == 0u) return QStringLiteral("(empty)");

    uint32_t peak = 1u;
    for (uint32_t i = 0u; i < count; ++i)
        peak = std::max(peak, telemetry.renderDurationWindowUs[i]);

    QString line;
    line.reserve(static_cast<int>(count));
    for (uint32_t i = 0u; i < count; ++i) {
        const uint32_t value = telemetry.renderDurationWindowUs[i];
        const int idx = static_cast<int>((static_cast<uint64_t>(value) * (levelCount - 1)) / peak);
        line.append(QChar::fromLatin1(levels[idx]));
    }
    return line;
}

QString foundationReportLine(const UIFoundationSnapshot& foundation)
{
    return QStringLiteral(
        "EngineInit=%1 OfflineRender=%2 Telemetry=%3 HealthSnapshot=%4 "
        "Diagnostics=%5 TelemetryRenderCycles=%6 HealthRenderOK=%7")
        .arg(passFail(foundation.engineInit),
             passFail(foundation.offlineRender),
             passFail(foundation.telemetry),
             passFail(foundation.healthSnapshot),
             passFail(foundation.diagnostics),
             QString::number(static_cast<qulonglong>(foundation.telemetryRenderCycles)),
             foundation.healthRenderOk ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));
}

QString foundationBlockText(const UIFoundationSnapshot& foundation, const UISelfTestSnapshot* selfTests)
{
    QString text = QStringLiteral(
        "Foundation:\n"
        "  EngineInit: %1\n"
        "  OfflineRender: %2\n"
        "  Telemetry: %3\n"
        "  HealthSnapshot: %4\n"
        "  Diagnostics: %5\n"
        "  TelemetryRenderCycles: %6\n"
        "  HealthRenderOK: %7")
        .arg(passFail(foundation.engineInit),
             passFail(foundation.offlineRender),
             passFail(foundation.telemetry),
             passFail(foundation.healthSnapshot),
             passFail(foundation.diagnostics),
             QString::number(static_cast<qulonglong>(foundation.telemetryRenderCycles)),
             foundation.healthRenderOk ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));

    if (selfTests != nullptr) {
        text += QStringLiteral(
            "\n  SelfTests: %1"
            "\n    SelfTest_TelemetryReadable: %2"
            "\n    SelfTest_HealthReadable: %3"
            "\n    SelfTest_OfflineRenderPasses: %4")
                .arg(passFail(selfTests->allPass),
                     passFail(selfTests->telemetryReadable),
                     passFail(selfTests->healthReadable),
                     passFail(selfTests->offlineRenderPasses));
    }
    return text;
}

// ── DiagnosticsDialog constructor ────────────────────────────────────────────
DiagnosticsDialog::DiagnosticsDialog(EngineBridge& engineBridge, QWidget* parent)
    : QDialog(parent)
    , bridge_(engineBridge)
{
    setWindowTitle(QStringLiteral("Diagnostics"));
    resize(780, 430);

    auto* layout = new QVBoxLayout(this);

    auto* pathLabel = new QLabel(QStringLiteral("ui_qt.log: %1").arg(uiLogAbsolutePath()), this);
    pathLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(pathLabel);

    auto* row = new QHBoxLayout();
    auto* refreshButton  = new QPushButton(QStringLiteral("Refresh Log Tail"), this);
    auto* copyButton     = new QPushButton(QStringLiteral("Copy Report"), this);
    auto* rtProbeButton  = new QPushButton(QStringLiteral("Start RT Probe (440Hz/5s)"), this);
    row->addWidget(refreshButton);
    row->addWidget(copyButton);
    row->addWidget(rtProbeButton);
    row->addStretch(1);
    layout->addLayout(row);

    statusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), this);
    statusLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(statusLabel_);

    detailsLabel_ = new QLabel(
        QStringLiteral("StatusReady=FALSE peakLinear=0 sampleRateHz=0 blockSize=0 limiterActive=N/A lastUpdateUtc=N/A"), this);
    detailsLabel_->setWordWrap(true);
    detailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(detailsLabel_);

    lastUpdateLabel_ = new QLabel(QStringLiteral("Last status update: N/A"), this);
    lastUpdateLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(lastUpdateLabel_);

    healthLabel_ = new QLabel(
        QStringLiteral("Engine Health:\n  Initialized: FALSE\n  Audio Ready: FALSE\n  Render OK: FALSE\n  Render Cycles: 0"), this);
    healthLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(healthLabel_);

    telemetryLabel_ = new QLabel(
        QStringLiteral("Telemetry:\n  Render Cycles: 0\n  Audio Callbacks: 0\n  XRuns: 0\n"
                       "  Last Render Us: 0\n  Max Render Us: 0\n  Last Callback Us: 0\n"
                       "  Max Callback Us: 0\n  Sparkline: (empty)"), this);
    telemetryLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    layout->addWidget(telemetryLabel_);

    foundationLabel_ = new QLabel(
        QStringLiteral("Foundation:\n  EngineInit: FAIL\n  OfflineRender: FAIL\n  Telemetry: FAIL\n"
                       "  HealthSnapshot: FAIL\n  Diagnostics: FAIL\n  TelemetryRenderCycles: 0\n  HealthRenderOK: FALSE"), this);
    foundationLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    foundationLabel_->setWordWrap(true);
    layout->addWidget(foundationLabel_);

    rtAudioLabel_ = new QLabel(
        QStringLiteral("RT Audio:\n  DeviceOpen: FALSE\n  Device: <none>\n  SampleRate: 0\n"
                       "  BufferFrames: 0\n  ChannelsOut: 0\n  CallbackCount: 0\n  XRuns: 0\n"
                       "  PeakDb: -120.0\n  Watchdog: FALSE"), this);
    rtAudioLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
    rtAudioLabel_->setWordWrap(true);
    layout->addWidget(rtAudioLabel_);

    logTailBox_ = new QPlainTextEdit(this);
    logTailBox_->setReadOnly(true);
    layout->addWidget(logTailBox_);

    QObject::connect(refreshButton, &QPushButton::clicked, this, &DiagnosticsDialog::refreshLogTail);
    QObject::connect(copyButton,    &QPushButton::clicked, this, &DiagnosticsDialog::copyReportToClipboard);
    QObject::connect(rtProbeButton, &QPushButton::clicked, this, [this]() {
        bridge_.startRtProbe(440.0, -12.0);
        QTimer::singleShot(5000, this, [this]() { bridge_.stopRtProbe(); });
    });

    qInfo() << "DiagnosticsDialogConstructed=PASS";
    refreshLogTail();
}

void DiagnosticsDialog::setStatus(const UIStatus& status)
{
    statusLabel_->setText(status.engineReady ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));
    detailsLabel_->setText(statusSummaryLine(status));
    lastUpdateLabel_->setText(QStringLiteral("Last status update: %1").arg(QString::fromStdString(status.lastUpdateUtc)));
}

void DiagnosticsDialog::setHealth(const UIHealthSnapshot& health)
{
    healthLabel_->setText(
        QStringLiteral("Engine Health:\n  Initialized: %1\n  Audio Ready: %2\n  Render OK: %3\n  Render Cycles: %4")
            .arg(boolToFlag(health.engineInitialized),
                 boolToFlag(health.audioDeviceReady),
                 boolToFlag(health.lastRenderCycleOk),
                 QString::number(static_cast<qulonglong>(health.renderCycleCounter))));
}

void DiagnosticsDialog::setTelemetry(const UIEngineTelemetrySnapshot& telemetry)
{
    telemetryLabel_->setText(
        QStringLiteral("Telemetry:\n  Render Cycles: %1\n  Audio Callbacks: %2\n  XRuns: %3\n"
                       "  Last Render Us: %4\n  Max Render Us: %5\n  Last Callback Us: %6\n"
                       "  Max Callback Us: %7\n  Sparkline: %8")
            .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
                 QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)),
                 QString::number(static_cast<qulonglong>(telemetry.xruns)),
                 QString::number(telemetry.lastRenderDurationUs),
                 QString::number(telemetry.maxRenderDurationUs),
                 QString::number(telemetry.lastCallbackDurationUs),
                 QString::number(telemetry.maxCallbackDurationUs),
                 telemetrySparkline(telemetry)));
}

void DiagnosticsDialog::refreshLogTail()
{
    std::ifstream stream(gLogPath);
    if (!stream.is_open()) {
        logTailBox_->setPlainText(QStringLiteral("log missing"));
        return;
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(stream, line)) lines.push_back(line);

    const size_t start = (lines.size() > 20u) ? (lines.size() - 20u) : 0u;
    QStringList tail;
    for (size_t i = start; i < lines.size(); ++i)
        tail.push_back(QString::fromStdString(lines[i]));

    logTailBox_->setPlainText(tail.isEmpty() ? QStringLiteral("log missing") : tail.join('\n'));
}

void DiagnosticsDialog::setFoundation(const UIFoundationSnapshot& foundation, const UISelfTestSnapshot* selfTests)
{
    foundationText_ = foundationBlockText(foundation, selfTests);
    foundationLabel_->setText(foundationText_);
}

void DiagnosticsDialog::setRtAudio(const UIEngineTelemetrySnapshot& telemetry)
{
    const double peakDb = static_cast<double>(telemetry.rtMeterPeakDb10) / 10.0;
    rtAudioLabel_->setText(
        QStringLiteral("RT Audio:\n  DeviceOpen: %1\n  DeviceId: %2\n  DeviceName: %3\n"
                       "  Requested: sr=%4 buffer=%5 ch_out=%6\n"
                       "  Applied: sr=%7 buffer=%8 ch_out=%9\n"
                       "  Fallback: %10\n  CallbackCount: %11\n  XRuns: %12\n"
                       "  XRunsTotal: %13\n  XRunsWindow: %14\n  JitterMaxNsWindow: %15\n"
                       "  RestartCount: %16\n  WatchdogState: %17\n"
                       "  LastDeviceErrorCode: %18\n  PeakDb: %19\n  Watchdog: %20")
            .arg(boolToFlag(telemetry.rtDeviceOpenOk),
                 QString::fromUtf8(telemetry.rtDeviceId),
                 QString::fromUtf8(telemetry.rtDeviceName),
                 QString::number(telemetry.rtRequestedSampleRate),
                 QString::number(telemetry.rtRequestedBufferFrames),
                 QString::number(telemetry.rtRequestedChannelsOut),
                 QString::number(telemetry.rtSampleRate),
                 QString::number(telemetry.rtBufferFrames),
                 QString::number(telemetry.rtChannelsOut),
                 telemetry.rtAgFallback ? QStringLiteral("TRUE") : QStringLiteral("FALSE"),
                 QString::number(static_cast<qulonglong>(telemetry.rtCallbackCount)),
                 QString::number(static_cast<qulonglong>(telemetry.rtXRunCount)),
                 QString::number(static_cast<qulonglong>(telemetry.rtXRunCountTotal)),
                 QString::number(static_cast<qulonglong>(telemetry.rtXRunCountWindow)),
                 QString::number(static_cast<qulonglong>(telemetry.rtJitterAbsNsMaxWindow)),
                 QString::number(telemetry.rtDeviceRestartCount),
                 rtWatchdogStateText(telemetry.rtWatchdogStateCode),
                 QString::number(telemetry.rtLastDeviceErrorCode),
                 QString::number(peakDb, 'f', 1),
                 boolToFlag(telemetry.rtWatchdogOk)));
}

void DiagnosticsDialog::copyReportToClipboard()
{
    QString report;
    report += statusLabel_->text()    + '\n';
    report += detailsLabel_->text()   + '\n';
    report += healthLabel_->text()    + '\n';
    report += telemetryLabel_->text() + '\n';
    report += foundationText_;
    report += '\n' + rtAudioLabel_->text();
    if (QGuiApplication::clipboard() != nullptr)
        QGuiApplication::clipboard()->setText(report);
}
