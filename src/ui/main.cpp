#include <QAction>
#include <QApplication>
#include <QDialog>
#include <QHBoxLayout>
#include <QLabel>
#include <QMainWindow>
#include <QMenuBar>
#include <QMessageLogContext>
#include <QMutex>
#include <QMutexLocker>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QShortcut>
#include <QStringList>
#include <QTimer>
#include <QVBoxLayout>
#include <QDateTime>
#include <QWidget>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include "ui/EngineBridge.h"

#ifndef NGKS_BUILD_STAMP
#define NGKS_BUILD_STAMP "unknown"
#endif

#ifndef NGKS_GIT_SHA
#define NGKS_GIT_SHA "unknown"
#endif

namespace {

QMutex gLogMutex;
std::string gLogPath;
bool gConsoleEcho = false;

QString uiLogAbsolutePath()
{
    return QString::fromStdString(std::filesystem::absolute(gLogPath).string());
}

const char* levelToText(QtMsgType type)
{
    switch (type) {
    case QtDebugMsg:
        return "DEBUG";
    case QtInfoMsg:
        return "INFO";
    case QtWarningMsg:
        return "WARN";
    case QtCriticalMsg:
        return "CRIT";
    case QtFatalMsg:
        return "FATAL";
    default:
        return "UNKNOWN";
    }
}

void writeLine(const QString& line)
{
    QMutexLocker locker(&gLogMutex);
    if (!gLogPath.empty()) {
        std::ofstream stream(gLogPath, std::ios::app);
        if (stream.is_open()) {
            stream << line.toStdString() << '\n';
            stream.flush();
        }
    }
    if (gConsoleEcho) {
        std::cerr << line.toStdString() << std::endl;
    }
}

void qtRuntimeMessageHandler(QtMsgType type, const QMessageLogContext& context, const QString& msg)
{
    const QString ts = QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
    const QString category = context.category ? QString::fromUtf8(context.category) : QStringLiteral("qt");
    const QString file = context.file ? QString::fromUtf8(context.file) : QStringLiteral("?");
    const int line = context.line;
    const QString text = QStringLiteral("%1 [%2] [%3] %4:%5 %6")
                             .arg(ts,
                                  QString::fromUtf8(levelToText(type)),
                                  category,
                                  file,
                                  QString::number(line),
                                  msg);
    writeLine(text);

    if (type == QtFatalMsg) {
        abort();
    }
}

void initializeUiRuntimeLog()
{
    std::filesystem::create_directories("data/runtime");
    gLogPath = "data/runtime/ui_qt.log";

    const QString echoValue = qEnvironmentVariable("NGKS_UI_LOG_ECHO").trimmed().toLower();
    gConsoleEcho = (echoValue == QStringLiteral("1") || echoValue == QStringLiteral("true") || echoValue == QStringLiteral("yes"));

    qInstallMessageHandler(qtRuntimeMessageHandler);

    const QString banner = QStringLiteral("=== UI bootstrap BuildStamp=%1 GitSHA=%2 ===")
                               .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA));
    writeLine(banner);
}

QString utcNowIso()
{
    return QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
}

QString statusSummaryLine(const UIStatus& status)
{
    return QStringLiteral("StatusReady=%1 peakLinear=%2 sampleRateHz=%3 blockSize=%4 limiterActive=%5 lastUpdateUtc=%6")
        .arg(status.engineReady ? QStringLiteral("TRUE") : QStringLiteral("FALSE"),
             QString::number(status.masterPeakLinear, 'f', 6),
             QString::number(status.sampleRateHz),
             QString::number(status.blockSize),
             QStringLiteral("N/A"),
             QString::fromStdString(status.lastUpdateUtc));
}

QString boolToFlag(bool value)
{
    return value ? QStringLiteral("TRUE") : QStringLiteral("FALSE");
}

QString healthSummaryLine(const UIHealthSnapshot& health)
{
    return QStringLiteral("HealthEngineInit=%1 HealthAudioReady=%2 HealthRenderOK=%3 RenderCycleCounter=%4")
        .arg(boolToFlag(health.engineInitialized),
             boolToFlag(health.audioDeviceReady),
             boolToFlag(health.lastRenderCycleOk),
             QString::number(static_cast<qulonglong>(health.renderCycleCounter)));
}

QString telemetrySummaryLine(const UIEngineTelemetrySnapshot& telemetry)
{
    return QStringLiteral("TelemetryRenderCycles=%1 TelemetryXRuns=%2 TelemetryLastRenderUs=%3")
        .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
             QString::number(static_cast<qulonglong>(telemetry.xruns)),
             QString::number(telemetry.lastRenderDurationUs));
}

class DiagnosticsDialog : public QDialog {
public:
    explicit DiagnosticsDialog(QWidget* parent = nullptr)
        : QDialog(parent)
    {
        setWindowTitle(QStringLiteral("Diagnostics"));
        resize(780, 430);

        auto* layout = new QVBoxLayout(this);

        auto* pathLabel = new QLabel(QStringLiteral("ui_qt.log: %1").arg(uiLogAbsolutePath()), this);
        pathLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(pathLabel);

        auto* row = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("Refresh Log Tail"), this);
        row->addWidget(refreshButton);
        row->addStretch(1);
        layout->addLayout(row);

        statusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), this);
        statusLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(statusLabel_);

        detailsLabel_ = new QLabel(QStringLiteral("StatusReady=FALSE peakLinear=0 sampleRateHz=0 blockSize=0 limiterActive=N/A lastUpdateUtc=N/A"), this);
        detailsLabel_->setWordWrap(true);
        detailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(detailsLabel_);

        lastUpdateLabel_ = new QLabel(QStringLiteral("Last status update: N/A"), this);
        lastUpdateLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(lastUpdateLabel_);

        healthLabel_ = new QLabel(
            QStringLiteral("Engine Health:\n  Initialized: FALSE\n  Audio Ready: FALSE\n  Render OK: FALSE\n  Render Cycles: 0"),
            this);
        healthLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(healthLabel_);

        telemetryLabel_ = new QLabel(
            QStringLiteral("Telemetry:\n  Render Cycles: 0\n  Audio Callbacks: 0\n  XRuns: 0\n  Last Render Us: 0"),
            this);
        telemetryLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(telemetryLabel_);

        logTailBox_ = new QPlainTextEdit(this);
        logTailBox_->setReadOnly(true);
        layout->addWidget(logTailBox_);

        QObject::connect(refreshButton, &QPushButton::clicked, this, &DiagnosticsDialog::refreshLogTail);

        qInfo() << "DiagnosticsDialogConstructed=PASS";
        refreshLogTail();
    }

    void setStatus(const UIStatus& status)
    {
        statusLabel_->setText(status.engineReady ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));
        detailsLabel_->setText(statusSummaryLine(status));
        lastUpdateLabel_->setText(QStringLiteral("Last status update: %1").arg(QString::fromStdString(status.lastUpdateUtc)));
    }

    void setHealth(const UIHealthSnapshot& health)
    {
        healthLabel_->setText(
            QStringLiteral("Engine Health:\n  Initialized: %1\n  Audio Ready: %2\n  Render OK: %3\n  Render Cycles: %4")
                .arg(boolToFlag(health.engineInitialized),
                     boolToFlag(health.audioDeviceReady),
                     boolToFlag(health.lastRenderCycleOk),
                     QString::number(static_cast<qulonglong>(health.renderCycleCounter))));
    }

    void setTelemetry(const UIEngineTelemetrySnapshot& telemetry)
    {
        telemetryLabel_->setText(
            QStringLiteral("Telemetry:\n  Render Cycles: %1\n  Audio Callbacks: %2\n  XRuns: %3\n  Last Render Us: %4")
                .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
                     QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)),
                     QString::number(static_cast<qulonglong>(telemetry.xruns)),
                     QString::number(telemetry.lastRenderDurationUs)));
    }

    void refreshLogTail()
    {
        std::ifstream stream(gLogPath);
        if (!stream.is_open()) {
            logTailBox_->setPlainText(QStringLiteral("log missing"));
            return;
        }

        std::vector<std::string> lines;
        std::string line;
        while (std::getline(stream, line)) {
            lines.push_back(line);
        }

        const size_t start = (lines.size() > 20u) ? (lines.size() - 20u) : 0u;
        QStringList tail;
        for (size_t i = start; i < lines.size(); ++i) {
            tail.push_back(QString::fromStdString(lines[i]));
        }

        if (tail.isEmpty()) {
            logTailBox_->setPlainText(QStringLiteral("log missing"));
        } else {
            logTailBox_->setPlainText(tail.join('\n'));
        }
    }

private:
    QLabel* statusLabel_{nullptr};
    QLabel* detailsLabel_{nullptr};
    QLabel* lastUpdateLabel_{nullptr};
    QLabel* healthLabel_{nullptr};
    QLabel* telemetryLabel_{nullptr};
    QPlainTextEdit* logTailBox_{nullptr};
};

class MainWindow : public QMainWindow {
public:
    explicit MainWindow(EngineBridge& engineBridge)
        : bridge_(engineBridge)
    {
        setWindowTitle(QStringLiteral("NGKsPlayerNative (Dev)"));
        resize(760, 300);

        auto* root = new QWidget(this);
        auto* layout = new QVBoxLayout(root);

        auto* title = new QLabel(QStringLiteral("NGKsPlayerNative (Dev)"), root);
        layout->addWidget(title);

        auto* buildInfo = new QLabel(
            QStringLiteral("BuildStamp=%1  GitSHA=%2").arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA)),
            root);
        buildInfo->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(buildInfo);

        bridgeStatusLabel_ = new QLabel(QStringLiteral("EngineBridge: OK"), root);
        layout->addWidget(bridgeStatusLabel_);

        engineStatusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), root);
        layout->addWidget(engineStatusLabel_);

        statusDetailsLabel_ = new QLabel(QStringLiteral("StatusReady=FALSE peakLinear=0 sampleRateHz=0 blockSize=0 limiterActive=N/A lastUpdateUtc=N/A"), root);
        statusDetailsLabel_->setWordWrap(true);
        statusDetailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(statusDetailsLabel_);

        healthDetailsLabel_ = new QLabel(QStringLiteral("HealthEngineInit=FALSE HealthAudioReady=FALSE HealthRenderOK=FALSE RenderCycleCounter=0"), root);
        healthDetailsLabel_->setWordWrap(true);
        healthDetailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(healthDetailsLabel_);

        telemetryDetailsLabel_ = new QLabel(QStringLiteral("TelemetryRenderCycles=0 TelemetryXRuns=0 TelemetryLastRenderUs=0"), root);
        telemetryDetailsLabel_->setWordWrap(true);
        telemetryDetailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(telemetryDetailsLabel_);

        layout->addStretch(1);
        setCentralWidget(root);

        auto* diagnosticsAction = menuBar()->addAction(QStringLiteral("Diagnostics"));
        QObject::connect(diagnosticsAction, &QAction::triggered, this, &MainWindow::showDiagnostics);

        auto* shortcut = new QShortcut(QKeySequence(QStringLiteral("Ctrl+D")), this);
        QObject::connect(shortcut, &QShortcut::activated, this, &MainWindow::showDiagnostics);

        pollTimer_.setInterval(250);
        QObject::connect(&pollTimer_, &QTimer::timeout, this, &MainWindow::pollStatus);
        pollTimer_.start();

        qInfo() << "MainWindowConstructed=PASS";
    }

private:
    void showDiagnostics()
    {
        if (!diagnosticsDialog_) {
            diagnosticsDialog_ = new DiagnosticsDialog(this);
        }
        if (!lastStatus_.lastUpdateUtc.empty()) {
            diagnosticsDialog_->setStatus(lastStatus_);
        }
        diagnosticsDialog_->setHealth(lastHealth_);
        diagnosticsDialog_->setTelemetry(lastTelemetry_);
        diagnosticsDialog_->refreshLogTail();
        diagnosticsDialog_->show();
        diagnosticsDialog_->raise();
        diagnosticsDialog_->activateWindow();
    }

    void pollStatus()
    {
        UIStatus status {};
        status.buildStamp = NGKS_BUILD_STAMP;
        status.gitSha = NGKS_GIT_SHA;
        status.lastUpdateUtc = utcNowIso().toStdString();
        const bool ready = bridge_.tryGetStatus(status);

        if (!ready) {
            status.engineReady = false;
        }

        UIHealthSnapshot health {};
        const bool healthReady = bridge_.tryGetHealth(health);
        if (!healthReady) {
            health.engineInitialized = false;
            health.audioDeviceReady = false;
            health.lastRenderCycleOk = false;
            health.renderCycleCounter = 0;
        }

        UIEngineTelemetrySnapshot telemetry {};
        const bool telemetryReady = bridge_.tryGetTelemetry(telemetry);
        if (!telemetryReady) {
            telemetry = {};
        }

        lastStatus_ = status;
        lastHealth_ = health;
        lastTelemetry_ = telemetry;
        engineStatusLabel_->setText(status.engineReady ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));
        statusDetailsLabel_->setText(statusSummaryLine(status));
        healthDetailsLabel_->setText(healthSummaryLine(health));
        telemetryDetailsLabel_->setText(telemetrySummaryLine(telemetry));

        if (diagnosticsDialog_) {
            diagnosticsDialog_->setStatus(status);
            diagnosticsDialog_->setHealth(health);
            diagnosticsDialog_->setTelemetry(telemetry);
        }

        if (!statusTickLogged_) {
            qInfo().noquote() << QStringLiteral("StatusPollTick=PASS %1").arg(statusSummaryLine(status));
            statusTickLogged_ = true;
        }

        if (!healthTickLogged_) {
            qInfo() << "HealthPollTick=PASS";
            qInfo().noquote() << QStringLiteral("HealthEngineInit=%1").arg(boolToFlag(health.engineInitialized));
            qInfo().noquote() << QStringLiteral("HealthAudioReady=%1").arg(boolToFlag(health.audioDeviceReady));
            qInfo().noquote() << QStringLiteral("HealthRenderOK=%1").arg(boolToFlag(health.lastRenderCycleOk));
            qInfo().noquote() << QStringLiteral("RenderCycleCounter=%1").arg(QString::number(static_cast<qulonglong>(health.renderCycleCounter)));
            healthTickLogged_ = true;
        }

        if (!telemetryTickLogged_) {
            qInfo() << "TelemetryPollTick=PASS";
            qInfo().noquote() << QStringLiteral("TelemetryRenderCycles=%1").arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)));
            qInfo().noquote() << QStringLiteral("TelemetryXRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("TelemetryLastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo() << "=== Telemetry Snapshot ===";
            qInfo().noquote() << QStringLiteral("RenderCycles=%1").arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)));
            qInfo().noquote() << QStringLiteral("XRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("LastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo() << "==========================";
            telemetryTickLogged_ = true;
        }
    }

public:
    void autoShowDiagnosticsIfRequested()
    {
        const QString autoshow = qEnvironmentVariable("NGKS_DIAG_AUTOSHOW").trimmed().toLower();
        if (autoshow == QStringLiteral("1") || autoshow == QStringLiteral("true") || autoshow == QStringLiteral("yes")) {
            showDiagnostics();
        }
    }

private:
    EngineBridge& bridge_;
    QTimer pollTimer_;
    DiagnosticsDialog* diagnosticsDialog_{nullptr};
    QLabel* bridgeStatusLabel_{nullptr};
    QLabel* engineStatusLabel_{nullptr};
    QLabel* statusDetailsLabel_{nullptr};
    QLabel* healthDetailsLabel_{nullptr};
    QLabel* telemetryDetailsLabel_{nullptr};
    UIStatus lastStatus_ {};
    UIHealthSnapshot lastHealth_ {};
    UIEngineTelemetrySnapshot lastTelemetry_ {};
    bool statusTickLogged_{false};
    bool healthTickLogged_{false};
    bool telemetryTickLogged_{false};
};

} // namespace

int main(int argc, char* argv[])
{
    initializeUiRuntimeLog();

    QApplication app(argc, argv);
    writeLine(QStringLiteral("UI app initialized pid=%1").arg(QString::number(QCoreApplication::applicationPid())));

    EngineBridge engineBridge;

    MainWindow window(engineBridge);
    window.show();
    window.autoShowDiagnosticsIfRequested();

    return app.exec();
}