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
#include <QDir>
#include <QFileInfo>
#include <QGuiApplication>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QShortcut>
#include <QStringList>
#include <QSysInfo>
#include <QTimer>
#include <QVBoxLayout>
#include <QDateTime>
#include <QWidget>

#include <cstdlib>
#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <atomic>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#include "ui/EngineBridge.h"

#ifndef NGKS_BUILD_STAMP
#define NGKS_BUILD_STAMP "unknown"
#endif

#ifndef NGKS_GIT_SHA
#define NGKS_GIT_SHA "unknown"
#endif

QString detectQtBinFromPath(const QString& pathValue);
bool writeDependencySnapshot(const QString& exePath,
                             const QString& cwd,
                             const QString& pathValue,
                             const QStringList& pluginPaths);
void installCrashCaptureHandlers();

namespace {

QMutex gLogMutex;
std::string gLogPath;
bool gConsoleEcho = false;
bool gRuntimeDirReady = false;
bool gLogWritable = false;
bool gDllProbePass = false;
QString gDllProbeMissing;
std::string gJsonLogPath;
std::string gDepsSnapshotPath;
QString gPathSnapshot;
QString gQtBinUsed;
std::atomic<bool> gCrashCaptured { false };

struct DllProbeEntry {
    QString name;
    bool pass{false};
};

std::vector<DllProbeEntry> gDllProbeEntries;

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

void writeJsonEvent(const QString& level, const QString& eventName, const QJsonObject& payload)
{
    QMutexLocker locker(&gLogMutex);
    if (gJsonLogPath.empty()) {
        return;
    }

    QJsonObject root;
    root.insert(QStringLiteral("timestamp_utc"), QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs));
    root.insert(QStringLiteral("level"), level);
    root.insert(QStringLiteral("event"), eventName);
    root.insert(QStringLiteral("payload"), payload);

    const QByteArray jsonLine = QJsonDocument(root).toJson(QJsonDocument::Compact);
    std::ofstream stream(gJsonLogPath, std::ios::app | std::ios::binary);
    if (!stream.is_open()) {
        return;
    }
    stream.write(jsonLine.constData(), static_cast<std::streamsize>(jsonLine.size()));
    stream.put('\n');
    stream.flush();
}

QString truncateForLog(const QString& value, int maxChars)
{
    if (value.size() <= maxChars) {
        return value;
    }
    return value.left(maxChars) + QStringLiteral("...(truncated)");
}

bool runDllProbe(QString& missingDlls)
{
#ifdef _WIN32
    static const wchar_t* kDllNames[] = {
        L"Qt6Core.dll",
        L"Qt6Gui.dll",
        L"Qt6Qml.dll",
        L"Qt6Quick.dll",
        L"Qt6Widgets.dll",
        L"vcruntime140.dll",
        L"vcruntime140_1.dll",
        L"msvcp140.dll"
    };

    QStringList missing;
    gDllProbeEntries.clear();
    for (const wchar_t* dllName : kDllNames) {
        HMODULE handle = LoadLibraryW(dllName);
        DllProbeEntry entry;
        entry.name = QString::fromWCharArray(dllName);
        if (handle == nullptr) {
            entry.pass = false;
            missing.push_back(entry.name);
            gDllProbeEntries.push_back(entry);
            continue;
        }
        entry.pass = true;
        gDllProbeEntries.push_back(entry);
        FreeLibrary(handle);
    }

    missingDlls = missing.join(',');
    return missing.isEmpty();
#else
    missingDlls.clear();
    return true;
#endif
}

QString currentExecutablePathForLog()
{
#ifdef _WIN32
    wchar_t buffer[MAX_PATH] {};
    const DWORD length = GetModuleFileNameW(nullptr, buffer, MAX_PATH);
    if (length > 0 && length < MAX_PATH) {
        return QString::fromWCharArray(buffer, static_cast<int>(length));
    }
#endif
    return QString::fromStdString(std::filesystem::absolute(".").string());
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
    gRuntimeDirReady = std::filesystem::exists("data/runtime") && std::filesystem::is_directory("data/runtime");
    gLogPath = "data/runtime/ui_qt.log";
    gJsonLogPath = (std::filesystem::path("data") / "runtime" / "ui_qt.jsonl").string();

    const QString echoValue = qEnvironmentVariable("NGKS_UI_LOG_ECHO").trimmed().toLower();
    gConsoleEcho = (echoValue == QStringLiteral("1") || echoValue == QStringLiteral("true") || echoValue == QStringLiteral("yes"));

    qInstallMessageHandler(qtRuntimeMessageHandler);

    const QString banner = QStringLiteral("=== UI bootstrap BuildStamp=%1 GitSHA=%2 ===")
                               .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA));
    writeLine(banner);

    {
        std::ofstream stream(gLogPath, std::ios::app);
        gLogWritable = stream.is_open();
    }

    {
        std::ofstream jsonStream(gJsonLogPath, std::ios::app);
        gLogWritable = gLogWritable && jsonStream.is_open();
    }

    gDllProbePass = runDllProbe(gDllProbeMissing);

    const QString exePath = currentExecutablePathForLog();
    const QString exeDir = QFileInfo(exePath).absolutePath();
    const QString cwd = QDir::currentPath();
    const QString pathValue = qEnvironmentVariable("PATH");
    gPathSnapshot = pathValue;
    gQtBinUsed = detectQtBinFromPath(pathValue);
    const QString qtDebugPlugins = qEnvironmentVariable("QT_DEBUG_PLUGINS");

    writeLine(QStringLiteral("EnvReport BuildStamp=%1 GitSHA=%2")
                  .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA)));
    writeLine(QStringLiteral("EnvReport ExePath=%1").arg(exePath));
    writeLine(QStringLiteral("EnvReport ExeDir=%1").arg(exeDir));
    writeLine(QStringLiteral("EnvReport Cwd=%1").arg(cwd));
    writeLine(QStringLiteral("EnvReport QtVersion=%1").arg(QString::fromLatin1(QT_VERSION_STR)));
    writeLine(QStringLiteral("EnvReport PlatformProduct=%1").arg(QSysInfo::prettyProductName()));
    writeLine(QStringLiteral("EnvReport QT_DEBUG_PLUGINS=%1").arg(qtDebugPlugins.isEmpty() ? QStringLiteral("<unset>") : qtDebugPlugins));
    writeLine(QStringLiteral("EnvReport QtBinUsed=%1").arg(gQtBinUsed));
    writeLine(QStringLiteral("EnvReport PATH=%1").arg(truncateForLog(pathValue, 1024)));
    writeLine(QStringLiteral("EnvReport=PASS"));

    QJsonObject bootstrapPayload;
    bootstrapPayload.insert(QStringLiteral("build_stamp"), QStringLiteral(NGKS_BUILD_STAMP));
    bootstrapPayload.insert(QStringLiteral("git_sha"), QStringLiteral(NGKS_GIT_SHA));
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("bootstrap"), bootstrapPayload);

    QJsonObject envPayload;
    envPayload.insert(QStringLiteral("exe_path"), exePath);
    envPayload.insert(QStringLiteral("exe_dir"), exeDir);
    envPayload.insert(QStringLiteral("cwd"), cwd);
    envPayload.insert(QStringLiteral("qt_version"), QString::fromLatin1(QT_VERSION_STR));
    envPayload.insert(QStringLiteral("platform_product"), QSysInfo::prettyProductName());
    envPayload.insert(QStringLiteral("qt_debug_plugins"), qtDebugPlugins.isEmpty() ? QStringLiteral("<unset>") : qtDebugPlugins);
    envPayload.insert(QStringLiteral("path"), truncateForLog(pathValue, 1024));
    envPayload.insert(QStringLiteral("qt_bin_used"), gQtBinUsed);
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("env_report"), envPayload);

    if (gDllProbePass) {
        writeLine(QStringLiteral("DllProbe=PASS"));
    } else {
        writeLine(QStringLiteral("DllProbe=FAIL missing=%1").arg(gDllProbeMissing));
    }

    QJsonObject dllPayload;
    dllPayload.insert(QStringLiteral("pass"), gDllProbePass);
    dllPayload.insert(QStringLiteral("missing"), gDllProbeMissing);
    QJsonArray dllItems;
    for (const auto& entry : gDllProbeEntries) {
        QJsonObject item;
        item.insert(QStringLiteral("name"), entry.name);
        item.insert(QStringLiteral("pass"), entry.pass);
        dllItems.append(item);
    }
    dllPayload.insert(QStringLiteral("dlls"), dllItems);
    writeJsonEvent(gDllProbePass ? QStringLiteral("INFO") : QStringLiteral("ERROR"), QStringLiteral("dll_probe"), dllPayload);
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
    return QStringLiteral("TelemetryRenderCycles=%1 TelemetryAudioCallbacks=%2 TelemetryXRuns=%3 TelemetryLastRenderUs=%4 TelemetryMaxRenderUs=%5 TelemetryLastCallbackUs=%6 TelemetryMaxCallbackUs=%7")
        .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
             QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)),
             QString::number(static_cast<qulonglong>(telemetry.xruns)),
             QString::number(telemetry.lastRenderDurationUs),
             QString::number(telemetry.maxRenderDurationUs),
             QString::number(telemetry.lastCallbackDurationUs),
             QString::number(telemetry.maxCallbackDurationUs));
}

QString telemetrySparkline(const UIEngineTelemetrySnapshot& telemetry)
{
    static const char* levels = " .:-=+*#%@";
    constexpr int levelCount = 10;

    uint32_t count = telemetry.renderDurationWindowCount;
    if (count > UIEngineTelemetrySnapshot::kRenderDurationWindowSize) {
        count = UIEngineTelemetrySnapshot::kRenderDurationWindowSize;
    }
    if (count == 0u) {
        return QStringLiteral("(empty)");
    }

    uint32_t peak = 1u;
    for (uint32_t i = 0u; i < count; ++i) {
        peak = std::max(peak, telemetry.renderDurationWindowUs[i]);
    }

    QString line;
    line.reserve(static_cast<int>(count));
    for (uint32_t i = 0u; i < count; ++i) {
        const uint32_t value = telemetry.renderDurationWindowUs[i];
        const int idx = static_cast<int>((static_cast<uint64_t>(value) * static_cast<uint64_t>(levelCount - 1)) / peak);
        line.append(QChar::fromLatin1(levels[idx]));
    }

    return line;
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
            QStringLiteral("Telemetry:\n  Render Cycles: 0\n  Audio Callbacks: 0\n  XRuns: 0\n  Last Render Us: 0\n  Max Render Us: 0\n  Last Callback Us: 0\n  Max Callback Us: 0\n  Sparkline: (empty)"),
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
            QStringLiteral("Telemetry:\n  Render Cycles: %1\n  Audio Callbacks: %2\n  XRuns: %3\n  Last Render Us: %4\n  Max Render Us: %5\n  Last Callback Us: %6\n  Max Callback Us: %7\n  Sparkline: %8")
                .arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)),
                     QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)),
                     QString::number(static_cast<qulonglong>(telemetry.xruns)),
                     QString::number(telemetry.lastRenderDurationUs),
                     QString::number(telemetry.maxRenderDurationUs),
                     QString::number(telemetry.lastCallbackDurationUs),
                     QString::number(telemetry.maxCallbackDurationUs),
                     telemetrySparkline(telemetry)));
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

        telemetryDetailsLabel_ = new QLabel(QStringLiteral("TelemetryRenderCycles=0 TelemetryAudioCallbacks=0 TelemetryXRuns=0 TelemetryLastRenderUs=0 TelemetryMaxRenderUs=0 TelemetryLastCallbackUs=0 TelemetryMaxCallbackUs=0"), root);
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
            qInfo().noquote() << QStringLiteral("TelemetryAudioCallbacks=%1").arg(QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)));
            qInfo().noquote() << QStringLiteral("TelemetryXRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("TelemetryLastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryMaxRenderUs=%1").arg(QString::number(telemetry.maxRenderDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryLastCallbackUs=%1").arg(QString::number(telemetry.lastCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryMaxCallbackUs=%1").arg(QString::number(telemetry.maxCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetrySparkline=%1").arg(telemetrySparkline(telemetry));
            qInfo() << "=== Telemetry Snapshot ===";
            qInfo().noquote() << QStringLiteral("RenderCycles=%1").arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)));
            qInfo().noquote() << QStringLiteral("AudioCallbacks=%1").arg(QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)));
            qInfo().noquote() << QStringLiteral("XRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("LastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo().noquote() << QStringLiteral("MaxRenderUs=%1").arg(QString::number(telemetry.maxRenderDurationUs));
            qInfo().noquote() << QStringLiteral("LastCallbackUs=%1").arg(QString::number(telemetry.lastCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("MaxCallbackUs=%1").arg(QString::number(telemetry.maxCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("Sparkline=%1").arg(telemetrySparkline(telemetry));
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
    installCrashCaptureHandlers();

    QApplication app(argc, argv);

    const QString smokeFlag = qEnvironmentVariable("NGKS_UI_SMOKE").trimmed().toLower();
    const bool smokeMode = (smokeFlag == QStringLiteral("1") || smokeFlag == QStringLiteral("true") || smokeFlag == QStringLiteral("yes"));
    int smokeSeconds = 5;
    if (smokeMode) {
        bool ok = false;
        const int parsed = qEnvironmentVariable("NGKS_UI_SMOKE_SECONDS").toInt(&ok);
        if (ok && parsed > 0) {
            smokeSeconds = parsed;
        }
        writeLine(QStringLiteral("=== UI Smoke Harness ENABLED seconds=%1 ===").arg(smokeSeconds));
        QJsonObject smokePayload;
        smokePayload.insert(QStringLiteral("enabled"), true);
        smokePayload.insert(QStringLiteral("seconds"), smokeSeconds);
        writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("ui_smoke"), smokePayload);
    }

    const QStringList pluginPaths = QCoreApplication::libraryPaths();
    writeLine(QStringLiteral("QtPluginPaths=%1").arg(pluginPaths.join(';')));
    writeLine(QStringLiteral("EnvReport PlatformName=%1").arg(QGuiApplication::platformName()));
    QJsonObject pathsPayload;
    pathsPayload.insert(QStringLiteral("plugin_paths"), pluginPaths.join(';'));
    pathsPayload.insert(QStringLiteral("platform_name"), QGuiApplication::platformName());
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("qt_paths"), pathsPayload);

    const QString exePath = QCoreApplication::applicationFilePath();
    const QString cwd = QDir::currentPath();
    const bool depSnapshotOk = writeDependencySnapshot(exePath, cwd, gPathSnapshot, pluginPaths);
    if (depSnapshotOk) {
        writeLine(QStringLiteral("DepSnapshot=PASS path=%1").arg(QString::fromStdString(gDepsSnapshotPath)));
    } else {
        writeLine(QStringLiteral("DepSnapshot=FAIL path=%1").arg(QString::fromStdString(gDepsSnapshotPath)));
    }
    QJsonObject depPayload;
    depPayload.insert(QStringLiteral("pass"), depSnapshotOk);
    depPayload.insert(QStringLiteral("path"), QString::fromStdString(gDepsSnapshotPath));
    writeJsonEvent(depSnapshotOk ? QStringLiteral("INFO") : QStringLiteral("ERROR"), QStringLiteral("dep_snapshot"), depPayload);

    const bool uiSelfCheckPass = gRuntimeDirReady && gLogWritable && gDllProbePass;
    if (uiSelfCheckPass) {
        writeLine(QStringLiteral("UiSelfCheck=PASS"));
    } else {
        QStringList reasons;
        if (!gRuntimeDirReady) {
            reasons.push_back(QStringLiteral("runtime_dir_missing"));
        }
        if (!gLogWritable) {
            reasons.push_back(QStringLiteral("log_not_writable"));
        }
        if (!gDllProbePass) {
            reasons.push_back(QStringLiteral("dll_probe_failed"));
        }
        writeLine(QStringLiteral("UiSelfCheck=FAIL reasons=%1").arg(reasons.join(',')));
        QJsonObject selfCheckPayload;
        selfCheckPayload.insert(QStringLiteral("pass"), false);
        selfCheckPayload.insert(QStringLiteral("reasons"), reasons.join(','));
        writeJsonEvent(QStringLiteral("ERROR"), QStringLiteral("self_check"), selfCheckPayload);
        return 2;
    }
    QJsonObject selfCheckPayload;
    selfCheckPayload.insert(QStringLiteral("pass"), true);
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("self_check"), selfCheckPayload);

    writeLine(QStringLiteral("UI app initialized pid=%1").arg(QString::number(QCoreApplication::applicationPid())));
    QJsonObject initPayload;
    initPayload.insert(QStringLiteral("pid"), static_cast<qint64>(QCoreApplication::applicationPid()));
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("app_init"), initPayload);

    EngineBridge engineBridge;

    MainWindow window(engineBridge);
    window.show();
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("window_show"), QJsonObject());
    window.autoShowDiagnosticsIfRequested();

    QObject::connect(&app, &QCoreApplication::aboutToQuit, [&]() {
        writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("shutdown"), QJsonObject());
        if (smokeMode) {
            writeLine(QStringLiteral("UiSmokeExit=PASS seconds=%1").arg(smokeSeconds));
            QJsonObject smokeExitPayload;
            smokeExitPayload.insert(QStringLiteral("pass"), true);
            smokeExitPayload.insert(QStringLiteral("seconds"), smokeSeconds);
            writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("ui_smoke_exit"), smokeExitPayload);
        }
    });

    if (smokeMode) {
        QTimer::singleShot(smokeSeconds * 1000, &app, &QCoreApplication::quit);
    }

    return app.exec();
}

QString detectQtBinFromPath(const QString& pathValue)
{
    const QStringList entries = pathValue.split(';', Qt::SkipEmptyParts);
    for (const QString& entry : entries) {
        const QString trimmed = entry.trimmed();
        if (trimmed.contains(QStringLiteral("Qt"), Qt::CaseInsensitive)
            && trimmed.contains(QStringLiteral("bin"), Qt::CaseInsensitive)) {
            return trimmed;
        }
    }
    return QStringLiteral("<unknown>");
}

bool writeDependencySnapshot(const QString& exePath,
                             const QString& cwd,
                             const QString& pathValue,
                             const QStringList& pluginPaths)
{
    const std::filesystem::path depsPath = std::filesystem::path("data") / "runtime" / "ui_deps.txt";
    gDepsSnapshotPath = depsPath.string();

    std::ofstream stream(gDepsSnapshotPath, std::ios::trunc);
    if (!stream.is_open()) {
        return false;
    }

    stream << "BuildStamp=" << NGKS_BUILD_STAMP << "\n";
    stream << "GitSHA=" << NGKS_GIT_SHA << "\n";
    stream << "ExePath=" << exePath.toStdString() << "\n";
    stream << "ExeDir=" << QFileInfo(exePath).absolutePath().toStdString() << "\n";
    stream << "Cwd=" << cwd.toStdString() << "\n";
    stream << "QtBinUsed=" << gQtBinUsed.toStdString() << "\n";
    stream << "PATH=" << truncateForLog(pathValue, 1024).toStdString() << "\n";
    stream << "QT_DEBUG_PLUGINS=" << qEnvironmentVariable("QT_DEBUG_PLUGINS").toStdString() << "\n";
    stream << "QT_LOGGING_RULES=" << qEnvironmentVariable("QT_LOGGING_RULES").toStdString() << "\n";
    stream << "QT_PLUGIN_PATH=" << qEnvironmentVariable("QT_PLUGIN_PATH").toStdString() << "\n";
    stream << "QtPluginPaths=" << pluginPaths.join(';').toStdString() << "\n";
    stream << "DllProbeResults:\n";
    for (const auto& entry : gDllProbeEntries) {
        stream << "  " << entry.name.toStdString() << '=' << (entry.pass ? "PASS" : "FAIL") << "\n";
    }
    stream.flush();
    return true;
}

void emitCrashCapture(const QString& triggerKind, const QString& codeText, const QString& details)
{
    if (gCrashCaptured.exchange(true)) {
        return;
    }

    const QString line = QStringLiteral("CrashCapture=TRIGGERED kind=%1 code=%2 stack=not_available detail=%3")
                             .arg(triggerKind, codeText, details);
    writeLine(line);

    QJsonObject payload;
    payload.insert(QStringLiteral("kind"), triggerKind);
    payload.insert(QStringLiteral("code"), codeText);
    payload.insert(QStringLiteral("stack"), QStringLiteral("not_available"));
    payload.insert(QStringLiteral("detail"), details);
    writeJsonEvent(QStringLiteral("CRIT"), QStringLiteral("crash_capture"), payload);
}

void onTerminateHandler()
{
    emitCrashCapture(QStringLiteral("terminate"), QStringLiteral("n/a"), QStringLiteral("std::terminate"));
    std::_Exit(3);
}

void onSignalHandler(int signalCode)
{
    emitCrashCapture(QStringLiteral("signal"), QString::number(signalCode), QStringLiteral("signal_handler"));
    std::_Exit(128 + signalCode);
}

#ifdef _WIN32
LONG WINAPI onUnhandledException(EXCEPTION_POINTERS* exceptionPointers)
{
    QString codeText = QStringLiteral("0x00000000");
    if (exceptionPointers != nullptr && exceptionPointers->ExceptionRecord != nullptr) {
        codeText = QStringLiteral("0x%1").arg(
            static_cast<qulonglong>(exceptionPointers->ExceptionRecord->ExceptionCode),
            8,
            16,
            QChar('0'));
    }
    emitCrashCapture(QStringLiteral("seh"), codeText, QStringLiteral("SetUnhandledExceptionFilter"));
    return EXCEPTION_EXECUTE_HANDLER;
}
#endif

void installCrashCaptureHandlers()
{
    std::set_terminate(onTerminateHandler);
    std::signal(SIGABRT, onSignalHandler);
    std::signal(SIGSEGV, onSignalHandler);
#ifdef _WIN32
    SetUnhandledExceptionFilter(onUnhandledException);
#endif
}