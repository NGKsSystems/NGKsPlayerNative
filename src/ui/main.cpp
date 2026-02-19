#include <QAction>
#include <QApplication>
#include <QComboBox>
#include <QDialog>
#include <QHBoxLayout>
#include <QLabel>
#include <QMainWindow>
#include <QMenuBar>
#include <QMessageLogContext>
#include <QMessageBox>
#include <QMutex>
#include <QMutexLocker>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QGuiApplication>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QSaveFile>
#include <QShortcut>
#include <QSignalBlocker>
#include <QStringList>
#include <QSysInfo>
#include <QTimer>
#include <QVBoxLayout>
#include <QDateTime>
#include <QWidget>
#include <QClipboard>

#include <cstdlib>
#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <atomic>
#include <functional>
#include <map>
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

const QString kAudioProfilesPath = QStringLiteral("data/runtime/audio_device_profiles.json");

struct UiAudioProfile {
    QString deviceId;
    QString deviceName;
    int sampleRate{0};
    int bufferFrames{0};
    int channelsOut{2};
};

struct UiAudioProfilesStore {
    QString activeProfile;
    std::map<QString, UiAudioProfile> profiles;
    QJsonObject root;
};

bool loadUiAudioProfiles(UiAudioProfilesStore& outStore, QString& outError)
{
    outStore = {};
    outError.clear();

    QFile file(kAudioProfilesPath);
    if (!file.exists()) {
        outError = QStringLiteral("No profiles found");
        return false;
    }
    if (!file.open(QIODevice::ReadOnly)) {
        outError = QStringLiteral("Unable to open profiles file");
        return false;
    }

    QJsonParseError parseError {};
    const QJsonDocument doc = QJsonDocument::fromJson(file.readAll(), &parseError);
    if (parseError.error != QJsonParseError::NoError || !doc.isObject()) {
        outError = QStringLiteral("Invalid profiles JSON");
        return false;
    }

    outStore.root = doc.object();
    outStore.activeProfile = outStore.root.value(QStringLiteral("active_profile")).toString();

    const QJsonObject profilesObj = outStore.root.value(QStringLiteral("profiles")).toObject();
    for (auto it = profilesObj.begin(); it != profilesObj.end(); ++it) {
        if (!it.value().isObject()) {
            continue;
        }
        const QJsonObject p = it.value().toObject();
        UiAudioProfile profile {};
        profile.deviceId = p.value(QStringLiteral("device_id")).toString();
        profile.deviceName = p.value(QStringLiteral("device_name")).toString();
        profile.sampleRate = p.value(QStringLiteral("sample_rate")).toInt(p.value(QStringLiteral("sr")).toInt(0));
        profile.bufferFrames = p.value(QStringLiteral("buffer_frames")).toInt(p.value(QStringLiteral("buffer")).toInt(128));
        profile.channelsOut = p.value(QStringLiteral("channels_out")).toInt(p.value(QStringLiteral("ch_out")).toInt(2));
        outStore.profiles[it.key()] = profile;
    }

    if (outStore.profiles.empty()) {
        outError = QStringLiteral("No profiles found");
        return false;
    }

    if (outStore.activeProfile.isEmpty() || outStore.profiles.find(outStore.activeProfile) == outStore.profiles.end()) {
        outStore.activeProfile = outStore.profiles.begin()->first;
    }

    return true;
}

bool writeUiAudioProfilesActiveProfile(const UiAudioProfilesStore& store, const QString& activeProfile, QString& outError)
{
    outError.clear();
    QJsonObject root = store.root;
    if (root.isEmpty()) {
        root.insert(QStringLiteral("profiles"), QJsonObject());
    }

    root.insert(QStringLiteral("active_profile"), activeProfile);

    QSaveFile saveFile(kAudioProfilesPath);
    if (!saveFile.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
        outError = QStringLiteral("Unable to open profiles file for write");
        return false;
    }

    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (saveFile.write(payload) != payload.size()) {
        outError = QStringLiteral("Failed writing profiles file");
        saveFile.cancelWriting();
        return false;
    }

    if (!saveFile.commit()) {
        outError = QStringLiteral("Failed to commit profiles file");
        return false;
    }

    return true;
}

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

QString rtWatchdogStateText(int32_t code)
{
    switch (code) {
    case 0:
        return QStringLiteral("GRACE");
    case 1:
        return QStringLiteral("ACTIVE");
    case 2:
        return QStringLiteral("STALL");
    case 3:
        return QStringLiteral("FAILED");
    default:
        return QStringLiteral("UNKNOWN");
    }
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

QString agSummaryLine(const UIEngineTelemetrySnapshot& telemetry)
{
    return QStringLiteral("RTAudioDeviceId=%1 RTAudioDeviceName=%2 RTAudioAGRequestedSR=%3 RTAudioAGRequestedBufferFrames=%4 RTAudioAGRequestedChOut=%5 RTAudioAGAppliedSR=%6 RTAudioAGAppliedBufferFrames=%7 RTAudioAGAppliedChOut=%8 RTAudioAGFallback=%9")
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

QString passFail(bool value)
{
    return value ? QStringLiteral("PASS") : QStringLiteral("FAIL");
}

QString foundationReportLine(const UIFoundationSnapshot& foundation)
{
    return QStringLiteral("EngineInit=%1 OfflineRender=%2 Telemetry=%3 HealthSnapshot=%4 Diagnostics=%5 TelemetryRenderCycles=%6 HealthRenderOK=%7")
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

class DiagnosticsDialog : public QDialog {
public:
    explicit DiagnosticsDialog(EngineBridge& engineBridge, QWidget* parent = nullptr)
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
        auto* refreshButton = new QPushButton(QStringLiteral("Refresh Log Tail"), this);
        auto* copyButton = new QPushButton(QStringLiteral("Copy Report"), this);
        auto* rtProbeButton = new QPushButton(QStringLiteral("Start RT Probe (440Hz/5s)"), this);
        row->addWidget(refreshButton);
        row->addWidget(copyButton);
        row->addWidget(rtProbeButton);
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

        foundationLabel_ = new QLabel(
            QStringLiteral("Foundation:\n  EngineInit: FAIL\n  OfflineRender: FAIL\n  Telemetry: FAIL\n  HealthSnapshot: FAIL\n  Diagnostics: FAIL\n  TelemetryRenderCycles: 0\n  HealthRenderOK: FALSE"),
            this);
        foundationLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        foundationLabel_->setWordWrap(true);
        layout->addWidget(foundationLabel_);

        rtAudioLabel_ = new QLabel(
            QStringLiteral("RT Audio:\n  DeviceOpen: FALSE\n  Device: <none>\n  SampleRate: 0\n  BufferFrames: 0\n  ChannelsOut: 0\n  CallbackCount: 0\n  XRuns: 0\n  PeakDb: -120.0\n  Watchdog: FALSE"),
            this);
        rtAudioLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        rtAudioLabel_->setWordWrap(true);
        layout->addWidget(rtAudioLabel_);

        logTailBox_ = new QPlainTextEdit(this);
        logTailBox_->setReadOnly(true);
        layout->addWidget(logTailBox_);

        QObject::connect(refreshButton, &QPushButton::clicked, this, &DiagnosticsDialog::refreshLogTail);
        QObject::connect(copyButton, &QPushButton::clicked, this, &DiagnosticsDialog::copyReportToClipboard);
        QObject::connect(rtProbeButton, &QPushButton::clicked, this, [this]() {
            bridge_.startRtProbe(440.0, -12.0);
            QTimer::singleShot(5000, this, [this]() { bridge_.stopRtProbe(); });
        });

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

    void setFoundation(const UIFoundationSnapshot& foundation, const UISelfTestSnapshot* selfTests)
    {
        foundationText_ = foundationBlockText(foundation, selfTests);
        foundationLabel_->setText(foundationText_);
    }

    void setRtAudio(const UIEngineTelemetrySnapshot& telemetry)
    {
        const double peakDb = static_cast<double>(telemetry.rtMeterPeakDb10) / 10.0;
        rtAudioLabel_->setText(
            QStringLiteral("RT Audio:\n  DeviceOpen: %1\n  DeviceId: %2\n  DeviceName: %3\n  Requested: sr=%4 buffer=%5 ch_out=%6\n  Applied: sr=%7 buffer=%8 ch_out=%9\n  Fallback: %10\n  CallbackCount: %11\n  XRuns: %12\n  XRunsTotal: %13\n  XRunsWindow: %14\n  JitterMaxNsWindow: %15\n  RestartCount: %16\n  WatchdogState: %17\n  LastDeviceErrorCode: %18\n  PeakDb: %19\n  Watchdog: %20")
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

    void copyReportToClipboard()
    {
        QString report;
        report += statusLabel_->text() + '\n';
        report += detailsLabel_->text() + '\n';
        report += healthLabel_->text() + '\n';
        report += telemetryLabel_->text() + '\n';
        report += foundationText_;
        report += '\n' + rtAudioLabel_->text();
        if (QGuiApplication::clipboard() != nullptr) {
            QGuiApplication::clipboard()->setText(report);
        }
    }

private:
    EngineBridge& bridge_;
    QLabel* statusLabel_{nullptr};
    QLabel* detailsLabel_{nullptr};
    QLabel* lastUpdateLabel_{nullptr};
    QLabel* healthLabel_{nullptr};
    QLabel* telemetryLabel_{nullptr};
    QLabel* foundationLabel_{nullptr};
    QLabel* rtAudioLabel_{nullptr};
    QPlainTextEdit* logTailBox_{nullptr};
    QString foundationText_;
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

        agDetailsLabel_ = new QLabel(QStringLiteral("RTAudioDeviceId=<none> RTAudioDeviceName=<none> RTAudioAGRequestedSR=0 RTAudioAGRequestedBufferFrames=0 RTAudioAGRequestedChOut=0 RTAudioAGAppliedSR=0 RTAudioAGAppliedBufferFrames=0 RTAudioAGAppliedChOut=0 RTAudioAGFallback=FALSE"), root);
        agDetailsLabel_->setWordWrap(true);
        agDetailsLabel_->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(agDetailsLabel_);

        auto* profileRow = new QHBoxLayout();
        auto* profileLabel = new QLabel(QStringLiteral("Audio Profile:"), root);
        audioProfileCombo_ = new QComboBox(root);
        audioProfileCombo_->setMinimumWidth(320);
        refreshAudioProfilesButton_ = new QPushButton(QStringLiteral("Refresh"), root);
        applyAudioProfileButton_ = new QPushButton(QStringLiteral("Apply"), root);
        profileRow->addWidget(profileLabel);
        profileRow->addWidget(audioProfileCombo_, 1);
        profileRow->addWidget(refreshAudioProfilesButton_);
        profileRow->addWidget(applyAudioProfileButton_);
        layout->addLayout(profileRow);

        QObject::connect(refreshAudioProfilesButton_, &QPushButton::clicked, this, [this]() {
            refreshAudioProfilesUi(true);
        });
        QObject::connect(applyAudioProfileButton_, &QPushButton::clicked, this, &MainWindow::applySelectedAudioProfile);

        refreshAudioProfilesUi(true);

        const QString akApplyAutorun = qEnvironmentVariable("NGKS_AK_AUTORUN_APPLY").trimmed().toLower();
        if (akApplyAutorun == QStringLiteral("1") || akApplyAutorun == QStringLiteral("true") || akApplyAutorun == QStringLiteral("yes")) {
            QTimer::singleShot(200, this, &MainWindow::applySelectedAudioProfile);
        }

        layout->addStretch(1);
        setCentralWidget(root);

        auto* diagnosticsAction = menuBar()->addAction(QStringLiteral("Diagnostics"));
        QObject::connect(diagnosticsAction, &QAction::triggered, this, &MainWindow::showDiagnostics);

        auto* shortcut = new QShortcut(QKeySequence(QStringLiteral("Ctrl+D")), this);
        QObject::connect(shortcut, &QShortcut::activated, this, &MainWindow::showDiagnostics);

        pollTimer_.setInterval(250);
        QObject::connect(&pollTimer_, &QTimer::timeout, this, &MainWindow::pollStatus);
        pollTimer_.start();

        const QString autorun = qEnvironmentVariable("NGKS_SELFTEST_AUTORUN").trimmed().toLower();
        selfTestAutorun_ = (autorun == QStringLiteral("1") || autorun == QStringLiteral("true") || autorun == QStringLiteral("yes"));
        const QString rtAutorun = qEnvironmentVariable("NGKS_RT_AUDIO_AUTORUN").trimmed().toLower();
        rtProbeAutorun_ = (rtAutorun == QStringLiteral("1") || rtAutorun == QStringLiteral("true") || rtAutorun == QStringLiteral("yes"));

        qInfo() << "MainWindowConstructed=PASS";

        if (selfTestAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::runFoundationSelfTests);
        }
        if (rtProbeAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::startRtProbeAutorun);
        }
    }

private:
    void refreshAudioProfilesUi(bool logMarker)
    {
        UiAudioProfilesStore store {};
        QString loadError;
        const bool loaded = loadUiAudioProfiles(store, loadError);

        {
            const QSignalBlocker blocker(audioProfileCombo_);
            audioProfileCombo_->clear();
            if (loaded) {
                for (const auto& entry : store.profiles) {
                    const QString& profileName = entry.first;
                    const UiAudioProfile& profile = entry.second;
                    const QString itemText = QStringLiteral("%1 (sr=%2, buf=%3, ch=%4)")
                                                 .arg(profileName,
                                                      QString::number(profile.sampleRate),
                                                      QString::number(profile.bufferFrames),
                                                      QString::number(profile.channelsOut));
                    audioProfileCombo_->addItem(itemText, profileName);
                }

                const int activeIndex = audioProfileCombo_->findData(store.activeProfile);
                if (activeIndex >= 0) {
                    audioProfileCombo_->setCurrentIndex(activeIndex);
                }
            }
        }

        audioProfilesStore_ = store;
        const bool controlsEnabled = loaded && !audioProfilesStore_.profiles.empty();
        audioProfileCombo_->setEnabled(controlsEnabled);
        applyAudioProfileButton_->setEnabled(controlsEnabled);

        if (!controlsEnabled) {
            const QString reason = loadError.isEmpty() ? QStringLiteral("No profiles available") : loadError;
            qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(reason);
            if (logMarker && diagnosticsDialog_ != nullptr) {
                diagnosticsDialog_->refreshLogTail();
            }
            return;
        }

        if (logMarker || lastAkActiveProfileMarker_ != audioProfilesStore_.activeProfile) {
            qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(audioProfilesStore_.activeProfile);
            lastAkActiveProfileMarker_ = audioProfilesStore_.activeProfile;
        }
    }

    void applySelectedAudioProfile()
    {
        const QString profileName = audioProfileCombo_->currentData().toString();
        const auto profileIt = audioProfilesStore_.profiles.find(profileName);
        if (profileName.isEmpty() || profileIt == audioProfilesStore_.profiles.end()) {
            qInfo().noquote() << QStringLiteral("RTAudioAKApplyProfile=FAIL");
            QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Selected profile is not valid."));
            return;
        }

        const UiAudioProfile& profile = profileIt->second;
        const bool applied = bridge_.applyAudioProfile(profile.deviceId.toStdString(),
                                                       profile.deviceName.toStdString(),
                                                       profile.sampleRate,
                                                       profile.bufferFrames,
                                                       profile.channelsOut);
        if (!applied) {
            qInfo().noquote() << QStringLiteral("RTAudioAKApplyProfile=FAIL");
            QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Failed to apply selected profile."));
            return;
        }

        QString saveError;
        if (!writeUiAudioProfilesActiveProfile(audioProfilesStore_, profileName, saveError)) {
            qInfo().noquote() << QStringLiteral("RTAudioAKApplyProfile=FAIL");
            QMessageBox::warning(this,
                                 QStringLiteral("Audio Profile"),
                                 QStringLiteral("Profile applied, but active_profile was not persisted: %1").arg(saveError));
            return;
        }

        qInfo().noquote() << QStringLiteral("RTAudioAKApplyProfile=PASS");
        qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(profileName);
        lastAgMarkerKey_.clear();
        refreshAudioProfilesUi(false);
    }

    void showDiagnostics()
    {
        if (!diagnosticsDialog_) {
            diagnosticsDialog_ = new DiagnosticsDialog(bridge_, this);
        }
        if (!lastStatus_.lastUpdateUtc.empty()) {
            diagnosticsDialog_->setStatus(lastStatus_);
        }
        diagnosticsDialog_->setHealth(lastHealth_);
        diagnosticsDialog_->setTelemetry(lastTelemetry_);
        diagnosticsDialog_->setFoundation(lastFoundation_, selfTestsRan_ ? &lastSelfTests_ : nullptr);
        diagnosticsDialog_->setRtAudio(lastTelemetry_);
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

        int64_t stallMs = 0;
        const bool watchdogOk = bridge_.pollRtWatchdog(500, stallMs);
        telemetry.rtWatchdogOk = watchdogOk;

        UIFoundationSnapshot foundation {};
        const bool foundationReady = bridge_.tryGetFoundation(foundation);
        if (!foundationReady) {
            foundation = {};
        }

        lastStatus_ = status;
        lastHealth_ = health;
        lastTelemetry_ = telemetry;
        lastFoundation_ = foundation;
        engineStatusLabel_->setText(status.engineReady ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));
        statusDetailsLabel_->setText(statusSummaryLine(status));
        healthDetailsLabel_->setText(healthSummaryLine(health));
        telemetryDetailsLabel_->setText(telemetrySummaryLine(telemetry));
        agDetailsLabel_->setText(agSummaryLine(telemetry));

        if (diagnosticsDialog_) {
            diagnosticsDialog_->setStatus(status);
            diagnosticsDialog_->setHealth(health);
            diagnosticsDialog_->setTelemetry(telemetry);
            diagnosticsDialog_->setFoundation(foundation, selfTestsRan_ ? &lastSelfTests_ : nullptr);
            diagnosticsDialog_->setRtAudio(telemetry);
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

        if (!foundationTickLogged_) {
            qInfo() << "FoundationPollTick=PASS";
            qInfo().noquote() << QStringLiteral("FoundationReportLine=%1").arg(foundationReportLine(foundation));
            qInfo().noquote() << QStringLiteral("FoundationTelemetryRenderCycles=%1").arg(QString::number(static_cast<qulonglong>(foundation.telemetryRenderCycles)));
            qInfo().noquote() << QStringLiteral("FoundationHealthRenderOK=%1").arg(boolToFlag(foundation.healthRenderOk));
            foundationTickLogged_ = true;
        }

        if (selfTestsRan_ && !foundationSelfTestLogged_) {
            qInfo().noquote() << QStringLiteral("FoundationSelfTestSummary=%1").arg(passFail(lastSelfTests_.allPass));
            foundationSelfTestLogged_ = true;
        }

        qInfo() << "RTAudioPollTick=PASS";
        qInfo().noquote() << QStringLiteral("RTAudioDeviceOpen=%1").arg(boolToFlag(telemetry.rtDeviceOpenOk));
        qInfo().noquote() << QStringLiteral("RTAudioCallbackCount=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtCallbackCount)));
        qInfo().noquote() << QStringLiteral("RTAudioXRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCount)));
        qInfo().noquote() << QStringLiteral("RTAudioXRunsTotal=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCountTotal)));
        qInfo().noquote() << QStringLiteral("RTAudioXRunsWindow=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCountWindow)));
        qInfo().noquote() << QStringLiteral("RTAudioJitterMaxNsWindow=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtJitterAbsNsMaxWindow)));
        qInfo().noquote() << QStringLiteral("RTAudioDeviceRestartCount=%1").arg(QString::number(telemetry.rtDeviceRestartCount));
        qInfo().noquote() << QStringLiteral("RTAudioWatchdogState=%1").arg(rtWatchdogStateText(telemetry.rtWatchdogStateCode));
        qInfo().noquote() << QStringLiteral("RTAudioPeakDb=%1").arg(QString::number(static_cast<double>(telemetry.rtMeterPeakDb10) / 10.0, 'f', 1));
        qInfo().noquote() << QStringLiteral("RTAudioWatchdog=%1").arg(boolToFlag(telemetry.rtWatchdogOk));
        if (!telemetry.rtWatchdogOk) {
            qInfo().noquote() << QStringLiteral("RTAudioWatchdogStallMs=%1").arg(QString::number(stallMs));
        }

        if (telemetry.rtDeviceOpenOk) {
            const QString markerKey = QStringLiteral("%1|%2|%3|%4|%5|%6|%7")
                .arg(QString::fromUtf8(telemetry.rtDeviceId),
                     QString::number(telemetry.rtRequestedSampleRate),
                     QString::number(telemetry.rtRequestedBufferFrames),
                     QString::number(telemetry.rtRequestedChannelsOut),
                     QString::number(telemetry.rtSampleRate),
                     QString::number(telemetry.rtBufferFrames),
                     QString::number(telemetry.rtChannelsOut));
            if (markerKey != lastAgMarkerKey_) {
                qInfo().noquote() << QStringLiteral("RTAudioAGRequestedSR=%1").arg(QString::number(telemetry.rtRequestedSampleRate));
                qInfo().noquote() << QStringLiteral("RTAudioAGAppliedSR=%1").arg(QString::number(telemetry.rtSampleRate));
                qInfo().noquote() << QStringLiteral("RTAudioAGFallback=%1").arg(telemetry.rtAgFallback ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));
                lastAgMarkerKey_ = markerKey;
            }
        }
    }

    void runFoundationSelfTests()
    {
        UISelfTestSnapshot selfTests {};
        bridge_.runSelfTests(selfTests);
        lastSelfTests_ = selfTests;
        selfTestsRan_ = true;

        qInfo() << "SelfTestSuite=BEGIN";
        qInfo().noquote() << QStringLiteral("SelfTest_TelemetryReadable=%1").arg(passFail(selfTests.telemetryReadable));
        qInfo().noquote() << QStringLiteral("SelfTest_HealthReadable=%1").arg(passFail(selfTests.healthReadable));
        qInfo().noquote() << QStringLiteral("SelfTest_OfflineRenderPasses=%1").arg(passFail(selfTests.offlineRenderPasses));
        qInfo() << "SelfTestSuite=END";
        qInfo().noquote() << QStringLiteral("FoundationSelfTestSummary=%1").arg(passFail(selfTests.allPass));
        foundationSelfTestLogged_ = true;

        UIFoundationSnapshot foundation {};
        if (bridge_.tryGetFoundation(foundation)) {
            lastFoundation_ = foundation;
            if (diagnosticsDialog_ != nullptr) {
                diagnosticsDialog_->setFoundation(lastFoundation_, &lastSelfTests_);
            }
        }
    }

    void startRtProbeAutorun()
    {
        bridge_.startRtProbe(440.0, -12.0);
        QTimer::singleShot(5000, this, [this]() { bridge_.stopRtProbe(); });
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
    QLabel* agDetailsLabel_{nullptr};
    QComboBox* audioProfileCombo_{nullptr};
    QPushButton* refreshAudioProfilesButton_{nullptr};
    QPushButton* applyAudioProfileButton_{nullptr};
    UIStatus lastStatus_ {};
    UIHealthSnapshot lastHealth_ {};
    UIEngineTelemetrySnapshot lastTelemetry_ {};
    UIFoundationSnapshot lastFoundation_ {};
    UISelfTestSnapshot lastSelfTests_ {};
    UiAudioProfilesStore audioProfilesStore_ {};
    bool selfTestsRan_{false};
    bool selfTestAutorun_{false};
    bool rtProbeAutorun_{false};
    bool statusTickLogged_{false};
    bool healthTickLogged_{false};
    bool telemetryTickLogged_{false};
    bool foundationTickLogged_{false};
    bool foundationSelfTestLogged_{false};
    QString lastAgMarkerKey_ {};
    QString lastAkActiveProfileMarker_ {};
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