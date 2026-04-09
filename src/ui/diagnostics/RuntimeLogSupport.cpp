#include "ui/diagnostics/RuntimeLogSupport.h"

#include <QDateTime>
#include <QDir>
#include <QFileInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QMutexLocker>
#include <QSaveFile>
#include <QSysInfo>

#include <algorithm>
#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#ifndef NGKS_BUILD_STAMP
#define NGKS_BUILD_STAMP "unknown"
#endif
#ifndef NGKS_GIT_SHA
#define NGKS_GIT_SHA "unknown"
#endif

// ── Global definitions ────────────────────────────────────────────────────────
std::string              gExeBaseDir;
std::string              gLogPath;
std::string              gJsonLogPath;
std::string              gDepsSnapshotPath;
bool                     gRuntimeDirReady  = false;
bool                     gLogWritable      = false;
bool                     gDllProbePass     = false;
QString                  gDllProbeMissing;
QString                  gPathSnapshot;
QString                  gQtBinUsed;
std::atomic<bool>        gCrashCaptured    { false };
std::vector<DllProbeEntry> gDllProbeEntries;

// ── Path helpers ──────────────────────────────────────────────────────────────
static std::string resolveExeBaseDir()
{
#ifdef _WIN32
    wchar_t buf[MAX_PATH]{};
    const DWORD len = GetModuleFileNameW(NULL, buf, MAX_PATH);
    if (len > 0 && len < MAX_PATH) {
        std::filesystem::path p(buf);
        return p.parent_path().string();
    }
#endif
    return std::filesystem::current_path().string();
}

QString runtimePath(const char* relative)
{
    return QString::fromStdString(
        (std::filesystem::path(gExeBaseDir) / relative).string());
}

QString kAudioProfilesPath() { return runtimePath("data/runtime/audio_device_profiles.json"); }
QString kLibraryPersistPath() { return runtimePath("data/runtime/library.json"); }
QString kPlaylistsPersistPath() { return runtimePath("data/runtime/playlists.json"); }

QString uiLogAbsolutePath()
{
    return QString::fromStdString(std::filesystem::absolute(gLogPath).string());
}

// ── Logging ───────────────────────────────────────────────────────────────────
static QMutex   s_logMutex;
static bool     s_consoleEcho = false;

QString truncateForLog(const QString& value, int maxChars)
{
    if (value.size() <= maxChars) return value;
    return value.left(maxChars) + QStringLiteral("...(truncated)");
}

void writeLine(const QString& line)
{
    QMutexLocker locker(&s_logMutex);
    if (!gLogPath.empty()) {
        std::ofstream stream(gLogPath, std::ios::app);
        if (stream.is_open()) {
            stream << line.toStdString() << '\n';
            stream.flush();
        }
    }
    if (s_consoleEcho) {
        std::cerr << line.toStdString() << std::endl;
    }
}

void writeJsonEvent(const QString& level, const QString& eventName, const QJsonObject& payload)
{
    if (gJsonLogPath.empty()) return;

    QJsonObject root;
    root.insert(QStringLiteral("timestamp_utc"), QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs));
    root.insert(QStringLiteral("level"), level);
    root.insert(QStringLiteral("event"), eventName);
    root.insert(QStringLiteral("payload"), payload);

    const QByteArray jsonLine = QJsonDocument(root).toJson(QJsonDocument::Compact);
    std::ofstream stream(gJsonLogPath, std::ios::app | std::ios::binary);
    if (!stream.is_open()) return;
    stream.write(jsonLine.constData(), static_cast<std::streamsize>(jsonLine.size()));
    stream.put('\n');
    stream.flush();
}

// ── DLL probe ─────────────────────────────────────────────────────────────────
static bool runDllProbe(QString& missingDlls)
{
#ifdef _WIN32
#ifdef _DEBUG
    static const wchar_t* kDllNames[] = {
        L"Qt6Cored.dll", L"Qt6Guid.dll", L"Qt6Sqld.dll",
        L"Qt6Widgetsd.dll", L"vcruntime140d.dll", L"msvcp140d.dll"
    };
#else
    static const wchar_t* kDllNames[] = {
        L"Qt6Core.dll", L"Qt6Gui.dll", L"Qt6Sql.dll",
        L"Qt6Widgets.dll", L"vcruntime140.dll", L"msvcp140.dll"
    };
#endif
    QStringList missing;
    gDllProbeEntries.clear();
    for (const wchar_t* dllName : kDllNames) {
        HMODULE handle = LoadLibraryW(dllName);
        DllProbeEntry entry;
        entry.name = QString::fromWCharArray(dllName);
        if (handle == nullptr) {
            entry.pass = false;
            missing.push_back(entry.name);
        } else {
            entry.pass = true;
            FreeLibrary(handle);
        }
        gDllProbeEntries.push_back(entry);
    }
    missingDlls = missing.join(',');
    return missing.isEmpty();
#else
    missingDlls.clear();
    return true;
#endif
}

// ── Qt message handler ────────────────────────────────────────────────────────
static const char* levelToText(QtMsgType type)
{
    switch (type) {
    case QtDebugMsg:    return "DEBUG";
    case QtInfoMsg:     return "INFO";
    case QtWarningMsg:  return "WARN";
    case QtCriticalMsg: return "CRIT";
    case QtFatalMsg:    return "FATAL";
    default:            return "UNKNOWN";
    }
}

static QString currentExecutablePathForLog()
{
#ifdef _WIN32
    wchar_t buffer[MAX_PATH]{};
    const DWORD length = GetModuleFileNameW(nullptr, buffer, MAX_PATH);
    if (length > 0 && length < MAX_PATH)
        return QString::fromWCharArray(buffer, static_cast<int>(length));
#endif
    return QString::fromStdString(std::filesystem::absolute(".").string());
}

static void qtRuntimeMessageHandler(QtMsgType type, const QMessageLogContext& context, const QString& msg)
{
    const QString ts       = QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
    const QString category = context.category ? QString::fromUtf8(context.category) : QStringLiteral("qt");
    const QString file     = context.file     ? QString::fromUtf8(context.file)     : QStringLiteral("?");
    const QString text = QStringLiteral("%1 [%2] [%3] %4:%5 %6")
                             .arg(ts, QString::fromUtf8(levelToText(type)), category,
                                  file, QString::number(context.line), msg);
    writeLine(text);
    if (type == QtFatalMsg) abort();
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────
void initializeUiRuntimeLog()
{
    if (gExeBaseDir.empty()) gExeBaseDir = resolveExeBaseDir();
    const auto rtDir = std::filesystem::path(gExeBaseDir) / "data" / "runtime";
    std::filesystem::create_directories(rtDir);
    gRuntimeDirReady = std::filesystem::exists(rtDir) && std::filesystem::is_directory(rtDir);
    gLogPath     = (rtDir / "ui_qt.log").string();
    gJsonLogPath = (rtDir / "ui_qt.jsonl").string();

    const QString echoValue = qEnvironmentVariable("NGKS_UI_LOG_ECHO").trimmed().toLower();
    s_consoleEcho = (echoValue == QStringLiteral("1") || echoValue == QStringLiteral("true") || echoValue == QStringLiteral("yes"));

    qInstallMessageHandler(qtRuntimeMessageHandler);

    const QString banner = QStringLiteral("=== UI bootstrap BuildStamp=%1 GitSHA=%2 ===")
                               .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA));
    writeLine(banner);

    { std::ofstream s(gLogPath, std::ios::app); gLogWritable = s.is_open(); }
    { std::ofstream s(gJsonLogPath, std::ios::app); gLogWritable = gLogWritable && s.is_open(); }

    gDllProbePass = runDllProbe(gDllProbeMissing);

    const QString exePath = currentExecutablePathForLog();
    const QString exeDir  = QFileInfo(exePath).absolutePath();
    const QString cwd     = QDir::currentPath();
    const QString pathValue = qEnvironmentVariable("PATH");
    gPathSnapshot = pathValue;
    gQtBinUsed    = detectQtBinFromPath(pathValue);
    const QString qtDebugPlugins = qEnvironmentVariable("QT_DEBUG_PLUGINS");

    writeLine(QStringLiteral("EnvReport BuildStamp=%1 GitSHA=%2")
                          .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA)));
    writeLine(QStringLiteral("EnvReport ExePath=%1").arg(exePath));
    writeLine(QStringLiteral("EnvReport ExeDir=%1").arg(exeDir));
    writeLine(QStringLiteral("EnvReport RuntimeBaseDir=%1").arg(QString::fromStdString(gExeBaseDir)));
    writeLine(QStringLiteral("EnvReport Cwd=%1").arg(cwd));
    writeLine(QStringLiteral("EnvReport QtVersion=%1").arg(QString::fromLatin1(QT_VERSION_STR)));
    writeLine(QStringLiteral("EnvReport PlatformProduct=%1").arg(QSysInfo::prettyProductName()));
    writeLine(QStringLiteral("EnvReport QT_DEBUG_PLUGINS=%1").arg(qtDebugPlugins.isEmpty() ? QStringLiteral("<unset>") : qtDebugPlugins));
    writeLine(QStringLiteral("EnvReport QtBinUsed=%1").arg(gQtBinUsed));
    writeLine(QStringLiteral("EnvReport PATH=%1").arg(truncateForLog(pathValue, 1024)));
    writeLine(QStringLiteral("EnvReport=PASS"));

    QJsonObject bootstrapPayload;
    bootstrapPayload.insert(QStringLiteral("build_stamp"), QStringLiteral(NGKS_BUILD_STAMP));
    bootstrapPayload.insert(QStringLiteral("git_sha"),     QStringLiteral(NGKS_GIT_SHA));
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("bootstrap"), bootstrapPayload);

    QJsonObject envPayload;
    envPayload.insert(QStringLiteral("exe_path"),          exePath);
    envPayload.insert(QStringLiteral("exe_dir"),           exeDir);
    envPayload.insert(QStringLiteral("cwd"),               cwd);
    envPayload.insert(QStringLiteral("qt_version"),        QString::fromLatin1(QT_VERSION_STR));
    envPayload.insert(QStringLiteral("platform_product"),  QSysInfo::prettyProductName());
    envPayload.insert(QStringLiteral("qt_debug_plugins"),  qtDebugPlugins.isEmpty() ? QStringLiteral("<unset>") : qtDebugPlugins);
    envPayload.insert(QStringLiteral("path"),              truncateForLog(pathValue, 1024));
    envPayload.insert(QStringLiteral("qt_bin_used"),       gQtBinUsed);
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("env_report"), envPayload);

    if (gDllProbePass) {
        writeLine(QStringLiteral("DllProbe=PASS"));
    } else {
        writeLine(QStringLiteral("DllProbe=FAIL missing=%1").arg(gDllProbeMissing));
    }

    QJsonObject dllPayload;
    dllPayload.insert(QStringLiteral("pass"),    gDllProbePass);
    dllPayload.insert(QStringLiteral("missing"), gDllProbeMissing);
    QJsonArray dllItems;
    for (const auto& entry : gDllProbeEntries) {
        QJsonObject item;
        item.insert(QStringLiteral("name"), entry.name);
        item.insert(QStringLiteral("pass"), entry.pass);
        dllItems.append(item);
    }
    dllPayload.insert(QStringLiteral("dlls"), dllItems);
    writeJsonEvent(gDllProbePass ? QStringLiteral("INFO") : QStringLiteral("ERROR"),
                   QStringLiteral("dll_probe"), dllPayload);
}

// ── detectQtBinFromPath ───────────────────────────────────────────────────────
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

// ── writeDependencySnapshot ───────────────────────────────────────────────────
bool writeDependencySnapshot(const QString& exePath,
                             const QString& cwd,
                             const QString& pathValue,
                             const QStringList& pluginPaths)
{
    const std::filesystem::path depsPath =
        std::filesystem::path(gExeBaseDir) / "data" / "runtime" / "ui_deps.txt";
    gDepsSnapshotPath = depsPath.string();

    std::ofstream stream(gDepsSnapshotPath, std::ios::trunc);
    if (!stream.is_open()) return false;

    stream << "BuildStamp=" << NGKS_BUILD_STAMP << "\n";
    stream << "GitSHA="     << NGKS_GIT_SHA << "\n";
    stream << "ExePath="    << exePath.toStdString() << "\n";
    stream << "ExeDir="     << QFileInfo(exePath).absolutePath().toStdString() << "\n";
    stream << "Cwd="        << cwd.toStdString() << "\n";
    stream << "QtBinUsed="  << gQtBinUsed.toStdString() << "\n";
    stream << "PATH="       << truncateForLog(pathValue, 1024).toStdString() << "\n";
    stream << "QT_DEBUG_PLUGINS=" << qEnvironmentVariable("QT_DEBUG_PLUGINS").toStdString() << "\n";
    stream << "QT_LOGGING_RULES=" << qEnvironmentVariable("QT_LOGGING_RULES").toStdString() << "\n";
    stream << "QT_PLUGIN_PATH="   << qEnvironmentVariable("QT_PLUGIN_PATH").toStdString() << "\n";
    stream << "QtPluginPaths="    << pluginPaths.join(';').toStdString() << "\n";
    stream << "DllProbeResults:\n";
    for (const auto& entry : gDllProbeEntries) {
        stream << "  " << entry.name.toStdString() << '=' << (entry.pass ? "PASS" : "FAIL") << "\n";
    }
    stream.flush();
    return true;
}

// ── Crash capture ─────────────────────────────────────────────────────────────
static void emitCrashCapture(const QString& triggerKind, const QString& codeText, const QString& details)
{
    if (gCrashCaptured.exchange(true)) return;

    const QString line = QStringLiteral("CrashCapture=TRIGGERED kind=%1 code=%2 stack=not_available detail=%3")
                             .arg(triggerKind, codeText, details);
    writeLine(line);

    QJsonObject payload;
    payload.insert(QStringLiteral("kind"),   triggerKind);
    payload.insert(QStringLiteral("code"),   codeText);
    payload.insert(QStringLiteral("stack"),  QStringLiteral("not_available"));
    payload.insert(QStringLiteral("detail"), details);
    writeJsonEvent(QStringLiteral("CRIT"), QStringLiteral("crash_capture"), payload);
}

static void onTerminateHandler()
{
    emitCrashCapture(QStringLiteral("terminate"), QStringLiteral("n/a"), QStringLiteral("std::terminate"));
    std::_Exit(3);
}

static void onSignalHandler(int signalCode)
{
    emitCrashCapture(QStringLiteral("signal"), QString::number(signalCode), QStringLiteral("signal_handler"));
    std::_Exit(128 + signalCode);
}

#ifdef _WIN32
static LONG WINAPI onUnhandledException(EXCEPTION_POINTERS* exceptionPointers)
{
    QString codeText = QStringLiteral("0x00000000");
    if (exceptionPointers != nullptr && exceptionPointers->ExceptionRecord != nullptr) {
        codeText = QStringLiteral("0x%1").arg(
            static_cast<qulonglong>(exceptionPointers->ExceptionRecord->ExceptionCode),
            8, 16, QChar('0'));
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
