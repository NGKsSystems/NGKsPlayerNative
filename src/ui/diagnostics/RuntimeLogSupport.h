#pragma once

#include <QJsonObject>
#include <QMutex>
#include <QString>
#include <QStringList>
#include <atomic>
#include <string>
#include <vector>

// ── DLL probe entry ──────────────────────────────────────────────────────────
struct DllProbeEntry {
    QString name;
    bool    pass{false};
};

// ── Globals (defined in RuntimeLogSupport.cpp, extern here) ─────────────────
extern std::string              gExeBaseDir;
extern std::string              gLogPath;
extern std::string              gJsonLogPath;
extern std::string              gDepsSnapshotPath;
extern bool                     gRuntimeDirReady;
extern bool                     gLogWritable;
extern bool                     gDllProbePass;
extern QString                  gDllProbeMissing;
extern QString                  gPathSnapshot;
extern QString                  gQtBinUsed;
extern std::atomic<bool>        gCrashCaptured;
extern std::vector<DllProbeEntry> gDllProbeEntries;

// ── Path helpers ─────────────────────────────────────────────────────────────
QString runtimePath(const char* relative);
QString kAudioProfilesPath();
QString kLibraryPersistPath();
QString kPlaylistsPersistPath();
QString uiLogAbsolutePath();

// ── Logging ──────────────────────────────────────────────────────────────────
void    writeLine(const QString& line);
void    writeJsonEvent(const QString& level, const QString& eventName, const QJsonObject& payload);
QString truncateForLog(const QString& value, int maxChars);

// ── Bootstrap ────────────────────────────────────────────────────────────────
void    initializeUiRuntimeLog();

// ── Dependency snapshot + crash handlers ────────────────────────────────────
bool    writeDependencySnapshot(const QString& exePath, const QString& cwd,
                                const QString& pathValue, const QStringList& pluginPaths);
QString detectQtBinFromPath(const QString& pathValue);
void    installCrashCaptureHandlers();
