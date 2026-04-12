#include "library/dj/DjLibraryWidget.h"
#include "dj/browser/DjBrowserPane.h"
#include "dj/browser/DjBrowserPane.h"
#include <QTableView>
#include <QAction>
#include <QApplication>
#include <QComboBox>
#include <QDialog>
#include <QHBoxLayout>
#include <QGridLayout>
#include <QStackedLayout>
#include <QLabel>
#include <QMainWindow>
#include <QMenu>
#include <QMenuBar>
#include <QMessageLogContext>
#include <QMessageBox>
#include <QMutex>
#include <QMutexLocker>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QScrollArea>
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
#include <QThread>
#include <QVBoxLayout>
#include <QDateTime>
#include <QWidget>
#include <QClipboard>
#include <QDesktopServices>
#include <QInputDialog>
#include <QUrl>
#include <QLineEdit>
#include <QListWidget>
#include <QTreeWidget>
#include <QHeaderView>
#include <QSplitter>
#include <QSlider>
#include <QStackedWidget>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QElapsedTimer>
#include <QFileDialog>
#include <QDirIterator>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <atomic>
#include <functional>
#include <map>
#include <random>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#include "ui/EngineBridge.h"
#include "ui/EqPanel.h"
#include "ui/DeckStrip.h"
#include "engine/DiagLog.h"

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

// ── Exe-relative runtime base dir (resolved once, before QApp) ──
std::string gExeBaseDir; // set by resolveExeBaseDir()

std::string resolveExeBaseDir()
{
#ifdef _WIN32
    wchar_t buf[MAX_PATH]{};
    const DWORD len = GetModuleFileNameW(NULL, buf, MAX_PATH);
    if (len > 0 && len < MAX_PATH) {
        std::filesystem::path p(buf);
        return p.parent_path().string();
    }
#endif
    // Fallback: CWD (original behavior)
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

struct Playlist {
    QString name;
    QStringList trackPaths;
};

bool savePlaylists(const std::vector<Playlist>& playlists)
{
    QJsonArray arr;
    for (const Playlist& pl : playlists) {
        QJsonObject obj;
        obj.insert(QStringLiteral("name"), pl.name);
        QJsonArray paths;
        for (const QString& p : pl.trackPaths) paths.append(p);
        obj.insert(QStringLiteral("tracks"), paths);
        arr.append(obj);
    }
    QJsonObject root;
    root.insert(QStringLiteral("version"), 1);
    root.insert(QStringLiteral("playlists"), arr);
    QSaveFile file(kPlaylistsPersistPath());
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) return false;
    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (file.write(payload) != payload.size()) { file.cancelWriting(); return false; }
    return file.commit();
}

bool loadPlaylists(std::vector<Playlist>& out)
{
    out.clear();
    QFile file(kPlaylistsPersistPath());
    if (!file.exists()) return false;
    if (!file.open(QIODevice::ReadOnly)) return false;
    QJsonParseError parseErr{};
    const QJsonDocument doc = QJsonDocument::fromJson(file.readAll(), &parseErr);
    if (parseErr.error != QJsonParseError::NoError || !doc.isObject()) return false;
    const QJsonArray arr = doc.object().value(QStringLiteral("playlists")).toArray();
    for (const QJsonValue& val : arr) {
        if (!val.isObject()) continue;
        const QJsonObject obj = val.toObject();
        Playlist pl;
        pl.name = obj.value(QStringLiteral("name")).toString();
        if (pl.name.isEmpty()) continue;
        const QJsonArray paths = obj.value(QStringLiteral("tracks")).toArray();
        for (const QJsonValue& pv : paths) {
            const QString p = pv.toString();
            if (!p.isEmpty()) pl.trackPaths.append(p);
        }
        out.push_back(std::move(pl));
    }
    return true;
}

struct TrackInfo {
    QString filePath;
    QString title;
    QString artist;
    QString album;
    QString displayName;
    qint64  durationMs{0};
    QString durationStr;
    QString bpm;
    QString musicalKey;
    qint64  fileSize{0};
    // Legacy DB fields
    QString genre;
    QString camelotKey;
    double  energy{-1.0};
    double  loudnessLUFS{0.0};
    double  loudnessRange{0.0};
    QString cueIn;
    QString cueOut;
    double  danceability{-1.0};
    double  acousticness{-1.0};
    double  instrumentalness{-1.0};
    double  liveness{-1.0};
    int     year{0};
    int     rating{0};
    QString comments;
    bool    legacyImported{false};
};

QString formatDurationMs(qint64 ms)
{
    if (ms <= 0) return QStringLiteral("--:--");
    const int totalSec = static_cast<int>(ms / 1000);
    const int min = totalSec / 60;
    const int sec = totalSec % 60;
    return QStringLiteral("%1:%2").arg(min).arg(sec, 2, 10, QLatin1Char('0'));
}

QString formatFileSize(qint64 bytes)
{
    if (bytes < 1024) return QStringLiteral("%1 B").arg(bytes);
    if (bytes < 1048576) return QStringLiteral("%1 KB").arg(bytes / 1024);
    return QStringLiteral("%1 MB").arg(QString::number(static_cast<double>(bytes) / 1048576.0, 'f', 1));
}

bool saveLibraryJson(const std::vector<TrackInfo>& tracks, const QString& folderPath)
{
    QJsonArray arr;
    for (const TrackInfo& t : tracks) {
        QJsonObject obj;
        obj.insert(QStringLiteral("filePath"), t.filePath);
        obj.insert(QStringLiteral("title"), t.title);
        obj.insert(QStringLiteral("artist"), t.artist);
        obj.insert(QStringLiteral("album"), t.album);
        obj.insert(QStringLiteral("displayName"), t.displayName);
        obj.insert(QStringLiteral("durationMs"), t.durationMs);
        obj.insert(QStringLiteral("durationStr"), t.durationStr);
        obj.insert(QStringLiteral("bpm"), t.bpm);
        obj.insert(QStringLiteral("musicalKey"), t.musicalKey);
        obj.insert(QStringLiteral("fileSize"), t.fileSize);
        // Legacy DB fields
        if (!t.genre.isEmpty()) obj.insert(QStringLiteral("genre"), t.genre);
        if (!t.camelotKey.isEmpty()) obj.insert(QStringLiteral("camelotKey"), t.camelotKey);
        if (t.energy >= 0) obj.insert(QStringLiteral("energy"), t.energy);
        if (t.loudnessLUFS != 0.0) obj.insert(QStringLiteral("loudnessLUFS"), t.loudnessLUFS);
        if (t.loudnessRange != 0.0) obj.insert(QStringLiteral("loudnessRange"), t.loudnessRange);
        if (!t.cueIn.isEmpty()) obj.insert(QStringLiteral("cueIn"), t.cueIn);
        if (!t.cueOut.isEmpty()) obj.insert(QStringLiteral("cueOut"), t.cueOut);
        if (t.danceability >= 0) obj.insert(QStringLiteral("danceability"), t.danceability);
        if (t.acousticness >= 0) obj.insert(QStringLiteral("acousticness"), t.acousticness);
        if (t.instrumentalness >= 0) obj.insert(QStringLiteral("instrumentalness"), t.instrumentalness);
        if (t.liveness >= 0) obj.insert(QStringLiteral("liveness"), t.liveness);
        if (t.year > 0) obj.insert(QStringLiteral("year"), t.year);
        if (t.rating > 0) obj.insert(QStringLiteral("rating"), t.rating);
        if (!t.comments.isEmpty()) obj.insert(QStringLiteral("comments"), t.comments);
        if (t.legacyImported) obj.insert(QStringLiteral("legacyImported"), true);
        arr.append(obj);
    }
    QJsonObject root;
    root.insert(QStringLiteral("version"), 1);
    root.insert(QStringLiteral("folderPath"), folderPath);
    root.insert(QStringLiteral("tracks"), arr);

    QSaveFile file(kLibraryPersistPath());
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) return false;
    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (file.write(payload) != payload.size()) { file.cancelWriting(); return false; }
    return file.commit();
}

bool loadLibraryJson(std::vector<TrackInfo>& outTracks, QString& outFolderPath)
{
    outTracks.clear();
    outFolderPath.clear();
    QFile file(kLibraryPersistPath());
    if (!file.exists()) return false;
    if (!file.open(QIODevice::ReadOnly)) return false;
    QJsonParseError parseErr{};
    const QJsonDocument doc = QJsonDocument::fromJson(file.readAll(), &parseErr);
    if (parseErr.error != QJsonParseError::NoError || !doc.isObject()) return false;
    const QJsonObject root = doc.object();
    outFolderPath = root.value(QStringLiteral("folderPath")).toString();
    const QJsonArray arr = root.value(QStringLiteral("tracks")).toArray();
    outTracks.reserve(static_cast<size_t>(arr.size()));
    for (const QJsonValue& val : arr) {
        if (!val.isObject()) continue;
        const QJsonObject obj = val.toObject();
        TrackInfo t;
        t.filePath    = obj.value(QStringLiteral("filePath")).toString();
        t.title       = obj.value(QStringLiteral("title")).toString();
        t.artist      = obj.value(QStringLiteral("artist")).toString();
        t.album       = obj.value(QStringLiteral("album")).toString();
        t.displayName = obj.value(QStringLiteral("displayName")).toString();
        t.durationMs  = obj.value(QStringLiteral("durationMs")).toInteger(0);
        t.durationStr = obj.value(QStringLiteral("durationStr")).toString();
        t.bpm         = obj.value(QStringLiteral("bpm")).toString();
        t.musicalKey  = obj.value(QStringLiteral("musicalKey")).toString();
        t.fileSize    = obj.value(QStringLiteral("fileSize")).toInteger(0);
        // Legacy DB fields
        t.genre             = obj.value(QStringLiteral("genre")).toString();
        t.camelotKey        = obj.value(QStringLiteral("camelotKey")).toString();
        t.energy            = obj.value(QStringLiteral("energy")).toDouble(-1.0);
        t.loudnessLUFS      = obj.value(QStringLiteral("loudnessLUFS")).toDouble(0.0);
        t.loudnessRange     = obj.value(QStringLiteral("loudnessRange")).toDouble(0.0);
        t.cueIn             = obj.value(QStringLiteral("cueIn")).toString();
        t.cueOut            = obj.value(QStringLiteral("cueOut")).toString();
        t.danceability      = obj.value(QStringLiteral("danceability")).toDouble(-1.0);
        t.acousticness      = obj.value(QStringLiteral("acousticness")).toDouble(-1.0);
        t.instrumentalness  = obj.value(QStringLiteral("instrumentalness")).toDouble(-1.0);
        t.liveness          = obj.value(QStringLiteral("liveness")).toDouble(-1.0);
        t.year              = obj.value(QStringLiteral("year")).toInt(0);
        t.rating            = obj.value(QStringLiteral("rating")).toInt(0);
        t.comments          = obj.value(QStringLiteral("comments")).toString();
        t.legacyImported    = obj.value(QStringLiteral("legacyImported")).toBool(false);
        if (t.filePath.isEmpty()) continue;
        outTracks.push_back(std::move(t));
    }
    return !outTracks.empty();
}

void readId3Tags(TrackInfo& track)
{
    if (!track.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) return;
    QFile f(track.filePath);
    if (!f.open(QIODevice::ReadOnly)) return;
    const QByteArray hdr = f.read(10);
    if (hdr.size() < 10 || hdr[0] != 'I' || hdr[1] != 'D' || hdr[2] != '3') return;
    const int ver = static_cast<unsigned char>(hdr[3]);
    const quint32 tagSz = (quint32(static_cast<unsigned char>(hdr[6])) << 21)
                         | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                         | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                         | quint32(static_cast<unsigned char>(hdr[9]));
    const QByteArray tag = f.read(qMin(qint64(tagSz), qint64(256 * 1024)));
    int pos = 0;
    while (pos + 10 <= tag.size()) {
        const QByteArray fid = tag.mid(pos, 4);
        if (fid[0] == '\0') break;
        quint32 fsz;
        if (ver >= 4) {
            fsz = (quint32(static_cast<unsigned char>(tag[pos+4])) << 21)
                | (quint32(static_cast<unsigned char>(tag[pos+5])) << 14)
                | (quint32(static_cast<unsigned char>(tag[pos+6])) << 7)
                | quint32(static_cast<unsigned char>(tag[pos+7]));
        } else {
            fsz = (quint32(static_cast<unsigned char>(tag[pos+4])) << 24)
                | (quint32(static_cast<unsigned char>(tag[pos+5])) << 16)
                | (quint32(static_cast<unsigned char>(tag[pos+6])) << 8)
                | quint32(static_cast<unsigned char>(tag[pos+7]));
        }
        pos += 10;
        if (fsz == 0 || pos + static_cast<int>(fsz) > tag.size()) break;
        const bool isTextFrame = (fid == "TBPM" || fid == "TKEY" || fid == "TIT2"
                                  || fid == "TPE1" || fid == "TALB" || fid == "TLEN");
        if (isTextFrame && fsz > 1) {
            const unsigned char enc = static_cast<unsigned char>(tag[pos]);
            QString val;
            if (enc == 0 || enc == 3) {
                val = QString::fromUtf8(tag.mid(pos + 1, static_cast<int>(fsz) - 1)).trimmed();
                val.remove(QChar('\0'));
            } else if (enc == 1 || enc == 2) {
                // UTF-16 (enc 1 = with BOM, enc 2 = big-endian no BOM)
                const char* raw = tag.constData() + pos + 1;
                const int rawLen = static_cast<int>(fsz) - 1;
                if (rawLen >= 2) {
                    const auto b0 = static_cast<unsigned char>(raw[0]);
                    const auto b1 = static_cast<unsigned char>(raw[1]);
                    if (enc == 1 && b0 == 0xFF && b1 == 0xFE) {
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw + 2), (rawLen - 2) / 2).trimmed();
                    } else if (enc == 1 && b0 == 0xFE && b1 == 0xFF) {
                        QByteArray swapped(rawLen - 2, '\0');
                        for (int i = 0; i < rawLen - 2; i += 2) {
                            swapped[i] = raw[2 + i + 1];
                            swapped[i + 1] = raw[2 + i];
                        }
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(swapped.constData()), swapped.size() / 2).trimmed();
                    } else {
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw), rawLen / 2).trimmed();
                    }
                    val.remove(QChar('\0'));
                }
            }
            if (!val.isEmpty()) {
                if (fid == "TBPM" && track.bpm.isEmpty()) {
                    track.bpm = val;
                    qInfo().noquote() << QStringLiteral("BPM_ANALYSIS_END source=id3_TBPM bpm=%1 path=%2")
                        .arg(val, track.filePath);
                }
                else if (fid == "TKEY" && track.musicalKey.isEmpty()) track.musicalKey = val;
                else if (fid == "TIT2" && track.title.isEmpty()) track.title = val;
                else if (fid == "TPE1" && track.artist.isEmpty()) track.artist = val;
                else if (fid == "TALB" && track.album.isEmpty()) track.album = val;
                else if (fid == "TLEN" && track.durationMs <= 0) {
                    bool ok = false;
                    const qint64 ms = val.toLongLong(&ok);
                    if (ok && ms > 0) {
                        track.durationMs = ms;
                        track.durationStr = formatDurationMs(ms);
                    }
                }
            }
        }
        pos += static_cast<int>(fsz);
    }
    // Update displayName from enriched tag data
    if (!track.artist.isEmpty() && !track.title.isEmpty()) {
        track.displayName = track.artist + QStringLiteral(" \u2014 ") + track.title;
    } else if (!track.title.isEmpty()) {
        track.displayName = track.title;
    }
}

// Normalize file path for matching: forward slashes, lowercase, trimmed
QString normalizePath(const QString& raw)
{
    return QDir::fromNativeSeparators(raw).trimmed().toLower();
}

// Locate the legacy ngksplayer library.db
QString findLegacyDbPath()
{
    // Prefer ngksplayer, fall back to proproductionsuite, then proaudioclipper
    const QStringList candidates = {
        QDir::homePath() + QStringLiteral("/AppData/Roaming/ngksplayer/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proproductionsuite/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proaudioclipper/library.db"),
    };
    for (const QString& p : candidates) {
        if (QFile::exists(p)) return p;
    }
    return {};
}

struct LegacyImportResult {
    int matched{0};
    int unmatched{0};
    int totalDbRows{0};
    QString dbPath;
};

LegacyImportResult importLegacyDb(std::vector<TrackInfo>& tracks, const QString& dbPath)
{
    LegacyImportResult result;
    result.dbPath = dbPath;

    if (dbPath.isEmpty() || !QFile::exists(dbPath)) return result;

    // Use a unique connection name to avoid the default connection
    const QString connName = QStringLiteral("legacyImport");
    {
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
        db.setDatabaseName(dbPath);
        db.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
        if (!db.open()) {
            qWarning().noquote() << QStringLiteral("LEGACY_DB_OPEN_FAIL=%1").arg(db.lastError().text());
            return result;
        }

        // Build a lookup from normalized filePath to index in tracks
        std::map<QString, size_t> pathIndex;
        for (size_t i = 0; i < tracks.size(); ++i) {
            pathIndex[normalizePath(tracks[i].filePath)] = i;
        }

        QSqlQuery q(db);
        q.setForwardOnly(true);
        if (!q.exec(QStringLiteral(
            "SELECT filePath, genre, bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange, "
            "cueIn, cueOut, danceability, acousticness, instrumentalness, liveness, "
            "year, rating, comments, album, artist, title "
            "FROM tracks"))) {
            qWarning().noquote() << QStringLiteral("LEGACY_DB_QUERY_FAIL=%1").arg(q.lastError().text());
            db.close();
            return result;
        }

        while (q.next()) {
            ++result.totalDbRows;
            const QString rawPath = q.value(0).toString();
            const QString normalized = normalizePath(rawPath);
            auto it = pathIndex.find(normalized);
            if (it == pathIndex.end()) {
                ++result.unmatched;
                continue;
            }

            TrackInfo& t = tracks[it->second];
            ++result.matched;
            t.legacyImported = true;

            // Only overwrite empty/default fields
            if (t.genre.isEmpty()) t.genre = q.value(1).toString().trimmed();

            if (t.bpm.isEmpty()) {
                const int dbBpm = q.value(2).toInt();
                if (dbBpm > 0) {
                    t.bpm = QString::number(dbBpm);
                    qInfo().noquote() << QStringLiteral("BPM_ANALYSIS_END source=legacy_db bpm=%1 path=%2")
                        .arg(dbBpm).arg(t.filePath);
                }
            }

            if (t.musicalKey.isEmpty()) t.musicalKey = q.value(3).toString().trimmed();
            if (t.camelotKey.isEmpty()) t.camelotKey = q.value(4).toString().trimmed();

            if (t.energy < 0) {
                const double v = q.value(5).toDouble();
                if (v > 0) t.energy = v;
            }

            if (t.loudnessLUFS == 0.0) t.loudnessLUFS = q.value(6).toDouble();
            if (t.loudnessRange == 0.0) t.loudnessRange = q.value(7).toDouble();

            if (t.cueIn.isEmpty()) t.cueIn = q.value(8).toString().trimmed();
            if (t.cueOut.isEmpty()) t.cueOut = q.value(9).toString().trimmed();

            if (t.danceability < 0) {
                const double v = q.value(10).toDouble();
                if (v > 0) t.danceability = v;
            }
            if (t.acousticness < 0) {
                const double v = q.value(11).toDouble();
                if (v > 0) t.acousticness = v;
            }
            if (t.instrumentalness < 0) {
                const double v = q.value(12).toDouble();
                if (v > 0) t.instrumentalness = v;
            }
            if (t.liveness < 0) {
                const double v = q.value(13).toDouble();
                if (v > 0) t.liveness = v;
            }

            if (t.year == 0) t.year = q.value(14).toInt();
            if (t.rating == 0) t.rating = q.value(15).toInt();
            if (t.comments.isEmpty()) t.comments = q.value(16).toString().trimmed();

            // Album/Artist/Title from DB if still empty
            if (t.album.isEmpty()) t.album = q.value(17).toString().trimmed();
            if (t.artist.isEmpty()) t.artist = q.value(18).toString().trimmed();
            if (t.title.isEmpty()) t.title = q.value(19).toString().trimmed();
        }

        db.close();
    }
    QSqlDatabase::removeDatabase(connName);

    return result;
}

std::vector<TrackInfo> scanFolderForTracks(const QString& folderPath)
{
    std::vector<TrackInfo> tracks;
    static const QStringList filters = {
        QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"),
        QStringLiteral("*.ogg"), QStringLiteral("*.aac"), QStringLiteral("*.m4a"),
        QStringLiteral("*.wma")
    };

    QDirIterator it(folderPath, filters, QDir::Files, QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        TrackInfo info;
        info.filePath = it.filePath();
        info.fileSize = it.fileInfo().size();
        const QString baseName = it.fileInfo().completeBaseName();

        const int dashPos = baseName.indexOf(QStringLiteral(" - "));
        if (dashPos > 0) {
            info.artist = baseName.left(dashPos).trimmed();
            info.title = baseName.mid(dashPos + 3).trimmed();
            info.displayName = info.artist + QStringLiteral(" \u2014 ") + info.title;
        } else {
            info.title = baseName;
            info.displayName = baseName;
        }
        readId3Tags(info);
        tracks.push_back(std::move(info));
    }
    return tracks;
}

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

    QFile file(kAudioProfilesPath());
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

    QSaveFile saveFile(kAudioProfilesPath());
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
    // Check only the DLLs this binary actually imports (verified via dumpbin).
    // Use debug-suffixed names for debug builds, release names for release builds.
    // Qt6Qml/Qt6Quick are intentionally excluded — native.exe does not import them.
#ifdef _DEBUG
    static const wchar_t* kDllNames[] = {
        L"Qt6Cored.dll",
        L"Qt6Guid.dll",
        L"Qt6Sqld.dll",
        L"Qt6Widgetsd.dll",
        L"vcruntime140d.dll",
        L"msvcp140d.dll"
    };
#else
    static const wchar_t* kDllNames[] = {
        L"Qt6Core.dll",
        L"Qt6Gui.dll",
        L"Qt6Sql.dll",
        L"Qt6Widgets.dll",
        L"vcruntime140.dll",
        L"msvcp140.dll"
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
    if (gExeBaseDir.empty()) gExeBaseDir = resolveExeBaseDir();
    const auto rtDir = std::filesystem::path(gExeBaseDir) / "data" / "runtime";
    std::filesystem::create_directories(rtDir);
    gRuntimeDirReady = std::filesystem::exists(rtDir) && std::filesystem::is_directory(rtDir);
    gLogPath = (rtDir / "ui_qt.log").string();
    gJsonLogPath = (rtDir / "ui_qt.jsonl").string();

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

// ═══════════════════════════════════════════════════════════════════
// VisualizerWidget — lightweight animated display surface
// Supports display modes: None, Bars, Line, Circle
// ═══════════════════════════════════════════════════════════════════
class VisualizerWidget : public QWidget {
public:
    enum class DisplayMode { None, Bars, Line, Circle };

    explicit VisualizerWidget(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        setMinimumHeight(120);
        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        setAttribute(Qt::WA_OpaquePaintEvent, true);
        elapsed_.start();
        audioActiveTimer_.start();
        for (int i = 0; i < kMaxBars; ++i) {
            barHeights_[i] = 0.0f;
            peakHold_[i] = 0.0f;
            peakAge_[i] = 0.0f;
        }
        // Seed particles with initial positions
        for (int i = 0; i < kParticleCount; ++i) {
            particles_[i] = {
                static_cast<float>(i) / kParticleCount,
                0.3f + 0.4f * std::sin(i * 1.618f),
                0.0f,
                0.005f + 0.01f * std::sin(i * 0.73f),
                0.4f + 0.6f * static_cast<float>(i % 7) / 6.0f
            };
        }
    }

    void setDisplayMode(DisplayMode m) { mode_ = m; update(); }
    DisplayMode displayMode() const { return mode_; }

    void setPulseEnabled(bool on) { pulseOn_ = on; update(); }
    bool pulseEnabled() const { return pulseOn_; }

    void setTuneLevel(int level) { tuneLevel_ = qBound(0, level, 4); update(); }
    int tuneLevel() const { return tuneLevel_; }

    void setAudioLevel(float level)
    {
        audioLevel_ = qBound(0.0f, level, 1.0f);
        audioActiveTimer_.restart();
    }

    // Title pulse: paint-pipeline driven, no Qt property changes
    void setTitleText(const QString& text) { titleText_ = text; }
    void setTitlePulse(float envelope) { titlePulse_ = qBound(0.0f, envelope, 1.0f); }
    void setUpNextText(const QString& text) { upNextText_ = text; }

    // Returns the dynamic bar count for the current width
    int barCount() const { return qBound(kMinBars, width() / kSlotPx, kMaxBars); }

    void tick()
    {
        const float dt = 0.033f;
        const float sensitivity = 0.3f + tuneLevel_ * 0.175f;
        const qint64 elapsedMs = elapsed_.elapsed();
        const bool hasAudio = audioActiveTimer_.elapsed() < 500;

        // Use raw audio level directly — DO NOT over-amplify to a constant 1.0.
        // The raw peak (0..1) drives bar height so bars actually follow volume.
        const float rawLevel = hasAudio ? audioLevel_ : 0.0f;
        // Sensitivity-scaled level for overall bar height
        const float level = qBound(0.0f, rawLevel * (1.0f + sensitivity), 1.0f);

        const int n = barCount();
        const float invN = 1.0f / n;

        // ── Update bar heights for dynamic count ──
        for (int i = 0; i < n; ++i) {
            float target;
            if (hasAudio && rawLevel > 0.001f) {
                const float freq = i * invN;
                // Frequency shape: bass bars taller, highs shorter
                const float shape = (1.0f - 0.35f * freq)
                    * (0.85f + 0.15f * std::sin(freq * 6.28318f));
                // Small per-bar jitter for visual spread (NOT the primary driver)
                const float jitter = std::sin(i * 2.71828f + elapsedMs * 0.003f
                    + i * i * 0.37f) * 0.12f;
                const float jitter2 = std::sin(i * 1.414f + elapsedMs * 0.005f) * 0.08f;
                // Audio level IS the primary driver — bars scale with volume
                target = level * shape + (jitter + jitter2) * level;
                target = qBound(0.0f, target, 1.0f);
            } else {
                target = (std::sin(elapsedMs * 0.0008f
                    * (1.0f + i * 0.04f) * sensitivity) + 1.0f)
                    * 0.5f * 0.10f;
            }

            // Live bar motion: fast attack, normal decay
            if (target > barHeights_[i])
                barHeights_[i] += (target - barHeights_[i]) * qMin(1.0f, dt * 30.0f);
            else
                barHeights_[i] += (target - barHeights_[i]) * qMin(1.0f, dt * 3.5f);

            // Peak-hold indicator (separate from bar body)
            if (barHeights_[i] >= peakHold_[i]) {
                peakHold_[i] = barHeights_[i];
                peakAge_[i] = 0.0f;
            } else {
                peakAge_[i] += dt;
                if (peakAge_[i] > 1.0f)
                    peakHold_[i] += (0.0f - peakHold_[i]) * qMin(1.0f, dt * 2.5f);
            }
        }

        // ── Update particles (use dynamic bar count for tip tracking) ──
        for (int i = 0; i < kParticleCount; ++i) {
            auto& pt = particles_[i];
            pt.x += pt.drift * dt * (0.5f + level * 2.0f);
            if (pt.x > 1.0f) pt.x -= 1.0f;
            if (pt.x < 0.0f) pt.x += 1.0f;

            const float targetBright = hasAudio
                ? qBound(0.0f, level * 1.2f + 0.05f * std::sin(elapsedMs * 0.002f + i * 0.5f), 1.0f)
                : 0.08f;
            if (targetBright > pt.brightness)
                pt.brightness += (targetBright - pt.brightness) * qMin(1.0f, dt * 12.0f);
            else
                pt.brightness += (targetBright - pt.brightness) * qMin(1.0f, dt * 2.0f);

            const int barIdx = qBound(0, static_cast<int>(pt.x * n), n - 1);
            const float barTip = 1.0f - barHeights_[barIdx] * 0.75f;
            const float floatRange = 0.10f + 0.05f * std::sin(elapsedMs * 0.0015f + i * 1.3f);
            pt.y += (barTip - floatRange - pt.y) * qMin(1.0f, dt * 4.0f);
        }

        phase_ += dt * 2.0f * sensitivity;
        update();
    }

protected:
    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);

        const int w = width();
        const int h = height();
        const int n = barCount();

        // ── Background: flat fill (cheaper than gradient — widget is behind overlay text) ──
        p.fillRect(rect(), QColor(0x0a, 0x0e, 0x27));

        if (mode_ == DisplayMode::None) return;

        const float pulseScale = pulseOn_
            ? 0.88f + 0.12f * std::sin(elapsed_.elapsed() * 0.004f)
            : 1.0f;

        if (mode_ == DisplayMode::Bars) {
            p.setRenderHint(QPainter::Antialiasing, false);
            p.setPen(Qt::NoPen);
            const float slotW = static_cast<float>(w) / n;
            const float gap = qMax(1.0f, slotW * 0.30f);
            const float barW = qBound(1.0f, slotW - gap, 3.0f);
            const float invN = 1.0f / n;

            // Pre-compute one band color per bar
            QRgb bandCol[kMaxBars];
            for (int i = 0; i < n; ++i)
                bandCol[i] = bandColor(i * invN, barHeights_[i]).rgb();

            // ── Layers 1+2 merged: Main bars + dim reflection in one pass ──
            for (int i = 0; i < n; ++i) {
                const float bh = barHeights_[i] * h * 0.75f * pulseScale;
                if (bh < 1.0f) continue;
                const float x = i * slotW + (slotW - barW) * 0.5f;
                // Main bar
                p.setBrush(QColor(bandCol[i]));
                p.drawRect(QRectF(x, h - bh, barW, bh));
                // Dim reflection below
                const float rh = bh * 0.10f;
                if (rh >= 1.0f) {
                    QColor ref(bandCol[i]);
                    ref.setAlpha(25);
                    p.setBrush(ref);
                    p.drawRect(QRectF(x, h - rh * 0.35f, barW, rh * 0.35f));
                }
            }

            // ── Layer 3: Bar-tip glow caps ──
            {
                p.setPen(Qt::NoPen);
                for (int i = 0; i < n; ++i) {
                    const float bh = barHeights_[i] * h * 0.75f * pulseScale;
                    if (bh < 4.0f) continue;
                    const float x = i * slotW + (slotW - barW) * 0.5f;
                    const float capH = qMin(2.5f, bh * 0.06f);
                    const int alpha = static_cast<int>(100 + barHeights_[i] * 155);
                    QColor glow(bandCol[i]);
                    glow.setAlpha(alpha);
                    p.setBrush(glow);
                    p.drawRect(QRectF(x, h - bh, barW, capH));
                }
            }

            // ── Layer 4: Peak-hold cap markers ──
            {
                p.setPen(Qt::NoPen);
                for (int i = 0; i < n; ++i) {
                    if (peakHold_[i] < 0.02f) continue;
                    const float peakY = h - peakHold_[i] * h * 0.75f * pulseScale;
                    const float x = i * slotW + (slotW - barW) * 0.5f;
                    const float fade = (peakAge_[i] < 1.0f) ? 1.0f
                        : qMax(0.0f, 1.0f - (peakAge_[i] - 1.0f) * 1.5f);
                    const int alpha = static_cast<int>(180 * fade);
                    QColor capCol(bandCol[i]);
                    capCol.setAlpha(alpha);
                    p.setBrush(capCol);
                    p.drawRect(QRectF(x, peakY - 1.5f, barW, 2.0f));
                }
            }

            // ── Layer 5: Sparkle / particle overlay ──
            {
                p.setPen(Qt::NoPen);
                for (int i = 0; i < kParticleCount; ++i) {
                    const auto& pt = particles_[i];
                    if (pt.brightness < 0.02f) continue;
                    const float px = pt.x * w;
                    const float py = pt.y * h;
                    const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
                    const int alpha = static_cast<int>(pt.brightness * 200);
                    const int barIdx = qBound(0, static_cast<int>(pt.x * n), n - 1);
                    QColor c(bandCol[barIdx]);
                    c.setAlpha(alpha);
                    p.setBrush(c);
                    p.drawEllipse(QPointF(px, py), sz, sz);
                }
            }

        } else if (mode_ == DisplayMode::Line) {
            // ════════════════════════════════════════════════════════
            // LINE MODE — same barHeights_[]/peakHold_[] data as Bars
            // 5 layers: glow line, main line, area fill, peak trace, particles
            // ════════════════════════════════════════════════════════
            p.setRenderHint(QPainter::Antialiasing, true);
            const float step = static_cast<float>(w) / (n - 1);
            const float invN = 1.0f / n;

            // Build main waveform path and peak-hold path
            QPainterPath linePath;
            QPainterPath peakPath;
            for (int i = 0; i < n; ++i) {
                const float x = i * step;
                const float y = h * 0.5f - (barHeights_[i] - 0.5f) * h * 0.70f * pulseScale;
                const float peakY = h * 0.5f - (peakHold_[i] - 0.5f) * h * 0.70f * pulseScale;
                if (i == 0) { linePath.moveTo(x, y); peakPath.moveTo(x, peakY); }
                else        { linePath.lineTo(x, y); peakPath.lineTo(x, peakY); }
            }

            // Frequency-band gradient (same palette as Bars)
            QLinearGradient bandGrad(0, 0, w, 0);
            bandGrad.setColorAt(0.00, bandColor(0.00f, 0.70f));
            bandGrad.setColorAt(0.22, bandColor(0.22f, 0.70f));
            bandGrad.setColorAt(0.55, bandColor(0.55f, 0.70f));
            bandGrad.setColorAt(1.00, bandColor(1.00f, 0.70f));

            // Layer 1: Glow line (wide, semi-transparent)
            {
                QLinearGradient glowGrad(0, 0, w, 0);
                QColor c0 = bandColor(0.0f, 0.5f); c0.setAlpha(50);
                QColor c1 = bandColor(0.55f, 0.5f); c1.setAlpha(50);
                QColor c2 = bandColor(1.0f, 0.5f); c2.setAlpha(50);
                glowGrad.setColorAt(0.0, c0);
                glowGrad.setColorAt(0.55, c1);
                glowGrad.setColorAt(1.0, c2);
                p.setPen(QPen(QBrush(glowGrad), 6.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
                p.setBrush(Qt::NoBrush);
                p.drawPath(linePath);
            }

            // Layer 2: Main line with band gradient
            p.setPen(QPen(QBrush(bandGrad), 2.5, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
            p.setBrush(Qt::NoBrush);
            p.drawPath(linePath);

            // Layer 3: Area fill under the line
            {
                QPainterPath fillPath = linePath;
                fillPath.lineTo(w, h);
                fillPath.lineTo(0, h);
                fillPath.closeSubpath();
                QLinearGradient fillGrad(0, 0, w, 0);
                QColor f0 = bandColor(0.0f, 0.4f); f0.setAlpha(35);
                QColor f1 = bandColor(0.55f, 0.4f); f1.setAlpha(35);
                QColor f2 = bandColor(1.0f, 0.4f); f2.setAlpha(35);
                fillGrad.setColorAt(0.0, f0);
                fillGrad.setColorAt(0.55, f1);
                fillGrad.setColorAt(1.0, f2);
                p.setPen(Qt::NoPen);
                p.fillPath(fillPath, QBrush(fillGrad));
            }

            // Layer 4: Peak-hold trace line (thin, bright)
            {
                QLinearGradient peakGrad(0, 0, w, 0);
                QColor k0 = bandColor(0.0f, 0.9f); k0.setAlpha(90);
                QColor k1 = bandColor(0.55f, 0.9f); k1.setAlpha(90);
                QColor k2 = bandColor(1.0f, 0.9f); k2.setAlpha(90);
                peakGrad.setColorAt(0.0, k0);
                peakGrad.setColorAt(0.55, k1);
                peakGrad.setColorAt(1.0, k2);
                p.setPen(QPen(QBrush(peakGrad), 1.0, Qt::SolidLine, Qt::RoundCap));
                p.setBrush(Qt::NoBrush);
                p.drawPath(peakPath);
            }

            // Layer 5: Particles colored by frequency position
            p.setPen(Qt::NoPen);
            for (int i = 0; i < kParticleCount; ++i) {
                const auto& pt = particles_[i];
                if (pt.brightness < 0.03f) continue;
                const float px = pt.x * w;
                const float py = pt.y * h;
                const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
                const int alpha = static_cast<int>(pt.brightness * 180);
                QColor c = bandColor(pt.x, pt.brightness);
                c.setAlpha(alpha);
                p.setBrush(c);
                p.drawEllipse(QPointF(px, py), sz, sz);
            }

        } else if (mode_ == DisplayMode::Circle) {
            // ════════════════════════════════════════════════════════
            // CIRCLE MODE — same barHeights_[]/peakHold_[] data as Bars
            // 5 layers: glow ring, fill, main ring, peak ring, particles
            // ════════════════════════════════════════════════════════
            p.setRenderHint(QPainter::Antialiasing, true);
            const float cx = w * 0.5f;
            const float cy = h * 0.5f;
            const float baseR = qMin(w, h) * 0.25f * pulseScale;
            const float invN = 1.0f / n;
            const float angleStep = 6.28318f / n;
            const float phaseRad = phase_ * 0.5f;
            const float phaseDeg = phaseRad * (180.0f / 3.14159f);

            // Build main polygon and peak-hold polygon
            QPolygonF mainPoly, peakPoly;
            for (int i = 0; i < n; ++i) {
                const float angle = i * angleStep + phaseRad;
                const float cosA = std::cos(angle);
                const float sinA = std::sin(angle);
                const float r  = baseR + barHeights_[i] * baseR * 0.8f;
                const float pr = baseR + peakHold_[i]   * baseR * 0.8f;
                mainPoly << QPointF(cx + r  * cosA, cy + r  * sinA);
                peakPoly << QPointF(cx + pr * cosA, cy + pr * sinA);
            }
            mainPoly << mainPoly.first();
            peakPoly << peakPoly.first();

            // Conical gradient matching frequency band palette
            QConicalGradient cg(cx, cy, phaseDeg);
            cg.setColorAt(0.00, bandColor(0.00f, 0.70f));
            cg.setColorAt(0.22, bandColor(0.22f, 0.70f));
            cg.setColorAt(0.55, bandColor(0.55f, 0.70f));
            cg.setColorAt(1.00, bandColor(1.00f, 0.70f));

            // Layer 1: Glow ring (wide, semi-transparent)
            {
                QConicalGradient glowCg(cx, cy, phaseDeg);
                QColor c0 = bandColor(0.0f, 0.5f); c0.setAlpha(45);
                QColor c1 = bandColor(0.55f, 0.5f); c1.setAlpha(45);
                QColor c2 = bandColor(1.0f, 0.5f); c2.setAlpha(45);
                glowCg.setColorAt(0.0, c0);
                glowCg.setColorAt(0.55, c1);
                glowCg.setColorAt(1.0, c2);
                p.setPen(QPen(QBrush(glowCg), 6.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
                p.setBrush(Qt::NoBrush);
                p.drawPolygon(mainPoly);
            }

            // Layer 2: Inner fill with radial gradient
            {
                QRadialGradient rg(cx, cy, baseR * 2.0f);
                rg.setColorAt(0.0, QColor(0x53, 0x34, 0x83, 40));
                rg.setColorAt(0.6, QColor(0x0f, 0x34, 0x60, 20));
                rg.setColorAt(1.0, QColor(0x0a, 0x0e, 0x27, 5));
                p.setPen(Qt::NoPen);
                p.setBrush(QBrush(rg));
                p.drawPolygon(mainPoly);
            }

            // Layer 3: Main ring with conical band gradient
            p.setPen(QPen(QBrush(cg), 2.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
            p.setBrush(Qt::NoBrush);
            p.drawPolygon(mainPoly);

            // Layer 4: Peak-hold outer ring
            {
                QConicalGradient peakCg(cx, cy, phaseDeg);
                QColor k0 = bandColor(0.0f, 0.8f); k0.setAlpha(65);
                QColor k1 = bandColor(0.55f, 0.8f); k1.setAlpha(65);
                QColor k2 = bandColor(1.0f, 0.8f); k2.setAlpha(65);
                peakCg.setColorAt(0.0, k0);
                peakCg.setColorAt(0.55, k1);
                peakCg.setColorAt(1.0, k2);
                p.setPen(QPen(QBrush(peakCg), 1.0));
                p.setBrush(Qt::NoBrush);
                p.drawPolygon(peakPoly);
            }

            // Layer 5: Particles orbiting outside the ring
            p.setPen(Qt::NoPen);
            for (int i = 0; i < kParticleCount; ++i) {
                const auto& pt = particles_[i];
                if (pt.brightness < 0.03f) continue;
                const float angle = pt.x * 6.28318f + phaseRad;
                const int barIdx = qBound(0, static_cast<int>(pt.x * n), n - 1);
                const float r = baseR + barHeights_[barIdx] * baseR * 0.8f + 10.0f;
                const float px = cx + r * std::cos(angle);
                const float py = cy + r * std::sin(angle);
                const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
                const int alpha = static_cast<int>(pt.brightness * 180);
                QColor c = bandColor(pt.x, pt.brightness);
                c.setAlpha(alpha);
                p.setBrush(c);
                p.drawEllipse(QPointF(px, py), sz, sz);
            }
        }

        // ── Now Playing + Title pulse overlay ──
        if (!titleText_.isEmpty()) {
            p.setRenderHint(QPainter::Antialiasing, true);

            // Pre-measure all fonts to vertically centre the block
            QFont hdrFont(QStringLiteral("Segoe UI"), 9, QFont::Bold);
            hdrFont.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
            QFontMetrics hfm(hdrFont);

            QFont tf(QStringLiteral("Segoe UI"), 18, QFont::Bold);
            QFontMetrics tfm(tf);

            QFont unf(QStringLiteral("Segoe UI"), 11);
            unf.setItalic(true);
            QFontMetrics unfm(unf);

            const bool hasUpNext = !upNextText_.isEmpty();
            // Spacing constants
            const int gapAfterHdr   = 10;  // NOW PLAYING → title
            const int gapAfterTitle = 20;  // title → UP NEXT
            const int gapAfterUpHdr = 6;   // UP NEXT → track name

            // Total block height
            int blockH = hfm.height() + gapAfterHdr + tfm.height();
            if (hasUpNext)
                blockH += gapAfterTitle + hfm.height() + gapAfterUpHdr + unfm.height();

            // Vertically centre the block in the widget
            int y = (height() - blockH) / 2 - 20; // shift up a touch so bars have room
            if (y < 16) y = 16;

            // "NOW PLAYING" header
            p.setFont(hdrFont);
            p.setPen(QColor(233, 69, 96, 200));
            const QString hdr = QStringLiteral("NOW PLAYING");
            const int hw = hfm.horizontalAdvance(hdr);
            p.drawText((width() - hw) / 2, y + hfm.ascent(), hdr);
            y += hfm.height() + gapAfterHdr;

            // Pulsing title
            const float t = qBound(0.0f, titlePulse_, 1.0f);
            const int cr = 255 - static_cast<int>(t * (255 - 233));
            const int cg = 255 - static_cast<int>(t * (255 - 69));
            const int cb = 255 - static_cast<int>(t * (255 - 96));
            const int glowAlpha = static_cast<int>(t * 120);

            p.setFont(tf);
            const int tw = tfm.horizontalAdvance(titleText_);
            const int tx = (width() - tw) / 2;
            const int ty = y + tfm.ascent();

            if (glowAlpha > 2 && t > 0.001f) {
                QColor glow(cr, cg, cb, glowAlpha);
                p.setPen(glow);
                for (auto [dx, dy] : {std::pair{-1,0},{1,0},{0,-1},{0,1},{-2,0},{2,0},{0,-2},{0,2}}) {
                    p.drawText(tx + dx, ty + dy, titleText_);
                }
            }
            p.setPen(QColor(cr, cg, cb, 220 + static_cast<int>(t * 35)));
            p.drawText(tx, ty, titleText_);
            y += tfm.height() + gapAfterTitle;

            // ── Up Next section ──
            if (hasUpNext) {
                // "UP NEXT" header
                p.setFont(hdrFont);
                p.setPen(QColor(233, 69, 96, 140));
                const QString upHdr = QStringLiteral("UP NEXT");
                const int uhw = hfm.horizontalAdvance(upHdr);
                p.drawText((width() - uhw) / 2, y + hfm.ascent(), upHdr);
                y += hfm.height() + gapAfterUpHdr;

                // Track name
                p.setFont(unf);
                p.setPen(QColor(180, 180, 180, 180));
                const int unw = unfm.horizontalAdvance(upNextText_);
                p.drawText((width() - unw) / 2, y + unfm.ascent(), upNextText_);
            }
        }
    }

private:
    // Dynamic bar count: width / kSlotPx, clamped to [kMinBars, kMaxBars]
    static constexpr int kSlotPx   = 3;    // target: 2px bar + 1px gap
    static constexpr int kMinBars  = 120;
    static constexpr int kMaxBars  = 256;
    static constexpr int kParticleCount = 40;

    // Frequency-band color: bass→warm, mids→magenta, highs→violet/blue-white
    // energy (0..1) drives brightness so quiet bands are dimmer
    static QColor bandColor(float freq, float energy) {
        float r, g, b;
        if (freq < 0.22f) {
            // Bass → warm red-orange
            const float s = freq / 0.22f;
            r = 0.96f; g = 0.38f + 0.10f * s; b = 0.18f + 0.08f * s;
        } else if (freq < 0.55f) {
            // Mids → hot pink / magenta
            const float s = (freq - 0.22f) / 0.33f;
            r = 0.92f - 0.05f * s; g = 0.28f + 0.10f * s; b = 0.40f + 0.22f * s;
        } else {
            // Highs → purple to blue-violet with white accent
            const float s = (freq - 0.55f) / 0.45f;
            r = 0.62f - 0.14f * s; g = 0.36f + 0.28f * s; b = 0.74f + 0.14f * s;
        }
        const float bright = 0.35f + 0.65f * qBound(0.0f, energy, 1.0f);
        return QColor(
            qBound(0, static_cast<int>(r * bright * 255), 255),
            qBound(0, static_cast<int>(g * bright * 255), 255),
            qBound(0, static_cast<int>(b * bright * 255), 255));
    }

    struct Particle {
        float x;
        float y;
        float brightness;
        float drift;
        float size;
    };

    DisplayMode mode_{DisplayMode::Bars};
    bool pulseOn_{true};
    int tuneLevel_{2};
    float barHeights_[256]{};
    float peakHold_[256]{};
    float peakAge_[256]{};
    float phase_{0.0f};
    float audioLevel_{0.0f};
    QString titleText_;
    float titlePulse_{0.0f};
    QString upNextText_;
    Particle particles_[40]{};
    QElapsedTimer elapsed_;
    QElapsedTimer audioActiveTimer_;
};

class MainWindow : public QMainWindow {
public:
    explicit MainWindow(EngineBridge& engineBridge)
        : bridge_(engineBridge)
    {
        setWindowTitle(QStringLiteral("NGKsPlayerNative"));
        resize(640, 480);

        auto* root = new QWidget(this);
        auto* rootLayout = new QVBoxLayout(root);
        rootLayout->setContentsMargins(0, 0, 0, 0);
        rootLayout->setSpacing(0);

        // ── Stacked pages ──
        stack_ = new QStackedWidget(root);
        stack_->addWidget(buildSplashPage());    // 0
        stack_->addWidget(buildLandingPage());   // 1
        stack_->addWidget(buildPlayerPage());    // 2
        stack_->addWidget(buildDjModePage());    // 3
        
        QTimer* mt = new QTimer(this);
        connect(mt, &QTimer::timeout, this, [this]() {
            stack_->setCurrentIndex(3);
            stack_->setCurrentIndex(3);
            if (stack_->currentIndex() == 3) {
                QWidget* page = stack_->widget(3);
                QWidget* djLib = page->findChild<QWidget*>("djLibraryWidget");
                  if (!djLib) return;
                int pHeight = page->height();
                int lHeight = djLib ? djLib->height() : 0;
                int tvpH = 0;
                if (djLib) {
                    QTableView* tv = djLib->findChild<QTableView*>();
                    if (tv && tv->viewport()) tvpH = tv->viewport()->height();
                }
                QString metrics = QString("dj_page_height_px=%1\ndj_library_region_height_px=%2\ndj_library_table_viewport_height_px=%3\n").arg(pHeight).arg(lHeight).arg(tvpH);
                QFile f("C:\\Users\\suppo\\Desktop\\NGKsSystems\\NGKsPlayerNative\\data\\runtime\\metrics.txt");
                std::cout << "MEASUREMENTS: " << metrics.toStdString() << "\n";
                std::cout.flush();
                if (f.open(QIODevice::WriteOnly)) { f.write(metrics.toUtf8()); f.close(); }
            }
        });
        mt->start(2000);

        stack_->setCurrentIndex(3);
        rootLayout->addWidget(stack_, 1);

        // ── Persistent status strip ──
        auto* statusStrip = new QWidget(root);
        statusStrip->setStyleSheet(QStringLiteral("background:#222; color:#ccc; font-size:11px;"));
        auto* stripLayout = new QHBoxLayout(statusStrip);
        stripLayout->setContentsMargins(8, 4, 8, 4);
        engineStatusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), statusStrip);
        runningLabel_ = new QLabel(QStringLiteral("Running: NO"), statusStrip);
        meterLabel_ = new QLabel(QStringLiteral("MeterL: 0.000  MeterR: 0.000"), statusStrip);
        stripLayout->addWidget(engineStatusLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(runningLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(meterLabel_);
        stripLayout->addStretch(1);
        rootLayout->addWidget(statusStrip);

        setCentralWidget(root);

        // ── Menu bar ──
        auto* diagnosticsAction = menuBar()->addAction(QStringLiteral("Diagnostics"));
        QObject::connect(diagnosticsAction, &QAction::triggered, this, &MainWindow::showDiagnostics);
        auto* shortcut = new QShortcut(QKeySequence(QStringLiteral("Ctrl+D")), this);
        QObject::connect(shortcut, &QShortcut::activated, this, &MainWindow::showDiagnostics);

        // ── Poll timer ──
        pollTimer_.setInterval(250);
        QObject::connect(&pollTimer_, &QTimer::timeout, this, &MainWindow::pollStatus);
        pollTimer_.start();

        // ── Autorun flags ──
        const QString autorun = qEnvironmentVariable("NGKS_SELFTEST_AUTORUN").trimmed().toLower();
        selfTestAutorun_ = (autorun == QStringLiteral("1") || autorun == QStringLiteral("true") || autorun == QStringLiteral("yes"));
        const QString rtAutorun = qEnvironmentVariable("NGKS_RT_AUDIO_AUTORUN").trimmed().toLower();
        rtProbeAutorun_ = (rtAutorun == QStringLiteral("1") || rtAutorun == QStringLiteral("true") || rtAutorun == QStringLiteral("yes"));

        // ── Restore persisted library ──
        {
            QString restoredFolder;
            std::vector<TrackInfo> restoredTracks;
            if (loadLibraryJson(restoredTracks, restoredFolder)) {
                allTracks_ = std::move(restoredTracks);
                importedFolderPath_ = restoredFolder;

                // Auto-merge legacy DB on restore if not already imported
                bool anyLegacy = false;
                for (const auto& t : allTracks_) {
                    if (t.legacyImported) { anyLegacy = true; break; }
                }
                if (!anyLegacy) {
                    const QString dbPath = findLegacyDbPath();
                    if (!dbPath.isEmpty()) {
                        const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
                        if (res.matched > 0) {
                            saveLibraryJson(allTracks_, importedFolderPath_);
                            qInfo().noquote() << QStringLiteral("LEGACY_DB_AUTO_IMPORT matched=%1 total=%2")
                                .arg(res.matched).arg(res.totalDbRows);
                        }
                    }
                }

                refreshLibraryList();
                rebuildPlayerQueue();
                qInfo().noquote() << QStringLiteral("LIBRARY_RESTORED=%1").arg(allTracks_.size());
            }
        }

        // ── Restore persisted playlists ──
        loadPlaylists(playlists_);
        qInfo().noquote() << QStringLiteral("PLAYLISTS_RESTORED=%1").arg(playlists_.size());

        // ── Splash auto-transition (2 s) ──
        QTimer::singleShot(2000, this, [this]() { stack_->setCurrentIndex(1); });

        qInfo() << "MainWindowConstructed=PASS";

        if (selfTestAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::runFoundationSelfTests);
        }
        if (rtProbeAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::startRtProbeAutorun);
        }
    }

private:
    // ── Page builders ──
    QWidget* buildSplashPage()
    {
        auto* page = new QWidget();
        auto* layout = new QVBoxLayout(page);
        layout->addStretch(2);

        auto* title = new QLabel(QStringLiteral("NGKsPlayerNative"), page);
        {
            QFont f = title->font();
            f.setPointSize(28);
            f.setBold(true);
            title->setFont(f);
        }
        title->setAlignment(Qt::AlignCenter);
        layout->addWidget(title);

        auto* subtitle = new QLabel(QStringLiteral("Audio Engine Platform"), page);
        subtitle->setAlignment(Qt::AlignCenter);
        {
            QFont f = subtitle->font();
            f.setPointSize(12);
            subtitle->setFont(f);
        }
        layout->addWidget(subtitle);

        layout->addStretch(3);
        return page;
    }

    QWidget* buildLandingPage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral(
            "QWidget { background: #1a1a2e; color: #e0e0e0; }"
            "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 6px 14px; font-size: 12px; }"
            "QPushButton:hover { background: #0f3460; }"
            "QPushButton:pressed { background: #533483; }"
            "QLineEdit { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 6px 10px; font-size: 12px; }"
            "QTreeWidget { background: #16213e; alternate-background-color: #1a1a3e;"
            "  color: #e0e0e0; border: 1px solid #0f3460; border-radius: 6px;"
            "  font-size: 12px; outline: none; }"
            "QTreeWidget::item { padding: 4px 6px; border-bottom: 1px solid #0f3460; }"
            "QTreeWidget::item:selected { background: #533483; color: #fff; }"
            "QTreeWidget::item:hover { background: #1f2b4d; }"
            "QHeaderView::section { background: #0f3460; color: #ccc; padding: 5px 8px;"
            "  border: none; border-right: 1px solid #1a1a2e; font-weight: bold; font-size: 11px; }"
            "QLabel#detailTitle { font-size: 15px; font-weight: bold; color: #e94560; }"
            "QLabel#detailField { font-size: 11px; color: #aaa; }"
            "QLabel#detailValue { font-size: 12px; color: #e0e0e0; }"
        ));

        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(16, 12, 16, 8);
        layout->setSpacing(8);

        // ── Header ──
        auto* title = new QLabel(QStringLiteral("Library"), page);
        {
            QFont f = title->font();
            f.setPointSize(16);
            f.setBold(true);
            title->setFont(f);
        }
        title->setStyleSheet(QStringLiteral("color: #e94560; margin-bottom: 2px;"));
        layout->addWidget(title);

        // ── Action row 1: library actions + nav ──
        auto* actionRow = new QHBoxLayout();
        actionRow->setSpacing(6);

        auto* importBtn = new QPushButton(QStringLiteral("Add Folder"), page);
        importBtn->setMinimumHeight(32);
        importBtn->setCursor(Qt::PointingHandCursor);
        importBtn->setToolTip(QStringLiteral("Import a folder of audio files into the library"));
        actionRow->addWidget(importBtn);

        auto* legacyDbBtn = new QPushButton(QStringLiteral("Import Legacy DB"), page);
        legacyDbBtn->setMinimumHeight(32);
        legacyDbBtn->setCursor(Qt::PointingHandCursor);
        legacyDbBtn->setToolTip(QStringLiteral("Merge BPM, key, LUFS and analysis fields from the legacy ngksplayer database"));
        actionRow->addWidget(legacyDbBtn);

        auto* playAllBtn = new QPushButton(QStringLiteral("Play All"), page);
        playAllBtn->setMinimumHeight(32);
        playAllBtn->setCursor(Qt::PointingHandCursor);
        playAllBtn->setToolTip(QStringLiteral("Play all visible tracks sequentially"));
        actionRow->addWidget(playAllBtn);

        auto* nowPlayingBtn = new QPushButton(QStringLiteral("Now Playing"), page);
        nowPlayingBtn->setMinimumHeight(32);
        nowPlayingBtn->setCursor(Qt::PointingHandCursor);
        nowPlayingBtn->setToolTip(QStringLiteral("Jump to the currently playing track"));
        actionRow->addWidget(nowPlayingBtn);

        auto* playlistsBtn = new QPushButton(QStringLiteral("Playlists"), page);
        playlistsBtn->setMinimumHeight(32);
        playlistsBtn->setCursor(Qt::PointingHandCursor);
        playlistsBtn->setToolTip(QStringLiteral("Browse, create, and filter by playlists"));
        actionRow->addWidget(playlistsBtn);

        actionRow->addStretch(1);

        auto* playerBtn = new QPushButton(QStringLiteral("Player"), page);
        auto* djBtn = new QPushButton(QStringLiteral("DJ Mode"), page);
        playerBtn->setMinimumHeight(32);
        djBtn->setMinimumHeight(32);
        playerBtn->setCursor(Qt::PointingHandCursor);
        djBtn->setCursor(Qt::PointingHandCursor);
        actionRow->addWidget(playerBtn);
        actionRow->addWidget(djBtn);
        layout->addLayout(actionRow);

        // ── Action row 2: tools (some wired, some coming-soon) ──
        auto* toolRow = new QHBoxLayout();
        toolRow->setSpacing(6);

        auto* tagEditorBtn = new QPushButton(QStringLiteral("Tag Editor"), page);
        tagEditorBtn->setMinimumHeight(30);
        tagEditorBtn->setEnabled(false);
        tagEditorBtn->setToolTip(QStringLiteral("Edit track tags (coming soon)"));
        toolRow->addWidget(tagEditorBtn);

        auto* settingsBtn = new QPushButton(QStringLiteral("Settings"), page);
        settingsBtn->setMinimumHeight(30);
        settingsBtn->setEnabled(false);
        settingsBtn->setToolTip(QStringLiteral("Application settings (coming soon)"));
        toolRow->addWidget(settingsBtn);

        auto* normalizeBtn = new QPushButton(QStringLiteral("Normalize"), page);
        normalizeBtn->setMinimumHeight(30);
        normalizeBtn->setEnabled(false);
        normalizeBtn->setToolTip(QStringLiteral("Normalize loudness across tracks (coming soon)"));
        toolRow->addWidget(normalizeBtn);

        auto* layerRemoverBtn = new QPushButton(QStringLiteral("Layer Remover"), page);
        layerRemoverBtn->setMinimumHeight(30);
        layerRemoverBtn->setEnabled(false);
        layerRemoverBtn->setToolTip(QStringLiteral("AI stem separation (coming soon)"));
        toolRow->addWidget(layerRemoverBtn);

        auto* clipperBtn = new QPushButton(QStringLiteral("Clipper V3"), page);
        clipperBtn->setMinimumHeight(30);
        clipperBtn->setEnabled(false);
        clipperBtn->setToolTip(QStringLiteral("Soft-clip mastering (coming soon)"));
        toolRow->addWidget(clipperBtn);

        toolRow->addStretch(1);
        layout->addLayout(toolRow);

        // ── Search row: mode selector + search bar + sort ── (directly above song list)
        auto* searchRow = new QHBoxLayout();
        searchRow->setSpacing(6);

        searchModeCombo_ = new QComboBox(page);
        searchModeCombo_->addItem(QStringLiteral("File Name"),  0);
        searchModeCombo_->addItem(QStringLiteral("Artist"),     1);
        searchModeCombo_->addItem(QStringLiteral("Album"),      2);
        searchModeCombo_->addItem(QStringLiteral("BPM"),        3);
        searchModeCombo_->addItem(QStringLiteral("Length"),      4);
        searchModeCombo_->addItem(QStringLiteral("All Fields"), 5);
        searchModeCombo_->setMinimumHeight(34);
        searchModeCombo_->setMinimumWidth(110);
        searchModeCombo_->setStyleSheet(QStringLiteral(
            "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 4px 10px; font-size: 12px; }"
            "QComboBox::drop-down { border: none; }"
            "QComboBox QAbstractItemView { background: #16213e; color: #e0e0e0;"
            "  selection-background-color: #533483; }"
        ));
        searchRow->addWidget(searchModeCombo_);

        searchBar_ = new QLineEdit(page);
        searchBar_->setPlaceholderText(QStringLiteral("Search by file name..."));
        searchBar_->setClearButtonEnabled(true);
        searchBar_->setMinimumHeight(34);
        searchRow->addWidget(searchBar_, 1);

        sortCombo_ = new QComboBox(page);
        sortCombo_->addItem(QStringLiteral("Sort: Title"),      0);
        sortCombo_->addItem(QStringLiteral("Sort: Artist"),     1);
        sortCombo_->addItem(QStringLiteral("Sort: Album"),      2);
        sortCombo_->addItem(QStringLiteral("Sort: Duration"),   3);
        sortCombo_->addItem(QStringLiteral("Sort: BPM"),        4);
        sortCombo_->addItem(QStringLiteral("Sort: Key"),        5);
        sortCombo_->setMinimumHeight(34);
        sortCombo_->setStyleSheet(QStringLiteral(
            "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 4px 10px; font-size: 12px; }"
            "QComboBox::drop-down { border: none; }"
            "QComboBox QAbstractItemView { background: #16213e; color: #e0e0e0;"
            "  selection-background-color: #533483; }"
        ));
        searchRow->addWidget(sortCombo_);
        layout->addLayout(searchRow);

        // ── Main content: splitter (tree | detail panel) ──
        auto* splitter = new QSplitter(Qt::Horizontal, page);
        splitter->setHandleWidth(2);
        splitter->setStyleSheet(QStringLiteral("QSplitter::handle { background: #0f3460; }"));

        // ── Track tree widget ──
        libraryTree_ = new QTreeWidget(splitter);
        libraryTree_->setAlternatingRowColors(true);
        libraryTree_->setRootIsDecorated(false);
        libraryTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        libraryTree_->setHeaderLabels({
            QStringLiteral("Name"), QStringLiteral("Artist"),
            QStringLiteral("Album"), QStringLiteral("Duration"),
            QStringLiteral("BPM"), QStringLiteral("Key")
        });
        libraryTree_->header()->setStretchLastSection(false);
        libraryTree_->header()->setSectionResizeMode(QHeaderView::Interactive);
        libraryTree_->header()->setMinimumSectionSize(40);
        libraryTree_->header()->resizeSection(0, 340);
        libraryTree_->header()->resizeSection(1, 140);
        libraryTree_->header()->resizeSection(2, 140);
        libraryTree_->header()->resizeSection(3, 70);
        libraryTree_->header()->resizeSection(4, 55);
        libraryTree_->header()->resizeSection(5, 50);
        libraryTree_->setSortingEnabled(false);

        // Empty state placeholder
        {
            auto* emptyItem = new QTreeWidgetItem(libraryTree_);
            emptyItem->setText(0, QStringLiteral("Click \"Import Folder\" to load music"));
            emptyItem->setFlags(Qt::NoItemFlags);
        }

        // ── Detail panel (scrollable) ──
        auto* detailScroll = new QScrollArea(splitter);
        detailScroll->setWidgetResizable(true);
        detailScroll->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
        detailScroll->setMinimumWidth(200);
        detailScroll->setMaximumWidth(280);
        detailScroll->setStyleSheet(QStringLiteral(
            "QScrollArea { background: #16213e; border-left: 1px solid #0f3460;"
            "  border-radius: 6px; }"
            "QScrollBar:vertical { background: #16213e; width: 6px; }"
            "QScrollBar::handle:vertical { background: #533483; border-radius: 3px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"
        ));
        auto* detailPanel = new QWidget();
        detailPanel->setStyleSheet(QStringLiteral("background: transparent;"));
        auto* detailLayout = new QVBoxLayout(detailPanel);
        detailLayout->setContentsMargins(12, 12, 12, 12);
        detailLayout->setSpacing(6);

        detailTitleLabel_ = new QLabel(QStringLiteral("Track Info"), detailPanel);
        detailTitleLabel_->setObjectName(QStringLiteral("detailTitle"));
        detailLayout->addWidget(detailTitleLabel_);

        auto addDetailRow = [&](const QString& label, QLabel*& valueLabel) {
            auto* fieldLabel = new QLabel(label, detailPanel);
            fieldLabel->setObjectName(QStringLiteral("detailField"));
            detailLayout->addWidget(fieldLabel);
            valueLabel = new QLabel(QStringLiteral("-"), detailPanel);
            valueLabel->setObjectName(QStringLiteral("detailValue"));
            valueLabel->setWordWrap(true);
            detailLayout->addWidget(valueLabel);
        };

        addDetailRow(QStringLiteral("TITLE"), detailTrackTitle_);
        addDetailRow(QStringLiteral("ARTIST"), detailTrackArtist_);
        addDetailRow(QStringLiteral("ALBUM"), detailTrackAlbum_);
        addDetailRow(QStringLiteral("GENRE"), detailTrackGenre_);
        addDetailRow(QStringLiteral("DURATION"), detailTrackDuration_);
        addDetailRow(QStringLiteral("BPM"), detailTrackBpm_);
        addDetailRow(QStringLiteral("KEY"), detailTrackKey_);
        addDetailRow(QStringLiteral("CAMELOT"), detailTrackCamelot_);
        addDetailRow(QStringLiteral("ENERGY"), detailTrackEnergy_);
        addDetailRow(QStringLiteral("LUFS"), detailTrackLufs_);
        addDetailRow(QStringLiteral("CUE IN/OUT"), detailTrackCue_);
        addDetailRow(QStringLiteral("DANCEABILITY"), detailTrackDance_);
        addDetailRow(QStringLiteral("FILE SIZE"), detailTrackSize_);
        addDetailRow(QStringLiteral("FILE PATH"), detailTrackPath_);

        detailLayout->addStretch(1);
        detailScroll->setWidget(detailPanel);
        splitter->addWidget(libraryTree_);
        splitter->addWidget(detailScroll);
        splitter->setSizes({500, 240});
        layout->addWidget(splitter, 1);

        // ── Bottom bar: track count ──
        auto* bottomRow = new QHBoxLayout();
        bottomRow->setSpacing(8);
        trackCountLabel_ = new QLabel(QStringLiteral("0 tracks"), page);
        trackCountLabel_->setStyleSheet(QStringLiteral("color: #888; font-size: 11px;"));
        bottomRow->addWidget(trackCountLabel_);
        bottomRow->addStretch(1);
        layout->addLayout(bottomRow);

        // ── Connections ──

        // Import folder
        QObject::connect(importBtn, &QPushButton::clicked, this, [this]() {
            const QString dir = QFileDialog::getExistingDirectory(this, QStringLiteral("Select Music Folder"));
            if (dir.isEmpty()) return;
            qInfo().noquote() << QStringLiteral("LIBRARY_SCAN_STARTED=%1").arg(dir);

            allTracks_ = scanFolderForTracks(dir);
            importedFolderPath_ = dir;
            qInfo().noquote() << QStringLiteral("FILES_FOUND=%1").arg(allTracks_.size());
            qInfo().noquote() << QStringLiteral("TRACKS_INDEXED=%1").arg(allTracks_.size());

            // Clear detail panel before rebuild
            clearTrackDetail();
            refreshLibraryList();

            // Save library to disk (metadata already extracted during scan)
            saveLibraryJson(allTracks_, importedFolderPath_);
            qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_SCAN");
        });

        // Import legacy DB
        QObject::connect(legacyDbBtn, &QPushButton::clicked, this, [this]() {
            if (allTracks_.empty()) {
                QMessageBox::information(this, QStringLiteral("Import Legacy DB"),
                    QStringLiteral("Import a music folder first, then merge legacy analysis data."));
                return;
            }
            const QString dbPath = findLegacyDbPath();
            if (dbPath.isEmpty()) {
                QMessageBox::warning(this, QStringLiteral("Legacy DB Not Found"),
                    QStringLiteral("Could not locate library.db in AppData/Roaming."));
                return;
            }
            qInfo().noquote() << QStringLiteral("LEGACY_DB_IMPORT_STARTED=%1").arg(dbPath);
            const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
            qInfo().noquote() << QStringLiteral("LEGACY_DB_IMPORT_DONE matched=%1 unmatched=%2 total=%3")
                .arg(res.matched).arg(res.unmatched).arg(res.totalDbRows);

            refreshLibraryList();
            saveLibraryJson(allTracks_, importedFolderPath_);
            qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_LEGACY_IMPORT");

            QMessageBox::information(this, QStringLiteral("Legacy DB Imported"),
                QStringLiteral("Matched %1 of %2 DB tracks.\n%3 unmatched.")
                    .arg(res.matched).arg(res.totalDbRows).arg(res.unmatched));
        });

        // Live search
        QObject::connect(searchBar_, &QLineEdit::textChanged, this, [this](const QString& text) {
            searchQuery_ = text;
            qInfo().noquote() << QStringLiteral("SEARCH_QUERY=%1 MODE=%2").arg(text).arg(
                searchModeCombo_ ? searchModeCombo_->currentText() : QStringLiteral("?"));
            refreshLibraryList();
        });

        // Search mode selector
        QObject::connect(searchModeCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
            static const QString placeholders[] = {
                QStringLiteral("Search by file name..."),
                QStringLiteral("Search by artist..."),
                QStringLiteral("Search by album..."),
                QStringLiteral("Search by BPM (e.g. 120 or 120-130)..."),
                QStringLiteral("Search by length (e.g. 3:00 or 3:00-5:00)..."),
                QStringLiteral("Search all fields..."),
            };
            const int mode = searchModeCombo_->currentData().toInt();
            if (mode >= 0 && mode <= 5) searchBar_->setPlaceholderText(placeholders[mode]);
            qInfo().noquote() << QStringLiteral("SEARCH_MODE=%1").arg(searchModeCombo_->currentText());
            if (!searchQuery_.isEmpty()) refreshLibraryList();
        });

        // Sort combo
        QObject::connect(sortCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
            refreshLibraryList();
        });

        // Single-click → show detail
        QObject::connect(libraryTree_, &QTreeWidget::currentItemChanged, this,
            [this](QTreeWidgetItem* current, QTreeWidgetItem*) {
            if (!current) return;
            const int trackIdx = current->data(0, Qt::UserRole).toInt();
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            showTrackDetail(trackIdx);
        });

        // Double-click track → play
        QObject::connect(libraryTree_, &QTreeWidget::itemDoubleClicked, this,
            [this](QTreeWidgetItem* item, int) {
            const int trackIdx = item->data(0, Qt::UserRole).toInt();
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            currentTrackIndex_ = trackIdx;
            qInfo().noquote() << QStringLiteral("TRACK_SELECTED=%1").arg(allTracks_[trackIdx].displayName);
            rebuildPlayerQueue();
            loadAndPlayTrack(trackIdx);
            qInfo().noquote() << QStringLiteral("PLAYER_OPENED=TRUE");
            stack_->setCurrentIndex(2);
        });

        // ── Right-click context menu on library tree ──
        libraryTree_->setContextMenuPolicy(Qt::CustomContextMenu);
        QObject::connect(libraryTree_, &QTreeWidget::customContextMenuRequested, this,
            [this](const QPoint& pos) {
            auto* item = libraryTree_->itemAt(pos);
            if (!item) return;
            const int trackIdx = item->data(0, Qt::UserRole).toInt();
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            // Ensure this item is selected so detail panel updates
            libraryTree_->setCurrentItem(item);
            const TrackInfo& t = allTracks_[trackIdx];

            QMenu menu(libraryTree_);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
                "  padding: 4px 0; }"
                "QMenu::item { padding: 6px 24px; }"
                "QMenu::item:selected { background: #533483; }"
                "QMenu::item:disabled { color: #666; }"
                "QMenu::separator { height: 1px; background: #0f3460; margin: 4px 8px; }"
            ));

            // ─ Playback actions ─
            auto* playAction = menu.addAction(QStringLiteral("Play"));
            auto* openInPlayerAction = menu.addAction(QStringLiteral("Open in Player"));
            menu.addSeparator();
            auto* playNextAction = menu.addAction(QStringLiteral("Play Next"));
            playNextAction->setEnabled(false);
            playNextAction->setToolTip(QStringLiteral("Queue system coming soon"));
            auto* addToQueueAction = menu.addAction(QStringLiteral("Add to Queue"));
            addToQueueAction->setEnabled(false);
            addToQueueAction->setToolTip(QStringLiteral("Queue system coming soon"));
            menu.addSeparator();

            // ─ Info & edit ─
            auto* trackInfoAction = menu.addAction(QStringLiteral("Track Info"));
            auto* editTagsAction = menu.addAction(QStringLiteral("Edit Tags"));
            editTagsAction->setEnabled(false);
            editTagsAction->setToolTip(QStringLiteral("Tag editor coming soon"));
            menu.addSeparator();

            // ─ Analysis ─
            auto* analyzeAction = menu.addAction(QStringLiteral("Analyze Track"));
            analyzeAction->setEnabled(false);
            analyzeAction->setToolTip(QStringLiteral("Analysis pipeline coming soon"));
            auto* refreshMetaAction = menu.addAction(QStringLiteral("Refresh Metadata"));
            menu.addSeparator();

            // ─ Playlist ─
            auto* playlistMenu = menu.addMenu(QStringLiteral("Add to Playlist"));
            playlistMenu->setStyleSheet(menu.styleSheet());
            std::vector<QAction*> playlistActions;
            for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                auto* a = playlistMenu->addAction(playlists_[pi].name);
                a->setData(static_cast<int>(pi));
                playlistActions.push_back(a);
            }
            if (!playlists_.empty()) playlistMenu->addSeparator();
            auto* newPlaylistAction = playlistMenu->addAction(QStringLiteral("New Playlist..."));
            menu.addSeparator();

            // ─ File operations ─
            auto* showInFolderAction = menu.addAction(QStringLiteral("Show in Folder"));
            auto* copyPathAction = menu.addAction(QStringLiteral("Copy File Path"));
            menu.addSeparator();

            // ─ Destructive ─
            auto* removeAction = menu.addAction(QStringLiteral("Remove from Library"));
            auto* deleteFromDiskAction = menu.addAction(QStringLiteral("Delete from Disk..."));

            // ── Execute selected action ──
            auto* chosen = menu.exec(libraryTree_->viewport()->mapToGlobal(pos));
            if (!chosen) return;

            if (chosen == playAction || chosen == openInPlayerAction) {
                currentTrackIndex_ = trackIdx;
                rebuildPlayerQueue();
                loadAndPlayTrack(trackIdx);
                qInfo().noquote() << QStringLiteral("CTX_PLAY=%1").arg(t.displayName);
                stack_->setCurrentIndex(2);
            } else if (chosen == trackInfoAction) {
                showTrackDetail(trackIdx);
                qInfo().noquote() << QStringLiteral("CTX_TRACK_INFO=%1").arg(t.displayName);
            } else if (chosen == refreshMetaAction) {
                readId3Tags(allTracks_[trackIdx]);
                updateTreeItemForTrack(trackIdx);
                saveLibraryJson(allTracks_, importedFolderPath_);
                qInfo().noquote() << QStringLiteral("CTX_REFRESH_META=%1").arg(allTracks_[trackIdx].displayName);
            } else if (chosen == showInFolderAction) {
                const QFileInfo fi(t.filePath);
                QDesktopServices::openUrl(QUrl::fromLocalFile(fi.absolutePath()));
                qInfo().noquote() << QStringLiteral("CTX_SHOW_FOLDER=%1").arg(fi.absolutePath());
            } else if (chosen == copyPathAction) {
                QGuiApplication::clipboard()->setText(t.filePath);
                qInfo().noquote() << QStringLiteral("CTX_COPY_PATH=%1").arg(t.filePath);
            } else if (chosen == removeAction) {
                qInfo().noquote() << QStringLiteral("CTX_REMOVE=%1").arg(t.displayName);
                allTracks_.erase(allTracks_.begin() + trackIdx);
                saveLibraryJson(allTracks_, importedFolderPath_);
                refreshLibraryList();
            } else if (chosen == deleteFromDiskAction) {
                // Warning dialog before permanent file deletion
                QMessageBox warning(libraryTree_);
                warning.setWindowTitle(QStringLiteral("Delete from Disk"));
                warning.setIcon(QMessageBox::Warning);
                warning.setText(QStringLiteral("Permanently delete this file?"));
                warning.setInformativeText(
                    QStringLiteral("Track: %1\nPath: %2\n\nThis cannot be undone.")
                        .arg(t.displayName, t.filePath));
                warning.setStandardButtons(QMessageBox::Cancel);
                auto* deleteBtn = warning.addButton(QStringLiteral("Delete"), QMessageBox::DestructiveRole);
                warning.setDefaultButton(QMessageBox::Cancel);
                warning.setStyleSheet(QStringLiteral(
                    "QMessageBox { background: #1a1a2e; color: #e0e0e0; }"
                    "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
                    "  border-radius: 4px; padding: 6px 16px; }"
                    "QPushButton:hover { background: #533483; }"
                ));
                warning.exec();
                if (warning.clickedButton() == deleteBtn) {
                    const QString path = t.filePath;
                    const QString name = t.displayName;
                    if (QFile::remove(path)) {
                        qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_OK=%1 PATH=%2").arg(name, path);
                        allTracks_.erase(allTracks_.begin() + trackIdx);
                        saveLibraryJson(allTracks_, importedFolderPath_);
                        refreshLibraryList();
                    } else {
                        qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_FAIL=%1 PATH=%2").arg(name, path);
                        QMessageBox::critical(libraryTree_, QStringLiteral("Delete Failed"),
                            QStringLiteral("Could not delete:\n%1\n\nThe file may be in use or read-only.").arg(path));
                    }
                } else {
                    qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_CANCELLED=%1").arg(t.displayName);
                }
            } else if (chosen == newPlaylistAction) {
                bool ok = false;
                const QString name = QInputDialog::getText(libraryTree_,
                    QStringLiteral("New Playlist"),
                    QStringLiteral("Playlist name:"),
                    QLineEdit::Normal, QString(), &ok);
                if (ok && !name.trimmed().isEmpty()) {
                    Playlist pl;
                    pl.name = name.trimmed();
                    pl.trackPaths.append(t.filePath);
                    playlists_.push_back(std::move(pl));
                    savePlaylists(playlists_);
                    qInfo().noquote() << QStringLiteral("CTX_NEW_PLAYLIST=%1 TRACK=%2").arg(name.trimmed(), t.displayName);
                }
            } else {
                // Check if an existing playlist was selected
                for (auto* pa : playlistActions) {
                    if (chosen == pa) {
                        const int pi = pa->data().toInt();
                        if (pi >= 0 && pi < static_cast<int>(playlists_.size())) {
                            playlists_[pi].trackPaths.append(t.filePath);
                            savePlaylists(playlists_);
                            qInfo().noquote() << QStringLiteral("CTX_ADD_TO_PLAYLIST=%1 TRACK=%2")
                                .arg(playlists_[pi].name, t.displayName);
                        }
                        break;
                    }
                }
            }
        });

        // ── Play All button ──
        QObject::connect(playAllBtn, &QPushButton::clicked, this, [this]() {
            if (allTracks_.empty()) return;
            // Play first visible track (or first track if no filter)
            if (libraryTree_->topLevelItemCount() > 0) {
                auto* first = libraryTree_->topLevelItem(0);
                const int idx = first->data(0, Qt::UserRole).toInt();
                if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
                    currentTrackIndex_ = idx;
                    rebuildPlayerQueue();
                    loadAndPlayTrack(idx);
                    qInfo().noquote() << QStringLiteral("PLAY_ALL_START=%1").arg(allTracks_[idx].displayName);
                    stack_->setCurrentIndex(2);
                }
            }
        });

        // ── Now Playing button — scroll to current track ──
        QObject::connect(nowPlayingBtn, &QPushButton::clicked, this, [this]() {
            if (currentTrackIndex_ < 0 || currentTrackIndex_ >= static_cast<int>(allTracks_.size())) return;
            for (int i = 0; i < libraryTree_->topLevelItemCount(); ++i) {
                if (libraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt() == currentTrackIndex_) {
                    libraryTree_->setCurrentItem(libraryTree_->topLevelItem(i));
                    libraryTree_->scrollToItem(libraryTree_->topLevelItem(i));
                    qInfo().noquote() << QStringLiteral("NOW_PLAYING_SCROLL=%1").arg(allTracks_[currentTrackIndex_].displayName);
                    break;
                }
            }
        });

        // ── Playlists button — popup menu to browse / filter by playlist ──
        QObject::connect(playlistsBtn, &QPushButton::clicked, this, [this, playlistsBtn]() {
            QMenu menu(playlistsBtn);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; }"
                "QMenu::item:selected { background: #533483; }"));

            // "Show All Library" — clears playlist filter
            auto* showAllAction = menu.addAction(QStringLiteral("Show All Library"));
            showAllAction->setEnabled(activePlaylistIndex_ >= 0);

            menu.addSeparator();

            // List existing playlists
            std::vector<QAction*> plActions;
            for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                const QString label = QStringLiteral("%1 (%2)")
                    .arg(playlists_[pi].name)
                    .arg(playlists_[pi].trackPaths.size());
                auto* a = menu.addAction(label);
                a->setCheckable(true);
                a->setChecked(static_cast<int>(pi) == activePlaylistIndex_);
                a->setData(static_cast<int>(pi));
                plActions.push_back(a);
            }

            if (!playlists_.empty()) menu.addSeparator();

            auto* newPlAction = menu.addAction(QStringLiteral("New Playlist..."));
            QAction* deletePlAction = nullptr;
            if (!playlists_.empty()) {
                deletePlAction = menu.addAction(QStringLiteral("Delete Playlist..."));
            }

            auto* chosen = menu.exec(playlistsBtn->mapToGlobal(
                QPoint(0, playlistsBtn->height())));
            if (!chosen) return;

            if (chosen == showAllAction) {
                activePlaylistIndex_ = -1;
                playlistsBtn->setText(QStringLiteral("Playlists"));
                refreshLibraryList();
                qInfo().noquote() << QStringLiteral("PLAYLIST_FILTER=ALL");
            } else if (chosen == newPlAction) {
                bool ok = false;
                const QString name = QInputDialog::getText(playlistsBtn,
                    QStringLiteral("New Playlist"),
                    QStringLiteral("Playlist name:"),
                    QLineEdit::Normal, QString(), &ok);
                if (ok && !name.trimmed().isEmpty()) {
                    Playlist pl;
                    pl.name = name.trimmed();
                    playlists_.push_back(std::move(pl));
                    savePlaylists(playlists_);
                    qInfo().noquote() << QStringLiteral("PLAYLIST_CREATED=%1").arg(name.trimmed());
                }
            } else if (chosen == deletePlAction) {
                // Show a second menu to pick which playlist to delete
                QMenu delMenu(playlistsBtn);
                delMenu.setStyleSheet(menu.styleSheet());
                std::vector<QAction*> delActions;
                for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                    auto* a = delMenu.addAction(playlists_[pi].name);
                    a->setData(static_cast<int>(pi));
                    delActions.push_back(a);
                }
                auto* delChosen = delMenu.exec(QCursor::pos());
                if (delChosen) {
                    const int di = delChosen->data().toInt();
                    if (di >= 0 && di < static_cast<int>(playlists_.size())) {
                        const QString deletedName = playlists_[di].name;
                        playlists_.erase(playlists_.begin() + di);
                        savePlaylists(playlists_);
                        if (activePlaylistIndex_ == di) {
                            activePlaylistIndex_ = -1;
                            playlistsBtn->setText(QStringLiteral("Playlists"));
                            refreshLibraryList();
                        } else if (activePlaylistIndex_ > di) {
                            --activePlaylistIndex_;
                        }
                        qInfo().noquote() << QStringLiteral("PLAYLIST_DELETED=%1").arg(deletedName);
                    }
                }
            } else {
                // Check if an existing playlist was selected to filter
                for (auto* pa : plActions) {
                    if (chosen == pa) {
                        const int pi = pa->data().toInt();
                        if (pi >= 0 && pi < static_cast<int>(playlists_.size())) {
                            activePlaylistIndex_ = pi;
                            playlistsBtn->setText(QStringLiteral("Playlists: %1").arg(playlists_[pi].name));
                            refreshLibraryList();
                            qInfo().noquote() << QStringLiteral("PLAYLIST_FILTER=%1").arg(playlists_[pi].name);
                        }
                        break;
                    }
                }
            }
        });

        // Nav buttons
        QObject::connect(playerBtn, &QPushButton::clicked, this, [this]() {
            rebuildPlayerQueue();
            stack_->setCurrentIndex(2);
        });
        QObject::connect(djBtn, &QPushButton::clicked, this, [this]() {
            bridge_.enterDjMode();
            populateDjLibraryTrees();
            stack_->setCurrentIndex(3);
        });

        return page;
    }

    QWidget* buildPlayerPage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral(
            "QWidget { background: #0a0e27; color: #e0e0e0; }"
            "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 8px 16px; font-size: 13px; }"
            "QPushButton:hover { background: #1a1a2e; border-color: #533483; }"
            "QPushButton:pressed { background: #533483; }"
            "QPushButton:disabled { background: #0d1117; color: #555; border-color: #1a1a2e; }"
            "QSlider::groove:horizontal { background: #1a1a2e; height: 8px; border-radius: 4px; }"
            "QSlider::handle:horizontal { background: #e94560; width: 16px; height: 16px;"
            "  margin: -4px 0; border-radius: 8px; }"
            "QSlider::sub-page:horizontal { background: #e94560; border-radius: 4px; min-width: 0px; }"
            "QListWidget { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 8px; outline: none; }"
            "QListWidget::item { padding: 8px 12px; border-bottom: 1px solid #0f3460; }"
            "QListWidget::item:selected { background: #533483; color: #ffffff; }"
            "QListWidget::item:hover { background: #1a1a2e; }"
            "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 4px; padding: 4px 8px; }"
            "QScrollBar:vertical { background: #0a0e27; width: 8px; }"
            "QScrollBar::handle:vertical { background: #533483; border-radius: 4px; min-height: 20px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"
        ));

        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(24, 16, 24, 16);
        layout->setSpacing(0);

        // ═══════════════════════════════════════════════════
        // A. Header row: Back + Title + Audio Profile
        // ═══════════════════════════════════════════════════
        auto* headerRow = new QHBoxLayout();
        headerRow->setSpacing(10);
        auto* backBtn = new QPushButton(QStringLiteral("<< Library"), page);
        backBtn->setMinimumHeight(34);
        backBtn->setCursor(Qt::PointingHandCursor);
        backBtn->setToolTip(QStringLiteral("Return to the library browser"));

        auto* titleLabel = new QLabel(QStringLiteral("Simple Player"), page);
        {
            QFont f = titleLabel->font();
            f.setPointSize(16);
            f.setBold(true);
            titleLabel->setFont(f);
            titleLabel->setStyleSheet(QStringLiteral("color: #e94560;"));
        }
        headerRow->addWidget(backBtn);
        headerRow->addSpacing(8);
        headerRow->addWidget(titleLabel);
        headerRow->addStretch(1);

        auto* profileLabel = new QLabel(QStringLiteral("Profile:"), page);
        profileLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
        audioProfileCombo_ = new QComboBox(page);
        audioProfileCombo_->setMinimumWidth(180);
        refreshAudioProfilesButton_ = new QPushButton(QStringLiteral("Refresh"), page);
        applyAudioProfileButton_ = new QPushButton(QStringLiteral("Apply"), page);
        headerRow->addWidget(profileLabel);
        headerRow->addWidget(audioProfileCombo_);
        headerRow->addWidget(refreshAudioProfilesButton_);
        headerRow->addWidget(applyAudioProfileButton_);
        layout->addLayout(headerRow);

        QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
            bridge_.leaveSimpleMode();
            stack_->setCurrentIndex(1);
        });
        QObject::connect(refreshAudioProfilesButton_, &QPushButton::clicked, this, [this]() {
            requestAudioProfilesRefresh(true);
        });
        QObject::connect(applyAudioProfileButton_, &QPushButton::clicked, this, &MainWindow::applySelectedAudioProfile);
        requestAudioProfilesRefresh(true);

        const QString akApplyAutorun = qEnvironmentVariable("NGKS_AK_AUTORUN_APPLY").trimmed().toLower();
        if (akApplyAutorun == QStringLiteral("1") || akApplyAutorun == QStringLiteral("true") || akApplyAutorun == QStringLiteral("yes")) {
            QTimer::singleShot(200, this, &MainWindow::applySelectedAudioProfile);
        }

        layout->addSpacing(14);

        // ═══════════════════════════════════════════════════
        // B. Hero / Now Playing panel with Visualizer
        //    Visualizer is the BACKGROUND layer; text overlays on top
        // ═══════════════════════════════════════════════════
        auto* heroFrame = new QFrame(page);
        heroFrame->setStyleSheet(QStringLiteral(
            "QFrame#heroFrame { background: #0a0e27; border: 1px solid #0f3460; border-radius: 12px; }"));
        heroFrame->setObjectName(QStringLiteral("heroFrame"));
        heroFrame->setMinimumHeight(220);

        // QStackedLayout::StackAll shows all children simultaneously, stacked
        auto* heroStack = new QStackedLayout(heroFrame);
        heroStack->setStackingMode(QStackedLayout::StackAll);
        heroStack->setContentsMargins(0, 0, 0, 0);

        // B1. BACKGROUND (index 0): Visualizer fills the entire hero frame
        visualizer_ = new VisualizerWidget(heroFrame);
        visualizer_->setMinimumHeight(220);
        heroStack->addWidget(visualizer_);

        // B2. FOREGROUND: Transparent overlay with text + controls
        auto* foreground = new QWidget(heroFrame);
        foreground->setStyleSheet(QStringLiteral("background: transparent;"));
        foreground->setAttribute(Qt::WA_TransparentForMouseEvents, false);
        auto* fgLayout = new QVBoxLayout(foreground);
        fgLayout->setContentsMargins(28, 16, 28, 0);
        fgLayout->setSpacing(4);

        nowPlayingTag_ = new QLabel(QStringLiteral("NOW PLAYING"), foreground);
        {
            QFont f = nowPlayingTag_->font();
            f.setPointSize(9);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
            nowPlayingTag_->setFont(f);
        }
        nowPlayingTag_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent;"));
        nowPlayingTag_->setAlignment(Qt::AlignCenter);
        fgLayout->addWidget(nowPlayingTag_);

        fgLayout->addSpacing(2);

        playerTrackLabel_ = new QLabel(QStringLiteral("No track loaded"), foreground);
        {
            QFont f = playerTrackLabel_->font();
            f.setPointSize(20);
            f.setBold(true);
            playerTrackLabel_->setFont(f);
        }
        playerTrackLabel_->setAlignment(Qt::AlignCenter);
        playerTrackLabel_->setWordWrap(true);
        playerTrackLabel_->setStyleSheet(QStringLiteral(
            "color: #ffffff; background: transparent; border: none; padding: 4px 8px;"));
        fgLayout->addWidget(playerTrackLabel_);

        playerArtistLabel_ = new QLabel(QString(), foreground);
        {
            QFont f = playerArtistLabel_->font();
            f.setPointSize(13);
            playerArtistLabel_->setFont(f);
        }
        playerArtistLabel_->setAlignment(Qt::AlignCenter);
        playerArtistLabel_->setStyleSheet(QStringLiteral(
            "color: #cccccc; background: transparent; border: none; padding: 2px 6px;"));
        fgLayout->addWidget(playerArtistLabel_);

        playerMetaLabel_ = new QLabel(QString(), foreground);
        {
            QFont f = playerMetaLabel_->font();
            f.setPointSize(10);
            playerMetaLabel_->setFont(f);
        }
        playerMetaLabel_->setAlignment(Qt::AlignCenter);
        playerMetaLabel_->setStyleSheet(QStringLiteral(
            "color: #999999; background: transparent; border: none; padding: 2px 6px;"));
        fgLayout->addWidget(playerMetaLabel_);

        playerStateLabel_ = new QLabel(QStringLiteral("Stopped"), foreground);
        {
            QFont f = playerStateLabel_->font();
            f.setPointSize(10);
            f.setBold(true);
            playerStateLabel_->setFont(f);
        }
        playerStateLabel_->setAlignment(Qt::AlignCenter);
        playerStateLabel_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent;"));
        fgLayout->addWidget(playerStateLabel_);

        // Up Next label
        upNextLabel_ = new QLabel(QStringLiteral("Up Next: \u2014"), foreground);
        {
            QFont f = upNextLabel_->font();
            f.setPointSize(9);
            f.setItalic(true);
            upNextLabel_->setFont(f);
        }
        upNextLabel_->setAlignment(Qt::AlignCenter);
        upNextLabel_->setStyleSheet(QStringLiteral("color: #888888; background: transparent;"));
        fgLayout->addWidget(upNextLabel_);

        fgLayout->addStretch(1);

        // B3. Control strip at bottom of foreground: [Pulse | Tune] ——— [Line | Bars | Circle | None]
        auto* vizControlRow = new QHBoxLayout();
        vizControlRow->setContentsMargins(0, 0, 0, 10);
        vizControlRow->setSpacing(6);

        // Left: Pulse ON/OFF
        pulseBtn_ = new QPushButton(QStringLiteral("Pulse: ON"), foreground);
        pulseBtn_->setMinimumSize(90, 28);
        pulseBtn_->setCursor(Qt::PointingHandCursor);
        pulseBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
            "  color: #e94560; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
            "QPushButton:hover { background: rgba(31,74,112,220); }"));
        vizControlRow->addWidget(pulseBtn_);

        QObject::connect(pulseBtn_, &QPushButton::clicked, this, [this]() {
            visualizer_->setPulseEnabled(!visualizer_->pulseEnabled());
            pulseBtn_->setText(visualizer_->pulseEnabled()
                ? QStringLiteral("Pulse: ON") : QStringLiteral("Pulse: OFF"));
            pulseBtn_->setStyleSheet(visualizer_->pulseEnabled()
                ? QStringLiteral(
                    "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
                    "  color: #e94560; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
                    "QPushButton:hover { background: rgba(31,74,112,220); }")
                : QStringLiteral(
                    "QPushButton { background: rgba(22,33,62,200); border: 1px solid #0f3460; border-radius: 4px;"
                    "  color: #666; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
                    "QPushButton:hover { background: rgba(26,26,46,220); }"));
            qInfo().noquote() << QStringLiteral("VIZ_PULSE=%1").arg(visualizer_->pulseEnabled() ? "ON" : "OFF");
        });

        // Tune button (cycles levels 0–4)
        tuneBtn_ = new QPushButton(QStringLiteral("Tune: 2"), foreground);
        tuneBtn_->setMinimumSize(80, 28);
        tuneBtn_->setCursor(Qt::PointingHandCursor);
        tuneBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
            "  color: #aaccee; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
            "QPushButton:hover { background: rgba(31,74,112,220); }"));
        vizControlRow->addWidget(tuneBtn_);

        QObject::connect(tuneBtn_, &QPushButton::clicked, this, [this]() {
            int next = (visualizer_->tuneLevel() + 1) % 5;
            visualizer_->setTuneLevel(next);
            tuneBtn_->setText(QStringLiteral("Tune: %1").arg(next));
            qInfo().noquote() << QStringLiteral("VIZ_TUNE=%1").arg(next);
        });

        vizControlRow->addStretch(1);

        // Right: display mode buttons
        auto makeVizModeBtn = [&](const QString& label) -> QPushButton* {
            auto* btn = new QPushButton(label, foreground);
            btn->setMinimumSize(60, 28);
            btn->setCursor(Qt::PointingHandCursor);
            btn->setCheckable(true);
            btn->setStyleSheet(QStringLiteral(
                "QPushButton { background: rgba(22,33,62,200); border: 1px solid #0f3460; border-radius: 4px;"
                "  color: #888; font-size: 10px; padding: 2px 8px; }"
                "QPushButton:hover { background: rgba(26,26,46,220); color: #ccc; }"
                "QPushButton:checked { background: rgba(83,52,131,200); color: #fff; border-color: #e94560; }"));
            return btn;
        };

        vizLineBtn_   = makeVizModeBtn(QStringLiteral("Line"));
        vizBarsBtn_   = makeVizModeBtn(QStringLiteral("Bars"));
        vizCircleBtn_ = makeVizModeBtn(QStringLiteral("Circle"));
        vizNoneBtn_   = makeVizModeBtn(QStringLiteral("None"));

        // Default: Bars is active
        vizBarsBtn_->setChecked(true);

        vizControlRow->addWidget(vizLineBtn_);
        vizControlRow->addWidget(vizBarsBtn_);
        vizControlRow->addWidget(vizCircleBtn_);
        vizControlRow->addWidget(vizNoneBtn_);

        auto switchVizMode = [this](VisualizerWidget::DisplayMode mode) {
            visualizer_->setDisplayMode(mode);
            vizLineBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Line);
            vizBarsBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Bars);
            vizCircleBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Circle);
            vizNoneBtn_->setChecked(mode == VisualizerWidget::DisplayMode::None);
            // (JUCE path: no audioBufferOutput_ to gate)
            const char* names[] = {"None", "Bars", "Line", "Circle"};
            qInfo().noquote() << QStringLiteral("VIZ_MODE=%1").arg(names[static_cast<int>(mode)]);
        };

        QObject::connect(vizLineBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Line); });
        QObject::connect(vizBarsBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Bars); });
        QObject::connect(vizCircleBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Circle); });
        QObject::connect(vizNoneBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::None); });

        fgLayout->addLayout(vizControlRow);

        // Add foreground as second layer (on top of visualizer)
        heroStack->addWidget(foreground);
        heroStack->setCurrentWidget(foreground); // ensure foreground is on top

        layout->addWidget(heroFrame);

        // Animation timer for visualizer (~30fps)
        vizTimer_ = new QTimer(this);
        vizTimer_->setInterval(33);
        QObject::connect(vizTimer_, &QTimer::timeout, this, [this]() {
            // Feed visualizer from bridge meters at 30fps (not 4Hz pollStatus)
            const float freshLevel = static_cast<float>(
                std::max(bridge_.meterL(), bridge_.meterR()));
            if (freshLevel > 0.0f || bridge_.running())
                visualizer_->setAudioLevel(freshLevel);

            // Title pulse envelope: fast attack, slow decay — all in JUCE data path
            if (bridge_.running()) {
                constexpr double kDecay = 0.88;
                constexpr double kMinThreshold = 0.015;
                const double rawLevel = static_cast<double>(freshLevel);
                if (rawLevel > titlePulseEnvelope_)
                    titlePulseEnvelope_ = rawLevel;
                else
                    titlePulseEnvelope_ *= kDecay;
                if (titlePulseEnvelope_ < kMinThreshold)
                    titlePulseEnvelope_ = 0.0;
            } else {
                titlePulseEnvelope_ *= 0.85;
                if (titlePulseEnvelope_ < 0.001)
                    titlePulseEnvelope_ = 0.0;
            }
            visualizer_->setTitlePulse(static_cast<float>(titlePulseEnvelope_));

            if (visualizer_->displayMode() != VisualizerWidget::DisplayMode::None)
                visualizer_->tick();
        });
        vizTimer_->start();

        layout->addSpacing(14);

        // ═══════════════════════════════════════════════════
        // C. Progress section: time | seek bar | time
        // ═══════════════════════════════════════════════════
        auto* timeRow = new QHBoxLayout();
        timeRow->setSpacing(12);

        playerTimeLabel_ = new QLabel(QStringLiteral("0:00"), page);
        {
            QFont f = playerTimeLabel_->font();
            f.setPointSize(11);
            playerTimeLabel_->setFont(f);
        }
        playerTimeLabel_->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
        playerTimeLabel_->setMinimumWidth(42);
        playerTimeLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);

        seekSlider_ = new QSlider(Qt::Horizontal, page);
        seekSlider_->setRange(0, 1);
        seekSlider_->setMinimumHeight(24);

        playerTimeTotalLabel_ = new QLabel(QStringLiteral("0:00"), page);
        {
            QFont f = playerTimeTotalLabel_->font();
            f.setPointSize(11);
            playerTimeTotalLabel_->setFont(f);
        }
        playerTimeTotalLabel_->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
        playerTimeTotalLabel_->setMinimumWidth(42);
        playerTimeTotalLabel_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);

        timeRow->addWidget(playerTimeLabel_);
        timeRow->addWidget(seekSlider_, 1);
        timeRow->addWidget(playerTimeTotalLabel_);
        layout->addLayout(timeRow);

        layout->addSpacing(10);

        // ═══════════════════════════════════════════════════
        // D. Transport section: |< | Play/Pause | >|
        // ═══════════════════════════════════════════════════
        auto* transportRow = new QHBoxLayout();
        transportRow->setSpacing(16);

        // Invisible spacer to balance the Mode button on the right
        auto* transportLeftSpacer = new QWidget(page);
        transportLeftSpacer->setFixedSize(160, 1);
        transportLeftSpacer->setStyleSheet(QStringLiteral("background: transparent;"));
        transportRow->addWidget(transportLeftSpacer);

        transportRow->addStretch(1);

        prevBtn_ = new QPushButton(QStringLiteral("|<  Prev"), page);
        prevBtn_->setToolTip(QStringLiteral("Previous track"));
        prevBtn_->setMinimumSize(90, 48);
        prevBtn_->setCursor(Qt::PointingHandCursor);
        {
            QFont f = prevBtn_->font();
            f.setPointSize(12);
            f.setBold(true);
            prevBtn_->setFont(f);
        }
        transportRow->addWidget(prevBtn_);

        playPauseBtn_ = new QPushButton(QStringLiteral("Play"), page);
        playPauseBtn_->setToolTip(QStringLiteral("Play / Pause"));
        playPauseBtn_->setMinimumSize(120, 56);
        playPauseBtn_->setCursor(Qt::PointingHandCursor);
        {
            QFont f = playPauseBtn_->font();
            f.setPointSize(15);
            f.setBold(true);
            playPauseBtn_->setFont(f);
        }
        playPauseBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background: #e94560; border: none; border-radius: 28px;"
            "  font-size: 15px; font-weight: bold; color: #ffffff; padding: 0 24px; }"
            "QPushButton:hover { background: #d63851; }"
            "QPushButton:pressed { background: #c02a42; }"));
        transportRow->addWidget(playPauseBtn_);

        nextBtn_ = new QPushButton(QStringLiteral("Next  >|"), page);
        nextBtn_->setToolTip(QStringLiteral("Next track"));
        nextBtn_->setMinimumSize(90, 48);
        nextBtn_->setCursor(Qt::PointingHandCursor);
        {
            QFont f = nextBtn_->font();
            f.setPointSize(12);
            f.setBold(true);
            nextBtn_->setFont(f);
        }
        transportRow->addWidget(nextBtn_);

        transportRow->addStretch(1);

        // Play mode button — right-aligned in transport row
        playModeBtn_ = new QPushButton(QStringLiteral("Mode: In Order"), page);
        playModeBtn_->setToolTip(QStringLiteral("Click to cycle: Play Once / In Order / Repeat All / Shuffle / Smart Shuffle"));
        playModeBtn_->setMinimumSize(160, 36);
        playModeBtn_->setCursor(Qt::PointingHandCursor);
        {
            QFont f = playModeBtn_->font();
            f.setPointSize(10);
            playModeBtn_->setFont(f);
        }
        playModeBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background: #2a2a3e; border: 1px solid #555580; border-radius: 6px;"
            "  color: #ccccee; padding: 4px 14px; }"
            "QPushButton:hover { background: #3a3a50; }"));
        transportRow->addWidget(playModeBtn_);

        layout->addLayout(transportRow);

        QObject::connect(playModeBtn_, &QPushButton::clicked, this, [this]() {
            switch (playMode_) {
            case PlayMode::PlayOnce:      playMode_ = PlayMode::PlayInOrder;   break;
            case PlayMode::PlayInOrder:   playMode_ = PlayMode::RepeatAll;     break;
            case PlayMode::RepeatAll:     playMode_ = PlayMode::Shuffle;       break;
            case PlayMode::Shuffle:       playMode_ = PlayMode::SmartShuffle;  break;
            case PlayMode::SmartShuffle:  playMode_ = PlayMode::PlayOnce;      break;
            }
            if (playMode_ == PlayMode::SmartShuffle) {
                rebuildSmartShufflePool();
            }
            updatePlayModeButton();
            updateUpNextLabel();
            qInfo().noquote() << QStringLiteral("PLAY_MODE_CHANGED=%1").arg(playModeLabel());
        });

        layout->addSpacing(10);

        // ═══════════════════════════════════════════════════
        // D2. Volume slider (below transport)
        // ═══════════════════════════════════════════════════
        auto* volRow = new QHBoxLayout();
        volRow->setSpacing(10);

        auto* volLabel = new QLabel(QStringLiteral("Vol:"), page);
        {
            QFont f = volLabel->font();
            f.setPointSize(11);
            volLabel->setFont(f);
        }
        volLabel->setStyleSheet(QStringLiteral("color: #aaaaaa;"));

        volumeSlider_ = new QSlider(Qt::Horizontal, page);
        volumeSlider_->setRange(0, 100);
        volumeSlider_->setValue(80);
        volumeSlider_->setMinimumWidth(180);
        volumeSlider_->setMaximumWidth(300);

        auto* volPercent = new QLabel(QStringLiteral("80%"), page);
        {
            QFont f = volPercent->font();
            f.setPointSize(11);
            volPercent->setFont(f);
        }
        volPercent->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
        volPercent->setMinimumWidth(36);

        volRow->addStretch(1);
        volRow->addWidget(volLabel);
        volRow->addWidget(volumeSlider_);
        volRow->addWidget(volPercent);
        volRow->addStretch(1);
        layout->addLayout(volRow);

        layout->addSpacing(10);

        // ═══════════════════════════════════════════════════
        // D3. 16-Band EQ Panel (modular widget)
        // ═══════════════════════════════════════════════════
        eqPanel_ = new EqPanel(&bridge_, page);
        layout->addWidget(eqPanel_);

        layout->addSpacing(14);

        // ═══════════════════════════════════════════════════
        // E. Library browser: search + sort + column tree
        // ═══════════════════════════════════════════════════
        auto* libHeaderRow = new QHBoxLayout();
        libHeaderRow->setSpacing(8);

        auto* libLabel = new QLabel(QStringLiteral("Library"), page);
        {
            QFont f = libLabel->font();
            f.setPointSize(13);
            f.setBold(true);
            libLabel->setFont(f);
        }
        libHeaderRow->addWidget(libLabel);

        libHeaderRow->addSpacing(12);

        playerSearchBar_ = new QLineEdit(page);
        playerSearchBar_->setPlaceholderText(QStringLiteral("Search tracks..."));
        playerSearchBar_->setClearButtonEnabled(true);
        playerSearchBar_->setMinimumHeight(28);
        playerSearchBar_->setStyleSheet(QStringLiteral(
            "QLineEdit { background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 4px; padding: 4px 8px; font-size: 12px; }"
            "QLineEdit:focus { border-color: #e94560; }"));
        libHeaderRow->addWidget(playerSearchBar_, 1);

        libHeaderRow->addSpacing(8);

        auto* sortLabel = new QLabel(QStringLiteral("Sort:"), page);
        sortLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
        libHeaderRow->addWidget(sortLabel);

        playerSortCombo_ = new QComboBox(page);
        playerSortCombo_->addItems({
            QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Album"),
            QStringLiteral("Duration"), QStringLiteral("BPM"), QStringLiteral("Key")
        });
        playerSortCombo_->setMinimumWidth(90);
        libHeaderRow->addWidget(playerSortCombo_);

        libHeaderRow->addSpacing(8);

        auto* libCountLabel = new QLabel(QStringLiteral("0 tracks"), page);
        libCountLabel->setStyleSheet(QStringLiteral("color: #666; font-size: 11px;"));
        libHeaderRow->addWidget(libCountLabel);

        layout->addLayout(libHeaderRow);
        layout->addSpacing(4);

        playerLibraryTree_ = new QTreeWidget(page);
        playerLibraryTree_->setHeaderLabels({
            QStringLiteral("Name"), QStringLiteral("Artist"), QStringLiteral("Album"),
            QStringLiteral("Duration"), QStringLiteral("BPM"), QStringLiteral("Key")
        });
        playerLibraryTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        playerLibraryTree_->setRootIsDecorated(false);
        playerLibraryTree_->setAlternatingRowColors(true);
        playerLibraryTree_->setSortingEnabled(false);
        playerLibraryTree_->setVerticalScrollBarPolicy(Qt::ScrollBarAsNeeded);
        playerLibraryTree_->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
        {
            QFont f = playerLibraryTree_->font();
            f.setPointSize(11);
            playerLibraryTree_->setFont(f);
        }
        playerLibraryTree_->setStyleSheet(QStringLiteral(
            "QTreeWidget { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 8px; outline: none; alternate-background-color: #1a1a2e; }"
            "QTreeWidget::item { padding: 4px 6px; }"
            "QTreeWidget::item:selected { background: #533483; color: #ffffff; }"
            "QTreeWidget::item:hover { background: #1a1a2e; }"
            "QHeaderView::section { background: #0f3460; color: #e0e0e0; border: none;"
            "  padding: 5px 8px; font-weight: bold; font-size: 11px; }"
            "QScrollBar:vertical { background: #0a0e27; width: 8px; }"
            "QScrollBar::handle:vertical { background: #533483; border-radius: 4px; min-height: 20px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"));
        // Column widths
        playerLibraryTree_->header()->setSectionResizeMode(QHeaderView::Interactive);
        playerLibraryTree_->setColumnWidth(0, 280);  // Name
        playerLibraryTree_->setColumnWidth(1, 140);  // Artist
        playerLibraryTree_->setColumnWidth(2, 130);  // Album
        playerLibraryTree_->setColumnWidth(3, 65);   // Duration
        playerLibraryTree_->setColumnWidth(4, 50);   // BPM
        playerLibraryTree_->setColumnWidth(5, 45);   // Key
        layout->addWidget(playerLibraryTree_, 1);

        // Store libCountLabel for refreshPlayerLibrary to update
        playerLibCountLabel_ = libCountLabel;

        // ═══════════════════════════════════════════════════
        // Audio engine: JUCE via EngineBridge (all audio)
        // ═══════════════════════════════════════════════════
        // Visualizer audio level is now driven from JUCE engine meters
        // via pollStatus() → bridge_.meterL()/meterR()

        // ── Signal connections (JUCE bridge) ──

        // Running state → hero state label + play/pause button text
        QObject::connect(&bridge_, &EngineBridge::runningChanged, this, [this]() {
            if (bridge_.running()) {
                playerStateLabel_->setText(QStringLiteral("Playing"));
                playPauseBtn_->setText(QStringLiteral("Pause"));
                qInfo().noquote() << QStringLiteral("JUCE_PLAYBACK_STATE=PLAYING");
            } else {
                playerStateLabel_->setText(QStringLiteral("Stopped"));
                playPauseBtn_->setText(QStringLiteral("Play"));
                qInfo().noquote() << QStringLiteral("JUCE_PLAYBACK_STATE=STOPPED");
            }
        });

        // Duration → seek slider range + total time label (JUCE bridge)
        QObject::connect(&bridge_, &EngineBridge::durationChanged, this, [this](double seconds) {
            const uint64_t gen = bridge_.currentLoadGen();
            if (gen != uiTrackGen_) {
                qInfo().noquote() << QStringLiteral("TRC_UI durationChanged DROP gen=%1 uiGen=%2 dur=%3")
                    .arg(gen).arg(uiTrackGen_).arg(seconds, 0, 'f', 2);
                return;
            }
            const int durSec = static_cast<int>(seconds);
            seekSlider_->setRange(0, durSec);
            playerTimeTotalLabel_->setText(QStringLiteral("%1:%2")
                .arg(durSec / 60).arg(durSec % 60, 2, 10, QChar('0')));
            qInfo().noquote() << QStringLiteral("TRC_UI durationChanged=%1 sliderMax=%2 gen=%3 IDX=%4 name=%5")
                .arg(seconds, 0, 'f', 2).arg(durSec).arg(gen).arg(currentTrackIndex_)
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"));
        });

        // Position → seek slider + current time label (JUCE bridge)
        QObject::connect(&bridge_, &EngineBridge::playheadChanged, this, [this](double seconds) {
            const uint64_t gen = bridge_.currentLoadGen();
            if (gen != uiTrackGen_) return; // stale generation
            const int posSec = static_cast<int>(seconds);
            if (!seekSliderPressed_) {
                seekSlider_->setValue(posSec);
            }
            playerTimeLabel_->setText(QStringLiteral("%1:%2")
                .arg(posSec / 60).arg(posSec % 60, 2, 10, QChar('0')));
        });

        // End of track (JUCE bridge)
        QObject::connect(&bridge_, &EngineBridge::endOfTrack, this, [this]() {
            const uint64_t gen = bridge_.currentLoadGen();
            if (gen != uiTrackGen_) {
                qInfo().noquote() << QStringLiteral("TRC_UI endOfTrack DROP gen=%1 uiGen=%2").arg(gen).arg(uiTrackGen_);
                return;
            }
            qInfo().noquote() << QStringLiteral("TRC_UI endOfTrack ACCEPT gen=%1 IDX=%2 name=%3 sliderVal=%4 sliderMax=%5")
                .arg(gen).arg(currentTrackIndex_)
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"))
                .arg(seekSlider_ ? seekSlider_->value() : -1)
                .arg(seekSlider_ ? seekSlider_->maximum() : -1);
            onEndOfMedia();
        });

        // Seek slider interaction
        QObject::connect(seekSlider_, &QSlider::sliderPressed, this, [this]() { seekSliderPressed_ = true; });
        QObject::connect(seekSlider_, &QSlider::sliderReleased, this, [this]() {
            seekSliderPressed_ = false;
            const int seekVal = seekSlider_->value();
            const int seekMax = seekSlider_->maximum();
            qInfo().noquote() << QStringLiteral("TRC_UI seekRelease val=%1 max=%2 IDX=%3 name=%4")
                .arg(seekVal).arg(seekMax).arg(currentTrackIndex_)
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"));
            bridge_.seek(static_cast<double>(seekVal));
        });

        // Play/Pause (JUCE path)
        QObject::connect(playPauseBtn_, &QPushButton::clicked, this, [this]() {
            if (bridge_.running()) {
                bridge_.pause();
                playerStateLabel_->setText(QStringLiteral("Paused"));
                playPauseBtn_->setText(QStringLiteral("Play"));
                qInfo().noquote() << QStringLiteral("JUCE_PAUSE=TRUE");
            } else {
                bridge_.start();
                playerStateLabel_->setText(QStringLiteral("Playing"));
                playPauseBtn_->setText(QStringLiteral("Pause"));
                qInfo().noquote() << QStringLiteral("JUCE_RESUME=TRUE");
            }
        });

        // Previous / Next
        QObject::connect(prevBtn_, &QPushButton::clicked, this, [this]() { playPrevTrack(); });
        QObject::connect(nextBtn_, &QPushButton::clicked, this, [this]() { playNextTrack(); });

        // Volume → JUCE master gain + percent label
        QObject::connect(volumeSlider_, &QSlider::valueChanged, this, [this, volPercent](int value) {
            bridge_.setMasterGain(static_cast<double>(value) / 100.0);
            volPercent->setText(QStringLiteral("%1%").arg(value));
        });

        // Library tree → double-click to play
        QObject::connect(playerLibraryTree_, &QTreeWidget::itemDoubleClicked, this, [this](QTreeWidgetItem* item, int) {
            const int idx = item->data(0, Qt::UserRole).toInt();
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
                currentTrackIndex_ = idx;
                loadAndPlayTrack(idx);
                qInfo().noquote() << QStringLiteral("PLAYER_LIB_PLAY=%1").arg(allTracks_[idx].displayName);
            }
        });

        // Search bar → filter library
        QObject::connect(playerSearchBar_, &QLineEdit::textChanged, this, [this](const QString&) {
            refreshPlayerLibrary();
        });

        // Sort combo → re-sort library
        QObject::connect(playerSortCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
            refreshPlayerLibrary();
        });

        return page;
    }

    QWidget* buildDjModePage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral("background: #080b10;"));
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(0);

        auto* libraryPane = new DjBrowserPane(page);
        QString libErr;
        if (!libraryPane->initialize(runtimePath("data/runtime/dj_library.db"), &libErr)) {
            qWarning() << "Browser DB init failed:" << libErr;
        }
        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);

        auto* topHalf = new QWidget(page);
        topHalf->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        auto* topLayout = new QVBoxLayout(topHalf);
        topLayout->setContentsMargins(0, 0, 0, 0);

        // ── Header row: Back + title ──
        auto* headerRow = new QHBoxLayout();
        headerRow->setSpacing(6);
        auto* backBtn = new QPushButton(QStringLiteral("\u2190 Back"), page);
        backBtn->setCursor(Qt::PointingHandCursor);
        backBtn->setStyleSheet(QStringLiteral(
            "QPushButton { background: rgba(20,20,30,200); border: 1px solid #333;"
            "  border-radius: 4px; color: #aaa; font-size: 9px; padding: 4px 10px; }"
            "QPushButton:hover { background: rgba(40,40,60,220); color: #ddd; }"));
        headerRow->addWidget(backBtn);

        auto* title = new QLabel(QStringLiteral("DJ MIXER"), page);
        {
            QFont f = title->font();
            f.setPointSize(14);
            f.setBold(true);
            title->setFont(f);
        }
        title->setStyleSheet(QStringLiteral("color: #e0e0e0; background: transparent;"));
        title->setAlignment(Qt::AlignCenter);
        headerRow->addWidget(title, 1);

        headerRow->addSpacing(60);  // balance the back button
        topLayout->addLayout(headerRow);

        QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
            bridge_.leaveDjMode();
            stack_->setCurrentIndex(1);
        });

        // ── Per-deck columns: Deck + Library side by side ──
        auto* deckRow = new QHBoxLayout();
        deckRow->setSpacing(6);

        // ── Deck A column: strip + library ──
        auto* colA = new QVBoxLayout();
        colA->setSpacing(4);
        djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, page);
        colA->addWidget(djDeckA_, 1);


        deckRow->addLayout(colA, 5);

        // ── Master section column (center) ──
        auto* masterCol = new QVBoxLayout();
        masterCol->setSpacing(4);
        masterCol->setContentsMargins(4, 0, 4, 0);

        auto* masterLabel = new QLabel(QStringLiteral("MASTER"), page);
        {
            QFont f = masterLabel->font(); f.setPointSizeF(7.5); f.setBold(true);
            masterLabel->setFont(f);
        }
        masterLabel->setAlignment(Qt::AlignCenter);
        masterLabel->setStyleSheet(QStringLiteral(
            "color: #e0e0e0; background: transparent; padding: 2px 0;"));
        masterCol->addWidget(masterLabel);

        // Master L/R meters
        auto* masterMeterRow = new QHBoxLayout();
        masterMeterRow->setSpacing(2);
        masterMeterRow->addStretch();
        djMasterMeterL_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), page);
        djMasterMeterR_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), page);
        masterMeterRow->addWidget(djMasterMeterL_);
        masterMeterRow->addWidget(djMasterMeterR_);
        masterMeterRow->addStretch();
        masterCol->addLayout(masterMeterRow, 1);

        // CUE MIX label + slider (horizontal)
        auto* cueMixLabel = new QLabel(QStringLiteral("CUE MIX"), page);
        {
            QFont f = cueMixLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            cueMixLabel->setFont(f);
        }
        cueMixLabel->setAlignment(Qt::AlignCenter);
        cueMixLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(cueMixLabel);

        djCueMix_ = new QSlider(Qt::Horizontal, page);
        djCueMix_->setRange(0, 1000);
        djCueMix_->setValue(500);
        djCueMix_->setFixedHeight(22);
        djCueMix_->setStyleSheet(QStringLiteral(
            "QSlider::groove:horizontal {"
            "  background: #161616; height: 6px; border-radius: 3px;"
            "  border: 1px solid #333; }"
            "QSlider::handle:horizontal {"
            "  background: #d0d0d0; width: 14px; height: 14px;"
            "  margin: -5px 0; border-radius: 3px;"
            "  border: 1px solid #666; }"
            "QSlider::sub-page:horizontal {"
            "  background: #4070a0; border-radius: 3px; }"
            "QSlider::add-page:horizontal {"
            "  background: #333; border-radius: 3px; }"));
        masterCol->addWidget(djCueMix_);

        // CUE VOL label + slider (horizontal)
        auto* cueVolLabel = new QLabel(QStringLiteral("CUE VOL"), page);
        {
            QFont f = cueVolLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            cueVolLabel->setFont(f);
        }
        cueVolLabel->setAlignment(Qt::AlignCenter);
        cueVolLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(cueVolLabel);

        djCueVol_ = new QSlider(Qt::Horizontal, page);
        djCueVol_->setRange(0, 1000);
        djCueVol_->setValue(1000);
        djCueVol_->setFixedHeight(22);
        djCueVol_->setStyleSheet(QStringLiteral(
            "QSlider::groove:horizontal {"
            "  background: #161616; height: 6px; border-radius: 3px;"
            "  border: 1px solid #333; }"
            "QSlider::handle:horizontal {"
            "  background: #d0d0d0; width: 14px; height: 14px;"
            "  margin: -5px 0; border-radius: 3px;"
            "  border: 1px solid #666; }"
            "QSlider::sub-page:horizontal {"
            "  background: #4070a0; border-radius: 3px; }"
            "QSlider::add-page:horizontal {"
            "  background: #333; border-radius: 3px; }"));
        masterCol->addWidget(djCueVol_);

        // OUTPUT MODE label + toggle button
        auto* outModeLabel = new QLabel(QStringLiteral("OUTPUT"), page);
        {
            QFont f = outModeLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            outModeLabel->setFont(f);
        }
        outModeLabel->setAlignment(Qt::AlignCenter);
        outModeLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(outModeLabel);

        djOutputModeBtn_ = new QPushButton(QStringLiteral("Stereo"), page);
        djOutputModeBtn_->setCursor(Qt::PointingHandCursor);
        djOutputModeBtn_->setCheckable(true);
        djOutputModeBtn_->setChecked(false);
        djOutputModeBtn_->setFixedHeight(24);
        djOutputModeBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background: #1a1a2a; border: 1px solid #444;"
            "  border-radius: 4px; color: #ccc; font-size: 8pt; font-weight: bold;"
            "  padding: 2px 6px; }"
            "QPushButton:checked { background: #2a4060; border: 1px solid #4090d0;"
            "  color: #60c0ff; }"
            "QPushButton:hover { background: #222240; }"));
        masterCol->addWidget(djOutputModeBtn_);

        masterCol->addStretch();
        deckRow->addLayout(masterCol, 2);

        // ── Deck B column: strip + library ──
        auto* colB = new QVBoxLayout();
        colB->setSpacing(4);
        djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, page);
        colB->addWidget(djDeckB_, 1);


        deckRow->addLayout(colB, 5);

        topLayout->addLayout(deckRow, 1);

        // ── Crossfader row ──
        auto* xfadeRow = new QHBoxLayout();
        xfadeRow->setSpacing(4);
        xfadeRow->setContentsMargins(0, 1, 0, 1);

        auto* xfadeLabel = new QLabel(QStringLiteral("A"), page);
        {
            QFont f = xfadeLabel->font(); f.setPointSize(12); f.setBold(true);
            xfadeLabel->setFont(f);
        }
        xfadeLabel->setStyleSheet(QStringLiteral(
            "color: #e07020; background: transparent;"));
        xfadeRow->addWidget(xfadeLabel);

        djCrossfader_ = new QSlider(Qt::Horizontal, page);
        djCrossfader_->setRange(0, 1000);
        djCrossfader_->setValue(500);
        djCrossfader_->setFixedHeight(32);
        djCrossfader_->setStyleSheet(QStringLiteral(
            "QSlider::groove:horizontal {"
            "  background: qlineargradient(x1:0,x2:1,"
            "    stop:0 rgba(224,112,32,25), stop:0.48 #0a0a0a,"
            "    stop:0.5 #222, stop:0.52 #0a0a0a,"
            "    stop:1 rgba(32,128,224,25));"
            "  height: 8px; border-radius: 4px;"
            "  border: 1px solid #222; }"
            "QSlider::handle:horizontal {"
            "  background: qlineargradient(x1:0,x2:1, stop:0 #d0d0d0, stop:0.5 #ffffff, stop:1 #d0d0d0);"
            "  width: 28px; height: 28px;"
            "  margin: -10px 0; border-radius: 4px;"
            "  border: 1px solid #666; }"
            "QSlider::sub-page:horizontal {"
            "  background: qlineargradient(x1:0,x2:1, stop:0 #e07020, stop:1 #333);"
            "  border-radius: 4px; }"
            "QSlider::add-page:horizontal {"
            "  background: qlineargradient(x1:0,x2:1, stop:0 #333, stop:1 #2080e0);"
            "  border-radius: 4px; }"));
        xfadeRow->addWidget(djCrossfader_, 1);

        auto* xfadeLabelB = new QLabel(QStringLiteral("B"), page);
        {
            QFont f = xfadeLabelB->font(); f.setPointSize(12); f.setBold(true);
            xfadeLabelB->setFont(f);
        }
        xfadeLabelB->setStyleSheet(QStringLiteral(
            "color: #2080e0; background: transparent;"));
        xfadeRow->addWidget(xfadeLabelB);

        topLayout->addLayout(xfadeRow);

        // ── DJ Library (now using DjBrowserPane) ──
        
        auto* mixerPane = new QWidget(page);
        mixerPane->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        auto* mixerLayout = new QVBoxLayout(mixerPane);
        mixerLayout->setContentsMargins(0, 0, 0, 0);
        mixerLayout->addWidget(topHalf, 1);

        auto* splitter = new QSplitter(Qt::Horizontal, page);
        splitter->addWidget(libraryPane);
        splitter->addWidget(mixerPane);
        
        QList<int> sizes;
        sizes << 420 << 1180;
        splitter->setSizes(sizes);
        
        splitter->setStretchFactor(0, 1);
        splitter->setStretchFactor(1, 3);
        
        layout->addWidget(splitter, 1);

        QObject::connect(djCrossfader_, &QSlider::valueChanged, this, [this](int value) {
            bridge_.setCrossfader(static_cast<double>(value) / 1000.0);
        });

        // ── Master section cue controls wiring ──
        QObject::connect(djCueMix_, &QSlider::valueChanged, this, [this](int value) {
            bridge_.setCueMix(static_cast<double>(value) / 1000.0);
        });

        QObject::connect(djCueVol_, &QSlider::valueChanged, this, [this](int value) {
            bridge_.setCueVolume(static_cast<double>(value) / 1000.0);
        });

        // ── Output mode toggle ──
        QObject::connect(djOutputModeBtn_, &QPushButton::toggled, this, [this](bool checked) {
            const int mode = checked ? 1 : 0;
            bridge_.setOutputMode(mode);
            djOutputModeBtn_->setText(checked
                ? QStringLiteral("Split Mono")
                : QStringLiteral("Stereo"));
            qInfo().noquote() << QStringLiteral("DJ_OUTPUT_MODE=%1").arg(mode);
        });

        // ── Device switch result logging (combo removed — signal still used for diagnostics) ──
        QObject::connect(&bridge_, &EngineBridge::deviceSwitchFinished, this,
            [](bool ok, const QString& activeDevice, long long elapsedMs) {
            qInfo().noquote() << QStringLiteral("DJ_DEVICE_SWITCH_DONE ok=%1 active='%2' [%3ms]")
                .arg(ok).arg(activeDevice).arg(elapsedMs);
        });

        // ── Audio profile applied result (async) ──
        QObject::connect(&bridge_, &EngineBridge::audioProfileApplied, this,
            &MainWindow::onAudioProfileApplied);

        // ── UI heartbeat — detects main-thread freezes ──
        {
            auto* hb = new QTimer(this);
            auto* lastBeat = new qint64(QDateTime::currentMSecsSinceEpoch());
            connect(hb, &QTimer::timeout, this, [lastBeat]() {
                const qint64 now = QDateTime::currentMSecsSinceEpoch();
                const qint64 gap = now - *lastBeat;
                if (gap > 400) {
                    const unsigned long tid = GetCurrentThreadId();
                    qWarning().noquote() << QStringLiteral("UI_HEARTBEAT: FREEZE gap=%1ms tid=%2")
                        .arg(gap).arg(tid);
                }
                *lastBeat = now;
            });
            hb->start(200);
        }

        // ── DeckStrip LOAD buttons: library is offline, no-op ──
        QObject::connect(djDeckA_, &DeckStrip::loadRequested, this, [](int) {});
        QObject::connect(djDeckB_, &DeckStrip::loadRequested, this, [](int) {});

        // Wire snapshot refresh
        QObject::connect(&bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
            if (djDeckA_) djDeckA_->refreshFromSnapshot();
            if (djDeckB_) djDeckB_->refreshFromSnapshot();
            if (djMasterMeterL_) djMasterMeterL_->setLevel(static_cast<float>(bridge_.masterPeakL()));
            if (djMasterMeterR_) djMasterMeterR_->setLevel(static_cast<float>(bridge_.masterPeakR()));
        });

        // ── Device-lost overlay banner + Recover Audio button ──
        djDeviceLostBanner_ = new QWidget(page);
        djDeviceLostBanner_->setVisible(false);
        djDeviceLostBanner_->setStyleSheet(QStringLiteral(
            "background: rgba(180,30,30,220); border: 2px solid #ff4444;"
            " border-radius: 6px;"));
        auto* bannerLayout = new QVBoxLayout(djDeviceLostBanner_);
        bannerLayout->setContentsMargins(12, 8, 12, 8);
        bannerLayout->setSpacing(6);

        djBannerTitleLabel_ = new QLabel(
            QStringLiteral("OUTPUT LOST!!!!   RECONNECT IMMEDIATELY!!!!!!!!!"), djDeviceLostBanner_);
        {
            QFont f = djBannerTitleLabel_->font(); f.setPointSize(18); f.setBold(true);
            djBannerTitleLabel_->setFont(f);
        }
        djBannerTitleLabel_->setAlignment(Qt::AlignCenter);
        djBannerTitleLabel_->setStyleSheet(QStringLiteral(
            "color: #ffffff; background: transparent;"));
        bannerLayout->addWidget(djBannerTitleLabel_);

        djRecoveryStatusLabel_ = new QLabel(QString(), djDeviceLostBanner_);
        djRecoveryStatusLabel_->setVisible(false);
        bannerLayout->addWidget(djRecoveryStatusLabel_);

        djRecoverBtn_ = new QPushButton(QString(), djDeviceLostBanner_);
        djRecoverBtn_->setVisible(false);
        djRecoverBtn_->setFixedHeight(0);
        bannerLayout->addWidget(djRecoverBtn_);

        djDeviceLostBanner_->setParent(splitter);
        djDeviceLostBanner_->raise();

        // ── Wire djDeviceLost signal → show banner ──
        QObject::connect(&bridge_, &EngineBridge::djDeviceLost, this, [this]() {
            // Stop any pending green-banner dismiss timer
            if (djBannerDismissTimer_) djBannerDismissTimer_->stop();
            if (djDeviceLostBanner_) {
                djDeviceLostBanner_->setVisible(true);
                djDeviceLostBanner_->setStyleSheet(QStringLiteral(
                    "background: rgba(180,30,30,220); border: 2px solid #ff4444;"
                    " border-radius: 6px;"));
            }
            if (djBannerTitleLabel_) djBannerTitleLabel_->setText(
                QStringLiteral("OUTPUT LOST!!!!   RECONNECT IMMEDIATELY!!!!!!!!!"));
            if (djRecoverBtn_) djRecoverBtn_->setVisible(false);
            if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setVisible(false);
        });

        // ── Wire Recover Audio button → attemptDjRecovery ──
        QObject::connect(djRecoverBtn_, &QPushButton::clicked, this, [this]() {
            if (djRecoverBtn_) djRecoverBtn_->setEnabled(false);
            if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setText(
                QStringLiteral("Attempting recovery..."));
            bridge_.attemptDjRecovery();
        });

        // ── Wire recovery result signals ──
        QObject::connect(&bridge_, &EngineBridge::djRecoverySuccess, this,
            [this](const QString& activeDevice) {
            if (djDeviceLostBanner_) djDeviceLostBanner_->setVisible(false);
            qInfo().noquote() << QStringLiteral("DJ_RECOVERY_UI: success device='%1'")
                .arg(activeDevice);
        });

        QObject::connect(&bridge_, &EngineBridge::djRecoveryFailed, this,
            [this](const QString& reason) {
            if (djRecoverBtn_) djRecoverBtn_->setEnabled(true);
            if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setText(
                QStringLiteral("Recovery failed:\n%1").arg(reason));
            qWarning().noquote() << QStringLiteral("DJ_RECOVERY_UI: failed reason='%1'")
                .arg(reason);
        });

        // ── Wire auto-recovery success → green banner + auto-dismiss ──
        djBannerDismissTimer_ = new QTimer(this);
        djBannerDismissTimer_->setSingleShot(true);
        QObject::connect(djBannerDismissTimer_, &QTimer::timeout, this, [this]() {
            if (djDeviceLostBanner_) djDeviceLostBanner_->setVisible(false);
            qInfo().noquote() << QStringLiteral("DJ_BANNER_HIDE_GREEN");
        });

        QObject::connect(&bridge_, &EngineBridge::djAutoRecoverySuccess, this,
            [this](const QString& activeDevice, bool wasPlaying) {
            if (djDeviceLostBanner_) {
                djDeviceLostBanner_->setVisible(true);
                djDeviceLostBanner_->setStyleSheet(QStringLiteral(
                    "background: rgba(40,100,50,220); border: 2px solid #44aa55;"
                    " border-radius: 6px;"));
            }
            if (djBannerTitleLabel_) djBannerTitleLabel_->setText(
                QStringLiteral("CONNECTION RESTORED"));
            if (djRecoverBtn_) djRecoverBtn_->setVisible(false);
            if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setVisible(false);
            qInfo().noquote() << QStringLiteral("DJ_BANNER_SHOW_GREEN device='%1' wasPlaying=%2")
                .arg(activeDevice).arg(wasPlaying ? 1 : 0);

            // Auto-dismiss after 3 seconds
            djBannerDismissTimer_->start(3000);
        });

        return page;
    }

    /// DJ library is offline — stub to prevent call-site errors.
    void populateDjLibraryTrees() { /* library not initialized */ }

    /// DJ library is offline — stub to prevent call-site errors.
    void refreshDjLibraryHighlights() { /* library not initialized */ }

    // ── Library helpers ──
    void refreshLibraryList()
    {
        if (!libraryTree_) return;
        // Save selection before rebuild
        int prevSelectedTrackIdx = -1;
        if (auto* cur = libraryTree_->currentItem())
            prevSelectedTrackIdx = cur->data(0, Qt::UserRole).toInt();
        // Block selection signals during rebuild to prevent stale callbacks
        const QSignalBlocker blocker(libraryTree_);
        libraryTree_->clear();
        int visibleCount = 0;
        const QString query = searchQuery_.trimmed().toLower();
        const int searchMode = searchModeCombo_ ? searchModeCombo_->currentData().toInt() : 5;

        // Build set of allowed paths when a playlist is active
        QSet<QString> playlistPathSet;
        if (activePlaylistIndex_ >= 0
            && activePlaylistIndex_ < static_cast<int>(playlists_.size())) {
            for (const QString& p : playlists_[activePlaylistIndex_].trackPaths)
                playlistPathSet.insert(p);
        }

        // Build filtered index list
        std::vector<int> filtered;
        for (int i = 0; i < static_cast<int>(allTracks_.size()); ++i) {
            const TrackInfo& t = allTracks_[i];
            // Playlist filter: skip tracks not in the active playlist
            if (!playlistPathSet.isEmpty() && !playlistPathSet.contains(t.filePath))
                continue;
            if (!query.isEmpty()) {
                bool match = false;
                switch (searchMode) {
                case 0: // File Name
                    match = t.displayName.toLower().contains(query);
                    break;
                case 1: // Artist
                    match = t.artist.toLower().contains(query);
                    break;
                case 2: // Album
                    match = t.album.toLower().contains(query);
                    break;
                case 3: { // BPM — supports single value or range like "120-130"
                    if (t.bpm.isEmpty()) break;
                    const double trackBpm = t.bpm.toDouble();
                    if (trackBpm <= 0) break;
                    const int dashIdx = query.indexOf('-');
                    if (dashIdx > 0) {
                        bool okLo = false, okHi = false;
                        const double lo = query.left(dashIdx).trimmed().toDouble(&okLo);
                        const double hi = query.mid(dashIdx + 1).trimmed().toDouble(&okHi);
                        if (okLo && okHi) match = (trackBpm >= lo && trackBpm <= hi);
                    } else {
                        bool ok = false;
                        const double target = query.toDouble(&ok);
                        if (ok) match = (std::abs(trackBpm - target) < 1.0);
                    }
                    break;
                }
                case 4: { // Length — supports "M:SS" or "M:SS-M:SS" range
                    if (t.durationMs <= 0) break;
                    auto parseMMSS = [](const QString& s, bool& ok) -> qint64 {
                        ok = false;
                        const int ci = s.indexOf(':');
                        if (ci > 0) {
                            bool mOk = false, sOk = false;
                            const int m = s.left(ci).trimmed().toInt(&mOk);
                            const int sec = s.mid(ci + 1).trimmed().toInt(&sOk);
                            if (mOk && sOk) { ok = true; return (qint64(m) * 60 + sec) * 1000; }
                        } else {
                            const int sec = s.trimmed().toInt(&ok);
                            if (ok) return qint64(sec) * 1000;
                        }
                        return 0;
                    };
                    const int dashIdx = query.indexOf('-');
                    if (dashIdx > 0 && query.indexOf(':') >= 0) {
                        bool okLo = false, okHi = false;
                        const qint64 lo = parseMMSS(query.left(dashIdx).trimmed(), okLo);
                        const qint64 hi = parseMMSS(query.mid(dashIdx + 1).trimmed(), okHi);
                        if (okLo && okHi) match = (t.durationMs >= lo && t.durationMs <= hi);
                    } else {
                        bool ok = false;
                        const qint64 target = parseMMSS(query, ok);
                        if (ok) match = (std::abs(t.durationMs - target) < 30000); // ±30s
                    }
                    break;
                }
                default: // All Fields (mode 5 or fallback)
                    match = t.displayName.toLower().contains(query)
                         || t.artist.toLower().contains(query)
                         || t.title.toLower().contains(query)
                         || t.album.toLower().contains(query)
                         || t.genre.toLower().contains(query)
                         || t.bpm.toLower().contains(query)
                         || t.musicalKey.toLower().contains(query)
                         || t.camelotKey.toLower().contains(query);
                    break;
                }
                if (!match) continue;
            }
            filtered.push_back(i);
        }

        // Sort
        const int sortCol = sortCombo_ ? sortCombo_->currentData().toInt() : 0;
        std::sort(filtered.begin(), filtered.end(), [&](int a, int b) {
            const TrackInfo& ta = allTracks_[a];
            const TrackInfo& tb = allTracks_[b];
            switch (sortCol) {
            case 1: return ta.artist.toLower() < tb.artist.toLower();
            case 2: return ta.album.toLower()  < tb.album.toLower();
            case 3: return ta.durationMs < tb.durationMs;
            case 4: return ta.bpm.toDouble() < tb.bpm.toDouble();
            case 5: return ta.musicalKey.toLower() < tb.musicalKey.toLower();
            default: return ta.title.toLower() < tb.title.toLower();
            }
        });

        for (int idx : filtered) {
            const TrackInfo& t = allTracks_[idx];
            auto* item = new QTreeWidgetItem(libraryTree_);
            item->setText(0, t.displayName.isEmpty() ? QStringLiteral("Unknown") : t.displayName);
            item->setText(1, t.artist.isEmpty() ? QStringLiteral("-") : t.artist);
            item->setText(2, t.album.isEmpty() ? QStringLiteral("-") : t.album);
            item->setText(3, t.durationStr.isEmpty() ? QStringLiteral("--:--") : t.durationStr);
            item->setText(4, t.bpm.isEmpty() ? QStringLiteral("-") : t.bpm);
            item->setText(5, t.musicalKey.isEmpty() ? QStringLiteral("-") : t.musicalKey);
            item->setData(0, Qt::UserRole, idx);
            ++visibleCount;
        }

        if (allTracks_.empty()) {
            auto* emptyItem = new QTreeWidgetItem(libraryTree_);
            emptyItem->setText(0, QStringLiteral("Click \"Import Folder\" to load music"));
            emptyItem->setFlags(Qt::NoItemFlags);
        } else if (visibleCount == 0) {
            auto* emptyItem = new QTreeWidgetItem(libraryTree_);
            if (activePlaylistIndex_ >= 0 && activePlaylistIndex_ < static_cast<int>(playlists_.size()))
                emptyItem->setText(0, QStringLiteral("Playlist \"%1\" is empty").arg(playlists_[activePlaylistIndex_].name));
            else
                emptyItem->setText(0, QStringLiteral("No matches for \"%1\"").arg(searchQuery_));
            emptyItem->setFlags(Qt::NoItemFlags);
        }

        // Restore selection if the previously-selected track is still visible
        if (prevSelectedTrackIdx >= 0) {
            bool reselected = false;
            for (int i = 0; i < libraryTree_->topLevelItemCount(); ++i) {
                if (libraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt() == prevSelectedTrackIdx) {
                    libraryTree_->setCurrentItem(libraryTree_->topLevelItem(i));
                    reselected = true;
                    break;
                }
            }
            if (!reselected) clearTrackDetail();
        }
        trackCountLabel_->setText(QStringLiteral("%1 tracks").arg(visibleCount));
        qInfo().noquote() << QStringLiteral("LIBRARY_RENDERED=TRUE");
        qInfo().noquote() << QStringLiteral("VISIBLE_TRACK_COUNT=%1").arg(visibleCount);
        if (!query.isEmpty()) {
            qInfo().noquote() << QStringLiteral("RESULT_COUNT=%1").arg(visibleCount);
        }
    }

    void updateTreeItemForTrack(int trackIndex)
    {
        if (!libraryTree_) return;
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        for (int i = 0; i < libraryTree_->topLevelItemCount(); ++i) {
            auto* item = libraryTree_->topLevelItem(i);
            if (item->data(0, Qt::UserRole).toInt() == trackIndex) {
                const TrackInfo& t = allTracks_[trackIndex];
                item->setText(0, t.displayName.isEmpty() ? QStringLiteral("Unknown") : t.displayName);
                item->setText(1, t.artist.isEmpty() ? QStringLiteral("-") : t.artist);
                item->setText(2, t.album.isEmpty() ? QStringLiteral("-") : t.album);
                item->setText(3, t.durationStr.isEmpty() ? QStringLiteral("--:--") : t.durationStr);
                item->setText(4, t.bpm.isEmpty() ? QStringLiteral("-") : t.bpm);
                item->setText(5, t.musicalKey.isEmpty() ? QStringLiteral("-") : t.musicalKey);
                break;
            }
        }
        // Also update detail panel if this track is selected
        if (libraryTree_->currentItem()
            && libraryTree_->currentItem()->data(0, Qt::UserRole).toInt() == trackIndex) {
            showTrackDetail(trackIndex);
        }
    }

    void showTrackDetail(int trackIndex)
    {
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        if (!detailTitleLabel_) return; // guard against pre-init calls
        const TrackInfo& t = allTracks_[trackIndex];
        detailTitleLabel_->setText(t.title.isEmpty() ? QStringLiteral("Unknown Track") : t.title);
        detailTrackTitle_->setText(t.title.isEmpty() ? QStringLiteral("-") : t.title);
        detailTrackArtist_->setText(t.artist.isEmpty() ? QStringLiteral("-") : t.artist);
        detailTrackAlbum_->setText(t.album.isEmpty() ? QStringLiteral("-") : t.album);
        detailTrackGenre_->setText(t.genre.isEmpty() ? QStringLiteral("-") : t.genre);
        detailTrackDuration_->setText(t.durationStr.isEmpty() ? QStringLiteral("--:--") : t.durationStr);
        detailTrackBpm_->setText(t.bpm.isEmpty() ? QStringLiteral("-") : t.bpm);
        detailTrackKey_->setText(t.musicalKey.isEmpty() ? QStringLiteral("-") : t.musicalKey);
        detailTrackCamelot_->setText(t.camelotKey.isEmpty() ? QStringLiteral("-") : t.camelotKey);
        detailTrackEnergy_->setText(t.energy >= 0 ? QString::number(t.energy, 'f', 1) : QStringLiteral("-"));
        detailTrackLufs_->setText(t.loudnessLUFS != 0.0
            ? QStringLiteral("%1 LUFS (range %2)").arg(QString::number(t.loudnessLUFS, 'f', 1), QString::number(t.loudnessRange, 'f', 1))
            : QStringLiteral("-"));
        {
            QString cueStr;
            if (!t.cueIn.isEmpty() || !t.cueOut.isEmpty())
                cueStr = QStringLiteral("%1 / %2").arg(t.cueIn.isEmpty() ? QStringLiteral("-") : t.cueIn,
                                                        t.cueOut.isEmpty() ? QStringLiteral("-") : t.cueOut);
            detailTrackCue_->setText(cueStr.isEmpty() ? QStringLiteral("-") : cueStr);
        }
        detailTrackDance_->setText(t.danceability >= 0 ? QString::number(t.danceability, 'f', 1) : QStringLiteral("-"));
        detailTrackSize_->setText(t.fileSize > 0 ? formatFileSize(t.fileSize) : QStringLiteral("-"));
        detailTrackPath_->setText(t.filePath);
    }

    void clearTrackDetail()
    {
        if (!detailTitleLabel_) return;
        detailTitleLabel_->setText(QStringLiteral("Track Info"));
        detailTrackTitle_->setText(QStringLiteral("-"));
        detailTrackArtist_->setText(QStringLiteral("-"));
        detailTrackAlbum_->setText(QStringLiteral("-"));
        detailTrackGenre_->setText(QStringLiteral("-"));
        detailTrackDuration_->setText(QStringLiteral("--:--"));
        detailTrackBpm_->setText(QStringLiteral("-"));
        detailTrackKey_->setText(QStringLiteral("-"));
        detailTrackCamelot_->setText(QStringLiteral("-"));
        detailTrackEnergy_->setText(QStringLiteral("-"));
        detailTrackLufs_->setText(QStringLiteral("-"));
        detailTrackCue_->setText(QStringLiteral("-"));
        detailTrackDance_->setText(QStringLiteral("-"));
        detailTrackSize_->setText(QStringLiteral("-"));
        detailTrackPath_->setText(QStringLiteral("-"));
    }

    void loadAndPlayTrack(int trackIndex)
    {
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        currentTrackIndex_ = trackIndex;
        const TrackInfo& track = allTracks_[trackIndex];

        // Update hero labels
        playerTrackLabel_->setText(track.displayName.isEmpty()
            ? QStringLiteral("Unknown Track") : track.displayName);
        playerTrackLabel_->hide(); // Hide QLabel — visualizer paints pulsing title
        playerArtistLabel_->hide();
        playerMetaLabel_->hide();
        playerStateLabel_->hide();
        upNextLabel_->hide();
        if (nowPlayingTag_) nowPlayingTag_->hide();
        visualizer_->setTitleText(track.displayName.isEmpty()
            ? QStringLiteral("Unknown Track") : track.displayName);

        // Artist + Album line
        QStringList artistParts;
        if (!track.artist.isEmpty()) artistParts << track.artist;
        if (!track.album.isEmpty()) artistParts << track.album;
        playerArtistLabel_->setText(artistParts.isEmpty()
            ? QString() : artistParts.join(QStringLiteral("  |  ")));

        // Metadata line: BPM / Key / Duration
        QStringList metaParts;
        if (!track.bpm.isEmpty()) metaParts << QStringLiteral("BPM: %1").arg(track.bpm);
        if (!track.musicalKey.isEmpty()) metaParts << QStringLiteral("Key: %1").arg(track.musicalKey);
        if (!track.durationStr.isEmpty()) metaParts << track.durationStr;
        if (!track.genre.isEmpty()) metaParts << track.genre;
        playerMetaLabel_->setText(metaParts.join(QStringLiteral("   ")));

        // Highlight in library tree
        highlightPlayerLibraryItem(trackIndex);

        // JUCE playback path — load real file into engine deck
        bridge_.stop();
        if (!juceSimpleModeReady_) {
            bridge_.enterSimpleMode();
            juceSimpleModeReady_ = true;
        }

        // Reset UI transport state to zero BEFORE loading
        if (seekSlider_) {
            seekSlider_->setRange(0, 1);
            seekSlider_->setValue(0);
        }
        if (playerTimeLabel_) playerTimeLabel_->setText(QStringLiteral("0:00"));
        if (playerTimeTotalLabel_) playerTimeTotalLabel_->setText(QStringLiteral("0:00"));

        // Pre-set UI generation so the authoritative signals from
        // loadTrack() pass the gen check in durationChanged/playheadChanged.
        uiTrackGen_ = bridge_.currentLoadGen() + 1;
        qInfo().noquote() << QStringLiteral("TRC_UI loadAndPlay IDX=%1 name=%2 uiGen=%3")
            .arg(trackIndex).arg(track.displayName).arg(uiTrackGen_);

        const bool loaded = bridge_.loadTrack(track.filePath);
        // bridge_.loadTrack incremented gen → now bridge_.currentLoadGen() == uiTrackGen_
        if (loaded) {
            bridge_.start();
            if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Pause"));
            qInfo().noquote() << QStringLiteral("TRC_UI started gen=%1 IDX=%2 name=%3 sliderMax=%4")
                .arg(uiTrackGen_).arg(trackIndex).arg(track.displayName)
                .arg(seekSlider_ ? seekSlider_->maximum() : -1);
        } else {
            qWarning().noquote() << QStringLiteral("TRC_UI loadTrack FAILED IDX=%1 name=%2")
                .arg(trackIndex).arg(track.displayName);
        }

        // Update "Up Next" label
        updateUpNextLabel();

        qInfo().noquote() << QStringLiteral("LOAD_AND_PLAY=%1 IDX=%2").arg(track.displayName).arg(trackIndex);
    }

    void playNextTrack()
    {
        if (allTracks_.empty() || !playerLibraryTree_) return;
        const int count = playerLibraryTree_->topLevelItemCount();
        if (count == 0) return;

        if (playMode_ == PlayMode::Shuffle) {
            // Pure random from visible list
            std::uniform_int_distribution<int> dist(0, count - 1);
            const int ri = dist(shuffleRng_);
            const int idx = playerLibraryTree_->topLevelItem(ri)->data(0, Qt::UserRole).toInt();
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SHUFFLE %1").arg(allTracks_[idx].displayName);
            return;
        }

        if (playMode_ == PlayMode::SmartShuffle) {
            advanceSmartShuffle();
            return;
        }

        // Linear modes: find current position, advance
        int curPos = -1;
        for (int i = 0; i < count; ++i) {
            if (playerLibraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt() == currentTrackIndex_) {
                curPos = i;
                break;
            }
        }

        int nextPos = curPos + 1;
        if (nextPos >= count) {
            if (playMode_ == PlayMode::RepeatAll) {
                nextPos = 0; // wrap
            } else {
                bridge_.stop();
                if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Play"));
                qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=END_OF_QUEUE");
                return;
            }
        }

        const int nextIdx = playerLibraryTree_->topLevelItem(nextPos)->data(0, Qt::UserRole).toInt();
        if (nextIdx >= 0 && nextIdx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(nextIdx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=%1").arg(allTracks_[nextIdx].displayName);
        }
    }

    void playPrevTrack()
    {
        if (allTracks_.empty() || !playerLibraryTree_) return;
        const int count = playerLibraryTree_->topLevelItemCount();
        if (count == 0) return;

        if (playMode_ == PlayMode::Shuffle || playMode_ == PlayMode::SmartShuffle) {
            // In shuffle modes, prev picks random (no history)
            std::uniform_int_distribution<int> dist(0, count - 1);
            const int ri = dist(shuffleRng_);
            const int idx = playerLibraryTree_->topLevelItem(ri)->data(0, Qt::UserRole).toInt();
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=SHUFFLE %1").arg(allTracks_[idx].displayName);
            return;
        }

        int curPos = -1;
        for (int i = 0; i < count; ++i) {
            if (playerLibraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt() == currentTrackIndex_) {
                curPos = i;
                break;
            }
        }

        int prevPos = curPos - 1;
        if (prevPos < 0) {
            if (playMode_ == PlayMode::RepeatAll) {
                prevPos = count - 1; // wrap
            } else {
                return; // already at start
            }
        }

        const int prevIdx = playerLibraryTree_->topLevelItem(prevPos)->data(0, Qt::UserRole).toInt();
        if (prevIdx >= 0 && prevIdx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(prevIdx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=%1").arg(allTracks_[prevIdx].displayName);
        }
    }

    void onEndOfMedia()
    {
        switch (playMode_) {
        case PlayMode::PlayOnce:
            // Stop. Do not auto-advance.
            bridge_.stop();
            if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Play"));
            if (playerTrackLabel_) playerTrackLabel_->show();
            if (playerArtistLabel_) playerArtistLabel_->show();
            if (playerMetaLabel_) playerMetaLabel_->show();
            if (playerStateLabel_) playerStateLabel_->show();
            if (upNextLabel_) upNextLabel_->show();
            if (nowPlayingTag_) nowPlayingTag_->show();
            visualizer_->setTitleText(QString());
            visualizer_->setUpNextText(QString());
            qInfo().noquote() << QStringLiteral("END_OF_MEDIA=PLAY_ONCE_STOP");
            break;
        case PlayMode::PlayInOrder:
        case PlayMode::RepeatAll:
        case PlayMode::Shuffle:
        case PlayMode::SmartShuffle:
            playNextTrack();
            break;
        }
    }

    QString playModeLabel() const
    {
        switch (playMode_) {
        case PlayMode::PlayOnce:      return QStringLiteral("Play Once");
        case PlayMode::PlayInOrder:   return QStringLiteral("In Order");
        case PlayMode::RepeatAll:     return QStringLiteral("Repeat All");
        case PlayMode::Shuffle:       return QStringLiteral("Shuffle");
        case PlayMode::SmartShuffle:  return QStringLiteral("Smart Shuffle");
        }
        return QStringLiteral("Unknown");
    }

    void updatePlayModeButton()
    {
        if (playModeBtn_)
            playModeBtn_->setText(QStringLiteral("Mode: %1").arg(playModeLabel()));
    }

    void updateUpNextLabel()
    {
        if (!upNextLabel_ || !playerLibraryTree_) return;
        const int count = playerLibraryTree_->topLevelItemCount();
        if (count == 0 || currentTrackIndex_ < 0) {
            upNextLabel_->setText(QStringLiteral("Up Next: \u2014"));
            if (visualizer_) visualizer_->setUpNextText(QString());
            return;
        }

        int nextIdx = -1;

        if (playMode_ == PlayMode::Shuffle) {
            upNextLabel_->setText(QStringLiteral("Up Next: (shuffle)"));
            if (visualizer_) visualizer_->setUpNextText(QStringLiteral("(shuffle)"));
            return;
        } else if (playMode_ == PlayMode::SmartShuffle) {
            // Show next from pool if available
            if (smartShufflePos_ >= 0 && smartShufflePos_ < static_cast<int>(smartShufflePool_.size())) {
                nextIdx = smartShufflePool_[smartShufflePos_];
            } else {
                upNextLabel_->setText(QStringLiteral("Up Next: (reshuffle)"));
                if (visualizer_) visualizer_->setUpNextText(QStringLiteral("(reshuffle)"));
                return;
            }
        } else {
            // Linear modes: find next in visible list
            for (int i = 0; i < count; ++i) {
                int idx = playerLibraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt();
                if (idx == currentTrackIndex_) {
                    if (i + 1 < count) {
                        nextIdx = playerLibraryTree_->topLevelItem(i + 1)->data(0, Qt::UserRole).toInt();
                    } else if (playMode_ == PlayMode::RepeatAll) {
                        nextIdx = playerLibraryTree_->topLevelItem(0)->data(0, Qt::UserRole).toInt();
                    }
                    break;
                }
            }
        }

        if (nextIdx >= 0 && nextIdx < static_cast<int>(allTracks_.size())) {
            const auto& t = allTracks_[nextIdx];
            QString name = t.displayName.isEmpty() ? QStringLiteral("Unknown") : t.displayName;
            QString labelName = name;
            if (!t.artist.isEmpty()) labelName = QStringLiteral("%1 \u2013 %2").arg(t.artist, name);
            upNextLabel_->setText(QStringLiteral("Up Next: %1").arg(labelName));
            if (visualizer_) visualizer_->setUpNextText(name);
        } else {
            upNextLabel_->setText(QStringLiteral("Up Next: \u2014"));
            if (visualizer_) visualizer_->setUpNextText(QString());
        }
    }

    void rebuildSmartShufflePool()
    {
        if (!playerLibraryTree_) return;
        const int count = playerLibraryTree_->topLevelItemCount();
        smartShufflePool_.clear();
        smartShufflePool_.reserve(count);
        for (int i = 0; i < count; ++i)
            smartShufflePool_.push_back(playerLibraryTree_->topLevelItem(i)->data(0, Qt::UserRole).toInt());
        std::shuffle(smartShufflePool_.begin(), smartShufflePool_.end(), shuffleRng_);
        smartShufflePos_ = 0;
        qInfo().noquote() << QStringLiteral("SMART_SHUFFLE_POOL_REBUILT=%1").arg(count);
    }

    void advanceSmartShuffle()
    {
        if (smartShufflePool_.empty() || smartShufflePos_ >= static_cast<int>(smartShufflePool_.size())) {
            rebuildSmartShufflePool();
        }
        if (smartShufflePool_.empty()) return;
        const int idx = smartShufflePool_[smartShufflePos_];
        ++smartShufflePos_;
        if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SMART_SHUFFLE %1 pos=%2/%3")
                .arg(allTracks_[idx].displayName).arg(smartShufflePos_).arg(smartShufflePool_.size());
        }
    }

    void rebuildPlayerQueue()
    {
        refreshPlayerLibrary();
    }

    void refreshPlayerLibrary()
    {
        if (!playerLibraryTree_) return;

        const QString searchText = playerSearchBar_ ? playerSearchBar_->text().trimmed().toLower() : QString();
        const int sortKey = playerSortCombo_ ? playerSortCombo_->currentIndex() : 0;

        // Build filtered index list
        std::vector<int> filtered;
        filtered.reserve(allTracks_.size());
        for (int i = 0; i < static_cast<int>(allTracks_.size()); ++i) {
            if (searchText.isEmpty()) {
                filtered.push_back(i);
                continue;
            }
            const TrackInfo& t = allTracks_[i];
            if (t.displayName.toLower().contains(searchText)
                || t.artist.toLower().contains(searchText)
                || t.album.toLower().contains(searchText)
                || t.genre.toLower().contains(searchText)
                || t.bpm.toLower().contains(searchText)
                || t.musicalKey.toLower().contains(searchText)) {
                filtered.push_back(i);
            }
        }

        // Sort
        std::sort(filtered.begin(), filtered.end(), [this, sortKey](int a, int b) {
            const TrackInfo& ta = allTracks_[a];
            const TrackInfo& tb = allTracks_[b];
            switch (sortKey) {
            case 1: return ta.artist.toLower() < tb.artist.toLower();
            case 2: return ta.album.toLower() < tb.album.toLower();
            case 3: return ta.durationMs < tb.durationMs;
            case 4: return ta.bpm.toFloat() < tb.bpm.toFloat();
            case 5: return ta.musicalKey.toLower() < tb.musicalKey.toLower();
            default: return ta.displayName.toLower() < tb.displayName.toLower();
            }
        });

        // Populate tree
        {
            QSignalBlocker blocker(playerLibraryTree_);
            playerLibraryTree_->clear();
            for (const int idx : filtered) {
                const TrackInfo& t = allTracks_[idx];
                auto* item = new QTreeWidgetItem(playerLibraryTree_);
                item->setText(0, t.displayName.isEmpty() ? QStringLiteral("Unknown") : t.displayName);
                item->setText(1, t.artist);
                item->setText(2, t.album);
                item->setText(3, t.durationStr);
                item->setText(4, t.bpm);
                item->setText(5, t.musicalKey);
                item->setData(0, Qt::UserRole, idx);
            }
        }

        highlightPlayerLibraryItem(currentTrackIndex_);

        // Invalidate Smart Shuffle pool when visible list changes
        if (playMode_ == PlayMode::SmartShuffle && !smartShufflePool_.empty()) {
            rebuildSmartShufflePool();
        }

        if (playerLibCountLabel_)
            playerLibCountLabel_->setText(QStringLiteral("%1 tracks").arg(filtered.size()));

        qInfo().noquote() << QStringLiteral("PLAYER_LIBRARY_REFRESHED=%1 SEARCH=%2 SORT=%3")
            .arg(filtered.size()).arg(searchText.isEmpty() ? QStringLiteral("(none)") : searchText).arg(sortKey);
    }

    void highlightPlayerLibraryItem(int trackIndex)
    {
        if (!playerLibraryTree_) return;
        for (int i = 0; i < playerLibraryTree_->topLevelItemCount(); ++i) {
            auto* item = playerLibraryTree_->topLevelItem(i);
            const bool isCurrent = (item->data(0, Qt::UserRole).toInt() == trackIndex);
            item->setSelected(isCurrent);
            QFont f = item->font(0);
            f.setBold(isCurrent);
            for (int c = 0; c < 6; ++c) item->setFont(c, f);
            if (isCurrent) {
                playerLibraryTree_->scrollToItem(item);
            }
        }
    }

    void requestAudioProfilesRefresh(bool logMarker)
    {
        if (QThread::currentThread() != thread()) {
            QMetaObject::invokeMethod(this, [this, logMarker]() { requestAudioProfilesRefresh(logMarker); }, Qt::QueuedConnection);
            return;
        }

        if (audioApplyInProgress_.load(std::memory_order_acquire)) {
            qInfo().noquote() << QStringLiteral("RTAudioALRefreshDeferred=TRUE");
            pendingAudioProfilesRefresh_ = true;
            pendingAudioProfilesRefreshLogMarker_ = pendingAudioProfilesRefreshLogMarker_ || logMarker;
            return;
        }

        refreshAudioProfilesUi(logMarker);
    }

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
        if (audioApplyInProgress_.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        qInfo().noquote() << QStringLiteral("RTAudioALApplyBegin=1");

        const QString profileName = audioProfileCombo_->currentData().toString();
        const auto profileIt = audioProfilesStore_.profiles.find(profileName);
        if (profileName.isEmpty() || profileIt == audioProfilesStore_.profiles.end()) {
            qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=<invalid>");
            qInfo().noquote() << QStringLiteral("RTAudioALDeviceReopen=FALSE");
            qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
            QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Selected profile is not valid."));
            finishAudioApply();
            return;
        }

        qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(profileName);
        const bool hadOpenDevice = lastTelemetry_.rtDeviceOpenOk;
        qInfo().noquote() << QStringLiteral("RTAudioALDeviceReopen=%1").arg(hadOpenDevice ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));

        pendingApplyProfileName_ = profileName;

        const UiAudioProfile& profile = profileIt->second;
        bridge_.applyAudioProfile(profile.deviceId.toStdString(),
                                  profile.deviceName.toStdString(),
                                  profile.sampleRate,
                                  profile.bufferFrames,
                                  profile.channelsOut);
        // Result arrives via audioProfileApplied signal → onAudioProfileApplied()
    }

    void onAudioProfileApplied(bool ok)
    {
        const QString profileName = pendingApplyProfileName_;
        pendingApplyProfileName_.clear();

        if (!ok) {
            qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
            QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Failed to apply selected profile."));
            finishAudioApply();
            return;
        }

        QString saveError;
        if (!writeUiAudioProfilesActiveProfile(audioProfilesStore_, profileName, saveError)) {
            qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
            QMessageBox::warning(this,
                                 QStringLiteral("Audio Profile"),
                                 QStringLiteral("Profile applied, but active_profile was not persisted: %1").arg(saveError));
            finishAudioApply();
            return;
        }

        qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=PASS");
        audioProfilesStore_.activeProfile = profileName;
        lastAkActiveProfileMarker_ = profileName;
        lastAgMarkerKey_.clear();
        finishAudioApply();
    }

    void finishAudioApply()
    {
        audioApplyInProgress_.store(false, std::memory_order_release);
        if (pendingAudioProfilesRefresh_) {
            const bool logMarker = pendingAudioProfilesRefreshLogMarker_;
            qInfo().noquote() << QStringLiteral("RTAudioALRefreshFlushed=TRUE");
            pendingAudioProfilesRefresh_ = false;
            pendingAudioProfilesRefreshLogMarker_ = false;
            QTimer::singleShot(0, this, [this, logMarker]() { requestAudioProfilesRefresh(logMarker); });
        }
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

        // ── JUCE engine status ──
        const bool juceReady = status.engineReady;
        engineStatusLabel_->setText(juceReady
            ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));

        const bool effectiveRunning = bridge_.running();
        runningLabel_->setText(effectiveRunning
            ? QStringLiteral("Running: YES") : QStringLiteral("Running: NO"));

        // Meters: JUCE engine only
        const double meterL = bridge_.meterL();
        const double meterR = bridge_.meterR();
        meterLabel_->setText(QStringLiteral("MeterL: %1  MeterR: %2")
            .arg(QString::number(meterL, 'f', 3),
                 QString::number(meterR, 'f', 3)));

        // Feed visualizer from JUCE engine meters
        if (visualizer_) {
            const float feedLevel = static_cast<float>(std::max(meterL, meterR));
            if (!meterDiagLogged_ && feedLevel > 0.0f) {
                qInfo().noquote() << QStringLiteral("DIAG_METER_FEED: L=%1 R=%2 feed=%3")
                    .arg(QString::number(meterL, 'f', 6),
                         QString::number(meterR, 'f', 6),
                         QString::number(feedLevel, 'f', 6));
                meterDiagLogged_ = true;
            }
            visualizer_->setAudioLevel(feedLevel);
        }

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
    QStackedWidget* stack_{nullptr};
    QTreeWidget* libraryTree_{nullptr};
    QComboBox* sortCombo_{nullptr};
    QComboBox* searchModeCombo_{nullptr};
    QLineEdit* searchBar_{nullptr};
    QLabel* trackCountLabel_{nullptr};
    // Detail panel labels
    QLabel* detailTitleLabel_{nullptr};
    QLabel* detailTrackTitle_{nullptr};
    QLabel* detailTrackArtist_{nullptr};
    QLabel* detailTrackAlbum_{nullptr};
    QLabel* detailTrackGenre_{nullptr};
    QLabel* detailTrackDuration_{nullptr};
    QLabel* detailTrackBpm_{nullptr};
    QLabel* detailTrackKey_{nullptr};
    QLabel* detailTrackCamelot_{nullptr};
    QLabel* detailTrackEnergy_{nullptr};
    QLabel* detailTrackLufs_{nullptr};
    QLabel* detailTrackCue_{nullptr};
    QLabel* detailTrackDance_{nullptr};
    QLabel* detailTrackSize_{nullptr};
    QLabel* detailTrackPath_{nullptr};
    QLabel* playerTrackLabel_{nullptr};
    QLabel* nowPlayingTag_{nullptr};
    double titlePulseEnvelope_{0.0};
    QLabel* playerArtistLabel_{nullptr};
    QLabel* playerMetaLabel_{nullptr};
    QLabel* playerStateLabel_{nullptr};
    QLabel* playerTimeLabel_{nullptr};
    QLabel* playerTimeTotalLabel_{nullptr};
    QSlider* seekSlider_{nullptr};
    QSlider* volumeSlider_{nullptr};
    QPushButton* playPauseBtn_{nullptr};
    QPushButton* prevBtn_{nullptr};
    QPushButton* nextBtn_{nullptr};
    QTreeWidget* playerLibraryTree_{nullptr};
    QLabel* playerLibCountLabel_{nullptr};
    QLineEdit* playerSearchBar_{nullptr};
    QComboBox* playerSortCombo_{nullptr};
    std::vector<TrackInfo> allTracks_;
    bool juceSimpleModeReady_{false};
    std::vector<Playlist> playlists_;
    int activePlaylistIndex_{-1}; // -1 = show all library
    QString importedFolderPath_;
    QString searchQuery_;
    int currentTrackIndex_{-1};
    bool seekSliderPressed_{false};
    uint64_t uiTrackGen_{0};    // must match bridge_.currentLoadGen() for UI updates

    // Play mode
    enum class PlayMode { PlayOnce, PlayInOrder, RepeatAll, Shuffle, SmartShuffle };
    PlayMode playMode_{PlayMode::PlayInOrder};
    std::vector<int> smartShufflePool_;
    int smartShufflePos_{-1};
    std::mt19937 shuffleRng_{std::random_device{}()};
    QPushButton* playModeBtn_{nullptr};

    // Visualizer / display surface
    VisualizerWidget* visualizer_{nullptr};
    QTimer* vizTimer_{nullptr};

    QPushButton* pulseBtn_{nullptr};
    QPushButton* tuneBtn_{nullptr};
    QPushButton* vizLineBtn_{nullptr};
    QPushButton* vizBarsBtn_{nullptr};
    QPushButton* vizCircleBtn_{nullptr};
    QPushButton* vizNoneBtn_{nullptr};
    QLabel* upNextLabel_{nullptr};

    // 16-band EQ panel
    EqPanel* eqPanel_{nullptr};

    // DJ mode widgets
    DeckStrip* djDeckA_{nullptr};
    DeckStrip* djDeckB_{nullptr};
    QSlider* djCrossfader_{nullptr};
    // djLibTreeA_/B_ removed — library stripped for rebuild
    LevelMeter* djMasterMeterL_{nullptr};
    LevelMeter* djMasterMeterR_{nullptr};
    QSlider* djCueMix_{nullptr};
    QSlider* djCueVol_{nullptr};
    QPushButton* djOutputModeBtn_{nullptr};
    QWidget* djDeviceLostBanner_{nullptr};
    QLabel* djBannerTitleLabel_{nullptr};
    QLabel* djRecoveryStatusLabel_{nullptr};
    QPushButton* djRecoverBtn_{nullptr};
    QTimer* djBannerDismissTimer_{nullptr};
    // djLibHighlight* removed — library stripped for rebuild
    int lastActiveDjDeck_{0};

    QLabel* engineStatusLabel_{nullptr};
    QLabel* runningLabel_{nullptr};
    QLabel* meterLabel_{nullptr};
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
    bool meterDiagLogged_{false};
    bool healthTickLogged_{false};
    bool telemetryTickLogged_{false};
    bool foundationTickLogged_{false};
    bool foundationSelfTestLogged_{false};
    std::atomic<bool> audioApplyInProgress_ { false };
    bool pendingAudioProfilesRefresh_{false};
    bool pendingAudioProfilesRefreshLogMarker_{false};
    QString lastAgMarkerKey_ {};
    QString lastAkActiveProfileMarker_ {};
    QString pendingApplyProfileName_ {};
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

    // ── Dump previous ring buffer if crash left one ──
    // On startup, check if data/runtime/trace_ring_dump.txt exists from a previous frozen session
    {
        const QString ringDumpPath = runtimePath("data/runtime/trace_ring_dump.txt");
        if (QFileInfo::exists(ringDumpPath)) {
            std::fprintf(stderr, "[AUDIO_TRACE] Previous session ring dump found at %s\n",
                         ringDumpPath.toUtf8().constData());
        }
    }

    // ── UI thread heartbeat timer ──
    // Prints to stderr every 500ms so we can detect UI thread stalls during unplug
    QTimer uiHeartbeatTimer;
    uiHeartbeatTimer.setInterval(500);
    uint64_t uiHeartbeatCount = 0;
    QObject::connect(&uiHeartbeatTimer, &QTimer::timeout, [&uiHeartbeatCount]() {
        ++uiHeartbeatCount;
        // Only log every 4th beat (~2 seconds) to keep noise down, but still detect stalls
        if ((uiHeartbeatCount % 4) == 0) {
            ngks::audioTrace("UI_HEARTBEAT", "beat=%llu", static_cast<unsigned long long>(uiHeartbeatCount));
        }
    });
    uiHeartbeatTimer.start();

    // ── Freeze-detect timer: dump ring buffer if UI heartbeat stalls ──
    QElapsedTimer uiAliveTimer;
    uiAliveTimer.start();
    QTimer freezeDetectTimer;
    freezeDetectTimer.setInterval(3000); // check every 3s 
    QObject::connect(&freezeDetectTimer, &QTimer::timeout, [&uiAliveTimer]() {
        // If this fires, the UI thread is alive (the timer ran).
        // Reset the alive timer.
        uiAliveTimer.restart();
    });
    freezeDetectTimer.start();

    MainWindow window(engineBridge);
    window.show();
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("window_show"), QJsonObject());
    window.autoShowDiagnosticsIfRequested();

    QObject::connect(&app, &QCoreApplication::aboutToQuit, [&]() {
        uiHeartbeatTimer.stop();
        freezeDetectTimer.stop();
        // Dump ring buffer on exit for post-mortem analysis
        ngks::traceRing().dumpToFile(
            runtimePath("data/runtime/trace_ring_dump.txt").toUtf8().constData());
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

    // ── Bench-load: auto-load a track for deterministic timing capture ──
    // Usage: native.exe --bench-load "C:\path\to\file.mp3"
    //        native.exe --bench-load "C:\path\to\file.mp3" --bench-deck 1
    {
        const QStringList args = QCoreApplication::arguments();
        const int blIdx = args.indexOf(QStringLiteral("--bench-load"));
        if (blIdx >= 0 && blIdx + 1 < args.size()) {
            const QString benchFile = args.at(blIdx + 1);
            int benchDeck = 0;
            const int bdIdx = args.indexOf(QStringLiteral("--bench-deck"));
            if (bdIdx >= 0 && bdIdx + 1 < args.size())
                benchDeck = args.at(bdIdx + 1).toInt();

            ngks::audioTrace("BENCH_LOAD_SCHEDULED", "deck=%d path=%s",
                             benchDeck, benchFile.toStdString().c_str());

            // Delay 2s to let audio device initialize
            QTimer::singleShot(2000, [&engineBridge, benchFile, benchDeck]() {
                ngks::audioTrace("BENCH_LOAD_FIRE", "deck=%d path=%s",
                                 benchDeck, benchFile.toStdString().c_str());
                engineBridge.loadTrackToDeck(benchDeck, benchFile);
            });

            // Second load of same file after 5s for warm-load measurement
            QTimer::singleShot(5000, [&engineBridge, benchFile, benchDeck]() {
                ngks::audioTrace("BENCH_WARM_LOAD_FIRE", "deck=%d path=%s",
                                 benchDeck, benchFile.toStdString().c_str());
                engineBridge.loadTrackToDeck(benchDeck, benchFile);
            });

            // Auto-quit after 10s
            QTimer::singleShot(10000, &app, &QCoreApplication::quit);
        }
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
    const std::filesystem::path depsPath = std::filesystem::path(gExeBaseDir) / "data" / "runtime" / "ui_deps.txt";
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
