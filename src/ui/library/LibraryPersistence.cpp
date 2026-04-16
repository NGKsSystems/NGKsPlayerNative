#include "ui/library/LibraryPersistence.h"
#include "ui/diagnostics/RuntimeLogSupport.h"

#include <QFile>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonParseError>
#include <QSaveFile>

namespace {

QString uiStatePersistPath()
{
    return runtimePath("data/runtime/ui_state.json");
}

QJsonObject loadUiStateRoot()
{
    QFile file(uiStatePersistPath());
    if (!file.exists() || !file.open(QIODevice::ReadOnly)) return {};

    QJsonParseError parseErr{};
    const QJsonDocument doc = QJsonDocument::fromJson(file.readAll(), &parseErr);
    if (parseErr.error != QJsonParseError::NoError || !doc.isObject()) return {};
    return doc.object();
}

bool saveUiStateRoot(const QJsonObject& root)
{
    QSaveFile file(uiStatePersistPath());
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) return false;
    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (file.write(payload) != payload.size()) {
        file.cancelWriting();
        return false;
    }
    return file.commit();
}

} // namespace

// ── Formatting helpers ────────────────────────────────────────────────────────
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
    if (bytes < 1024)    return QStringLiteral("%1 B").arg(bytes);
    if (bytes < 1048576) return QStringLiteral("%1 KB").arg(bytes / 1024);
    return QStringLiteral("%1 MB").arg(QString::number(static_cast<double>(bytes) / 1048576.0, 'f', 1));
}

// ── saveLibraryJson ───────────────────────────────────────────────────────────
bool saveLibraryJson(const std::vector<TrackInfo>& tracks, const QString& folderPath)
{
    QJsonArray arr;
    for (const TrackInfo& t : tracks) {
        QJsonObject obj;
        if (t.mediaId > 0)              obj.insert(QStringLiteral("mediaId"),          t.mediaId);
        if (!t.fileFingerprint.isEmpty()) obj.insert(QStringLiteral("fileFingerprint"), t.fileFingerprint);
        obj.insert(QStringLiteral("filePath"),    t.filePath);
        obj.insert(QStringLiteral("title"),       t.title);
        obj.insert(QStringLiteral("artist"),      t.artist);
        obj.insert(QStringLiteral("album"),       t.album);
        obj.insert(QStringLiteral("displayName"), t.displayName);
        obj.insert(QStringLiteral("durationMs"),  t.durationMs);
        obj.insert(QStringLiteral("durationStr"), t.durationStr);
        obj.insert(QStringLiteral("bpm"),         t.bpm);
        obj.insert(QStringLiteral("musicalKey"),  t.musicalKey);
        obj.insert(QStringLiteral("fileSize"),    t.fileSize);
        if (!t.genre.isEmpty())         obj.insert(QStringLiteral("genre"),             t.genre);
        if (!t.camelotKey.isEmpty())    obj.insert(QStringLiteral("camelotKey"),        t.camelotKey);
        if (t.energy >= 0)              obj.insert(QStringLiteral("energy"),            t.energy);
        if (t.loudnessLUFS != 0.0)      obj.insert(QStringLiteral("loudnessLUFS"),      t.loudnessLUFS);
        if (t.loudnessRange != 0.0)     obj.insert(QStringLiteral("loudnessRange"),     t.loudnessRange);
        if (!t.cueIn.isEmpty())         obj.insert(QStringLiteral("cueIn"),             t.cueIn);
        if (!t.cueOut.isEmpty())        obj.insert(QStringLiteral("cueOut"),            t.cueOut);
        if (t.danceability >= 0)        obj.insert(QStringLiteral("danceability"),      t.danceability);
        if (t.acousticness >= 0)        obj.insert(QStringLiteral("acousticness"),      t.acousticness);
        if (t.instrumentalness >= 0)    obj.insert(QStringLiteral("instrumentalness"),  t.instrumentalness);
        if (t.liveness >= 0)            obj.insert(QStringLiteral("liveness"),          t.liveness);
        if (t.year > 0)                 obj.insert(QStringLiteral("year"),              t.year);
        if (t.rating > 0)               obj.insert(QStringLiteral("rating"),            t.rating);
        if (!t.comments.isEmpty())      obj.insert(QStringLiteral("comments"),          t.comments);
        if (t.legacyImported)           obj.insert(QStringLiteral("legacyImported"),    true);
        if (!t.regularAnalysisState.isEmpty()) obj.insert(QStringLiteral("regularAnalysisState"), t.regularAnalysisState);
        if (!t.regularAnalysisJson.isEmpty())  obj.insert(QStringLiteral("regularAnalysisJson"),  t.regularAnalysisJson);
        if (!t.liveAnalysisState.isEmpty())    obj.insert(QStringLiteral("liveAnalysisState"),    t.liveAnalysisState);
        if (!t.liveAnalysisJson.isEmpty())     obj.insert(QStringLiteral("liveAnalysisJson"),     t.liveAnalysisJson);
        arr.append(obj);
    }
    QJsonObject root;
    root.insert(QStringLiteral("version"),    1);
    root.insert(QStringLiteral("folderPath"), folderPath);
    root.insert(QStringLiteral("tracks"),     arr);

    QSaveFile file(kLibraryPersistPath());
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) return false;
    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (file.write(payload) != payload.size()) { file.cancelWriting(); return false; }
    return file.commit();
}

// ── loadLibraryJson ───────────────────────────────────────────────────────────
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
        t.mediaId            = obj.value(QStringLiteral("mediaId")).toInteger(0);
        t.fileFingerprint    = obj.value(QStringLiteral("fileFingerprint")).toString();
        t.filePath           = obj.value(QStringLiteral("filePath")).toString();
        t.title              = obj.value(QStringLiteral("title")).toString();
        t.artist             = obj.value(QStringLiteral("artist")).toString();
        t.album              = obj.value(QStringLiteral("album")).toString();
        t.displayName        = obj.value(QStringLiteral("displayName")).toString();
        t.durationMs         = obj.value(QStringLiteral("durationMs")).toInteger(0);
        t.durationStr        = obj.value(QStringLiteral("durationStr")).toString();
        t.bpm                = obj.value(QStringLiteral("bpm")).toString();
        t.musicalKey         = obj.value(QStringLiteral("musicalKey")).toString();
        t.fileSize           = obj.value(QStringLiteral("fileSize")).toInteger(0);
        t.genre              = obj.value(QStringLiteral("genre")).toString();
        t.camelotKey         = obj.value(QStringLiteral("camelotKey")).toString();
        t.energy             = obj.value(QStringLiteral("energy")).toDouble(-1.0);
        t.loudnessLUFS       = obj.value(QStringLiteral("loudnessLUFS")).toDouble(0.0);
        t.loudnessRange      = obj.value(QStringLiteral("loudnessRange")).toDouble(0.0);
        t.cueIn              = obj.value(QStringLiteral("cueIn")).toString();
        t.cueOut             = obj.value(QStringLiteral("cueOut")).toString();
        t.danceability       = obj.value(QStringLiteral("danceability")).toDouble(-1.0);
        t.acousticness       = obj.value(QStringLiteral("acousticness")).toDouble(-1.0);
        t.instrumentalness   = obj.value(QStringLiteral("instrumentalness")).toDouble(-1.0);
        t.liveness           = obj.value(QStringLiteral("liveness")).toDouble(-1.0);
        t.year               = obj.value(QStringLiteral("year")).toInt(0);
        t.rating             = obj.value(QStringLiteral("rating")).toInt(0);
        t.comments           = obj.value(QStringLiteral("comments")).toString();
        t.legacyImported     = obj.value(QStringLiteral("legacyImported")).toBool(false);
        t.regularAnalysisState = obj.value(QStringLiteral("regularAnalysisState")).toString();
        t.regularAnalysisJson  = obj.value(QStringLiteral("regularAnalysisJson")).toString();
        t.liveAnalysisState    = obj.value(QStringLiteral("liveAnalysisState")).toString();
        t.liveAnalysisJson     = obj.value(QStringLiteral("liveAnalysisJson")).toString();
        if (t.filePath.isEmpty()) continue;
        outTracks.push_back(std::move(t));
    }
    return !outTracks.empty();
}

// ── savePlaylists ─────────────────────────────────────────────────────────────
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
    root.insert(QStringLiteral("version"),   1);
    root.insert(QStringLiteral("playlists"), arr);
    QSaveFile file(kPlaylistsPersistPath());
    if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) return false;
    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (file.write(payload) != payload.size()) { file.cancelWriting(); return false; }
    return file.commit();
}

// ── loadPlaylists ─────────────────────────────────────────────────────────────
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

bool saveUiStateBlob(const QString& key, const QByteArray& state)
{
    if (key.trimmed().isEmpty()) return false;

    QJsonObject root = loadUiStateRoot();
    root.insert(key, QString::fromLatin1(state.toBase64()));
    return saveUiStateRoot(root);
}

bool loadUiStateBlob(const QString& key, QByteArray& outState)
{
    outState.clear();
    if (key.trimmed().isEmpty()) return false;

    const QJsonObject root = loadUiStateRoot();
    const QString encoded = root.value(key).toString();
    if (encoded.isEmpty()) return false;

    outState = QByteArray::fromBase64(encoded.toLatin1());
    return !outState.isEmpty();
}
