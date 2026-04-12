#include "ui/library/LegacyLibraryImport.h"

#include <QDir>
#include <QFile>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <map>

// ── normalizePath ─────────────────────────────────────────────────────────────
QString normalizePath(const QString& raw)
{
    return QDir::fromNativeSeparators(raw).trimmed().toLower();
}

// ── findLegacyDbPath ──────────────────────────────────────────────────────────
QString findLegacyDbPath()
{
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

// ── importLegacyDb ────────────────────────────────────────────────────────────
LegacyImportResult importLegacyDb(std::vector<TrackInfo>& tracks, const QString& dbPath)
{
    LegacyImportResult result;
    result.dbPath = dbPath;

    if (dbPath.isEmpty() || !QFile::exists(dbPath)) return result;

    const QString connName = QStringLiteral("legacyImport");
    {
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
        db.setDatabaseName(dbPath);
        db.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
        if (!db.open()) {
            qWarning().noquote() << QStringLiteral("LEGACY_DB_OPEN_FAIL=%1").arg(db.lastError().text());
            return result;
        }

        std::map<QString, size_t> pathIndex;
        for (size_t i = 0; i < tracks.size(); ++i)
            pathIndex[normalizePath(tracks[i].filePath)] = i;

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
            const QString normalized = normalizePath(q.value(0).toString());
            auto it = pathIndex.find(normalized);
            if (it == pathIndex.end()) { ++result.unmatched; continue; }

            TrackInfo& t = tracks[it->second];
            ++result.matched;
            t.legacyImported = true;

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

            if (t.energy < 0) { const double v = q.value(5).toDouble(); if (v > 0) t.energy = v; }
            if (t.loudnessLUFS  == 0.0) t.loudnessLUFS  = q.value(6).toDouble();
            if (t.loudnessRange == 0.0) t.loudnessRange = q.value(7).toDouble();
            if (t.cueIn.isEmpty())  t.cueIn  = q.value(8).toString().trimmed();
            if (t.cueOut.isEmpty()) t.cueOut = q.value(9).toString().trimmed();

            if (t.danceability     < 0) { const double v = q.value(10).toDouble(); if (v > 0) t.danceability     = v; }
            if (t.acousticness     < 0) { const double v = q.value(11).toDouble(); if (v > 0) t.acousticness     = v; }
            if (t.instrumentalness < 0) { const double v = q.value(12).toDouble(); if (v > 0) t.instrumentalness = v; }
            if (t.liveness         < 0) { const double v = q.value(13).toDouble(); if (v > 0) t.liveness         = v; }

            if (t.year    == 0)   t.year    = q.value(14).toInt();
            if (t.rating  == 0)   t.rating  = q.value(15).toInt();
            if (t.comments.isEmpty()) t.comments = q.value(16).toString().trimmed();
            if (t.album.isEmpty())    t.album    = q.value(17).toString().trimmed();
            if (t.artist.isEmpty())   t.artist   = q.value(18).toString().trimmed();
            if (t.title.isEmpty())    t.title    = q.value(19).toString().trimmed();
        }
        db.close();
    }
    QSqlDatabase::removeDatabase(connName);
    return result;
}

