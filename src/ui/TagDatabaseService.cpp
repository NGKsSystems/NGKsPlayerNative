#include "TagDatabaseService.h"
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>
#include <QFileInfo>
#include <QDir>
#include <QDebug>
#include <QStandardPaths>

TagDatabaseService::TagDatabaseService(QObject* parent)
    : QObject(parent)
{
}

TagDatabaseService::~TagDatabaseService()
{
    if (!overlayConn_.isEmpty())
        QSqlDatabase::removeDatabase(overlayConn_);
    if (!libraryConn_.isEmpty())
        QSqlDatabase::removeDatabase(libraryConn_);
}

bool TagDatabaseService::init(const QString& overlayDbPath, const QString& libraryDbPath)
{
    // ── 1. Overlay DB (read-write) ─────────────────────────────────
    overlayConn_ = QStringLiteral("ngks_tag_overlay");
    {
        QFileInfo fi(overlayDbPath);
        QDir().mkpath(fi.absolutePath());

        QSqlDatabase db = QSqlDatabase::addDatabase(
            QStringLiteral("QSQLITE"), overlayConn_);
        db.setDatabaseName(overlayDbPath);

        if (!db.open()) {
            qDebug() << "[TAG_DB] OVERLAY_INIT_FAIL" << db.lastError().text();
            return false;
        }
        qDebug() << "[TAG_DB] OVERLAY_INIT_OK" << overlayDbPath;

        if (!ensureOverlaySchema()) {
            qDebug() << "[TAG_DB] OVERLAY_SCHEMA_FAIL";
            return false;
        }
    }

    // ── 2. Library DB (read-only, from existing NGKsPlayer) ────────
    libraryConn_ = QStringLiteral("ngks_library_ro");
    {
        QFileInfo fi(libraryDbPath);
        if (fi.exists()) {
            QSqlDatabase db = QSqlDatabase::addDatabase(
                QStringLiteral("QSQLITE"), libraryConn_);
            db.setDatabaseName(libraryDbPath);
            db.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));

            if (db.open()) {
                // Quick sanity: does tracks table exist?
                QSqlQuery q(db);
                if (q.exec(QStringLiteral("SELECT COUNT(*) FROM tracks")) && q.next()) {
                    int rows = q.value(0).toInt();
                    qDebug() << "[TAG_DB] LIBRARY_INIT_OK" << libraryDbPath
                             << "tracks=" << rows;
                    libraryAvailable_ = true;
                } else {
                    qDebug() << "[TAG_DB] LIBRARY_NO_TRACKS_TABLE" << libraryDbPath;
                }
            } else {
                qDebug() << "[TAG_DB] LIBRARY_OPEN_FAIL" << db.lastError().text();
            }
        } else {
            qDebug() << "[TAG_DB] LIBRARY_NOT_FOUND" << libraryDbPath;
        }
    }

    return true;
}

bool TagDatabaseService::ensureOverlaySchema()
{
    QSqlDatabase db = QSqlDatabase::database(overlayConn_);
    QSqlQuery q(db);

    const QString ddl = QStringLiteral(
        "CREATE TABLE IF NOT EXISTS track_overlay ("
        "  file_path     TEXT PRIMARY KEY,"
        "  bpm           TEXT,"
        "  musical_key   TEXT,"
        "  comments      TEXT,"
        "  rating        INTEGER DEFAULT 0,"
        "  labels        TEXT,"
        "  color_label   TEXT,"
        "  dj_notes      TEXT,"
        "  cue_in        TEXT,"
        "  cue_out       TEXT,"
        "  energy        REAL DEFAULT -1,"
        "  loudness_lufs REAL DEFAULT 0,"
        "  loudness_range REAL DEFAULT 0,"
        "  danceability  REAL DEFAULT -1,"
        "  acousticness  REAL DEFAULT -1,"
        "  instrumentalness REAL DEFAULT -1,"
        "  liveness      REAL DEFAULT -1,"
        "  camelot_key   TEXT,"
        "  scan_ts       TEXT"
        ")"
    );

    if (!q.exec(ddl)) {
        qDebug() << "[TAG_DB] DDL_FAIL" << q.lastError().text();
        return false;
    }

    qDebug() << "[TAG_DB] OVERLAY_SCHEMA_OK";
    return true;
}

QString TagDatabaseService::canonicalPath(const QString& filePath) const
{
    QFileInfo fi(filePath);
    QString c = fi.canonicalFilePath();
    return c.isEmpty() ? filePath : c;
}

// ── Column mapping for overlay DB ──────────────────────────────────

static const QHash<QString, QString>& fieldToOverlayCol()
{
    static const QHash<QString, QString> m{
        {QStringLiteral("bpm"),           QStringLiteral("bpm")},
        {QStringLiteral("key"),           QStringLiteral("musical_key")},
        {QStringLiteral("comments"),      QStringLiteral("comments")},
        {QStringLiteral("rating"),        QStringLiteral("rating")},
        {QStringLiteral("labels"),        QStringLiteral("labels")},
        {QStringLiteral("colorLabel"),    QStringLiteral("color_label")},
        {QStringLiteral("djNotes"),       QStringLiteral("dj_notes")},
        {QStringLiteral("cueIn"),         QStringLiteral("cue_in")},
        {QStringLiteral("cueOut"),        QStringLiteral("cue_out")},
        {QStringLiteral("energy"),        QStringLiteral("energy")},
        {QStringLiteral("loudness"),      QStringLiteral("loudness_lufs")},
        {QStringLiteral("lra"),           QStringLiteral("loudness_range")},
        {QStringLiteral("danceability"),  QStringLiteral("danceability")},
        {QStringLiteral("acousticness"),  QStringLiteral("acousticness")},
        {QStringLiteral("instrumentalness"), QStringLiteral("instrumentalness")},
        {QStringLiteral("liveness"),      QStringLiteral("liveness")},
        {QStringLiteral("camelot"),       QStringLiteral("camelot_key")},
        {QStringLiteral("scanTimestamp"), QStringLiteral("scan_ts")},
    };
    return m;
}

// ── Load from existing library.db tracks table ─────────────────────

QHash<QString, QVariant> TagDatabaseService::loadFromLibrary(const QString& filePath)
{
    QHash<QString, QVariant> result;
    if (!libraryAvailable_) return result;

    QSqlDatabase db = QSqlDatabase::database(libraryConn_);
    QSqlQuery q(db);

    // The existing library uses filePath column, stored with backslashes on Windows
    q.prepare(QStringLiteral(
        "SELECT bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange,"
        "       danceability, acousticness, instrumentalness, liveness,"
        "       cueIn, cueOut, rating, comments, labels, color,"
        "       genre, rawBpm, bpmNote, loudness, gainRecommendation"
        " FROM tracks WHERE filePath = ?"));
    q.addBindValue(filePath);

    if (!q.exec()) {
        qDebug() << "[TAG_DB] LIBRARY_QUERY_FAIL" << q.lastError().text();
        return result;
    }

    if (!q.next()) {
        // Try with opposite slash direction
        QString alt = filePath;
        if (filePath.contains(QLatin1Char('/'))) {
            alt.replace(QLatin1Char('/'), QLatin1Char('\\'));
        } else {
            alt.replace(QLatin1Char('\\'), QLatin1Char('/'));
        }
        q.prepare(QStringLiteral(
            "SELECT bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange,"
            "       danceability, acousticness, instrumentalness, liveness,"
            "       cueIn, cueOut, rating, comments, labels, color,"
            "       genre, rawBpm, bpmNote, loudness, gainRecommendation"
            " FROM tracks WHERE filePath = ?"));
        q.addBindValue(alt);
        if (!q.exec() || !q.next()) {
            qDebug() << "[TAG_DB] LIBRARY_NO_RECORD" << filePath;
            return result;
        }
    }

    // Map library columns to our field names
    auto add = [&](const QString& field, int idx) {
        QVariant v = q.value(idx);
        if (!v.isNull() && v.toString() != QLatin1String("") &&
            v.toString() != QLatin1String("0") && v.toDouble() != -1.0) {
            result.insert(field, v);
        }
    };

    add(QStringLiteral("bpm"),               0);
    add(QStringLiteral("key"),               1);
    add(QStringLiteral("camelot"),            2);
    add(QStringLiteral("energy"),             3);
    add(QStringLiteral("loudness"),           4);  // loudnessLUFS
    add(QStringLiteral("lra"),               5);  // loudnessRange
    add(QStringLiteral("danceability"),       6);
    add(QStringLiteral("acousticness"),       7);
    add(QStringLiteral("instrumentalness"),   8);
    add(QStringLiteral("liveness"),           9);
    add(QStringLiteral("cueIn"),             10);
    add(QStringLiteral("cueOut"),            11);
    add(QStringLiteral("rating"),            12);
    add(QStringLiteral("comments"),          13);
    add(QStringLiteral("labels"),            14);
    add(QStringLiteral("colorLabel"),        15);

    qDebug() << "[TAG_DB] LIBRARY_LOAD_OK" << filePath << "fields=" << result.size();
    return result;
}

// ── Load from overlay DB ───────────────────────────────────────────

QHash<QString, QVariant> TagDatabaseService::loadFromOverlay(const QString& filePath)
{
    QHash<QString, QVariant> result;
    const QString canon = canonicalPath(filePath);

    QSqlDatabase db = QSqlDatabase::database(overlayConn_);
    QSqlQuery q(db);
    q.prepare(QStringLiteral("SELECT * FROM track_overlay WHERE file_path = ?"));
    q.addBindValue(canon);

    if (!q.exec() || !q.next()) {
        return result;
    }

    const auto& map = fieldToOverlayCol();
    for (auto it = map.constBegin(); it != map.constEnd(); ++it) {
        QVariant val = q.value(it.value());
        if (!val.isNull()) {
            result.insert(it.key(), val);
        }
    }

    qDebug() << "[TAG_DB] OVERLAY_LOAD_OK" << canon << "fields=" << result.size();
    return result;
}

// ── Public load: library first, then overlay wins ──────────────────

QHash<QString, QVariant> TagDatabaseService::loadByFile(const QString& filePath)
{
    // Start with library data
    QHash<QString, QVariant> result = loadFromLibrary(filePath);

    // Overlay data wins (user edits override library)
    QHash<QString, QVariant> overlay = loadFromOverlay(filePath);
    for (auto it = overlay.constBegin(); it != overlay.constEnd(); ++it) {
        result.insert(it.key(), it.value());  // overwrites library value
    }

    qDebug() << "[TAG_DB] MERGED_LOAD" << filePath
             << "library=" << (result.size() - overlay.size())
             << "overlay=" << overlay.size()
             << "total=" << result.size();
    return result;
}

// ── Save single field ──────────────────────────────────────────────

bool TagDatabaseService::saveField(const QString& filePath,
                                   const QString& fieldName,
                                   const QVariant& value)
{
    QHash<QString, QVariant> h;
    h.insert(fieldName, value);
    return saveBulk(filePath, h);
}

// ── Bulk save (overlay DB only) ────────────────────────────────────

bool TagDatabaseService::saveBulk(const QString& filePath,
                                  const QHash<QString, QVariant>& fields)
{
    if (fields.isEmpty()) return true;

    const QString canon = canonicalPath(filePath);
    const auto& colMap = fieldToOverlayCol();

    QSqlDatabase db = QSqlDatabase::database(overlayConn_);

    // UPSERT: ensure the row exists
    {
        QSqlQuery ins(db);
        ins.prepare(QStringLiteral(
            "INSERT OR IGNORE INTO track_overlay (file_path) VALUES (?)"));
        ins.addBindValue(canon);
        if (!ins.exec()) {
            qDebug() << "[TAG_DB] UPSERT_INSERT_FAIL" << ins.lastError().text();
            return false;
        }
    }

    // UPDATE each field
    for (auto it = fields.constBegin(); it != fields.constEnd(); ++it) {
        auto colIt = colMap.constFind(it.key());
        if (colIt == colMap.constEnd()) {
            qDebug() << "[TAG_DB] SAVE_SKIP unknown field" << it.key();
            continue;
        }
        const QString col = colIt.value();

        QSqlQuery upd(db);
        upd.prepare(QStringLiteral("UPDATE track_overlay SET %1 = ? WHERE file_path = ?").arg(col));
        upd.addBindValue(it.value());
        upd.addBindValue(canon);
        if (!upd.exec()) {
            qDebug() << "[TAG_DB] SAVE_FIELD_FAIL" << col << upd.lastError().text();
            return false;
        }
        qDebug() << "[TAG_DB] FIELD_SAVE_TARGET" << it.key() << "->" << col << "val=" << it.value();
    }

    qDebug() << "[TAG_DB] SAVE_OK" << canon << "fields=" << fields.size();
    return true;
}

bool TagDatabaseService::hasRecord(const QString& filePath) const
{
    const QString canon = const_cast<TagDatabaseService*>(this)->canonicalPath(filePath);
    QSqlDatabase db = QSqlDatabase::database(overlayConn_);
    QSqlQuery q(db);
    q.prepare(QStringLiteral("SELECT 1 FROM track_overlay WHERE file_path = ?"));
    q.addBindValue(canon);
    if (!q.exec()) return false;
    return q.next();
}
