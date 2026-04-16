#include "ui/library/DjLibraryDatabase.h"

#include <QDir>
#include <QFileInfo>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QUuid>
#include <QVariant>
#include <QtDebug>

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────
static inline QString qs(const char* s) { return QLatin1String(s); }

static QString normalizeLibraryPath(const QString& path)
{
    if (path.trimmed().isEmpty()) return QString();

    QFileInfo info(path);
    QString normalized = info.exists()
        ? info.absoluteFilePath()
        : QDir(path).absolutePath();
    normalized = QDir::cleanPath(QDir::fromNativeSeparators(normalized));
#ifdef Q_OS_WIN
    normalized = normalized.toLower();
#endif
    return normalized;
}

static bool ensureColumnExists(QSqlDatabase& db,
                               const QString& tableName,
                               const QString& columnName,
                               const QString& definition)
{
    QSqlQuery pragma(db);
    if (!pragma.exec(QStringLiteral("PRAGMA table_info(%1);").arg(tableName))) {
        qWarning().noquote() << qs("DjLibraryDatabase: PRAGMA table_info failed") << pragma.lastError().text();
        return false;
    }

    while (pragma.next()) {
        if (pragma.value(1).toString().compare(columnName, Qt::CaseInsensitive) == 0) {
            return true;
        }
    }

    QSqlQuery alter(db);
    const QString sql = QStringLiteral("ALTER TABLE %1 ADD COLUMN %2 %3;")
        .arg(tableName, columnName, definition);
    if (!alter.exec(sql)) {
        qWarning().noquote() << qs("DjLibraryDatabase: ALTER TABLE failed") << alter.lastError().text();
        return false;
    }
    return true;
}

DjLibraryDatabase::~DjLibraryDatabase()
{
    close();
}

bool DjLibraryDatabase::open(const QString& dbPath)
{
    if (open_) close();

    connName_ = qs("DjLibDb_") + QUuid::createUuid().toString(QUuid::WithoutBraces);
    db_ = QSqlDatabase::addDatabase(qs("QSQLITE"), connName_);
    db_.setDatabaseName(dbPath);

    if (!db_.open()) {
        qWarning().noquote() << qs("DjLibraryDatabase::open FAIL") << db_.lastError().text();
        QSqlDatabase::removeDatabase(connName_);
        return false;
    }

    if (!createSchema()) {
        qWarning().noquote() << qs("DjLibraryDatabase::createSchema FAIL");
        db_.close();
        QSqlDatabase::removeDatabase(connName_);
        return false;
    }

    open_ = true;
    qInfo().noquote() << qs("DjLibraryDatabase: opened") << dbPath;
    return true;
}

void DjLibraryDatabase::close()
{
    if (!open_) return;
    db_.close();
    db_ = QSqlDatabase();
    QSqlDatabase::removeDatabase(connName_);
    open_ = false;
}

bool DjLibraryDatabase::createSchema()
{
    QSqlQuery q(db_);
    const bool ok = q.exec(qs(
        "CREATE TABLE IF NOT EXISTS library_tracks ("
        "  track_id      INTEGER PRIMARY KEY,"
        "  media_id      INTEGER NOT NULL DEFAULT 0,"
        "  file_fingerprint TEXT NOT NULL DEFAULT '',"
        "  file_path     TEXT    NOT NULL,"
        "  display_name  TEXT    NOT NULL DEFAULT '',"
        "  title         TEXT    NOT NULL DEFAULT '',"
        "  artist        TEXT    NOT NULL DEFAULT '',"
        "  album         TEXT    NOT NULL DEFAULT '',"
        "  genre         TEXT    NOT NULL DEFAULT '',"
        "  duration_ms   INTEGER NOT NULL DEFAULT 0,"
        "  duration_str  TEXT    NOT NULL DEFAULT '',"
        "  bpm           TEXT    NOT NULL DEFAULT '',"
        "  musical_key   TEXT    NOT NULL DEFAULT '',"
        "  camelot_key   TEXT    NOT NULL DEFAULT '',"
        "  energy        REAL    NOT NULL DEFAULT -1,"
        "  loudness_lufs REAL    NOT NULL DEFAULT 0,"
        "  file_size     INTEGER NOT NULL DEFAULT 0,"
        "  cue_in        TEXT    NOT NULL DEFAULT '',"
        "  cue_out       TEXT    NOT NULL DEFAULT '',"
        "  danceability  REAL    NOT NULL DEFAULT -1,"
        "  year_tag      INTEGER NOT NULL DEFAULT 0,"
        "  rating        INTEGER NOT NULL DEFAULT 0,"
        "  comments      TEXT    NOT NULL DEFAULT '',"
        "  legacy_imported INTEGER NOT NULL DEFAULT 0,"
        "  regular_analysis_state TEXT NOT NULL DEFAULT '',"
        "  regular_analysis_json  TEXT NOT NULL DEFAULT '',"
        "  live_analysis_state    TEXT NOT NULL DEFAULT '',"
        "  live_analysis_json     TEXT NOT NULL DEFAULT ''"
        ");"
    ));
    if (!ok) {
        qWarning().noquote() << qs("DjLibraryDatabase: CREATE TABLE failed") << q.lastError().text();
        return false;
    }

    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_display ON library_tracks(display_name COLLATE NOCASE);"));
    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_artist  ON library_tracks(artist       COLLATE NOCASE);"));
    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_album   ON library_tracks(album        COLLATE NOCASE);"));
    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_media   ON library_tracks(media_id);"));
    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_fingerprint ON library_tracks(file_fingerprint);"));
    q.exec(qs("CREATE INDEX IF NOT EXISTS idx_lib_path    ON library_tracks(file_path);"));
    return ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("media_id"), QStringLiteral("INTEGER NOT NULL DEFAULT 0"))
        && ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("file_fingerprint"), QStringLiteral("TEXT NOT NULL DEFAULT ''"))
        && ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("regular_analysis_state"), QStringLiteral("TEXT NOT NULL DEFAULT ''"))
        && ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("regular_analysis_json"), QStringLiteral("TEXT NOT NULL DEFAULT ''"))
        && ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("live_analysis_state"), QStringLiteral("TEXT NOT NULL DEFAULT ''"))
        && ensureColumnExists(db_, QStringLiteral("library_tracks"), QStringLiteral("live_analysis_json"), QStringLiteral("TEXT NOT NULL DEFAULT ''"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Write
// ─────────────────────────────────────────────────────────────────────────────
bool DjLibraryDatabase::bulkInsert(const std::vector<TrackInfo>& tracks)
{
    if (!open_) return false;

    db_.transaction();

    QSqlQuery del(db_);
    del.exec(qs("DELETE FROM library_tracks;"));

    QSqlQuery ins(db_);
    ins.prepare(qs(
        "INSERT INTO library_tracks "
        "(track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        " duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        " loudness_lufs, file_size, cue_in, cue_out, danceability,"
        " year_tag, rating, comments, legacy_imported,"
        " regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json)"
        " VALUES "
        "(:tid, :mid, :ffp, :fp, :dn, :ti, :ar, :al, :ge,"
        " :dm, :ds, :bp, :mk, :ck, :en,"
        " :ll, :fs, :ci, :co, :da,"
        " :yr, :rt, :cm, :li, :ras, :raj, :las, :laj);"
    ));

    // Qt binds null QStrings as SQL NULL which violates NOT NULL constraints
    // even when the column has DEFAULT ''. Coerce null to empty string.
    auto nn = [](const QString& s) -> QString {
        return s.isNull() ? QLatin1String("") : s;
    };

    for (int i = 0; i < static_cast<int>(tracks.size()); ++i) {
        const TrackInfo& t = tracks[static_cast<size_t>(i)];
        ins.bindValue(qs(":tid"), i);
        ins.bindValue(qs(":mid"), static_cast<qint64>(t.mediaId));
        ins.bindValue(qs(":ffp"), nn(t.fileFingerprint));
        ins.bindValue(qs(":fp"),  nn(normalizeLibraryPath(t.filePath)));
        ins.bindValue(qs(":dn"),  nn(t.displayName));
        ins.bindValue(qs(":ti"),  nn(t.title));
        ins.bindValue(qs(":ar"),  nn(t.artist));
        ins.bindValue(qs(":al"),  nn(t.album));
        ins.bindValue(qs(":ge"),  nn(t.genre));
        ins.bindValue(qs(":dm"),  static_cast<qint64>(t.durationMs));
        ins.bindValue(qs(":ds"),  nn(t.durationStr));
        ins.bindValue(qs(":bp"),  nn(t.bpm));
        ins.bindValue(qs(":mk"),  nn(t.musicalKey));
        ins.bindValue(qs(":ck"),  nn(t.camelotKey));
        ins.bindValue(qs(":en"),  t.energy);
        ins.bindValue(qs(":ll"),  t.loudnessLUFS);
        ins.bindValue(qs(":fs"),  static_cast<qint64>(t.fileSize));
        ins.bindValue(qs(":ci"),  nn(t.cueIn));
        ins.bindValue(qs(":co"),  nn(t.cueOut));
        ins.bindValue(qs(":da"),  t.danceability);
        ins.bindValue(qs(":yr"),  t.year);
        ins.bindValue(qs(":rt"),  t.rating);
        ins.bindValue(qs(":cm"),  nn(t.comments));
        ins.bindValue(qs(":li"),  t.legacyImported ? 1 : 0);
        ins.bindValue(qs(":ras"), nn(t.regularAnalysisState));
        ins.bindValue(qs(":raj"), nn(t.regularAnalysisJson));
        ins.bindValue(qs(":las"), nn(t.liveAnalysisState));
        ins.bindValue(qs(":laj"), nn(t.liveAnalysisJson));
        if (!ins.exec()) {
            qWarning().noquote() << qs("DjLibraryDatabase::bulkInsert row") << i << ins.lastError().text();
        }
    }

    const bool ok = db_.commit();
    qInfo().noquote() << qs("DjLibraryDatabase::bulkInsert inserted=") << tracks.size() << qs("ok=") << ok;
    return ok;
}

bool DjLibraryDatabase::upsertTrack(qint64 trackId, const TrackInfo& t)
{
    if (!open_) return false;
    QSqlQuery q(db_);
    q.prepare(qs(
        "INSERT OR REPLACE INTO library_tracks "
        "(track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        " duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        " loudness_lufs, file_size, cue_in, cue_out, danceability,"
        " year_tag, rating, comments, legacy_imported,"
        " regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json)"
        " VALUES "
        "(:tid, :mid, :ffp, :fp, :dn, :ti, :ar, :al, :ge,"
        " :dm, :ds, :bp, :mk, :ck, :en,"
        " :ll, :fs, :ci, :co, :da,"
        " :yr, :rt, :cm, :li, :ras, :raj, :las, :laj);"
    ));
    q.bindValue(qs(":tid"), trackId);
    q.bindValue(qs(":mid"), static_cast<qint64>(t.mediaId));
    q.bindValue(qs(":ffp"), t.fileFingerprint);
    q.bindValue(qs(":fp"),  normalizeLibraryPath(t.filePath));
    q.bindValue(qs(":dn"),  t.displayName);
    q.bindValue(qs(":ti"),  t.title);
    q.bindValue(qs(":ar"),  t.artist);
    q.bindValue(qs(":al"),  t.album);
    q.bindValue(qs(":ge"),  t.genre);
    q.bindValue(qs(":dm"),  static_cast<qint64>(t.durationMs));
    q.bindValue(qs(":ds"),  t.durationStr);
    q.bindValue(qs(":bp"),  t.bpm);
    q.bindValue(qs(":mk"),  t.musicalKey);
    q.bindValue(qs(":ck"),  t.camelotKey);
    q.bindValue(qs(":en"),  t.energy);
    q.bindValue(qs(":ll"),  t.loudnessLUFS);
    q.bindValue(qs(":fs"),  static_cast<qint64>(t.fileSize));
    q.bindValue(qs(":ci"),  t.cueIn);
    q.bindValue(qs(":co"),  t.cueOut);
    q.bindValue(qs(":da"),  t.danceability);
    q.bindValue(qs(":yr"),  t.year);
    q.bindValue(qs(":rt"),  t.rating);
    q.bindValue(qs(":cm"),  t.comments);
    q.bindValue(qs(":li"),  t.legacyImported ? 1 : 0);
    q.bindValue(qs(":ras"), t.regularAnalysisState);
    q.bindValue(qs(":raj"), t.regularAnalysisJson);
    q.bindValue(qs(":las"), t.liveAnalysisState);
    q.bindValue(qs(":laj"), t.liveAnalysisJson);
    return q.exec();
}

bool DjLibraryDatabase::deleteTrack(qint64 trackId)
{
    if (!open_) return false;
    QSqlQuery q(db_);
    q.prepare(qs("DELETE FROM library_tracks WHERE track_id = :tid;"));
    q.bindValue(qs(":tid"), trackId);
    return q.exec();
}

// ─────────────────────────────────────────────────────────────────────────────
// Private query helpers
// ─────────────────────────────────────────────────────────────────────────────
QString DjLibraryDatabase::sortOrderClause(int sortCol) const
{
    switch (sortCol) {
    case 1:  return qs("artist   COLLATE NOCASE ASC,  display_name COLLATE NOCASE ASC");
    case 2:  return qs("album    COLLATE NOCASE ASC,  display_name COLLATE NOCASE ASC");
    case 3:  return qs("duration_ms ASC");
    case 4:  return qs("CAST(bpm AS REAL) ASC");
    case 5:  return qs("musical_key COLLATE NOCASE ASC");
    default: return qs("display_name COLLATE NOCASE ASC");
    }
}

QString DjLibraryDatabase::buildWhereClause(const QString& search,
                                             int searchMode,
                                             const QStringList& playlistPaths,
                                             QStringList& outBindNames,
                                             QVariantList& outBindValues) const
{
    QStringList clauses;

    // ── playlist filter ──
    if (!playlistPaths.isEmpty()) {
        QStringList holders;
        for (int i = 0; i < playlistPaths.size(); ++i) {
            const QString name = QStringLiteral(":pl%1").arg(i);
            holders << name;
            outBindNames << name;
            outBindValues << playlistPaths[i];
        }
        clauses << QStringLiteral("file_path IN (%1)").arg(holders.join(QLatin1Char(',')));
    }

    // ── text search ──
    if (!search.isEmpty()) {
        const QString like = QLatin1Char('%') + search + QLatin1Char('%');
        switch (searchMode) {
        case 0: // display name
            clauses << qs("display_name LIKE :sq ESCAPE '\\'");
            outBindNames << qs(":sq"); outBindValues << like;
            break;
        case 1: // artist
            clauses << qs("artist LIKE :sq ESCAPE '\\'");
            outBindNames << qs(":sq"); outBindValues << like;
            break;
        case 2: // album
            clauses << qs("album LIKE :sq ESCAPE '\\'");
            outBindNames << qs(":sq"); outBindValues << like;
            break;
        case 3: { // BPM — "120" or "120-130"
            const int dash = search.indexOf(QLatin1Char('-'));
            bool okLo = false, okHi = false;
            if (dash > 0) {
                const double lo = search.left(dash).trimmed().toDouble(&okLo);
                const double hi = search.mid(dash + 1).trimmed().toDouble(&okHi);
                if (okLo && okHi) {
                    clauses << qs("CAST(bpm AS REAL) BETWEEN :blo AND :bhi");
                    outBindNames << qs(":blo") << qs(":bhi");
                    outBindValues << lo << hi;
                }
            } else {
                const double target = search.toDouble(&okLo);
                if (okLo) {
                    clauses << qs("ABS(CAST(bpm AS REAL) - :bt) < 1.0");
                    outBindNames << qs(":bt"); outBindValues << target;
                }
            }
            break;
        }
        case 4: { // length — "M:SS" or "M:SS-M:SS"
            auto parseMSS = [](const QString& s, bool& ok) -> qint64 {
                ok = false;
                const int ci = s.indexOf(QLatin1Char(':'));
                if (ci > 0) {
                    bool mOk = false, sOk = false;
                    const int m   = s.left(ci).trimmed().toInt(&mOk);
                    const int sec = s.mid(ci + 1).trimmed().toInt(&sOk);
                    if (mOk && sOk) { ok = true; return (qint64(m) * 60 + sec) * 1000; }
                } else {
                    const int sec = s.trimmed().toInt(&ok);
                    if (ok) return qint64(sec) * 1000;
                }
                return 0;
            };
            const int dash = search.indexOf(QLatin1Char('-'));
            if (dash > 0) {
                bool okL = false, okH = false;
                const qint64 lo = parseMSS(search.left(dash).trimmed(), okL);
                const qint64 hi = parseMSS(search.mid(dash + 1).trimmed(), okH);
                if (okL && okH) {
                    clauses << qs("duration_ms BETWEEN :dlo AND :dhi");
                    outBindNames << qs(":dlo") << qs(":dhi");
                    outBindValues << lo << hi;
                }
            } else {
                bool ok = false;
                const qint64 target = parseMSS(search, ok);
                if (ok) {
                    clauses << qs("ABS(duration_ms - :dt) < 30000");
                    outBindNames << qs(":dt"); outBindValues << target;
                }
            }
            break;
        }
        default: // all fields (mode 5)
            clauses << qs(
                "(display_name LIKE :sq ESCAPE '\\'"
                " OR title LIKE :sq ESCAPE '\\'"
                " OR artist LIKE :sq ESCAPE '\\'"
                " OR album LIKE :sq ESCAPE '\\'"
                " OR genre LIKE :sq ESCAPE '\\'"
                " OR bpm LIKE :sq ESCAPE '\\'"
                " OR musical_key LIKE :sq ESCAPE '\\'"
                " OR camelot_key LIKE :sq ESCAPE '\\')"
            );
            outBindNames << qs(":sq"); outBindValues << like;
            break;
        }
    }

    if (clauses.isEmpty()) return qs("1=1");
    return clauses.join(qs(" AND "));
}

// ─────────────────────────────────────────────────────────────────────────────
// Read
// ─────────────────────────────────────────────────────────────────────────────
DjLibraryDatabase::Row DjLibraryDatabase::rowFromQuery(const QSqlQuery& q)
{
    Row r;
    r.trackId            = q.value(0).toLongLong();
    r.info.mediaId       = q.value(1).toLongLong();
    r.info.fileFingerprint = q.value(2).toString();
    r.info.filePath      = q.value(3).toString();
    r.info.displayName   = q.value(4).toString();
    r.info.title         = q.value(5).toString();
    r.info.artist        = q.value(6).toString();
    r.info.album         = q.value(7).toString();
    r.info.genre         = q.value(8).toString();
    r.info.durationMs    = q.value(9).toLongLong();
    r.info.durationStr   = q.value(10).toString();
    r.info.bpm           = q.value(11).toString();
    r.info.musicalKey    = q.value(12).toString();
    r.info.camelotKey    = q.value(13).toString();
    r.info.energy        = q.value(14).toDouble();
    r.info.loudnessLUFS  = q.value(15).toDouble();
    r.info.fileSize      = q.value(16).toLongLong();
    r.info.cueIn         = q.value(17).toString();
    r.info.cueOut        = q.value(18).toString();
    r.info.danceability  = q.value(19).toDouble();
    r.info.year          = q.value(20).toInt();
    r.info.rating        = q.value(21).toInt();
    r.info.comments      = q.value(22).toString();
    r.info.legacyImported = q.value(23).toInt() != 0;
    r.info.regularAnalysisState = q.value(24).toString();
    r.info.regularAnalysisJson = q.value(25).toString();
    r.info.liveAnalysisState = q.value(26).toString();
    r.info.liveAnalysisJson = q.value(27).toString();
    return r;
}

int DjLibraryDatabase::queryCount(const QString& search, int searchMode,
                                   const QStringList& playlistPaths) const
{
    if (!open_) return 0;
    QStringList names; QVariantList vals;
    const QString where = buildWhereClause(search.toLower(), searchMode, playlistPaths, names, vals);
    QSqlQuery q(db_);
    q.prepare(QStringLiteral("SELECT COUNT(*) FROM library_tracks WHERE %1;").arg(where));
    for (int i = 0; i < names.size(); ++i) q.bindValue(names[i], vals[i]);
    if (!q.exec() || !q.next()) return 0;
    return q.value(0).toInt();
}

std::vector<DjLibraryDatabase::Row>
DjLibraryDatabase::queryPage(const QString& search, int searchMode,
                              const QStringList& playlistPaths,
                              int sortCol, int offset, int limit) const
{
    std::vector<Row> result;
    if (!open_) return result;

    QStringList names; QVariantList vals;
    const QString where = buildWhereClause(search.toLower(), searchMode, playlistPaths, names, vals);
    const QString order = sortOrderClause(sortCol);

    QSqlQuery q(db_);
    q.prepare(QStringLiteral(
        "SELECT track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        "       duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        "       loudness_lufs, file_size, cue_in, cue_out, danceability,"
        "       year_tag, rating, comments, legacy_imported,"
        "       regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json"
        " FROM library_tracks WHERE %1 ORDER BY %2 LIMIT :lim OFFSET :off;"
    ).arg(where, order));

    for (int i = 0; i < names.size(); ++i) q.bindValue(names[i], vals[i]);
    q.bindValue(qs(":lim"), limit);
    q.bindValue(qs(":off"), offset);

    if (!q.exec()) {
        qWarning().noquote() << qs("DjLibraryDatabase::queryPage FAIL") << q.lastError().text();
        return result;
    }
    while (q.next()) result.push_back(rowFromQuery(q));
    return result;
}

std::optional<TrackInfo> DjLibraryDatabase::trackById(qint64 trackId) const
{
    if (!open_) return std::nullopt;
    QSqlQuery q(db_);
    q.prepare(QStringLiteral(
        "SELECT track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        "       duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        "       loudness_lufs, file_size, cue_in, cue_out, danceability,"
        "       year_tag, rating, comments, legacy_imported,"
        "       regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json"
        " FROM library_tracks WHERE track_id = :tid LIMIT 1;"
    ));
    q.bindValue(qs(":tid"), trackId);
    if (!q.exec() || !q.next()) return std::nullopt;
    return rowFromQuery(q).info;
}

std::optional<TrackInfo> DjLibraryDatabase::trackByPath(const QString& path) const
{
    if (!open_) return std::nullopt;
    const QString normalizedPath = normalizeLibraryPath(path);
    if (normalizedPath.isEmpty()) return std::nullopt;
    QSqlQuery q(db_);
    q.prepare(QStringLiteral(
        "SELECT track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        "       duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        "       loudness_lufs, file_size, cue_in, cue_out, danceability,"
        "       year_tag, rating, comments, legacy_imported,"
        "       regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json"
        " FROM library_tracks WHERE file_path = :fp LIMIT 1;"
    ));
    q.bindValue(qs(":fp"), normalizedPath);
    if (!q.exec() || !q.next()) return std::nullopt;
    return rowFromQuery(q).info;
}

std::optional<TrackInfo> DjLibraryDatabase::trackByFingerprint(const QString& fingerprint) const
{
    if (!open_) return std::nullopt;
    const QString normalizedFingerprint = fingerprint.trimmed().toLower();
    if (normalizedFingerprint.isEmpty()) return std::nullopt;

    QSqlQuery q(db_);
    q.prepare(QStringLiteral(
        "SELECT track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        "       duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        "       loudness_lufs, file_size, cue_in, cue_out, danceability,"
        "       year_tag, rating, comments, legacy_imported,"
        "       regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json"
        " FROM library_tracks WHERE lower(file_fingerprint) = :ffp LIMIT 1;"
    ));
    q.bindValue(qs(":ffp"), normalizedFingerprint);
    if (!q.exec() || !q.next()) return std::nullopt;
    return rowFromQuery(q).info;
}

std::optional<TrackInfo> DjLibraryDatabase::trackByFileNameAndSize(const QString& fileName, qint64 fileSize) const
{
    if (!open_) return std::nullopt;
    const QString trimmedName = fileName.trimmed();
    if (trimmedName.isEmpty() || fileSize <= 0) return std::nullopt;

    QSqlQuery q(db_);
    q.prepare(QStringLiteral(
        "SELECT track_id, media_id, file_fingerprint, file_path, display_name, title, artist, album, genre,"
        "       duration_ms, duration_str, bpm, musical_key, camelot_key, energy,"
        "       loudness_lufs, file_size, cue_in, cue_out, danceability,"
        "       year_tag, rating, comments, legacy_imported,"
        "       regular_analysis_state, regular_analysis_json, live_analysis_state, live_analysis_json"
        " FROM library_tracks"
        " WHERE file_size = :fs AND lower(file_path) LIKE :suffix"
        " LIMIT 2;"
    ));
    q.bindValue(qs(":fs"), fileSize);
    q.bindValue(qs(":suffix"), QStringLiteral("%/") + trimmedName.toLower());

    std::optional<TrackInfo> match;
    int count = 0;
    if (!q.exec()) return std::nullopt;
    while (q.next()) {
        ++count;
        if (count > 1) return std::nullopt;
        match = rowFromQuery(q).info;
    }
    return match;
}

std::optional<qint64> DjLibraryDatabase::trackIdByPath(const QString& path) const
{
    if (!open_) return std::nullopt;
    const QString normalizedPath = normalizeLibraryPath(path);
    if (normalizedPath.isEmpty()) return std::nullopt;
    QSqlQuery q(db_);
    q.prepare(QStringLiteral("SELECT track_id FROM library_tracks WHERE file_path = :fp LIMIT 1;"));
    q.bindValue(qs(":fp"), normalizedPath);
    if (!q.exec() || !q.next()) return std::nullopt;
    return q.value(0).toLongLong();
}

int DjLibraryDatabase::totalCount() const
{
    if (!open_) return 0;
    QSqlQuery q(db_);
    if (!q.exec(qs("SELECT COUNT(*) FROM library_tracks;")) || !q.next()) return 0;
    return q.value(0).toInt();
}
