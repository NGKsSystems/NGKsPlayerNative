#pragma once

#include "ui/library/LibraryPersistence.h"

#include <QSqlDatabase>
#include <QString>
#include <QStringList>

#include <optional>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
// DjLibraryDatabase
//
// SQLite-backed store for the DJ library.  track_id is the integer primary key
// assigned during bulkInsert (equal to the insertion index).  It is the sole
// stable identifier for a track across the DB→model→view pipeline.
//
// The display model (DjLibraryModel) always reads from SQLite with LIMIT/OFFSET
// paging — the full dataset is never held in widget items or a shadow vector.
// ─────────────────────────────────────────────────────────────────────────────
class DjLibraryDatabase {
public:
    // Row returned from paged queries.
    struct Row {
        qint64   trackId{-1};
        TrackInfo info;
    };

    DjLibraryDatabase() = default;
    ~DjLibraryDatabase();

    // Opens (and creates if needed) the SQLite file at dbPath.
    // Returns false on failure.
    bool open(const QString& dbPath);
    void close();
    bool isOpen() const { return open_; }

    // ── Write ──────────────────────────────────────────────────────────────
    // Replaces entire library: drops all rows and re-inserts.
    // track_id for row i = i  (= allTracks_ index).
    bool bulkInsert(const std::vector<TrackInfo>& tracks);

    // Upsert a single track (track_id must be valid).
    bool upsertTrack(qint64 trackId, const TrackInfo& info);

    // Remove by track_id.
    bool deleteTrack(qint64 trackId);

    // ── Read ───────────────────────────────────────────────────────────────
    // Total number of rows matching the given filter (no paging).
    int queryCount(const QString& search, int searchMode,
                   const QStringList& playlistPaths) const;

    // Paged result: offset + limit rows matching filter + sort.
    // sortCol codes match the library sort combo:
    //   0=name, 1=artist, 2=album, 3=duration, 4=bpm, 5=key
    std::vector<Row> queryPage(const QString& search, int searchMode,
                               const QStringList& playlistPaths,
                               int sortCol,
                               int offset, int limit) const;

    // Lookup by primary key.
    std::optional<TrackInfo> trackById(qint64 trackId) const;

    // Lookup by exact file path.
    std::optional<TrackInfo> trackByPath(const QString& path) const;

    // Total rows in the table (no filter).
    int totalCount() const;

    // The DB connection name used internally.
    const QString& connectionName() const { return connName_; }

private:
    bool createSchema();
    QString buildWhereClause(const QString& search, int searchMode,
                             const QStringList& playlistPaths,
                             QStringList& outBindNames,
                             QVariantList& outBindValues) const;
    QString sortOrderClause(int sortCol) const;

    static Row rowFromQuery(const QSqlQuery& q);

    QSqlDatabase db_;
    QString      connName_;
    bool         open_{false};
};
