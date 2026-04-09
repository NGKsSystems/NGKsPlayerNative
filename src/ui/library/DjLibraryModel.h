#pragma once

#include "ui/library/DjLibraryDatabase.h"

#include <QAbstractTableModel>
#include <QMimeData>
#include <QStringList>
#include <QVector>

// ─────────────────────────────────────────────────────────────────────────────
// DjLibraryModel
//
// QAbstractTableModel backed by DjLibraryDatabase.  Uses fetchMore() / canFetchMore()
// so QTableView only causes DB reads in kPageSize-row increments as the user scrolls.
//
// Columns: 0=Name 1=Artist 2=Album 3=Duration 4=BPM 5=Key
// Qt::UserRole on any column → track_id (qint64).
//
// Drag support: drags carry two MIME types so drop targets can accept by
// either track identity or raw file URI:
//   "application/x-ngks-track-id"  — track_id as a UTF-8 decimal string
//   "text/uri-list"                — local file URI (file:///.../track.mp3)
// ─────────────────────────────────────────────────────────────────────────────
class DjLibraryModel : public QAbstractTableModel {
    Q_OBJECT
public:
    static constexpr int kPageSize = 200;
    static constexpr int kColumns  = 6;

    static constexpr QLatin1String kMimeType    { "application/x-ngks-track-id" };
    static constexpr QLatin1String kMimeTypeUri  { "text/uri-list" };

    explicit DjLibraryModel(DjLibraryDatabase* db, QObject* parent = nullptr);

    // ── QAbstractTableModel ────────────────────────────────────────────────
    int      rowCount   (const QModelIndex& parent = {}) const override;
    int      columnCount(const QModelIndex& parent = {}) const override;
    QVariant data       (const QModelIndex& index, int role = Qt::DisplayRole) const override;
    QVariant headerData (int section, Qt::Orientation orientation,
                         int role = Qt::DisplayRole) const override;

    // ── Incremental loading ────────────────────────────────────────────────
    bool canFetchMore(const QModelIndex& parent) const override;
    void fetchMore   (const QModelIndex& parent) override;

    // ── Drag ──────────────────────────────────────────────────────────────
    Qt::ItemFlags  flags   (const QModelIndex& index) const override;
    QStringList    mimeTypes() const override;
    QMimeData*     mimeData (const QModelIndexList& indexes) const override;

    // ── Filter / sort ──────────────────────────────────────────────────────
    // Call reload() after changing filter/sort.
    void setSearch      (const QString& text, int mode);
    void setSortCol     (int col);
    void setPlaylistPaths(const QStringList& paths);

    // Clears cached rows, re-queries count, fetches first page.
    void reload();

    // ── Accessors ─────────────────────────────────────────────────────────
    qint64           trackIdAt  (int row) const;
    const TrackInfo& trackInfoAt(int row) const;

    // Total rows matching current filter in the DB (may exceed rowCount()).
    int totalFilteredCount() const { return totalCount_; }

    // Row index of a trackId in the loaded set (-1 if not loaded yet).
    int rowOfTrackId(qint64 id) const;

private:
    struct Row {
        qint64    trackId{-1};
        TrackInfo info;
    };

    DjLibraryDatabase* db_{nullptr};

    // Current filter state
    QString     search_;
    int         searchMode_{5};
    int         sortCol_{0};
    QStringList playlistPaths_;

    // Loaded data
    QVector<Row> rows_;
    int          totalCount_{0};
};
