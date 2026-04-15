#pragma once

#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/DjLibraryModel.h"

#include <QStringList>
#include <QWidget>

#include <optional>

class QHeaderView;
#include "ui/library/TrackDragView.h"

// ─────────────────────────────────────────────────────────────────────────────
// DjLibraryWidget
//
// Drop-in replacement for QTreeWidget for the DJ library panel.
// Hosts a QTableView + DjLibraryModel that pages rows from SQLite so the full
// dataset is never held in QWidgetItems.
//
// Usage in main.cpp:
//   libraryTree_ = new DjLibraryWidget(splitter);
//   libraryTree_->setDatabase(&djDb_);
//   libraryTree_->header()->resizeSection(0, 340);
//   connect(libraryTree_, &DjLibraryWidget::trackActivated, ...)
// ─────────────────────────────────────────────────────────────────────────────
class DjLibraryWidget : public QWidget {
    Q_OBJECT
public:
    explicit DjLibraryWidget(QWidget* parent = nullptr);

    // Must be called before applyFilter().
    void setDatabase(DjLibraryDatabase* db);

    // ── Layout / presentation ─────────────────────────────────────────────
    // Returns the horizontal header of the internal QTableView.
    // Use for column widths, resize modes, etc.
    QHeaderView* header() const;

    // Returns the QTableView's viewport (for mapToGlobal in context menus).
    QWidget* viewport() const;

    void setViewFont(const QFont& f);

    // Apply stylesheet that may contain QTreeWidget colour rules —
    // translated to QTableView equivalents transparently.
    void setViewStyleSheet(const QString& ss);

    // ── Data control ──────────────────────────────────────────────────────
    // Re-queries DB with new filter / sort params and repopulates view.
    void applyFilter(const QString& search, int searchMode,
                     const QStringList& playlistPaths, int sortCol);

    // Total rows matching the current filter in the DB.
    int totalFilteredCount() const;

    // Rows currently loaded (first page(s)).
    int visibleRowCount() const;

    // ── Selection ─────────────────────────────────────────────────────────
    qint64 currentTrackId() const;
    void   setCurrentTrackId(qint64 id);

    // If the row for id is not yet loaded, fetch pages until it is or give up.
    void   scrollToTrackId(qint64 id);

    // track_id of the first visible row, or -1.
    qint64 firstVisibleTrackId() const;

    // ── Navigation helpers ────────────────────────────────────────────────
    // Returns track_id at row (fetches pages as needed). Returns -1 if out of range.
    qint64 trackIdAt(int row) const;

    // Returns row of track_id in the loaded pages, fetching more if needed.
    // Returns -1 if not found.
    int    rowOfTrackId(qint64 id) const;

    // ── Track lookup ──────────────────────────────────────────────────────
    std::optional<TrackInfo> trackById(qint64 id) const;

signals:
    /// Single-click / arrow-key selection changed.
    void trackSelected(qint64 trackId);

    /// Double-click (activate / play).
    void trackActivated(qint64 trackId);

    /// Right-click on a track row; globalPos is in screen coordinates.
    void contextMenuRequested(qint64 trackId, QPoint globalPos);

private slots:
    void onActivated(const QModelIndex& index);
    void onCurrentChanged(const QModelIndex& current, const QModelIndex& previous);
    void onCustomContextMenu(const QPoint& pos);

private:
    DjLibraryDatabase* db_{nullptr};
    DjLibraryModel*    model_{nullptr};
    TrackDragView*     view_{nullptr};
};
