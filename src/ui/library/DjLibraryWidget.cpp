#include "ui/library/DjLibraryWidget.h"

#include <QHeaderView>
#include <QModelIndex>
#include <QTableView>
#include <QVBoxLayout>

DjLibraryWidget::DjLibraryWidget(QWidget* parent)
    : QWidget(parent)
{
    auto* lay = new QVBoxLayout(this);
    lay->setContentsMargins(0, 0, 0, 0);
    lay->setSpacing(0);

    view_ = new QTableView(this);
    view_->setEditTriggers(QAbstractItemView::NoEditTriggers);
    view_->setSelectionMode(QAbstractItemView::SingleSelection);
    view_->setSelectionBehavior(QAbstractItemView::SelectRows);
    view_->setAlternatingRowColors(true);
    view_->setShowGrid(false);
    view_->setSortingEnabled(false);            // We sort through the model
    view_->verticalHeader()->setVisible(false);
    view_->horizontalHeader()->setStretchLastSection(false);
    view_->setContextMenuPolicy(Qt::CustomContextMenu);

    // Drag support
    view_->setDragEnabled(true);
    view_->setDragDropMode(QAbstractItemView::DragOnly);
    view_->setDefaultDropAction(Qt::CopyAction);

    lay->addWidget(view_);

    connect(view_, &QTableView::activated,
            this,  &DjLibraryWidget::onActivated);
    connect(view_, &QTableView::customContextMenuRequested,
            this,  &DjLibraryWidget::onCustomContextMenu);
}

void DjLibraryWidget::setDatabase(DjLibraryDatabase* db)
{
    db_ = db;

    // Rebuild model with the new database pointer
    if (model_) {
        view_->setModel(nullptr);
        delete model_;
    }

    model_ = new DjLibraryModel(db_, this);
    view_->setModel(model_);

    connect(view_->selectionModel(), &QItemSelectionModel::currentRowChanged,
            this, &DjLibraryWidget::onCurrentChanged);
}

// ─────────────────────────────────────────────────────────────────────────────
// Layout / presentation
// ─────────────────────────────────────────────────────────────────────────────
QHeaderView* DjLibraryWidget::header() const
{
    return view_->horizontalHeader();
}

QWidget* DjLibraryWidget::viewport() const
{
    return view_->viewport();
}

void DjLibraryWidget::setViewFont(const QFont& f)
{
    view_->setFont(f);
}

void DjLibraryWidget::setViewStyleSheet(const QString& ss)
{
    // The caller may pass a QTreeWidget stylesheet; we just apply it to the
    // QTableView — visual rules that match overlap, others are ignored.
    view_->setStyleSheet(ss);
}

// ─────────────────────────────────────────────────────────────────────────────
// Data control
// ─────────────────────────────────────────────────────────────────────────────
void DjLibraryWidget::applyFilter(const QString& search, int searchMode,
                                   const QStringList& playlistPaths, int sortCol)
{
    if (!model_) return;
    model_->setSearch(search, searchMode);
    model_->setSortCol(sortCol);
    model_->setPlaylistPaths(playlistPaths);
    model_->reload();
}

int DjLibraryWidget::totalFilteredCount() const
{
    if (!model_) return 0;
    return model_->totalFilteredCount();
}

int DjLibraryWidget::visibleRowCount() const
{
    if (!model_) return 0;
    return model_->rowCount();
}

// ─────────────────────────────────────────────────────────────────────────────
// Selection
// ─────────────────────────────────────────────────────────────────────────────
qint64 DjLibraryWidget::currentTrackId() const
{
    if (!model_ || !view_->selectionModel()) return -1;
    const QModelIndex idx = view_->selectionModel()->currentIndex();
    if (!idx.isValid()) return -1;
    return model_->trackIdAt(idx.row());
}

void DjLibraryWidget::setCurrentTrackId(qint64 id)
{
    if (!model_) return;
    int row = model_->rowOfTrackId(id);
    if (row < 0) return;
    const QModelIndex idx = model_->index(row, 0);
    view_->selectionModel()->setCurrentIndex(idx, QItemSelectionModel::ClearAndSelect | QItemSelectionModel::Rows);
}

void DjLibraryWidget::scrollToTrackId(qint64 id)
{
    if (!model_) return;

    int row = model_->rowOfTrackId(id);

    // If not loaded yet, keep fetching until we have it or exhaust pages
    while (row < 0 && model_->canFetchMore({})) {
        model_->fetchMore({});
        row = model_->rowOfTrackId(id);
    }

    if (row < 0) return;
    const QModelIndex idx = model_->index(row, 0);
    view_->scrollTo(idx, QAbstractItemView::PositionAtCenter);
}

qint64 DjLibraryWidget::firstVisibleTrackId() const
{
    if (!model_) return -1;
    const QModelIndex idx = view_->indexAt(view_->viewport()->rect().topLeft());
    if (!idx.isValid()) return model_->trackIdAt(0);
    return model_->trackIdAt(idx.row());
}

// ─────────────────────────────────────────────────────────────────────────────
// Navigation helpers
// ─────────────────────────────────────────────────────────────────────────────
qint64 DjLibraryWidget::trackIdAt(int row) const
{
    if (!model_ || row < 0) return -1;
    while (model_->rowCount({}) <= row && model_->canFetchMore({}))
        model_->fetchMore({});
    if (row >= model_->rowCount({})) return -1;
    return model_->trackIdAt(row);
}

int DjLibraryWidget::rowOfTrackId(qint64 id) const
{
    if (!model_) return -1;
    int row = model_->rowOfTrackId(id);
    while (row < 0 && model_->canFetchMore({})) {
        model_->fetchMore({});
        row = model_->rowOfTrackId(id);
    }
    return row;
}

// ─────────────────────────────────────────────────────────────────────────────
// Track lookup
// ─────────────────────────────────────────────────────────────────────────────
std::optional<TrackInfo> DjLibraryWidget::trackById(qint64 id) const
{
    if (!db_) return std::nullopt;
    return db_->trackById(id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Private slots
// ─────────────────────────────────────────────────────────────────────────────
void DjLibraryWidget::onActivated(const QModelIndex& index)
{
    if (!model_ || !index.isValid()) return;
    const qint64 tid = model_->trackIdAt(index.row());
    if (tid >= 0) emit trackActivated(tid);
}

void DjLibraryWidget::onCurrentChanged(const QModelIndex& current, const QModelIndex& /*previous*/)
{
    if (!model_ || !current.isValid()) return;
    const qint64 tid = model_->trackIdAt(current.row());
    if (tid >= 0) emit trackSelected(tid);
}

void DjLibraryWidget::onCustomContextMenu(const QPoint& pos)
{
    if (!model_) return;
    const QModelIndex idx = view_->indexAt(pos);
    if (!idx.isValid()) return;
    const qint64 tid = model_->trackIdAt(idx.row());
    if (tid >= 0) emit contextMenuRequested(tid, view_->viewport()->mapToGlobal(pos));
}
