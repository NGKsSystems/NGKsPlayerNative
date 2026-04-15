#include <QMenu>
#include "ui/library/LibraryBrowserWidget.h"
#include "ui/library/DjLibraryDatabase.h"

#include <QComboBox>
#include <QFont>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QVBoxLayout>

// ─────────────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────────────

LibraryBrowserWidget::LibraryBrowserWidget(Mode mode, QWidget* parent)
    : QWidget(parent), mode_(mode)
{
    if (mode_ == Mode::MainPanel)
        buildMainPanel();
    else
        buildPlayerPanel();
}

void LibraryBrowserWidget::buildMainPanel()
{
    auto* root = new QVBoxLayout(this);
    root->setContentsMargins(0, 0, 0, 0);
    root->setSpacing(0);

    // ── Search row ──────────────────────────────────────────────────────────
    auto* searchRow = new QHBoxLayout();
    searchRow->setSpacing(6);
    searchRow->setContentsMargins(0, 0, 0, 4);

    searchModeCombo_ = new QComboBox(this);
    searchModeCombo_->addItem(QStringLiteral("File Name"),  0);
    searchModeCombo_->addItem(QStringLiteral("Artist"),     1);
    searchModeCombo_->addItem(QStringLiteral("Album"),      2);
    searchModeCombo_->addItem(QStringLiteral("BPM"),        3);
    searchModeCombo_->addItem(QStringLiteral("Length"),     4);
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

    searchBar_ = new QLineEdit(this);
    searchBar_->setPlaceholderText(QStringLiteral("Search by file name..."));
    searchBar_->setClearButtonEnabled(true);
    searchBar_->setMinimumHeight(34);
    searchRow->addWidget(searchBar_, 1);

    sortCombo_ = new QComboBox(this);
    sortCombo_->addItem(QStringLiteral("Sort: Title"),    0);
    sortCombo_->addItem(QStringLiteral("Sort: Artist"),   1);
    sortCombo_->addItem(QStringLiteral("Sort: Album"),    2);
    sortCombo_->addItem(QStringLiteral("Sort: Duration"), 3);
    sortCombo_->addItem(QStringLiteral("Sort: BPM"),      4);
    sortCombo_->addItem(QStringLiteral("Sort: Key"),      5);
    sortCombo_->setMinimumHeight(34);
    sortCombo_->setStyleSheet(QStringLiteral(
        "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 6px; padding: 4px 10px; font-size: 12px; }"
        "QComboBox::drop-down { border: none; }"
        "QComboBox QAbstractItemView { background: #16213e; color: #e0e0e0;"
        "  selection-background-color: #533483; }"
    ));
    searchRow->addWidget(sortCombo_);

    root->addLayout(searchRow);

    // ── Track table ─────────────────────────────────────────────────────────
    view_ = new DjLibraryWidget(this);
    view_->header()->setStretchLastSection(false);
    view_->header()->setSectionResizeMode(QHeaderView::Interactive);
    view_->header()->setMinimumSectionSize(40);
    view_->header()->resizeSection(0, 340);
    view_->header()->resizeSection(1, 140);
    view_->header()->resizeSection(2, 140);
    view_->header()->resizeSection(3, 100); // Genre
    view_->header()->resizeSection(4,  70); // Duration
    view_->header()->resizeSection(5,  55); // BPM
    view_->header()->resizeSection(6,  50); // Key
    view_->header()->resizeSection(7,  60); // Camelot
    view_->header()->resizeSection(8,  55); // LUFS
    root->addWidget(view_, 1);

    // Column visibility context menu
    view_->header()->setContextMenuPolicy(Qt::CustomContextMenu);
    connect(view_->header(), &QWidget::customContextMenuRequested, this, [this](const QPoint& pos) {
        QMenu menu(this);
        menu.setStyleSheet(
            "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
            "QMenu::item { padding: 6px 24px; }"
            "QMenu::item:selected { background: #533483; }"
        );
        for (int i = 1; i < view_->header()->count(); ++i) { // Skip Name (0)
            QString colName = view_->header()->model()->headerData(i, Qt::Horizontal).toString();
            QAction* a = menu.addAction(colName);
            a->setCheckable(true);
            a->setChecked(!view_->header()->isSectionHidden(i));
            connect(a, &QAction::triggered, this, [this, i](bool checked) {
                view_->header()->setSectionHidden(i, !checked);
            });
        }
        menu.exec(view_->header()->mapToGlobal(pos));
    });


    // ── Internal wiring (search/sort → filter) ──────────────────────────────
    connect(searchBar_, &QLineEdit::textChanged,
            this, &LibraryBrowserWidget::onFilterChanged);

    connect(searchModeCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, [this](int) {
        static const QString placeholders[] = {
            QStringLiteral("Search by file name..."),
            QStringLiteral("Search by artist..."),
            QStringLiteral("Search by album..."),
            QStringLiteral("Search by BPM (e.g. 120 or 120-130)..."),
            QStringLiteral("Search by length (e.g. 3:00 or 3:00-5:00)..."),
            QStringLiteral("Search all fields..."),
        };
        const int mode = searchModeCombo_->currentData().toInt();
        if (mode >= 0 && mode <= 5)
            searchBar_->setPlaceholderText(placeholders[mode]);
        onFilterChanged();
    });

    connect(sortCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, &LibraryBrowserWidget::onFilterChanged);

    // ── Forward DjLibraryWidget signals ─────────────────────────────────────
    connect(view_, &DjLibraryWidget::trackActivated,
            this, &LibraryBrowserWidget::trackActivated);
    connect(view_, &DjLibraryWidget::trackSelected,
            this, &LibraryBrowserWidget::trackSelected);
    connect(view_, &DjLibraryWidget::contextMenuRequested,
            this, &LibraryBrowserWidget::contextMenuRequested);
}

void LibraryBrowserWidget::buildPlayerPanel()
{
    auto* root = new QVBoxLayout(this);
    root->setContentsMargins(0, 0, 0, 0);
    root->setSpacing(4);

    // ── Header row: label + search + sort + count ────────────────────────────
    auto* headerRow = new QHBoxLayout();
    headerRow->setSpacing(8);

    auto* libLabel = new QLabel(QStringLiteral("Library"), this);
    {
        QFont f = libLabel->font();
        f.setPointSize(13);
        f.setBold(true);
        libLabel->setFont(f);
    }
    headerRow->addWidget(libLabel);
    headerRow->addSpacing(12);

    searchBar_ = new QLineEdit(this);
    searchBar_->setPlaceholderText(QStringLiteral("Search tracks..."));
    searchBar_->setClearButtonEnabled(true);
    searchBar_->setMinimumHeight(28);
    searchBar_->setStyleSheet(QStringLiteral(
        "QLineEdit { background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 4px; padding: 4px 8px; font-size: 12px; }"
        "QLineEdit:focus { border-color: #e94560; }"));
    headerRow->addWidget(searchBar_, 1);

    headerRow->addSpacing(8);

    auto* sortLabel = new QLabel(QStringLiteral("Sort:"), this);
    sortLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
    headerRow->addWidget(sortLabel);

    sortCombo_ = new QComboBox(this);
    sortCombo_->addItems({
        QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Album"),
        QStringLiteral("Duration"), QStringLiteral("BPM"), QStringLiteral("Key")
    });
    sortCombo_->setMinimumWidth(90);
    headerRow->addWidget(sortCombo_);

    headerRow->addSpacing(8);

    countLabel_ = new QLabel(QStringLiteral("0 tracks"), this);
    countLabel_->setStyleSheet(QStringLiteral("color: #666; font-size: 11px;"));
    headerRow->addWidget(countLabel_);

    root->addLayout(headerRow);

    // ── Track table ─────────────────────────────────────────────────────────
    view_ = new DjLibraryWidget(this);
    {
        QFont f = view_->font();
        f.setPointSize(11);
        view_->setViewFont(f);
    }
    view_->setViewStyleSheet(QStringLiteral(
        "QTableView { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 8px; outline: none; alternate-background-color: #1a1a2e; }"
        "QTableView::item { padding: 4px 6px; }"
        "QTableView::item:selected { background: #533483; color: #ffffff; }"
        "QTableView::item:hover { background: #1a1a2e; }"
        "QHeaderView::section { background: #0f3460; color: #e0e0e0; border: none;"
        "  padding: 5px 8px; font-weight: bold; font-size: 11px; }"
        "QScrollBar:vertical { background: #0a0e27; width: 8px; }"
        "QScrollBar::handle:vertical { background: #533483; border-radius: 4px; min-height: 20px; }"
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"));
    view_->header()->setSectionResizeMode(QHeaderView::Interactive);
    view_->header()->resizeSection(0, 280);
    view_->header()->resizeSection(1, 140);
    view_->header()->resizeSection(2, 130);
    view_->header()->resizeSection(3,  90); // Genre
    view_->header()->resizeSection(4,  65); // Duration
    view_->header()->resizeSection(5,  50); // BPM
    view_->header()->resizeSection(6,  45); // Key
    view_->header()->resizeSection(7,  55); // Camelot
    view_->header()->resizeSection(8,  50); // LUFS
    root->addWidget(view_, 1);

    // Column visibility context menu
    view_->header()->setContextMenuPolicy(Qt::CustomContextMenu);
    connect(view_->header(), &QWidget::customContextMenuRequested, this, [this](const QPoint& pos) {
        QMenu menu(this);
        menu.setStyleSheet(
            "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
            "QMenu::item { padding: 6px 24px; }"
            "QMenu::item:selected { background: #533483; }"
        );
        for (int i = 1; i < view_->header()->count(); ++i) { // Skip Name (0)
            QString colName = view_->header()->model()->headerData(i, Qt::Horizontal).toString();
            QAction* a = menu.addAction(colName);
            a->setCheckable(true);
            a->setChecked(!view_->header()->isSectionHidden(i));
            connect(a, &QAction::triggered, this, [this, i](bool checked) {
                view_->header()->setSectionHidden(i, !checked);
            });
        }
        menu.exec(view_->header()->mapToGlobal(pos));
    });


    // ── Internal wiring ──────────────────────────────────────────────────────
    connect(searchBar_, &QLineEdit::textChanged,
            this, &LibraryBrowserWidget::onFilterChanged);
    connect(sortCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
            this, &LibraryBrowserWidget::onFilterChanged);

    // ── Forward DjLibraryWidget signals ─────────────────────────────────────
    connect(view_, &DjLibraryWidget::trackActivated,
            this, &LibraryBrowserWidget::trackActivated);
    connect(view_, &DjLibraryWidget::trackSelected,
            this, &LibraryBrowserWidget::trackSelected);
    connect(view_, &DjLibraryWidget::contextMenuRequested,
            this, &LibraryBrowserWidget::contextMenuRequested);
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

void LibraryBrowserWidget::setDatabase(DjLibraryDatabase* db)
{
    view_->setDatabase(db);
}

void LibraryBrowserWidget::setPlaylistFilter(const QStringList& paths)
{
    playlistFilter_ = paths;
}

void LibraryBrowserWidget::refresh()
{
    applyCurrentFilter();
}

// ─────────────────────────────────────────────────────────────────────────────
// Passthroughs
// ─────────────────────────────────────────────────────────────────────────────

qint64 LibraryBrowserWidget::currentTrackId() const  { return view_->currentTrackId(); }
void   LibraryBrowserWidget::setCurrentTrackId(qint64 id) { view_->setCurrentTrackId(id); }
void   LibraryBrowserWidget::scrollToTrackId(qint64 id)   { view_->scrollToTrackId(id); }
qint64 LibraryBrowserWidget::firstVisibleTrackId() const   { return view_->firstVisibleTrackId(); }
qint64 LibraryBrowserWidget::trackIdAt(int row) const      { return view_->trackIdAt(row); }
int    LibraryBrowserWidget::rowOfTrackId(qint64 id) const { return view_->rowOfTrackId(id); }
int    LibraryBrowserWidget::totalFilteredCount() const    { return view_->totalFilteredCount(); }

std::optional<TrackInfo> LibraryBrowserWidget::trackById(qint64 id) const
{
    return view_->trackById(id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Private
// ─────────────────────────────────────────────────────────────────────────────

void LibraryBrowserWidget::onFilterChanged()
{
    applyCurrentFilter();
}

void LibraryBrowserWidget::applyCurrentFilter()
{
    if (!view_) return;

    const QString query    = searchBar_ ? searchBar_->text().trimmed() : QString();
    const int searchMode   = (searchModeCombo_ && mode_ == Mode::MainPanel)
                             ? searchModeCombo_->currentData().toInt() : 5;
    const int sortCol      = sortCombo_ ? sortCombo_->currentIndex() : 0;

    view_->applyFilter(query, searchMode, playlistFilter_, sortCol);

    const int count = view_->totalFilteredCount();
    if (countLabel_)
        countLabel_->setText(QStringLiteral("%1 tracks").arg(count));
    emit trackCountChanged(count);
}
