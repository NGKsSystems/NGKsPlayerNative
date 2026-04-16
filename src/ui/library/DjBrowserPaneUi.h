#pragma once

#include "ui/library/DjBrowserFolderTreeProxyModel.h"
#include "ui/library/DjBrowserFileTableModel.h"
#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryScanner.h"
#include "ui/library/TrackDragView.h"

#include <QAbstractItemView>
#include <QDir>
#include <QFileSystemModel>
#include <QHeaderView>
#include <QHBoxLayout>
#include <QItemSelectionModel>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QSplitter>
#include <QStandardPaths>
#include <QTreeView>
#include <QVBoxLayout>

#include <functional>

namespace DjBrowserPaneUi {

constexpr int FolderPathRole = Qt::UserRole + 101;

struct Widgets {
    QFileSystemModel* dirModel{};
    DjBrowserFolderTreeProxyModel* dirProxy{};
    DjBrowserFileTableModel* fileModel{};
    DjBrowserFileTableModel* fileModel2{};
    QTreeView* dirView{};
    TrackDragView* fileView{};
    TrackDragView* fileView2{};
    QPushButton* importFolderBtn{};
    QPushButton* importAnalysisBtn{};
    QPushButton* showAllFoldersBtn{};
    QLineEdit* filterBox{};
    QLineEdit* filterBox2{};
    QLabel* folderLabel{};
    QLabel* folderLabel2{};
    QWidget* paneHeader{};
    QWidget* paneHeader2{};
    QLabel* footerLabel{};
};

struct InteractionHandlers {
    std::function<void(const QString&)> setSearchText;
    std::function<void(const QString&)> setFolderPath;
    std::function<void()> treeStateChanged;
    std::function<void(const QModelIndex&)> beginRename;
    std::function<void(const QPoint&)> showHeaderMenu;
    std::function<void(const QPoint&)> showFileMenu;
    std::function<void(const QString&)> setSearchText2;
    std::function<void(const QModelIndex&)> beginRename2;
    std::function<void(const QPoint&)> showHeaderMenu2;
    std::function<void(const QPoint&)> showFileMenu2;
    std::function<void()> activatePane1;
    std::function<void()> activatePane2;
};

inline QString menuStyle()
{
    return QStringLiteral(
        "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
        "QMenu::item { padding: 6px 24px; }"
        "QMenu::item:selected { background: #533483; }"
        "QMenu::item:disabled { color: #555; }"
        "QMenu::separator { height: 1px; background: #0f3460; margin: 4px 8px; }");
}

inline QLineEdit* createSearchBox(QWidget* parent, const QString& placeholder)
{
    auto* box = new QLineEdit(parent);
    box->setPlaceholderText(placeholder);
    box->setClearButtonEnabled(true);
    DjBrowserUiFeedback::applyInputChrome(box);
    return box;
}

// Builds one right-side file-browser pane (header + file model + file view).
// Assigns all output widgets through reference parameters.
inline QWidget* buildRightPane(QWidget* parent,
                                DjLibraryDatabase* db,
                                DjBrowserFileTableModel*& outModel,
                                TrackDragView*& outView,
                                QLineEdit*& outFilterBox,
                                QLabel*& outFolderLabel,
                                QWidget*& outHeader,
                                const QString& searchPlaceholder)
{
    auto* pane = new QWidget(parent);
    auto* paneLayout = new QVBoxLayout(pane);
    paneLayout->setContentsMargins(0, 0, 0, 0);
    paneLayout->setSpacing(0);

    outHeader = new QWidget(pane);
    outHeader->setMinimumHeight(26);
    auto* hdrLayout = new QHBoxLayout(outHeader);
    hdrLayout->setContentsMargins(6, 2, 6, 2);
    hdrLayout->setSpacing(6);

    outFolderLabel = new QLabel(QStringLiteral("\u2014 no folder \u2014"), outHeader);
    outFolderLabel->setStyleSheet(QStringLiteral("color: #8092a8; font-size: 11px; font-weight: 600;"));
    outFilterBox = createSearchBox(pane, searchPlaceholder);
    outFilterBox->setMaximumWidth(160);
    hdrLayout->addWidget(outFolderLabel, 1);
    hdrLayout->addWidget(outFilterBox, 0);

    paneLayout->addWidget(outHeader);

    outModel = new DjBrowserFileTableModel(db, parent);
    outModel->setFolderPath(QDir::homePath());

    outView = new TrackDragView(pane);
    outView->setModel(outModel);
    outView->setSelectionBehavior(QAbstractItemView::SelectRows);
    outView->setSelectionMode(QAbstractItemView::SingleSelection);
    outView->setAlternatingRowColors(true);
    outView->setShowGrid(false);
    outView->setMouseTracking(true);
    outView->setEditTriggers(QAbstractItemView::EditKeyPressed);
    outView->setDragEnabled(true);
    outView->setDragDropMode(QAbstractItemView::DragOnly);
    outView->setDefaultDropAction(Qt::CopyAction);
    outView->setSortingEnabled(true);
    outView->verticalHeader()->hide();
    outView->horizontalHeader()->setStretchLastSection(true);
    outView->horizontalHeader()->setSectionsClickable(true);
    outView->horizontalHeader()->setSortIndicatorShown(true);
    outView->setItemDelegateForColumn(
        DjBrowserFileTableModel::NameColumn,
        new DjBrowserNameDelegate(outView));
    outView->setStyleSheet(QStringLiteral(
        "QTableView { background: #0a0c12; color: #d7deea; border: 1px solid #222; gridline-color: #151b27; alternate-background-color: #0d1119; selection-background-color: #f28c28; selection-color: #10141f; }"
        "QTableView::item { padding: 2px 6px; }"
        "QTableView::item:selected { background: #f28c28; color: #10141f; }"
        "QTableView::item:hover { background: #1a2434; color: #f6f8fb; }"
        "QHeaderView::section { background: #111; color: #c9d3e3; border: none; padding: 4px 6px; }"));
    outView->setColumnWidth(DjBrowserFileTableModel::NameColumn, 210);
    outView->setColumnWidth(DjBrowserFileTableModel::SizeColumn, 82);
    outView->setColumnWidth(DjBrowserFileTableModel::TypeColumn, 120);
    outView->setColumnWidth(DjBrowserFileTableModel::DateModifiedColumn, 140);
    outView->setColumnWidth(DjBrowserFileTableModel::BpmColumn, 60);
    outView->setColumnWidth(DjBrowserFileTableModel::KeyColumn, 70);
    outView->setColumnWidth(DjBrowserFileTableModel::CamelotColumn, 80);
    outView->setColumnWidth(DjBrowserFileTableModel::LufsColumn, 70);
    outView->setColumnWidth(DjBrowserFileTableModel::GenreColumn, 110);
    outView->sortByColumn(DjBrowserFileTableModel::NameColumn, Qt::AscendingOrder);

    paneLayout->addWidget(outView, 1);
    return pane;
}

inline Widgets build(QWidget* parent, DjLibraryDatabase* db)
{
    Widgets widgets;

    auto* layout = new QVBoxLayout(parent);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(4);

    widgets.importFolderBtn = new QPushButton(QStringLiteral("Import Folder"), parent);
    widgets.importFolderBtn->setCursor(Qt::PointingHandCursor);
    widgets.importFolderBtn->setMinimumHeight(24);
    widgets.importFolderBtn->setStyleSheet(QStringLiteral(
        "QPushButton { background: #13203a; color: #e4edf8; border: 1px solid #2f4f88; border-radius: 4px; padding: 3px 8px; }"
        "QPushButton:hover { background: #1a2c4f; }"
        "QPushButton:disabled { background: #111722; color: #65748d; border-color: #263244; }"));
    widgets.importFolderBtn->hide();

    widgets.importAnalysisBtn = new QPushButton(QStringLiteral("Run Analysis"), parent);
    widgets.importAnalysisBtn->setCursor(Qt::PointingHandCursor);
    widgets.importAnalysisBtn->setMinimumHeight(24);
    widgets.importAnalysisBtn->setStyleSheet(QStringLiteral(
        "QPushButton { background: #13203a; color: #e4edf8; border: 1px solid #2f4f88; border-radius: 4px; padding: 3px 8px; }"
        "QPushButton:hover { background: #1a2c4f; }"
        "QPushButton:disabled { background: #111722; color: #65748d; border-color: #263244; }"));
    widgets.importAnalysisBtn->hide();

    widgets.footerLabel = new QLabel(QStringLiteral("Ready."), parent);
    widgets.footerLabel->hide();

    auto* splitter = new QSplitter(Qt::Horizontal, parent);
    splitter->setChildrenCollapsible(false);

    // ── Left tree: QFileSystemModel rooted at user home (C:\Users\suppo) ──
    const QString homePath = QStandardPaths::writableLocation(QStandardPaths::HomeLocation);

    widgets.dirModel = new QFileSystemModel(parent);
    widgets.dirModel->setFilter(QDir::AllDirs | QDir::NoDotAndDotDot);
    widgets.dirModel->setRootPath(homePath);
    widgets.dirProxy = new DjBrowserFolderTreeProxyModel(parent);
    widgets.dirProxy->setFileSystemModel(widgets.dirModel);

    widgets.dirView = new QTreeView(parent);
    widgets.dirView->setModel(widgets.dirProxy);
    widgets.dirView->setRootIndex(widgets.dirProxy->indexForPath(homePath));
    widgets.dirView->setHeaderHidden(true);
    widgets.dirView->setMinimumWidth(220);
    widgets.dirView->hideColumn(1);
    widgets.dirView->hideColumn(2);
    widgets.dirView->hideColumn(3);
    widgets.dirView->setStyleSheet(QStringLiteral(
        "QTreeView { background: #0a0c12; color: #d7deea; border: 1px solid #222; }"
        "QTreeView::item:selected { background: #1a2434; color: #f6f8fb; }"));

    auto* leftPane = new QWidget(parent);
    auto* leftLayout = new QVBoxLayout(leftPane);
    leftLayout->setContentsMargins(0, 0, 0, 0);
    leftLayout->setSpacing(4);

    auto* folderHeader = new QHBoxLayout();
    folderHeader->setContentsMargins(0, 0, 0, 0);
    folderHeader->setSpacing(6);

    auto* folderLabel = new QLabel(QStringLiteral("Folders"), leftPane);
    folderLabel->setStyleSheet(QStringLiteral("color: #c9d3e3; font-weight: 600; padding-left: 2px;"));
    folderHeader->addWidget(folderLabel, 0);
    widgets.showAllFoldersBtn = new QPushButton(QStringLiteral("Show All"), leftPane);
    widgets.showAllFoldersBtn->setCursor(Qt::PointingHandCursor);
    widgets.showAllFoldersBtn->setMinimumHeight(22);
    widgets.showAllFoldersBtn->setStyleSheet(QStringLiteral(
        "QPushButton { background: #111722; color: #c9d3e3; border: 1px solid #2a3548; border-radius: 4px; padding: 2px 8px; }"
        "QPushButton:hover { background: #182131; }"
        "QPushButton:disabled { color: #5e6b80; border-color: #1f2838; }"));
    folderHeader->addWidget(widgets.showAllFoldersBtn, 0);
    folderHeader->addStretch(1);

    leftLayout->addLayout(folderHeader);
    leftLayout->addWidget(widgets.dirView, 1);

    // ── Right side: two panes side by side ──
    auto* rightSplitter = new QSplitter(Qt::Horizontal, parent);
    rightSplitter->setChildrenCollapsible(false);
    rightSplitter->setStyleSheet(QStringLiteral(
        "QSplitter::handle:horizontal { background: #0f1520; width: 5px; }"));

    auto* pane1 = buildRightPane(parent, db,
        widgets.fileModel, widgets.fileView,
        widgets.filterBox, widgets.folderLabel, widgets.paneHeader,
        QStringLiteral("Search pane A…"));
    auto* pane2 = buildRightPane(parent, db,
        widgets.fileModel2, widgets.fileView2,
        widgets.filterBox2, widgets.folderLabel2, widgets.paneHeader2,
        QStringLiteral("Search pane B…"));

    rightSplitter->addWidget(pane1);
    rightSplitter->addWidget(pane2);
    rightSplitter->setSizes({600, 600});

    splitter->addWidget(leftPane);
    splitter->addWidget(rightSplitter);
    splitter->setSizes({300, 900});
    layout->addWidget(splitter, 1);

    return widgets;
}

inline void wireInteractions(QObject* owner, const Widgets& widgets, const InteractionHandlers& handlers)
{
    // Pane 1 search
    QObject::connect(widgets.filterBox, &QLineEdit::textChanged, owner, [handlers](const QString& text) {
        if (handlers.setSearchText) handlers.setSearchText(text);
    });

    // Pane 2 search
    QObject::connect(widgets.filterBox2, &QLineEdit::textChanged, owner, [handlers](const QString& text) {
        if (handlers.setSearchText2) handlers.setSearchText2(text);
    });

    // Tree selection → active pane folder
    QObject::connect(widgets.dirView->selectionModel(), &QItemSelectionModel::currentChanged, owner,
        [handlers, dirProxy = widgets.dirProxy](const QModelIndex& current, const QModelIndex&) {
            if (handlers.setFolderPath) handlers.setFolderPath(dirProxy->filePath(current));
        });

    QObject::connect(widgets.dirView, &QTreeView::clicked, owner, [handlers](const QModelIndex&) {
        if (handlers.treeStateChanged) handlers.treeStateChanged();
    });

    QObject::connect(widgets.showAllFoldersBtn, &QPushButton::clicked, owner, [handlers]() {
        if (handlers.treeStateChanged) handlers.treeStateChanged();
    });

    // Pane 1 double-click rename
    QObject::connect(widgets.fileView, &QTableView::doubleClicked, owner, [handlers](const QModelIndex& index) {
        if (!index.isValid() || index.column() != DjBrowserFileTableModel::NameColumn) return;
        if (handlers.beginRename) handlers.beginRename(index);
    });

    // Pane 2 double-click rename
    QObject::connect(widgets.fileView2, &QTableView::doubleClicked, owner, [handlers](const QModelIndex& index) {
        if (!index.isValid() || index.column() != DjBrowserFileTableModel::NameColumn) return;
        if (handlers.beginRename2) handlers.beginRename2(index);
    });

    // Pane 1 header context menu
    {
        auto* header = widgets.fileView->horizontalHeader();
        header->setContextMenuPolicy(Qt::CustomContextMenu);
        QObject::connect(header, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
            if (handlers.showHeaderMenu) handlers.showHeaderMenu(pos);
        });
    }

    // Pane 2 header context menu
    {
        auto* header2 = widgets.fileView2->horizontalHeader();
        header2->setContextMenuPolicy(Qt::CustomContextMenu);
        QObject::connect(header2, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
            if (handlers.showHeaderMenu2) handlers.showHeaderMenu2(pos);
        });
    }

    // Pane 1 file context menu
    widgets.fileView->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(widgets.fileView, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
        if (handlers.showFileMenu) handlers.showFileMenu(pos);
    });

    // Pane 2 file context menu
    widgets.fileView2->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(widgets.fileView2, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
        if (handlers.showFileMenu2) handlers.showFileMenu2(pos);
    });

    // Clicking in either pane makes it the active target for tree selections
    QObject::connect(widgets.fileView, &QAbstractItemView::clicked, owner, [handlers](const QModelIndex&) {
        if (handlers.activatePane1) handlers.activatePane1();
    });
    QObject::connect(widgets.fileView2, &QAbstractItemView::clicked, owner, [handlers](const QModelIndex&) {
        if (handlers.activatePane2) handlers.activatePane2();
    });
}

} // namespace DjBrowserPaneUi