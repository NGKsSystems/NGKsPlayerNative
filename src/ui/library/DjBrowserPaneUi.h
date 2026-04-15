#pragma once

#include "ui/library/DjBrowserFileTableModel.h"
#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/TrackDragView.h"

#include <QAbstractItemView>
#include <QFileSystemModel>
#include <QHeaderView>
#include <QHBoxLayout>
#include <QItemSelectionModel>
#include <QLabel>
#include <QLineEdit>
#include <QSplitter>
#include <QTreeView>
#include <QVBoxLayout>

#include <functional>

namespace DjBrowserPaneUi {

struct Widgets {
    QFileSystemModel* dirModel{};
    DjBrowserFileTableModel* fileModel{};
    QTreeView* dirView{};
    TrackDragView* fileView{};
    QLineEdit* filterBox{};
    QLabel* footerLabel{};
};

struct InteractionHandlers {
    std::function<void(const QString&)> setSearchText;
    std::function<void(const QString&)> setFolderPath;
    std::function<void(const QModelIndex&)> beginRename;
    std::function<void(const QPoint&)> showHeaderMenu;
    std::function<void(const QPoint&)> showFileMenu;
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

inline Widgets build(QWidget* parent, DjLibraryDatabase* db)
{
    Widgets widgets;

    auto* layout = new QVBoxLayout(parent);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(4);

    auto* topBar = new QHBoxLayout();
    topBar->setContentsMargins(0, 0, 0, 0);
    topBar->setSpacing(6);

    widgets.filterBox = createSearchBox(parent, QStringLiteral("Search current folder..."));
    topBar->addWidget(widgets.filterBox, 1);

    layout->addLayout(topBar);

    widgets.footerLabel = new QLabel(QStringLiteral("Ready."), parent);

    auto* splitter = new QSplitter(Qt::Vertical, parent);

    widgets.dirModel = new QFileSystemModel(parent);
    widgets.dirModel->setFilter(QDir::NoDotAndDotDot | QDir::AllDirs);
    widgets.dirModel->setRootPath(QStringLiteral("C:/Users/suppo"));

    widgets.dirView = new QTreeView(parent);
    widgets.dirView->setModel(widgets.dirModel);
    widgets.dirView->setRootIndex(widgets.dirModel->index(QStringLiteral("C:/Users/suppo")));
    for (int i = 1; i < widgets.dirModel->columnCount(); ++i) widgets.dirView->hideColumn(i);
    widgets.dirView->setHeaderHidden(true);
    widgets.dirView->setStyleSheet(QStringLiteral("QTreeView { background: #0a0c12; color: #aaa; border: 1px solid #222; }"));

    widgets.fileModel = new DjBrowserFileTableModel(db, parent);
    widgets.fileModel->setFolderPath(QStringLiteral("C:/Users/suppo"));

    widgets.fileView = new TrackDragView(parent);
    widgets.fileView->setModel(widgets.fileModel);
    widgets.fileView->setSelectionBehavior(QAbstractItemView::SelectRows);
    widgets.fileView->setSelectionMode(QAbstractItemView::SingleSelection);
    widgets.fileView->setAlternatingRowColors(true);
    widgets.fileView->setShowGrid(false);
    widgets.fileView->setMouseTracking(true);
    widgets.fileView->setEditTriggers(QAbstractItemView::EditKeyPressed);
    widgets.fileView->setDragEnabled(true);
    widgets.fileView->setDragDropMode(QAbstractItemView::DragOnly);
    widgets.fileView->setDefaultDropAction(Qt::CopyAction);
    widgets.fileView->setSortingEnabled(true);
    widgets.fileView->verticalHeader()->hide();
    widgets.fileView->horizontalHeader()->setStretchLastSection(true);
    widgets.fileView->horizontalHeader()->setSectionsClickable(true);
    widgets.fileView->horizontalHeader()->setSortIndicatorShown(true);
    widgets.fileView->setItemDelegateForColumn(DjBrowserFileTableModel::NameColumn, new DjBrowserNameDelegate(widgets.fileView));
    widgets.fileView->setStyleSheet(QStringLiteral(
        "QTableView { background: #0a0c12; color: #d7deea; border: 1px solid #222; gridline-color: #151b27; alternate-background-color: #0d1119; selection-background-color: #f28c28; selection-color: #10141f; }"
        "QTableView::item { padding: 2px 6px; }"
        "QTableView::item:selected { background: #f28c28; color: #10141f; }"
        "QTableView::item:hover { background: #1a2434; color: #f6f8fb; }"
        "QHeaderView::section { background: #111; color: #c9d3e3; border: none; padding: 4px 6px; }"));
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::NameColumn, 210);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::SizeColumn, 82);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::TypeColumn, 120);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::DateModifiedColumn, 140);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::BpmColumn, 60);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::KeyColumn, 70);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::CamelotColumn, 80);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::LufsColumn, 70);
    widgets.fileView->setColumnWidth(DjBrowserFileTableModel::GenreColumn, 110);
    widgets.fileView->sortByColumn(DjBrowserFileTableModel::NameColumn, Qt::AscendingOrder);

    splitter->addWidget(widgets.dirView);
    splitter->addWidget(widgets.fileView);
    splitter->setSizes({250, 400});
    layout->addWidget(splitter, 1);
    layout->addWidget(widgets.footerLabel);

    return widgets;
}

inline void wireInteractions(QObject* owner, const Widgets& widgets, const InteractionHandlers& handlers)
{
    QObject::connect(widgets.filterBox, &QLineEdit::textChanged, owner, [handlers](const QString& text) {
        if (handlers.setSearchText) handlers.setSearchText(text);
    });

    QObject::connect(widgets.dirView->selectionModel(), &QItemSelectionModel::currentChanged, owner,
        [handlers, dirModel = widgets.dirModel](const QModelIndex& current, const QModelIndex&) {
            if (handlers.setFolderPath) handlers.setFolderPath(dirModel->filePath(current));
        });

    QObject::connect(widgets.fileView, &QTableView::doubleClicked, owner, [handlers](const QModelIndex& index) {
        if (!index.isValid() || index.column() != DjBrowserFileTableModel::NameColumn) return;
        if (handlers.beginRename) handlers.beginRename(index);
    });

    auto* header = widgets.fileView->horizontalHeader();
    header->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(header, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
        if (handlers.showHeaderMenu) handlers.showHeaderMenu(pos);
    });

    widgets.fileView->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(widgets.fileView, &QWidget::customContextMenuRequested, owner, [handlers](const QPoint& pos) {
        if (handlers.showFileMenu) handlers.showFileMenu(pos);
    });
}

} // namespace DjBrowserPaneUi