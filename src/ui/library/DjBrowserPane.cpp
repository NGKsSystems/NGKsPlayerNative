#include "DjBrowserPane.h"
#include <QHeaderView>
#include <QHBoxLayout>

DjBrowserPane::DjBrowserPane(QWidget* parent) : QWidget(parent) {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(4);

    searchBox_ = new QLineEdit(this);
    searchBox_->setPlaceholderText(QStringLiteral("Search DJ library..."));
    searchBox_->setStyleSheet(QStringLiteral("background: #111; color: #ccc; border: 1px solid #333; padding: 4px; border-radius: 3px;"));
    layout->addWidget(searchBox_);

    auto* splitter = new QSplitter(Qt::Vertical, this);

    dirModel_ = new QFileSystemModel(this);
    dirModel_->setFilter(QDir::NoDotAndDotDot | QDir::AllDirs);
    dirModel_->setRootPath(QStringLiteral("C:/Users/suppo"));

    dirView_ = new QTreeView(this);
    dirView_->setModel(dirModel_);
    dirView_->setRootIndex(dirModel_->index(QStringLiteral("C:/Users/suppo")));
    for (int i = 1; i < dirModel_->columnCount(); ++i) dirView_->hideColumn(i);
    dirView_->setHeaderHidden(true);
    dirView_->setStyleSheet(QStringLiteral("QTreeView { background: #0a0c12; color: #aaa; border: 1px solid #222; }"));

    fileModel_ = new QFileSystemModel(this);
    fileModel_->setFilter(QDir::NoDotAndDotDot | QDir::Files);
#if QT_VERSION >= QT_VERSION_CHECK(6, 0, 0)
    fileModel_->setNameFilters({QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"), QStringLiteral("*.ogg")});
#else
    fileModel_->setNameFilters(QStringList() << QStringLiteral("*.mp3") << QStringLiteral("*.wav") << QStringLiteral("*.flac") << QStringLiteral("*.ogg"));
#endif
    fileModel_->setNameFilterDisables(false);
    fileModel_->setRootPath(QStringLiteral("C:/Users/suppo"));

    fileView_ = new QTableView(this);
    fileView_->setModel(fileModel_);
    fileView_->setRootIndex(fileModel_->index(QStringLiteral("C:/Users/suppo")));
    fileView_->setSelectionBehavior(QAbstractItemView::SelectRows);
    fileView_->setSelectionMode(QAbstractItemView::SingleSelection);
    fileView_->setDragEnabled(true);
    fileView_->setDragDropMode(QAbstractItemView::DragOnly);
    fileView_->verticalHeader()->hide();
    fileView_->setStyleSheet(QStringLiteral("QTableView { background: #0a0c12; color: #aaa; border: 1px solid #222; } QHeaderView::section { background: #111; color: #aaa; border: none; }"));

    splitter->addWidget(dirView_);
    splitter->addWidget(fileView_);
    splitter->setSizes({250, 400});
    layout->addWidget(splitter, 1);

    connect(dirView_->selectionModel(), &QItemSelectionModel::currentChanged, this, [this](const QModelIndex& current, const QModelIndex&) {
        QString path = dirModel_->filePath(current);
        fileView_->setRootIndex(fileModel_->setRootPath(path)); 
    });
}
