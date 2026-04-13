#include "DjBrowserPane.h"
#include <QGuiApplication>
#include <QClipboard>
#include <QFile>
#include <QSettings>

#include <QSortFilterProxyModel>
#include <QDirIterator>
#include <QFileInfo>
#include <QString>
#include <QDialog>
#include <QFormLayout>
#include <QDialogButtonBox>
#include <QMessageBox>
#include <QInputDialog>

#include <QDialog>
#include <QFormLayout>
#include <QDialogButtonBox>
#include <QMessageBox>
#include <QInputDialog>

class MediaDirFilterProxy : public QSortFilterProxyModel {
public:
    MediaDirFilterProxy(QObject* parent = nullptr) : QSortFilterProxyModel(parent) {}

protected:
    bool filterAcceptsRow(int source_row, const QModelIndex& source_parent) const override {
        auto* fsModel = qobject_cast<QFileSystemModel*>(sourceModel());
        if (!fsModel) return true;
        
        QModelIndex idx = fsModel->index(source_row, 0, source_parent);
        QFileInfo fi = fsModel->fileInfo(idx);
        
        if (fi.fileName().startsWith(".")) return false; // Hide dotfiles like .vscode, .templateengine
        if (fi.fileName().startsWith("_") && fi.fileName() != "_Music") return false; // Hide _proof_remote, etc.
        
        // Hide common non-media folders on Windows
        QString lowerName = fi.fileName().toLower();
        if (lowerName == "appdata" || lowerName == "windows" || lowerName == "program files" || 
            lowerName == "program files (x86)" || lowerName == "programdata" || lowerName == "contacts" || 
            lowerName == "desktop" || lowerName == "documents" || lowerName == "favorites" || 
            lowerName == "links" || lowerName == "saved games" || lowerName == "searches" || 
            lowerName == "go" || lowerName == "public" || lowerName == "pictures" || 
            lowerName == "videos" || lowerName == "3d objects" || lowerName == "node_modules" ||
            lowerName == "onedrive" || lowerName == "roaming" || lowerName == "local" ||
            lowerName.endsWith("-autologger")) {
            return false;
        }

        return true;
    }
};

#include <QHeaderView>
#include <QHBoxLayout>
#include <QMenu>
#include <QAction>
#include <QMimeData>
#include <QUrl>
#include <QFileSystemModel>
#include <QJsonDocument>
#include <QJsonObject>
#include <QHash>
#include <QDir>
#include <QFileInfo>
#include <QThreadPool>
#include <QMetaObject>
#include "../TagReaderService.h"

#include <QPointer>

class FileViewProxyModel : public QSortFilterProxyModel {
    mutable QHash<QString, QStringList> trackMetadataCache_;
    
public:
    FileViewProxyModel(QObject* parent = nullptr) : QSortFilterProxyModel(parent) {}
    
    int columnCount(const QModelIndex& parent = QModelIndex()) const override {
        return sourceModel() ? sourceModel()->columnCount(parent) + 4 : 0;
    }
    
    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override {
        if (!index.isValid() || !sourceModel()) return QVariant();
        int srcCols = sourceModel()->columnCount();
        if (index.column() >= srcCols) {
            if (role == Qt::DisplayRole) {
                auto* fsModel = qobject_cast<QFileSystemModel*>(sourceModel());
                if (!fsModel) return QString("-");

                QModelIndex baseIndex = QSortFilterProxyModel::index(index.row(), 0, index.parent());
                QModelIndex srcIndex = mapToSource(baseIndex);
                if (!srcIndex.isValid()) return QString("-");

                QString path = fsModel->filePath(srcIndex);
                if (path.isEmpty()) return QString("-");

                if (!trackMetadataCache_.contains(path)) {
                    // Pre-fill cache with "-" to stop re-submitting loading jobs while scrolling
                    trackMetadataCache_.insert(path, { "-", "-", "-", "-" });

                    // Run the expensive tag+JSON fetching off the UI thread
                    QString cacheFileName = QFileInfo(path).completeBaseName() + ".analysis.json";
                    QString pwdCache = QDir::currentPath() + "/analysis_cache/" + cacheFileName;
                    QString exeRelativeCache = QCoreApplication::applicationDirPath() + "/../../../analysis_cache/" + cacheFileName;
                    QString siblingCache = QFileInfo(path).dir().path() + "/../analysis_cache/" + cacheFileName;
                    
                    // We must pass 'this' to the lambda safely, use a QPointer
                    QPointer<const FileViewProxyModel> safeThis(this);
                    
                    QThreadPool::globalInstance()->start([safeThis, path, baseIndex, pwdCache, exeRelativeCache, siblingCache]() mutable {
                        QStringList md = { "-", "-", "-", "-" };
                        
                        // 1) Read physical tags (BPM, Key, LUFS, Genre) WITHOUT album art
                        TrackTagData tags = TagReaderService::loadTagsForFile(path, true);
                        if (!tags.bpm.isEmpty()) md[0] = tags.bpm;
                        if (!tags.musicalKey.isEmpty()) md[1] = tags.musicalKey;
                        // For lufs, limit it to 1 decimal point format if it exists
                        if (tags.loudnessLUFS != 0.0) md[2] = QString::number(tags.loudnessLUFS, 'f', 1);
                        if (!tags.genre.isEmpty()) md[3] = tags.genre;

                        // 2) Look for exact analysis json to override any stale tags with fresh ML numbers
                        QFile f(pwdCache);
                        if (!f.exists()) f.setFileName(exeRelativeCache);
                        if (!f.exists()) f.setFileName(siblingCache);
                        
                        if (f.exists() && f.open(QIODevice::ReadOnly)) {
                            QJsonObject obj = QJsonDocument::fromJson(f.readAll()).object();
                            if (obj.contains("final_bpm")) {
                                md[0] = QString::number(qRound(obj["final_bpm"].toDouble()));
                            }
                            if (obj.contains("final_key_name")) {
                                md[1] = obj["final_key_name"].toString();
                            } else if (obj.contains("final_key")) {
                                md[1] = obj["final_key"].toString();
                            }
                            if (obj.contains("lufs")) {
                                md[2] = QString::number(obj["lufs"].toDouble(), 'f', 1);
                            }
                            if (obj.contains("genre")) {
                                md[3] = obj["genre"].toString();
                            }
                        }

                        // Jump back to UI thread to set cache and trigger UI repaint
                        QMetaObject::invokeMethod(const_cast<FileViewProxyModel*>(safeThis.data()), [safeThis, path, baseIndex, md]() {
                            if (safeThis) {
                                auto* nonConstThis = const_cast<FileViewProxyModel*>(safeThis.data());
                                nonConstThis->trackMetadataCache_[path] = md;
                                // Emit dataChanged for all extra columns (-4 to -1 index.column())
                                int cols = nonConstThis->columnCount();
                                QModelIndex iLeft = nonConstThis->index(baseIndex.row(), cols - 4, baseIndex.parent());
                                QModelIndex iRight = nonConstThis->index(baseIndex.row(), cols - 1, baseIndex.parent());
                                emit nonConstThis->dataChanged(iLeft, iRight, QVector<int>{Qt::DisplayRole});
                            }
                        }, Qt::QueuedConnection);
                    });
                }

                int extraCol = index.column() - srcCols;
                if (extraCol >= 0 && extraCol < 4) {
                    return trackMetadataCache_[path][extraCol];
                }
                return QString("-");
            }
            if (role == Qt::TextAlignmentRole) {
                return Qt::AlignCenter;
            }
            return QVariant(); // other roles
        }
        return QSortFilterProxyModel::data(index, role);
    }

QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override {
        if (!sourceModel()) return QVariant();
        if (orientation == Qt::Horizontal && role == Qt::DisplayRole) {
            int srcCols = sourceModel()->columnCount();
            if (section == srcCols) return QString("BPM");
            if (section == srcCols + 1) return QString("Key");
            if (section == srcCols + 2) return QString("LUFS");
            if (section == srcCols + 3) return QString("Genre");
        }
        return QSortFilterProxyModel::headerData(section, orientation, role);
    }

    QModelIndex index(int row, int column, const QModelIndex& parent = QModelIndex()) const override {
        if (row < 0 || column < 0) return QModelIndex();
        
        int sourceCols = sourceModel() ? sourceModel()->columnCount() : 0;
        if (column >= sourceCols) {
            // For custom columns, use column 0's mapping internally just to store a valid proxy index
            QModelIndex baseIndex = QSortFilterProxyModel::index(row, 0, parent);
            if (!baseIndex.isValid()) return QModelIndex();
            return createIndex(row, column, baseIndex.internalPointer());
        }
        return QSortFilterProxyModel::index(row, column, parent);
    }

    QModelIndex parent(const QModelIndex& child) const override {
        if (!child.isValid()) return QModelIndex();
        int sourceCols = sourceModel() ? sourceModel()->columnCount() : 0;
        if (child.column() >= sourceCols) {
            // Reconstruct the base index for column 0 to query the parent
            QModelIndex baseIndex = createIndex(child.row(), 0, child.internalPointer());
            return QSortFilterProxyModel::parent(baseIndex);
        }
        return QSortFilterProxyModel::parent(child);
    }

    QModelIndex mapToSource(const QModelIndex& proxyIndex) const override {
        if (!proxyIndex.isValid()) return QModelIndex();
        int sourceCols = sourceModel() ? sourceModel()->columnCount() : 0;
        if (proxyIndex.column() >= sourceCols) {
            // Custom columns don't have a direct source model index in that column.
            // Map column 0 instead, then return an invalid column, or just return column 0's source index?
            // Returning an invalid index is safer so source doesn't process it.
            return QModelIndex();
        }
        return QSortFilterProxyModel::mapToSource(proxyIndex);
    }

    Qt::ItemFlags flags(const QModelIndex& index) const override {
        if (!index.isValid()) return Qt::NoItemFlags;
        if (index.column() >= (sourceModel() ? sourceModel()->columnCount() : 0)) {
            return Qt::ItemIsEnabled | Qt::ItemIsSelectable | Qt::ItemIsDragEnabled;
        }
        return QSortFilterProxyModel::flags(index) | Qt::ItemIsDragEnabled;
    }

    QMimeData* mimeData(const QModelIndexList& indexes) const override {
        auto* fsModel = qobject_cast<QFileSystemModel*>(sourceModel());
        if (!fsModel) return QSortFilterProxyModel::mimeData(indexes);

        QModelIndexList validIndexes;
        for (const QModelIndex& idx : indexes) {
            if (idx.column() == 0) {
                validIndexes.append(mapToSource(idx));
            }
        }

        QMimeData* mime = fsModel->mimeData(validIndexes);
        if (mime) {
            if (mime->hasUrls() && !mime->urls().isEmpty()) {
                QString path = mime->urls().first().toLocalFile();
                mime->setData(QStringLiteral("application/x-ngks-dj-track"), path.toUtf8());
                mime->setText(path); // Extra safe for dropEvent fallback
            }
        }
        return mime;
    }

    QStringList mimeTypes() const override {
        auto* fsModel = qobject_cast<QFileSystemModel*>(sourceModel());
        QStringList types = fsModel ? fsModel->mimeTypes() : QSortFilterProxyModel::mimeTypes();
        types << QStringLiteral("application/x-ngks-dj-track");
        return types;
    }

    Qt::DropActions supportedDragActions() const override {
        return sourceModel() ? sourceModel()->supportedDragActions() : Qt::CopyAction;
    }
};

DjBrowserPane::DjBrowserPane(QWidget* parent) : QWidget(parent) {
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(4);

    auto* splitter = new QSplitter(Qt::Vertical, this);

    dirModel_ = new QFileSystemModel(this);
    dirModel_->setFilter(QDir::NoDotAndDotDot | QDir::AllDirs);
    dirModel_->setRootPath(QStringLiteral("C:/Users/suppo"));

    dirView_ = new QTreeView(this);
    auto* proxy = new MediaDirFilterProxy(this);
    proxy->setSourceModel(dirModel_);
    dirView_->setModel(proxy);
    dirView_->setRootIndex(proxy->mapFromSource(dirModel_->index(QStringLiteral("C:/Users/suppo"))));
    for (int i = 1; i < dirModel_->columnCount(); ++i) dirView_->hideColumn(i);
    dirView_->setHeaderHidden(true);
    dirView_->setStyleSheet(QStringLiteral("QTreeView { background: #0a0c12; color: #aaa; border: 1px solid #222; }"));

    fileModel_ = new QFileSystemModel(this);
    fileModel_->setFilter(QDir::NoDotAndDotDot | QDir::Files);
    fileModel_->setReadOnly(false);
#if QT_VERSION >= QT_VERSION_CHECK(6, 0, 0)
    fileModel_->setNameFilters({QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"), QStringLiteral("*.ogg")});
#else
    fileModel_->setNameFilters(QStringList() << QStringLiteral("*.mp3") << QStringLiteral("*.wav") << QStringLiteral("*.flac") << QStringLiteral("*.ogg"));
#endif
    fileModel_->setNameFilterDisables(false);
    fileModel_->setRootPath(QStringLiteral("C:/Users/suppo"));

    fileView_ = new QTableView(this);
    auto* fProxy = new FileViewProxyModel(this);
    fProxy->setSourceModel(fileModel_);
    fileView_->setModel(fProxy);
    fileView_->setRootIndex(fProxy->mapFromSource(fileModel_->index(QStringLiteral("C:/Users/suppo"))));
    fileView_->setSelectionBehavior(QAbstractItemView::SelectRows);
    fileView_->setSelectionMode(QAbstractItemView::SingleSelection);
    fileView_->setDragEnabled(true);
    fileView_->setDragDropMode(QAbstractItemView::DragOnly);
    fileView_->setDefaultDropAction(Qt::CopyAction);
    fileView_->verticalHeader()->hide();
    fileView_->setStyleSheet(QStringLiteral("QTableView { background: #0a0c12; color: #aaa; border: 1px solid #222; selection-background-color: #004488; } QHeaderView::section { background: #111; color: #aaa; border: none; } QLineEdit { background: #222; color: #fff; border: 1px solid #666; }"));

    fileView_->horizontalHeader()->setContextMenuPolicy(Qt::CustomContextMenu);
    connect(fileView_->horizontalHeader(), &QWidget::customContextMenuRequested, this, [this](const QPoint& pos) {
        QMenu menu(this);
        menu.setStyleSheet(QStringLiteral("QMenu { background: #1a1a1a; color: #ddd; border: 1px solid #444; } QMenu::item:selected { background: #333; }"));
        auto* fp = dynamic_cast<FileViewProxyModel*>(fileView_->model());
        int cols = fp ? fp->columnCount() : fileModel_->columnCount();
        for (int i = 0; i < cols; ++i) {
            QString head = fp ? fp->headerData(i, Qt::Horizontal).toString() : fileModel_->headerData(i, Qt::Horizontal).toString();
            auto* action = menu.addAction(head);
            action->setCheckable(true);
            action->setChecked(!fileView_->isColumnHidden(i));
            connect(action, &QAction::toggled, this, [this, i](bool checked) {
                fileView_->setColumnHidden(i, !checked);
            });
        }
        menu.exec(fileView_->horizontalHeader()->mapToGlobal(pos));
    });

    fileView_->setContextMenuPolicy(Qt::CustomContextMenu);
        connect(fileView_, &QWidget::customContextMenuRequested, this, [this](const QPoint& pos) {
            QModelIndex proxyIdx = fileView_->indexAt(pos);
            if (!proxyIdx.isValid()) return;
            auto* fp = dynamic_cast<FileViewProxyModel*>(fileView_->model());
            QModelIndex srcIdx = fp ? fp->mapToSource(proxyIdx) : proxyIdx;

            QString path = QDir::toNativeSeparators(fileModel_->filePath(srcIdx));
            QString nativePath = QDir::toNativeSeparators(path);
            bool isDir = fileModel_->isDir(srcIdx);

            QMenu menu(this);
        menu.setStyleSheet(QStringLiteral("QMenu { background: #1a1a1a; color: #ddd; border: 1px solid #444; } QMenu::item:selected { background: #333; } QMenu::separator { height: 1px; background: #555; margin: 4px 0; }"));
        
        auto* aCut = menu.addAction("Cut");
        auto* aCopy = menu.addAction("Copy");
        auto* aPaste = menu.addAction("Paste");
        menu.addSeparator();
        auto* aRename = menu.addAction("Rename (single item)");
        auto* aBatchRename = menu.addAction("Batch Rename (Find/Replace in Folder)");
        auto* aDelete = menu.addAction("Delete");

        connect(aBatchRename, &QAction::triggered, this, [this, path, isDir]() {
            QString targetDir = isDir ? path : QFileInfo(path).absolutePath();
            QDialog dlg(this);
            dlg.setWindowTitle("Batch Rename (Find/Replace)");
            dlg.setStyleSheet("background: #222; color: #fff;");
            QFormLayout form(&dlg);
            QLineEdit tFind(&dlg);
            QLineEdit tRep(&dlg);
            tFind.setStyleSheet("background: #111; border: 1px solid #555; padding: 2px; color: #fff;");
            tRep.setStyleSheet("background: #111; border: 1px solid #555; padding: 2px; color: #fff;");
            form.addRow("Find:", &tFind);
            form.addRow("Replace with:", &tRep);
            QDialogButtonBox btns(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, Qt::Horizontal, &dlg);
            form.addRow(&btns);
            connect(&btns, &QDialogButtonBox::accepted, &dlg, &QDialog::accept);
            connect(&btns, &QDialogButtonBox::rejected, &dlg, &QDialog::reject);
            if (dlg.exec() == QDialog::Accepted) {
                QString fw = tFind.text();
                QString rw = tRep.text();
                if (fw.isEmpty()) return;
                QDir dir(targetDir);
                QStringList exts = {"*.mp3", "*.wav", "*.flac", "*.ogg"};
                QStringList items = dir.entryList(exts, QDir::Files);
                int ren = 0;
                for (const QString& f : items) {
                    if (f.contains(fw)) {
                        QString newF = f;
                        newF.replace(fw, rw);
                        if (dir.rename(f, newF)) {
                            ren++;
                        }
                    }
                }
                QMessageBox msg(this);
                msg.setStyleSheet("background: #222; color: #fff;");
                msg.setWindowTitle("Batch Rename");
                msg.setText(QString("Renamed %1 files in %2").arg(ren).arg(targetDir));
                msg.exec();
            }
        });

        connect(aRename, &QAction::triggered, this, [this, proxyIdx]() {
            fileView_->edit(proxyIdx);
        });
        connect(aDelete, &QAction::triggered, this, [this, path]() {
            QFile::remove(path); // Hard delete file to fix "delete doesn't work"
        });

        connect(aCopy, &QAction::triggered, this, [path]() {
            QGuiApplication::clipboard()->setText(path);
        });
        
        connect(aCut, &QAction::triggered, this, [path]() {
            QGuiApplication::clipboard()->setText("CUT:" + path);
        });

        connect(aPaste, &QAction::triggered, this, [this, path]() {
            QString clp = QGuiApplication::clipboard()->text();
            if(!clp.isEmpty() && clp.startsWith("CUT:")) {
                QString src = clp.mid(4);
                QFile::rename(src, QFileInfo(path).absolutePath() + "/" + QFileInfo(src).fileName());
            } else if (!clp.isEmpty()) {
                QFile::copy(clp, QFileInfo(path).absolutePath() + "/Copy_" + QFileInfo(clp).fileName());
            }
        });
        
        if (!isDir) {
            menu.addSeparator();
            auto* aAnalyze = menu.addAction("Analyze (Regular)");
            auto* aAnalyzeLive = menu.addAction("Analyze (Live Type - Background)");
            menu.addSeparator();
            auto* aLoadDeck1 = menu.addAction("Load to Deck A");
            auto* aLoadDeck2 = menu.addAction("Load to Deck B");
            auto* aQueue = menu.addAction("Place in Queue");

            connect(aAnalyze, &QAction::triggered, this, [this, nativePath]() { emit requestAnalyze(nativePath, false); });
            connect(aAnalyzeLive, &QAction::triggered, this, [this, nativePath]() { emit requestAnalyze(nativePath, true); });
            connect(aLoadDeck1, &QAction::triggered, this, [this, nativePath]() { emit requestLoadDeck(nativePath, 1); });
            connect(aLoadDeck2, &QAction::triggered, this, [this, nativePath]() { emit requestLoadDeck(nativePath, 2); });
            connect(aQueue, &QAction::triggered, this, [this, nativePath]() { emit requestEnqueue(nativePath); });
        }

        menu.exec(fileView_->viewport()->mapToGlobal(pos));
    });

    
    QWidget* dirContainer = new QWidget(this);
    QVBoxLayout* dirLayout = new QVBoxLayout(dirContainer);
    dirLayout->setContentsMargins(0, 0, 0, 0);
    dirLayout->setSpacing(2);
    dirLayout->addWidget(dirView_, 1);
    
    searchBox_ = new QLineEdit(this);
    searchBox_->setPlaceholderText(QStringLiteral("Search DJ library..."));
    searchBox_->setStyleSheet(QStringLiteral("background: #111; color: #ccc; border: 1px solid #555; padding: 4px; border-radius: 3px;"));
    dirLayout->addWidget(searchBox_);

    connect(searchBox_, &QLineEdit::textChanged, this, [this](const QString& text) {
        if (text.isEmpty()) {
#if QT_VERSION >= QT_VERSION_CHECK(6, 0, 0)
            fileModel_->setNameFilters({QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"), QStringLiteral("*.ogg")});
#else
            fileModel_->setNameFilters(QStringList() << QStringLiteral("*.mp3") << QStringLiteral("*.wav") << QStringLiteral("*.flac") << QStringLiteral("*.ogg"));
#endif
        } else {
            fileModel_->setNameFilters({ "*" + text + "*.mp3", "*" + text + "*.wav", "*" + text + "*.flac", "*" + text + "*.ogg" });
        }
    });

    splitter->addWidget(dirContainer);
    splitter->addWidget(fileView_);
    splitter->setSizes({250, 400});
    layout->addWidget(splitter, 1);

    connect(dirView_->selectionModel(), &QItemSelectionModel::currentChanged, this, [this](const QModelIndex& current, const QModelIndex&) {
        auto* proxy = qobject_cast<QSortFilterProxyModel*>(dirView_->model());
        QString path = dirModel_->filePath(proxy ? proxy->mapToSource(current) : current);
        QModelIndex srcIdx = fileModel_->setRootPath(path);
        auto* fp = dynamic_cast<FileViewProxyModel*>(fileView_->model());
        fileView_->setRootIndex(fp ? fp->mapFromSource(srcIdx) : srcIdx);
    });

    restoreHeaderState();
}

DjBrowserPane::~DjBrowserPane() {
    saveHeaderState();
}

void DjBrowserPane::saveHeaderState() {
    QSettings settings(QStringLiteral("NGKsSystems"), QStringLiteral("NGKsPlayerNative"));
    settings.setValue(QStringLiteral("DjBrowserPane/headerState"), fileView_->horizontalHeader()->saveState());
}

void DjBrowserPane::restoreHeaderState() {
    QSettings settings(QStringLiteral("NGKsSystems"), QStringLiteral("NGKsPlayerNative"));
    if (settings.contains(QStringLiteral("DjBrowserPane/headerState"))) {
        fileView_->horizontalHeader()->restoreState(settings.value(QStringLiteral("DjBrowserPane/headerState")).toByteArray());
    }
}
