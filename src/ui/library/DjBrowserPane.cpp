#include "DjBrowserPane.h"

#include "ui/library/DjBrowserAnalysisCoordinator.h"
#include "ui/library/DjBrowserFileTableModel.h"
#include "ui/library/DjBrowserMenuController.h"
#include "ui/library/DjBrowserPaneActions.h"
#include "ui/library/DjBrowserPaneUi.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/LibraryScanner.h"
#include "ui/library/LibraryPersistence.h"

#include <QClipboard>
#include <QDir>
#include <QFileInfo>
#include <QHeaderView>
#include <QSignalBlocker>
#include <QMimeData>
#include <QStandardPaths>

namespace {

const QString kBrowserHeaderStateKey = QStringLiteral("dj_browser/file_header_state");
const QString kBrowserHiddenFoldersKey = QStringLiteral("dj_browser/hidden_folders");

QString normalizedPath(const QString& path)
{
    return QDir::fromNativeSeparators(QDir(path).absolutePath());
}

}


DjBrowserPane::DjBrowserPane(DjLibraryDatabase* db, QWidget* parent)
    : QWidget(parent)
    , db_(db)
    , dirModel_(nullptr)
    , dirProxy_(nullptr)
    , fileModel_(nullptr)
    , fileModel2_(nullptr)
    , dirView_(nullptr)
    , fileView_(nullptr)
    , fileView2_(nullptr)
    , paneHeader_(nullptr)
    , paneHeader2_(nullptr)
    , folderLabel_(nullptr)
    , folderLabel2_(nullptr)
    , importFolderBtn_(nullptr)
    , importAnalysisBtn_(nullptr)
    , showAllFoldersBtn_(nullptr)
    , footerLabel_(nullptr)
    , analysisCoordinator_(nullptr)
    , activePaneIndex_(0)
{
    const auto widgets = DjBrowserPaneUi::build(this, db_);
    dirModel_ = widgets.dirModel;
    dirProxy_ = widgets.dirProxy;
    fileModel_ = widgets.fileModel;
    fileModel2_ = widgets.fileModel2;
    dirView_ = widgets.dirView;
    fileView_ = widgets.fileView;
    fileView2_ = widgets.fileView2;
    paneHeader_ = widgets.paneHeader;
    paneHeader2_ = widgets.paneHeader2;
    folderLabel_ = widgets.folderLabel;
    folderLabel2_ = widgets.folderLabel2;
    importFolderBtn_ = widgets.importFolderBtn;
    importAnalysisBtn_ = widgets.importAnalysisBtn;
    showAllFoldersBtn_ = widgets.showAllFoldersBtn;
    footerLabel_ = widgets.footerLabel;

    QObject::connect(importFolderBtn_, &QPushButton::clicked, this, [this]() {
        emit importFolderRequested();
    });
    QObject::connect(importAnalysisBtn_, &QPushButton::clicked, this, [this]() {
        emit importAnalysisRequested();
    });

    if (footerLabel_) {
        footerLabel_->setStyleSheet(DjBrowserUiFeedback::footerStyleForTone(QStringLiteral("info")));
        footerLabel_->hide();
    }
    analysisCoordinator_ = new DjBrowserAnalysisCoordinator(db_, this);
    analysisCoordinator_->setFooterCallback([this](const QString& text, const QString& tone) {
        updateFooterMessage(text, tone);
    });
    analysisCoordinator_->setRefreshCallback([this]() {
        fileModel_->refresh();
        if (fileModel2_) fileModel2_->refresh();
    });

    restoreHeaderState();
    restoreHiddenFolderState();
    updateHiddenFoldersButtonState();

    auto* header = fileView_->horizontalHeader();
    QObject::connect(header, &QHeaderView::sectionMoved, this, [this]() { persistHeaderState(); });
    QObject::connect(header, &QHeaderView::sectionResized, this, [this]() { persistHeaderState(); });
    QObject::connect(header, &QHeaderView::sortIndicatorChanged, this, [this]() { persistHeaderState(); });

    DjBrowserPaneUi::wireInteractions(this, widgets, {
        [this](const QString& text) { fileModel_->setSearchText(text); },
        [this](const QString& path) {
            if (path.isEmpty()) return;
            if (activePaneIndex_ == 0) {
                fileModel_->setFolderPath(path);
                if (folderLabel_) folderLabel_->setText(QFileInfo(path).fileName());
            } else {
                if (fileModel2_) fileModel2_->setFolderPath(path);
                if (folderLabel2_) folderLabel2_->setText(QFileInfo(path).fileName());
            }
        },
        [this]() {
            normalizePaneFolders();
            persistHiddenFolderState();
            updateHiddenFoldersButtonState();
        },
        [this](const QModelIndex& index) { fileView_->edit(index); },
        [this](const QPoint& pos) { showHeaderContextMenu(pos); },
        [this](const QPoint& pos) { showFileContextMenu(pos); },
        [this](const QString& text) { if (fileModel2_) fileModel2_->setSearchText(text); },
        [this](const QModelIndex& index) { if (fileView2_) fileView2_->edit(index); },
        [this](const QPoint& pos) { Q_UNUSED(pos) /* header menu pane 2 – same as pane 1 via TODO */ },
        [this](const QPoint& pos) { showFileContextMenu2(pos); },
        [this]() { setActivePaneIndex(0); },
        [this]() { setActivePaneIndex(1); }
    });

    QObject::connect(dirProxy_, &QAbstractItemModel::dataChanged, this,
        [this](const QModelIndex&, const QModelIndex&, const QList<int>& roles) {
            if (!roles.isEmpty() && !roles.contains(Qt::CheckStateRole)) return;
            normalizePaneFolders();
            persistHiddenFolderState();
            updateHiddenFoldersButtonState();
        });

    QObject::connect(showAllFoldersBtn_, &QPushButton::clicked, this, [this]() {
        if (!dirProxy_ || !dirProxy_->hasHiddenPaths()) return;
        dirProxy_->resetHiddenPaths();
        persistHiddenFolderState();
        updateHiddenFoldersButtonState();
        normalizePaneFolders();
        updateFooterMessage(QStringLiteral("All hidden folders are visible again."), QStringLiteral("info"));
    });

    // Clicking pane headers also activates the respective pane
    auto installPaneHeaderFilter = [this](QWidget* header) {
        if (!header) return;
        header->installEventFilter(this);
        for (QObject* child : header->children()) {
            if (QWidget* w = qobject_cast<QWidget*>(child))
                w->installEventFilter(this);
        }
    };
    installPaneHeaderFilter(paneHeader_);
    installPaneHeaderFilter(paneHeader2_);

    updatePaneHeaderStyles();
}

void DjBrowserPane::setBrowserRootFolder(const QString& folderPath)
{
    if (!dirModel_ || !dirProxy_ || !dirView_ || !fileModel_) return;

    const QString resolvedPath = folderPath.trimmed().isEmpty()
        ? QDir::homePath()
        : QDir(folderPath).absolutePath();
    const QString normalizedImport = nearestVisibleFolderPath(resolvedPath);

    // Navigate the QFileSystemModel to select and expand to the imported folder
    const QModelIndex idx = dirProxy_->indexForPath(normalizedImport);
    if (idx.isValid()) {
        QModelIndex parent = idx.parent();
        while (parent.isValid()) {
            dirView_->expand(parent);
            parent = parent.parent();
        }
        dirView_->setCurrentIndex(idx);
        dirView_->scrollTo(idx, QAbstractItemView::PositionAtCenter);
    }

    fileModel_->setFolderPath(normalizedImport);
    if (folderLabel_) folderLabel_->setText(QFileInfo(normalizedImport).fileName());

    if (fileModel2_) fileModel2_->setFolderPath(normalizedImport);
    if (folderLabel2_) folderLabel2_->setText(QFileInfo(normalizedImport).fileName());
}

void DjBrowserPane::showHeaderContextMenu(const QPoint& pos)
{
    auto* header = fileView_->horizontalHeader();
    const auto action = DjBrowserMenuController::showHeaderContextMenu(
        this,
        header,
        fileModel_,
        [this](int column) { return fileView_->isColumnHidden(column); },
        pos,
        DjBrowserPaneUi::menuStyle());

    if (action.kind == DjBrowserMenuController::HeaderMenuActionKind::SortAscending) {
        fileView_->sortByColumn(action.column, Qt::AscendingOrder);
        return;
    }
    if (action.kind == DjBrowserMenuController::HeaderMenuActionKind::SortDescending) {
        fileView_->sortByColumn(action.column, Qt::DescendingOrder);
        return;
    }
    if (action.kind != DjBrowserMenuController::HeaderMenuActionKind::ToggleColumn) return;

    int visibleCount = 0;
    for (int column = 0; column < fileModel_->columnCount(); ++column) {
        if (!fileView_->isColumnHidden(column)) ++visibleCount;
    }

    const bool currentlyVisible = !fileView_->isColumnHidden(action.column);
    if (currentlyVisible && visibleCount == 1) return;

    fileView_->setColumnHidden(action.column, currentlyVisible);
    persistHeaderState();
}

void DjBrowserPane::showFileContextMenu(const QPoint& pos)
{
    const QModelIndex idx = fileView_->indexAt(pos);
    if (!idx.isValid()) return;

    fileView_->setCurrentIndex(idx);
    fileView_->selectRow(idx.row());

    const QString filePath = fileModel_->filePathAt(idx.row());
    const QString fileName = QFileInfo(filePath).fileName();

    const auto action = DjBrowserMenuController::showFileContextMenu(
        fileView_,
        fileView_->viewport(),
        pos,
        QGuiApplication::clipboard()->mimeData() && QGuiApplication::clipboard()->mimeData()->hasUrls(),
        DjBrowserPaneUi::menuStyle());
    if (action == DjBrowserMenuController::FileMenuAction::None) return;

    DjBrowserMenuController::FileMenuContext context;
    context.parent = this;
    context.filePath = filePath;
    context.fileName = fileName;
    context.folderPath = fileModel_->folderPath();
    context.cutSourcePath = &cutSourcePath_;
    context.refreshFiles = [this]() { fileModel_->refresh(); };
    context.renameFile = [this](const QString& path) { return renameFile(path); };
    context.loadToDeck = [this](int deckIdx, const QString& path) { emit loadToDeckRequested(deckIdx, path); };
    context.startRegularAnalysis = [this](const QString& path) { startRegularAnalysis(path); };
    context.startBackgroundAnalysis = [this](const QString& path) { startBackgroundAnalysis(path); };
    context.showBulkReplaceDialog = [this]() { promptBulkReplaceDialog(); };
    context.backToLibrary = [this]() { emit backRequested(); };
    context.importFolder = [this]() { emit importFolderRequested(); };
    context.runAnalysis = [this]() { emit importAnalysisRequested(); };
    context.showProAudioClipper = [this]() { emit showProAudioClipperRequested(); };
    context.showAncillaryScreens = [this]() { emit showAncillaryScreensRequested(); };

    DjBrowserMenuController::handleFileMenuAction(action, context);
}

bool DjBrowserPane::renameFile(const QString& filePath)
{
    return DjBrowserPaneActions::renameVisibleFile(
        this,
        fileModel_,
        fileView_,
        filePath,
        [this](const QString& text, const QString& tone) { updateFooterMessage(text, tone); });
}

void DjBrowserPane::promptBulkReplaceDialog()
{
    const auto request = DjBrowserPaneActions::promptBulkReplace(this, lastFindText_, lastReplaceText_);
    if (!request.accepted) return;

    lastFindText_ = request.findText;
    lastReplaceText_ = request.replaceText;
    bulkReplaceFiles(lastFindText_, lastReplaceText_);
}

void DjBrowserPane::restoreHeaderState()
{
    QByteArray state;
    if (!loadUiStateBlob(kBrowserHeaderStateKey, state)) return;

    auto* header = fileView_->horizontalHeader();
    if (!header->restoreState(state)) {
        updateFooterMessage(QStringLiteral("Browser column layout could not be restored."), QStringLiteral("error"));
    }
}

void DjBrowserPane::restoreHiddenFolderState()
{
    if (!dirProxy_) return;

    QByteArray state;
    if (!loadUiStateBlob(kBrowserHiddenFoldersKey, state) || state.isEmpty()) return;

    const QStringList hiddenPaths = QString::fromUtf8(state).split('\n', Qt::SkipEmptyParts);
    dirProxy_->setHiddenPaths(hiddenPaths);
}

void DjBrowserPane::persistHiddenFolderState() const
{
    if (!dirProxy_) return;
    saveUiStateBlob(kBrowserHiddenFoldersKey, dirProxy_->hiddenPaths().join('\n').toUtf8());
}

void DjBrowserPane::updateHiddenFoldersButtonState()
{
    if (!showAllFoldersBtn_ || !dirProxy_) return;
    showAllFoldersBtn_->setEnabled(dirProxy_->hasHiddenPaths());
}

QString DjBrowserPane::nearestVisibleFolderPath(const QString& path) const
{
    const QString homePath = normalizedPath(QStandardPaths::writableLocation(QStandardPaths::HomeLocation));
    if (!dirProxy_) return homePath;

    QString candidate = normalizedPath(path.trimmed().isEmpty() ? homePath : path);
    while (!candidate.isEmpty() && dirProxy_->isPathHiddenOrUnderHiddenParent(candidate)) {
        const QString parentPath = normalizedPath(QDir(candidate).absolutePath());
        if (parentPath == candidate) break;
        candidate = parentPath;
    }

    if (candidate.isEmpty() || dirProxy_->isPathHiddenOrUnderHiddenParent(candidate)) {
        return homePath;
    }

    return candidate;
}

void DjBrowserPane::normalizePaneFolders()
{
    if (!dirView_ || !dirProxy_ || !fileModel_) return;

    const QString pane1Path = nearestVisibleFolderPath(fileModel_->folderPath());
    if (pane1Path != fileModel_->folderPath()) {
        fileModel_->setFolderPath(pane1Path);
        if (folderLabel_) folderLabel_->setText(QFileInfo(pane1Path).fileName());
    }

    if (fileModel2_) {
        const QString pane2Path = nearestVisibleFolderPath(fileModel2_->folderPath());
        if (pane2Path != fileModel2_->folderPath()) {
            fileModel2_->setFolderPath(pane2Path);
            if (folderLabel2_) folderLabel2_->setText(QFileInfo(pane2Path).fileName());
        }
    }

    const QString activePath = (activePaneIndex_ == 0 || !fileModel2_)
        ? pane1Path
        : nearestVisibleFolderPath(fileModel2_->folderPath());
    const QModelIndex currentIndex = dirProxy_->indexForPath(activePath);
    if (currentIndex.isValid() && dirView_->currentIndex() != currentIndex) {
        const QSignalBlocker blocker(dirView_->selectionModel());
        dirView_->setCurrentIndex(currentIndex);
    }
}

void DjBrowserPane::persistHeaderState()
{
    auto* header = fileView_->horizontalHeader();
    saveUiStateBlob(kBrowserHeaderStateKey, header->saveState());
}

bool DjBrowserPane::bulkReplaceFiles(const QString& findText, const QString& replaceText)
{
    return DjBrowserPaneActions::bulkReplaceFiles(
        this,
        db_,
        fileModel_,
        findText,
        replaceText,
        [this](const QString& text, const QString& tone) { updateFooterMessage(text, tone); });
}

void DjBrowserPane::updateFooterMessage(const QString& text, const QString& tone)
{
    DjBrowserPaneActions::updateFooterMessage(footerLabel_, text, tone);
}

void DjBrowserPane::startRegularAnalysis(const QString& filePath)
{
    analysisCoordinator_->startRegularAnalysis(filePath);
}

void DjBrowserPane::startBackgroundAnalysis(const QString& filePath)
{
    analysisCoordinator_->startBackgroundAnalysis(filePath);
}

void DjBrowserPane::setActivePaneIndex(int idx)
{
    if (activePaneIndex_ == idx) return;
    activePaneIndex_ = idx;
    updatePaneHeaderStyles();
}

void DjBrowserPane::updatePaneHeaderStyles()
{
    const auto kActiveStyle    = QStringLiteral("QWidget { background: #0e1a30; border-left: 3px solid #4a8af4; }");
    const auto kInactiveStyle  = QStringLiteral("QWidget { background: #0a0c12; border-left: 3px solid transparent; }");
    const auto kActiveLbl      = QStringLiteral("color: #c9d3e3; font-size: 11px; font-weight: 600;");
    const auto kInactiveLbl    = QStringLiteral("color: #5a6a80; font-size: 11px; font-weight: 600;");

    if (paneHeader_)  paneHeader_->setStyleSheet(activePaneIndex_ == 0 ? kActiveStyle  : kInactiveStyle);
    if (paneHeader2_) paneHeader2_->setStyleSheet(activePaneIndex_ == 1 ? kActiveStyle : kInactiveStyle);
    if (folderLabel_)  folderLabel_->setStyleSheet(activePaneIndex_ == 0 ? kActiveLbl  : kInactiveLbl);
    if (folderLabel2_) folderLabel2_->setStyleSheet(activePaneIndex_ == 1 ? kActiveLbl : kInactiveLbl);
}

bool DjBrowserPane::eventFilter(QObject* obj, QEvent* event)
{
    if (event->type() == QEvent::MouseButtonPress) {
        if (paneHeader_ && (obj == paneHeader_ || obj->parent() == paneHeader_)) {
            setActivePaneIndex(0);
        } else if (paneHeader2_ && (obj == paneHeader2_ || obj->parent() == paneHeader2_)) {
            setActivePaneIndex(1);
        }
    }
    return QWidget::eventFilter(obj, event);
}

void DjBrowserPane::showFileContextMenu2(const QPoint& pos)
{
    if (!fileView2_ || !fileModel2_) return;

    const QModelIndex idx = fileView2_->indexAt(pos);
    if (!idx.isValid()) return;

    fileView2_->setCurrentIndex(idx);
    fileView2_->selectRow(idx.row());

    const QString filePath = fileModel2_->filePathAt(idx.row());
    const QString fileName = QFileInfo(filePath).fileName();

    const auto action = DjBrowserMenuController::showFileContextMenu(
        fileView2_,
        fileView2_->viewport(),
        pos,
        QGuiApplication::clipboard()->mimeData() && QGuiApplication::clipboard()->mimeData()->hasUrls(),
        DjBrowserPaneUi::menuStyle());
    if (action == DjBrowserMenuController::FileMenuAction::None) return;

    DjBrowserMenuController::FileMenuContext context;
    context.parent = this;
    context.filePath = filePath;
    context.fileName = fileName;
    context.folderPath = fileModel2_->folderPath();
    context.cutSourcePath = &cutSourcePath_;
    context.refreshFiles = [this]() { fileModel2_->refresh(); };
    context.renameFile = [this](const QString& path) {
        return DjBrowserPaneActions::renameVisibleFile(
            this, fileModel2_, fileView2_, path,
            [this](const QString& text, const QString& tone) { updateFooterMessage(text, tone); });
    };
    context.loadToDeck = [this](int deckIdx, const QString& path) { emit loadToDeckRequested(deckIdx, path); };
    context.startRegularAnalysis = [this](const QString& path) { startRegularAnalysis(path); };
    context.startBackgroundAnalysis = [this](const QString& path) { startBackgroundAnalysis(path); };
    context.showBulkReplaceDialog = [this]() { promptBulkReplaceDialog(); };
    context.backToLibrary = [this]() { emit backRequested(); };
    context.importFolder = [this]() { emit importFolderRequested(); };
    context.runAnalysis = [this]() { emit importAnalysisRequested(); };
    context.showProAudioClipper = [this]() { emit showProAudioClipperRequested(); };
    context.showAncillaryScreens = [this]() { emit showAncillaryScreensRequested(); };

    DjBrowserMenuController::handleFileMenuAction(action, context);
}

void DjBrowserPane::setImportUiState(const QString& title,
                                     const QString& detail,
                                     bool importEnabled,
                                     bool runAnalysisEnabled)
{
    if (importFolderBtn_) importFolderBtn_->setEnabled(importEnabled);
    if (importAnalysisBtn_) importAnalysisBtn_->setEnabled(runAnalysisEnabled);
    if (importFolderBtn_) importFolderBtn_->setToolTip(title + QStringLiteral("\n") + detail);
    if (importAnalysisBtn_) importAnalysisBtn_->setToolTip(title + QStringLiteral("\n") + detail);
    if (footerLabel_) footerLabel_->setToolTip(title + QStringLiteral("\n") + detail);
}
