#include "DjBrowserPane.h"

#include "ui/library/DjBrowserAnalysisCoordinator.h"
#include "ui/library/DjBrowserFileTableModel.h"
#include "ui/library/DjBrowserMenuController.h"
#include "ui/library/DjBrowserPaneActions.h"
#include "ui/library/DjBrowserPaneUi.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/LibraryPersistence.h"

#include <QClipboard>
#include <QFileInfo>
#include <QHeaderView>
#include <QMimeData>

namespace {

const QString kBrowserHeaderStateKey = QStringLiteral("dj_browser/file_header_state");

}

DjBrowserPane::DjBrowserPane(DjLibraryDatabase* db, QWidget* parent)
    : QWidget(parent)
    , db_(db)
    , dirModel_(nullptr)
    , fileModel_(nullptr)
    , dirView_(nullptr)
    , fileView_(nullptr)
    , footerLabel_(nullptr)
    , analysisCoordinator_(nullptr)
{
    const auto widgets = DjBrowserPaneUi::build(this, db_);
    dirModel_ = widgets.dirModel;
    fileModel_ = widgets.fileModel;
    dirView_ = widgets.dirView;
    fileView_ = widgets.fileView;
    footerLabel_ = widgets.footerLabel;

    footerLabel_->setStyleSheet(DjBrowserUiFeedback::footerStyleForTone(QStringLiteral("info")));
    analysisCoordinator_ = new DjBrowserAnalysisCoordinator(db_, this);
    analysisCoordinator_->setFooterCallback([this](const QString& text, const QString& tone) {
        updateFooterMessage(text, tone);
    });
    analysisCoordinator_->setRefreshCallback([this]() {
        fileModel_->refresh();
    });

    restoreHeaderState();

    auto* header = fileView_->horizontalHeader();
    QObject::connect(header, &QHeaderView::sectionMoved, this, [this]() { persistHeaderState(); });
    QObject::connect(header, &QHeaderView::sectionResized, this, [this]() { persistHeaderState(); });
    QObject::connect(header, &QHeaderView::sortIndicatorChanged, this, [this]() { persistHeaderState(); });

    DjBrowserPaneUi::wireInteractions(this, widgets, {
        [this](const QString& text) { fileModel_->setSearchText(text); },
        [this](const QString& folderPath) { fileModel_->setFolderPath(folderPath); },
        [this](const QModelIndex& index) { fileView_->edit(index); },
        [this](const QPoint& pos) { showHeaderContextMenu(pos); },
        [this](const QPoint& pos) { showFileContextMenu(pos); }
    });
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
