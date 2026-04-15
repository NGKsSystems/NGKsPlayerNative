#pragma once

#include "ui/library/DjBrowserUiFeedback.h"

#include <QAbstractItemModel>
#include <QClipboard>
#include <QFile>
#include <QFileInfo>
#include <QGuiApplication>
#include <QHeaderView>
#include <QMenu>
#include <QMimeData>
#include <QString>
#include <QUrl>
#include <QWidget>

#include <functional>

namespace DjBrowserMenuController {

enum class HeaderMenuActionKind {
    None,
    SortAscending,
    SortDescending,
    ToggleColumn,
};

struct HeaderMenuAction {
    HeaderMenuActionKind kind{HeaderMenuActionKind::None};
    int column{-1};
};

inline HeaderMenuAction showHeaderContextMenu(QWidget* parent,
                                              QHeaderView* header,
                                              QAbstractItemModel* model,
                                              const std::function<bool(int)>& isColumnHidden,
                                              const QPoint& pos,
                                              const QString& menuStyle)
{
    const int clickedColumn = header->logicalIndexAt(pos);

    QMenu menu(parent);
    menu.setStyleSheet(menuStyle);

    QAction* sortAscAction = nullptr;
    QAction* sortDescAction = nullptr;
    if (clickedColumn >= 0) {
        sortAscAction = menu.addAction(QStringLiteral("Sort Ascending"));
        sortDescAction = menu.addAction(QStringLiteral("Sort Descending"));
        menu.addSeparator();
    }

    QList<QAction*> columnActions;
    for (int column = 0; column < model->columnCount(); ++column) {
        auto* action = menu.addAction(model->headerData(column, Qt::Horizontal, Qt::DisplayRole).toString());
        action->setCheckable(true);
        action->setChecked(!isColumnHidden(column));
        columnActions.push_back(action);
    }

    QAction* chosen = menu.exec(header->mapToGlobal(pos));
    if (!chosen) return {};
    if (chosen == sortAscAction) return {HeaderMenuActionKind::SortAscending, clickedColumn};
    if (chosen == sortDescAction) return {HeaderMenuActionKind::SortDescending, clickedColumn};

    for (int column = 0; column < columnActions.size(); ++column) {
        if (chosen == columnActions[column]) return {HeaderMenuActionKind::ToggleColumn, column};
    }

    return {};
}

enum class FileMenuAction {
    None,
    Cut,
    Copy,
    Paste,
    Rename,
    Delete,
    AnalyzeRegular,
    AnalyzeLive,
    LoadDeckA,
    LoadDeckB,
    BulkRename,
};

struct FileMenuContext {
    QWidget* parent{nullptr};
    QString filePath;
    QString fileName;
    QString folderPath;
    QString findText;
    QString replaceText;
    QString* cutSourcePath{nullptr};
    std::function<void()> refreshFiles;
    std::function<bool(const QString&)> renameFile;
    std::function<void(int, const QString&)> loadToDeck;
    std::function<void(const QString&)> startRegularAnalysis;
    std::function<void(const QString&)> startBackgroundAnalysis;
    std::function<bool(const QString&, const QString&)> bulkReplaceFiles;
    std::function<void(const QString&, const QString&)> updateFooter;
    std::function<void()> focusFindReplace;
};

inline FileMenuAction showFileContextMenu(QWidget* parent,
                                          QWidget* anchor,
                                          const QPoint& pos,
                                          bool canPaste,
                                          const QString& menuStyle)
{
    QMenu menu(parent);
    menu.setStyleSheet(menuStyle);

    auto* cutAction = menu.addAction(QStringLiteral("Cut"));
    auto* copyAction = menu.addAction(QStringLiteral("Copy"));
    auto* pasteAction = menu.addAction(QStringLiteral("Paste"));
    menu.addSeparator();
    auto* renameAction = menu.addAction(QStringLiteral("Rename"));
    auto* deleteAction = menu.addAction(QStringLiteral("Delete"));
    menu.addSeparator();
    auto* analyzeAction = menu.addAction(QStringLiteral("Analyze (Regular)"));
    auto* analyzeLiveAction = menu.addAction(QStringLiteral("Analyze (Live Type - Background)"));
    menu.addSeparator();
    auto* loadAAction = menu.addAction(QStringLiteral("Load to Deck A"));
    auto* loadBAction = menu.addAction(QStringLiteral("Load to Deck B"));
    auto* queueAction = menu.addAction(QStringLiteral("Place in Queue"));
    menu.addSeparator();
    auto* bulkRenameAction = menu.addAction(QStringLiteral("Bulk Find && Replace for File Names"));

    pasteAction->setEnabled(canPaste);
    queueAction->setEnabled(false);

    QAction* chosen = menu.exec(anchor->mapToGlobal(pos));
    if (!chosen) return FileMenuAction::None;
    if (chosen == cutAction) return FileMenuAction::Cut;
    if (chosen == copyAction) return FileMenuAction::Copy;
    if (chosen == pasteAction) return FileMenuAction::Paste;
    if (chosen == renameAction) return FileMenuAction::Rename;
    if (chosen == deleteAction) return FileMenuAction::Delete;
    if (chosen == analyzeAction) return FileMenuAction::AnalyzeRegular;
    if (chosen == analyzeLiveAction) return FileMenuAction::AnalyzeLive;
    if (chosen == loadAAction) return FileMenuAction::LoadDeckA;
    if (chosen == loadBAction) return FileMenuAction::LoadDeckB;
    if (chosen == bulkRenameAction) return FileMenuAction::BulkRename;
    return FileMenuAction::None;
}

inline void setClipboardFile(const QString& filePath, QString* cutSourcePath, bool cut)
{
    auto* mime = new QMimeData();
    mime->setUrls({QUrl::fromLocalFile(filePath)});
    QGuiApplication::clipboard()->setMimeData(mime);

    if (!cutSourcePath) return;
    if (cut) *cutSourcePath = filePath;
    else cutSourcePath->clear();
}

inline void pasteClipboardFiles(const FileMenuContext& context)
{
    const QMimeData* mimeData = QGuiApplication::clipboard()->mimeData();
    if (!mimeData || !mimeData->hasUrls()) return;

    for (const QUrl& url : mimeData->urls()) {
        const QString src = url.toLocalFile();
        const QString dst = context.folderPath + QLatin1Char('/') + QFileInfo(src).fileName();
        if (context.cutSourcePath && !context.cutSourcePath->isEmpty() && src == *context.cutSourcePath) {
            QFile::rename(src, dst);
            context.cutSourcePath->clear();
        } else {
            QFile::copy(src, dst);
        }
    }

    if (context.refreshFiles) context.refreshFiles();
}

inline void handleFileMenuAction(FileMenuAction action, const FileMenuContext& context)
{
    switch (action) {
    case FileMenuAction::Cut:
        setClipboardFile(context.filePath, context.cutSourcePath, true);
        return;
    case FileMenuAction::Copy:
        setClipboardFile(context.filePath, context.cutSourcePath, false);
        return;
    case FileMenuAction::Paste:
        pasteClipboardFiles(context);
        return;
    case FileMenuAction::Rename:
        if (context.renameFile) context.renameFile(context.filePath);
        return;
    case FileMenuAction::Delete: {
        const auto result = DjBrowserUiFeedback::themedQuestion(
            context.parent,
            QStringLiteral("Delete"),
            QStringLiteral("Permanently delete '%1'?").arg(context.fileName),
            QMessageBox::Yes | QMessageBox::No,
            QMessageBox::No);
        if (result != QMessageBox::Yes) return;

        if (!QFile::remove(context.filePath)) {
            DjBrowserUiFeedback::themedWarning(
                context.parent,
                QStringLiteral("Delete Failed"),
                QStringLiteral("Could not delete '%1'.").arg(context.fileName));
            return;
        }

        if (context.refreshFiles) context.refreshFiles();
        return;
    }
    case FileMenuAction::AnalyzeRegular:
        if (context.startRegularAnalysis) context.startRegularAnalysis(context.filePath);
        return;
    case FileMenuAction::AnalyzeLive:
        if (context.startBackgroundAnalysis) context.startBackgroundAnalysis(context.filePath);
        return;
    case FileMenuAction::LoadDeckA:
        if (context.loadToDeck) context.loadToDeck(0, context.filePath);
        return;
    case FileMenuAction::LoadDeckB:
        if (context.loadToDeck) context.loadToDeck(1, context.filePath);
        return;
    case FileMenuAction::BulkRename:
        if (context.findText.trimmed().isEmpty()) {
            if (context.focusFindReplace) context.focusFindReplace();
            if (context.updateFooter) {
                context.updateFooter(
                    QStringLiteral("Enter Find above, then press Replace."),
                    QStringLiteral("info"));
            }
            return;
        }

        if (context.bulkReplaceFiles) context.bulkReplaceFiles(context.findText, context.replaceText);
        return;
    case FileMenuAction::None:
        return;
    }
}

} // namespace DjBrowserMenuController