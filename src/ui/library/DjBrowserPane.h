#pragma once
#include <QWidget>
#include <QFileSystemModel>
#include <QTreeView>
#include <QVBoxLayout>
#include "ui/library/TrackDragView.h"

class DjBrowserAnalysisCoordinator;
class DjBrowserFileTableModel;
class DjLibraryDatabase;
class QLabel;

class DjBrowserPane : public QWidget {
    Q_OBJECT
public:
    explicit DjBrowserPane(DjLibraryDatabase* db, QWidget* parent = nullptr);

signals:
    void loadToDeckRequested(int deckIdx, const QString& path);

private:
    void showHeaderContextMenu(const QPoint& pos);
    void showFileContextMenu(const QPoint& pos);
    bool renameFile(const QString& filePath);
    void promptBulkReplaceDialog();
    void restoreHeaderState();
    void persistHeaderState();
    bool bulkReplaceFiles(const QString& findText, const QString& replaceText);
    void updateFooterMessage(const QString& text, const QString& tone = QStringLiteral("info"));
    void startRegularAnalysis(const QString& filePath);
    void startBackgroundAnalysis(const QString& filePath);

    DjLibraryDatabase*    db_;
    QFileSystemModel* dirModel_;
    DjBrowserFileTableModel* fileModel_;
    QTreeView*        dirView_;
    TrackDragView*    fileView_;
    QLabel*           footerLabel_;
    DjBrowserAnalysisCoordinator* analysisCoordinator_;
    QString           cutSourcePath_;  // tracks pending cut operation
    QString           lastFindText_;
    QString           lastReplaceText_;
};
