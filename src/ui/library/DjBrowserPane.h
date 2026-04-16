#pragma once
#include <QObject>
#include <QWidget>
#include <QFileSystemModel>
#include <QTreeView>
#include <QVBoxLayout>
#include <QString>
#include "ui/library/TrackDragView.h"

class QEvent;

class DjBrowserAnalysisCoordinator;
class DjBrowserFileTableModel;
class DjBrowserFolderTreeProxyModel;
class DjLibraryDatabase;
class QLabel;
class QPushButton;

class DjBrowserPane : public QWidget {
    Q_OBJECT
public:
    explicit DjBrowserPane(DjLibraryDatabase* db, QWidget* parent = nullptr);
    void setBrowserRootFolder(const QString& folderPath);

    void setImportUiState(const QString& title,
                          const QString& detail,
                          bool importEnabled,
                          bool runAnalysisEnabled);

signals:
    void loadToDeckRequested(int deckIdx, const QString& path);
    void importFolderRequested();
    void importAnalysisRequested();
    void backRequested();
    void showProAudioClipperRequested();
    void showAncillaryScreensRequested();

private:
    void showHeaderContextMenu(const QPoint& pos);
    void showFileContextMenu(const QPoint& pos);
    void showFileContextMenu2(const QPoint& pos);
    bool renameFile(const QString& filePath);
    void promptBulkReplaceDialog();
    void restoreHeaderState();
    void persistHeaderState();
    bool bulkReplaceFiles(const QString& findText, const QString& replaceText);
    void updateFooterMessage(const QString& text, const QString& tone = QStringLiteral("info"));
    void startRegularAnalysis(const QString& filePath);
    void startBackgroundAnalysis(const QString& filePath);
    void setActivePaneIndex(int idx);
    void updatePaneHeaderStyles();
    void restoreHiddenFolderState();
    void persistHiddenFolderState() const;
    void updateHiddenFoldersButtonState();
    void normalizePaneFolders();
    QString nearestVisibleFolderPath(const QString& path) const;
    bool eventFilter(QObject* obj, QEvent* event) override;

    DjLibraryDatabase*    db_;
    QFileSystemModel*     dirModel_;
    DjBrowserFolderTreeProxyModel* dirProxy_;
    DjBrowserFileTableModel* fileModel_;
    DjBrowserFileTableModel* fileModel2_;
    QTreeView*        dirView_;
    TrackDragView*    fileView_;
    TrackDragView*    fileView2_;
    QWidget*          paneHeader_;
    QWidget*          paneHeader2_;
    QLabel*           folderLabel_;
    QLabel*           folderLabel2_;
    QPushButton*      importFolderBtn_;
    QPushButton*      importAnalysisBtn_;
    QPushButton*      showAllFoldersBtn_;
    QLabel*           footerLabel_;
    DjBrowserAnalysisCoordinator* analysisCoordinator_;
    QString           cutSourcePath_;
    QString           lastFindText_;
    QString           lastReplaceText_;
    int               activePaneIndex_;
};
