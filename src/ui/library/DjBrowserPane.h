#pragma once
#include <QWidget>
#include <QFileSystemModel>
#include <QTreeView>
#include <QTableView>
#include <QVBoxLayout>
#include <QSplitter>
#include <QLineEdit>

class DjLibraryDatabase;

class DjBrowserPane : public QWidget {
    Q_OBJECT
public:
    explicit DjBrowserPane(QWidget* parent = nullptr);
    ~DjBrowserPane() override;

    void setDatabase(DjLibraryDatabase* db);

private:
    QFileSystemModel* dirModel_;
    QFileSystemModel* fileModel_;
    QTreeView* dirView_;
    QTableView* fileView_;
    QLineEdit* searchBox_;
    DjLibraryDatabase* db_{nullptr};

    void saveHeaderState();
    void restoreHeaderState();
    
signals:
    void requestAnalyze(const QString& path, bool live);
    void requestLoadDeck(const QString& path, int deck);
    void requestEnqueue(const QString& path);
};
