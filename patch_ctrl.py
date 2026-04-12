import re

# --- 1. DjBrowserController.h ---
with open('src/ui/dj/browser/DjBrowserController.h', 'r', encoding='utf-8') as f:
    text = f.read()

if 'DjLibraryDatabase' not in text:
    text = text.replace('class DjTrackTableView;', 'class DjTrackTableView;\nclass DjLibraryDatabase;')
    text = text.replace('DjTrackTableView* table,', 'DjTrackTableView* table,\n        DjLibraryDatabase* db,')
    text = text.replace('private:', 'private:\n    DjTrackTableView* table_;\n    DjLibraryDatabase* db_;\n    QString currentPath_;\n    QString currentSearch_;\n\n    void updateTable();')

with open('src/ui/dj/browser/DjBrowserController.h', 'w', encoding='utf-8') as f:
    f.write(text)

# --- 2. DjBrowserController.cpp ---
cpp_content = '''#include "DjBrowserController.h"
#include "DjSearchBar.h"
#include "DjSourceTreeWidget.h"
#include "DjTrackTableView.h"
#include "../../library/dj/DjLibraryDatabase.h"
#include <QTimer>

DjBrowserController::DjBrowserController(DjSearchBar* search,
                                         DjSourceTreeWidget* tree,         
                                         DjTrackTableView* table,
                                         DjLibraryDatabase* db,
                                         QObject* parent)
    : QObject(parent), table_(table), db_(db) {

    if (search) {
        connect(search, &DjSearchBar::searchTextChanged, this, [this](const QString& text) {
            currentSearch_ = text;
            updateTable();
        });
    }
    if (tree) {
        connect(tree, &DjSourceTreeWidget::folderSelected, this, [this](const QString& path) {
            currentPath_ = path;
            updateTable();
        });
    }
    // Defer initial load so db init can finish in DjBrowserPane
    QTimer::singleShot(50, this, &DjBrowserController::updateTable);
}

void DjBrowserController::updateTable() {
    if (!db_ || !table_) return;
    if (!db_->isOpen()) return;

    // Fetch up to 10k rows for now (enough for robust shell testing)      
    QVector<DjTrackRow> rows = db_->fetchPage(0, 10000, currentSearch_);

    // Apply strict folder filter if supported
    if (!currentPath_.isEmpty() && currentPath_ != "Root" && currentPath_ != "Library") {
        QVector<DjTrackRow> filtered;
        for (const auto& r : rows) {
            if (r.filePath.startsWith(currentPath_, Qt::CaseInsensitive)) {
                filtered.push_back(r);
            }
        }
        rows = filtered;
    }

    table_->loadDatabaseRows(rows);
}
'''
with open('src/ui/dj/browser/DjBrowserController.cpp', 'w', encoding='utf-8') as f:
    f.write(cpp_content)

print("Controller updated")
