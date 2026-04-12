import re
with open('src/ui/dj/browser/DjBrowserController.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

new_update = '''
#include <QDir>
#include <QDirIterator>
#include <QFileInfo>

void DjBrowserController::updateTable() {
    if (!table_) return;

    QVector<DjTrackRow> rows;

    if (!currentPath_.isEmpty() && currentPath_ != "Root" && currentPath_ != "Library") {
        QDir dir(currentPath_);
        if (dir.exists()) {
            QDirIterator it(currentPath_, QDir::Files, QDirIterator::NoIteratorFlags);
            while (it.hasNext()) {
                QString path = it.next();
                QString lower = path.toLower();
                if (lower.endsWith(".mp3") || lower.endsWith(".wav") || lower.endsWith(".flac") ||
                    lower.endsWith(".m4a") || lower.endsWith(".aac") || lower.endsWith(".ogg")) {
                    
                    QFileInfo fi(path);
                    DjTrackRow r;
                    r.filePath = path;
                    r.title = fi.completeBaseName();
                    
                    if (!currentSearch_.isEmpty() && !r.title.contains(currentSearch_, Qt::CaseInsensitive)) {
                        continue;
                    }
                    
                    rows.push_back(r);
                }
            }
        }
    } else {
        if (db_ && db_->isOpen()) {
            rows = db_->fetchPage(0, 10000, currentSearch_);
        }
    }

    table_->loadDatabaseRows(rows);
}
'''

content = re.sub(r'void DjBrowserController::updateTable\(\) \{.*\}', new_update.strip(), content, flags=re.DOTALL)

with open('src/ui/dj/browser/DjBrowserController.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
