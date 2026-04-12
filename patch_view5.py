import re
with open('src/ui/dj/browser/DjTrackTableView.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('''void DjTrackTableView::loadDatabaseRows(const QVector<DjTrackRow>& rows) { 
    sourceModel_->loadDatabaseRows(rows);
}

void DjTrackTableView::loadDatabaseRows(const QVector<DjTrackRow>& rows) { 
    sourceModel_->loadDatabaseRows(rows);
}''', '''void DjTrackTableView::loadDatabaseRows(const QVector<DjTrackRow>& rows) { 
    sourceModel_->loadDatabaseRows(rows);
}''')

with open('src/ui/dj/browser/DjTrackTableView.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
