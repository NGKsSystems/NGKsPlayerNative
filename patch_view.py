import re

# --- 1. DjTrackTableView.h ---
with open('src/ui/dj/browser/DjTrackTableView.h', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('void loadPath(const QString& path);', 'void loadDatabaseRows(const QVector<DjTrackRow>& rows);')
if 'DjTrackRow' not in text:
    text = text.replace('class DjTrackTableModel;', 'class DjTrackTableModel;\nclass DjTrackRow;')

with open('src/ui/dj/browser/DjTrackTableView.h', 'w', encoding='utf-8') as f:
    f.write(text)

# --- 2. DjTrackTableView.cpp ---
with open('src/ui/dj/browser/DjTrackTableView.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'void DjTrackTableView::loadPath.*?\{.*\}', '', text, flags=re.DOTALL)
text += '''
void DjTrackTableView::loadDatabaseRows(const QVector<DjTrackRow>& rows) { 
    sourceModel_->loadDatabaseRows(rows);
}
'''
with open('src/ui/dj/browser/DjTrackTableView.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

# --- 3. main.cpp ---
with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# I need to add initialize call to DjBrowserPane
# find auto* libraryPane = new DjBrowserPane(page);
replacement = '''auto* libraryPane = new DjBrowserPane(page);
        QString libErr;
        if (!libraryPane->initialize(runtimePath("data/runtime/dj_library.db"), &libErr)) {
            qWarning() << "Browser DB init failed:" << libErr;
        }'''
text = text.replace('auto* libraryPane = new DjBrowserPane(page);', replacement)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("View and Main updated")
