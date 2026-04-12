import re

with open('src/ui/dj/browser/DjBrowserController.h', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('};', 'private:\n    DjTrackTableView* table_;\n    DjLibraryDatabase* db_;\n    QString currentPath_;\n    QString currentSearch_;\n\n    void updateTable();\n};')

with open('src/ui/dj/browser/DjBrowserController.h', 'w', encoding='utf-8') as f:
    f.write(text)
