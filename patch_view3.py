import re
with open('src/ui/dj/browser/DjTrackTableView.h', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('#include <QVector>', '#include <QVector>\n#include \"core/library/DjLibraryDatabase.h\"')

with open('src/ui/dj/browser/DjTrackTableView.h', 'w', encoding='utf-8') as f:
    f.write(content)
