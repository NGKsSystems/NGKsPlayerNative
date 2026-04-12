import re

with open('src/ui/dj/browser/DjTrackTableView.h', 'r', encoding='utf-8') as f:
    text = f.read()

# Make sure QVector is included since we used it in DjTrackTableView.h
if '<QVector>' not in text:
    text = text.replace('#include <QTableView>', '#include <QTableView>\n#include <QVector>')

with open('src/ui/dj/browser/DjTrackTableView.h', 'w', encoding='utf-8') as f:
    f.write(text)
