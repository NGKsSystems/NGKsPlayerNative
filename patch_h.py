import re

path = 'src/ui/library/dj/DjLibraryWidget.h'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

bad = '''    void trackActivated(qint64 trackId, const QString& filePath);

private:'''
good = '''    void trackActivated(qint64 trackId, const QString& filePath);

protected:
    void resizeEvent(QResizeEvent* event) override;

private:'''

text = text.replace(bad, good)
if '<QResizeEvent>' not in text:
    text = '#include <QResizeEvent>\n' + text

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("H_PATCHED")
