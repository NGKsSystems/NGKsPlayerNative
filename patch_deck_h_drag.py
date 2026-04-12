import re
with open('src/ui/DeckStrip.h', 'r', encoding='utf-8') as f:
    content = f.read()

repl = '''
    void loadTrack(const QString& filePath);

protected:
    void dragEnterEvent(QDragEnterEvent* event) override;
    void dropEvent(QDropEvent* event) override;

public:
'''
content = content.replace('    void loadTrack(const QString& filePath);', repl.strip())

with open('src/ui/DeckStrip.h', 'w', encoding='utf-8') as f:
    f.write(content)
