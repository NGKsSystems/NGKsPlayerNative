import re
with open('src/ui/DeckStrip.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

impl = '''
#include <QDragEnterEvent>
#include <QDropEvent>
#include <QMimeData>
#include <QMessageBox>

void DeckStrip::dragEnterEvent(QDragEnterEvent* event) {
    if (event->mimeData()->hasFormat("application/x-ngks-dj-track") || event->mimeData()->hasText()) {
        event->acceptProposedAction();
    }
}

void DeckStrip::dropEvent(QDropEvent* event) {
    QString path;
    if (event->mimeData()->hasFormat("application/x-ngks-dj-track")) {
        path = QString::fromUtf8(event->mimeData()->data("application/x-ngks-dj-track"));
    } else if (event->mimeData()->hasText()) {
        path = event->mimeData()->text();
    }

    if (path.isEmpty()) return;

    if (bridge_->deckIsPlaying(deckIndex_)) {
        QMessageBox::warning(this, "Load Blocked", "Deck is currently playing");
        return;
    }

    event->acceptProposedAction();
    loadTrack(path);
}
'''
if "void DeckStrip::dragEnterEvent" not in content:
    content += '\n' + impl.strip() + '\n'

# find setAcceptDrops
content = content.replace('EngineBridge* bridge, QWidget* parent)', 'EngineBridge* bridge, QWidget* parent)')
if "setAcceptDrops(true)" not in content:
    content = content.replace('waveModeBtn_ = new QPushButton', 'setAcceptDrops(true);\n    waveModeBtn_ = new QPushButton')

if "#include <QDragEnterEvent>" not in content:
    content = "#include <QDragEnterEvent>\n#include <QDropEvent>\n#include <QMimeData>\n#include <QMessageBox>\n" + content

with open('src/ui/DeckStrip.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
