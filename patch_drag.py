import re

file_view = "src/ui/dj/browser/DjTrackTableView.cpp"
with open(file_view, "r", encoding='utf8') as f:
    content = f.read()

if "DRAG_START" not in content:
    content = content.replace("drag->exec(Qt::CopyAction);", "qInfo() << \"DRAG_START file='\" << filePath << \"'\";\n    drag->exec(Qt::CopyAction);")
    content = '#include <QDebug>\n' + content
    with open(file_view, "w", encoding='utf8') as f:
        f.write(content)

file_strip = "src/ui/DeckStrip.cpp"
with open(file_strip, "r", encoding='utf8') as f:
    content = f.read()

if "DECK_DRAG_ENTER" not in content:
    content = content.replace('void DeckStrip::dragEnterEvent(QDragEnterEvent* event) {\n',
    'void DeckStrip::dragEnterEvent(QDragEnterEvent* event) {\n    QString name = QString(kDeckNames[deckIndex_]);\n    bool valid = event->mimeData()->hasFormat("application/x-ngks-dj-track") || event->mimeData()->hasText();\n    qInfo() << "DECK_DRAG_ENTER deck=" << name << " valid=" << (valid ? 1 : 0);\n')
    content = '#include <QDebug>\n' + content

if "DECK_LOAD" not in content:
    m = re.search(r'void DeckStrip::dropEvent\(QDropEvent\* event\)\s*\{(.|\n)*?loadTrack\(path\);\n  }', content)
    if m:
        o_drop = m.group(0)
        n_drop = """void DeckStrip::dropEvent(QDropEvent* event) {
    if (!event->mimeData()->hasFormat("application/x-ngks-dj-track") && !event->mimeData()->hasText()) return;
    
    QString path;
    if (event->mimeData()->hasFormat("application/x-ngks-dj-track")) {
        path = QString::fromUtf8(event->mimeData()->data("application/x-ngks-dj-track"));
    } else {
        path = event->mimeData()->text();
    }
    
    QString name = QString(kDeckNames[deckIndex_]);
    qInfo() << "DECK_DROP deck=" << name << " file='" << path << "'";

    if (path.isEmpty()) return;

    if (bridge_->deckIsPlaying(deckIndex_)) {
        qInfo() << "DECK_LOAD_BLOCKED_PLAYING deck=" << name;
        QMessageBox* box = new QMessageBox(QMessageBox::Warning, "Load Blocked", "Deck is currently playing", QMessageBox::Ok, this);
        box->setAttribute(Qt::WA_DeleteOnClose);
        box->open();
        return;
    }

    qInfo() << "DECK_LOAD_ALLOWED deck=" << name;
    event->acceptProposedAction();
    loadTrack(path);
  }"""
        content = content.replace(o_drop, n_drop)

with open(file_strip, "w", encoding='utf8') as f:
    f.write(content)
