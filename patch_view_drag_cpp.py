import re
with open('src/ui/dj/browser/DjTrackTableView.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

impl = '''
#include <QMouseEvent>
#include <QApplication>
#include <QDrag>
#include <QMimeData>

void DjTrackTableView::mousePressEvent(QMouseEvent* event) {
    if (event->button() == Qt::LeftButton) {
        dragStartPos_ = event->pos();
    }
    QTableView::mousePressEvent(event);
}

void DjTrackTableView::mouseMoveEvent(QMouseEvent* event) {
    if (!(event->buttons() & Qt::LeftButton)) {
        return QTableView::mouseMoveEvent(event);
    }
    if ((event->pos() - dragStartPos_).manhattanLength() < QApplication::startDragDistance()) {
        return QTableView::mouseMoveEvent(event);
    }

    QModelIndex idx = indexAt(dragStartPos_);
    if (!idx.isValid()) {
        return QTableView::mouseMoveEvent(event);
    }

    QString filePath = idx.data(Qt::UserRole + 1).toString();
    if (filePath.isEmpty()) {
        return QTableView::mouseMoveEvent(event);
    }

    QDrag* drag = new QDrag(this);
    QMimeData* mime = new QMimeData;
    mime->setText(filePath);
    mime->setData("application/x-ngks-dj-track", filePath.toUtf8());
    drag->setMimeData(mime);
    drag->exec(Qt::CopyAction);
}
'''
if "void DjTrackTableView::mousePressEvent" not in content:
    content += '\n' + impl.strip() + '\n'

if "#include <QMouseEvent>" not in content:
    content = "#include <QMouseEvent>\n#include <QApplication>\n#include <QDrag>\n#include <QMimeData>\n" + content

with open('src/ui/dj/browser/DjTrackTableView.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
