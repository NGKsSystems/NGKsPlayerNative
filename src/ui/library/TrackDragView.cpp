#include "ui/library/TrackDragView.h"

#include <QDrag>
#include <QFontMetrics>
#include <QMimeData>
#include <QPainter>
#include <QPixmap>

TrackDragView::TrackDragView(QWidget* parent) : QTableView(parent) {}

void TrackDragView::startDrag(Qt::DropActions supportedActions)
{
    const QModelIndexList rows = selectionModel()->selectedRows();
    if (rows.isEmpty()) {
        QTableView::startDrag(supportedActions);
        return;
    }

    // Use only the first selected row's column-0 display text
    const QString label =
        model()->data(rows.first().siblingAtColumn(0), Qt::DisplayRole).toString();

    QMimeData* mime = model()->mimeData(rows);
    if (!mime) {
        QTableView::startDrag(supportedActions);
        return;
    }

    // Build a compact pill-shaped pixmap matching the dark app palette
    const QFont font = this->font();
    const QFontMetrics fm(font);
    const int pad = 10;
    const int h   = fm.height() + pad * 2;
    const int w   = qMin(fm.horizontalAdvance(label) + pad * 2 + 4, 320);

    QPixmap pix(w, h);
    pix.fill(Qt::transparent);

    QPainter p(&pix);
    p.setRenderHint(QPainter::Antialiasing);

    // Background pill
    p.setPen(Qt::NoPen);
    p.setBrush(QColor(30, 32, 42, 220));
    p.drawRoundedRect(pix.rect(), 6, 6);

    // Accent left bar
    p.setBrush(QColor(255, 140, 0));
    p.drawRoundedRect(0, 0, 3, h, 2, 2);

    // Track name text
    p.setPen(QColor(220, 220, 220));
    p.setFont(font);
    p.drawText(pix.rect().adjusted(pad, 0, -pad, 0),
               Qt::AlignVCenter | Qt::AlignLeft,
               fm.elidedText(label, Qt::ElideRight, w - pad * 2));

    p.end();

    QDrag* drag = new QDrag(this);
    drag->setMimeData(mime);
    drag->setPixmap(pix);
    drag->setHotSpot(QPoint(20, h / 2));
    drag->exec(supportedActions, Qt::CopyAction);
}
