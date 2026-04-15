#pragma once
#include <QTableView>

// QTableView subclass that renders only the track name (column 0) as the
// drag pixmap instead of the full multi-column row ghost that Qt produces
// by default.
class TrackDragView : public QTableView {
    Q_OBJECT
public:
    explicit TrackDragView(QWidget* parent = nullptr);

protected:
    void startDrag(Qt::DropActions supportedActions) override;
};
