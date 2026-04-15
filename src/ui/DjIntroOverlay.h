#pragma once
#include <QWidget>

class QMediaPlayer;
class QVideoWidget;

// Full-screen video intro played when entering DJ mode.
// Emits finished() when the clip ends (or on error), triggering
// the transition to DjModePage.
class DjIntroOverlay : public QWidget {
    Q_OBJECT
public:
    explicit DjIntroOverlay(QWidget* parent = nullptr);
    void play(const QString& videoPath);

signals:
    void finished();

private:
    QMediaPlayer* player_{nullptr};
    QVideoWidget* videoWidget_{nullptr};
};
