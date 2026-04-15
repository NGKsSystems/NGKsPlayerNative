#pragma once
#include <QWidget>

class QAudioOutput;
class QEvent;
class QGraphicsOpacityEffect;
class QKeyEvent;
class QLabel;
class QMediaPlayer;
class QMouseEvent;
class QPropertyAnimation;
class QResizeEvent;
class QVideoWidget;

// Full-screen video intro played when entering DJ mode.
// Emits finished() when the clip ends (or on error), triggering
// the transition to DjModePage.
class DjIntroOverlay : public QWidget {
    Q_OBJECT
    Q_PROPERTY(qreal introGain READ introGain WRITE setIntroGain)
public:
    explicit DjIntroOverlay(QWidget* parent = nullptr);
    void play(const QString& videoPath);
    void stop();
    void skip();
    void duckForEngineReady();
    bool isActive() const;

    qreal introGain() const { return introGain_; }
    void setIntroGain(qreal value);

signals:
    void finished();

private:
    bool eventFilter(QObject* watched, QEvent* event) override;
    void mousePressEvent(QMouseEvent* event) override;
    void keyPressEvent(QKeyEvent* event) override;
    void resizeEvent(QResizeEvent* event) override;
    void animateSkipHint(qreal targetOpacity, int durationMs);
    void beginFinish();
    void completeFinish();
    void updateSkipHintGeometry();
    void updatePlaybackMix();

    QAudioOutput* audioOutput_{nullptr};
    QGraphicsOpacityEffect* opacityEffect_{nullptr};
    QPropertyAnimation* fadeAnimation_{nullptr};
    QPropertyAnimation* gainAnimation_{nullptr};
    QGraphicsOpacityEffect* skipHintOpacityEffect_{nullptr};
    QPropertyAnimation* skipHintAnimation_{nullptr};
    QLabel* skipHint_{nullptr};
    QMediaPlayer* player_{nullptr};
    QVideoWidget* videoWidget_{nullptr};
    bool finishing_{false};
    qreal introGain_{1.0};
};
