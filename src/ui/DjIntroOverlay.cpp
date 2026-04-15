#include "ui/DjIntroOverlay.h"

#include <QAudioOutput>
#include <QEvent>
#include <QGraphicsOpacityEffect>
#include <QKeyEvent>
#include <QLabel>
#include <QMediaPlayer>
#include <QMouseEvent>
#include <QPropertyAnimation>
#include <QEasingCurve>
#include <QResizeEvent>
#include <QVideoWidget>
#include <QVBoxLayout>
#include <QSizePolicy>
#include <QUrl>

namespace {

constexpr int kFadeDurationMs = 650;
constexpr int kDuckDurationMs = 240;
constexpr int kHintFadeInDurationMs = 420;
constexpr int kHintFadeOutDurationMs = 180;
constexpr qreal kDuckGain = 0.22;

}

DjIntroOverlay::DjIntroOverlay(QWidget* parent)
    : QWidget(parent)
{
    setAttribute(Qt::WA_StyledBackground, true);
    setStyleSheet(QStringLiteral("background: #000;"));
    setFocusPolicy(Qt::StrongFocus);
    hide();

    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(0);

    videoWidget_ = new QVideoWidget(this);
    videoWidget_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
    videoWidget_->installEventFilter(this);
    layout->addWidget(videoWidget_);

    skipHint_ = new QLabel(
        QStringLiteral("Click or press Space, Enter, or Esc to skip"),
        this);
    skipHint_->setAlignment(Qt::AlignCenter);
    skipHint_->setAttribute(Qt::WA_TransparentForMouseEvents, true);
    skipHint_->setStyleSheet(QStringLiteral(
        "background: rgba(8, 12, 20, 180);"
        "color: #f5f7fa;"
        "border: 1px solid rgba(255, 255, 255, 60);"
        "border-radius: 16px;"
        "padding: 8px 16px;"
        "font-size: 12px;"
        "font-weight: 600;"
        "letter-spacing: 0.4px;"));
    skipHintOpacityEffect_ = new QGraphicsOpacityEffect(skipHint_);
    skipHintOpacityEffect_->setOpacity(0.0);
    skipHint_->setGraphicsEffect(skipHintOpacityEffect_);
    skipHintAnimation_ = new QPropertyAnimation(skipHintOpacityEffect_, "opacity", this);
    skipHintAnimation_->setEasingCurve(QEasingCurve::InOutCubic);
    skipHint_->hide();
    skipHint_->raise();

    opacityEffect_ = new QGraphicsOpacityEffect(this);
    opacityEffect_->setOpacity(1.0);
    setGraphicsEffect(opacityEffect_);

    fadeAnimation_ = new QPropertyAnimation(opacityEffect_, "opacity", this);
    fadeAnimation_->setDuration(kFadeDurationMs);
    fadeAnimation_->setEasingCurve(QEasingCurve::InOutCubic);

    gainAnimation_ = new QPropertyAnimation(this, "introGain", this);
    gainAnimation_->setDuration(kDuckDurationMs);
    gainAnimation_->setEasingCurve(QEasingCurve::InOutCubic);

    player_ = new QMediaPlayer(this);
    audioOutput_ = new QAudioOutput(this);
    audioOutput_->setMuted(false);
    audioOutput_->setVolume(1.0);
    player_->setAudioOutput(audioOutput_);
    player_->setVideoOutput(videoWidget_);

    connect(player_, &QMediaPlayer::mediaStatusChanged, this,
        [this](QMediaPlayer::MediaStatus status) {
            if (status == QMediaPlayer::EndOfMedia ||
                status == QMediaPlayer::InvalidMedia) {
                beginFinish();
            }
        });

    connect(player_, &QMediaPlayer::positionChanged, this, [this](qint64 position) {
        if (finishing_) return;

        const qint64 duration = player_->duration();
        if (duration <= kFadeDurationMs) return;
        if (position >= duration - kFadeDurationMs) beginFinish();
    });

    connect(player_, &QMediaPlayer::errorOccurred, this, [this](QMediaPlayer::Error, const QString&) {
        beginFinish();
    });

    connect(fadeAnimation_, &QPropertyAnimation::valueChanged, this, [this](const QVariant& value) {
        Q_UNUSED(value);
        updatePlaybackMix();
    });

    connect(fadeAnimation_, &QPropertyAnimation::finished, this, [this]() {
        completeFinish();
    });
}

void DjIntroOverlay::play(const QString& videoPath)
{
    fadeAnimation_->stop();
    gainAnimation_->stop();
    player_->stop();
    finishing_ = false;
    opacityEffect_->setOpacity(1.0);
    introGain_ = 1.0;
    audioOutput_->setMuted(false);
    updatePlaybackMix();

    if (videoPath.trimmed().isEmpty()) {
        hide();
        emit finished();
        return;
    }

    show();
    raise();
    if (skipHint_) {
        skipHintAnimation_->stop();
        skipHintOpacityEffect_->setOpacity(0.0);
        skipHint_->show();
        skipHint_->raise();
        updateSkipHintGeometry();
        animateSkipHint(1.0, kHintFadeInDurationMs);
    }
    activateWindow();
    setFocus(Qt::OtherFocusReason);
    grabKeyboard();
    player_->setSource(QUrl::fromLocalFile(videoPath));
    player_->play();
}

void DjIntroOverlay::stop()
{
    fadeAnimation_->stop();
    gainAnimation_->stop();
    player_->stop();
    finishing_ = false;
    opacityEffect_->setOpacity(1.0);
    introGain_ = 1.0;
    updatePlaybackMix();
    releaseKeyboard();
    if (skipHint_) {
        skipHintAnimation_->stop();
        skipHintOpacityEffect_->setOpacity(0.0);
        skipHint_->hide();
    }
    hide();
}

void DjIntroOverlay::skip()
{
    if (!isActive()) return;
    beginFinish();
}

void DjIntroOverlay::duckForEngineReady()
{
    if (!isActive() || finishing_) return;
    gainAnimation_->stop();
    gainAnimation_->setStartValue(introGain_);
    gainAnimation_->setEndValue(kDuckGain);
    gainAnimation_->start();
}

bool DjIntroOverlay::isActive() const
{
    return isVisible() && player_ && player_->playbackState() != QMediaPlayer::StoppedState;
}

void DjIntroOverlay::setIntroGain(qreal value)
{
    introGain_ = std::clamp(value, 0.0, 1.0);
    updatePlaybackMix();
}

bool DjIntroOverlay::eventFilter(QObject* watched, QEvent* event)
{
    if (watched == videoWidget_) {
        if (event->type() == QEvent::MouseButtonPress) {
            skip();
            return true;
        }
        if (event->type() == QEvent::KeyPress) {
            auto* keyEvent = static_cast<QKeyEvent*>(event);
            if (keyEvent->key() == Qt::Key_Escape ||
                keyEvent->key() == Qt::Key_Return ||
                keyEvent->key() == Qt::Key_Enter ||
                keyEvent->key() == Qt::Key_Space) {
                skip();
                return true;
            }
        }
    }

    return QWidget::eventFilter(watched, event);
}

void DjIntroOverlay::mousePressEvent(QMouseEvent* event)
{
    skip();
    QWidget::mousePressEvent(event);
}

void DjIntroOverlay::keyPressEvent(QKeyEvent* event)
{
    if (event->key() == Qt::Key_Escape ||
        event->key() == Qt::Key_Return ||
        event->key() == Qt::Key_Enter ||
        event->key() == Qt::Key_Space) {
        skip();
        event->accept();
        return;
    }

    QWidget::keyPressEvent(event);
}

void DjIntroOverlay::resizeEvent(QResizeEvent* event)
{
    QWidget::resizeEvent(event);
    updateSkipHintGeometry();
}

void DjIntroOverlay::beginFinish()
{
    if (finishing_) return;
    finishing_ = true;

    if (skipHint_ && skipHint_->isVisible()) {
        animateSkipHint(0.0, kHintFadeOutDurationMs);
    }

    fadeAnimation_->stop();
    fadeAnimation_->setStartValue(opacityEffect_->opacity());
    fadeAnimation_->setEndValue(0.0);
    fadeAnimation_->start();
}

void DjIntroOverlay::completeFinish()
{
    player_->stop();
    hide();
    opacityEffect_->setOpacity(1.0);
    introGain_ = 1.0;
    updatePlaybackMix();
    finishing_ = false;
    releaseKeyboard();
    if (skipHint_) {
        skipHintAnimation_->stop();
        skipHintOpacityEffect_->setOpacity(0.0);
        skipHint_->hide();
    }
    emit finished();
}

void DjIntroOverlay::animateSkipHint(qreal targetOpacity, int durationMs)
{
    if (!skipHintAnimation_ || !skipHintOpacityEffect_) return;

    skipHintAnimation_->stop();
    skipHintAnimation_->setDuration(durationMs);
    skipHintAnimation_->setStartValue(skipHintOpacityEffect_->opacity());
    skipHintAnimation_->setEndValue(targetOpacity);
    skipHintAnimation_->start();
}

void DjIntroOverlay::updateSkipHintGeometry()
{
    if (!skipHint_) return;

    const int maxWidth = qMin(width() - 32, 420);
    if (maxWidth <= 0) return;

    skipHint_->setWordWrap(true);
    skipHint_->setFixedWidth(maxWidth);
    skipHint_->adjustSize();

    const int hintX = (width() - skipHint_->width()) / 2;
    const int hintY = qMax(16, height() - skipHint_->height() - 28);
    skipHint_->move(hintX, hintY);
}

void DjIntroOverlay::updatePlaybackMix()
{
    if (!audioOutput_ || !opacityEffect_) return;
    audioOutput_->setVolume(std::clamp(opacityEffect_->opacity() * introGain_, 0.0, 1.0));
}
