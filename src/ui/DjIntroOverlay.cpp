#include "ui/DjIntroOverlay.h"

#include <QMediaPlayer>
#include <QAudioOutput>
#include <QVideoWidget>
#include <QVBoxLayout>
#include <QUrl>
#include <QSizePolicy>

DjIntroOverlay::DjIntroOverlay(QWidget* parent)
    : QWidget(parent)
{
    setStyleSheet(QStringLiteral("background: #000;"));

    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);
    layout->setSpacing(0);

    videoWidget_ = new QVideoWidget(this);
    videoWidget_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
    layout->addWidget(videoWidget_);

    player_ = new QMediaPlayer(this);
    auto* audioOutput = new QAudioOutput(this);
    player_->setAudioOutput(audioOutput);
    player_->setVideoOutput(videoWidget_);

    connect(player_, &QMediaPlayer::mediaStatusChanged, this,
        [this](QMediaPlayer::MediaStatus status) {
            if (status == QMediaPlayer::EndOfMedia ||
                status == QMediaPlayer::InvalidMedia) {
                player_->stop();
                emit finished();
            }
        });
}

void DjIntroOverlay::play(const QString& videoPath)
{
    player_->setSource(QUrl::fromLocalFile(videoPath));
    player_->play();
}
