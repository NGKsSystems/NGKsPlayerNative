#include "ui/TrackDetailPanel.h"

#include <QLabel>
#include <QVBoxLayout>

TrackDetailPanel::TrackDetailPanel(QWidget* parent)
    : QWidget(parent)
{
    setStyleSheet(QStringLiteral("background: transparent;"));

    auto* lv = new QVBoxLayout(this);
    lv->setContentsMargins(12, 12, 12, 12);
    lv->setSpacing(6);

    titleLabel_ = new QLabel(QStringLiteral("Track Info"), this);
    titleLabel_->setObjectName(QStringLiteral("detailTitle"));
    lv->addWidget(titleLabel_);

    addRow(QStringLiteral("TITLE"),        trackTitle_);
    addRow(QStringLiteral("ARTIST"),       trackArtist_);
    addRow(QStringLiteral("ALBUM"),        trackAlbum_);
    addRow(QStringLiteral("GENRE"),        trackGenre_);
    addRow(QStringLiteral("DURATION"),     trackDuration_);
    addRow(QStringLiteral("BPM"),          trackBpm_);
    addRow(QStringLiteral("KEY"),          trackKey_);
    addRow(QStringLiteral("CAMELOT"),      trackCamelot_);
    addRow(QStringLiteral("ENERGY"),       trackEnergy_);
    addRow(QStringLiteral("LUFS"),         trackLufs_);
    addRow(QStringLiteral("CUE IN/OUT"),   trackCue_);
    addRow(QStringLiteral("DANCEABILITY"), trackDance_);
    addRow(QStringLiteral("FILE SIZE"),    trackSize_);
    addRow(QStringLiteral("FILE PATH"),    trackPath_);

    lv->addStretch(1);
}

void TrackDetailPanel::addRow(const QString& fieldLabel, QLabel*& valueOut)
{
    auto* lv = qobject_cast<QVBoxLayout*>(layout());

    auto* fl = new QLabel(fieldLabel, this);
    fl->setObjectName(QStringLiteral("detailField"));
    lv->addWidget(fl);

    valueOut = new QLabel(QStringLiteral("-"), this);
    valueOut->setObjectName(QStringLiteral("detailValue"));
    valueOut->setWordWrap(true);
    lv->addWidget(valueOut);
}

void TrackDetailPanel::display(const TrackInfo& t)
{
    titleLabel_->setText(t.title.isEmpty() ? QStringLiteral("Unknown Track") : t.title);
    trackTitle_->setText(    t.title.isEmpty()       ? QStringLiteral("-") : t.title);
    trackArtist_->setText(   t.artist.isEmpty()      ? QStringLiteral("-") : t.artist);
    trackAlbum_->setText(    t.album.isEmpty()       ? QStringLiteral("-") : t.album);
    trackGenre_->setText(    t.genre.isEmpty()       ? QStringLiteral("-") : t.genre);
    trackDuration_->setText( t.durationStr.isEmpty() ? QStringLiteral("--:--") : t.durationStr);
    trackBpm_->setText(      t.bpm.isEmpty()         ? QStringLiteral("-") : t.bpm);
    trackKey_->setText(      t.musicalKey.isEmpty()  ? QStringLiteral("-") : t.musicalKey);
    trackCamelot_->setText(  t.camelotKey.isEmpty()  ? QStringLiteral("-") : t.camelotKey);
    trackEnergy_->setText(   t.energy >= 0
        ? QString::number(t.energy, 'f', 1) : QStringLiteral("-"));
    trackLufs_->setText(     t.loudnessLUFS != 0.0
        ? QStringLiteral("%1 LUFS (range %2)")
              .arg(QString::number(t.loudnessLUFS, 'f', 1),
                   QString::number(t.loudnessRange, 'f', 1))
        : QStringLiteral("-"));
    {
        QString cueStr;
        if (!t.cueIn.isEmpty() || !t.cueOut.isEmpty())
            cueStr = QStringLiteral("%1 / %2")
                .arg(t.cueIn.isEmpty()  ? QStringLiteral("-") : t.cueIn,
                     t.cueOut.isEmpty() ? QStringLiteral("-") : t.cueOut);
        trackCue_->setText(cueStr.isEmpty() ? QStringLiteral("-") : cueStr);
    }
    trackDance_->setText(    t.danceability >= 0
        ? QString::number(t.danceability, 'f', 1) : QStringLiteral("-"));
    trackSize_->setText(     t.fileSize > 0 ? formatFileSize(t.fileSize) : QStringLiteral("-"));
    trackPath_->setText(     t.filePath);
}

void TrackDetailPanel::clear()
{
    titleLabel_->setText(   QStringLiteral("Track Info"));
    trackTitle_->setText(   QStringLiteral("-"));
    trackArtist_->setText(  QStringLiteral("-"));
    trackAlbum_->setText(   QStringLiteral("-"));
    trackGenre_->setText(   QStringLiteral("-"));
    trackDuration_->setText(QStringLiteral("--:--"));
    trackBpm_->setText(     QStringLiteral("-"));
    trackKey_->setText(     QStringLiteral("-"));
    trackCamelot_->setText( QStringLiteral("-"));
    trackEnergy_->setText(  QStringLiteral("-"));
    trackLufs_->setText(    QStringLiteral("-"));
    trackCue_->setText(     QStringLiteral("-"));
    trackDance_->setText(   QStringLiteral("-"));
    trackSize_->setText(    QStringLiteral("-"));
    trackPath_->setText(    QStringLiteral("-"));
}
