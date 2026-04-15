#pragma once

#include <QWidget>
#include "ui/library/LibraryPersistence.h"

class QLabel;
class QVBoxLayout;

/// Scrollable side panel that shows metadata for a single selected track.
/// Owner is responsible for wrapping this in a QScrollArea.
class TrackDetailPanel : public QWidget
{
    Q_OBJECT
public:
    explicit TrackDetailPanel(QWidget* parent = nullptr);

    void display(const TrackInfo& track);
    void clear();

private:
    void addRow(const QString& fieldLabel, QLabel*& valueOut);

    QLabel* titleLabel_{nullptr};
    QLabel* trackTitle_{nullptr};
    QLabel* trackArtist_{nullptr};
    QLabel* trackAlbum_{nullptr};
    QLabel* trackGenre_{nullptr};
    QLabel* trackDuration_{nullptr};
    QLabel* trackBpm_{nullptr};
    QLabel* trackKey_{nullptr};
    QLabel* trackCamelot_{nullptr};
    QLabel* trackEnergy_{nullptr};
    QLabel* trackLufs_{nullptr};
    QLabel* trackCue_{nullptr};
    QLabel* trackDance_{nullptr};
    QLabel* trackSize_{nullptr};
    QLabel* trackPath_{nullptr};
};
