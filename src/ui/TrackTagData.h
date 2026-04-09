#pragma once
#include <QString>
#include <QPixmap>
#include <QHash>
#include "FieldOwnership.h"

struct TrackTagData
{
    QString sourceFilePath;

    // Core ID3 tags
    QString title;
    QString artist;
    QString album;
    QString albumArtist;
    QString genre;
    QString year;
    QString trackNumber;
    QString discNumber;
    QString bpm;
    QString musicalKey;
    QString comments;

    QPixmap albumArt;
    bool    hasAlbumArt{false};
    bool    dirty{false};

    // DJ workflow (app-level, not ID3)
    int     rating{0};
    QString colorLabel;
    QString labels;
    QString djNotes;

    // Analysis (read-only display)
    double  energy{-1.0};
    double  loudnessLUFS{0.0};
    double  loudnessRange{0.0};
    QString cueIn;
    QString cueOut;
    double  danceability{-1.0};
    double  acousticness{-1.0};
    double  instrumentalness{-1.0};
    double  liveness{-1.0};
    QString camelotKey;
    double  transitionDifficulty{-1.0};

    // BPM resolver results
    double  rawBpm{0.0};
    double  resolvedBpm{0.0};
    double  bpmConfidence{0.0};
    QString bpmFamily;
    int     bpmCandidateCount{0};
    double  bpmCandidateGap{0.0};

    // Key detection detail
    double  keyConfidence{0.0};
    bool    keyAmbiguous{false};
    QString keyRunnerUp;
    QString keyCorrectionReason;

    // ── Per-field source tracking (hybrid model) ──
    QHash<QString, FieldSource> fieldSources;

    void setSource(const QString& field, FieldSource src)
    { fieldSources[field] = src; }

    FieldSource sourceOf(const QString& field) const
    { return fieldSources.value(field, FieldSource::None); }
};
