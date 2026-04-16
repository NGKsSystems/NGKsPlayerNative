#pragma once

#include <QByteArray>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <vector>

// ── TrackInfo ─────────────────────────────────────────────────────────────────
struct TrackInfo {
    qint64  mediaId{0};
    QString fileFingerprint;
    QString filePath;
    QString title;
    QString artist;
    QString album;
    QString displayName;
    qint64  durationMs{0};
    QString durationStr;
    QString bpm;
    QString musicalKey;
    qint64  fileSize{0};
    // Legacy DB fields
    QString genre;
    QString camelotKey;
    double  energy{-1.0};
    double  loudnessLUFS{0.0};
    double  loudnessRange{0.0};
    QString cueIn;
    QString cueOut;
    double  danceability{-1.0};
    double  acousticness{-1.0};
    double  instrumentalness{-1.0};
    double  liveness{-1.0};
    int     year{0};
    int     rating{0};
    QString comments;
    bool    legacyImported{false};
    QString regularAnalysisState;
    QString regularAnalysisJson;
    QString liveAnalysisState;
    QString liveAnalysisJson;
};

// ── Playlist ──────────────────────────────────────────────────────────────────
struct Playlist {
    QString  name;
    QStringList trackPaths;
};

// ── Formatting helpers ────────────────────────────────────────────────────────
QString formatDurationMs(qint64 ms);
QString formatFileSize(qint64 bytes);

// ── Library JSON persistence ──────────────────────────────────────────────────
bool saveLibraryJson(const std::vector<TrackInfo>& tracks, const QString& folderPath);
bool loadLibraryJson(std::vector<TrackInfo>& outTracks, QString& outFolderPath);

// ── Playlist persistence ──────────────────────────────────────────────────────
bool savePlaylists(const std::vector<Playlist>& playlists);
bool loadPlaylists(std::vector<Playlist>& out);

// ── UI state persistence ──────────────────────────────────────────────────────
bool saveUiStateBlob(const QString& key, const QByteArray& state);
bool loadUiStateBlob(const QString& key, QByteArray& outState);
