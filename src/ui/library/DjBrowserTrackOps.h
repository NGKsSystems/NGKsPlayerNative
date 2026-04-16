#pragma once

#include "ui/AnalysisResult.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryPersistence.h"

#include <QDateTime>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QStringList>

#include <optional>

namespace DjBrowserTrackOps {

struct BatchRenameResult {
    int renamedCount{0};
    int failedCount{0};
};

inline std::optional<QPair<int, QChar>> parseCamelotKey(const QString& text)
{
    QString digits;
    QChar letter;
    for (const QChar ch : text.trimmed()) {
        if (ch.isDigit()) digits.append(ch);
        else if (ch.isLetter()) letter = ch.toUpper();
    }

    bool ok = false;
    const int number = digits.toInt(&ok);
    if (!ok || !letter.isLetter()) return std::nullopt;
    return QPair<int, QChar>(number, letter);
}

inline QString standardKeyFromCamelot(const QString& camelot)
{
    static const char* noteNames[12] = {
        "C", "Db", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"
    };
    static const int majorNumToRoot[13] = {
        -1, 11, 6, 1, 8, 3, 10, 5, 0, 7, 2, 9, 4
    };
    static const int minorNumToRoot[13] = {
        -1, 8, 3, 10, 5, 0, 7, 2, 9, 4, 11, 6, 1
    };

    const auto parsed = parseCamelotKey(camelot);
    if (!parsed) return camelot.trimmed();

    const int number = parsed->first;
    if (number < 1 || number > 12) return camelot.trimmed();

    const bool isMajor = parsed->second == QLatin1Char('B');
    const int root = isMajor ? majorNumToRoot[number] : minorNumToRoot[number];
    if (root < 0) return camelot.trimmed();

    QString key = QString::fromLatin1(noteNames[root]);
    if (!isMajor) key += QLatin1Char('m');
    return key;
}

inline QString formatStoredBpm(double bpm)
{
    if (bpm <= 0.0) return {};
    QString text = QString::number(bpm, 'f', 1);
    if (text.endsWith(QStringLiteral(".0"))) text.chop(2);
    return text;
}

inline QString compactJson(const QJsonObject& object)
{
    return QString::fromUtf8(QJsonDocument(object).toJson(QJsonDocument::Compact));
}

inline QJsonObject serializeAnalysisResult(const AnalysisResult& result)
{
    QJsonObject object;
    object.insert(QStringLiteral("valid"), result.valid);
    object.insert(QStringLiteral("errorMsg"), result.errorMsg);
    object.insert(QStringLiteral("bpm"), result.bpm);
    object.insert(QStringLiteral("loudnessLUFS"), result.loudnessLUFS);
    object.insert(QStringLiteral("peakDBFS"), result.peakDBFS);
    object.insert(QStringLiteral("energy"), result.energy);
    object.insert(QStringLiteral("rawBpm"), result.rawBpm);
    object.insert(QStringLiteral("resolvedBpm"), result.resolvedBpm);
    object.insert(QStringLiteral("bpmConfidence"), result.bpmConfidence);
    object.insert(QStringLiteral("bpmFamily"), result.bpmFamily);
    object.insert(QStringLiteral("onsetDensity"), result.onsetDensity);
    object.insert(QStringLiteral("hfPercussiveScore"), result.hfPercussiveScore);
    object.insert(QStringLiteral("cueInSeconds"), result.cueInSeconds);
    object.insert(QStringLiteral("cueOutSeconds"), result.cueOutSeconds);
    object.insert(QStringLiteral("dynamicRangeLU"), result.dynamicRangeLU);
    object.insert(QStringLiteral("danceability"), result.danceability);
    object.insert(QStringLiteral("acousticness"), result.acousticness);
    object.insert(QStringLiteral("instrumentalness"), result.instrumentalness);
    object.insert(QStringLiteral("liveness"), result.liveness);
    object.insert(QStringLiteral("camelotKey"), result.camelotKey);
    object.insert(QStringLiteral("lra"), result.lra);
    object.insert(QStringLiteral("transitionDifficulty"), result.transitionDifficulty);
    object.insert(QStringLiteral("keyConfidence"), result.keyConfidence);
    object.insert(QStringLiteral("keyAmbiguous"), result.keyAmbiguous);
    object.insert(QStringLiteral("keyRunnerUp"), result.keyRunnerUp);
    object.insert(QStringLiteral("keyCorrectionReason"), result.keyCorrectionReason);
    object.insert(QStringLiteral("beatGridConfidence"), result.beatGridConfidence);
    object.insert(QStringLiteral("spectralCentroid"), result.spectralCentroid);
    object.insert(QStringLiteral("introDuration"), result.introDuration);
    object.insert(QStringLiteral("outroDuration"), result.outroDuration);
    object.insert(QStringLiteral("durationSeconds"), result.durationSeconds);
    object.insert(QStringLiteral("sampleRate"), result.sampleRate);

    QJsonArray candidates;
    for (const BpmCandidate& candidate : result.bpmCandidates) {
        QJsonObject item;
        item.insert(QStringLiteral("bpm"), candidate.bpm);
        item.insert(QStringLiteral("family"), candidate.family);
        item.insert(QStringLiteral("score"), candidate.score);
        item.insert(QStringLiteral("reason"), candidate.reason);
        candidates.append(item);
    }
    object.insert(QStringLiteral("bpmCandidates"), candidates);
    return object;
}

inline TrackInfo ensureTrackRecord(DjLibraryDatabase* db, const QString& filePath)
{
    TrackInfo track = db ? db->trackByPath(filePath).value_or(TrackInfo{}) : TrackInfo{};
    const QFileInfo fileInfo(filePath);
    track.filePath = filePath;
    if (track.displayName.isEmpty()) track.displayName = fileInfo.completeBaseName();
    if (track.title.isEmpty()) track.title = fileInfo.completeBaseName();
    if (track.fileSize <= 0) track.fileSize = fileInfo.size();
    return track;
}

inline bool persistRegularAnalysisState(DjLibraryDatabase* db,
                                        const QString& filePath,
                                        const QString& state,
                                        const QJsonObject& payload)
{
    if (!db || !db->isOpen()) return false;

    TrackInfo track = ensureTrackRecord(db, filePath);
    track.regularAnalysisState = state;
    track.regularAnalysisJson = compactJson(payload);
    const qint64 trackId = db->trackIdByPath(filePath)
        .value_or(QDateTime::currentMSecsSinceEpoch());
    return db->upsertTrack(trackId, track);
}

inline bool persistLiveAnalysisPanel(DjLibraryDatabase* db,
                                     const QString& filePath,
                                     const QJsonObject& panel)
{
    if (!db || !db->isOpen()) return false;

    TrackInfo track = ensureTrackRecord(db, filePath);
    track.liveAnalysisState = panel.value(QStringLiteral("state")).toString(
        panel.value(QStringLiteral("panel_state")).toString());
    track.liveAnalysisJson = compactJson(panel);
    const qint64 trackId = db->trackIdByPath(filePath)
        .value_or(QDateTime::currentMSecsSinceEpoch());
    return db->upsertTrack(trackId, track);
}

inline void syncTrackPathChange(DjLibraryDatabase* db, const QString& oldPath, const QString& newPath)
{
    if (!db) return;

    const auto trackId = db->trackIdByPath(oldPath);
    const auto track = db->trackByPath(oldPath);
    if (!trackId || !track) return;

    TrackInfo updated = *track;
    updated.filePath = newPath;
    updated.displayName = QFileInfo(newPath).completeBaseName();
    db->upsertTrack(*trackId, updated);
}

inline bool renameFileAndSyncTrack(DjLibraryDatabase* db, const QString& oldPath, const QString& newPath)
{
    if (oldPath.isEmpty() || newPath.isEmpty() || oldPath == newPath) return false;
    if (QFile::exists(newPath)) return false;
    if (!QFile::rename(oldPath, newPath)) return false;

    syncTrackPathChange(db, oldPath, newPath);
    return true;
}

inline BatchRenameResult replaceFileNamesAndSyncTracks(DjLibraryDatabase* db,
                                                       const QDir& folder,
                                                       const QStringList& fileNames,
                                                       const QString& findText,
                                                       const QString& replaceText)
{
    BatchRenameResult result;
    for (const QString& name : fileNames) {
        const QString oldPath = folder.filePath(name);
        const QString newName = QString(name).replace(findText, replaceText, Qt::CaseInsensitive);
        if (newName == name) continue;

        const QString newPath = folder.filePath(newName);
        if (!renameFileAndSyncTrack(db, oldPath, newPath)) {
            ++result.failedCount;
            continue;
        }

        ++result.renamedCount;
    }

    return result;
}

inline bool persistAnalysisResult(DjLibraryDatabase* db, const QString& filePath, const AnalysisResult& result)
{
    if (!db || !db->isOpen()) return false;

    const QFileInfo fileInfo(filePath);
    TrackInfo track = ensureTrackRecord(db, filePath);

    track.regularAnalysisState = result.valid
        ? QStringLiteral("ANALYSIS_COMPLETE")
        : QStringLiteral("ANALYSIS_FAILED");
    track.regularAnalysisJson = compactJson(serializeAnalysisResult(result));

    if (!result.valid) {
        const qint64 trackId = db->trackIdByPath(filePath)
            .value_or(QDateTime::currentMSecsSinceEpoch());
        return db->upsertTrack(trackId, track);
    }

    if (result.durationSeconds > 0.0) {
        track.durationMs = static_cast<qint64>(result.durationSeconds * 1000.0);
        track.durationStr = formatDurationMs(track.durationMs);
    }

    const QString bpm = formatStoredBpm(result.bpm);
    if (!bpm.isEmpty()) track.bpm = bpm;
    if (!result.camelotKey.isEmpty()) {
        track.camelotKey = result.camelotKey;
        track.musicalKey = standardKeyFromCamelot(result.camelotKey);
    }

    track.energy = result.energy;
    track.loudnessLUFS = result.loudnessLUFS;
    if (result.cueInSeconds > 0.0) track.cueIn = QString::number(result.cueInSeconds, 'f', 2);
    if (result.cueOutSeconds > 0.0) track.cueOut = QString::number(result.cueOutSeconds, 'f', 2);
    track.danceability = result.danceability;

    const qint64 trackId = db->trackIdByPath(filePath)
        .value_or(QDateTime::currentMSecsSinceEpoch());
    return db->upsertTrack(trackId, track);
}

} // namespace DjBrowserTrackOps