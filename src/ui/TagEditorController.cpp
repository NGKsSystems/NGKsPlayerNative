#include "TagEditorController.h"
#include "TagReaderService.h"
#include "TagWriterService.h"
#include "AlbumArtService.h"
#include "TagDatabaseService.h"
#include "AudioAnalysisService.h"
#include "FieldOwnership.h"
#include <QCoreApplication>
#include <QDebug>
#include <QThread>

TagEditorController::TagEditorController(QObject* parent)
    : QObject(parent)
{
    dbService_ = new TagDatabaseService(this);
    analysisService_ = new AudioAnalysisService(this);

    // Overlay DB: local to the app
    const QString overlayPath = QCoreApplication::applicationDirPath()
                                + QStringLiteral("/../../../data/runtime/tag_overlay.db");

    // Library DB: existing NGKsPlayer analysis database in %APPDATA%\ngksplayer
    const QString roaming = QString::fromLocal8Bit(qgetenv("APPDATA"));
    const QString libraryPath = roaming + QStringLiteral("/ngksplayer/library.db");

    if (!dbService_->init(overlayPath, libraryPath)) {
        qDebug() << "[TAG_EDITOR] DB_INIT_FAIL – overlay features degraded";
    }
}

TagEditorController::~TagEditorController() = default;

// ── Merge helper: apply DB overlay onto file-loaded TrackTagData ───

void TagEditorController::mergeDbOverlay(const QHash<QString, QVariant>& dbFields)
{
    if (dbFields.isEmpty()) {
        qDebug() << "[TAG_EDITOR] MERGE skip (no DB record)";
        return;
    }

    auto merge = [&](const QString& fieldName, QString& target) {
        FieldOwner owner = ownerOf(fieldName);
        auto it = dbFields.constFind(fieldName);
        bool hasDb = (it != dbFields.constEnd() && !it.value().toString().isEmpty());

        switch (owner) {
        case FieldOwner::FileOnly:
            current_.setSource(fieldName, FieldSource::File);
            break;
        case FieldOwner::DbOnly:
            if (hasDb) {
                target = it.value().toString();
                current_.setSource(fieldName, FieldSource::Db);
            }
            break;
        case FieldOwner::HybridFilePriority:
            if (!target.isEmpty()) {
                current_.setSource(fieldName, FieldSource::File);
            } else if (hasDb) {
                target = it.value().toString();
                current_.setSource(fieldName, FieldSource::Db);
            }
            break;
        case FieldOwner::HybridDbPriority:
            if (hasDb) {
                target = it.value().toString();
                current_.setSource(fieldName, FieldSource::Db);
            } else if (!target.isEmpty()) {
                current_.setSource(fieldName, FieldSource::File);
            }
            break;
        }
        qDebug() << "[TAG_EDITOR] MERGE_FIELD" << fieldName
                 << "owner=" << ownerName(owner)
                 << "source=" << sourceName(current_.sourceOf(fieldName))
                 << "val=" << target.left(40);
    };

    auto mergeDouble = [&](const QString& fieldName, double& target) {
        FieldOwner owner = ownerOf(fieldName);
        auto it = dbFields.constFind(fieldName);
        bool hasDb = (it != dbFields.constEnd() && it.value().toDouble() >= 0.0);

        switch (owner) {
        case FieldOwner::FileOnly:
            current_.setSource(fieldName, FieldSource::File);
            break;
        case FieldOwner::DbOnly:
            if (hasDb) {
                target = it.value().toDouble();
                current_.setSource(fieldName, FieldSource::Db);
            }
            break;
        case FieldOwner::HybridFilePriority:
            if (target >= 0.0) {
                current_.setSource(fieldName, FieldSource::File);
            } else if (hasDb) {
                target = it.value().toDouble();
                current_.setSource(fieldName, FieldSource::Db);
            }
            break;
        case FieldOwner::HybridDbPriority:
            if (hasDb) {
                target = it.value().toDouble();
                current_.setSource(fieldName, FieldSource::Db);
            } else if (target >= 0.0) {
                current_.setSource(fieldName, FieldSource::File);
            }
            break;
        }
        qDebug() << "[TAG_EDITOR] MERGE_FIELD" << fieldName
                 << "owner=" << ownerName(owner)
                 << "source=" << sourceName(current_.sourceOf(fieldName))
                 << "val=" << target;
    };

    // Core ID3 (FILE_ONLY) – just mark source
    for (const auto& f : {TagFields::Title, TagFields::Artist, TagFields::Album,
                          TagFields::AlbumArtist, TagFields::Genre, TagFields::Year,
                          TagFields::TrackNumber, TagFields::DiscNumber}) {
        current_.setSource(f, FieldSource::File);
    }

    // HYBRID_FILE_PRIORITY
    merge(TagFields::Bpm, current_.bpm);
    merge(TagFields::Key, current_.musicalKey);
    merge(TagFields::Comments, current_.comments);
    merge(TagFields::Labels, current_.labels);

    // Rating (int)
    {
        auto it = dbFields.constFind(TagFields::Rating);
        bool hasDb = (it != dbFields.constEnd() && it.value().toInt() > 0);
        if (current_.rating > 0) {
            current_.setSource(TagFields::Rating, FieldSource::File);
        } else if (hasDb) {
            current_.rating = it.value().toInt();
            current_.setSource(TagFields::Rating, FieldSource::Db);
        }
        qDebug() << "[TAG_EDITOR] MERGE_FIELD rating"
                 << "source=" << sourceName(current_.sourceOf(TagFields::Rating))
                 << "val=" << current_.rating;
    }

    // HYBRID_DB_PRIORITY
    merge(TagFields::CueIn, current_.cueIn);
    merge(TagFields::CueOut, current_.cueOut);
    merge(TagFields::Camelot, current_.camelotKey);

    mergeDouble(TagFields::Energy, current_.energy);
    mergeDouble(TagFields::Loudness, current_.loudnessLUFS);
    mergeDouble(TagFields::LRA, current_.loudnessRange);
    mergeDouble(TagFields::Danceability, current_.danceability);
    mergeDouble(TagFields::Acousticness, current_.acousticness);
    mergeDouble(TagFields::Instrumentalness, current_.instrumentalness);
    mergeDouble(TagFields::Liveness, current_.liveness);

    // DB_ONLY
    merge(TagFields::ColorLabel, current_.colorLabel);
    merge(TagFields::DjNotes, current_.djNotes);
}

// ── Load ───────────────────────────────────────────────────────────

void TagEditorController::loadFile(const QString& path)
{
    qDebug() << "[TAG_EDITOR] FILE_OPEN" << path;

    // 1. Read tags from file
    current_ = TagReaderService::loadTagsForFile(path);
    qDebug() << "[TAG_EDITOR] METADATA_LOADED"
             << "title=" << current_.title
             << "artist=" << current_.artist
             << "album=" << current_.album;

    // 2. Album art pipeline: embedded -> folder fallback -> empty
    if (current_.hasAlbumArt) {
        artSource_ = ArtEmbedded;
        qDebug() << "[TAG_EDITOR] ALBUM_ART_SOURCE EMBEDDED"
                 << current_.albumArt.width() << "x" << current_.albumArt.height();
    } else {
        QPixmap folderArt = AlbumArtService::findFolderAlbumArt(path);
        if (!folderArt.isNull()) {
            current_.albumArt = folderArt;
            current_.hasAlbumArt = true;
            artSource_ = ArtFolder;
            qDebug() << "[TAG_EDITOR] ALBUM_ART_SOURCE FOLDER_FALLBACK"
                     << folderArt.width() << "x" << folderArt.height();
        } else {
            artSource_ = ArtNone;
            qDebug() << "[TAG_EDITOR] ALBUM_ART_SOURCE NONE";
        }
    }

    // 3. Load DB overlay and merge
    QHash<QString, QVariant> dbFields = dbService_->loadByFile(path);
    mergeDbOverlay(dbFields);

    // 4. Store original for revert
    original_ = current_;
    current_.dirty = false;
    original_.dirty = false;

    emit fileLoaded(current_);
    emit dirtyChanged(false);
}

// ── Save ───────────────────────────────────────────────────────────

void TagEditorController::saveDbFields()
{
    QHash<QString, QVariant> dbPayload;

    auto maybeAdd = [&](const QString& fieldName, const QVariant& val) {
        FieldOwner owner = ownerOf(fieldName);
        if (owner == FieldOwner::DbOnly ||
            owner == FieldOwner::HybridFilePriority ||
            owner == FieldOwner::HybridDbPriority) {
            dbPayload.insert(fieldName, val);
            qDebug() << "[TAG_EDITOR] FIELD_SAVE_TARGET" << fieldName << "-> DB";
        }
    };

    maybeAdd(TagFields::Bpm, current_.bpm);
    maybeAdd(TagFields::Key, current_.musicalKey);
    maybeAdd(TagFields::Comments, current_.comments);
    maybeAdd(TagFields::Rating, current_.rating);
    maybeAdd(TagFields::Labels, current_.labels);
    maybeAdd(TagFields::ColorLabel, current_.colorLabel);
    maybeAdd(TagFields::DjNotes, current_.djNotes);
    maybeAdd(TagFields::CueIn, current_.cueIn);
    maybeAdd(TagFields::CueOut, current_.cueOut);
    maybeAdd(TagFields::Energy, current_.energy);
    maybeAdd(TagFields::Loudness, current_.loudnessLUFS);
    maybeAdd(TagFields::LRA, current_.loudnessRange);
    maybeAdd(TagFields::Danceability, current_.danceability);
    maybeAdd(TagFields::Acousticness, current_.acousticness);
    maybeAdd(TagFields::Instrumentalness, current_.instrumentalness);
    maybeAdd(TagFields::Liveness, current_.liveness);
    maybeAdd(TagFields::Camelot, current_.camelotKey);

    if (!dbPayload.isEmpty()) {
        bool ok = dbService_->saveBulk(current_.sourceFilePath, dbPayload);
        qDebug() << "[TAG_EDITOR] DB_SAVE" << (ok ? "OK" : "FAIL")
                 << "fields=" << dbPayload.size();
    }
}

void TagEditorController::saveFile()
{
    if (!hasFile()) {
        emit saveResult(false, QStringLiteral("No file loaded"));
        return;
    }

    qDebug() << "[TAG_EDITOR] SAVE_BEGIN" << current_.sourceFilePath;

    // ── FILE_ONLY and HYBRID fields → write to file ──
    for (const auto& f : {TagFields::Title, TagFields::Artist, TagFields::Album,
                          TagFields::AlbumArtist, TagFields::Genre, TagFields::Year,
                          TagFields::TrackNumber, TagFields::DiscNumber,
                          TagFields::Bpm, TagFields::Key, TagFields::Comments}) {
        qDebug() << "[TAG_EDITOR] FIELD_SAVE_TARGET" << f << "-> FILE";
    }

    const bool ok = TagWriterService::saveTagsToFile(current_);
    if (!ok) {
        qDebug() << "[TAG_EDITOR] SAVE_FAIL TagWriterService returned false";
        emit saveResult(false, QStringLiteral("Failed to write tags to file"));
        return;
    }

    qDebug() << "[TAG_EDITOR] SAVE_SUCCESS wrote to disk";

    // ── DB_ONLY and HYBRID fields → write to DB ──
    saveDbFields();

    // ── Verification: reload file and compare FILE_ONLY fields ──
    TrackTagData verify = TagReaderService::loadTagsForFile(current_.sourceFilePath);

    bool verified = true;
    QStringList mismatches;
    auto chk = [&](const QString& field, const QString& saved, const QString& read) {
        if (saved != read) {
            verified = false;
            mismatches.append(field + QStringLiteral(": saved='") + saved
                              + QStringLiteral("' read='") + read + QStringLiteral("'"));
        }
    };
    chk(QStringLiteral("title"),       current_.title,       verify.title);
    chk(QStringLiteral("artist"),      current_.artist,      verify.artist);
    chk(QStringLiteral("album"),       current_.album,       verify.album);
    chk(QStringLiteral("albumArtist"), current_.albumArtist, verify.albumArtist);
    chk(QStringLiteral("genre"),       current_.genre,       verify.genre);
    chk(QStringLiteral("year"),        current_.year,        verify.year);
    chk(QStringLiteral("trackNumber"), current_.trackNumber, verify.trackNumber);
    chk(QStringLiteral("discNumber"),  current_.discNumber,  verify.discNumber);
    chk(QStringLiteral("bpm"),         current_.bpm,         verify.bpm);
    chk(QStringLiteral("musicalKey"),  current_.musicalKey,  verify.musicalKey);
    chk(QStringLiteral("comments"),    current_.comments,    verify.comments);

    if (current_.hasAlbumArt && !verify.hasAlbumArt) {
        verified = false;
        mismatches.append(QStringLiteral("albumArt: saved=present read=missing"));
    }
    if (!current_.hasAlbumArt && verify.hasAlbumArt) {
        verified = false;
        mismatches.append(QStringLiteral("albumArt: saved=removed read=present"));
    }

    if (verified) {
        qDebug() << "[TAG_EDITOR] SAVE_VERIFY_SUCCESS all fields match after reload";
        original_ = current_;
        setDirty(false);
        emit saveResult(true, QStringLiteral("Saved and verified successfully"));
    } else {
        qDebug() << "[TAG_EDITOR] SAVE_VERIFY_FAIL" << mismatches.join(QStringLiteral("; "));
        emit saveResult(false,
            QStringLiteral("Save wrote to disk but verification failed: ")
            + mismatches.join(QStringLiteral("; ")));
    }
}

// ── Revert / Dirty / Art ───────────────────────────────────────────

void TagEditorController::revertChanges()
{
    if (!hasFile()) return;
    qDebug() << "[TAG_EDITOR] REVERT" << current_.sourceFilePath;
    current_ = original_;
    current_.dirty = false;
    emit fileLoaded(current_);
    emit dirtyChanged(false);
}

void TagEditorController::markDirty()
{
    qDebug() << "[TAG_EDITOR] DIRTY_SET" << current_.sourceFilePath;
    setDirty(true);
}

void TagEditorController::updateFields(const QString& title, const QString& artist,
                                       const QString& album, const QString& albumArtist,
                                       const QString& genre, const QString& year,
                                       const QString& trackNumber, const QString& discNumber,
                                       const QString& bpm, const QString& musicalKey,
                                       const QString& comments)
{
    current_.title       = title;
    current_.artist      = artist;
    current_.album       = album;
    current_.albumArtist = albumArtist;
    current_.genre       = genre;
    current_.year        = year;
    current_.trackNumber = trackNumber;
    current_.discNumber  = discNumber;
    current_.bpm         = bpm;
    current_.musicalKey  = musicalKey;
    current_.comments    = comments;
}

void TagEditorController::replaceAlbumArt(const QPixmap& art)
{
    qDebug() << "[TAG_EDITOR] REPLACE_ART"
             << art.width() << "x" << art.height();
    current_.albumArt = art;
    current_.hasAlbumArt = !art.isNull();
    setDirty(true);
    emit fileLoaded(current_);
}

void TagEditorController::removeAlbumArt()
{
    qDebug() << "[TAG_EDITOR] REMOVE_ART" << current_.sourceFilePath;
    current_.albumArt = QPixmap();
    current_.hasAlbumArt = false;
    setDirty(true);
    emit fileLoaded(current_);
}

void TagEditorController::setDirty(bool d)
{
    if (current_.dirty != d) {
        current_.dirty = d;
        emit dirtyChanged(d);
    }
}

// ── Analysis pipeline ──────────────────────────────────────────────

void TagEditorController::runAnalysis()
{
    if (!hasFile()) {
        qDebug() << "[TAG_EDITOR] ANALYSIS_SKIP no file loaded";
        return;
    }

    qDebug() << "[TAG_EDITOR] ANALYSIS_TRIGGER" << current_.sourceFilePath;
    emit analysisStarted();

    // Run analysis (blocking — called from UI thread for now)
    AnalysisResult result = analysisService_->analyzeFile(current_.sourceFilePath,
                                                            current_.genre);

    if (result.valid) {
        applyAnalysisResult(result);

        // Persist to DB
        QHash<QString, QVariant> dbPayload;
        dbPayload.insert(TagFields::Bpm,      current_.bpm);
        dbPayload.insert(TagFields::Energy,    current_.energy);
        dbPayload.insert(TagFields::Loudness,  current_.loudnessLUFS);
        dbPayload.insert(TagFields::LRA,       current_.loudnessRange);
        dbPayload.insert(TagFields::CueIn,     current_.cueIn);
        dbPayload.insert(TagFields::CueOut,    current_.cueOut);
        dbPayload.insert(TagFields::Danceability,     current_.danceability);
        dbPayload.insert(TagFields::Acousticness,     current_.acousticness);
        dbPayload.insert(TagFields::Instrumentalness, current_.instrumentalness);
        dbPayload.insert(TagFields::Liveness,         current_.liveness);
        dbPayload.insert(TagFields::Camelot,           current_.camelotKey);
        dbPayload.insert(TagFields::Key,               current_.musicalKey);

        bool saveOk = dbService_->saveBulk(current_.sourceFilePath, dbPayload);
        qDebug() << "[ANALYSIS] ANALYSIS_SAVE_DB" << (saveOk ? "success" : "fail")
                 << "fields=" << dbPayload.size();
        qInfo().noquote() << "[KEY_DETECT] KEY_FINAL_TO_DB:"
            << current_.musicalKey << "/" << current_.camelotKey
            << "save=" << (saveOk ? "OK" : "FAIL");
        qInfo().noquote() << "[KEY_DETECT] CAMELOT_FINAL_TO_DB:"
            << current_.camelotKey
            << "save=" << (saveOk ? "OK" : "FAIL");
    } else {
        qDebug() << "[ANALYSIS] ANALYSIS_SAVE_DB fail reason=" << result.errorMsg;
    }

    emit analysisFinished(result);
    emit fileLoaded(current_);  // refresh UI with new data
}

void TagEditorController::clearAnalysis()
{
    if (!hasFile()) return;

    qInfo().noquote() << "[TAG_EDITOR] CLEAR_ANALYSIS" << current_.sourceFilePath;

    // Reset analysis fields to defaults
    current_.bpm          = QString();
    current_.musicalKey   = QString();
    current_.camelotKey   = QString();
    current_.energy       = -1.0;
    current_.loudnessLUFS = 0.0;
    current_.loudnessRange = 0.0;
    current_.cueIn        = QString();
    current_.cueOut       = QString();
    current_.danceability      = -1.0;
    current_.acousticness      = -1.0;
    current_.instrumentalness  = -1.0;
    current_.liveness          = -1.0;
    current_.transitionDifficulty = -1.0;

    // Clear from overlay DB
    QHash<QString, QVariant> dbPayload;
    dbPayload.insert(TagFields::Bpm,               QVariant());
    dbPayload.insert(TagFields::Key,               QVariant());
    dbPayload.insert(TagFields::Camelot,           QVariant());
    dbPayload.insert(TagFields::Energy,            QVariant());
    dbPayload.insert(TagFields::Loudness,          QVariant());
    dbPayload.insert(TagFields::LRA,               QVariant());
    dbPayload.insert(TagFields::CueIn,             QVariant());
    dbPayload.insert(TagFields::CueOut,            QVariant());
    dbPayload.insert(TagFields::Danceability,      QVariant());
    dbPayload.insert(TagFields::Acousticness,      QVariant());
    dbPayload.insert(TagFields::Instrumentalness,  QVariant());
    dbPayload.insert(TagFields::Liveness,          QVariant());

    bool ok = dbService_->saveBulk(current_.sourceFilePath, dbPayload);
    qInfo().noquote() << "[TAG_EDITOR] CLEAR_ANALYSIS_DB"
        << (ok ? "OK" : "FAIL") << "fields=" << dbPayload.size();

    emit fileLoaded(current_);  // refresh UI to show cleared values
}

// Reverse Camelot wheel → standard key notation
static QString camelotToStandardKey(const QString& camelot)
{
    // Camelot number → root note index (0=C .. 11=B)
    // Major (B): 8B=C, 3B=C#, 10B=D, 5B=Eb, 12B=E, 7B=F, 2B=F#, 9B=G, 4B=Ab, 11B=A, 6B=Bb, 1B=B
    // Minor (A): 5A=Cm, 12A=C#m, 7A=Dm, 2A=Ebm, 9A=Em, 4A=Fm, 11A=F#m, 6A=Gm, 1A=Abm, 8A=Am, 3A=Bbm, 10A=Bm
    static const char* noteNames[12] = {
        "C", "Db", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"
    };
    // camelotMajor[root] = camelotNum  →  invert to: camelotNum → root
    // Forward: {8,3,10,5,12,7,2,9,4,11,6,1} for C..B
    static const int majorNumToRoot[13] = {
        -1, 11, 6, 1, 8, 3, 10, 5, 0, 7, 2, 9, 4
    }; // index = camelot number (1-12), value = root note index
    // Forward minor: {5,12,7,2,9,4,11,6,1,8,3,10} for Cm..Bm
    static const int minorNumToRoot[13] = {
        -1, 8, 3, 10, 5, 0, 7, 2, 9, 4, 11, 6, 1
    };

    if (camelot.length() < 2) return camelot;
    QChar modeCh = camelot.at(camelot.length() - 1);
    bool isMajor = (modeCh == QLatin1Char('B') || modeCh == QLatin1Char('b'));
    int num = QStringView(camelot).left(camelot.length() - 1).toInt();
    if (num < 1 || num > 12) return camelot;

    int root = isMajor ? majorNumToRoot[num] : minorNumToRoot[num];
    if (root < 0) return camelot;

    QString key = QString::fromLatin1(noteNames[root]);
    if (!isMajor) key += QLatin1Char('m');
    return key;
}

void TagEditorController::applyAnalysisResult(const AnalysisResult& result)
{
    // Apply analysis results to current TrackTagData
    // Analysis is an explicit user action — always overwrite

    // BPM: always overwrite from analysis (user clicked Analyze)
    qDebug() << "[TAG_EDITOR] ANALYSIS_APPLY bpm_old=" << current_.bpm
             << "bpm_new=" << result.bpm;
    current_.bpm = QString::number(result.bpm, 'f', 1);
    current_.setSource(TagFields::Bpm, FieldSource::Db);

    // BPM resolver details
    current_.rawBpm        = result.rawBpm;
    current_.resolvedBpm   = result.resolvedBpm;
    current_.bpmConfidence = result.bpmConfidence;
    current_.bpmFamily     = result.bpmFamily;
    current_.bpmCandidateCount = static_cast<int>(result.bpmCandidates.size());
    if (result.bpmCandidates.size() >= 2)
        current_.bpmCandidateGap = result.bpmCandidates[0].score
                                 - result.bpmCandidates[1].score;
    else
        current_.bpmCandidateGap = 1.0;

    // Key detection detail
    current_.keyConfidence      = result.keyConfidence;
    current_.keyAmbiguous       = result.keyAmbiguous;
    current_.keyRunnerUp        = result.keyRunnerUp;
    current_.keyCorrectionReason = result.keyCorrectionReason;

    // Key: derive standard key from Camelot and overwrite
    current_.camelotKey = result.camelotKey;
    current_.setSource(TagFields::Camelot, FieldSource::Db);
    current_.musicalKey = camelotToStandardKey(result.camelotKey);
    current_.setSource(TagFields::Key, FieldSource::Db);

    qInfo().noquote() << "[KEY_DETECT] KEY_RAW_SELECTED:"
        << result.camelotKey
        << "confidence=" << result.keyConfidence
        << "ambiguous=" << result.keyAmbiguous
        << "runnerUp=" << result.keyRunnerUp;
    qInfo().noquote() << "[KEY_DETECT] KEY_CORRECTED_SELECTED:"
        << result.camelotKey
        << "correction=" << (result.keyCorrectionReason.isEmpty()
                             ? QStringLiteral("none")
                             : result.keyCorrectionReason);
    qInfo().noquote() << "[KEY_DETECT] KEY_FINAL_TO_UI:"
        << current_.musicalKey << "/" << current_.camelotKey;
    qInfo().noquote() << "[KEY_DETECT] KEY_UI_SOURCE: FieldSource::Db (analysis)";
    qInfo().noquote() << "[KEY_DETECT] CAMELOT_FINAL_TO_UI:" << current_.camelotKey;
    qInfo().noquote() << "[KEY_DETECT] CAMELOT_UI_SOURCE: FieldSource::Db (analysis)";

    // Energy (HYBRID_DB_PRIORITY — always overwrite)
    current_.energy = result.energy;
    current_.setSource(TagFields::Energy, FieldSource::Db);

    // Loudness (HYBRID_DB_PRIORITY)
    current_.loudnessLUFS = result.loudnessLUFS;
    current_.setSource(TagFields::Loudness, FieldSource::Db);

    // LRA (HYBRID_DB_PRIORITY)
    current_.loudnessRange = result.lra;
    current_.setSource(TagFields::LRA, FieldSource::Db);

    // Cue In/Out (HYBRID_DB_PRIORITY)
    current_.cueIn = QString::number(result.cueInSeconds, 'f', 2);
    current_.setSource(TagFields::CueIn, FieldSource::Db);
    current_.cueOut = QString::number(result.cueOutSeconds, 'f', 2);
    current_.setSource(TagFields::CueOut, FieldSource::Db);

    // Auto DJ features (HYBRID_DB_PRIORITY)
    current_.danceability = result.danceability;
    current_.setSource(TagFields::Danceability, FieldSource::Db);
    current_.acousticness = result.acousticness;
    current_.setSource(TagFields::Acousticness, FieldSource::Db);
    current_.instrumentalness = result.instrumentalness;
    current_.setSource(TagFields::Instrumentalness, FieldSource::Db);
    current_.liveness = result.liveness;
    current_.setSource(TagFields::Liveness, FieldSource::Db);

    current_.transitionDifficulty = result.transitionDifficulty;

    qDebug() << "[TAG_EDITOR] ANALYSIS_APPLIED"
             << "energy=" << current_.energy
             << "lufs=" << current_.loudnessLUFS
             << "bpm=" << current_.bpm
             << "key=" << current_.musicalKey
             << "camelot=" << current_.camelotKey;
}
