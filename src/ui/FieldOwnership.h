#pragma once
#include <QString>
#include <QHash>

// ── Field ownership model for hybrid metadata + DB overlay ─────────

enum class FieldOwner {
    FileOnly,           // write to file only
    DbOnly,             // write to DB only
    HybridFilePriority, // file wins unless empty, then DB
    HybridDbPriority    // DB wins unless missing, then file
};

enum class FieldSource {
    None,
    File,
    Db,
    Merged
};

// Per-field tracking state
struct FieldState {
    QString value;
    FieldOwner owner{FieldOwner::FileOnly};
    FieldSource source{FieldSource::None};
    bool dirty{false};

    bool isEmpty() const { return value.isEmpty(); }
};

// Double-precision variant for analysis fields
struct FieldStateDouble {
    double value{-1.0};
    FieldOwner owner{FieldOwner::DbOnly};
    FieldSource source{FieldSource::None};
    bool dirty{false};

    bool isEmpty() const { return value < 0.0; }
};

// Integer variant for rating
struct FieldStateInt {
    int value{0};
    FieldOwner owner{FieldOwner::HybridFilePriority};
    FieldSource source{FieldSource::None};
    bool dirty{false};

    bool isEmpty() const { return value == 0; }
};

// ── Canonical field name list ──────────────────────────────────────

namespace TagFields {
    // FILE_ONLY
    inline const QString Title        = QStringLiteral("title");
    inline const QString Artist       = QStringLiteral("artist");
    inline const QString Album        = QStringLiteral("album");
    inline const QString AlbumArtist  = QStringLiteral("albumArtist");
    inline const QString Genre        = QStringLiteral("genre");
    inline const QString Year         = QStringLiteral("year");
    inline const QString TrackNumber  = QStringLiteral("trackNumber");
    inline const QString DiscNumber   = QStringLiteral("discNumber");

    // HYBRID_FILE_PRIORITY
    inline const QString Bpm          = QStringLiteral("bpm");
    inline const QString Key          = QStringLiteral("key");
    inline const QString Comments     = QStringLiteral("comments");
    inline const QString Rating       = QStringLiteral("rating");
    inline const QString Labels       = QStringLiteral("labels");
    inline const QString AlbumArt     = QStringLiteral("albumArt");

    // HYBRID_DB_PRIORITY
    inline const QString CueIn        = QStringLiteral("cueIn");
    inline const QString CueOut       = QStringLiteral("cueOut");
    inline const QString Energy       = QStringLiteral("energy");
    inline const QString Loudness     = QStringLiteral("loudness");
    inline const QString Danceability = QStringLiteral("danceability");
    inline const QString Acousticness = QStringLiteral("acousticness");
    inline const QString Instrumentalness = QStringLiteral("instrumentalness");
    inline const QString Liveness     = QStringLiteral("liveness");
    inline const QString Camelot      = QStringLiteral("camelot");
    inline const QString LRA          = QStringLiteral("lra");

    // DB_ONLY
    inline const QString ScanTimestamp = QStringLiteral("scanTimestamp");
    inline const QString ColorLabel    = QStringLiteral("colorLabel");
    inline const QString DjNotes       = QStringLiteral("djNotes");
}

// ── Ownership lookup ───────────────────────────────────────────────

inline FieldOwner ownerOf(const QString& fieldName)
{
    // FILE_ONLY
    if (fieldName == TagFields::Title ||
        fieldName == TagFields::Artist ||
        fieldName == TagFields::Album ||
        fieldName == TagFields::AlbumArtist ||
        fieldName == TagFields::Genre ||
        fieldName == TagFields::Year ||
        fieldName == TagFields::TrackNumber ||
        fieldName == TagFields::DiscNumber)
        return FieldOwner::FileOnly;

    // HYBRID_FILE_PRIORITY
    if (fieldName == TagFields::Bpm ||
        fieldName == TagFields::Key ||
        fieldName == TagFields::Comments ||
        fieldName == TagFields::Rating ||
        fieldName == TagFields::Labels ||
        fieldName == TagFields::AlbumArt)
        return FieldOwner::HybridFilePriority;

    // HYBRID_DB_PRIORITY
    if (fieldName == TagFields::CueIn ||
        fieldName == TagFields::CueOut ||
        fieldName == TagFields::Energy ||
        fieldName == TagFields::Loudness ||
        fieldName == TagFields::Danceability ||
        fieldName == TagFields::Acousticness ||
        fieldName == TagFields::Instrumentalness ||
        fieldName == TagFields::Liveness ||
        fieldName == TagFields::Camelot ||
        fieldName == TagFields::LRA)
        return FieldOwner::HybridDbPriority;

    // DB_ONLY (everything else)
    return FieldOwner::DbOnly;
}

inline const char* ownerName(FieldOwner o)
{
    switch (o) {
    case FieldOwner::FileOnly:           return "FILE_ONLY";
    case FieldOwner::DbOnly:             return "DB_ONLY";
    case FieldOwner::HybridFilePriority: return "HYBRID_FILE_PRIORITY";
    case FieldOwner::HybridDbPriority:   return "HYBRID_DB_PRIORITY";
    }
    return "UNKNOWN";
}

inline const char* sourceName(FieldSource s)
{
    switch (s) {
    case FieldSource::None:   return "NONE";
    case FieldSource::File:   return "FILE";
    case FieldSource::Db:     return "DB";
    case FieldSource::Merged: return "MERGED";
    }
    return "UNKNOWN";
}
