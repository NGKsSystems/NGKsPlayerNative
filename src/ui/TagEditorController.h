#pragma once
#include <QObject>
#include "TrackTagData.h"
#include "AnalysisResult.h"

class TagDatabaseService;
class AudioAnalysisService;

class TagEditorController : public QObject
{
    Q_OBJECT
public:
    explicit TagEditorController(QObject* parent = nullptr);
    ~TagEditorController();

    void loadFile(const QString& path);
    void saveFile();
    void revertChanges();
    void markDirty();
    void replaceAlbumArt(const QPixmap& art);
    void removeAlbumArt();
    void updateFields(const QString& title, const QString& artist,
                      const QString& album, const QString& albumArtist,
                      const QString& genre, const QString& year,
                      const QString& trackNumber, const QString& discNumber,
                      const QString& bpm, const QString& musicalKey,
                      const QString& comments);

    void runAnalysis();
    void clearAnalysis();

    const TrackTagData& data() const { return current_; }
    bool isDirty() const { return current_.dirty; }
    bool hasFile() const { return !current_.sourceFilePath.isEmpty(); }

    enum ArtSource { ArtNone, ArtEmbedded, ArtFolder };
    ArtSource lastArtSource() const { return artSource_; }

signals:
    void fileLoaded(const TrackTagData& data);
    void dirtyChanged(bool dirty);
    void saveResult(bool success, const QString& message);
    void analysisStarted();
    void analysisFinished(const AnalysisResult& result);

private:
    void setDirty(bool d);
    void mergeDbOverlay(const QHash<QString, QVariant>& dbFields);
    void saveDbFields();
    void applyAnalysisResult(const AnalysisResult& result);

    TrackTagData current_;
    TrackTagData original_;
    ArtSource artSource_{ArtNone};
    TagDatabaseService* dbService_{nullptr};
    AudioAnalysisService* analysisService_{nullptr};
};
