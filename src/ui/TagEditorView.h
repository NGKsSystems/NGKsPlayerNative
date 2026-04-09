#pragma once
#include <QWidget>
#include "TagEditorController.h"

class QLineEdit;
class QTextEdit;
class QLabel;
class QPushButton;
class QComboBox;

class TagEditorView : public QWidget
{
    Q_OBJECT
public:
    explicit TagEditorView(QWidget* parent = nullptr);
    TagEditorController* controller() const { return controller_; }
    void openFile(const QString& path);
    void setExtraContext(int rating, const QString& colorLabel,
                         const QString& labels,
                         double energy, double loudnessLUFS,
                         double loudnessRange,
                         const QString& cueIn, const QString& cueOut,
                         double danceability, double acousticness,
                         double instrumentalness, double liveness,
                         const QString& camelotKey,
                         double transitionDifficulty,
                         double rawBpm = 0.0,
                         double resolvedBpm = 0.0,
                         double bpmConfidence = 0.0,
                         const QString& bpmFamily = {});

signals:
    void backRequested();

private slots:
    void onFileLoaded(const TrackTagData& data);
    void onDirtyChanged(bool dirty);
    void onSaveResult(bool success, const QString& msg);
    void onBrowseFile();
    void onReplaceArt();
    void onRemoveArt();
    void onFieldEdited();

private:
    void buildUi();
    void populateFields(const TrackTagData& data);
    void clearAnalysisDisplay();
    void updateQualityBadge(const TrackTagData& data);

    TagEditorController* controller_;

    QLabel*      filePathLabel_;

    // Core metadata
    QLineEdit*   titleEdit_;
    QLineEdit*   artistEdit_;
    QLineEdit*   albumEdit_;
    QLineEdit*   albumArtistEdit_;
    QLineEdit*   genreEdit_;
    QLineEdit*   yearEdit_;
    QLineEdit*   trackNumEdit_;
    QLineEdit*   discNumEdit_;
    QLineEdit*   bpmEdit_;
    QLineEdit*   keyEdit_;
    QTextEdit*   commentsEdit_;

    // DJ workflow
    QComboBox*   ratingCombo_;
    QComboBox*   colorCombo_;
    QLineEdit*   labelsEdit_;
    QTextEdit*   djNotesEdit_;

    // Album art
    QLabel*      artPreview_;
    QPushButton* replaceArtBtn_;
    QPushButton* removeArtBtn_;

    // Analysis display (read-only)
    QLabel* analysisEnergyVal_;
    QLabel* analysisLoudnessVal_;
    QLabel* analysisBpmDiagVal_;
    QLabel* analysisQualityBadge_;
    QLabel* analysisCueInVal_;
    QLabel* analysisCueOutVal_;
    QLabel* analysisDanceVal_;
    QLabel* analysisAcousticVal_;
    QLabel* analysisInstrumVal_;
    QLabel* analysisLivenessVal_;
    QLabel* analysisCamelotVal_;
    QLabel* analysisLRAVal_;
    QLabel* analysisTransDiffVal_;

    // Actions
    QPushButton* saveBtn_;
    QPushButton* revertBtn_;
    QPushButton* analyzeBtn_;
    QPushButton* clearAnalysisBtn_;
    QLabel*      statusLabel_;

    bool populatingFields_{false};
};
