#pragma once

#include <QObject>
#include <QString>
#include <QWidget>
#include <vector>

class AncillaryScreensWidget;
class AnalysisBridge;
class DeckStrip;
class QAction;
class DjBrowserPane;
class DjLibraryDatabase;
class EngineBridge;
class LevelMeter;
struct TrackInfo;

class QLabel;
class QMenu;
class QPushButton;
class QSlider;
class QTimer;
class QWidget;

class DjModePage : public QWidget
{
    Q_OBJECT
public:
    explicit DjModePage(EngineBridge& bridge, DjLibraryDatabase& db,
                        QWidget* parent = nullptr);

    /// Update track-list pointer (call after allTracks_ changes).
    void setTrackList(const std::vector<TrackInfo>* tracks);
    void setBrowserRootFolder(const QString& folderPath);

    void setImportUiState(const QString& title,
                          const QString& detail,
                          bool importEnabled,
                          bool runAnalysisEnabled);
    QMenu* utilityMenu() const { return djUtilityMenu_; }

    /// No-op stub — DJ library is offline.
    void refreshLibrary() {}

signals:
    void backRequested();
    void importFolderRequested();
    void importAnalysisRequested();

private:
    void wireDeckAnalysisBridge(AnalysisBridge* analysisBridge, DeckStrip* deckWidget, int deckIndex);
    void loadDeckTrack(int deckIndex, const QString& path);
    void startDeckLiveAnalysis(int deckIndex, const QString& path);
    void syncDeckLiveAnalysis(int deckIndex);
    AnalysisBridge* deckAnalysisBridge(int deckIndex) const;
    QString* pendingDeckAnalysisPath(int deckIndex);

    void openAncillaryScreens();
    void showProAudioClipperPlaceholder();

    // ── Dependencies (not owned) ──────────────────────────────────────────────
    EngineBridge& bridge_;
    DjLibraryDatabase& db_;
    const std::vector<TrackInfo>* tracks_{nullptr};

    // ── Ancillary (owned lazily) ──────────────────────────────────────────────
    AncillaryScreensWidget* ancillaryWidget_{nullptr};

    // ── Browser / import pane ─────────────────────────────────────────────────
    DjBrowserPane* djBrowser_{nullptr};
    QMenu* djUtilityMenu_{nullptr};
    QAction* backAction_{nullptr};
    QAction* importFolderAction_{nullptr};
    QAction* importAnalysisAction_{nullptr};
    QAction* proAudioClipperAction_{nullptr};
    QAction* ancillaryScreensAction_{nullptr};

    // ── Deck widgets ──────────────────────────────────────────────────────────
    DeckStrip* djDeckA_{nullptr};
    DeckStrip* djDeckB_{nullptr};
    AnalysisBridge* deckAnalysisBridgeA_{nullptr};
    AnalysisBridge* deckAnalysisBridgeB_{nullptr};
    QString deckAnalysisPathA_;
    QString deckAnalysisPathB_;

    // ── Master section ────────────────────────────────────────────────────────
    QSlider* djCrossfader_{nullptr};
    LevelMeter* djMasterMeterL_{nullptr};
    LevelMeter* djMasterMeterR_{nullptr};
    QSlider* djCueMix_{nullptr};
    QSlider* djCueVol_{nullptr};
    QPushButton* djOutputModeBtn_{nullptr};

    // ── Device-lost banner ────────────────────────────────────────────────────
    QWidget* djDeviceLostBanner_{nullptr};
    QLabel* djBannerTitleLabel_{nullptr};
    QLabel* djRecoveryStatusLabel_{nullptr};
    QPushButton* djRecoverBtn_{nullptr};
    QTimer* djBannerDismissTimer_{nullptr};
};
