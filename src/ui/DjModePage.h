#pragma once

#include <QWidget>
#include <vector>

class AncillaryScreensWidget;
class DeckStrip;
class DjBrowserPane;
class DjLibraryDatabase;
class EngineBridge;
class LevelMeter;
struct TrackInfo;

class QLabel;
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

    /// No-op stub — DJ library is offline.
    void refreshLibrary() {}

signals:
    void backRequested();

private:
    // ── Dependencies (not owned) ──────────────────────────────────────────────
    EngineBridge& bridge_;
    DjLibraryDatabase& db_;
    const std::vector<TrackInfo>* tracks_{nullptr};

    // ── Ancillary (owned lazily) ──────────────────────────────────────────────
    AncillaryScreensWidget* ancillaryWidget_{nullptr};

    // ── Deck widgets ──────────────────────────────────────────────────────────
    DeckStrip* djDeckA_{nullptr};
    DeckStrip* djDeckB_{nullptr};

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
