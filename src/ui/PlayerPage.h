#pragma once

#include <QWidget>
#include <atomic>
#include <random>
#include <vector>

#include "ui/library/LibraryPersistence.h"
#include "ui/audio/AudioProfileStore.h"

class DiagnosticsDialog;
class DjLibraryDatabase;
class DjLibraryWidget;
class EqPanel;
class EngineBridge;
class VisualizerWidget;

class QComboBox;
class QLabel;
class QLineEdit;
class QPushButton;
class QSlider;
class QTimer;

class PlayerPage : public QWidget
{
    Q_OBJECT
public:
    explicit PlayerPage(EngineBridge& bridge, DjLibraryDatabase& db, QWidget* parent = nullptr);

    /// Update the track list pointer (call after allTracks_ changes in MainWindow).
    void setTrackList(const std::vector<TrackInfo>* tracks) { tracks_ = tracks; }

    /// Start playing the given track index (called by landing page).
    void activateTrack(int trackIndex);

    /// Rebuild the player library filter (call when page becomes visible or library changes).
    void refreshLibrary();

    /// Feed audio meter level from pollStatus (30fps handled internally by vizTimer).
    void setAudioLevel(float level);

    int currentTrackIndex() const { return currentTrackIndex_; }

signals:
    void backRequested();
    void diagnosticsRefreshRequested();

private slots:
    void applySelectedAudioProfile();
    void onAudioProfileApplied(bool ok);

private:
    void loadAndPlayTrack(int trackIndex);
    void playNextTrack();
    void playPrevTrack();
    void onEndOfMedia();
    QString playModeLabel() const;
    void updatePlayModeButton();
    void updateUpNextLabel();
    void rebuildSmartShufflePool();
    void advanceSmartShuffle();
    void refreshPlayerLibrary();
    void highlightPlayerLibraryItem(int trackIndex);
    void requestAudioProfilesRefresh(bool logMarker);
    void refreshAudioProfilesUi(bool logMarker);
    void finishAudioApply();

    // ── Dependencies (not owned) ──────────────────────────────────────────────
    EngineBridge& bridge_;
    DjLibraryDatabase& db_;
    const std::vector<TrackInfo>* tracks_{nullptr};

    // ── Playback state ────────────────────────────────────────────────────────
    enum class PlayMode { PlayOnce, PlayInOrder, RepeatAll, Shuffle, SmartShuffle };
    PlayMode playMode_{PlayMode::PlayInOrder};
    int currentTrackIndex_{-1};
    bool seekSliderPressed_{false};
    uint64_t uiTrackGen_{0};
    bool juceSimpleModeReady_{false};
    double titlePulseEnvelope_{0.0};
    bool meterDiagLogged_{false};

    // ── Shuffle ───────────────────────────────────────────────────────────────
    std::vector<int> smartShufflePool_;
    int smartShufflePos_{-1};
    std::mt19937 shuffleRng_{std::random_device{}()};

    // ── Audio profile state ───────────────────────────────────────────────────
    UiAudioProfilesStore audioProfilesStore_{};
    std::atomic<bool> audioApplyInProgress_{false};
    bool pendingAudioProfilesRefresh_{false};
    bool pendingAudioProfilesRefreshLogMarker_{false};
    QString lastAgMarkerKey_{};
    QString lastAkActiveProfileMarker_{};
    QString pendingApplyProfileName_{};

    // ── Hero / visualizer widgets ─────────────────────────────────────────────
    VisualizerWidget* visualizer_{nullptr};
    QTimer* vizTimer_{nullptr};
    QLabel* playerTrackLabel_{nullptr};
    QLabel* nowPlayingTag_{nullptr};
    QLabel* playerArtistLabel_{nullptr};
    QLabel* playerMetaLabel_{nullptr};
    QLabel* playerStateLabel_{nullptr};
    QLabel* upNextLabel_{nullptr};
    QPushButton* pulseBtn_{nullptr};
    QPushButton* tuneBtn_{nullptr};
    QPushButton* vizLineBtn_{nullptr};
    QPushButton* vizBarsBtn_{nullptr};
    QPushButton* vizCircleBtn_{nullptr};
    QPushButton* vizNoneBtn_{nullptr};

    // ── Transport ─────────────────────────────────────────────────────────────
    QSlider* seekSlider_{nullptr};
    QSlider* volumeSlider_{nullptr};
    QPushButton* playPauseBtn_{nullptr};
    QPushButton* prevBtn_{nullptr};
    QPushButton* nextBtn_{nullptr};
    QPushButton* playModeBtn_{nullptr};
    QLabel* playerTimeLabel_{nullptr};
    QLabel* playerTimeTotalLabel_{nullptr};

    // ── EQ ────────────────────────────────────────────────────────────────────
    EqPanel* eqPanel_{nullptr};

    // ── Library ───────────────────────────────────────────────────────────────
    DjLibraryWidget* playerLibraryTree_{nullptr};
    QLineEdit* playerSearchBar_{nullptr};
    QComboBox* playerSortCombo_{nullptr};
    QLabel* playerLibCountLabel_{nullptr};

    // ── Audio profile widgets ─────────────────────────────────────────────────
    QComboBox* audioProfileCombo_{nullptr};
    QPushButton* refreshAudioProfilesButton_{nullptr};
    QPushButton* applyAudioProfileButton_{nullptr};
};
