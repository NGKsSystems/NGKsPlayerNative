#pragma once

#include <QWidget>
#include <QLabel>
#include <QPushButton>
#include <QSlider>
#include <QString>
#include <QPainter>
#include <QLinearGradient>
#include <algorithm>
#include <cmath>

#include "WaveformState.h"

class EngineBridge;
class EqPanel;

// ═══════════════════════════════════════════════════════════════════
// LevelMeter — tall vertical bar with green→yellow→red gradient,
// peak hold line, clip indicator, segmented LED look.
// ═══════════════════════════════════════════════════════════════════
class LevelMeter : public QWidget {
public:
    explicit LevelMeter(const QColor& accent, QWidget* parent = nullptr)
        : QWidget(parent), accent_(accent)
    {
        setSizePolicy(QSizePolicy::Fixed, QSizePolicy::Expanding);
        setFixedWidth(18);
        setMinimumHeight(120);
    }

    void setLevel(float linear) {
        level_ = std::clamp(linear, 0.0f, 1.2f);
        if (level_ >= peakHold_) {
            peakHold_ = level_;
            holdCount_ = kHoldFrames;
        } else if (holdCount_ > 0) {
            --holdCount_;
        } else {
            peakHold_ *= kDecay;
            if (peakHold_ < 0.002f) peakHold_ = 0.0f;
        }
        update();
    }

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing, false);
        const int w = width();
        const int h = height();

        // Dark background with subtle border
        p.fillRect(0, 0, w, h, QColor(0x05, 0x05, 0x08));
        p.setPen(QColor(0x28, 0x28, 0x28));
        p.drawRect(0, 0, w - 1, h - 1);

        // LED segment gaps — 3px segments with 1px gap
        constexpr int segH = 3;
        constexpr int gapH = 1;
        constexpr int segStep = segH + gapH;

        const float clamped = std::min(level_, 1.0f);
        const int fillH = static_cast<int>(clamped * static_cast<float>(h));

        if (fillH > 0) {
            // Draw as discrete LED segments
            for (int y = h - 1; y >= h - fillH && y >= 1; y -= segStep) {
                const float frac = 1.0f - static_cast<float>(y) / static_cast<float>(h);
                QColor seg;
                if (frac < 0.55f)
                    seg = QColor(0x00, 0xcc, 0x33);       // green
                else if (frac < 0.75f)
                    seg = QColor(0x88, 0xcc, 0x00);       // yellow-green
                else if (frac < 0.85f)
                    seg = QColor(0xdd, 0xbb, 0x00);       // yellow
                else if (frac < 0.92f)
                    seg = QColor(0xff, 0x77, 0x00);       // orange
                else
                    seg = QColor(0xff, 0x22, 0x00);       // red

                p.setPen(Qt::NoPen);
                p.setBrush(seg);
                const int segTop = std::max(1, y - segH + 1);
                p.drawRect(2, segTop, w - 4, y - segTop + 1);
            }
        }

        // Clip indicator at top
        if (level_ > 1.0f)
            p.fillRect(2, 1, w - 4, 4, QColor(0xff, 0x00, 0x00));

        // Peak hold line
        if (peakHold_ > 0.01f) {
            const int peakY = h - static_cast<int>(std::min(peakHold_, 1.0f) * static_cast<float>(h));
            QColor holdColor = accent_;
            holdColor.setAlpha(240);
            p.setPen(QPen(holdColor, 2));
            p.drawLine(2, peakY, w - 3, peakY);
        }

        // Bottom glow
        QLinearGradient glow(0, h, 0, h - 14);
        QColor g = accent_;
        g.setAlpha(35);
        glow.setColorAt(0.0, g);
        glow.setColorAt(1.0, QColor(0, 0, 0, 0));
        p.setPen(Qt::NoPen);
        p.setBrush(glow);
        p.drawRect(0, h - 14, w, 14);
    }

private:
    static constexpr int kHoldFrames = 62;
    static constexpr float kDecay = 0.94f;
    float level_{0.0f};
    float peakHold_{0.0f};
    int holdCount_{0};
    QColor accent_;
};

/// Vertical channel-strip widget for DJ mixer.
/// Signal flow: Transport → Seek/Time → Meters → EQ → Volume Fader
class DeckStrip : public QWidget {
    Q_OBJECT
public:
    /// @param deckIndex  0=A, 1=B, 2=C, 3=D
    /// @param accentHex  CSS color for deck identity (e.g. "#e07020" orange)
    explicit DeckStrip(int deckIndex, const QString& accentHex,
                       EngineBridge* bridge, QWidget* parent = nullptr);

    int deckIndex() const { return deckIndex_; }

    /// Load a track file into this deck.
    void loadTrack(const QString& filePath);

    /// Set track metadata from library (title, artist, bpm, key).
    void setTrackMetadata(const QString& title, const QString& artist,
                          const QString& bpm, const QString& key);

public slots:
    /// Called on each DJ snapshot poll to refresh meters, playhead, labels.
    void refreshFromSnapshot();

signals:
    /// Emitted when Load button is clicked (so DJ page can open picker).
    void loadRequested(int deckIndex);

private:
    void buildUi();
    void wireSignals();

    static QString formatTime(double seconds);

    int deckIndex_{0};
    QString accent_;
    EngineBridge* bridge_{nullptr};

    // Widgets — signal flow order
    QFrame* displayPanel_{nullptr};
    QLabel* deckLabel_{nullptr};
    QLabel* statusLabel_{nullptr};
    QLabel* trackLabel_{nullptr};
    QLabel* trackTitleLabel_{nullptr};
    QLabel* trackArtistLabel_{nullptr};
    QLabel* infoBpmLabel_{nullptr};
    QLabel* infoKeyLabel_{nullptr};
    QLabel* infoDurationLabel_{nullptr};
    QWidget* waveformOverview_{nullptr};
    QLabel* elapsedLabel_{nullptr};
    QLabel* remainLabel_{nullptr};
    QPushButton* loadBtn_{nullptr};
    QPushButton* playBtn_{nullptr};
    QPushButton* pauseBtn_{nullptr};
    QPushButton* stopBtn_{nullptr};
    QPushButton* cueBtn_{nullptr};
    QPushButton* syncBtn_{nullptr};
    QPushButton* loopBtn_{nullptr};
    QPushButton* hotCueBtn_{nullptr};
    QPushButton* hotCue1Btn_{nullptr};
    QPushButton* hotCue2Btn_{nullptr};
    QPushButton* hotCue3Btn_{nullptr};
    QPushButton* hotCue4Btn_{nullptr};
    QPushButton* loopInBtn_{nullptr};
    QPushButton* loopOutBtn_{nullptr};
    QPushButton* reloopBtn_{nullptr};
    QLabel* loopSizeLabel_{nullptr};
    QPushButton* muteBtn_{nullptr};
    QPushButton* cueMonBtn_{nullptr};
    QSlider* seekSlider_{nullptr};
    QLabel* timeLabel_{nullptr};
    LevelMeter* meterL_{nullptr};
    LevelMeter* meterR_{nullptr};
    EqPanel* eqPanel_{nullptr};
    QSlider* volumeFader_{nullptr};
    QLabel* volumeDbLabel_{nullptr};

    bool seekDragging_{false};
    bool trackLoaded_{false};
    bool muted_{false};
    bool cueMonActive_{false};
    bool waveformFetchPending_{false};
    bool waveformFullyDecoded_{false};
    int  waveformFetchPolls_{0};
    QString waveformTrackPath_;  // path of track whose waveform is currently displayed

    // BPM track identity binding — reject stale BPM from prior loads
    QString bpmTrackPath_;  // path of track whose BPM is currently displayed

    // Stored metadata from library
    QString metaTitle_;
    QString metaArtist_;
    QString metaBpm_;
    QString metaKey_;

    // Waveform state controller
    WaveformStateController waveformCtrl_;
    QPushButton* waveModeBtn_{nullptr};
    bool prevPlaying_{false};  // edge detection for play/pause/stop transitions
};
