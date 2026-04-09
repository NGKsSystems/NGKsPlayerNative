#pragma once

#include <QElapsedTimer>
#include <QWidget>
#include <QString>

// ── VisualizerWidget ──────────────────────────────────────────────────────────
// Lightweight animated display surface.
// Supports display modes: None, Bars, Line, Circle.
// No Q_OBJECT — uses no custom signals or slots.
// ─────────────────────────────────────────────────────────────────────────────
class VisualizerWidget : public QWidget {
public:
    enum class DisplayMode { None, Bars, Line, Circle };

    explicit VisualizerWidget(QWidget* parent = nullptr);

    void setDisplayMode(DisplayMode m);
    DisplayMode displayMode() const;

    void setPulseEnabled(bool on);
    bool pulseEnabled() const;

    void setTuneLevel(int level);
    int tuneLevel() const;

    void setAudioLevel(float level);

    void setTitleText(const QString& text);
    void setTitlePulse(float envelope);
    void setUpNextText(const QString& text);

    int barCount() const;
    void tick();

protected:
    void paintEvent(QPaintEvent*) override;

private:
    static constexpr int kSlotPx        = 3;
    static constexpr int kMinBars       = 120;
    static constexpr int kMaxBars       = 256;
    static constexpr int kParticleCount = 40;

    static QColor bandColor(float freq, float energy);

    struct Particle {
        float x;
        float y;
        float brightness;
        float drift;
        float size;
    };

    DisplayMode mode_{DisplayMode::Bars};
    bool        pulseOn_{true};
    int         tuneLevel_{2};
    float       barHeights_[256]{};
    float       peakHold_[256]{};
    float       peakAge_[256]{};
    float       phase_{0.0f};
    float       audioLevel_{0.0f};
    QString     titleText_;
    float       titlePulse_{0.0f};
    QString     upNextText_;
    Particle    particles_[40]{};
    QElapsedTimer elapsed_;
    QElapsedTimer audioActiveTimer_;
};
