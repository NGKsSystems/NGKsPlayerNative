#include "ui/DeckStrip.h"
#include "ui/EngineBridge.h"
#include "ui/EqPanel.h"

#include <QFont>
#include <QFrame>
#include <QHBoxLayout>
#include <QLinearGradient>
#include <QPainter>
#include <QPen>
#include <QSignalBlocker>
#include <QSizePolicy>
#include <QVBoxLayout>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <vector>

static const char* kDeckNames[] = { "A", "B", "C", "D" };

// ═══════════════════════════════════════════════════════════════════
// FaderScale — painted dB tick marks alongside the volume fader
// ═══════════════════════════════════════════════════════════════════
class FaderScale : public QWidget {
public:
    explicit FaderScale(const QColor& accent, int sliderMin, int sliderMax,
                        QWidget* parent = nullptr)
        : QWidget(parent), accent_(accent), slMin_(sliderMin), slMax_(sliderMax)
    {
        setSizePolicy(QSizePolicy::Fixed, QSizePolicy::Expanding);
        setFixedWidth(28);
        setMinimumHeight(50);
    }

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);
        const int w = width();
        const int h = height();

        const int margin = 5;
        const int grooveH = h - 2 * margin;
        if (grooveH < 10) return;

        struct Mark { const char* text; int sliderVal; bool unity; };
        Mark marks[] = {
            { "+6",   200, false },
            {  "0",   100, true  },
            { "-6",    50, false },
            { "-12",   25, false },
            { "-\xe2\x88\x9e",  0, false },
        };

        QFont f = font();
        f.setPointSizeF(5.0);
        p.setFont(f);

        for (const auto& m : marks) {
            if (m.sliderVal < slMin_ || m.sliderVal > slMax_) continue;
            const float frac = static_cast<float>(m.sliderVal - slMin_)
                             / static_cast<float>(slMax_ - slMin_);
            const int y = margin + static_cast<int>((1.0f - frac) * grooveH);

            if (m.unity) {
                QColor glow = accent_;
                glow.setAlpha(140);
                p.setPen(QPen(glow, 1.5));
                p.drawLine(w - 7, y, w, y);
                glow.setAlpha(30);
                p.fillRect(w - 7, y - 2, 7, 5, glow);
                p.setPen(accent_);
            } else {
                p.setPen(QPen(QColor(0x44, 0x44, 0x44), 1.0));
                p.drawLine(w - 4, y, w, y);
                p.setPen(QColor(0x55, 0x55, 0x55));
            }

            QRect tr(0, y - 5, w - 8, 10);
            p.drawText(tr, Qt::AlignRight | Qt::AlignVCenter,
                        QString::fromUtf8(m.text));
        }
    }

private:
    QColor accent_;
    int slMin_, slMax_;
};

// ═══════════════════════════════════════════════════════════════════
// FaderRail — painted background track for the volume fader
// ═══════════════════════════════════════════════════════════════════
class FaderRail : public QWidget {
public:
    explicit FaderRail(const QColor& accent, QWidget* parent = nullptr)
        : QWidget(parent), accent_(accent)
    {
        setSizePolicy(QSizePolicy::Fixed, QSizePolicy::Expanding);
        setFixedWidth(24);
        setMinimumHeight(50);
    }

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);
        const int w = width();
        const int h = height();
        const int cx = w / 2;

        // Rail groove — dark recessed channel
        const int railW = 6;
        const int rx = cx - railW / 2;
        p.setPen(Qt::NoPen);
        p.setBrush(QColor(0x06, 0x06, 0x06));
        p.drawRoundedRect(rx, 2, railW, h - 4, 2, 2);
        // Inner shadow
        p.setPen(QPen(QColor(0x00, 0x00, 0x00, 80), 1.0));
        p.drawLine(rx, 3, rx, h - 3);

        // Bottom base cap
        QColor baseColor = accent_;
        baseColor.setAlpha(60);
        p.setPen(Qt::NoPen);
        p.setBrush(baseColor);
        p.drawRoundedRect(cx - 5, h - 5, 10, 4, 1, 1);
    }

private:
    QColor accent_;
};

// ═══════════════════════════════════════════════════════════════════
// WaveformOverview — polished state-aware waveform display
// ═══════════════════════════════════════════════════════════════════
class WaveformOverview : public QWidget {
public:
    explicit WaveformOverview(const QColor& accent, QWidget* parent = nullptr)
        : QWidget(parent), accent_(accent)
    {
        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        setMinimumHeight(48);
    }

    void setWaveformData(const std::vector<ngks::WaveMinMax>& data)
    {
        bins_ = data;
        hasData_ = !bins_.empty();
        if (hasData_) {
            // Track-wide peak reference — anchored to the entire track,
            // never to a visible window. This is the ONLY normalization.
            float absMax = 0.0f;
            for (const auto& b : bins_) {
                absMax = std::max(absMax, std::max(std::abs(b.lo), std::abs(b.hi)));
            }
            peakRef_ = (absMax > 0.0001f) ? absMax : 1.0f;

            // Track-wide RMS reference for logging/diagnostics
            double rmsSum = 0.0;
            for (const auto& b : bins_) {
                rmsSum += static_cast<double>(b.rms);
            }
            float trackRmsAvg = static_cast<float>(rmsSum / bins_.size());

            auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
            std::fprintf(stderr,
                "WAVE_SCALE_MODE deck=%d mode=RMS_BODY_PEAK_TIP "
                "trackPeak=%.4f trackRmsAvg=%.4f bins=%zu tid=%zu\n",
                deckIndex_, peakRef_, trackRmsAvg,
                bins_.size(), static_cast<size_t>(tid));
            std::fprintf(stderr,
                "WAVE_SCALE_REFERENCE deck=%d scaleSource=TRACK_WIDE_PEAK "
                "peakRef=%.4f tid=%zu\n",
                deckIndex_, peakRef_, static_cast<size_t>(tid));
            std::fflush(stderr);
        } else {
            peakRef_ = 1.0f;
        }
        update();
    }

    void setPlayheadFraction(float frac)
    {
        frac = std::clamp(frac, 0.0f, 1.0f);
        if (std::abs(frac - playhead_) > 0.0005f) {
            playhead_ = frac;
            update();
        }
    }

    void setViewState(WaveViewState state) {
        if (state != viewState_) {
            viewState_ = state;
            smoothAnchor_ = targetAnchor_;
            update();
        }
    }

    void setViewportAnchor(float anchor) {
        targetAnchor_ = std::clamp(anchor, 0.0f, 1.0f);
        if (viewState_ == WaveViewState::LIVE_SCROLL) {
            constexpr float alpha = 0.18f;
            smoothAnchor_ += (targetAnchor_ - smoothAnchor_) * alpha;
        } else {
            smoothAnchor_ = targetAnchor_;
        }
        if (viewState_ == WaveViewState::LIVE_SCROLL ||
            viewState_ == WaveViewState::CUE_FOCUS)
            update();
    }

    void setCueFocusTarget(float target) {
        cueFocusTarget_ = std::clamp(target, 0.0f, 1.0f);
    }

    void setDeckIndex(int idx) { deckIndex_ = idx; }

    void clearWaveform()
    {
        bins_.clear();
        hasData_ = false;
        playhead_ = 0.0f;
        viewState_ = WaveViewState::EMPTY;
        targetAnchor_ = 0.0f;
        smoothAnchor_ = 0.0f;
        cueFocusTarget_ = 0.0f;
        update();
    }

protected:
    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing, false);
        const int w = width();
        const int h = height();

        // ── Layout: seamless overview strip at bottom when zoomed ──
        const bool isZoomed = (viewState_ == WaveViewState::CUE_FOCUS ||
                               viewState_ == WaveViewState::LIVE_SCROLL);
        const int overviewH = isZoomed ? 6 : 0;
        const int mainH = h - overviewH;
        const int cy = mainH / 2;

        // Background — very dark with subtle warmth
        p.fillRect(0, 0, w, h, QColor(0x08, 0x0a, 0x0e));

        // Subtle border — LIVE uses muted teal tint, others use accent
        if (viewState_ == WaveViewState::LIVE_SCROLL) {
            p.setPen(QPen(QColor(0x20, 0x60, 0x48, 50), 1));
        } else {
            p.setPen(QPen(QColor(accent_.red(), accent_.green(), accent_.blue(), 30), 1));
        }
        p.drawRect(0, 0, w - 1, h - 1);

        if (!hasData_ || bins_.empty()) {
            QFont f = font();
            f.setPointSizeF(6.5);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 2.0);
            p.setFont(f);
            QColor tc = accent_; tc.setAlpha(30);
            p.setPen(tc);
            p.drawText(QRect(0, 0, w, mainH), Qt::AlignCenter, QStringLiteral("NO WAVEFORM"));
            return;
        }

        // ═══ Viewport window ═══
        float viewStart = 0.0f;
        float viewEnd   = 1.0f;

        switch (viewState_) {
        case WaveViewState::OVERVIEW:
        case WaveViewState::STATIC_PLAY:
        case WaveViewState::EMPTY:
            viewStart = 0.0f;
            viewEnd   = 1.0f;
            break;
        case WaveViewState::CUE_FOCUS: {
            constexpr float zoomSpan = 0.12f;
            const float center = cueFocusTarget_;
            viewStart = center - zoomSpan * 0.5f;
            viewEnd   = center + zoomSpan * 0.5f;
            if (viewStart < 0.0f) { viewEnd -= viewStart; viewStart = 0.0f; }
            if (viewEnd > 1.0f)   { viewStart -= (viewEnd - 1.0f); viewEnd = 1.0f; }
            viewStart = std::max(0.0f, viewStart);
            viewEnd   = std::min(1.0f, viewEnd);
            break;
        }
        case WaveViewState::LIVE_SCROLL: {
            constexpr float zoomSpan = 0.25f;
            constexpr float phScreenFrac = 0.30f;
            const float center = smoothAnchor_ + zoomSpan * (0.5f - phScreenFrac);
            viewStart = center - zoomSpan * 0.5f;
            viewEnd   = center + zoomSpan * 0.5f;
            if (viewStart < 0.0f) { viewEnd -= viewStart; viewStart = 0.0f; }
            if (viewEnd > 1.0f)   { viewStart -= (viewEnd - 1.0f); viewEnd = 1.0f; }
            viewStart = std::max(0.0f, viewStart);
            viewEnd   = std::min(1.0f, viewEnd);
            break;
        }
        }

        const float viewSpan = viewEnd - viewStart;
        if (viewSpan < 0.001f) return;

        // ═══ Grid — subtle, fewer lines ═══
        {
            const int gridCount = isZoomed ? 8 : 12;
            p.setPen(QPen(QColor(0x14, 0x18, 0x20), 1));
            for (int i = 1; i < gridCount; ++i) {
                const int gx = (w * i) / gridCount;
                p.drawLine(gx, 1, gx, mainH - 1);
            }
        }

        // Center line — very faint
        {
            QColor cl = accent_; cl.setAlpha(20);
            p.setPen(QPen(cl, 1));
            p.drawLine(1, cy, w - 2, cy);
        }

        // ═══ Waveform bars ═══
        const int numBins = static_cast<int>(bins_.size());
        const float invRef = 1.0f / peakRef_;
        const int usableW = w - 2;
        const int maxBarH = cy - 1;

        // Color palette — LIVE uses desaturated teal, not neon green
        QColor playedColor, aheadColor;
        switch (viewState_) {
        case WaveViewState::LIVE_SCROLL:
            // Muted teal: distinct but not aggressive
            playedColor = QColor(0x18, 0x55, 0x40, 70);
            aheadColor  = QColor(0x30, 0x99, 0x78, 180);
            break;
        case WaveViewState::CUE_FOCUS:
            playedColor = QColor(accent_.red(), accent_.green(), accent_.blue(), 140);
            aheadColor  = QColor(accent_.red(), accent_.green(), accent_.blue(), 230);
            break;
        default:
            playedColor = QColor(accent_.red(), accent_.green(), accent_.blue(), 90);
            aheadColor  = QColor(accent_.red(), accent_.green(), accent_.blue(), 185);
            break;
        }

        const float playheadInView = (playhead_ - viewStart) / viewSpan;
        const int playheadX = 1 + static_cast<int>(std::clamp(playheadInView, 0.0f, 1.0f) * usableW);
        const bool playheadVisible = (playhead_ >= viewStart && playhead_ <= viewEnd);

        p.setPen(Qt::NoPen);

        const int binStart = std::max(0, static_cast<int>(viewStart * numBins));
        const int binEnd   = std::min(numBins, static_cast<int>(std::ceil(viewEnd * numBins)));
        const int visBins  = binEnd - binStart;
        if (visBins <= 0) return;

        // ═══ BAR DRAWING LOOP — RMS body + peak tips for honest dynamics ═══
        // RMS envelope shows actual energy per bucket (loudness truth).
        // Peak tips show transient reach. This is how pro DJ waveforms work:
        // at overview zoom (~4K samples/bin), raw min/max always hits ±peak
        // because every slice contains transients. RMS preserves dynamics.
        for (int i = 0; i < visBins; ++i) {
            const int b = binStart + i;
            const float binFrac = static_cast<float>(b) / numBins;
            const float relPos = (binFrac - viewStart) / viewSpan;
            const int x  = 1 + static_cast<int>(relPos * usableW);
            const float relPosNext = (static_cast<float>(b + 1) / numBins - viewStart) / viewSpan;
            const int x2 = 1 + static_cast<int>(relPosNext * usableW);
            const int barW = std::max(1, x2 - x - (visBins > usableW ? 0 : 1));

            const auto& bin = bins_[static_cast<size_t>(b)];

            // RMS-based bar height — honest energy envelope
            const float rmsNorm = std::min(1.0f, bin.rms * invRef);
            const int rmsH = std::max(1, static_cast<int>(rmsNorm * maxBarH));

            // Draw RMS body (symmetric around center — energy envelope)
            p.setBrush(x < playheadX ? playedColor : aheadColor);
            p.drawRect(x, cy - rmsH, barW, rmsH * 2);

            // Peak tips — thin 1px lines at true min/max extent
            // Only draw if peak extends meaningfully beyond RMS
            const float peakHi = std::min(1.0f, std::abs(bin.hi) * invRef);
            const float peakLo = std::min(1.0f, std::abs(bin.lo) * invRef);
            const float peakMax = std::max(peakHi, peakLo);
            if (peakMax > rmsNorm + 0.03f) {
                const int peakH = static_cast<int>(peakMax * maxBarH);
                QColor tipColor = (x < playheadX ? playedColor : aheadColor);
                tipColor.setAlpha(tipColor.alpha() / 3);
                p.setBrush(tipColor);
                // Top tip
                p.drawRect(x, cy - peakH, barW, peakH - rmsH);
                // Bottom tip
                p.drawRect(x, cy + rmsH, barW, peakH - rmsH);
            }
        }

        // ═══ Playhead — clean single line, subtle glow ═══
        if (playheadVisible) {
            QColor phLine = (viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x55, 0xdd, 0xaa, 220)   // soft teal
                : QColor(0xff, 0xff, 0xff, 210);
            p.setPen(QPen(phLine, 2));
            p.drawLine(playheadX, 1, playheadX, mainH - 1);

            // Narrow glow — not a wide slab
            QColor glow = (viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x55, 0xdd, 0xaa, 30)
                : QColor(accent_.red(), accent_.green(), accent_.blue(), 35);
            p.setPen(Qt::NoPen);
            p.setBrush(glow);
            p.drawRect(playheadX - 2, 1, 5, mainH - 2);

            // Small top notch
            p.setBrush((viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x55, 0xdd, 0xaa) : QColor(0xee, 0xee, 0xee));
            QPoint tri[3] = {
                QPoint(playheadX, 1),
                QPoint(playheadX - 3, -2),
                QPoint(playheadX + 3, -2)
            };
            p.drawPolygon(tri, 3);
        }

        // ═══ State label — small, translucent, top-right ═══
        {
            QFont f = font();
            f.setPointSizeF(5.5);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
            p.setFont(f);

            const char* label = nullptr;
            QColor labelColor;
            switch (viewState_) {
            case WaveViewState::CUE_FOCUS:
                label = "CUE";
                labelColor = QColor(accent_.red(), accent_.green(), accent_.blue(), 120);
                break;
            case WaveViewState::LIVE_SCROLL:
                label = "LIVE";
                labelColor = QColor(0x50, 0xaa, 0x80, 120);
                break;
            case WaveViewState::STATIC_PLAY:
                label = "STATIC";
                labelColor = QColor(0x88, 0x88, 0x88, 70);
                break;
            default:
                break;
            }
            if (label) {
                p.setPen(labelColor);
                const int tw = p.fontMetrics().horizontalAdvance(QLatin1String(label));
                p.drawText(w - tw - 4, 9, QLatin1String(label));
            }
        }

        // ═══ Overview strip — flush at bottom, no gap, blended ═══
        if (isZoomed && overviewH > 0) {
            const int oy = mainH;
            const int miniW = w - 2;

            // Gradient fade from main area into strip — no hard edge
            {
                QLinearGradient fade(0, oy - 2, 0, oy);
                fade.setColorAt(0.0, QColor(0x08, 0x0a, 0x0e, 0));
                fade.setColorAt(1.0, QColor(0x04, 0x06, 0x09));
                p.setPen(Qt::NoPen);
                p.setBrush(fade);
                p.drawRect(1, oy - 2, w - 2, 2);
            }

            // Strip background
            p.setPen(Qt::NoPen);
            p.setBrush(QColor(0x04, 0x06, 0x09));
            p.drawRect(1, oy, w - 2, overviewH);

            // Miniature waveform — RMS energy envelope for honest dynamics
            const int miniMax = std::max(1, (overviewH / 2));
            const int miniCy = oy + overviewH / 2;
            QColor miniC = accent_; miniC.setAlpha(50);
            p.setBrush(miniC);
            for (int x = 0; x < miniW; ++x) {
                const int b = static_cast<int>(static_cast<float>(x) / miniW * numBins);
                if (b < 0 || b >= numBins) continue;
                const auto& mb = bins_[static_cast<size_t>(b)];
                const float rmsN = std::min(1.0f, mb.rms * invRef);
                const int bh = std::max(1, static_cast<int>(rmsN * miniMax));
                p.drawRect(1 + x, miniCy - bh, 1, bh * 2);
            }

            // Viewport window — soft highlight, no hard border
            const int vpX1 = 1 + static_cast<int>(viewStart * miniW);
            const int vpX2 = 1 + static_cast<int>(viewEnd * miniW);
            QColor vpFill = (viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x40, 0x90, 0x70, 20)
                : QColor(accent_.red(), accent_.green(), accent_.blue(), 18);
            p.setPen(Qt::NoPen);
            p.setBrush(vpFill);
            p.drawRect(vpX1, oy, vpX2 - vpX1, overviewH);

            // Thin edge lines instead of box border
            QColor edge = (viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x50, 0xaa, 0x80, 80)
                : QColor(accent_.red(), accent_.green(), accent_.blue(), 70);
            p.setPen(QPen(edge, 1));
            p.drawLine(vpX1, oy, vpX1, oy + overviewH - 1);
            p.drawLine(vpX2, oy, vpX2, oy + overviewH - 1);

            // Playhead dot on strip
            const int miniPhX = 1 + static_cast<int>(playhead_ * miniW);
            QColor miniPh = (viewState_ == WaveViewState::LIVE_SCROLL)
                ? QColor(0x55, 0xdd, 0xaa, 160) : QColor(0xff, 0xff, 0xff, 140);
            p.setPen(QPen(miniPh, 1));
            p.drawLine(miniPhX, oy + 1, miniPhX, oy + overviewH - 2);
        }

        // ═══ Logging ═══
        if (++renderLogTick_ >= 120) {
            renderLogTick_ = 0;
            auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            const char* scaleMode = "rms_body_peak_tip";

            // Compute visible-region stats for diagnostics
            float visRmsMax = 0.0f, visPeakMax = 0.0f;
            for (int i = 0; i < visBins; ++i) {
                const int b = binStart + i;
                const auto& bin = bins_[static_cast<size_t>(b)];
                visRmsMax = std::max(visRmsMax, bin.rms);
                visPeakMax = std::max(visPeakMax, std::max(std::abs(bin.lo), std::abs(bin.hi)));
            }

            std::fprintf(stderr,
                "WAVE_SCALE_TRACK_PEAK deck=%d peakRef=%.4f tid=%zu\n",
                deckIndex_, peakRef_, tid);
            std::fprintf(stderr,
                "WAVE_SCALE_VISIBLE_PEAK deck=%d visRmsMax=%.4f visPeakMax=%.4f "
                "invRef=%.4f viewRange=[%.3f,%.3f] tid=%zu\n",
                deckIndex_, visRmsMax, visPeakMax, invRef, viewStart, viewEnd, tid);
            std::fprintf(stderr,
                "WAVE_SCALE_RENDER_HEIGHT deck=%d maxBarH=%d cy=%d mainH=%d "
                "renderHeight=%d tid=%zu\n",
                deckIndex_, maxBarH, cy, mainH, h, tid);
            std::fprintf(stderr,
                "WAVE_RENDER_RESOLUTION deck=%d visBins=%d usableW=%d "
                "barWidth=%s bucketCount=%d scale=%s tid=%zu\n",
                deckIndex_, visBins, usableW,
                visBins > usableW ? "1px" : "multi",
                numBins, scaleMode, tid);

            switch (viewState_) {
            case WaveViewState::CUE_FOCUS:
                std::fprintf(stderr,
                    "WAVE_VIEW_CUE_FOCUS deck=%d state=%s range=[%.3f,%.3f] "
                    "anchor=%.4f cueTarget=%.4f scale=%s tid=%zu\n",
                    deckIndex_, waveViewStateName(viewState_), viewStart, viewEnd,
                    smoothAnchor_, cueFocusTarget_, scaleMode, tid);
                break;
            case WaveViewState::LIVE_SCROLL:
                std::fprintf(stderr,
                    "WAVE_VIEW_LIVE deck=%d state=%s range=[%.3f,%.3f] "
                    "anchor=%.4f smoothAnchor=%.4f scale=%s tid=%zu\n",
                    deckIndex_, waveViewStateName(viewState_), viewStart, viewEnd,
                    targetAnchor_, smoothAnchor_, scaleMode, tid);
                std::fprintf(stderr,
                    "WAVE_SCROLL_UPDATE deck=%d playhead=%.4f phX=%d smooth=%.4f "
                    "target=%.4f delta=%.6f tid=%zu\n",
                    deckIndex_, playhead_, playheadX, smoothAnchor_,
                    targetAnchor_, targetAnchor_ - smoothAnchor_, tid);
                std::fprintf(stderr,
                    "WAVE_PLAYHEAD_ANCHOR deck=%d phFrac=%.4f anchorFrac=%.4f "
                    "phScreenX=%d viewW=%d state=%s tid=%zu\n",
                    deckIndex_, playhead_, smoothAnchor_, playheadX, w,
                    waveViewStateName(viewState_), tid);
                break;
            case WaveViewState::STATIC_PLAY:
                std::fprintf(stderr,
                    "WAVE_VIEW_STATIC deck=%d state=%s range=[%.3f,%.3f] "
                    "playhead=%.4f phX=%d scale=%s tid=%zu\n",
                    deckIndex_, waveViewStateName(viewState_), viewStart, viewEnd,
                    playhead_, playheadX, scaleMode, tid);
                break;
            case WaveViewState::OVERVIEW:
                std::fprintf(stderr,
                    "WAVE_VIEW_OVERVIEW deck=%d state=%s range=[%.3f,%.3f] "
                    "playhead=%.4f scale=%s tid=%zu\n",
                    deckIndex_, waveViewStateName(viewState_), viewStart, viewEnd,
                    playhead_, scaleMode, tid);
                break;
            default:
                break;
            }
            std::fflush(stderr);
        }
    }

private:
    QColor accent_;
    std::vector<ngks::WaveMinMax> bins_;
    float peakRef_{1.0f};
    float playhead_{0.0f};
    bool hasData_{false};

    WaveViewState viewState_{WaveViewState::EMPTY};
    float targetAnchor_{0.0f};
    float smoothAnchor_{0.0f};
    float cueFocusTarget_{0.0f};
    int deckIndex_{0};
    int renderLogTick_{0};
};

// ═══════════════════════════════════════════════════════════════════
// DeckStrip implementation
// ═══════════════════════════════════════════════════════════════════

DeckStrip::DeckStrip(int deckIndex, const QString& accentHex,
                     EngineBridge* bridge, QWidget* parent)
    : QWidget(parent), deckIndex_(deckIndex), accent_(accentHex), bridge_(bridge),
      waveformCtrl_(deckIndex)
{
    buildUi();
    wireSignals();
}

void DeckStrip::buildUi()
{
    const QColor accentColor(accent_);
    const QString borderColor = accent_ + QStringLiteral("44");

    // Accent-blended guide color (~12% blend for more visible flow line)
    const QColor base(0x0d, 0x11, 0x17);
    QColor guideColor;
    guideColor.setRed((base.red() * 88 + accentColor.red() * 12) / 100);
    guideColor.setGreen((base.green() * 88 + accentColor.green() * 12) / 100);
    guideColor.setBlue((base.blue() * 88 + accentColor.blue() * 12) / 100);
    const QString guideHex = guideColor.name();

    // Separator style — slightly more visible
    const QString flowSepStyle = QStringLiteral("background: rgba(%1,%2,%3,22);")
        .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

    // Shared small button style
    const QString btnSecondary = QStringLiteral(
        "QPushButton {"
        "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #181c24, stop:1 #0e1018);"
        "  border: 1px solid rgba(%1,%2,%3,60); border-radius: 3px;"
        "  color: #999; padding: 1px 4px;"
        "  min-height: 24px; }"
        "QPushButton:hover {"
        "  background: rgba(%1,%2,%3,30); color: #ccc;"
        "  border: 1px solid rgba(%1,%2,%3,140); }"
        "QPushButton:pressed {"
        "  background: #060810; color: #ddd; }")
        .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

    const QString btnPrimary = QStringLiteral(
        "QPushButton {"
        "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #1c2030, stop:1 #0e1018);"
        "  border: 1px solid rgba(%1,%2,%3,100); border-radius: 4px;"
        "  color: #e0e0e0; padding: 2px 6px;"
        "  min-height: 30px; font-weight: bold; }"
        "QPushButton:hover {"
        "  background: rgba(%1,%2,%3,50); color: #fff;"
        "  border: 1px solid rgba(%1,%2,%3,200); }"
        "QPushButton:pressed {"
        "  background: #060810; color: #fff; }")
        .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

    // Small compact button style for hot cues / loop
    const QString btnSmall = QStringLiteral(
        "QPushButton {"
        "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
        "  border-radius: 2px; color: #777; padding: 1px 2px;"
        "  min-height: 20px; min-width: 28px; }"
        "QPushButton:hover {"
        "  background: rgba(%1,%2,%3,25); color: #ccc;"
        "  border: 1px solid rgba(%1,%2,%3,120); }"
        "QPushButton:pressed { background: #060810; color: #ddd; }")
        .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

    const QString sectionLabelStyle = QStringLiteral(
        "color: %1; background: transparent; border: none;"
        " font-size: 6pt; font-weight: bold; letter-spacing: 1px;").arg(accent_);

    auto* outerVBox = new QVBoxLayout(this);
    outerVBox->setContentsMargins(0, 0, 0, 0);
    outerVBox->setSpacing(0);

    auto* outerFrame = new QFrame(this);
    outerFrame->setObjectName(QStringLiteral("deckFrame%1").arg(deckIndex_));
    outerFrame->setStyleSheet(QStringLiteral(
        "QFrame#deckFrame%1 {"
        "  background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0,"
        "    stop:0 #0c0f14, stop:0.46 #0c0f14,"
        "    stop:0.5 %2, stop:0.54 #0c0f14, stop:1 #0c0f14);"
        "  border: 1px solid %3; border-radius: 5px; }")
        .arg(deckIndex_).arg(guideHex, borderColor));
    outerVBox->addWidget(outerFrame);

    auto* mainLayout = new QVBoxLayout(outerFrame);
    mainLayout->setContentsMargins(5, 2, 5, 3);
    mainLayout->setSpacing(1);

    auto addFlowSep = [&]() {
        auto* sep = new QFrame(outerFrame);
        sep->setFixedHeight(1);
        sep->setStyleSheet(flowSepStyle);
        mainLayout->addWidget(sep);
    };

    // ═══ SECTION 1: DECK HEADER (Identity + Status) ═══
    {
        displayPanel_ = new QFrame(outerFrame);
        displayPanel_->setObjectName(QStringLiteral("deckDisplay%1").arg(deckIndex_));
        displayPanel_->setStyleSheet(QStringLiteral(
            "QFrame#deckDisplay%1 {"
            "  background: #070a0f; border: 1px solid %2;"
            "  border-radius: 3px; }")
            .arg(deckIndex_).arg(borderColor));

        auto* displayLayout = new QVBoxLayout(displayPanel_);
        displayLayout->setContentsMargins(6, 3, 6, 3);
        displayLayout->setSpacing(1);

        // Header row: DECK A ... STOPPED
        auto* headerRow = new QHBoxLayout();
        headerRow->setSpacing(4);

        const char* dn = (deckIndex_ >= 0 && deckIndex_ < 4) ? kDeckNames[deckIndex_] : "?";
        deckLabel_ = new QLabel(QStringLiteral("DECK %1").arg(QLatin1StringView(dn)), displayPanel_);
        {
            QFont f = deckLabel_->font();
            f.setPointSize(12);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 2.0);
            deckLabel_->setFont(f);
        }
        deckLabel_->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; border: none;").arg(accent_));
        headerRow->addWidget(deckLabel_);
        headerRow->addStretch();

        statusLabel_ = new QLabel(QStringLiteral("EMPTY"), displayPanel_);
        {
            QFont f = statusLabel_->font();
            f.setPointSizeF(7.0);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
            statusLabel_->setFont(f);
        }
        statusLabel_->setStyleSheet(QStringLiteral(
            "color: #555; background: transparent; border: none;"));
        statusLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);
        headerRow->addWidget(statusLabel_);
        displayLayout->addLayout(headerRow);

        mainLayout->addWidget(displayPanel_);
    }

    // ═══ SECTION 2: TRACK DETAIL BLOCK ═══
    {
        auto* trackBlock = new QFrame(outerFrame);
        trackBlock->setObjectName(QStringLiteral("trackBlock%1").arg(deckIndex_));
        trackBlock->setStyleSheet(QStringLiteral(
            "QFrame#trackBlock%1 { background: #080b10; border: none; }").arg(deckIndex_));

        auto* trackLayout = new QVBoxLayout(trackBlock);
        trackLayout->setContentsMargins(6, 2, 6, 2);
        trackLayout->setSpacing(0);

        // Track title
        trackTitleLabel_ = new QLabel(QStringLiteral("\u2014 NO TRACK \u2014"), outerFrame);
        {
            QFont f = trackTitleLabel_->font();
            f.setPointSizeF(9.5);
            f.setBold(true);
            trackTitleLabel_->setFont(f);
        }
        trackTitleLabel_->setStyleSheet(QStringLiteral(
            "color: #444; background: transparent; border: none;"));
        trackTitleLabel_->setAlignment(Qt::AlignCenter);
        trackTitleLabel_->setWordWrap(false);
        trackTitleLabel_->setFixedHeight(20);
        trackLayout->addWidget(trackTitleLabel_);

        // Artist
        trackArtistLabel_ = new QLabel(QString(), outerFrame);
        {
            QFont f = trackArtistLabel_->font();
            f.setPointSizeF(7.5);
            trackArtistLabel_->setFont(f);
        }
        trackArtistLabel_->setStyleSheet(QStringLiteral(
            "color: #666; background: transparent; border: none;"));
        trackArtistLabel_->setAlignment(Qt::AlignCenter);
        trackArtistLabel_->setFixedHeight(14);
        trackLayout->addWidget(trackArtistLabel_);

        // BPM | Key | Duration row
        auto* infoRow = new QHBoxLayout();
        infoRow->setSpacing(6);
        infoRow->setContentsMargins(0, 1, 0, 0);

        auto makeInfoLabel = [&](const QString& text) {
            auto* lbl = new QLabel(text, outerFrame);
            QFont f = lbl->font();
            f.setPointSizeF(7.0);
            f.setBold(true);
            lbl->setFont(f);
            lbl->setStyleSheet(QStringLiteral(
                "color: #555; background: transparent; border: none;"));
            lbl->setAlignment(Qt::AlignCenter);
            return lbl;
        };

        infoRow->addStretch();
        auto* bpmTag = makeInfoLabel(QStringLiteral("BPM"));
        bpmTag->setStyleSheet(QStringLiteral(
            "color: #444; background: transparent; border: none; font-size: 5pt;"));
        infoRow->addWidget(bpmTag);
        infoBpmLabel_ = makeInfoLabel(QStringLiteral("---"));
        infoRow->addWidget(infoBpmLabel_);

        auto* sep1 = makeInfoLabel(QStringLiteral("\u2502"));
        sep1->setStyleSheet(QStringLiteral("color: #333; background: transparent; border: none;"));
        infoRow->addWidget(sep1);

        auto* keyTag = makeInfoLabel(QStringLiteral("KEY"));
        keyTag->setStyleSheet(QStringLiteral(
            "color: #444; background: transparent; border: none; font-size: 5pt;"));
        infoRow->addWidget(keyTag);
        infoKeyLabel_ = makeInfoLabel(QStringLiteral("---"));
        infoRow->addWidget(infoKeyLabel_);

        auto* sep2 = makeInfoLabel(QStringLiteral("\u2502"));
        sep2->setStyleSheet(QStringLiteral("color: #333; background: transparent; border: none;"));
        infoRow->addWidget(sep2);

        infoDurationLabel_ = makeInfoLabel(QStringLiteral("--:--"));
        infoRow->addWidget(infoDurationLabel_);
        infoRow->addStretch();

        trackLayout->addLayout(infoRow);
        mainLayout->addWidget(trackBlock);

        // Keep legacy trackLabel_ for compatibility (hidden)
        trackLabel_ = new QLabel(QString(), outerFrame);
        trackLabel_->hide();
        // Keep legacy timeLabel_ (hidden — replaced by elapsed/remain)
        timeLabel_ = new QLabel(QString(), outerFrame);
        timeLabel_->hide();
    }

    addFlowSep();

    // ═══ SECTION 3: WAVEFORM OVERVIEW + SEEK ═══
    {
        auto* waveSection = new QVBoxLayout();
        waveSection->setSpacing(0);
        waveSection->setContentsMargins(0, 0, 0, 0);

        waveformOverview_ = new WaveformOverview(accentColor, outerFrame);
        static_cast<WaveformOverview*>(waveformOverview_)->setDeckIndex(deckIndex_);
        waveformOverview_->setMinimumHeight(48);
        waveformOverview_->setMaximumHeight(80);
        waveSection->addWidget(waveformOverview_, 1);

        // Seek slider — slim, integrated
        seekSlider_ = new QSlider(Qt::Horizontal, outerFrame);
        seekSlider_->setRange(0, 10000);
        seekSlider_->setValue(0);
        seekSlider_->setFixedHeight(14);
        seekSlider_->setCursor(Qt::PointingHandCursor);
        seekSlider_->setStyleSheet(QStringLiteral(
            "QSlider::groove:horizontal {"
            "  background: #060810; height: 10px; border-radius: 1px;"
            "  border: 1px solid rgba(%1,%2,%3,20); }"
            "QSlider::handle:horizontal {"
            "  background: #cccccc; width: 2px; height: 14px;"
            "  margin: -2px 0; border: none; }"
            "QSlider::sub-page:horizontal {"
            "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1,"
            "    stop:0 rgba(%1,%2,%3,30), stop:0.3 rgba(%1,%2,%3,70),"
            "    stop:0.7 rgba(%1,%2,%3,70), stop:1 rgba(%1,%2,%3,30));"
            "  border-radius: 1px; border: none; }"
            "QSlider::add-page:horizontal {"
            "  background: transparent; border-radius: 1px; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        waveSection->addWidget(seekSlider_);

        mainLayout->addLayout(waveSection, 1);
    }

    // ═══ SECTION 3b: TIME DISPLAY (Elapsed / Remaining) ═══
    {
        auto* timeRow = new QHBoxLayout();
        timeRow->setSpacing(0);
        timeRow->setContentsMargins(4, 0, 4, 0);

        elapsedLabel_ = new QLabel(QStringLiteral("0:00"), outerFrame);
        {
            QFont f = elapsedLabel_->font();
            f.setPointSizeF(9.0);
            f.setBold(true);
            elapsedLabel_->setFont(f);
        }
        elapsedLabel_->setStyleSheet(QStringLiteral(
            "color: #333; background: transparent; border: none;"));
        elapsedLabel_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        elapsedLabel_->setFixedHeight(18);
        timeRow->addWidget(elapsedLabel_);

        timeRow->addStretch();

        remainLabel_ = new QLabel(QStringLiteral("-0:00"), outerFrame);
        {
            QFont f = remainLabel_->font();
            f.setPointSizeF(9.0);
            f.setBold(true);
            remainLabel_->setFont(f);
        }
        remainLabel_->setStyleSheet(QStringLiteral(
            "color: #333; background: transparent; border: none;"));
        remainLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);
        remainLabel_->setFixedHeight(18);
        timeRow->addWidget(remainLabel_);

        mainLayout->addLayout(timeRow);
    }

    // ═══ SECTION 3c: WAVEFORM MODE TOGGLE (LIVE / STATIC) ═══
    {
        waveModeBtn_ = new QPushButton(QStringLiteral("STATIC"), outerFrame);
        waveModeBtn_->setCheckable(true);
        waveModeBtn_->setChecked(false);  // unchecked = STATIC, checked = LIVE
        waveModeBtn_->setFixedHeight(20);
        waveModeBtn_->setCursor(Qt::PointingHandCursor);
        waveModeBtn_->setToolTip(QStringLiteral("Waveform mode: STATIC (playhead moves) / LIVE (waveform scrolls)"));
        {
            QFont f = waveModeBtn_->font();
            f.setPointSizeF(7.0);
            f.setBold(true);
            waveModeBtn_->setFont(f);
        }
        waveModeBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
            "  border-radius: 2px; color: #888; padding: 1px 8px; }"
            "QPushButton:checked {"
            "  background: rgba(%1,%2,%3,40); border: 1px solid rgba(%1,%2,%3,140);"
            "  color: #ddd; }"
            "QPushButton:hover {"
            "  border: 1px solid rgba(%1,%2,%3,100); color: #bbb; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        mainLayout->addWidget(waveModeBtn_);
    }

    addFlowSep();

    // ═══ SECTION 4: TRANSPORT — Grouped layout ═══
    {
        auto* transportPanel = new QFrame(outerFrame);
        transportPanel->setObjectName(QStringLiteral("deckTransport%1").arg(deckIndex_));
        transportPanel->setStyleSheet(QStringLiteral(
            "QFrame#deckTransport%1 {"
            "  background: #0a0d14; border: none;"
            "  border-radius: 2px; }")
            .arg(deckIndex_));

        auto* transportRow = new QHBoxLayout(transportPanel);
        transportRow->setSpacing(2);
        transportRow->setContentsMargins(3, 2, 3, 2);

        // LEFT: LOAD | SKIP BACK
        loadBtn_ = new QPushButton(QStringLiteral("LOAD"), outerFrame);
        loadBtn_->setStyleSheet(btnSecondary);
        loadBtn_->setCursor(Qt::PointingHandCursor);
        loadBtn_->setToolTip(QStringLiteral("Load track into deck"));
        { QFont f = loadBtn_->font(); f.setPointSizeF(7.5); f.setBold(true); loadBtn_->setFont(f); }
        transportRow->addWidget(loadBtn_);

        syncBtn_ = new QPushButton(QStringLiteral("\u23EA"), outerFrame);
        syncBtn_->setStyleSheet(btnSecondary);
        syncBtn_->setCursor(Qt::PointingHandCursor);
        syncBtn_->setToolTip(QStringLiteral("Skip back 30 seconds"));
        { QFont f = syncBtn_->font(); f.setPointSizeF(9.0); syncBtn_->setFont(f); }
        transportRow->addWidget(syncBtn_);

        transportRow->addSpacing(4);

        // CENTER: PLAY | CUE
        playBtn_ = new QPushButton(QStringLiteral("PLAY"), outerFrame);
        playBtn_->setStyleSheet(btnPrimary);
        playBtn_->setCursor(Qt::PointingHandCursor);
        playBtn_->setToolTip(QStringLiteral("Play / Pause"));
        { QFont f = playBtn_->font(); f.setPointSizeF(10.0); f.setBold(true); playBtn_->setFont(f); }
        transportRow->addWidget(playBtn_, 1);

        cueBtn_ = new QPushButton(QStringLiteral("CUE"), outerFrame);
        cueBtn_->setCheckable(true);
        cueBtn_->setChecked(false);
        cueBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #1c2030, stop:1 #0e1018);"
            "  border: 1px solid rgba(%1,%2,%3,100); border-radius: 4px;"
            "  color: #e0e0e0; padding: 2px 6px;"
            "  min-height: 30px; font-weight: bold; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,50); color: #fff;"
            "  border: 1px solid rgba(%1,%2,%3,200); }"
            "QPushButton:checked {"
            "  background: rgba(%1,%2,%3,35); border: 1px solid %4;"
            "  color: %4; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue())
            .arg(accent_));
        cueBtn_->setCursor(Qt::PointingHandCursor);
        cueBtn_->setToolTip(QStringLiteral("CUE \u2014 monitor deck in headphones"));
        { QFont f = cueBtn_->font(); f.setPointSizeF(9.0); f.setBold(true); cueBtn_->setFont(f); }
        transportRow->addWidget(cueBtn_, 1);

        transportRow->addSpacing(4);

        // RIGHT: SKIP FWD
        hotCueBtn_ = new QPushButton(QStringLiteral("\u23E9"), outerFrame);
        hotCueBtn_->setStyleSheet(btnSecondary);
        hotCueBtn_->setCursor(Qt::PointingHandCursor);
        hotCueBtn_->setToolTip(QStringLiteral("Skip forward 30 seconds"));
        { QFont f = hotCueBtn_->font(); f.setPointSizeF(9.0); hotCueBtn_->setFont(f); }
        transportRow->addWidget(hotCueBtn_);

        // Hidden: pause + stop + loop kept for API compatibility
        pauseBtn_ = new QPushButton(outerFrame); pauseBtn_->hide();
        stopBtn_ = new QPushButton(outerFrame); stopBtn_->hide();
        loopBtn_ = new QPushButton(outerFrame); loopBtn_->hide();

        mainLayout->addWidget(transportPanel);
    }

    addFlowSep();

    // ═══ SECTION 5: HOT CUE ROW ═══
    {
        auto* hotCueRow = new QHBoxLayout();
        hotCueRow->setSpacing(2);
        hotCueRow->setContentsMargins(3, 1, 3, 1);

        auto* hotCueLbl = new QLabel(QStringLiteral("HOT CUE"), outerFrame);
        hotCueLbl->setStyleSheet(sectionLabelStyle);
        hotCueLbl->setFixedWidth(40);
        hotCueLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        hotCueRow->addWidget(hotCueLbl);

        hotCueRow->addSpacing(2);

        auto makeHotCue = [&](const QString& text) -> QPushButton* {
            auto* btn = new QPushButton(text, outerFrame);
            btn->setStyleSheet(btnSmall);
            btn->setCursor(Qt::PointingHandCursor);
            QFont f = btn->font(); f.setPointSizeF(7.0); f.setBold(true); btn->setFont(f);
            return btn;
        };

        hotCue1Btn_ = makeHotCue(QStringLiteral("1"));
        hotCue2Btn_ = makeHotCue(QStringLiteral("2"));
        hotCue3Btn_ = makeHotCue(QStringLiteral("3"));
        hotCue4Btn_ = makeHotCue(QStringLiteral("4"));

        hotCueRow->addWidget(hotCue1Btn_, 1);
        hotCueRow->addWidget(hotCue2Btn_, 1);
        hotCueRow->addWidget(hotCue3Btn_, 1);
        hotCueRow->addWidget(hotCue4Btn_, 1);

        mainLayout->addLayout(hotCueRow);
    }

    // ═══ SECTION 6: LOOP ROW ═══
    {
        auto* loopRow = new QHBoxLayout();
        loopRow->setSpacing(2);
        loopRow->setContentsMargins(3, 1, 3, 1);

        auto* loopLbl = new QLabel(QStringLiteral("LOOP"), outerFrame);
        loopLbl->setStyleSheet(sectionLabelStyle);
        loopLbl->setFixedWidth(40);
        loopLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        loopRow->addWidget(loopLbl);

        loopRow->addSpacing(2);

        auto makeLoopBtn = [&](const QString& text) -> QPushButton* {
            auto* btn = new QPushButton(text, outerFrame);
            btn->setStyleSheet(btnSmall);
            btn->setCursor(Qt::PointingHandCursor);
            QFont f = btn->font(); f.setPointSizeF(6.5); f.setBold(true); btn->setFont(f);
            return btn;
        };

        loopInBtn_ = makeLoopBtn(QStringLiteral("IN"));
        loopOutBtn_ = makeLoopBtn(QStringLiteral("OUT"));
        reloopBtn_ = makeLoopBtn(QStringLiteral("RELOOP"));

        loopRow->addWidget(loopInBtn_, 1);
        loopRow->addWidget(loopOutBtn_, 1);
        loopRow->addWidget(reloopBtn_, 1);

        loopSizeLabel_ = new QLabel(QStringLiteral("4 BEAT"), outerFrame);
        {
            QFont f = loopSizeLabel_->font(); f.setPointSizeF(6.0); f.setBold(true);
            loopSizeLabel_->setFont(f);
        }
        loopSizeLabel_->setStyleSheet(QStringLiteral(
            "color: #555; background: #0a0c12; border: 1px solid rgba(%1,%2,%3,30);"
            " border-radius: 2px; padding: 1px 4px;")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        loopSizeLabel_->setAlignment(Qt::AlignCenter);
        loopSizeLabel_->setFixedWidth(42);
        loopRow->addWidget(loopSizeLabel_);

        mainLayout->addLayout(loopRow);
    }

    addFlowSep();

    // ═══ SECTION 7: MUTE — utility toggle ═══
    {
        auto* utilRow = new QHBoxLayout();
        utilRow->setSpacing(3);
        utilRow->setContentsMargins(2, 0, 2, 0);

        cueMonBtn_ = nullptr;

        muteBtn_ = new QPushButton(QStringLiteral("MUTE"), outerFrame);
        muteBtn_->setCheckable(true);
        muteBtn_->setChecked(false);
        muteBtn_->setCursor(Qt::PointingHandCursor);
        muteBtn_->setToolTip(QStringLiteral("Mute deck output to master"));
        { QFont f = muteBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); muteBtn_->setFont(f); }
        muteBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid #333; border-radius: 3px;"
            "  color: #888; padding: 1px 6px; min-height: 22px; }"
            "QPushButton:hover { border: 1px solid #555; color: #bbb; }"
            "QPushButton:checked {"
            "  background: rgba(255,40,40,30); border: 1px solid #cc3333;"
            "  color: #ff4444; }"));
        utilRow->addWidget(muteBtn_, 1);

        mainLayout->addLayout(utilRow);
    }

    addFlowSep();

    // ═══ SECTION 8: LEVEL METERS (L/R) ═══
    {
        auto* meterCluster = new QHBoxLayout();
        meterCluster->setSpacing(4);
        meterCluster->setContentsMargins(0, 0, 0, 0);

        auto* lblL = new QLabel(QStringLiteral("L"), outerFrame);
        lblL->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; font-size: 6pt; font-weight: bold;").arg(accent_));
        lblL->setAlignment(Qt::AlignBottom | Qt::AlignHCenter);
        lblL->setFixedWidth(10);
        meterCluster->addWidget(lblL);

        meterL_ = new LevelMeter(accentColor, outerFrame);
        meterCluster->addWidget(meterL_);

        meterCluster->addSpacing(2);

        meterR_ = new LevelMeter(accentColor, outerFrame);
        meterCluster->addWidget(meterR_);

        auto* lblR = new QLabel(QStringLiteral("R"), outerFrame);
        lblR->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; font-size: 6pt; font-weight: bold;").arg(accent_));
        lblR->setAlignment(Qt::AlignBottom | Qt::AlignHCenter);
        lblR->setFixedWidth(10);
        meterCluster->addWidget(lblR);

        auto* meterOuter = new QHBoxLayout();
        meterOuter->addStretch(1);
        meterOuter->addLayout(meterCluster);
        meterOuter->addStretch(1);

        auto* meterBox = new QWidget(outerFrame);
        meterBox->setMinimumHeight(100);
        meterBox->setMaximumHeight(180);
        meterBox->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);
        meterBox->setLayout(meterOuter);
        meterBox->setStyleSheet(QStringLiteral("background: transparent;"));
        mainLayout->addWidget(meterBox, 2);
    }

    addFlowSep();

    // ═══ SECTION 9: EQ (INTEGRATED — tight) ═══
    {
        eqPanel_ = new EqPanel(bridge_, outerFrame);
        eqPanel_->setDeckIndex(deckIndex_);
        eqPanel_->setAccentColor(accent_);
        eqPanel_->setCollapsed(true);
        eqPanel_->setContentsMargins(0, 0, 0, 0);
        mainLayout->addWidget(eqPanel_);
    }

    addFlowSep();

    // ═══ SECTION 10: VOLUME FADER (anchored with rail + scale) ═══
    {
        auto* faderSection = new QHBoxLayout();
        faderSection->setSpacing(0);
        faderSection->setContentsMargins(0, 0, 0, 0);

        auto* scale = new FaderScale(accentColor, 0, 200, outerFrame);

        auto* faderStack = new QVBoxLayout();
        faderStack->setSpacing(0);
        faderStack->setContentsMargins(0, 0, 0, 0);

        volumeFader_ = new QSlider(Qt::Vertical, outerFrame);
        volumeFader_->setRange(0, 200);
        volumeFader_->setValue(100);
        volumeFader_->setMinimumHeight(60);
        volumeFader_->setMaximumHeight(110);
        volumeFader_->setFixedWidth(24);
        volumeFader_->setStyleSheet(QStringLiteral(
            "QSlider::groove:vertical {"
            "  background: qlineargradient(x1:0,x2:1, stop:0 #080808, stop:0.4 #141414,"
            "    stop:0.5 #1a1a1a, stop:0.6 #141414, stop:1 #080808);"
            "  width: 6px; border-radius: 3px;"
            "  border: 1px solid #222; }"
            "QSlider::handle:vertical {"
            "  background: qlineargradient(x1:0,x2:1, stop:0 #e8e8e8, stop:0.5 #ffffff, stop:1 #e8e8e8);"
            "  border: 1px solid %1; width: 20px; height: 10px;"
            "  margin: 0 -7px; border-radius: 2px; }"
            "QSlider::sub-page:vertical { background: #0a0a0a; border-radius: 3px; }"
            "QSlider::add-page:vertical {"
            "  background: qlineargradient(x1:0,x2:0, y1:1,y2:0, stop:0 %1, stop:1 rgba(%2,%3,%4,80));"
            "  border-radius: 3px; }")
            .arg(accent_)
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));

        faderStack->addWidget(volumeFader_, 1, Qt::AlignHCenter);

        volumeDbLabel_ = new QLabel(QStringLiteral("0.0 dB"), outerFrame);
        volumeDbLabel_->setAlignment(Qt::AlignCenter);
        volumeDbLabel_->setFixedWidth(42);
        volumeDbLabel_->setFixedHeight(12);
        {
            QFont f = volumeDbLabel_->font();
            f.setPointSizeF(6.0);
            f.setBold(true);
            volumeDbLabel_->setFont(f);
        }
        volumeDbLabel_->setStyleSheet(QStringLiteral("color: #bbb; background: transparent;"));
        faderStack->addWidget(volumeDbLabel_, 0, Qt::AlignHCenter);

        faderSection->addStretch(1);
        faderSection->addWidget(scale);
        faderSection->addLayout(faderStack);
        faderSection->addStretch(1);

        mainLayout->addLayout(faderSection, 1);
    }
}

void DeckStrip::wireSignals()
{
    connect(loadBtn_, &QPushButton::clicked, this, [this]() {
        emit loadRequested(deckIndex_);
    });
    connect(playBtn_, &QPushButton::clicked, this, [this]() {
        if (bridge_->deckIsPlaying(deckIndex_))
            bridge_->pauseDeck(deckIndex_);
        else
            bridge_->playDeck(deckIndex_);
    });
    connect(pauseBtn_, &QPushButton::clicked, this, [this]() {
        bridge_->pauseDeck(deckIndex_);
    });
    connect(stopBtn_, &QPushButton::clicked, this, [this]() {
        bridge_->stopDeck(deckIndex_);
    });
    // CUE = PFL toggle (matches Electron CUE behavior) + snap waveform to cue
    connect(cueBtn_, &QPushButton::clicked, this, [this](bool checked) {
        bridge_->setDeckCueMonitor(deckIndex_, checked);
        waveformCtrl_.onCuePressed();   // snap waveform to cue focus
    });
    // Skip back / forward 30 seconds
    connect(syncBtn_, &QPushButton::clicked, this, [this]() {
        const double pos = bridge_->deckPlayhead(deckIndex_);
        bridge_->seekDeck(deckIndex_, std::max(0.0, pos - 30.0));
    });
    connect(hotCueBtn_, &QPushButton::clicked, this, [this]() {
        const double pos = bridge_->deckPlayhead(deckIndex_);
        const double dur = bridge_->deckDuration(deckIndex_);
        bridge_->seekDeck(deckIndex_, std::min(dur, pos + 30.0));
    });
    // LOOP — placeholder (no-op for now)
    connect(loopBtn_, &QPushButton::clicked, this, []() {});

    // Hot Cue 1–4 — placeholders
    connect(hotCue1Btn_, &QPushButton::clicked, this, []() {});
    connect(hotCue2Btn_, &QPushButton::clicked, this, []() {});
    connect(hotCue3Btn_, &QPushButton::clicked, this, []() {});
    connect(hotCue4Btn_, &QPushButton::clicked, this, []() {});

    // Loop IN/OUT/RELOOP — placeholders
    connect(loopInBtn_, &QPushButton::clicked, this, []() {});
    connect(loopOutBtn_, &QPushButton::clicked, this, []() {});
    connect(reloopBtn_, &QPushButton::clicked, this, []() {});

    // MUTE toggle
    connect(muteBtn_, &QPushButton::clicked, this, [this](bool checked) {
        bridge_->setDeckMute(deckIndex_, checked);
    });

    connect(volumeFader_, &QSlider::valueChanged, this, [this](int value) {
        const double linear = static_cast<double>(value) / 100.0;
        bridge_->setDeckGain(deckIndex_, linear);
        if (value == 0) {
            volumeDbLabel_->setText(QStringLiteral("-\u221E dB"));
        } else {
            const double db = 20.0 * std::log10(linear);
            const QString sign = (db >= 0.05) ? QStringLiteral("+") : QString();
            volumeDbLabel_->setText(QStringLiteral("%1%2 dB").arg(sign).arg(db, 0, 'f', 1));
        }
    });

    connect(seekSlider_, &QSlider::sliderPressed, this, [this]() {
        seekDragging_ = true;
    });
    connect(seekSlider_, &QSlider::sliderReleased, this, [this]() {
        seekDragging_ = false;
        const double dur = bridge_->deckDuration(deckIndex_);
        if (dur > 0.0) {
            const double frac = static_cast<double>(seekSlider_->value()) / 10000.0;
            bridge_->seekDeck(deckIndex_, frac * dur);
        }
    });

    // Waveform mode toggle: STATIC ↔ LIVE
    connect(waveModeBtn_, &QPushButton::toggled, this, [this](bool checked) {
        if (checked) {
            waveformCtrl_.setUserMode(WaveUserMode::LIVE);
            waveModeBtn_->setText(QStringLiteral("LIVE"));
        } else {
            waveformCtrl_.setUserMode(WaveUserMode::STATIC);
            waveModeBtn_->setText(QStringLiteral("STATIC"));
        }
    });
}

void DeckStrip::loadTrack(const QString& filePath)
{
    bridge_->loadTrackToDeck(deckIndex_, filePath);
}

void DeckStrip::refreshFromSnapshot()
{
    const double ph = bridge_->deckPlayhead(deckIndex_);
    const double dur = bridge_->deckDuration(deckIndex_);
    const bool playing = bridge_->deckIsPlaying(deckIndex_);
    const QString label = bridge_->deckTrackLabel(deckIndex_);
    const double peakL = bridge_->deckPeakL(deckIndex_);
    const double peakR = bridge_->deckPeakR(deckIndex_);
    const QString currentPath = bridge_->deckFilePath(deckIndex_);

    // ── Track loaded state transition ──
    const bool loaded = !label.isEmpty();
    if (loaded && !trackLoaded_) {
        trackLoaded_ = true;
        waveformCtrl_.onTrackLoaded(dur);   // → CUE_FOCUS
        trackTitleLabel_->setStyleSheet(QStringLiteral(
            "color: #e8e8e8; background: transparent; border: none;"));
        elapsedLabel_->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; border: none;").arg(accent_));
        remainLabel_->setStyleSheet(QStringLiteral(
            "color: #bbb; background: transparent; border: none;"));
        infoDurationLabel_->setStyleSheet(QStringLiteral(
            "color: #999; background: transparent; border: none;"));
        const QString glowBorder = accent_ + QStringLiteral("80");
        displayPanel_->setStyleSheet(QStringLiteral(
            "QFrame#deckDisplay%1 {"
            "  background: #070a0f; border: 1px solid %2;"
            "  border-radius: 3px; }")
            .arg(deckIndex_).arg(glowBorder));
    }

    // ── Detect track change (new path loaded into this deck) ──
    if (!currentPath.isEmpty() && currentPath != waveformTrackPath_) {
        qInfo().noquote() << QStringLiteral("WAVEFORM_REQUEST deck=%1 path=%2")
            .arg(deckIndex_).arg(currentPath);
        waveformTrackPath_ = currentPath;
        waveformFetchPending_ = true;
        waveformFullyDecoded_ = false;
        waveformFetchPolls_ = 0;
        auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
        wf->clearWaveform();
        qInfo().noquote() << QStringLiteral("WAVEFORM_CACHE_KEY deck=%1 key=%2")
            .arg(deckIndex_).arg(currentPath);

        // ── BPM track-change detection ──
        const double engineBpm = bridge_->deckBpmFixed(deckIndex_);
        qInfo().noquote() << QStringLiteral("BPM_DECK_MATCH deck=%1 metaBpm=%2 engineBpm=%3 path=%4")
            .arg(deckIndex_)
            .arg(metaBpm_.isEmpty() ? QStringLiteral("unknown") : metaBpm_)
            .arg(engineBpm, 0, 'f', 1)
            .arg(currentPath);
    }

    // ── Waveform data fetch (wait for FULL decode, not just preload) ──
    if (waveformFetchPending_) {
        const bool fullyDecoded = bridge_->isDeckFullyDecoded(deckIndex_);
        const QString nowPath = bridge_->deckFilePath(deckIndex_);

        // Reject if track changed while we were waiting
        if (nowPath != waveformTrackPath_) {
            qInfo().noquote() << QStringLiteral("WAVEFORM_ATTACH_REJECT_STALE deck=%1 expected=%2 got=%3")
                .arg(deckIndex_).arg(waveformTrackPath_).arg(nowPath);
            waveformFetchPending_ = false;
        } else if (fullyDecoded) {
            auto t0 = std::chrono::steady_clock::now();
            auto wfData = bridge_->getWaveformOverview(deckIndex_, 2048);
            auto t1 = std::chrono::steady_clock::now();
            const double genMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
            if (!wfData.empty()) {
                auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
                wf->setWaveformData(wfData);
                waveformFetchPending_ = false;
                waveformFullyDecoded_ = true;
                std::fprintf(stderr,
                    "WAVE_MINMAX_BUILD_END deck=%d bins=%zu genMs=%.1f path=%s\n",
                    deckIndex_, wfData.size(), genMs,
                    waveformTrackPath_.toUtf8().constData());
                std::fflush(stderr);
                qInfo().noquote() << QStringLiteral("WAVEFORM_ATTACH deck=%1 bins=%2 path=%3")
                    .arg(deckIndex_).arg(wfData.size()).arg(waveformTrackPath_);
                qInfo().noquote() << QStringLiteral("WAVEFORM_DECK_MATCH deck=%1 path=%2")
                    .arg(deckIndex_).arg(waveformTrackPath_);
            }
        } else if (++waveformFetchPolls_ > 600) {
            // Give up after ~10 seconds (600 polls × 16ms)
            qInfo().noquote() << QStringLiteral("WAVEFORM_CACHE_MISS deck=%1 timeout path=%2")
                .arg(deckIndex_).arg(waveformTrackPath_);
            waveformFetchPending_ = false;
        }
    }

    // Update track title — use metadata if available, else engine label
    if (loaded) {
        if (!metaTitle_.isEmpty())
            trackTitleLabel_->setText(metaTitle_);
        else
            trackTitleLabel_->setText(label);

        if (!metaArtist_.isEmpty())
            trackArtistLabel_->setText(metaArtist_);
    }

    // Update info row — BPM with track identity validation
    if (!currentPath.isEmpty() && currentPath != bpmTrackPath_) {
        // Track changed since BPM was bound — reject stale BPM
        qInfo().noquote() << QStringLiteral("BPM_ATTACH_REJECT_STALE deck=%1 oldPath=%2 newPath=%3")
            .arg(deckIndex_).arg(bpmTrackPath_, currentPath);
        // setTrackMetadata should have been called for the new track;
        // if not, show unknown
        if (bpmTrackPath_.isEmpty() || bpmTrackPath_ != currentPath) {
            if (metaBpm_.isEmpty()) {
                infoBpmLabel_->setText(QStringLiteral("---"));
                infoBpmLabel_->setStyleSheet(QStringLiteral("color: #555; background: transparent; border: none;"));
            }
            bpmTrackPath_ = currentPath;
        }
    }

    if (!metaBpm_.isEmpty()) {
        infoBpmLabel_->setText(metaBpm_);
    } else if (loaded) {
        infoBpmLabel_->setText(QStringLiteral("---"));
    }

    if (!metaKey_.isEmpty())
        infoKeyLabel_->setText(metaKey_);
    else if (loaded)
        infoKeyLabel_->setText(QStringLiteral("---"));

    meterL_->setLevel(static_cast<float>(peakL));
    meterR_->setLevel(static_cast<float>(peakR));

    // ── VU source logging (periodic) ──
    {
        static int vuLogTick[4]{};
        if (++vuLogTick[deckIndex_] >= 120) { // ~every 2 seconds
            vuLogTick[deckIndex_] = 0;
            qInfo().noquote() << QStringLiteral("VU_SOURCE_LEVEL deck=%1 peakL=%2 peakR=%3 playing=%4")
                .arg(deckIndex_)
                .arg(peakL, 0, 'f', 4)
                .arg(peakR, 0, 'f', 4)
                .arg(playing ? QStringLiteral("true") : QStringLiteral("false"));
            qInfo().noquote() << QStringLiteral("VU_UI_UPDATE deck=%1 meterL_level=%2 meterR_level=%3")
                .arg(deckIndex_)
                .arg(peakL, 0, 'f', 4)
                .arg(peakR, 0, 'f', 4);
        }
    }

    // ── Time display ──
    if (loaded) {
        elapsedLabel_->setText(formatTime(ph));
        const double remain = dur - ph;
        remainLabel_->setText(QStringLiteral("-%1").arg(formatTime(remain > 0 ? remain : 0)));
        infoDurationLabel_->setText(formatTime(dur));

        // ── Playhead on waveform ──
        if (dur > 0.0) {
            auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
            wf->setPlayheadFraction(static_cast<float>(ph / dur));
            waveformCtrl_.updatePlayhead(ph, dur);

            // Push controller state into renderer
            wf->setViewState(waveformCtrl_.state());
            wf->setViewportAnchor(static_cast<float>(waveformCtrl_.viewportAnchor()));
            wf->setCueFocusTarget(static_cast<float>(waveformCtrl_.cueFocusTarget()));
        }

        // ── Waveform state: transport edge detection ──
        if (playing && !prevPlaying_) {
            waveformCtrl_.onPlay();   // → LIVE_SCROLL or STATIC_PLAY
        } else if (!playing && prevPlaying_) {
            // Distinguish stop (playhead ≈ 0) from pause
            if (ph < 0.05) {
                waveformCtrl_.onStop();   // → OVERVIEW
            } else {
                waveformCtrl_.onPause();  // hold current view
            }
        }
        prevPlaying_ = playing;
    } else {
        // ── Empty deck — clear waveform and metadata ──
        if (trackLoaded_) {
            trackLoaded_ = false;
            waveformCtrl_.onTrackUnloaded();   // → EMPTY
            auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
            wf->clearWaveform();
            waveformFetchPending_ = false;
            waveformTrackPath_.clear();
            waveformFullyDecoded_ = false;

            // ── Clear stale metadata ──
            metaBpm_.clear();
            metaKey_.clear();
            metaTitle_.clear();
            metaArtist_.clear();
            bpmTrackPath_.clear();
            infoBpmLabel_->setText(QStringLiteral("---"));
            infoBpmLabel_->setStyleSheet(QStringLiteral("color: #555; background: transparent; border: none;"));
            infoKeyLabel_->setText(QStringLiteral("---"));
            infoKeyLabel_->setStyleSheet(QStringLiteral("color: #555; background: transparent; border: none;"));
            trackTitleLabel_->setText(QStringLiteral("No Track Loaded"));
            trackTitleLabel_->setStyleSheet(QStringLiteral("color: #444; background: transparent; border: none;"));
            trackArtistLabel_->setText(QString());

            qInfo().noquote() << QStringLiteral("BPM_FALLBACK_BLOCKED deck=%1 reason=deck_unloaded")
                .arg(deckIndex_);
        }
    }

    if (!seekDragging_ && dur > 0.0) {
        const int pos = static_cast<int>((ph / dur) * 10000.0);
        QSignalBlocker blocker(seekSlider_);
        seekSlider_->setValue(std::clamp(pos, 0, 10000));
    }

    // ── CUE (PFL) / MUTE button state sync ──
    {
        const bool muted = bridge_->deckIsMuted(deckIndex_);
        const bool cueMon = bridge_->deckCueEnabled(deckIndex_);
        if (muteBtn_->isChecked() != muted) {
            QSignalBlocker b(muteBtn_);
            muteBtn_->setChecked(muted);
        }
        if (cueBtn_->isChecked() != cueMon) {
            QSignalBlocker b(cueBtn_);
            cueBtn_->setChecked(cueMon);
        }
    }

    // ── Status label ──
    if (!loaded) {
        statusLabel_->setText(QStringLiteral("EMPTY"));
        statusLabel_->setStyleSheet(QStringLiteral(
            "color: #555; background: transparent; border: none;"));
    } else if (playing) {
        statusLabel_->setText(QStringLiteral("PLAYING"));
        statusLabel_->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; border: none;").arg(accent_));
    } else {
        statusLabel_->setText(QStringLiteral("PAUSED"));
        statusLabel_->setStyleSheet(QStringLiteral(
            "color: #999; background: transparent; border: none;"));
    }

    // ── Play button — accent fill when playing, text switches ──
    const QColor ac(accent_);
    if (playing) {
        playBtn_->setText(QStringLiteral("PAUSE"));
        playBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: %1; border: 1px solid %1;"
            "  border-radius: 4px; color: #fff; padding: 2px 6px;"
            "  min-height: 30px; font-weight: bold; }"
            "QPushButton:hover { background: %1; }").arg(accent_));
    } else {
        playBtn_->setText(QStringLiteral("PLAY"));
        playBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #1c2030, stop:1 #0e1018);"
            "  border: 1px solid rgba(%1,%2,%3,100); border-radius: 4px;"
            "  color: #e0e0e0; padding: 2px 6px;"
            "  min-height: 30px; font-weight: bold; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,50); color: #fff;"
            "  border: 1px solid rgba(%1,%2,%3,200); }"
            "QPushButton:pressed {"
            "  background: #060810; color: #fff; }")
            .arg(ac.red()).arg(ac.green()).arg(ac.blue()));
    }
}

void DeckStrip::setTrackMetadata(const QString& title, const QString& artist,
                                 const QString& bpm, const QString& key)
{
    metaTitle_ = title;
    metaArtist_ = artist;
    metaBpm_ = bpm;
    metaKey_ = key;

    // Immediate UI update
    if (!title.isEmpty()) {
        trackTitleLabel_->setText(title);
        trackTitleLabel_->setStyleSheet(QStringLiteral(
            "color: #e8e8e8; background: transparent; border: none;"));
    }
    if (!artist.isEmpty())
        trackArtistLabel_->setText(artist);

    // ── BPM: always update (show "---" when unknown) ──
    if (!bpm.isEmpty()) {
        infoBpmLabel_->setText(bpm);
        infoBpmLabel_->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; border: none;").arg(accent_));
        qInfo().noquote() << QStringLiteral("BPM_SOURCE_METADATA deck=%1 bpm=%2 title=%3")
            .arg(deckIndex_).arg(bpm, title);
    } else {
        infoBpmLabel_->setText(QStringLiteral("---"));
        infoBpmLabel_->setStyleSheet(QStringLiteral(
            "color: #555; background: transparent; border: none;"));
        qInfo().noquote() << QStringLiteral("BPM_SOURCE_UNKNOWN deck=%1 title=%2")
            .arg(deckIndex_).arg(title);
    }

    // ── Key: always update (show "---" when unknown) ──
    if (!key.isEmpty()) {
        infoKeyLabel_->setText(key);
        infoKeyLabel_->setStyleSheet(QStringLiteral(
            "color: %1; background: transparent; border: none;").arg(accent_));
    } else {
        infoKeyLabel_->setText(QStringLiteral("---"));
        infoKeyLabel_->setStyleSheet(QStringLiteral(
            "color: #555; background: transparent; border: none;"));
    }

    // Bind BPM to current track path for stale rejection
    bpmTrackPath_ = bridge_ ? bridge_->deckFilePath(deckIndex_) : QString();
    qInfo().noquote() << QStringLiteral("BPM_ATTACH deck=%1 bpm=%2 path=%3")
        .arg(deckIndex_)
        .arg(bpm.isEmpty() ? QStringLiteral("unknown") : bpm)
        .arg(bpmTrackPath_);
}

QString DeckStrip::formatTime(double seconds)
{
    if (seconds < 0.0 || !std::isfinite(seconds)) return QStringLiteral("0:00");
    const int totalSec = static_cast<int>(seconds);
    const int m = totalSec / 60;
    const int s = totalSec % 60;
    return QStringLiteral("%1:%2").arg(m).arg(s, 2, 10, QLatin1Char('0'));
}
