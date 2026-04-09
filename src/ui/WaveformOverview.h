#pragma once

#include <QWidget>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QLinearGradient>
#include <QWheelEvent>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <thread>
#include <vector>

#include "WaveformState.h"
#include "engine/runtime/graph/DeckNode.h"

// Forward-declared in case it's not available; the stem overlay
// feature is gated on this flag at construction time.
#ifndef NGKS_ENABLE_STEM_OVERLAY
#define NGKS_ENABLE_STEM_OVERLAY true
#endif

// ═══════════════════════════════════════════════════════════════════
// WaveformOverview — polished state-aware waveform display
// ═══════════════════════════════════════════════════════════════════
class WaveformOverview : public QWidget {
public:
    explicit WaveformOverview(const QColor& accent, QWidget* parent = nullptr,
                              bool enableStemOverlay = NGKS_ENABLE_STEM_OVERLAY)
        : QWidget(parent), accent_(accent), stemOverlayFeatureEnabled_(enableStemOverlay)
    {
        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        setMinimumHeight(48);
    }

    void setBackgroundColor(const QColor& c) { bgColor_ = c; update(); }
    void setBorderVisible(bool v) { borderVisible_ = v; update(); }

    void setWaveformData(const std::vector<ngks::WaveMinMax>& data)
    {
        auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));

        if (waveformDataFrozen_) {
            std::fprintf(stderr,
                "WAVE_DATA_MUTATION_BLOCKED deck=%d frozenBins=%zu "
                "attemptedBins=%zu frozenHash=%zu tid=%zu\n",
                deckIndex_, bins_.size(), data.size(), dataHash_, tid);
            std::fflush(stderr);
            return;
        }

        bins_ = data;
        hasData_ = !bins_.empty();
        if (hasData_) {
            float absMax = 0.0f;
            for (const auto& b : bins_) {
                absMax = std::max(absMax, std::max(std::abs(b.lo), std::abs(b.hi)));
            }
            peakRef_ = (absMax > 0.0001f) ? absMax : 1.0f;

            double rmsSum = 0.0;
            for (const auto& b : bins_) {
                rmsSum += static_cast<double>(b.rms);
            }
            float trackRmsAvg = static_cast<float>(rmsSum / bins_.size());

            dataHash_ = bins_.size();
            for (size_t i = 0; i < bins_.size(); i += 64) {
                dataHash_ ^= std::hash<float>{}(bins_[i].lo) + 0x9e3779b9
                    + (dataHash_ << 6) + (dataHash_ >> 2);
                dataHash_ ^= std::hash<float>{}(bins_[i].rms) + 0x9e3779b9
                    + (dataHash_ << 6) + (dataHash_ >> 2);
            }

            waveformDataFrozen_ = true;

            std::fprintf(stderr,
                "WAVE_DATA_CREATED deck=%d bins=%zu peakRef=%.4f "
                "rmsAvg=%.4f hash=%zu FROZEN=true tid=%zu\n",
                deckIndex_, bins_.size(), peakRef_, trackRmsAvg,
                dataHash_, tid);
            std::fprintf(stderr,
                "WAVE_SCALE_MODE deck=%d mode=RMS_BODY_PEAK_TIP "
                "trackPeak=%.4f trackRmsAvg=%.4f bins=%zu tid=%zu\n",
                deckIndex_, peakRef_, trackRmsAvg,
                bins_.size(), tid);
            std::fprintf(stderr,
                "WAVE_SCALE_REFERENCE deck=%d scaleSource=TRACK_WIDE_PEAK "
                "peakRef=%.4f tid=%zu\n",
                deckIndex_, peakRef_, tid);
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
            auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::fprintf(stderr,
                "WAVE_STYLE_STATE_ENTER deck=%d old=%s new=%s tid=%zu\n",
                deckIndex_, waveViewStateName(viewState_), waveViewStateName(state), tid);
            std::fprintf(stderr,
                "WAVE_STYLE_UNIFIED deck=%d state=%s "
                "fillAlpha=140/230 visualGain=1.00 "
                "powerCurve=none barGap=standard "
                "transient=none smoothing=none tid=%zu\n",
                deckIndex_, waveViewStateName(state), tid);
            std::fflush(stderr);
            viewState_ = state;
            smoothAnchor_ = targetAnchor_;
            update();
        }
    }

    void setViewportAnchor(float anchor) {
        targetAnchor_ = std::clamp(anchor, 0.0f, 1.0f);
        if (viewState_ == WaveViewState::LIVE_SCROLL) {
            constexpr float alpha = 0.12f;
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

    void setTrackDuration(float dur) { trackDuration_ = std::max(0.1f, dur); }

    void clearWaveform()
    {
        auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        if (waveformDataFrozen_) {
            std::fprintf(stderr,
                "WAVE_DATA_UNFROZEN deck=%d bins=%zu hash=%zu "
                "reason=clearWaveform tid=%zu\n",
                deckIndex_, bins_.size(), dataHash_, tid);
            std::fflush(stderr);
        }
        bins_.clear();
        bandBins_.clear();
        hasData_ = false;
        hasBandData_ = false;
        waveformDataFrozen_ = false;
        dataHash_ = 0;
        playhead_ = 0.0f;
        trackDuration_ = 120.0f;
        liveWindowSec_ = 7.0f;
        viewState_ = WaveViewState::EMPTY;
        targetAnchor_ = 0.0f;
        smoothAnchor_ = 0.0f;
        cueFocusTarget_ = 0.0f;
        update();
    }

    void setBandEnergyData(const std::vector<ngks::BandEnergy>& data)
    {
        bandBins_ = data;
        hasBandData_ = !bandBins_.empty();
        if (hasBandData_) {
            float maxE = 0.0f;
            float maxBass = 0.0f, maxMids = 0.0f, maxHigh = 0.0f;
            for (const auto& b : bandBins_) {
                maxE = std::max(maxE, b.low + b.lowMid + b.highMid + b.high);
                maxBass = std::max(maxBass, b.low);
                maxMids = std::max(maxMids, b.lowMid + b.highMid);
                maxHigh = std::max(maxHigh, b.high);
            }
            bandPeakRef_ = (maxE > 0.0001f) ? maxE : 1.0f;
            bandMaxBass_ = (maxBass > 0.0001f) ? maxBass : 1.0f;
            bandMaxMids_ = (maxMids > 0.0001f) ? maxMids : 1.0f;
            bandMaxHigh_ = (maxHigh > 0.0001f) ? maxHigh : 1.0f;
            std::fprintf(stderr,
                "WAVE_COLOR_NORM deck=%d bins=%d "
                "bandMaxBass=%.4f bandMaxMids=%.4f bandMaxHigh=%.4f\n",
                deckIndex_, static_cast<int>(bandBins_.size()),
                bandMaxBass_, bandMaxMids_, bandMaxHigh_);
            std::fflush(stderr);
        } else {
            bandPeakRef_ = 1.0f;
            bandMaxBass_ = 1.0f;
            bandMaxMids_ = 1.0f;
            bandMaxHigh_ = 1.0f;
        }
        update();
    }

    void setStemOverlayEnabled(bool enabled)
    {
        if (stemOverlayEnabled_ != enabled) {
            stemOverlayEnabled_ = enabled;
            update();
        }
    }

    bool stemOverlayEnabled() const { return stemOverlayEnabled_; }

    void cycleDebugBandSolo()
    {
        debugBandSolo_ = (debugBandSolo_ + 1) % 4;
        static const char* kNames[] = { "ALL", "BASS", "MIDS", "HIGHS" };
        std::fprintf(stderr, "WAVE_COLOR_DEBUG_MODE deck=%d solo=%s(%d)\n",
                     deckIndex_, kNames[debugBandSolo_], debugBandSolo_);
        std::fflush(stderr);
        update();
    }
    int debugBandSolo() const { return debugBandSolo_; }

protected:
    void wheelEvent(QWheelEvent* event) override
    {
        if (viewState_ != WaveViewState::LIVE_SCROLL &&
            viewState_ != WaveViewState::CUE_FOCUS) {
            QWidget::wheelEvent(event);
            return;
        }
        const float delta = event->angleDelta().y();
        if (std::abs(delta) < 1.0f) { QWidget::wheelEvent(event); return; }
        const float factor = (delta > 0) ? 0.85f : 1.18f;
        liveWindowSec_ = std::clamp(liveWindowSec_ * factor, 2.0f, 30.0f);
        event->accept();
        update();
    }

    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing, false);
        const int w = width();
        const int h = height();

        const bool isZoomed = (viewState_ == WaveViewState::CUE_FOCUS ||
                               viewState_ == WaveViewState::LIVE_SCROLL);
        const int overviewH = isZoomed ? 8 : 0;
        const bool showBands = stemOverlayFeatureEnabled_ && stemOverlayEnabled_ && hasBandData_ && !bandBins_.empty();
        const int bandStripH = showBands ? 8 : 0;
        const int mainH = h - overviewH - bandStripH;
        const int cy = mainH / 2;

        if (bgColor_.alpha() > 0)
            p.fillRect(0, 0, w, h, bgColor_);

        if (borderVisible_) {
            p.setPen(QPen(QColor(accent_.red(), accent_.green(), accent_.blue(), 30), 1));
            p.drawRect(0, 0, w - 1, h - 1);
        }

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
        case WaveViewState::EMPTY:
            viewStart = 0.0f;
            viewEnd   = 1.0f;
            break;
        case WaveViewState::CUE_FOCUS: {
            const float zoomSpan = std::min(1.0f, liveWindowSec_ / trackDuration_);
            const float center = cueFocusTarget_;
            viewStart = center - zoomSpan * 0.5f;
            viewEnd   = center + zoomSpan * 0.5f;
            if (viewStart < 0.0f) { viewEnd -= viewStart; viewStart = 0.0f; }
            if (viewEnd > 1.0f)   { viewStart -= (viewEnd - 1.0f); viewEnd = 1.0f; }
            viewStart = std::max(0.0f, viewStart);
            viewEnd   = std::min(1.0f, viewEnd);
            break;
        }
        case WaveViewState::STATIC_PLAY:
            viewStart = 0.0f;
            viewEnd   = 1.0f;
            break;
        case WaveViewState::LIVE_SCROLL: {
            const float zoomSpan = std::min(1.0f, liveWindowSec_ / trackDuration_);
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

        // ═══ Grid ═══
        {
            const int gridCount = isZoomed ? 8 : 12;
            p.setPen(QPen(QColor(0x14, 0x18, 0x20), 1));
            for (int i = 1; i < gridCount; ++i) {
                const int gx = (w * i) / gridCount;
                p.drawLine(gx, 1, gx, mainH - 1);
            }
        }

        // Center line
        {
            QColor cl = accent_; cl.setAlpha(20);
            p.setPen(QPen(cl, 1));
            p.drawLine(1, cy, w - 2, cy);
        }

        // ═══ Waveform bars ═══
        const int numBins = static_cast<int>(bins_.size());
        const float invRef = 1.0f / peakRef_;
        const int usableW = w - 2;
        const int maxBarH = static_cast<int>((cy - 1) * 0.92f);

        const bool isLive = (viewState_ == WaveViewState::LIVE_SCROLL);
        const QColor waveBase = isLive ? QColor(0, 235, 170) : QColor(70, 165, 255);
        QColor playedColor(waveBase.red(), waveBase.green(), waveBase.blue(), 160);
        QColor aheadColor (waveBase.red(), waveBase.green(), waveBase.blue(), 220);

        const float playheadInView = (playhead_ - viewStart) / viewSpan;
        const int playheadX = 1 + static_cast<int>(std::clamp(playheadInView, 0.0f, 1.0f) * usableW);
        const bool playheadVisible = (playhead_ >= viewStart && playhead_ <= viewEnd);

        p.setPen(Qt::NoPen);

        const int binStart = std::max(0, static_cast<int>(viewStart * numBins));
        const int binEnd   = std::min(numBins, static_cast<int>(std::ceil(viewEnd * numBins)));
        const int visBins  = binEnd - binStart;
        if (visBins <= 0) return;

        constexpr float kBarWidthPx = 1.0f;
        constexpr float kBarGapPx   = 0.35f;
        constexpr float kStridePx   = kBarWidthPx + kBarGapPx;

        const bool packed = (visBins > usableW);
        const float stridePx = packed ? 1.0f : kStridePx;
        const int barW = 1;

        for (float fx = 0.0f; fx < static_cast<float>(usableW); fx += stridePx) {
            const int px = static_cast<int>(fx);
            const float frac = viewStart + (static_cast<float>(px) / usableW) * viewSpan;
            int b = static_cast<int>(frac * numBins);
            b = std::clamp(b, 0, numBins - 1);

            const int x = 1 + px;
            const auto& bin = bins_[static_cast<size_t>(b)];

            float rmsNorm = std::min(1.0f, bin.rms * invRef);
            if (b > 0 && b < numBins - 1) {
                const float prev = std::min(1.0f, bins_[static_cast<size_t>(b - 1)].rms * invRef);
                const float next = std::min(1.0f, bins_[static_cast<size_t>(b + 1)].rms * invRef);
                rmsNorm = rmsNorm * 0.92f + (prev + next) * 0.5f * 0.08f;
            }
            rmsNorm = std::pow(rmsNorm, 0.92f);
            const int rmsH = std::max(1, static_cast<int>(rmsNorm * maxBarH));

            const bool played = (x < playheadX);
            QColor barColor = played ? playedColor : aheadColor;

            // Stem color tinting
            if (showBands && hasBandData_) {
                const int bandNumBins = static_cast<int>(bandBins_.size());
                const int bi = std::clamp(static_cast<int>(frac * bandNumBins), 0, bandNumBins - 1);
                const auto& band = bandBins_[static_cast<size_t>(bi)];

                const float eBass = band.low;
                const float eMids = band.lowMid + band.highMid;
                const float eHigh = band.high;

                float nB = std::sqrt(eBass / bandMaxBass_);
                float nM = std::sqrt(eMids / bandMaxMids_);
                float nH = std::sqrt(eHigh / bandMaxHigh_);
                if (nB < 0.08f) nB = 0.0f;
                if (nM < 0.08f) nM = 0.0f;
                if (nH < 0.08f) nH = 0.0f;

                float vB = nB * 0.60f, vM = nM * 1.00f, vH = nH * 1.35f;

                if (debugBandSolo_ == 1) { vM = 0; vH = 0; }
                else if (debugBandSolo_ == 2) { vB = 0; vH = 0; }
                else if (debugBandSolo_ == 3) { vB = 0; vM = 0; }

                float b1 = vB, b2 = vM;
                int dom = 0;
                if (vM > b1) { b2 = b1; b1 = vM; dom = 1; }
                if (vH > b1) { b2 = b1; b1 = vH; dom = 2; }
                else if (vH > b2) { b2 = vH; }

                if (b1 > 0.001f) {
                    static const int kPalR[] = { 255,  80,   0 };
                    static const int kPalG[] = {  80, 220, 210 };
                    static const int kPalB[] = {  50,  90, 255 };

                    const float ratio = b1 / (b2 + 0.001f);

                    int colR, colG, colB;
                    if (ratio >= 1.65f) {
                        colR = kPalR[dom]; colG = kPalG[dom]; colB = kPalB[dom];
                    } else {
                        int sec = 0;
                        if (dom == 0) sec = (vM >= vH) ? 1 : 2;
                        else if (dom == 1) sec = (vB >= vH) ? 0 : 2;
                        else sec = (vB >= vM) ? 0 : 1;
                        colR = static_cast<int>(kPalR[dom] * 0.7f + kPalR[sec] * 0.3f);
                        colG = static_cast<int>(kPalG[dom] * 0.7f + kPalG[sec] * 0.3f);
                        colB = static_cast<int>(kPalB[dom] * 0.7f + kPalB[sec] * 0.3f);
                    }

                    const float colorStrength = 0.65f + 0.10f * rmsNorm;
                    const float satBoost = isLive ? 1.25f : 1.18f;

                    const float avg = (colR + colG + colB) / 3.0f;
                    colR = std::clamp(static_cast<int>(avg + (colR - avg) * satBoost), 0, 255);
                    colG = std::clamp(static_cast<int>(avg + (colG - avg) * satBoost), 0, 255);
                    colB = std::clamp(static_cast<int>(avg + (colB - avg) * satBoost), 0, 255);

                    const float keep = 1.0f - colorStrength;
                    barColor = QColor(
                        std::clamp(static_cast<int>(barColor.red()   * keep + colR * colorStrength), 0, 255),
                        std::clamp(static_cast<int>(barColor.green() * keep + colG * colorStrength), 0, 255),
                        std::clamp(static_cast<int>(barColor.blue()  * keep + colB * colorStrength), 0, 255),
                        barColor.alpha());
                }
            }

            const float peakHi = std::min(1.0f, std::abs(bin.hi) * invRef);
            const float peakLo = std::min(1.0f, std::abs(bin.lo) * invRef);
            const float peakMax = std::max(peakHi, peakLo);

            if (showBands && hasBandData_ && rmsH > 3) {
                const int innerH = std::max(1, rmsH / 3);
                const int outerH = rmsH - innerH;

                p.setBrush(barColor);
                p.drawRect(x, cy - rmsH, barW, outerH);
                p.drawRect(x, cy + innerH, barW, outerH);

                const int lumaI = static_cast<int>(
                    barColor.red() * 0.299f + barColor.green() * 0.587f + barColor.blue() * 0.114f);
                QColor innerColor(
                    (barColor.red()   + lumaI) / 2,
                    (barColor.green() + lumaI) / 2,
                    (barColor.blue()  + lumaI) / 2,
                    barColor.alpha());
                p.setBrush(innerColor);
                p.drawRect(x, cy - innerH, barW, innerH * 2);
            } else {
                p.setBrush(barColor);
                p.drawRect(x, cy - rmsH, barW, rmsH * 2);
            }

            if (peakMax > rmsNorm + 0.03f) {
                const int peakH = static_cast<int>(peakMax * 1.10f * maxBarH);
                QColor tipColor(230, 245, 255, played ? 170 : 240);
                p.setBrush(tipColor);
                p.drawRect(x, cy - peakH, barW, peakH - rmsH);
                p.drawRect(x, cy + rmsH, barW, peakH - rmsH);
            }
        }

        // ═══ Playhead ═══
        if (playheadVisible) {
            QColor phLine(0xff, 0xff, 0xff, 210);
            p.setPen(QPen(phLine, 2));
            p.drawLine(playheadX, 1, playheadX, mainH - 1);

            QColor glow(accent_.red(), accent_.green(), accent_.blue(), 35);
            p.setPen(Qt::NoPen);
            p.setBrush(glow);
            p.drawRect(playheadX - 2, 1, 5, mainH - 2);

            p.setBrush(QColor(0xee, 0xee, 0xee));
            QPoint tri[3] = {
                QPoint(playheadX, 1),
                QPoint(playheadX - 3, -2),
                QPoint(playheadX + 3, -2)
            };
            p.drawPolygon(tri, 3);
        }

        // ═══ State label ═══
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
                labelColor = QColor(accent_.red(), accent_.green(), accent_.blue(), 120);
                break;
            case WaveViewState::STATIC_PLAY:
                label = "STATIC";
                labelColor = QColor(accent_.red(), accent_.green(), accent_.blue(), 80);
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

        // ═══ Stem overlay mode label ═══
        if (showBands) {
            QFont sf = font();
            sf.setPointSizeF(5.0);
            sf.setBold(true);
            sf.setLetterSpacing(QFont::AbsoluteSpacing, 0.8);
            p.setFont(sf);
            p.setPen(QColor(0x99, 0x99, 0x99, 80));
            const int stw = p.fontMetrics().horizontalAdvance(QLatin1String("STEMS"));
            p.drawText(w - stw - 4, 18, QLatin1String("STEMS"));
        }

        // ═══ Band-energy strip ═══
        if (showBands) {
            const int bandY = mainH;
            const int bandNumBins = static_cast<int>(bandBins_.size());
            const float bandInvRef = 1.0f / bandPeakRef_;
            const int bandUsableW = w - 2;

            p.setPen(Qt::NoPen);
            p.setBrush(QColor(0x04, 0x06, 0x09));
            p.drawRect(1, bandY, bandUsableW, bandStripH);

            static const QColor stripBassColor (255,  80,  50, 200);
            static const QColor stripMidsColor ( 80, 220,  90, 180);
            static const QColor stripHighColor (  0, 210, 255, 170);

            for (int x = 0; x < bandUsableW; ++x) {
                const float binFrac = (static_cast<float>(x) / bandUsableW) * viewSpan + viewStart;
                const int bi = static_cast<int>(binFrac * bandNumBins);
                if (bi < 0 || bi >= bandNumBins) continue;

                const auto& band = bandBins_[static_cast<size_t>(bi)];
                const float eBass = band.low;
                const float eMids = band.lowMid + band.highMid;
                const float eHigh = band.high;
                const float totalE = (eBass + eMids + eHigh) * bandInvRef;
                if (totalE < 0.01f) continue;

                const float nB = std::sqrt(eBass / bandMaxBass_);
                const float nM = std::sqrt(eMids / bandMaxMids_);
                const float nH = std::sqrt(eHigh / bandMaxHigh_);
                const float wB = nB * 0.60f;
                const float wM = nM * 1.00f;
                const float wH = nH * 1.35f;
                const float nSum = wB + wM + wH;
                if (nSum < 0.001f) continue;
                const float bassFrac = wB / nSum;
                const float midsFrac = wM / nSum;
                (void)midsFrac;
                const float highFrac = wH / nSum;
                (void)highFrac;

                const float energyScale = std::min(1.0f, std::sqrt(totalE));
                const int totalH = std::max(1, static_cast<int>(bandStripH * energyScale));

                int bassH = std::max(0, static_cast<int>(totalH * bassFrac));
                int midsH = std::max(0, static_cast<int>(totalH * midsFrac));
                int highH = std::max(0, totalH - bassH - midsH);

                if (debugBandSolo_ == 1) { midsH = 0; highH = 0; }
                else if (debugBandSolo_ == 2) { bassH = 0; highH = 0; }
                else if (debugBandSolo_ == 3) { bassH = 0; midsH = 0; }

                int yy = bandY + bandStripH;
                if (bassH > 0) {
                    p.setBrush(stripBassColor);
                    p.drawRect(1 + x, yy - bassH, 1, bassH);
                    yy -= bassH;
                }
                if (midsH > 0) {
                    p.setBrush(stripMidsColor);
                    p.drawRect(1 + x, yy - midsH, 1, midsH);
                    yy -= midsH;
                }
                if (highH > 0) {
                    p.setBrush(stripHighColor);
                    p.drawRect(1 + x, yy - highH, 1, highH);
                }
            }

            p.setPen(QPen(QColor(0x20, 0x25, 0x30), 1));
            p.drawLine(1, bandY, w - 2, bandY);
        }

        // ═══ Overview strip ═══
        if (isZoomed && overviewH > 0) {
            const int oy = mainH + bandStripH;
            const int miniW = w - 2;

            {
                QLinearGradient fade(0, oy - 2, 0, oy);
                fade.setColorAt(0.0, QColor(0x08, 0x0a, 0x0e, 0));
                fade.setColorAt(1.0, QColor(0x04, 0x06, 0x09));
                p.setPen(Qt::NoPen);
                p.setBrush(fade);
                p.drawRect(1, oy - 2, w - 2, 2);
            }

            p.setPen(Qt::NoPen);
            p.setBrush(QColor(0x04, 0x06, 0x09));
            p.drawRect(1, oy, w - 2, overviewH);

            const int miniMax = std::max(1, (overviewH / 2));
            const int miniCy = oy + overviewH / 2;
            QColor miniC = accent_; miniC.setAlpha(140);
            p.setBrush(miniC);
            for (int x = 0; x < miniW; ++x) {
                const int bIdx = static_cast<int>(static_cast<float>(x) / miniW * numBins);
                if (bIdx < 0 || bIdx >= numBins) continue;
                const auto& mb = bins_[static_cast<size_t>(bIdx)];
                const float rmsN = std::min(1.0f, mb.rms * invRef);
                const int bh = std::max(1, static_cast<int>(rmsN * miniMax));
                p.drawRect(1 + x, miniCy - bh, 1, bh * 2);
            }

            const int vpX1 = 1 + static_cast<int>(viewStart * miniW);
            const int vpX2 = 1 + static_cast<int>(viewEnd * miniW);
            QColor vpFill(accent_.red(), accent_.green(), accent_.blue(), 18);
            p.setPen(Qt::NoPen);
            p.setBrush(vpFill);
            p.drawRect(vpX1, oy, vpX2 - vpX1, overviewH);

            QColor edge(accent_.red(), accent_.green(), accent_.blue(), 70);
            p.setPen(QPen(edge, 1));
            p.drawLine(vpX1, oy, vpX1, oy + overviewH - 1);
            p.drawLine(vpX2, oy, vpX2, oy + overviewH - 1);

            const int miniPhX = 1 + static_cast<int>(playhead_ * miniW);
            QColor miniPh(0xff, 0xff, 0xff, 140);
            p.setPen(QPen(miniPh, 1));
            p.drawLine(miniPhX, oy + 1, miniPhX, oy + overviewH - 2);
        }

        // ═══ Logging (throttled) ═══
        if (++renderLogTick_ >= 120) {
            renderLogTick_ = 0;
            auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));

            float visRmsMax = 0.0f, visPeakMax = 0.0f;
            for (int i = 0; i < visBins; ++i) {
                const int bIdx = binStart + i;
                const auto& binL = bins_[static_cast<size_t>(bIdx)];
                visRmsMax = std::max(visRmsMax, binL.rms);
                visPeakMax = std::max(visPeakMax, std::max(std::abs(binL.lo), std::abs(binL.hi)));
            }

            std::fprintf(stderr,
                "WAVE_RENDER deck=%d state=%s bins=%d visBins=%d "
                "viewRange=[%.3f,%.3f] playhead=%.4f tid=%zu\n",
                deckIndex_, waveViewStateName(viewState_),
                numBins, visBins, viewStart, viewEnd, playhead_, tid);
            std::fflush(stderr);
        }
    }

private:
    QColor accent_;
    QColor bgColor_{0x06, 0x08, 0x0c};
    bool borderVisible_{true};
    std::vector<ngks::WaveMinMax> bins_;
    float peakRef_{1.0f};
    float playhead_{0.0f};
    bool hasData_{false};
    bool waveformDataFrozen_{false};
    size_t dataHash_{0};

    std::vector<ngks::BandEnergy> bandBins_;
    float bandPeakRef_{1.0f};
    float bandMaxBass_{1.0f};
    float bandMaxMids_{1.0f};
    float bandMaxHigh_{1.0f};
    bool hasBandData_{false};
    bool stemOverlayEnabled_{false};
    bool stemOverlayFeatureEnabled_{true};
    int debugBandSolo_{0};

    WaveViewState viewState_{WaveViewState::EMPTY};
    float targetAnchor_{0.0f};
    float smoothAnchor_{0.0f};
    float cueFocusTarget_{0.0f};
    float trackDuration_{120.0f};
    float liveWindowSec_{7.0f};
    int deckIndex_{0};
    int renderLogTick_{0};
};
