#include "ui/widgets/VisualizerWidget.h"

#include <QColor>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QPolygonF>

#include <algorithm>
#include <cmath>

// ── Constructor ───────────────────────────────────────────────────────────────
VisualizerWidget::VisualizerWidget(QWidget* parent)
    : QWidget(parent)
{
    setMinimumHeight(120);
    setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
    setAttribute(Qt::WA_OpaquePaintEvent, true);
    elapsed_.start();
    audioActiveTimer_.start();
    for (int i = 0; i < kMaxBars; ++i) {
        barHeights_[i] = 0.0f;
        peakHold_[i]   = 0.0f;
        peakAge_[i]    = 0.0f;
    }
    for (int i = 0; i < kParticleCount; ++i) {
        particles_[i] = {
            static_cast<float>(i) / kParticleCount,
            0.3f + 0.4f * std::sin(i * 1.618f),
            0.0f,
            0.005f + 0.01f * std::sin(i * 0.73f),
            0.4f + 0.6f * static_cast<float>(i % 7) / 6.0f
        };
    }
}

// ── Accessors ─────────────────────────────────────────────────────────────────
void VisualizerWidget::setDisplayMode(DisplayMode m) { mode_ = m; update(); }
VisualizerWidget::DisplayMode VisualizerWidget::displayMode() const { return mode_; }

void VisualizerWidget::setPulseEnabled(bool on) { pulseOn_ = on; update(); }
bool VisualizerWidget::pulseEnabled() const { return pulseOn_; }

void VisualizerWidget::setTuneLevel(int level) { tuneLevel_ = qBound(0, level, 4); update(); }
int  VisualizerWidget::tuneLevel() const { return tuneLevel_; }

void VisualizerWidget::setAudioLevel(float level)
{
    audioLevel_ = qBound(0.0f, level, 1.0f);
    audioActiveTimer_.restart();
}

void VisualizerWidget::setTitleText(const QString& text) { titleText_  = text; }
void VisualizerWidget::setTitlePulse(float envelope)     { titlePulse_ = qBound(0.0f, envelope, 1.0f); }
void VisualizerWidget::setUpNextText(const QString& text) { upNextText_ = text; }

int VisualizerWidget::barCount() const { return qBound(kMinBars, width() / kSlotPx, kMaxBars); }

// ── tick ──────────────────────────────────────────────────────────────────────
void VisualizerWidget::tick()
{
    const float dt         = 0.033f;
    const float sensitivity = 0.3f + tuneLevel_ * 0.175f;
    const qint64 elapsedMs = elapsed_.elapsed();
    const bool hasAudio    = audioActiveTimer_.elapsed() < 500;

    const float rawLevel = hasAudio ? audioLevel_ : 0.0f;
    const float level    = qBound(0.0f, rawLevel * (1.0f + sensitivity), 1.0f);

    const int n    = barCount();
    const float invN = 1.0f / n;

    for (int i = 0; i < n; ++i) {
        float target;
        if (hasAudio && rawLevel > 0.001f) {
            const float freq   = i * invN;
            const float shape  = (1.0f - 0.35f * freq) * (0.85f + 0.15f * std::sin(freq * 6.28318f));
            const float jitter  = std::sin(i * 2.71828f + elapsedMs * 0.003f + i * i * 0.37f) * 0.12f;
            const float jitter2 = std::sin(i * 1.414f   + elapsedMs * 0.005f) * 0.08f;
            target = level * shape + (jitter + jitter2) * level;
            target = qBound(0.0f, target, 1.0f);
        } else {
            target = (std::sin(elapsedMs * 0.0008f * (1.0f + i * 0.04f) * sensitivity) + 1.0f)
                     * 0.5f * 0.10f;
        }
        if (target > barHeights_[i])
            barHeights_[i] += (target - barHeights_[i]) * qMin(1.0f, dt * 30.0f);
        else
            barHeights_[i] += (target - barHeights_[i]) * qMin(1.0f, dt * 3.5f);

        if (barHeights_[i] >= peakHold_[i]) {
            peakHold_[i] = barHeights_[i];
            peakAge_[i]  = 0.0f;
        } else {
            peakAge_[i] += dt;
            if (peakAge_[i] > 1.0f)
                peakHold_[i] += (0.0f - peakHold_[i]) * qMin(1.0f, dt * 2.5f);
        }
    }

    for (int i = 0; i < kParticleCount; ++i) {
        auto& pt = particles_[i];
        pt.x += pt.drift * dt * (0.5f + level * 2.0f);
        if (pt.x > 1.0f) pt.x -= 1.0f;
        if (pt.x < 0.0f) pt.x += 1.0f;

        const float targetBright = hasAudio
            ? qBound(0.0f, level * 1.2f + 0.05f * std::sin(elapsedMs * 0.002f + i * 0.5f), 1.0f)
            : 0.08f;
        if (targetBright > pt.brightness)
            pt.brightness += (targetBright - pt.brightness) * qMin(1.0f, dt * 12.0f);
        else
            pt.brightness += (targetBright - pt.brightness) * qMin(1.0f, dt * 2.0f);

        const int barIdx = qBound(0, static_cast<int>(pt.x * n), n - 1);
        const float barTip   = 1.0f - barHeights_[barIdx] * 0.75f;
        const float floatRange = 0.10f + 0.05f * std::sin(elapsedMs * 0.0015f + i * 1.3f);
        pt.y += (barTip - floatRange - pt.y) * qMin(1.0f, dt * 4.0f);
    }

    phase_ += dt * 2.0f * sensitivity;
    update();
}

// ── bandColor ─────────────────────────────────────────────────────────────────
QColor VisualizerWidget::bandColor(float freq, float energy)
{
    float r, g, b;
    if (freq < 0.22f) {
        const float s = freq / 0.22f;
        r = 0.96f; g = 0.38f + 0.10f * s; b = 0.18f + 0.08f * s;
    } else if (freq < 0.55f) {
        const float s = (freq - 0.22f) / 0.33f;
        r = 0.92f - 0.05f * s; g = 0.28f + 0.10f * s; b = 0.40f + 0.22f * s;
    } else {
        const float s = (freq - 0.55f) / 0.45f;
        r = 0.62f - 0.14f * s; g = 0.36f + 0.28f * s; b = 0.74f + 0.14f * s;
    }
    const float bright = 0.35f + 0.65f * qBound(0.0f, energy, 1.0f);
    return QColor(
        qBound(0, static_cast<int>(r * bright * 255), 255),
        qBound(0, static_cast<int>(g * bright * 255), 255),
        qBound(0, static_cast<int>(b * bright * 255), 255));
}

// ── paintEvent ────────────────────────────────────────────────────────────────
void VisualizerWidget::paintEvent(QPaintEvent*)
{
    QPainter p(this);
    const int w  = width();
    const int h  = height();
    const int n  = barCount();

    p.fillRect(rect(), QColor(0x0a, 0x0e, 0x27));
    if (mode_ == DisplayMode::None) return;

    const float pulseScale = pulseOn_
        ? 0.88f + 0.12f * std::sin(elapsed_.elapsed() * 0.004f)
        : 1.0f;

    if (mode_ == DisplayMode::Bars) {
        p.setRenderHint(QPainter::Antialiasing, false);
        p.setPen(Qt::NoPen);
        const float slotW = static_cast<float>(w) / n;
        const float gap   = qMax(1.0f, slotW * 0.30f);
        const float barW  = qBound(1.0f, slotW - gap, 3.0f);
        const float invN  = 1.0f / n;

        QRgb bandCol[kMaxBars];
        for (int i = 0; i < n; ++i)
            bandCol[i] = bandColor(i * invN, barHeights_[i]).rgb();

        // Layers 1+2: main bars + dim reflection
        for (int i = 0; i < n; ++i) {
            const float bh = barHeights_[i] * h * 0.75f * pulseScale;
            if (bh < 1.0f) continue;
            const float x = i * slotW + (slotW - barW) * 0.5f;
            p.setBrush(QColor(bandCol[i]));
            p.drawRect(QRectF(x, h - bh, barW, bh));
            const float rh = bh * 0.10f;
            if (rh >= 1.0f) {
                QColor ref(bandCol[i]); ref.setAlpha(25);
                p.setBrush(ref);
                p.drawRect(QRectF(x, h - rh * 0.35f, barW, rh * 0.35f));
            }
        }
        // Layer 3: tip glow caps
        for (int i = 0; i < n; ++i) {
            const float bh = barHeights_[i] * h * 0.75f * pulseScale;
            if (bh < 4.0f) continue;
            const float x    = i * slotW + (slotW - barW) * 0.5f;
            const float capH = qMin(2.5f, bh * 0.06f);
            QColor glow(bandCol[i]); glow.setAlpha(static_cast<int>(100 + barHeights_[i] * 155));
            p.setBrush(glow);
            p.drawRect(QRectF(x, h - bh, barW, capH));
        }
        // Layer 4: peak-hold markers
        for (int i = 0; i < n; ++i) {
            if (peakHold_[i] < 0.02f) continue;
            const float peakY = h - peakHold_[i] * h * 0.75f * pulseScale;
            const float x     = i * slotW + (slotW - barW) * 0.5f;
            const float fade  = (peakAge_[i] < 1.0f) ? 1.0f
                : qMax(0.0f, 1.0f - (peakAge_[i] - 1.0f) * 1.5f);
            QColor capCol(bandCol[i]); capCol.setAlpha(static_cast<int>(180 * fade));
            p.setBrush(capCol);
            p.drawRect(QRectF(x, peakY - 1.5f, barW, 2.0f));
        }
        // Layer 5: sparkle particles
        for (int i = 0; i < kParticleCount; ++i) {
            const auto& pt = particles_[i];
            if (pt.brightness < 0.02f) continue;
            const int barIdx = qBound(0, static_cast<int>(pt.x * n), n - 1);
            QColor c(bandCol[barIdx]); c.setAlpha(static_cast<int>(pt.brightness * 200));
            p.setBrush(c);
            const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
            p.drawEllipse(QPointF(pt.x * w, pt.y * h), sz, sz);
        }

    } else if (mode_ == DisplayMode::Line) {
        p.setRenderHint(QPainter::Antialiasing, true);
        const float step = static_cast<float>(w) / (n - 1);
        const float invN = 1.0f / n;

        QPainterPath linePath, peakPath;
        for (int i = 0; i < n; ++i) {
            const float x     = i * step;
            const float y     = h * 0.5f - (barHeights_[i] - 0.5f) * h * 0.70f * pulseScale;
            const float peakY = h * 0.5f - (peakHold_[i]  - 0.5f) * h * 0.70f * pulseScale;
            if (i == 0) { linePath.moveTo(x, y); peakPath.moveTo(x, peakY); }
            else        { linePath.lineTo(x, y); peakPath.lineTo(x, peakY); }
        }

        QLinearGradient bandGrad(0, 0, w, 0);
        bandGrad.setColorAt(0.00, bandColor(0.00f, 0.70f));
        bandGrad.setColorAt(0.22, bandColor(0.22f, 0.70f));
        bandGrad.setColorAt(0.55, bandColor(0.55f, 0.70f));
        bandGrad.setColorAt(1.00, bandColor(1.00f, 0.70f));

        // Layer 1: glow line
        {
            QLinearGradient glowGrad(0, 0, w, 0);
            QColor c0 = bandColor(0.0f, 0.5f); c0.setAlpha(50);
            QColor c1 = bandColor(0.55f, 0.5f); c1.setAlpha(50);
            QColor c2 = bandColor(1.0f, 0.5f); c2.setAlpha(50);
            glowGrad.setColorAt(0.0, c0); glowGrad.setColorAt(0.55, c1); glowGrad.setColorAt(1.0, c2);
            p.setPen(QPen(QBrush(glowGrad), 6.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
            p.setBrush(Qt::NoBrush);
            p.drawPath(linePath);
        }
        // Layer 2: main line
        p.setPen(QPen(QBrush(bandGrad), 2.5, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
        p.setBrush(Qt::NoBrush);
        p.drawPath(linePath);
        // Layer 3: area fill
        {
            QPainterPath fillPath = linePath;
            fillPath.lineTo(w, h); fillPath.lineTo(0, h); fillPath.closeSubpath();
            QLinearGradient fillGrad(0, 0, w, 0);
            QColor f0 = bandColor(0.0f, 0.4f); f0.setAlpha(35);
            QColor f1 = bandColor(0.55f, 0.4f); f1.setAlpha(35);
            QColor f2 = bandColor(1.0f, 0.4f); f2.setAlpha(35);
            fillGrad.setColorAt(0.0, f0); fillGrad.setColorAt(0.55, f1); fillGrad.setColorAt(1.0, f2);
            p.setPen(Qt::NoPen);
            p.fillPath(fillPath, QBrush(fillGrad));
        }
        // Layer 4: peak-hold trace
        {
            QLinearGradient peakGrad(0, 0, w, 0);
            QColor k0 = bandColor(0.0f, 0.9f); k0.setAlpha(90);
            QColor k1 = bandColor(0.55f, 0.9f); k1.setAlpha(90);
            QColor k2 = bandColor(1.0f, 0.9f); k2.setAlpha(90);
            peakGrad.setColorAt(0.0, k0); peakGrad.setColorAt(0.55, k1); peakGrad.setColorAt(1.0, k2);
            p.setPen(QPen(QBrush(peakGrad), 1.0, Qt::SolidLine, Qt::RoundCap));
            p.setBrush(Qt::NoBrush);
            p.drawPath(peakPath);
        }
        // Layer 5: particles
        p.setPen(Qt::NoPen);
        for (int i = 0; i < kParticleCount; ++i) {
            const auto& pt = particles_[i];
            if (pt.brightness < 0.03f) continue;
            QColor c = bandColor(pt.x, pt.brightness);
            c.setAlpha(static_cast<int>(pt.brightness * 180));
            p.setBrush(c);
            const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
            p.drawEllipse(QPointF(pt.x * w, pt.y * h), sz, sz);
        }

    } else if (mode_ == DisplayMode::Circle) {
        p.setRenderHint(QPainter::Antialiasing, true);
        const float cx       = w * 0.5f;
        const float cy       = h * 0.5f;
        const float baseR    = qMin(w, h) * 0.25f * pulseScale;
        const float invN     = 1.0f / n;
        const float angleStep = 6.28318f / n;
        const float phaseRad  = phase_ * 0.5f;
        const float phaseDeg  = phaseRad * (180.0f / 3.14159f);

        QPolygonF mainPoly, peakPoly;
        for (int i = 0; i < n; ++i) {
            const float angle = i * angleStep + phaseRad;
            const float cosA  = std::cos(angle); const float sinA = std::sin(angle);
            const float r  = baseR + barHeights_[i] * baseR * 0.8f;
            const float pr = baseR + peakHold_[i]   * baseR * 0.8f;
            mainPoly << QPointF(cx + r  * cosA, cy + r  * sinA);
            peakPoly << QPointF(cx + pr * cosA, cy + pr * sinA);
        }
        mainPoly << mainPoly.first();
        peakPoly << peakPoly.first();

        QConicalGradient cg(cx, cy, phaseDeg);
        cg.setColorAt(0.00, bandColor(0.00f, 0.70f));
        cg.setColorAt(0.22, bandColor(0.22f, 0.70f));
        cg.setColorAt(0.55, bandColor(0.55f, 0.70f));
        cg.setColorAt(1.00, bandColor(1.00f, 0.70f));

        // Layer 1: glow ring
        {
            QConicalGradient glowCg(cx, cy, phaseDeg);
            QColor c0 = bandColor(0.0f, 0.5f); c0.setAlpha(45);
            QColor c1 = bandColor(0.55f, 0.5f); c1.setAlpha(45);
            QColor c2 = bandColor(1.0f, 0.5f); c2.setAlpha(45);
            glowCg.setColorAt(0.0, c0); glowCg.setColorAt(0.55, c1); glowCg.setColorAt(1.0, c2);
            p.setPen(QPen(QBrush(glowCg), 6.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
            p.setBrush(Qt::NoBrush);
            p.drawPolygon(mainPoly);
        }
        // Layer 2: inner fill
        {
            QRadialGradient rg(cx, cy, baseR * 2.0f);
            rg.setColorAt(0.0, QColor(0x53, 0x34, 0x83, 40));
            rg.setColorAt(0.6, QColor(0x0f, 0x34, 0x60, 20));
            rg.setColorAt(1.0, QColor(0x0a, 0x0e, 0x27,  5));
            p.setPen(Qt::NoPen);
            p.setBrush(QBrush(rg));
            p.drawPolygon(mainPoly);
        }
        // Layer 3: main ring
        p.setPen(QPen(QBrush(cg), 2.0, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
        p.setBrush(Qt::NoBrush);
        p.drawPolygon(mainPoly);
        // Layer 4: peak-hold ring
        {
            QConicalGradient peakCg(cx, cy, phaseDeg);
            QColor k0 = bandColor(0.0f, 0.8f); k0.setAlpha(65);
            QColor k1 = bandColor(0.55f, 0.8f); k1.setAlpha(65);
            QColor k2 = bandColor(1.0f, 0.8f); k2.setAlpha(65);
            peakCg.setColorAt(0.0, k0); peakCg.setColorAt(0.55, k1); peakCg.setColorAt(1.0, k2);
            p.setPen(QPen(QBrush(peakCg), 1.0));
            p.setBrush(Qt::NoBrush);
            p.drawPolygon(peakPoly);
        }
        // Layer 5: orbiting particles
        p.setPen(Qt::NoPen);
        for (int i = 0; i < kParticleCount; ++i) {
            const auto& pt = particles_[i];
            if (pt.brightness < 0.03f) continue;
            const float angle = pt.x * 6.28318f + phaseRad;
            const int barIdx  = qBound(0, static_cast<int>(pt.x * n), n - 1);
            const float r     = baseR + barHeights_[barIdx] * baseR * 0.8f + 10.0f;
            QColor c = bandColor(pt.x, pt.brightness);
            c.setAlpha(static_cast<int>(pt.brightness * 180));
            p.setBrush(c);
            const float sz = pt.size * (1.0f + pt.brightness * 1.5f);
            p.drawEllipse(QPointF(cx + r * std::cos(angle), cy + r * std::sin(angle)), sz, sz);
        }
    }

    // ── Now Playing + Title pulse overlay ────────────────────────────────────
    if (!titleText_.isEmpty()) {
        p.setRenderHint(QPainter::Antialiasing, true);

        QFont hdrFont(QStringLiteral("Segoe UI"), 9, QFont::Bold);
        hdrFont.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
        QFontMetrics hfm(hdrFont);
        QFont tf(QStringLiteral("Segoe UI"), 18, QFont::Bold);
        QFontMetrics tfm(tf);
        QFont unf(QStringLiteral("Segoe UI"), 11);
        unf.setItalic(true);
        QFontMetrics unfm(unf);

        const bool hasUpNext  = !upNextText_.isEmpty();
        const int gapAfterHdr   = 10;
        const int gapAfterTitle = 20;
        const int gapAfterUpHdr = 6;
        int blockH = hfm.height() + gapAfterHdr + tfm.height();
        if (hasUpNext)
            blockH += gapAfterTitle + hfm.height() + gapAfterUpHdr + unfm.height();
        int y = (height() - blockH) / 2 - 20;
        if (y < 16) y = 16;

        p.setFont(hdrFont);
        p.setPen(QColor(233, 69, 96, 200));
        const QString hdr = QStringLiteral("NOW PLAYING");
        p.drawText((width() - hfm.horizontalAdvance(hdr)) / 2, y + hfm.ascent(), hdr);
        y += hfm.height() + gapAfterHdr;

        const float t   = qBound(0.0f, titlePulse_, 1.0f);
        const int cr    = 255 - static_cast<int>(t * (255 - 233));
        const int cg2   = 255 - static_cast<int>(t * (255 - 69));
        const int cb    = 255 - static_cast<int>(t * (255 - 96));
        const int glowAlpha = static_cast<int>(t * 120);

        p.setFont(tf);
        const int tw = tfm.horizontalAdvance(titleText_);
        const int tx = (width() - tw) / 2;
        const int ty = y + tfm.ascent();

        if (glowAlpha > 2 && t > 0.001f) {
            QColor glow(cr, cg2, cb, glowAlpha);
            p.setPen(glow);
            for (auto [dx, dy] : {std::pair{-1,0},{1,0},{0,-1},{0,1},{-2,0},{2,0},{0,-2},{0,2}})
                p.drawText(tx + dx, ty + dy, titleText_);
        }
        p.setPen(QColor(cr, cg2, cb, 220 + static_cast<int>(t * 35)));
        p.drawText(tx, ty, titleText_);
        y += tfm.height() + gapAfterTitle;

        if (hasUpNext) {
            p.setFont(hdrFont);
            p.setPen(QColor(233, 69, 96, 140));
            const QString upHdr = QStringLiteral("UP NEXT");
            p.drawText((width() - hfm.horizontalAdvance(upHdr)) / 2, y + hfm.ascent(), upHdr);
            y += hfm.height() + gapAfterUpHdr;
            p.setFont(unf);
            p.setPen(QColor(180, 180, 180, 180));
            p.drawText((width() - unfm.horizontalAdvance(upNextText_)) / 2, y + unfm.ascent(), upNextText_);
        }
    }
}
