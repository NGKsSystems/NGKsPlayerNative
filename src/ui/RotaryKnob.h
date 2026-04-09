#pragma once

#include <QWidget>
#include <QPainter>
#include <QPainterPath>
#include <QMouseEvent>
#include <QWheelEvent>
#include <QToolTip>
#include <cmath>
#include <functional>

// ═══════════════════════════════════════════════════════════════════
// RotaryKnob — DJ-style circular knob with pointer indicator,
// center detent, and color-coded LPF / neutral / HPF zones.
//
// value range: 0.0 → 1.0   (0.5 = center/neutral)
// angle sweep: 270°  (from 225° to -45°, clockwise)
//
// Interaction:
//   Drag         = coarse adjust
//   Shift+drag   = fine adjust (0.2× sensitivity)
//   Ctrl+drag    = ultra-fine adjust (0.05× sensitivity)
//   Scroll       = step adjust
//   Shift+scroll = fine step (0.2×)
//   Double-click = reset to default value
//
// No Q_OBJECT — uses std::function callbacks to avoid MOC dependency.
// ═══════════════════════════════════════════════════════════════════
class RotaryKnob : public QWidget
{
public:
    explicit RotaryKnob(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        setMinimumSize(90, 90);
        setMaximumSize(140, 140);
        setFixedSize(110, 110);
        setSizePolicy(QSizePolicy::Fixed, QSizePolicy::Fixed);
        setCursor(Qt::PointingHandCursor);
    }

    double value() const { return value_; }

    void setValue(double v)
    {
        v = std::clamp(v, 0.0, 1.0);
        if (std::abs(v - value_) < 1e-6) return;
        value_ = v;
        update();
        if (onValueChanged) onValueChanged(value_);
    }

    // Default value for double-click reset (default 0.5 = center)
    void setDefaultValue(double v) { defaultValue_ = std::clamp(v, 0.0, 1.0); }
    double defaultValue() const { return defaultValue_; }

    // Center-detent threshold (normalized, symmetric around 0.5)
    void setDetentThreshold(double t) { detentThreshold_ = t; }

    // Invisible hit padding around the painted knob (pixels)
    void setHitPadding(double p) { hitPadding_ = p; update(); }

    // Colors for LPF (left) and HPF (right) zones
    void setLpfColor(const QColor& c) { lpfColor_ = c; update(); }
    void setHpfColor(const QColor& c) { hpfColor_ = c; update(); }
    void setNeutralColor(const QColor& c) { neutralColor_ = c; update(); }

    // Callbacks
    std::function<void(double)> onValueChanged;
    std::function<void()> onCenterSnapped;
    std::function<void()> onControlReset;
    std::function<void(double, const char*)> onFineAdjust;

protected:
    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing, true);

        const double w = width();
        const double h = height();
        const double sz = std::min(w, h) - hitPadding_ * 2.0;
        const double cx = w / 2.0;
        const double cy = h / 2.0;
        const double outerR = sz / 2.0 - 3.0;
        const double arcR   = outerR - 4.0;       // radius for value arc
        const double knobR  = outerR - 12.0;       // knob body

        // ── Outer ring (thick, dark) ──
        {
            QPen ringPen(QColor(50, 50, 55), 5.0, Qt::SolidLine, Qt::RoundCap);
            p.setPen(ringPen);
            p.setBrush(Qt::NoBrush);
            const QRectF arcRect(cx - outerR, cy - outerR, outerR * 2, outerR * 2);
            p.drawArc(arcRect, -45 * 16, 270 * 16);
        }

        // ── Arc track (background groove) ──
        {
            QPen arcPen(QColor(40, 40, 45), 6.0, Qt::SolidLine, Qt::RoundCap);
            p.setPen(arcPen);
            p.setBrush(Qt::NoBrush);
            const QRectF arcRect(cx - arcR, cy - arcR, arcR * 2, arcR * 2);
            p.drawArc(arcRect, -45 * 16, 270 * 16);
        }

        // ── Value arc (bright, wide) ──
        {
            QColor arcColor = computeColor();
            // Make arc brighter/more saturated
            arcColor = arcColor.lighter(130);
            QPen valuePen(arcColor, 6.0, Qt::SolidLine, Qt::RoundCap);
            p.setPen(valuePen);
            const QRectF arcRect(cx - arcR, cy - arcR, arcR * 2, arcR * 2);

            const int startAngle16 = 225 * 16;
            const double sweepDeg = -value_ * 270.0;
            p.drawArc(arcRect, startAngle16, static_cast<int>(sweepDeg * 16));
        }

        // ── Center detent tick mark (bright white at top) ──
        {
            const bool atCenter = std::abs(value_ - 0.5) < detentThreshold_;
            QColor tickColor = atCenter ? QColor(255, 255, 255) : QColor(140, 140, 140);
            p.setPen(QPen(tickColor, atCenter ? 3.0 : 2.0));
            const double tickAngle = 90.0 * M_PI / 180.0;
            const double r1 = outerR + 2.0;
            const double r2 = outerR - 8.0;
            p.drawLine(QPointF(cx + r2 * std::cos(tickAngle), cy - r2 * std::sin(tickAngle)),
                       QPointF(cx + r1 * std::cos(tickAngle), cy - r1 * std::sin(tickAngle)));

            // Center glow when at detent
            if (atCenter) {
                QRadialGradient glow(cx, cy - outerR + 2.0, 8.0);
                glow.setColorAt(0.0, QColor(255, 255, 255, 100));
                glow.setColorAt(1.0, QColor(255, 255, 255, 0));
                p.setPen(Qt::NoPen);
                p.setBrush(glow);
                p.drawEllipse(QPointF(cx, cy - outerR + 2.0), 8.0, 8.0);
            }
        }

        // ── Knob body (raised, metallic) ──
        {
            QRadialGradient grad(cx, cy - knobR * 0.3, knobR * 1.2);
            grad.setColorAt(0.0, QColor(90, 90, 95));
            grad.setColorAt(0.5, QColor(55, 55, 60));
            grad.setColorAt(1.0, QColor(35, 35, 40));
            p.setPen(QPen(QColor(100, 100, 105), 1.5));
            p.setBrush(grad);
            p.drawEllipse(QPointF(cx, cy), knobR, knobR);
        }

        // ── Pointer indicator (thick, bright) ──
        {
            const double angleDeg = 225.0 - value_ * 270.0;
            const double angleRad = angleDeg * M_PI / 180.0;

            QColor ptrColor = computeColor().lighter(140);
            p.setPen(QPen(ptrColor, 3.5, Qt::SolidLine, Qt::RoundCap));

            const double r1 = knobR * 0.25;
            const double r2 = knobR * 0.92;
            p.drawLine(
                QPointF(cx + r1 * std::cos(angleRad), cy - r1 * std::sin(angleRad)),
                QPointF(cx + r2 * std::cos(angleRad), cy - r2 * std::sin(angleRad)));
        }
    }

    void mousePressEvent(QMouseEvent* e) override
    {
        if (e->button() == Qt::LeftButton) {
            dragging_ = true;
            lastY_ = e->position().y();
            e->accept();
        }
    }

    void mouseMoveEvent(QMouseEvent* e) override
    {
        if (!dragging_) return;
        const double dy = lastY_ - e->position().y();  // up = positive
        lastY_ = e->position().y();

        // Modifier-based sensitivity: normal / fine / ultra-fine
        double sensitivity = 0.004;  // ~250px for full sweep
        const char* adjustMode = nullptr;
        if (e->modifiers() & Qt::ControlModifier) {
            sensitivity *= 0.05;   // ultra-fine
            adjustMode = "ULTRA";
        } else if (e->modifiers() & Qt::ShiftModifier) {
            sensitivity *= 0.2;    // fine
            adjustMode = "FINE";
        }

        double newVal = value_ + dy * sensitivity;
        newVal = std::clamp(newVal, 0.0, 1.0);

        // Center detent
        if (std::abs(newVal - 0.5) < detentThreshold_ && std::abs(value_ - 0.5) >= detentThreshold_) {
            newVal = 0.5;
            if (onCenterSnapped) onCenterSnapped();
        } else if (std::abs(newVal - 0.5) < detentThreshold_) {
            newVal = 0.5;
        }

        if (std::abs(newVal - value_) > 1e-6) {
            value_ = newVal;
            update();
            if (onValueChanged) onValueChanged(value_);
            if (adjustMode && onFineAdjust) onFineAdjust(value_, adjustMode);
        }

        // Value tooltip near cursor
        showValueTooltip(e->globalPosition().toPoint());
        e->accept();
    }

    void mouseReleaseEvent(QMouseEvent* e) override
    {
        if (e->button() == Qt::LeftButton) {
            dragging_ = false;
            e->accept();
        }
    }

    void mouseDoubleClickEvent(QMouseEvent* e) override
    {
        // Double-click = reset to default value
        if (e->button() == Qt::LeftButton) {
            value_ = defaultValue_;
            update();
            if (onValueChanged) onValueChanged(value_);
            if (std::abs(defaultValue_ - 0.5) < detentThreshold_) {
                if (onCenterSnapped) onCenterSnapped();
            }
            if (onControlReset) onControlReset();
            showValueTooltip(e->globalPosition().toPoint());
            e->accept();
        }
    }

    void wheelEvent(QWheelEvent* e) override
    {
        double step = 0.02;
        const char* adjustMode = nullptr;
        if (e->modifiers() & Qt::ShiftModifier) {
            step *= 0.2;  // fine scroll
            adjustMode = "FINE";
        }
        double delta = (e->angleDelta().y() > 0) ? step : -step;
        double newVal = std::clamp(value_ + delta, 0.0, 1.0);

        // Center detent on wheel
        if (std::abs(newVal - 0.5) < detentThreshold_) {
            newVal = 0.5;
        }

        if (std::abs(newVal - value_) > 1e-6) {
            value_ = newVal;
            update();
            if (onValueChanged) onValueChanged(value_);
            if (adjustMode && onFineAdjust) onFineAdjust(value_, adjustMode);
        }
        showValueTooltip(e->globalPosition().toPoint());
        e->accept();
    }

private:
    void showValueTooltip(const QPoint& globalPos)
    {
        const int pct = static_cast<int>(value_ * 100.0);
        QToolTip::showText(globalPos, QStringLiteral("%1%").arg(pct), this);
    }
    QColor computeColor() const
    {
        if (std::abs(value_ - 0.5) < detentThreshold_) {
            return neutralColor_;
        }
        if (value_ < 0.5) {
            // LPF intensity: 1.0 at value=0, 0.0 at value=0.5
            const double t = 1.0 - (value_ / 0.5);
            return blendColor(neutralColor_, lpfColor_, t);
        }
        // HPF intensity: 0.0 at value=0.5, 1.0 at value=1.0
        const double t = (value_ - 0.5) / 0.5;
        return blendColor(neutralColor_, hpfColor_, t);
    }

    static QColor blendColor(const QColor& a, const QColor& b, double t)
    {
        return QColor(
            static_cast<int>(a.red()   + t * (b.red()   - a.red())),
            static_cast<int>(a.green() + t * (b.green() - a.green())),
            static_cast<int>(a.blue()  + t * (b.blue()  - a.blue())));
    }

    double value_ = 0.5;
    double defaultValue_ = 0.5;
    double detentThreshold_ = 0.03;
    double hitPadding_ = 0.0;
    bool dragging_ = false;
    double lastY_ = 0.0;
    QColor lpfColor_{255, 160, 40};    // orange
    QColor hpfColor_{60, 140, 255};    // blue
    QColor neutralColor_{220, 220, 220}; // white
};
