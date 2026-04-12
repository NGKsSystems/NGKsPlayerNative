#include "ui/DeckStrip.h"
#include "ui/EngineBridge.h"
#include "ui/EqPanel.h"
#include "ui/WaveformOverview.h"

#include <QDragEnterEvent>
#include <QDropEvent>
#include <QFont>
#include <QFrame>
#include <QMimeData>
#include <QUrl>
#include <QHBoxLayout>
#include <QLinearGradient>
#include <QMouseEvent>
#include <QWheelEvent>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QPixmap>
#include <QSignalBlocker>
#include <QSizePolicy>
#include <QVBoxLayout>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <vector>

static const char* kDeckNames[] = { "A", "B", "C", "D" };

// Master kill-switch for stem/energy overlay feature.
// Set to true to re-enable. All overlay rendering, data fetch, and toggle
// paths are gated on this flag.
static constexpr bool kEnableStemOverlay = true;

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
        setMinimumHeight(24);
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
        setMinimumHeight(24);
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

// WaveformOverview is now in ui/WaveformOverview.h


// ═══════════════════════════════════════════════════════════════════
// JogWheel — circular jog surface for seek/scratch interaction
// ═══════════════════════════════════════════════════════════════════
class JogWheel : public QWidget {
public:
    enum Role { Primary = 0, Secondary = 1 };

    explicit JogWheel(const QColor& accent, int diameter = 300,
                      Role role = Primary, QWidget* parent = nullptr)
        : QWidget(parent), accent_(accent), role_(role)
    {
        setFixedSize(diameter, diameter);
        setCursor(Qt::OpenHandCursor);
    }

    /// Set album art for the primary jog. Ignored on secondary.
    void setAlbumArt(const QPixmap& art) {
        if (role_ != Primary) return;
        coverArt_ = art;
        coverScaled_ = QPixmap();  // invalidate cache
        update();
    }

    /// Clear album art (e.g. on track unload).
    void clearAlbumArt() {
        coverArt_ = QPixmap();
        coverScaled_ = QPixmap();
        update();
    }

    std::function<void(double, Qt::KeyboardModifiers)> onJogTurn;

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing, true);
        p.setRenderHint(QPainter::SmoothPixmapTransform, true);
        const int w = width();
        const int h = height();
        const int sz = std::min(w, h) - 4;
        const int cx = w / 2, cy = h / 2;
        const bool isPrimary = (role_ == Primary);

        // ── Outer shadow/glow halo ──
        for (int i = 3; i >= 1; --i) {
            QColor halo = accent_;
            halo.setAlpha(isPrimary ? (10 * i) : (5 * i));
            p.setPen(QPen(halo, 1.5));
            p.setBrush(Qt::NoBrush);
            p.drawEllipse(cx - sz / 2 - i, cy - sz / 2 - i, sz + 2 * i, sz + 2 * i);
        }

        // ── Outer ring ──
        const int outerRingW = isPrimary ? 3 : 1;
        {
            QRadialGradient rg(cx, cy, sz / 2);
            rg.setColorAt(0.0, QColor(0x14, 0x18, 0x22));
            rg.setColorAt(1.0, QColor(0x08, 0x0a, 0x10));
            p.setBrush(rg);
        }
        QColor ringColor = accent_;
        ringColor.setAlpha(isPrimary ? 140 : 50);
        p.setPen(QPen(ringColor, outerRingW));
        p.drawEllipse(cx - sz / 2, cy - sz / 2, sz, sz);

        // ── Tick marks ──
        {
            const int tickCount = isPrimary ? 40 : 24;
            const int tickOuterR = sz / 2 - 2;
            const int tickInnerR = tickOuterR - (isPrimary ? 8 : 5);
            for (int i = 0; i < tickCount; ++i) {
                const double a = (2.0 * 3.14159265358979 * i) / tickCount;
                const bool major = (i % (isPrimary ? 5 : 4)) == 0;
                QColor tc = major ? QColor(0x66, 0x6a, 0x70) : QColor(0x33, 0x36, 0x3a);
                p.setPen(QPen(tc, major ? 1.5 : 0.8));
                const double sa = std::sin(a), ca = std::cos(a);
                p.drawLine(
                    cx + static_cast<int>(sa * tickInnerR),
                    cy - static_cast<int>(ca * tickInnerR),
                    cx + static_cast<int>(sa * tickOuterR),
                    cy - static_cast<int>(ca * tickOuterR));
            }
        }

        // ── Inner platter ──
        const int innerSz = sz - 24;

        if (isPrimary && !coverArt_.isNull()) {
            // ═══ PRIMARY WITH ALBUM ART ═══
            // Clip to inner circle and paint album art
            const int artSz = innerSz - 2;
            p.save();
            QPainterPath clipPath;
            clipPath.addEllipse(cx - artSz / 2, cy - artSz / 2, artSz, artSz);
            p.setClipPath(clipPath);

            // Cache scaled pixmap
            if (coverScaled_.isNull() || coverScaled_.width() != artSz) {
                coverScaled_ = coverArt_.scaled(artSz, artSz,
                    Qt::KeepAspectRatioByExpanding, Qt::SmoothTransformation);
                // Center-crop if not square
                if (coverScaled_.width() > artSz || coverScaled_.height() > artSz) {
                    int sx = (coverScaled_.width() - artSz) / 2;
                    int sy = (coverScaled_.height() - artSz) / 2;
                    coverScaled_ = coverScaled_.copy(sx, sy, artSz, artSz);
                }
            }
            p.drawPixmap(cx - artSz / 2, cy - artSz / 2, coverScaled_);

            // Darken overlay for contrast
            p.setClipPath(clipPath);
            p.setPen(Qt::NoPen);
            p.setBrush(QColor(0, 0, 0, 80));
            p.drawEllipse(cx - artSz / 2, cy - artSz / 2, artSz, artSz);

            p.restore();

            // Inner ring around the art
            QColor artRing = accent_;
            artRing.setAlpha(100);
            p.setPen(QPen(artRing, 1.5));
            p.setBrush(Qt::NoBrush);
            p.drawEllipse(cx - artSz / 2, cy - artSz / 2, artSz, artSz);

            // (no center label — album art fills the platter)

        } else if (isPrimary) {
            // ═══ PRIMARY, NO ALBUM ART — rich fallback ═══
            {
                QRadialGradient pg(cx, cy, innerSz / 2);
                pg.setColorAt(0.0, QColor(0x18, 0x1c, 0x28));
                pg.setColorAt(0.5, QColor(0x12, 0x15, 0x1e));
                pg.setColorAt(1.0, QColor(0x0a, 0x0d, 0x14));
                p.setBrush(pg);
            }
            p.setPen(QPen(QColor(0x2a, 0x2e, 0x38), 1));
            p.drawEllipse(cx - innerSz / 2, cy - innerSz / 2, innerSz, innerSz);

            // Vinyl grooves
            p.setBrush(Qt::NoBrush);
            for (int i = 1; i <= 6; ++i) {
                const int gr = innerSz / 2 - i * (innerSz / 14);
                p.setPen(QPen(QColor(0x1e, 0x22, 0x2c, 70), 0.5));
                p.drawEllipse(cx - gr, cy - gr, gr * 2, gr * 2);
            }

        } else {
            // ═══ SECONDARY — fine-tune precision wheel ═══
            // Darker, tighter platter with concentric precision rings
            {
                QRadialGradient pg(cx, cy, innerSz / 2);
                pg.setColorAt(0.0, QColor(0x10, 0x13, 0x1a));
                pg.setColorAt(0.6, QColor(0x0c, 0x0e, 0x15));
                pg.setColorAt(1.0, QColor(0x08, 0x0a, 0x10));
                p.setBrush(pg);
            }
            p.setPen(QPen(QColor(0x1e, 0x22, 0x2c), 1));
            p.drawEllipse(cx - innerSz / 2, cy - innerSz / 2, innerSz, innerSz);

            // Fine graduated rings — precision encoder look
            p.setBrush(Qt::NoBrush);
            const int ringCount = 8;
            for (int i = 1; i <= ringCount; ++i) {
                const int gr = innerSz / 2 - i * (innerSz / (ringCount * 2 + 2));
                const int alpha = (i <= 2 || i >= ringCount - 1) ? 30 : 18;
                p.setPen(QPen(QColor(0x20, 0x24, 0x30, alpha), 0.5));
                p.drawEllipse(cx - gr, cy - gr, gr * 2, gr * 2);
            }

            // Fine-tune crosshair — thin etched lines through center
            {
                const int chLen = innerSz / 5;
                p.setPen(QPen(QColor(0x2a, 0x2e, 0x3a, 50), 0.5));
                p.drawLine(cx - chLen, cy, cx + chLen, cy);
                p.drawLine(cx, cy - chLen, cx, cy + chLen);
            }

            // "FINE" label — small text near bottom of platter
            {
                QFont f = font();
                f.setPointSize(6);
                f.setBold(true);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
                p.setFont(f);
                p.setPen(QColor(0x3a, 0x3e, 0x50));
                p.drawText(QRect(cx - innerSz / 3, cy + innerSz / 5,
                                  innerSz * 2 / 3, innerSz / 6),
                            Qt::AlignHCenter | Qt::AlignTop,
                            QStringLiteral("FINE"));
            }
        }

        // ── Center dot ──
        p.setPen(Qt::NoPen);
        if (isPrimary) {
            // Primary — accent hub dot
            QRadialGradient dg(cx, cy, 7);
            QColor bright = accent_; bright.setAlpha(255);
            QColor dim = accent_; dim.setAlpha(120);
            dg.setColorAt(0.0, bright);
            dg.setColorAt(1.0, dim);
            p.setBrush(dg);
            p.drawEllipse(cx - 6, cy - 6, 12, 12);
        } else {
            // Secondary — small precision crosshair dot (no glow)
            p.setBrush(QColor(accent_.red(), accent_.green(), accent_.blue(), 180));
            p.drawEllipse(cx - 3, cy - 3, 6, 6);
            // Thin ring around center
            p.setBrush(Qt::NoBrush);
            p.setPen(QPen(QColor(accent_.red(), accent_.green(), accent_.blue(), 60), 0.75));
            p.drawEllipse(cx - 7, cy - 7, 14, 14);
        }

        // ── Position indicator line ──
        const double rad = angle_ * 3.14159265358979 / 180.0;
        const int lineStart = isPrimary ? 0 : innerSz / 6;
        const int lineLen = innerSz / 2 - (isPrimary ? 10 : 14);
        const int lsx = cx + static_cast<int>(std::sin(rad) * lineStart);
        const int lsy = cy - static_cast<int>(std::cos(rad) * lineStart);
        const int lx = cx + static_cast<int>(std::sin(rad) * lineLen);
        const int ly = cy - static_cast<int>(std::cos(rad) * lineLen);
        {
            QColor lglow = accent_;
            lglow.setAlpha(isPrimary ? 50 : 20);
            p.setPen(QPen(lglow, isPrimary ? 6 : 3));
            p.drawLine(lsx, lsy, lx, ly);
        }
        QColor lineColor = accent_;
        lineColor.setAlpha(isPrimary ? 255 : 180);
        p.setPen(QPen(lineColor, isPrimary ? 2.5 : 1.2));
        p.drawLine(lsx, lsy, lx, ly);

        // ── Accent ring glow ──
        QColor glowColor = accent_;
        glowColor.setAlpha(isPrimary ? 70 : 30);
        p.setPen(QPen(glowColor, isPrimary ? 4 : 2));
        p.setBrush(Qt::NoBrush);
        p.drawEllipse(cx - sz / 2 + 3, cy - sz / 2 + 3, sz - 6, sz - 6);
    }

    void mousePressEvent(QMouseEvent* e) override {
        dragging_ = true;
        lastPos_ = e->position().toPoint();
        setCursor(Qt::ClosedHandCursor);
    }

    void mouseMoveEvent(QMouseEvent* e) override {
        if (!dragging_) return;
        const int cx = width() / 2;
        const int cy = height() / 2;
        const QPoint pos = e->position().toPoint();

        const double prevAngle = std::atan2(
            static_cast<double>(lastPos_.x() - cx),
            -static_cast<double>(lastPos_.y() - cy));
        const double currAngle = std::atan2(
            static_cast<double>(pos.x() - cx),
            -static_cast<double>(pos.y() - cy));
        double delta = (currAngle - prevAngle) * 180.0 / 3.14159265358979;

        if (delta > 180.0) delta -= 360.0;
        if (delta < -180.0) delta += 360.0;

        angle_ += delta;
        while (angle_ > 360.0) angle_ -= 360.0;
        while (angle_ < 0.0) angle_ += 360.0;

        lastPos_ = pos;
        update();

        if (onJogTurn) onJogTurn(delta, e->modifiers());
    }

    void mouseReleaseEvent(QMouseEvent*) override {
        dragging_ = false;
        setCursor(Qt::OpenHandCursor);
    }

private:
    QColor accent_;
    Role role_{Primary};
    double angle_{0.0};
    QPoint lastPos_;
    bool dragging_{false};
    QPixmap coverArt_;
    QPixmap coverScaled_;
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
    setAcceptDrops(true);
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
        "    stop:0 #0e1118, stop:0.46 #0e1118,"
        "    stop:0.5 %2, stop:0.54 #0e1118, stop:1 #0e1118);"
        "  border: 1px solid %3; border-radius: 6px; }")
        .arg(deckIndex_).arg(guideHex, borderColor));
    outerVBox->addWidget(outerFrame);

    auto* mainLayout = new QVBoxLayout(outerFrame);
    mainLayout->setContentsMargins(2, 1, 2, 2);
    mainLayout->setSpacing(0);

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

    // ═══ LOAD BUTTON (hidden — loading via library double-click) ═══
    {
        loadBtn_ = new QPushButton(QStringLiteral("LOAD"), outerFrame);
        loadBtn_->hide();
    }

    // ═══ SECTION 2: TRACK DETAIL BLOCK ═══
    {
        auto* trackBlock = new QFrame(outerFrame);
        trackBlock->setObjectName(QStringLiteral("trackBlock%1").arg(deckIndex_));
        trackBlock->setStyleSheet(QStringLiteral(
            "QFrame#trackBlock%1 { background: #080b10; border: none; }").arg(deckIndex_));

        auto* trackLayout = new QVBoxLayout(trackBlock);
        trackLayout->setContentsMargins(6, 2, 6, 2);
        trackLayout->setSpacing(1);

        // ── Track info row: Title (left) | Artist (center) | Duration (right) ──
        {
            auto* infoRow = new QHBoxLayout();
            infoRow->setSpacing(6);
            infoRow->setContentsMargins(0, 0, 0, 0);

            trackTitleLabel_ = new QLabel(QStringLiteral("\u2014 NO TRACK \u2014"), trackBlock);
            {
                QFont f = trackTitleLabel_->font();
                f.setPointSizeF(9.0);
                f.setBold(true);
                trackTitleLabel_->setFont(f);
            }
            trackTitleLabel_->setStyleSheet(QStringLiteral(
                "color: #555; background: transparent; border: none;"));
            trackTitleLabel_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
            trackTitleLabel_->setMinimumWidth(60);
            infoRow->addWidget(trackTitleLabel_, 1);

            trackArtistLabel_ = new QLabel(QString(), trackBlock);
            {
                QFont f = trackArtistLabel_->font();
                f.setPointSizeF(8.0);
                trackArtistLabel_->setFont(f);
            }
            trackArtistLabel_->setStyleSheet(QStringLiteral(
                "color: #666; background: transparent; border: none;"));
            trackArtistLabel_->setAlignment(Qt::AlignCenter);
            infoRow->addWidget(trackArtistLabel_, 1);

            infoDurationLabel_ = new QLabel(QStringLiteral("--:--"), trackBlock);
            {
                QFont f = infoDurationLabel_->font();
                f.setPointSizeF(8.5);
                f.setBold(true);
                infoDurationLabel_->setFont(f);
            }
            infoDurationLabel_->setStyleSheet(QStringLiteral(
                "color: #555; background: transparent; border: none;"));
            infoDurationLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);
            infoRow->addWidget(infoDurationLabel_);

            trackLayout->addLayout(infoRow);
        }

        // BPM/Key (hidden — shown inside analysis dashboard)
        infoBpmLabel_ = new QLabel(QStringLiteral("---"), outerFrame);
        infoBpmLabel_->hide();
        infoKeyLabel_ = new QLabel(QStringLiteral("---"), outerFrame);
        infoKeyLabel_->hide();

        // ── DJ Analysis Panel (custom-painted cards, NOT text labels) ──
        analysisDash_ = new DjAnalysisPanelWidget(QColor(accent_), trackBlock);
        analysisDash_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);
        trackLayout->addWidget(analysisDash_);
        qInfo().noquote() << QStringLiteral("DJ_ANALYSIS_WIDGET class=DjAnalysisPanelWidget deck=%1")
            .arg(deckIndex_);

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
        waveformOverview_->setMinimumHeight(24);
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

    // (LIVE button removed — waveform is always live)
    waveModeBtn_ = nullptr;

    addFlowSep();

    // ═══ ZONE 3: JOG TOGGLE / COLLAPSIBLE JOG PANEL ═══
    {
        auto* jogRow = new QHBoxLayout();
        jogRow->setSpacing(3);
        jogRow->setContentsMargins(3, 1, 3, 1);

        jogToggleBtn_ = new QPushButton(QStringLiteral("JOG"), outerFrame);
        jogToggleBtn_->setCheckable(true);
        jogToggleBtn_->setChecked(false);
        jogToggleBtn_->setCursor(Qt::PointingHandCursor);
        jogToggleBtn_->setToolTip(QStringLiteral("Toggle jog wheel panel"));
        jogToggleBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
            "  border-radius: 3px; color: #888; padding: 1px 8px;"
            "  min-height: 22px; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,25); color: #ccc;"
            "  border: 1px solid rgba(%1,%2,%3,120); }"
            "QPushButton:checked {"
            "  background: rgba(%1,%2,%3,40);"
            "  border: 2px solid rgba(%1,%2,%3,180); color: #fff; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        { QFont f = jogToggleBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); jogToggleBtn_->setFont(f); }
        jogRow->addWidget(jogToggleBtn_);
        jogRow->addStretch();
        jogToggleBtn_->setVisible(false);

        // Jog panel (always visible — added to jogCenterRow_ later)
        jogPanel_ = new QFrame(outerFrame);
        jogPanel_->setObjectName(QStringLiteral("jogPanel%1").arg(deckIndex_));
        jogPanel_->setStyleSheet(QStringLiteral(
            "QFrame#jogPanel%1 {"
            "  background: transparent; border: none; }")
            .arg(deckIndex_));

        auto* jogLayout = new QVBoxLayout(jogPanel_);
        jogLayout->setSpacing(1);
        jogLayout->setContentsMargins(0, 1, 0, 1);

        // Primary jog wheel
        jogWheel_ = new JogWheel(accentColor, 230, JogWheel::Primary, jogPanel_);
        jogLayout->addWidget(jogWheel_, 0, Qt::AlignHCenter);

        // Mode toggles + label
        auto* jogModeRow = new QHBoxLayout();
        jogModeRow->setSpacing(3);
        jogModeRow->setContentsMargins(0, 0, 0, 0);

        jogSeekBtn_ = new QPushButton(QStringLiteral("SEEK"), jogPanel_);
        jogSeekBtn_->setCheckable(true);
        jogSeekBtn_->setChecked(true);
        jogSeekBtn_->setCursor(Qt::PointingHandCursor);
        jogSeekBtn_->setStyleSheet(btnSmall);
        { QFont f = jogSeekBtn_->font(); f.setPointSizeF(6.5); f.setBold(true); jogSeekBtn_->setFont(f); }
        jogModeRow->addWidget(jogSeekBtn_, 1);

        jogScratchBtn_ = new QPushButton(QStringLiteral("SCRATCH"), jogPanel_);
        jogScratchBtn_->setCheckable(true);
        jogScratchBtn_->setChecked(false);
        jogScratchBtn_->setCursor(Qt::PointingHandCursor);
        jogScratchBtn_->setStyleSheet(btnSmall);
        { QFont f = jogScratchBtn_->font(); f.setPointSizeF(6.5); f.setBold(true); jogScratchBtn_->setFont(f); }
        jogModeRow->addWidget(jogScratchBtn_, 1);

        jogModeLabel_ = new QLabel(QStringLiteral("MODE: SEEK"), jogPanel_);
        jogModeLabel_->setAlignment(Qt::AlignCenter);
        { QFont f = jogModeLabel_->font(); f.setPointSizeF(5.5); jogModeLabel_->setFont(f); }
        jogModeLabel_->setStyleSheet(QStringLiteral(
            "color: #666; background: transparent; border: none;"));
        jogModeRow->addWidget(jogModeLabel_);

        jogLayout->addLayout(jogModeRow);

        jogPanel_->setVisible(true);
        jogVisible_ = true;

        // ── Secondary jog panel (equal size) ──
        jogPanelSecondary_ = new QFrame(outerFrame);
        jogPanelSecondary_->setObjectName(QStringLiteral("jogPanelSec%1").arg(deckIndex_));
        jogPanelSecondary_->setStyleSheet(QStringLiteral(
            "QFrame#jogPanelSec%1 {"
            "  background: transparent; border: none; }")
            .arg(deckIndex_));

        auto* jogSecLayout = new QVBoxLayout(jogPanelSecondary_);
        jogSecLayout->setSpacing(1);
        jogSecLayout->setContentsMargins(0, 1, 0, 1);

        jogWheelSecondary_ = new JogWheel(accentColor, 230, JogWheel::Secondary, jogPanelSecondary_);
        jogSecLayout->addWidget(jogWheelSecondary_, 0, Qt::AlignHCenter);

        auto* secLabel = new QLabel(QStringLiteral("FINE"), jogPanelSecondary_);
        secLabel->setAlignment(Qt::AlignHCenter);
        { QFont f = secLabel->font(); f.setPointSizeF(6.0); f.setBold(true); secLabel->setFont(f); }
        secLabel->setStyleSheet(QStringLiteral(
            "color: #888; background: transparent; border: none;"));
        jogSecLayout->addWidget(secLabel);

        jogPanelSecondary_->setVisible(true);
    }

    // ═══ ZONE 3: COMPACT TRANSPORT ═══
    {
        auto* transportPanel = new QFrame(outerFrame);
        transportPanel->setObjectName(QStringLiteral("deckTransport%1").arg(deckIndex_));
        transportPanel->setStyleSheet(QStringLiteral(
            "QFrame#deckTransport%1 {"
            "  background: #0a0d14; border: none;"
            "  border-radius: 2px; }")
            .arg(deckIndex_));

        auto* transportOuter = new QVBoxLayout(transportPanel);
        transportOuter->setSpacing(1);
        transportOuter->setContentsMargins(2, 1, 2, 1);

        // ── Row A: [ PLAY ] [ CUE ] ──
        auto* rowA = new QHBoxLayout();
        rowA->setSpacing(3);
        rowA->setContentsMargins(0, 0, 0, 0);

        playBtn_ = new QPushButton(QStringLiteral("PLAY"), outerFrame);
        playBtn_->setStyleSheet(btnPrimary);
        playBtn_->setCursor(Qt::PointingHandCursor);
        playBtn_->setToolTip(QStringLiteral("Play / Pause"));
        { QFont f = playBtn_->font(); f.setPointSizeF(10.0); f.setBold(true); playBtn_->setFont(f); }
        rowA->addWidget(playBtn_, 1);

        cueBtn_ = new QPushButton(QStringLiteral("CUE"), outerFrame);
        cueBtn_->setCheckable(true);
        cueBtn_->setChecked(false);
        cueBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #1c2030, stop:1 #0e1018);"
            "  border: 1px solid rgba(%1,%2,%3,100); border-radius: 4px;"
            "  color: #e0e0e0; padding: 2px 6px;"
            "  min-height: 32px; font-weight: bold; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,50); color: #fff;"
            "  border: 1px solid rgba(%1,%2,%3,200); }"
            "QPushButton:checked {"
            "  background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #0c4020, stop:1 #083018);"
            "  border: 2px solid #00ff44; color: #00ff55;"
            "  font-size: 10pt; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        cueBtn_->setCursor(Qt::PointingHandCursor);
        cueBtn_->setToolTip(QStringLiteral("CUE \u2014 monitor deck in headphones"));
        { QFont f = cueBtn_->font(); f.setPointSizeF(9.0); f.setBold(true); cueBtn_->setFont(f); }
        rowA->addWidget(cueBtn_, 1);

        transportOuter->addLayout(rowA);

        // ── Row B: [ << ] [ >> ] [ SYNC ] [ MASTER ] ──
        auto* rowB = new QHBoxLayout();
        rowB->setSpacing(2);
        rowB->setContentsMargins(0, 0, 0, 0);

        syncBtn_ = new QPushButton(QStringLiteral("\u00AB"), outerFrame);
        syncBtn_->setStyleSheet(btnSecondary);
        syncBtn_->setCursor(Qt::PointingHandCursor);
        syncBtn_->setToolTip(QStringLiteral("Seek back 30 seconds"));
        { QFont f = syncBtn_->font(); f.setPointSizeF(9.0); f.setBold(true); syncBtn_->setFont(f); }
        rowB->addWidget(syncBtn_, 1);

        hotCueBtn_ = new QPushButton(QStringLiteral("\u00BB"), outerFrame);
        hotCueBtn_->setStyleSheet(btnSecondary);
        hotCueBtn_->setCursor(Qt::PointingHandCursor);
        hotCueBtn_->setToolTip(QStringLiteral("Seek forward 30 seconds"));
        { QFont f = hotCueBtn_->font(); f.setPointSizeF(9.0); f.setBold(true); hotCueBtn_->setFont(f); }
        rowB->addWidget(hotCueBtn_, 1);

        syncToggleBtn_ = new QPushButton(QStringLiteral("SYNC"), outerFrame);
        syncToggleBtn_->setCheckable(true);
        syncToggleBtn_->setChecked(false);
        syncToggleBtn_->setCursor(Qt::PointingHandCursor);
        syncToggleBtn_->setToolTip(QStringLiteral("Sync deck tempo"));
        syncToggleBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
            "  border-radius: 3px; color: #888; padding: 1px 2px;"
            "  min-height: 24px; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,25); color: #ccc;"
            "  border: 1px solid rgba(%1,%2,%3,120); }"
            "QPushButton:checked {"
            "  background: rgba(%1,%2,%3,40);"
            "  border: 2px solid rgba(%1,%2,%3,180); color: #fff; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        { QFont f = syncToggleBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); syncToggleBtn_->setFont(f); }
        rowB->addWidget(syncToggleBtn_, 1);

        masterToggleBtn_ = new QPushButton(QStringLiteral("MASTER"), outerFrame);
        masterToggleBtn_->setCheckable(true);
        masterToggleBtn_->setChecked(false);
        masterToggleBtn_->setCursor(Qt::PointingHandCursor);
        masterToggleBtn_->setToolTip(QStringLiteral("Set as tempo master"));
        masterToggleBtn_->setStyleSheet(QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
            "  border-radius: 3px; color: #888; padding: 1px 2px;"
            "  min-height: 24px; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,25); color: #ccc;"
            "  border: 1px solid rgba(%1,%2,%3,120); }"
            "QPushButton:checked {"
            "  background: rgba(255,180,0,40);"
            "  border: 2px solid #ffb400; color: #ffcc00; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
        { QFont f = masterToggleBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); masterToggleBtn_->setFont(f); }
        rowB->addWidget(masterToggleBtn_, 1);

        transportOuter->addLayout(rowB);

        // Hidden: pause + stop + loop kept for API compatibility
        pauseBtn_ = new QPushButton(outerFrame); pauseBtn_->hide();
        stopBtn_ = new QPushButton(outerFrame); stopBtn_->hide();
        loopBtn_ = new QPushButton(outerFrame); loopBtn_->hide();

        mainLayout->addWidget(transportPanel);
    }

    // ═══ ZONE 4: DUAL-JOG CONTROL SURFACE (mirrored per deck) ═══
    // Shared container — both jogs sit in one unified panel
    jogContainer_ = new QFrame(outerFrame);
    jogContainer_->setObjectName(QStringLiteral("jogContainer%1").arg(deckIndex_));
    jogContainer_->setStyleSheet(QStringLiteral(
        "QFrame#jogContainer%1 {"
        "  background: transparent;"
        "  border: none; }")
        .arg(deckIndex_));

    jogCenterRow_ = new QHBoxLayout(jogContainer_);
    jogCenterRow_->setSpacing(2);
    // Center both jogs horizontally with equal stretch on each side.
    jogCenterRow_->setContentsMargins(2, 3, 2, 3);
    jogCenterRow_->addStretch(1);
    if (deckIndex_ == 0) {
        jogCenterRow_->addWidget(jogPanel_, 0, Qt::AlignTop);
        jogCenterRow_->addWidget(jogPanelSecondary_, 0, Qt::AlignTop);
    } else {
        jogCenterRow_->addWidget(jogPanel_, 0, Qt::AlignTop);
        jogCenterRow_->addWidget(jogPanelSecondary_, 0, Qt::AlignTop);
    }
    jogCenterRow_->addStretch(1);
    mainLayout->addWidget(jogContainer_);

    // ═══ ZONE 5+6: COLLAPSIBLE PERFORMANCE + CUE EDIT ═══
    {
        // ── Toggle button row: [PERFORMANCE] [CUE EDIT] ──
        auto* panelToggleRow = new QHBoxLayout();
        panelToggleRow->setSpacing(2);
        panelToggleRow->setContentsMargins(1, 0, 1, 0);

        const QString btnToggleStyle = QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,50);"
            "  border-radius: 3px; color: #888; padding: 1px 8px;"
            "  min-height: 22px; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,25); color: #ccc;"
            "  border: 1px solid rgba(%1,%2,%3,120); }"
            "QPushButton:checked {"
            "  background: rgba(%1,%2,%3,40);"
            "  border: 2px solid rgba(%1,%2,%3,180); color: #fff; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

        perfToggleBtn_ = new QPushButton(QStringLiteral("PERFORMANCE"), outerFrame);
        perfToggleBtn_->setCheckable(true);
        perfToggleBtn_->setChecked(false);
        perfToggleBtn_->setCursor(Qt::PointingHandCursor);
        perfToggleBtn_->setToolTip(QStringLiteral("Toggle hot cues, loop, auto-loop, beat jump"));
        perfToggleBtn_->setStyleSheet(btnToggleStyle);
        { QFont f = perfToggleBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); perfToggleBtn_->setFont(f); }
        panelToggleRow->addWidget(perfToggleBtn_, 1);

        cueEditToggleBtn_ = new QPushButton(QStringLiteral("CUE EDIT"), outerFrame);
        cueEditToggleBtn_->setCheckable(true);
        cueEditToggleBtn_->setChecked(false);
        cueEditToggleBtn_->setCursor(Qt::PointingHandCursor);
        cueEditToggleBtn_->setToolTip(QStringLiteral("Toggle cue fine adjustment controls"));
        cueEditToggleBtn_->setStyleSheet(btnToggleStyle);
        { QFont f = cueEditToggleBtn_->font(); f.setPointSizeF(7.0); f.setBold(true); cueEditToggleBtn_->setFont(f); }
        panelToggleRow->addWidget(cueEditToggleBtn_, 1);

        mainLayout->addLayout(panelToggleRow);
    }

    // ── PERFORMANCE panel (collapsed by default) ──
    {
        perfPanel_ = new QFrame(outerFrame);
        perfPanel_->setObjectName(QStringLiteral("perfPanel%1").arg(deckIndex_));
        perfPanel_->setStyleSheet(QStringLiteral(
            "QFrame#perfPanel%1 {"
            "  background: #080b10; border: 1px solid rgba(%2,%3,%4,30);"
            "  border-radius: 4px; }")
            .arg(deckIndex_)
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));

        auto* perfLayout = new QVBoxLayout(perfPanel_);
        perfLayout->setSpacing(2);
        perfLayout->setContentsMargins(4, 3, 4, 3);

        // Hot Cue row
        {
            auto* hotCueRow = new QHBoxLayout();
            hotCueRow->setSpacing(2);
            hotCueRow->setContentsMargins(0, 0, 0, 0);

            auto* hotCueLbl = new QLabel(QStringLiteral("HOT CUE"), perfPanel_);
            hotCueLbl->setStyleSheet(sectionLabelStyle);
            hotCueLbl->setFixedWidth(40);
            hotCueLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
            hotCueRow->addWidget(hotCueLbl);
            hotCueRow->addSpacing(2);

            auto makeHotCue = [&](const QString& text) -> QPushButton* {
                auto* btn = new QPushButton(text, perfPanel_);
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

            perfLayout->addLayout(hotCueRow);
        }

        // Loop row
        {
            auto* loopRow = new QHBoxLayout();
            loopRow->setSpacing(2);
            loopRow->setContentsMargins(0, 0, 0, 0);

            auto* loopLbl = new QLabel(QStringLiteral("LOOP"), perfPanel_);
            loopLbl->setStyleSheet(sectionLabelStyle);
            loopLbl->setFixedWidth(40);
            loopLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
            loopRow->addWidget(loopLbl);
            loopRow->addSpacing(2);

            auto makeLoopBtn = [&](const QString& text) -> QPushButton* {
                auto* btn = new QPushButton(text, perfPanel_);
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

            loopSizeLabel_ = new QLabel(QStringLiteral("4 BEAT"), perfPanel_);
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

            perfLayout->addLayout(loopRow);
        }

        // Auto-loop row
        {
            auto* autoLoopRow = new QHBoxLayout();
            autoLoopRow->setSpacing(2);
            autoLoopRow->setContentsMargins(0, 0, 0, 0);

            auto* autoLbl = new QLabel(QStringLiteral("AUTO"), perfPanel_);
            autoLbl->setStyleSheet(sectionLabelStyle);
            autoLbl->setFixedWidth(40);
            autoLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
            autoLoopRow->addWidget(autoLbl);
            autoLoopRow->addSpacing(2);

            auto makeSmall = [&](const QString& text) -> QPushButton* {
                auto* btn = new QPushButton(text, perfPanel_);
                btn->setStyleSheet(btnSmall);
                btn->setCursor(Qt::PointingHandCursor);
                QFont f = btn->font(); f.setPointSizeF(6.5); f.setBold(true); btn->setFont(f);
                return btn;
            };

            autoLoop1Btn_ = makeSmall(QStringLiteral("1"));
            autoLoop2Btn_ = makeSmall(QStringLiteral("2"));
            autoLoop4Btn_ = makeSmall(QStringLiteral("4"));
            autoLoop8Btn_ = makeSmall(QStringLiteral("8"));
            autoLoop16Btn_ = makeSmall(QStringLiteral("16"));

            autoLoopRow->addWidget(autoLoop1Btn_, 1);
            autoLoopRow->addWidget(autoLoop2Btn_, 1);
            autoLoopRow->addWidget(autoLoop4Btn_, 1);
            autoLoopRow->addWidget(autoLoop8Btn_, 1);
            autoLoopRow->addWidget(autoLoop16Btn_, 1);

            perfLayout->addLayout(autoLoopRow);
        }

        // Beat jump row
        {
            auto* jumpRow = new QHBoxLayout();
            jumpRow->setSpacing(2);
            jumpRow->setContentsMargins(0, 0, 0, 0);

            auto* jumpLbl = new QLabel(QStringLiteral("JUMP"), perfPanel_);
            jumpLbl->setStyleSheet(sectionLabelStyle);
            jumpLbl->setFixedWidth(40);
            jumpLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
            jumpRow->addWidget(jumpLbl);
            jumpRow->addSpacing(2);

            auto makeJump = [&](const QString& text) -> QPushButton* {
                auto* btn = new QPushButton(text, perfPanel_);
                btn->setStyleSheet(btnSmall);
                btn->setCursor(Qt::PointingHandCursor);
                QFont f = btn->font(); f.setPointSizeF(6.5); f.setBold(true); btn->setFont(f);
                return btn;
            };

            beatJumpNeg8Btn_ = makeJump(QStringLiteral("-8"));
            beatJumpNeg4Btn_ = makeJump(QStringLiteral("-4"));
            beatJumpNeg2Btn_ = makeJump(QStringLiteral("-2"));
            beatJumpPos2Btn_ = makeJump(QStringLiteral("+2"));
            beatJumpPos4Btn_ = makeJump(QStringLiteral("+4"));
            beatJumpPos8Btn_ = makeJump(QStringLiteral("+8"));

            jumpRow->addWidget(beatJumpNeg8Btn_, 1);
            jumpRow->addWidget(beatJumpNeg4Btn_, 1);
            jumpRow->addWidget(beatJumpNeg2Btn_, 1);
            jumpRow->addSpacing(4);
            jumpRow->addWidget(beatJumpPos2Btn_, 1);
            jumpRow->addWidget(beatJumpPos4Btn_, 1);
            jumpRow->addWidget(beatJumpPos8Btn_, 1);

            perfLayout->addLayout(jumpRow);
        }

        perfPanel_->setVisible(false);  // collapsed by default
        perfVisible_ = false;
        mainLayout->addWidget(perfPanel_);
    }

    // ── CUE EDIT panel (collapsed by default) ──
    {
        cueEditPanel_ = new QFrame(outerFrame);
        cueEditPanel_->setObjectName(QStringLiteral("cueEditPanel%1").arg(deckIndex_));
        cueEditPanel_->setStyleSheet(QStringLiteral(
            "QFrame#cueEditPanel%1 {"
            "  background: #080b10; border: 1px solid rgba(%2,%3,%4,30);"
            "  border-radius: 4px; }")
            .arg(deckIndex_)
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));

        auto* cueEditLayout = new QHBoxLayout(cueEditPanel_);
        cueEditLayout->setSpacing(1);
        cueEditLayout->setContentsMargins(4, 3, 4, 3);

        auto* cueAdjLbl = new QLabel(QStringLiteral("CUE \u00B1"), cueEditPanel_);
        cueAdjLbl->setStyleSheet(sectionLabelStyle);
        cueAdjLbl->setFixedWidth(30);
        cueAdjLbl->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        cueEditLayout->addWidget(cueAdjLbl);

        const QString btnCueAdj = QStringLiteral(
            "QPushButton {"
            "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,40);"
            "  border-radius: 2px; color: #888; padding: 0px 2px;"
            "  min-height: 18px; min-width: 22px; }"
            "QPushButton:hover {"
            "  background: rgba(%1,%2,%3,25); color: #ccc;"
            "  border: 1px solid rgba(%1,%2,%3,110); }"
            "QPushButton:pressed { background: #060810; color: #ddd; }")
            .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

        auto makeCueAdj = [&](const QString& text) -> QPushButton* {
            auto* btn = new QPushButton(text, cueEditPanel_);
            btn->setStyleSheet(btnCueAdj);
            btn->setCursor(Qt::PointingHandCursor);
            QFont f = btn->font(); f.setPointSizeF(6.0); f.setBold(true); btn->setFont(f);
            return btn;
        };

        cueAdjNeg5_ = makeCueAdj(QStringLiteral("-5"));
        cueAdjNeg1_ = makeCueAdj(QStringLiteral("-1"));
        cueAdjNegH_ = makeCueAdj(QStringLiteral("-0.5"));
        cueAdjNegT_ = makeCueAdj(QStringLiteral("-0.1"));
        cueAdjPosT_ = makeCueAdj(QStringLiteral("+0.1"));
        cueAdjPosH_ = makeCueAdj(QStringLiteral("+0.5"));
        cueAdjPos1_ = makeCueAdj(QStringLiteral("+1"));
        cueAdjPos5_ = makeCueAdj(QStringLiteral("+5"));

        cueEditLayout->addWidget(cueAdjNeg5_, 1);
        cueEditLayout->addWidget(cueAdjNeg1_, 1);
        cueEditLayout->addWidget(cueAdjNegH_, 1);
        cueEditLayout->addWidget(cueAdjNegT_, 1);
        cueEditLayout->addSpacing(4);
        cueEditLayout->addWidget(cueAdjPosT_, 1);
        cueEditLayout->addWidget(cueAdjPosH_, 1);
        cueEditLayout->addWidget(cueAdjPos1_, 1);
        cueEditLayout->addWidget(cueAdjPos5_, 1);

        cueEditPanel_->setVisible(false);  // collapsed by default
        cueEditVisible_ = false;
        mainLayout->addWidget(cueEditPanel_);
    }

    // ═══ ZONE 7: MIXER CONTROLS (horizontal strip below jog) ═══
    {
        const int knobSz = 32;
        const int hitPad = 3;
        const int widgetSz = knobSz + hitPad * 2;

        auto makeKnobCell = [&](const QString& label, RotaryKnob*& knob,
                                QLabel*& valLabel, QWidget* parent,
                                double initVal, double defaultVal) -> QVBoxLayout* {
            auto* cell = new QVBoxLayout();
            cell->setSpacing(0);
            cell->setContentsMargins(0, 0, 0, 0);

            knob = new RotaryKnob(parent);
            knob->setFixedSize(widgetSz, widgetSz);
            knob->setMinimumSize(widgetSz, widgetSz);
            knob->setMaximumSize(widgetSz, widgetSz);
            knob->setHitPadding(static_cast<double>(hitPad));
            knob->setValue(initVal);
            knob->setDefaultValue(defaultVal);
            cell->addWidget(knob, 0, Qt::AlignHCenter);

            auto* lbl = new QLabel(label, parent);
            lbl->setAlignment(Qt::AlignHCenter);
            { QFont f = lbl->font(); f.setPointSizeF(5.0); f.setBold(true); lbl->setFont(f); }
            lbl->setStyleSheet(QStringLiteral(
                "color: #999; background: transparent; border: none;"));
            cell->addWidget(lbl);

            valLabel = new QLabel(QStringLiteral("0"), parent);
            valLabel->setAlignment(Qt::AlignHCenter);
            { QFont f = valLabel->font(); f.setPointSizeF(4.5); valLabel->setFont(f); }
            valLabel->setStyleSheet(QStringLiteral(
                "color: #666; background: transparent; border: none;"));
            valLabel->setFixedHeight(10);
            cell->addWidget(valLabel);

            return cell;
        };

        // ── PITCH fader + range → under jog wheel (inside jogPanel_) ──
        {
            auto* jogLayout = qobject_cast<QVBoxLayout*>(jogPanel_->layout());

            pitchFader_ = new QSlider(Qt::Horizontal, jogPanel_);
            pitchFader_->setRange(-1000, 1000);
            pitchFader_->setValue(0);
            pitchFader_->setFixedHeight(20);
            pitchFader_->setCursor(Qt::PointingHandCursor);
            pitchFader_->setStyleSheet(QStringLiteral(
                "QSlider::groove:horizontal {"
                "  background: #060810; height: 6px; border-radius: 3px;"
                "  border: 1px solid rgba(%1,%2,%3,30); }"
                "QSlider::handle:horizontal {"
                "  background: #cccccc; width: 10px; height: 16px;"
                "  margin: -5px 0; border-radius: 3px; }"
                "QSlider::sub-page:horizontal {"
                "  background: rgba(%1,%2,%3,50); border-radius: 3px; }"
                "QSlider::add-page:horizontal { background: transparent; }")
                .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
            pitchFader_->installEventFilter(this);
            jogLayout->addWidget(pitchFader_);

            pitchFaderReadout_ = new QLabel(QStringLiteral("+0.0%"), jogPanel_);
            pitchFaderReadout_->setAlignment(Qt::AlignHCenter);
            { QFont f = pitchFaderReadout_->font(); f.setPointSizeF(5.5); pitchFaderReadout_->setFont(f); }
            pitchFaderReadout_->setStyleSheet(QStringLiteral(
                "color: #666; background: transparent; border: none;"));
            pitchFaderReadout_->setFixedHeight(14);
            jogLayout->addWidget(pitchFaderReadout_);

            auto* pitchRangeRow = new QHBoxLayout();
            pitchRangeRow->setSpacing(2);
            pitchRangeRow->setContentsMargins(0, 0, 0, 0);

            const QString btnMicro = QStringLiteral(
                "QPushButton {"
                "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,35);"
                "  border-radius: 2px; color: #777; padding: 0px 2px;"
                "  min-height: 16px; min-width: 24px; }"
                "QPushButton:hover {"
                "  background: rgba(%1,%2,%3,20); color: #bbb;"
                "  border: 1px solid rgba(%1,%2,%3,100); }"
                "QPushButton:checked {"
                "  background: rgba(%1,%2,%3,30);"
                "  border: 1px solid rgba(%1,%2,%3,160); color: #eee; }"
                "QPushButton:pressed { background: #060810; color: #ddd; }")
                .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue());

            pitchRange6Btn_ = new QPushButton(QStringLiteral("\u00B16"), jogPanel_);
            pitchRange6Btn_->setCheckable(true); pitchRange6Btn_->setChecked(true);
            pitchRange6Btn_->setStyleSheet(btnMicro); pitchRange6Btn_->setCursor(Qt::PointingHandCursor);
            { QFont f = pitchRange6Btn_->font(); f.setPointSizeF(5.5); f.setBold(true); pitchRange6Btn_->setFont(f); }
            pitchRangeRow->addWidget(pitchRange6Btn_);

            pitchRange10Btn_ = new QPushButton(QStringLiteral("\u00B110"), jogPanel_);
            pitchRange10Btn_->setCheckable(true); pitchRange10Btn_->setChecked(false);
            pitchRange10Btn_->setStyleSheet(btnMicro); pitchRange10Btn_->setCursor(Qt::PointingHandCursor);
            { QFont f = pitchRange10Btn_->font(); f.setPointSizeF(5.5); f.setBold(true); pitchRange10Btn_->setFont(f); }
            pitchRangeRow->addWidget(pitchRange10Btn_);

            pitchRange16Btn_ = new QPushButton(QStringLiteral("\u00B116"), jogPanel_);
            pitchRange16Btn_->setCheckable(true); pitchRange16Btn_->setChecked(false);
            pitchRange16Btn_->setStyleSheet(btnMicro); pitchRange16Btn_->setCursor(Qt::PointingHandCursor);
            { QFont f = pitchRange16Btn_->font(); f.setPointSizeF(5.5); f.setBold(true); pitchRange16Btn_->setFont(f); }
            pitchRangeRow->addWidget(pitchRange16Btn_);

            keyLockBtn_ = new QPushButton(QStringLiteral("KEY"), jogPanel_);
            keyLockBtn_->setCheckable(true); keyLockBtn_->setChecked(false);
            keyLockBtn_->setCursor(Qt::PointingHandCursor);
            keyLockBtn_->setToolTip(QStringLiteral("Lock musical key"));
            keyLockBtn_->setStyleSheet(QStringLiteral(
                "QPushButton {"
                "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,35);"
                "  border-radius: 2px; color: #777; padding: 0px 2px; min-height: 16px; }"
                "QPushButton:hover { background: rgba(%1,%2,%3,20); color: #bbb; }"
                "QPushButton:checked { background: rgba(0,200,100,25); border: 1px solid #00cc66; color: #00ee77; }")
                .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue()));
            { QFont f = keyLockBtn_->font(); f.setPointSizeF(5.5); f.setBold(true); keyLockBtn_->setFont(f); }
            pitchRangeRow->addWidget(keyLockBtn_);

            pitchRangeRow->addStretch();
            jogLayout->addLayout(pitchRangeRow);

            // Hidden for API compat
            pitchKnob_ = new RotaryKnob(outerFrame);
            pitchKnob_->setVisible(false);
            pitchValueLabel_ = new QLabel(QString(), outerFrame);
            pitchValueLabel_->setVisible(false);
        }

        // ── Horizontal knob strip: GAIN | REV | FILTER | FLG | FX | MUTE | meters ──
        auto* knobStrip = new QHBoxLayout();
        knobStrip->setSpacing(4);
        knobStrip->setContentsMargins(4, 2, 4, 2);

        auto* gainCell = makeKnobCell(QStringLiteral("GAIN"), gainKnob_,
                                      volumeDbLabel_, outerFrame, 0.5, 0.5);
        volumeDbLabel_->setText(QStringLiteral("0 dB"));
        gainKnob_->setDetentThreshold(0.03);
        knobStrip->addLayout(gainCell);

        auto* reverbCell = makeKnobCell(QStringLiteral("REV"), reverbKnob_,
                                        reverbValueLabel_, outerFrame, 0.0, 0.0);
        reverbValueLabel_->setText(QStringLiteral("0%"));
        reverbKnob_->setDefaultValue(0.0);
        knobStrip->addLayout(reverbCell);

        {
            filterLabel_ = new QLabel(QStringLiteral("FILTER"), outerFrame);
            filterLabel_->setVisible(false);

            auto* filterCell = makeKnobCell(QStringLiteral("FILTER"), filterKnob_,
                                            filterValueLabel_, outerFrame, 0.5, 0.5);
            filterValueLabel_->setText(QStringLiteral("0.00"));
            knobStrip->addLayout(filterCell);

            if (filterCell->count() > 1) {
                if (auto* w = filterCell->itemAt(1) ? filterCell->itemAt(1)->widget() : nullptr) {
                    if (auto* lbl = qobject_cast<QLabel*>(w)) filterLabel_ = lbl;
                }
            }
        }

        auto* flangerCell = makeKnobCell(QStringLiteral("FLG"), flangerKnob_,
                                         flangerValueLabel_, outerFrame, 0.0, 0.0);
        flangerValueLabel_->setText(QStringLiteral("0%"));
        flangerKnob_->setDefaultValue(0.0);
        knobStrip->addLayout(flangerCell);

        // FX button
        {
            auto* fxCell = new QVBoxLayout();
            fxCell->setSpacing(0);
            fxCell->setContentsMargins(0, 0, 0, 0);

            deckFxBtn_ = new QPushButton(QStringLiteral("FX"), outerFrame);
            deckFxBtn_->setCheckable(true); deckFxBtn_->setChecked(false);
            deckFxBtn_->setCursor(Qt::PointingHandCursor);
            deckFxBtn_->setFixedSize(widgetSz, widgetSz);
            { QFont f = deckFxBtn_->font(); f.setPointSizeF(6.0); f.setBold(true); deckFxBtn_->setFont(f); }
            deckFxBtn_->setStyleSheet(QStringLiteral(
                "QPushButton {"
                "  background: #0e1018; border: 1px solid rgba(%1,%2,%3,40);"
                "  border-radius: %4px; color: #666; }"
                "QPushButton:hover { border: 1px solid rgba(%1,%2,%3,120); color: #bbb; }"
                "QPushButton:checked {"
                "  background: rgba(%1,%2,%3,50);"
                "  border: 2px solid rgba(%1,%2,%3,200); color: #fff; }")
                .arg(accentColor.red()).arg(accentColor.green()).arg(accentColor.blue())
                .arg(widgetSz / 2));
            fxCell->addWidget(deckFxBtn_, 0, Qt::AlignHCenter);

            auto* fxLbl = new QLabel(QStringLiteral("FX"), outerFrame);
            fxLbl->setAlignment(Qt::AlignHCenter);
            { QFont f = fxLbl->font(); f.setPointSizeF(5.0); f.setBold(true); fxLbl->setFont(f); }
            fxLbl->setStyleSheet(QStringLiteral(
                "color: #999; background: transparent; border: none;"));
            fxCell->addWidget(fxLbl);

            auto* fxSpacer = new QLabel(QString(), outerFrame);
            fxSpacer->setFixedHeight(10);
            fxSpacer->setStyleSheet(QStringLiteral("background: transparent; border: none;"));
            fxCell->addWidget(fxSpacer);

            knobStrip->addLayout(fxCell);
        }

        // Separator
        knobStrip->addSpacing(4);

        // MUTE button
        {
            auto* muteCell = new QVBoxLayout();
            muteCell->setSpacing(0);
            muteCell->setContentsMargins(0, 0, 0, 0);

            muteBtn_ = new QPushButton(QStringLiteral("MUTE"), outerFrame);
            muteBtn_->setCheckable(true); muteBtn_->setChecked(false);
            muteBtn_->setCursor(Qt::PointingHandCursor);
            muteBtn_->setToolTip(QStringLiteral("Mute deck output"));
            muteBtn_->setFixedSize(widgetSz, 20);
            { QFont f = muteBtn_->font(); f.setPointSizeF(5.0); f.setBold(true); muteBtn_->setFont(f); }
            muteBtn_->setStyleSheet(QStringLiteral(
                "QPushButton {"
                "  background: #0e1018; border: 1px solid #333; border-radius: 2px; color: #888; }"
                "QPushButton:hover { border: 1px solid #555; color: #bbb; }"
                "QPushButton:checked {"
                "  background: #4a0808; border: 1px solid #ff1111; color: #ff2222; }"));
            muteCell->addWidget(muteBtn_, 0, Qt::AlignHCenter);
            muteCell->addStretch();
            knobStrip->addLayout(muteCell);
        }

        // Level meters
        meterL_ = new LevelMeter(accentColor, outerFrame);
        meterL_->setFixedSize(10, 50);
        knobStrip->addWidget(meterL_);

        meterR_ = new LevelMeter(accentColor, outerFrame);
        meterR_->setFixedSize(10, 50);
        knobStrip->addWidget(meterR_);

        mainLayout->addLayout(knobStrip);

        // Keep mixerFrame_ for API compat (hidden)
        mixerFrame_ = new QFrame(outerFrame);
        mixerFrame_->setVisible(false);

        // Density buttons (hidden, kept for API)
        mixerDensityUpBtn_ = new QPushButton(outerFrame); mixerDensityUpBtn_->hide();
        mixerDensityDownBtn_ = new QPushButton(outerFrame); mixerDensityDownBtn_->hide();
    }

    // ═══ ZONE 8: EQ / ADVANCED (compact footer) ═══
    {
        cueMonBtn_ = nullptr;

        auto* eqRow = new QHBoxLayout();
        eqRow->setSpacing(3);
        eqRow->setContentsMargins(2, 1, 2, 1);

        eqPanel_ = new EqPanel(bridge_, outerFrame);
        eqPanel_->setDeckIndex(deckIndex_);
        eqPanel_->setAccentColor(accent_);
        eqPanel_->setCollapsed(true);
        eqPanel_->setContentsMargins(0, 0, 0, 0);
        eqRow->addWidget(eqPanel_, 1);

        mainLayout->addLayout(eqRow);
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
        std::fprintf(stderr, "MIX_CUE_ASSIGN deck=%d enabled=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });
    // Skip back / forward 30 seconds (with logging)
    connect(syncBtn_, &QPushButton::clicked, this, [this]() {
        const double pos = bridge_->deckPlayhead(deckIndex_);
        bridge_->seekDeck(deckIndex_, std::max(0.0, pos - 30.0));
        std::fprintf(stderr, "TRANSPORT_SEEK_BACK deck=%d from=%.2f to=%.2f\n",
                     deckIndex_, pos, std::max(0.0, pos - 30.0));
        std::fflush(stderr);
    });
    connect(hotCueBtn_, &QPushButton::clicked, this, [this]() {
        const double pos = bridge_->deckPlayhead(deckIndex_);
        const double dur = bridge_->deckDuration(deckIndex_);
        bridge_->seekDeck(deckIndex_, std::min(dur, pos + 30.0));
        std::fprintf(stderr, "TRANSPORT_SEEK_FORWARD deck=%d from=%.2f to=%.2f\n",
                     deckIndex_, pos, std::min(dur, pos + 30.0));
        std::fflush(stderr);
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

    // CUE fine tune — adjust playhead position by ±N seconds
    auto wireCueAdj = [this](QPushButton* btn, double delta) {
        connect(btn, &QPushButton::clicked, this, [this, delta]() {
            const double pos = bridge_->deckPlayhead(deckIndex_);
            const double dur = bridge_->deckDuration(deckIndex_);
            const double target = std::clamp(pos + delta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "CUE_ADJUST deck=%d delta=%.1f from=%.2f to=%.2f\n",
                         deckIndex_, delta, pos, target);
            std::fflush(stderr);
        });
    };
    wireCueAdj(cueAdjNeg5_, -5.0);
    wireCueAdj(cueAdjNeg1_, -1.0);
    wireCueAdj(cueAdjNegH_, -0.5);
    wireCueAdj(cueAdjNegT_, -0.1);
    wireCueAdj(cueAdjPosT_,  0.1);
    wireCueAdj(cueAdjPosH_,  0.5);
    wireCueAdj(cueAdjPos1_,  1.0);
    wireCueAdj(cueAdjPos5_,  5.0);

    // Player Control Strip — PITCH / REVERB / FLANGER knobs (structure, logging only)
    pitchKnob_->onValueChanged = [this](double value) {
        const int pct = static_cast<int>(value * 100.0);
        if (pitchValueLabel_) pitchValueLabel_->setText(QStringLiteral("%1%").arg(pct));
        std::fprintf(stderr, "FX_PARAM_CHANGE deck=%d param=PITCH value=%.3f\n",
                     deckIndex_, value);
        std::fflush(stderr);
    };
    reverbKnob_->onValueChanged = [this](double value) {
        const int pct = static_cast<int>(value * 100.0);
        if (reverbValueLabel_) reverbValueLabel_->setText(QStringLiteral("%1%").arg(pct));
        std::fprintf(stderr, "FX_PARAM_CHANGE deck=%d param=REVERB value=%.3f\n",
                     deckIndex_, value);
        std::fflush(stderr);
    };
    reverbKnob_->onControlReset = [this]() {
        std::fprintf(stderr, "CONTROL_RESET deck=%d param=REVERB\n", deckIndex_);
        std::fflush(stderr);
    };
    reverbKnob_->onFineAdjust = [this](double value, const char* mode) {
        std::fprintf(stderr, "CONTROL_FINE_ADJUST deck=%d param=REVERB value=%.3f mode=%s\n",
                     deckIndex_, value, mode);
        std::fflush(stderr);
    };
    flangerKnob_->onValueChanged = [this](double value) {
        const int pct = static_cast<int>(value * 100.0);
        if (flangerValueLabel_) flangerValueLabel_->setText(QStringLiteral("%1%").arg(pct));
        std::fprintf(stderr, "FX_PARAM_CHANGE deck=%d param=FLANGER value=%.3f\n",
                     deckIndex_, value);
        std::fflush(stderr);
    };
    flangerKnob_->onControlReset = [this]() {
        std::fprintf(stderr, "CONTROL_RESET deck=%d param=FLANGER\n", deckIndex_);
        std::fflush(stderr);
    };
    flangerKnob_->onFineAdjust = [this](double value, const char* mode) {
        std::fprintf(stderr, "CONTROL_FINE_ADJUST deck=%d param=FLANGER value=%.3f mode=%s\n",
                     deckIndex_, value, mode);
        std::fflush(stderr);
    };

    // DECK FX toggle (structure only — future-proof)
    connect(deckFxBtn_, &QPushButton::clicked, this, [this](bool checked) {
        deckFxBtn_->setText(checked ? QStringLiteral("ON") : QStringLiteral("OFF"));
        std::fprintf(stderr, "DECK_FX_TOGGLE deck=%d enabled=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    // MUTE toggle
    connect(muteBtn_, &QPushButton::clicked, this, [this](bool checked) {
        bridge_->setDeckMute(deckIndex_, checked);
        std::fprintf(stderr, "MIX_SIGNAL_PATH_STATE deck=%d muted=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    // DJ FILTER knob (rotary, center=neutral, left=LPF, right=HPF)
    filterKnob_->onValueChanged = [this](double value) {
        bridge_->setDeckFilter(deckIndex_, value);
        // Value readout: -1.00 (full LPF) through 0.00 (center) to +1.00 (full HPF)
        const double display = (value - 0.5) * 2.0;
        if (filterValueLabel_)
            filterValueLabel_->setText(QStringLiteral("%1%2")
                .arg(display >= 0.005 ? QStringLiteral("+") : QString())
                .arg(display, 0, 'f', 2));
        // Logging
        const char* dir = (value < 0.47) ? "LPF" : (value > 0.53) ? "HPF" : "CENTER";
        std::fprintf(stderr, "MIX_FILTER_SET deck=%d pos=%.3f direction=%s\n",
                     deckIndex_, value, dir);
        std::fflush(stderr);
        // Update label with filter state
        if (value < 0.47) {
            filterLabel_->setText(QStringLiteral("FILTER LPF"));
            filterLabel_->setStyleSheet(QStringLiteral(
                "color: #ff9030; background: transparent; border: none; padding: 0; font-size: 6pt; font-weight: bold;"));
        } else if (value > 0.53) {
            filterLabel_->setText(QStringLiteral("FILTER HPF"));
            filterLabel_->setStyleSheet(QStringLiteral(
                "color: #50a0f0; background: transparent; border: none; padding: 0; font-size: 6pt; font-weight: bold;"));
        } else {
            filterLabel_->setText(QStringLiteral("FILTER"));
            filterLabel_->setStyleSheet(QStringLiteral(
                "color: #ccc; background: transparent; border: none; padding: 0; font-size: 6pt; font-weight: bold;"));
        }
    };
    filterKnob_->onCenterSnapped = [this]() {
        std::fprintf(stderr, "FILTER_CENTER_SNAP deck=%d\n", deckIndex_);
        std::fflush(stderr);
    };
    filterKnob_->onControlReset = [this]() {
        std::fprintf(stderr, "CONTROL_RESET deck=%d param=FILTER\n", deckIndex_);
        std::fflush(stderr);
    };
    filterKnob_->onFineAdjust = [this](double value, const char* mode) {
        std::fprintf(stderr, "CONTROL_FINE_ADJUST deck=%d param=FILTER value=%.3f mode=%s\n",
                     deckIndex_, value, mode);
        std::fflush(stderr);
    };

    gainKnob_->onValueChanged = [this](double value) {
        // Map 0.0–1.0 knob → 0.0–2.0 linear gain (0.5 = unity = 0 dB)
        const double linear = value * 2.0;
        bridge_->setDeckGain(deckIndex_, linear);
        if (linear < 1e-4) {
            volumeDbLabel_->setText(QStringLiteral("-\u221E dB"));
        } else {
            const double db = 20.0 * std::log10(linear);
            const QString sign = (db >= 0.05) ? QStringLiteral("+") : QString();
            volumeDbLabel_->setText(QStringLiteral("%1%2 dB").arg(sign).arg(db, 0, 'f', 1));
        }
        std::fprintf(stderr, "MIX_GAIN_SET deck=%d linear=%.3f\n",
                     deckIndex_, linear);
        std::fflush(stderr);
    };
    gainKnob_->onCenterSnapped = [this]() {
        std::fprintf(stderr, "GAIN_CENTER_SNAP deck=%d (0dB)\n", deckIndex_);
        std::fflush(stderr);
    };
    gainKnob_->onControlReset = [this]() {
        std::fprintf(stderr, "CONTROL_RESET deck=%d param=GAIN\n", deckIndex_);
        std::fflush(stderr);
    };
    gainKnob_->onFineAdjust = [this](double value, const char* mode) {
        std::fprintf(stderr, "CONTROL_FINE_ADJUST deck=%d param=GAIN value=%.3f mode=%s\n",
                     deckIndex_, value, mode);
        std::fflush(stderr);
    };

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

    // Waveform mode: locked to LIVE (button removed, guard retained)
    if (waveModeBtn_) {
        connect(waveModeBtn_, &QPushButton::toggled, this, [this](bool) {
            waveformCtrl_.setUserMode(WaveUserMode::LIVE);
            waveModeBtn_->setText(QStringLiteral("LIVE"));
        });
    }

    // ═══ NEW: Jog system ═══
    connect(jogToggleBtn_, &QPushButton::clicked, this, [this](bool checked) {
        jogVisible_ = checked;
        jogPanel_->setVisible(checked);
        std::fprintf(stderr, "JOG_PANEL_TOGGLE deck=%d visible=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    // ═══ NEW: PERFORMANCE + CUE EDIT panel toggles ═══
    connect(perfToggleBtn_, &QPushButton::clicked, this, [this](bool checked) {
        perfVisible_ = checked;
        perfPanel_->setVisible(checked);
        std::fprintf(stderr, "PERF_PANEL_TOGGLE deck=%d visible=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    connect(cueEditToggleBtn_, &QPushButton::clicked, this, [this](bool checked) {
        cueEditVisible_ = checked;
        cueEditPanel_->setVisible(checked);
        std::fprintf(stderr, "CUE_EDIT_PANEL_TOGGLE deck=%d visible=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    connect(jogSeekBtn_, &QPushButton::clicked, this, [this]() {
        jogMode_ = 0;
        jogSeekBtn_->setChecked(true);
        jogScratchBtn_->setChecked(false);
        jogModeLabel_->setText(QStringLiteral("MODE: SEEK"));
        std::fprintf(stderr, "JOG_MODE_SET deck=%d mode=SEEK\n", deckIndex_);
        std::fflush(stderr);
    });

    connect(jogScratchBtn_, &QPushButton::clicked, this, [this]() {
        jogMode_ = 1;
        jogSeekBtn_->setChecked(false);
        jogScratchBtn_->setChecked(true);
        jogModeLabel_->setText(QStringLiteral("MODE: SCRATCH"));
        std::fprintf(stderr, "JOG_MODE_SET deck=%d mode=SCRATCH\n", deckIndex_);
        std::fflush(stderr);
    });

    static_cast<JogWheel*>(jogWheel_)->onJogTurn = [this](double deltaDeg, Qt::KeyboardModifiers mods) {
        // CTRL held: reduce sensitivity 3× for semi-precision
        const double sensitivity = (mods & Qt::ControlModifier) ? (1.0 / 3.0) : 1.0;
        const char* modLabel = (mods & Qt::ControlModifier) ? " [CTRL]" : "";

        if (jogMode_ == 0) {
            // SEEK mode: 1 full rotation = 5 seconds (1.67s with CTRL)
            const double seekDelta = (deltaDeg / 360.0) * 5.0 * sensitivity;
            const double pos = bridge_->deckPlayhead(deckIndex_);
            const double dur = bridge_->deckDuration(deckIndex_);
            const double target = std::clamp(pos + seekDelta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "PRIMARY_JOG_SEEK deck=%d delta=%.3f from=%.2f to=%.2f%s\n",
                         deckIndex_, seekDelta, pos, target, modLabel);
            std::fflush(stderr);
        } else {
            // SCRATCH mode: log only (full scratch engine not yet wired)
            const double scratchDelta = deltaDeg * sensitivity;
            std::fprintf(stderr, "PRIMARY_JOG_SCRATCH deck=%d delta_deg=%.2f%s\n",
                         deckIndex_, scratchDelta, modLabel);
            std::fflush(stderr);
        }
    };

    // ═══ SECONDARY JOG: context-driven precision control ═══
    static_cast<JogWheel*>(jogWheelSecondary_)->onJogTurn = [this](double deltaDeg, Qt::KeyboardModifiers mods) {
        // SHIFT held: ultra-fine 4× reduction
        const double shiftMul = (mods & Qt::ShiftModifier) ? 0.25 : 1.0;
        const char* shiftTag = (mods & Qt::ShiftModifier) ? " [SHIFT]" : "";

        const double pos = bridge_->deckPlayhead(deckIndex_);
        const double dur = bridge_->deckDuration(deckIndex_);

        if (cueEditVisible_) {
            // FINE TRIM: tightest resolution — 1 rotation = 20ms
            const double seekDelta = (deltaDeg / 360.0) * 0.02 * shiftMul;
            const double target = std::clamp(pos + seekDelta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "SECONDARY_JOG_FINE_TRIM deck=%d delta=%.5f to=%.4f%s\n",
                         deckIndex_, seekDelta, target, shiftTag);
            std::fflush(stderr);
        } else if (!bridge_->deckIsPlaying(deckIndex_) && trackLoaded_) {
            // CUE ADJUST: paused/stopped with track — 1 rotation = 50ms
            const double seekDelta = (deltaDeg / 360.0) * 0.05 * shiftMul;
            const double target = std::clamp(pos + seekDelta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "SECONDARY_JOG_CUE_ADJUST deck=%d delta=%.5f to=%.4f%s\n",
                         deckIndex_, seekDelta, target, shiftTag);
            std::fflush(stderr);
        } else if (bridge_->deckIsPlaying(deckIndex_)) {
            // NUDGE: tempo nudge while playing — 1 rotation = 150ms
            const double seekDelta = (deltaDeg / 360.0) * 0.15 * shiftMul;
            const double target = std::clamp(pos + seekDelta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "SECONDARY_JOG_NUDGE deck=%d delta=%.5f to=%.4f%s\n",
                         deckIndex_, seekDelta, target, shiftTag);
            std::fflush(stderr);
        } else {
            // Fallback: no track loaded — fine seek
            const double seekDelta = (deltaDeg / 360.0) * 0.1 * shiftMul;
            const double target = std::clamp(pos + seekDelta, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "SECONDARY_JOG_IDLE deck=%d delta=%.5f%s\n",
                         deckIndex_, seekDelta, shiftTag);
            std::fflush(stderr);
        }
    };

    // ═══ NEW: SYNC / MASTER toggles ═══
    connect(syncToggleBtn_, &QPushButton::clicked, this, [this](bool checked) {
        syncEnabled_ = checked;
        std::fprintf(stderr, "DECK_SYNC_TOGGLE deck=%d enabled=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    connect(masterToggleBtn_, &QPushButton::clicked, this, [this](bool checked) {
        masterEnabled_ = checked;
        std::fprintf(stderr, "DECK_MASTER_TOGGLE deck=%d enabled=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    // ═══ NEW: Pitch fader ═══
    connect(pitchFader_, &QSlider::valueChanged, this, [this](int value) {
        // Map slider value to percentage based on current pitch range
        const double pct = (static_cast<double>(value) / 1000.0) * pitchRange_;
        const QString sign = (pct >= 0.05) ? QStringLiteral("+") : QString();
        pitchFaderReadout_->setText(QStringLiteral("%1%2%")
            .arg(sign).arg(pct, 0, 'f', 1));
        std::fprintf(stderr, "MIX_PITCH_SET deck=%d pct=%.2f range=%.0f\n",
                     deckIndex_, pct, pitchRange_);
        std::fflush(stderr);
    });
    // Double-click reset for pitch fader
    pitchFader_->installEventFilter(this);

    // ═══ NEW: Pitch range toggles ═══
    auto wirePitchRange = [this](QPushButton* btn, double range) {
        connect(btn, &QPushButton::clicked, this, [this, range, btn]() {
            pitchRange_ = range;
            pitchRange6Btn_->setChecked(range == 6.0);
            pitchRange10Btn_->setChecked(range == 10.0);
            pitchRange16Btn_->setChecked(range == 16.0);
            // Re-emit current fader value with new range mapping
            const double pct = (static_cast<double>(pitchFader_->value()) / 1000.0) * pitchRange_;
            const QString sign = (pct >= 0.05) ? QStringLiteral("+") : QString();
            pitchFaderReadout_->setText(QStringLiteral("%1%2%")
                .arg(sign).arg(pct, 0, 'f', 1));
            std::fprintf(stderr, "PITCH_RANGE_SET deck=%d range=%.0f\n",
                         deckIndex_, range);
            std::fflush(stderr);
        });
    };
    wirePitchRange(pitchRange6Btn_, 6.0);
    wirePitchRange(pitchRange10Btn_, 10.0);
    wirePitchRange(pitchRange16Btn_, 16.0);

    // ═══ NEW: KEY LOCK ═══
    connect(keyLockBtn_, &QPushButton::clicked, this, [this](bool checked) {
        keyLocked_ = checked;
        std::fprintf(stderr, "KEY_LOCK_TOGGLE deck=%d enabled=%d\n",
                     deckIndex_, checked ? 1 : 0);
        std::fflush(stderr);
    });

    // ═══ NEW: Auto-loop ═══
    auto wireAutoLoop = [this](QPushButton* btn, int beats) {
        connect(btn, &QPushButton::clicked, this, [this, beats]() {
            std::fprintf(stderr, "AUTO_LOOP_SET deck=%d beats=%d\n",
                         deckIndex_, beats);
            std::fflush(stderr);
        });
    };
    wireAutoLoop(autoLoop1Btn_, 1);
    wireAutoLoop(autoLoop2Btn_, 2);
    wireAutoLoop(autoLoop4Btn_, 4);
    wireAutoLoop(autoLoop8Btn_, 8);
    wireAutoLoop(autoLoop16Btn_, 16);

    // ═══ NEW: Beat jump ═══
    auto wireBeatJump = [this](QPushButton* btn, int beats) {
        connect(btn, &QPushButton::clicked, this, [this, beats]() {
            // Estimate beat duration from BPM (fallback 2s if no BPM)
            const double bpm = bridge_->deckBpmFixed(deckIndex_);
            const double beatSec = (bpm > 0.0) ? (60.0 / bpm) : 2.0;
            const double jumpSec = beatSec * static_cast<double>(beats);
            const double pos = bridge_->deckPlayhead(deckIndex_);
            const double dur = bridge_->deckDuration(deckIndex_);
            const double target = std::clamp(pos + jumpSec, 0.0, dur);
            bridge_->seekDeck(deckIndex_, target);
            std::fprintf(stderr, "BEAT_JUMP deck=%d beats=%d sec=%.3f from=%.2f to=%.2f\n",
                         deckIndex_, beats, jumpSec, pos, target);
            std::fflush(stderr);
        });
    };
    wireBeatJump(beatJumpNeg8Btn_, -8);
    wireBeatJump(beatJumpNeg4Btn_, -4);
    wireBeatJump(beatJumpNeg2Btn_, -2);
    wireBeatJump(beatJumpPos2Btn_, 2);
    wireBeatJump(beatJumpPos4Btn_, 4);
    wireBeatJump(beatJumpPos8Btn_, 8);

    // ═══ NEW: Mixer density ═══
    connect(mixerDensityUpBtn_, &QPushButton::clicked, this, [this]() {
        if (mixerDensityMode_ < 1) {
            mixerDensityMode_ = 1;
            applyMixerDensity();
            std::fprintf(stderr, "MIXER_DENSITY_CHANGED deck=%d mode=NORMAL\n", deckIndex_);
            std::fflush(stderr);
        }
    });
    connect(mixerDensityDownBtn_, &QPushButton::clicked, this, [this]() {
        if (mixerDensityMode_ > 0) {
            mixerDensityMode_ = 0;
            applyMixerDensity();
            std::fprintf(stderr, "MIXER_DENSITY_CHANGED deck=%d mode=COMPACT\n", deckIndex_);
            std::fflush(stderr);
        }
    });
}

void DeckStrip::dragEnterEvent(QDragEnterEvent* e)
{
    if (e->mimeData()->hasFormat(QStringLiteral("application/x-ngks-track-id")) || e->mimeData()->hasUrls())
        e->acceptProposedAction();
    else
        QWidget::dragEnterEvent(e);
}

void DeckStrip::dropEvent(QDropEvent* e)
{
    if (e->mimeData()->hasFormat(QStringLiteral("application/x-ngks-track-id"))) {
        bool ok = false;
        const qint64 tid = QString::fromUtf8(
            e->mimeData()->data(QStringLiteral("application/x-ngks-track-id"))).toLongLong(&ok);
        if (ok) {
            e->acceptProposedAction();
            emit loadTrackRequested(deckIndex_, tid);
        }
        return;
    } else if (e->mimeData()->hasUrls()) {
        QList<QUrl> urls = e->mimeData()->urls();
        if (!urls.isEmpty()) {
            QString path = urls.first().toLocalFile();
            if (!path.isEmpty()) {
                e->acceptProposedAction();
                emit loadFileRequested(deckIndex_, path);
            }
        }
        return;
    }
    QWidget::dropEvent(e);
}

bool DeckStrip::eventFilter(QObject* obj, QEvent* event)
{
    // Pitch fader: double-click resets to 0 (center)
    if (obj == pitchFader_ && event->type() == QEvent::MouseButtonDblClick) {
        pitchFader_->setValue(0);
        std::fprintf(stderr, "CONTROL_RESET deck=%d param=PITCH\n", deckIndex_);
        std::fflush(stderr);
        return true;
    }
    return QWidget::eventFilter(obj, event);
}

void DeckStrip::applyMixerDensity()
{
    // Horizontal knob strip — density modes adjust knob size
    const int knobSz = (mixerDensityMode_ == 0) ? 30 : 34;
    const int hitPad = 3;
    const int widgetSz = knobSz + hitPad * 2;

    for (auto* knob : { gainKnob_, reverbKnob_, flangerKnob_, filterKnob_ }) {
        if (knob) {
            knob->setFixedSize(widgetSz, widgetSz);
            knob->setMinimumSize(widgetSz, widgetSz);
            knob->setMaximumSize(widgetSz, widgetSz);
            knob->setHitPadding(static_cast<double>(hitPad));
        }
    }
    if (deckFxBtn_) deckFxBtn_->setFixedSize(widgetSz, widgetSz);
}

void DeckStrip::loadTrack(const QString& filePath)
{
    bridge_->loadTrackToDeck(deckIndex_, filePath);
}

void DeckStrip::updateAnalysisPanel(const QJsonObject& panel)
{
    if (analysisDash_)
        analysisDash_->updatePanel(panel);
}

void DeckStrip::toggleStemOverlay()
{
    if (!kEnableStemOverlay) return;   // feature disabled at compile time
    auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
    const bool next = !wf->stemOverlayEnabled();
    wf->setStemOverlayEnabled(next);
    std::fprintf(stderr, "STEM_OVERLAY_TOGGLE deck=%d enabled=%d\n",
                 deckIndex_, next ? 1 : 0);
    std::fflush(stderr);
}

void DeckStrip::cycleDebugBandSolo()
{
    if (!kEnableStemOverlay) return;
    auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
    wf->cycleDebugBandSolo();
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

                // ── Stem overlay: band-energy analysis ──
                if (kEnableStemOverlay) {
                    auto bt0 = std::chrono::steady_clock::now();
                    auto bandData = bridge_->getBandEnergyOverview(deckIndex_, 2048);
                    auto bt1 = std::chrono::steady_clock::now();
                    const double bandMs = std::chrono::duration<double, std::milli>(bt1 - bt0).count();
                    if (!bandData.empty()) {
                        wf->setBandEnergyData(bandData);
                        std::fprintf(stderr,
                            "STEM_OVERLAY_ANALYSIS_END deck=%d bands=%zu genMs=%.1f path=%s\n",
                            deckIndex_, bandData.size(), bandMs,
                            waveformTrackPath_.toUtf8().constData());
                        std::fflush(stderr);
                    }
                }
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
            wf->setTrackDuration(static_cast<float>(dur));
            waveformCtrl_.updatePlayhead(ph, dur);

            // Push controller state into renderer
            wf->setViewState(waveformCtrl_.state());
            wf->setViewportAnchor(static_cast<float>(waveformCtrl_.viewportAnchor()));
            wf->setCueFocusTarget(static_cast<float>(waveformCtrl_.cueFocusTarget()));
        }

        // ── Waveform state: transport edge detection ──
        if (playing && !prevPlaying_) {
            const char* oldState = waveViewStateName(waveformCtrl_.state());
            waveformCtrl_.onPlay();   // → LIVE_SCROLL (always)
            auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::fprintf(stderr,
                "WAVE_STYLE_PLAY_TRANSITION deck=%d old=%s new=%s "
                "styleChange=NONE tid=%zu\n",
                deckIndex_, oldState, waveViewStateName(waveformCtrl_.state()), tid);
            std::fflush(stderr);
        } else if (!playing && prevPlaying_) {
            const char* oldState = waveViewStateName(waveformCtrl_.state());
            // Distinguish stop (playhead ≈ 0) from pause
            if (ph < 0.05) {
                waveformCtrl_.onStop();   // → OVERVIEW
            } else {
                waveformCtrl_.onPause();  // hold current view
            }
            auto tid = static_cast<size_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()));
            std::fprintf(stderr,
                "WAVE_STYLE_PLAY_TRANSITION deck=%d old=%s new=%s "
                "styleChange=NONE tid=%zu\n",
                deckIndex_, oldState, waveViewStateName(waveformCtrl_.state()), tid);
            std::fflush(stderr);
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

// ═══════════════════════════════════════════════════════════════════
// Fast playhead tick (~60fps) — lightweight position-only update.
// Heavy state (meters, labels, waveform fetch) stays in the 250ms poll.
// ═══════════════════════════════════════════════════════════════════
void DeckStrip::tickPlayhead()
{
    const double ph  = bridge_->deckPlayhead(deckIndex_);
    const double dur = bridge_->deckDuration(deckIndex_);
    if (dur <= 0.0 || !trackLoaded_) return;

    emit playheadMoved(deckIndex_, ph);

    auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
    wf->setPlayheadFraction(static_cast<float>(ph / dur));
    wf->setTrackDuration(static_cast<float>(dur));
    waveformCtrl_.updatePlayhead(ph, dur);

    wf->setViewState(waveformCtrl_.state());
    wf->setViewportAnchor(static_cast<float>(waveformCtrl_.viewportAnchor()));
    wf->setCueFocusTarget(static_cast<float>(waveformCtrl_.cueFocusTarget()));
}

void DeckStrip::setTrackMetadata(const QString& title, const QString& artist,
                                 const QString& bpm, const QString& key,
                                 const QString& duration)
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
    if (!artist.isEmpty()) {
        trackArtistLabel_->setText(artist);
        trackArtistLabel_->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; border: none;"));
    }
    if (!duration.isEmpty()) {
        infoDurationLabel_->setText(duration);
        infoDurationLabel_->setStyleSheet(QStringLiteral(
            "color: #999; background: transparent; border: none;"));
    }

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

    // Immediately populate the analysis dashboard with available metadata
    if (analysisDash_)
        analysisDash_->showTrackLoading(title, artist, bpm, key);
}

void DeckStrip::setAlbumArt(const QPixmap& art)
{
    if (jogWheel_) {
        static_cast<JogWheel*>(jogWheel_)->setAlbumArt(art);
    }
}

QString DeckStrip::formatTime(double seconds)
{
    if (seconds < 0.0 || !std::isfinite(seconds)) return QStringLiteral("0:00");
    const int totalSec = static_cast<int>(seconds);
    const int m = totalSec / 60;
    const int s = totalSec % 60;
    return QStringLiteral("%1:%2").arg(m).arg(s, 2, 10, QLatin1Char('0'));
}
