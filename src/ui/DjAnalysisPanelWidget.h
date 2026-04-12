#pragma once
/*  DjAnalysisPanelWidget  —  Instrument-panel analysis strip for DJ mode.
    Pixel-locked 96px strip: KEY (25%) | BPM (50%) | SECTION (25%).
    Baseline grid: center ±18px for secondary rows.
    No cards, no boxes, no pills — structure only.                            */

#include <QWidget>
#include <QPainter>
#include <QPainterPath>
#include <QJsonObject>
#include <QFont>
#include <QString>
#include <algorithm>
#include <cmath>

class DjAnalysisPanelWidget : public QWidget
{

public:
    explicit DjAnalysisPanelWidget(const QColor& /*accent*/, QWidget* parent = nullptr)
        : QWidget(parent)
    {
        setObjectName(QStringLiteral("djAnalysisPanel"));
        setMinimumHeight(32);
        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);
        setAttribute(Qt::WA_OpaquePaintEvent, true);
    }

    void updatePanel(const QJsonObject& panel)
    {
        lastPanel_ = panel;
        const QString st = panel.value(QStringLiteral("state")).toString();

        if (st == QStringLiteral("NO_TRACK")) {
            state_ = State::NoTrack;
            update();
            return;
        }
        if (st == QStringLiteral("ANALYSIS_QUEUED") || st == QStringLiteral("ANALYSIS_RUNNING")) {
            state_ = State::Running;
            progressText_ = panel.value(QStringLiteral("progress_text")).toString();
            statusText_ = panel.value(QStringLiteral("status_text")).toString();
            update();
            return;
        }
        if (st == QStringLiteral("ANALYSIS_FAILED") || st == QStringLiteral("ANALYSIS_CANCELED")) {
            state_ = State::Error;
            errorText_ = panel.value(QStringLiteral("error_text")).toString();
            if (errorText_.isEmpty()) errorText_ = QStringLiteral("Analysis failed");
            update();
            return;
        }

        // ANALYSIS_COMPLETE
        state_ = State::Complete;

        globalBpm_ = panel.value(QStringLiteral("bpm_text")).toString();
        liveBpm_ = panel.value(QStringLiteral("live_bpm_text")).toString();
        if (liveBpm_.isEmpty()) liveBpm_ = globalBpm_;
        bpmConf_ = panel.value(QStringLiteral("live_bpm_confidence")).toDouble();
        if (bpmConf_ <= 0.0) bpmConf_ = 0.75;

        globalKey_ = panel.value(QStringLiteral("key_text")).toString();
        liveKey_ = panel.value(QStringLiteral("live_key_text")).toString();
        if (liveKey_.isEmpty()) liveKey_ = globalKey_;
        keyConf_ = panel.value(QStringLiteral("live_key_confidence")).toDouble();
        if (keyConf_ <= 0.0) keyConf_ = 0.75;

        sectionLabel_ = panel.value(QStringLiteral("live_section_label")).toString();
        sectionRange_ = panel.value(QStringLiteral("live_section_time_range")).toString();
        sectionCount_ = panel.value(QStringLiteral("section_count")).toInt();

        update();
    }

    void showTrackLoading(const QString& title, const QString& artist,
                          const QString& bpm, const QString& key,
                          const QString& /*duration*/ = QString())
    {
        state_ = State::Loading;
        globalBpm_ = bpm.isEmpty() ? QStringLiteral("---") : bpm;
        liveBpm_ = globalBpm_;
        bpmConf_ = 0.0;
        globalKey_ = key.isEmpty() ? QStringLiteral("--") : key;
        liveKey_ = globalKey_;
        keyConf_ = 0.0;
        sectionLabel_.clear();
        sectionRange_.clear();
        sectionCount_ = 0;
        (void)title; (void)artist;
        update();
    }

    const QJsonObject& lastPanel() const { return lastPanel_; }

protected:
    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);
        p.setRenderHint(QPainter::TextAntialiasing);

        const QRect r = rect();

        // Background: #0B0F14 with subtle vertical gradient
        {
            QLinearGradient bg(0, 0, 0, r.height());
            bg.setColorAt(0.0, QColor(0x0B, 0x0F, 0x14));
            bg.setColorAt(1.0, QColor(0x0E, 0x14, 0x1B));
            p.fillRect(r, bg);
        }

        switch (state_) {
        case State::NoTrack:   paintNoTrack(p, r);       break;
        case State::Loading:   paintStrip(p, r, true);   break;
        case State::Running:   paintRunning(p, r);       break;
        case State::Error:     paintError(p, r);         break;
        case State::Complete:  paintStrip(p, r, false);  break;
        }
    }

private:
    enum class State { NoTrack, Loading, Running, Error, Complete };

    QJsonObject lastPanel_;
    State state_{State::NoTrack};

    QString globalBpm_, liveBpm_;
    double bpmConf_{0.0};
    QString globalKey_, liveKey_;
    double keyConf_{0.0};
    QString sectionLabel_, sectionRange_;
    int sectionCount_{0};
    QString progressText_, statusText_, errorText_;

    // ── Locked color system ──
    static constexpr struct {
        uint32_t neon      = 0x3BFFB6;   // primary neon
        uint32_t secondary = 0x8FA3B8;   // secondary text
        uint32_t muted     = 0x6B7C8F;   // muted labels
        uint32_t bg        = 0x0B0F14;   // background
        uint32_t sepLine   = 0x788CA0;   // separator base (used at ~15% alpha)
    } kColors{};

    static QColor neon()       { return QColor(0x3B, 0xFF, 0xB6); }
    static QColor secondary()  { return QColor(0x8F, 0xA3, 0xB8); }
    static QColor muted()      { return QColor(0x6B, 0x7C, 0x8F); }

    static QColor confColor(double c) {
        if (c >= 0.75) return neon();
        if (c >= 0.50) return QColor(0xFF, 0xCC, 0x44);
        return QColor(0xFF, 0x88, 0x44);
    }

    // ── No Track ──
    void paintNoTrack(QPainter& p, const QRect& r) const
    {
        QFont f = font();
        f.setPixelSize(10);
        f.setBold(false);
        f.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
        p.setFont(f);
        p.setPen(muted());
        p.drawText(r, Qt::AlignCenter, QStringLiteral("NO TRACK"));
    }

    // ── Analyzing ──
    void paintRunning(QPainter& p, const QRect& r) const
    {
        QFont f = font();
        f.setPixelSize(12);
        f.setBold(true);
        f.setLetterSpacing(QFont::AbsoluteSpacing, 2.0);
        p.setFont(f);
        p.setPen(QColor(0xFF, 0xCC, 0x44));
        const QString txt = statusText_.isEmpty() ? QStringLiteral("ANALYZING") : statusText_.toUpper();
        p.drawText(r.adjusted(16, 0, -16, -12), Qt::AlignLeft | Qt::AlignVCenter, txt);

        if (!progressText_.isEmpty()) {
            f.setPixelSize(10);
            f.setBold(false);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 0);
            p.setFont(f);
            p.setPen(secondary());
            p.drawText(r.adjusted(16, 12, -16, 0), Qt::AlignLeft | Qt::AlignVCenter, progressText_);
        }
    }

    // ── Error ──
    void paintError(QPainter& p, const QRect& r) const
    {
        QFont f = font();
        f.setPixelSize(11);
        f.setBold(true);
        f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
        p.setFont(f);
        p.setPen(QColor(0xE9, 0x45, 0x60));
        p.drawText(r.adjusted(16, 0, -16, 0), Qt::AlignLeft | Qt::AlignVCenter,
                    errorText_.left(60).toUpper());
    }

    // ════════════════════════════════════════════════════════════════
    //  INSTRUMENT STRIP — pixel-locked 3-zone grid
    //  LEFT 25% (KEY) | CENTER 50% (BPM) | RIGHT 25% (SECTION)
    // ════════════════════════════════════════════════════════════════
    void paintStrip(QPainter& p, const QRect& r, bool loading) const
    {
        const int H = r.height();           // 96px
        const int W = r.width();
        const int padL = 16;
        const int padR = 16;

        // ── 3-zone grid: 25% | 50% | 25% — LOCKED ──
        const int leftW   = W / 4;
        const int centerW = W / 2;
        const int rightX  = leftW + centerW;
        const int rightW  = W - rightX;

        // ── Baseline grid ──
        const int cy       = H / 2;        // center line
        const int topRow   = cy - 18;       // secondary top
        const int botRow   = cy + 18;       // secondary bottom
        const int labelY   = cy - 22;       // zone label baseline (above primary)

        // ── BPM inner glow — tight radial, crisp edge ──
        const QColor bpmCol = loading ? QColor(0xFF, 0xCC, 0x44) : confColor(bpmConf_);
        {
            const qreal cx = leftW + centerW / 2.0;
            QRadialGradient rg(cx, cy, std::min(centerW * 0.22, 52.0));
            rg.setColorAt(0.0, QColor(bpmCol.red(), bpmCol.green(), bpmCol.blue(), 30));
            rg.setColorAt(0.7, QColor(bpmCol.red(), bpmCol.green(), bpmCol.blue(), 10));
            rg.setColorAt(1.0, Qt::transparent);
            p.setPen(Qt::NoPen);
            p.setBrush(rg);
            p.drawRect(QRect(leftW, 0, centerW, H));
        }

        // ── Vertical separators: 1px, 50% height, very low opacity — guide not divide ──
        {
            const int sepH = static_cast<int>(H * 0.5);
            const int sepTop = (H - sepH) / 2;
            p.setPen(QPen(QColor(120, 140, 160, 22), 1.0));
            p.drawLine(leftW, sepTop, leftW, sepTop + sepH);
            p.drawLine(rightX, sepTop, rightX, sepTop + sepH);
        }

        QFont f = font();

        // ════════ CENTER — BPM (dominant) ════════
        {
            // Label "BPM" — 10px, muted, 22px above center
            f.setPixelSize(10);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 2.0);
            p.setFont(f);
            p.setPen(muted());
            const QRect labelRect(leftW, labelY - 10, centerW, 14);
            p.drawText(labelRect, Qt::AlignHCenter | Qt::AlignBottom, QStringLiteral("BPM"));

            // ── Extract clean numeric BPM + detect state ──
            const QString rawBpm = liveBpm_.isEmpty() ? QStringLiteral("---") : liveBpm_;
            const bool isLive = rawBpm.contains(QStringLiteral("live"), Qt::CaseInsensitive);
            const bool isGlobal = rawBpm.contains(QStringLiteral("global"), Qt::CaseInsensitive);
            // Strip everything after digits/dot/dash: remove " BPM", "(global)", "(live)", etc.
            QString bpmNum = rawBpm;
            bpmNum.remove(QStringLiteral(" BPM"), Qt::CaseInsensitive);
            {
                const int paren = bpmNum.indexOf(QLatin1Char('('));
                if (paren >= 0) bpmNum = bpmNum.left(paren);
            }
            bpmNum = bpmNum.trimmed();

            // Value — 40px, weight 700, centered, numeric only
            f.setPixelSize(40);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
            p.setFont(f);
            p.setPen(bpmCol);
            const QRect valRect(leftW, cy - 22, centerW, 44);
            p.drawText(valRect, Qt::AlignCenter, bpmNum);

            // BPM underline anchor — subtle inset gradient, 40px wide, 1px
            {
                const qreal ulCx = leftW + centerW / 2.0;
                const qreal ulY  = cy + 16.0;
                const qreal ulHW = 20.0;
                QLinearGradient ulg(ulCx - ulHW, ulY, ulCx + ulHW, ulY);
                ulg.setColorAt(0.0, Qt::transparent);
                ulg.setColorAt(0.3, QColor(bpmCol.red(), bpmCol.green(), bpmCol.blue(), 50));
                ulg.setColorAt(0.7, QColor(bpmCol.red(), bpmCol.green(), bpmCol.blue(), 50));
                ulg.setColorAt(1.0, Qt::transparent);
                p.setPen(QPen(QBrush(ulg), 1.0));
                p.drawLine(QPointF(ulCx - ulHW, ulY), QPointF(ulCx + ulHW, ulY));
            }

            // ── State indicator — small label below underline ──
            if (!loading) {
                QString stateTag;
                QColor stateCol;
                if (bpmConf_ < 0.50) {
                    stateTag = QStringLiteral("LOW CONF");
                    stateCol = QColor(0xFF, 0x88, 0x44);
                } else if (isLive) {
                    stateTag = QStringLiteral("LIVE");
                    stateCol = neon();
                } else if (isGlobal || !liveBpm_.isEmpty()) {
                    stateTag = QStringLiteral("GLOBAL");
                    stateCol = QColor(0x55, 0x60, 0x72);
                }

                if (!stateTag.isEmpty()) {
                    // Dot — 3px, 20px below center
                    const qreal dotX = leftW + centerW / 2.0;
                    const qreal dotY = cy + 20.0;
                    p.setPen(Qt::NoPen);
                    p.setBrush(stateCol);
                    p.drawEllipse(QPointF(dotX, dotY), 1.5, 1.5);

                    // State text — 8px, dim, 26px below center
                    f.setPixelSize(8);
                    f.setBold(true);
                    f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
                    p.setFont(f);
                    p.setPen(QColor(stateCol.red(), stateCol.green(), stateCol.blue(), 140));
                    const QRect stRect(leftW, cy + 24, centerW, 12);
                    p.drawText(stRect, Qt::AlignHCenter | Qt::AlignTop, stateTag);
                }
            }
        }

        // ════════ LEFT — KEY (stacked: Camelot + musical) ════════
        {
            const QColor keyCol = loading ? QColor(0xFF, 0xCC, 0x44) : confColor(keyConf_);

            // Label "KEY" — 9px, muted, 22px above center
            f.setPixelSize(9);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 1.5);
            p.setFont(f);
            p.setPen(muted());
            const QRect keyLabelRect(padL, labelY - 9, leftW - padL, 12);
            p.drawText(keyLabelRect, Qt::AlignLeft | Qt::AlignBottom, QStringLiteral("KEY"));

            // Parse Camelot / musical key
            const QString keyVal = liveKey_.isEmpty() ? QStringLiteral("--") : liveKey_;
            const int pp = keyVal.indexOf(QLatin1Char('('));
            QString camelot = keyVal;
            QString musical;
            if (pp > 0) {
                camelot = keyVal.left(pp).trimmed();
                musical = keyVal.mid(pp + 1);
                musical.remove(QLatin1Char(')'));
                musical = musical.trimmed();
            }

            // Camelot — 20px, weight 600, -4px from center (tighter stack)
            f.setPixelSize(20);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 0.5);
            p.setFont(f);
            p.setPen(keyCol);
            const QRect camRect(padL, cy - 4 - 16, leftW - padL, 22);
            p.drawText(camRect, Qt::AlignLeft | Qt::AlignBottom, camelot);

            // Musical key — 11px, secondary, +8px from center (tighter gap)
            if (!musical.isEmpty()) {
                f.setPixelSize(11);
                f.setBold(false);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 0);
                p.setFont(f);
                p.setPen(secondary());
                const QRect musRect(padL, cy + 8 - 4, leftW - padL, 14);
                p.drawText(musRect, Qt::AlignLeft | Qt::AlignTop, musical);
            } else if (!globalKey_.isEmpty() && !loading) {
                f.setPixelSize(11);
                f.setBold(false);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 0);
                p.setFont(f);
                p.setPen(QColor(0x55, 0x60, 0x72));
                const QRect musRect(padL, cy + 8 - 4, leftW - padL, 14);
                p.drawText(musRect, Qt::AlignLeft | Qt::AlignTop, globalKey_);
            }
        }

        // ════════ RIGHT — SECTION (stacked: label + time) ════════
        {
            // Label "SECTION" — 9px, muted, 22px above center, right-aligned
            f.setPixelSize(9);
            f.setBold(true);
            f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
            p.setFont(f);
            p.setPen(muted());
            const QRect secLabelRect(rightX, labelY - 9, rightW - padR, 12);
            p.drawText(secLabelRect, Qt::AlignRight | Qt::AlignBottom, QStringLiteral("SECTION"));

            if (!sectionLabel_.isEmpty()) {
                // Section name — 13px, weight 700, bright neon, -6px from center
                f.setPixelSize(13);
                f.setBold(true);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 0.5);
                p.setFont(f);
                p.setPen(QColor(0x55, 0xFF, 0xC8));
                const QRect secRect(rightX, cy - 6 - 10, rightW - padR, 14);
                p.drawText(secRect, Qt::AlignRight | Qt::AlignBottom,
                            sectionLabel_.toUpper());

                // Time range — 10px, secondary, +14px from center
                if (!sectionRange_.isEmpty()) {
                    f.setPixelSize(10);
                    f.setBold(false);
                    f.setLetterSpacing(QFont::AbsoluteSpacing, 0);
                    p.setFont(f);
                    p.setPen(secondary());
                    const QRect timeRect(rightX, cy + 14 - 6, rightW - padR, 12);
                    p.drawText(timeRect, Qt::AlignRight | Qt::AlignTop, sectionRange_);
                }
            } else if (loading) {
                f.setPixelSize(10);
                f.setBold(false);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 1.0);
                p.setFont(f);
                p.setPen(QColor(0x55, 0x60, 0x72));
                const QRect waitRect(rightX, cy - 6, rightW - padR, 12);
                p.drawText(waitRect, Qt::AlignRight | Qt::AlignVCenter, QStringLiteral("\u2026"));
            } else {
                f.setPixelSize(10);
                f.setBold(false);
                f.setLetterSpacing(QFont::AbsoluteSpacing, 0);
                p.setFont(f);
                p.setPen(QColor(0x55, 0x60, 0x72));
                const QString txt = sectionCount_ > 0
                    ? QStringLiteral("%1 sect.").arg(sectionCount_)
                    : QStringLiteral("--");
                const QRect defRect(rightX, cy - 6, rightW - padR, 12);
                p.drawText(defRect, Qt::AlignRight | Qt::AlignVCenter, txt);
            }
        }
    }
};

