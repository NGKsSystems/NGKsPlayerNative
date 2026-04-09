#pragma once
/*  AnalysisDashboardWidget  —  Read-only Qt dashboard panel that mirrors
    the Python AnalysisDashboardPanel zone layout.

    Zones:
    ┌─────────────────── HEADER ────────────────────┐
    │  [State Icon]  Track ID       Duration/State   │
    ├──────────┬──────────────┬──────────────────────┤
    │  LEFT    │   CENTER     │   RIGHT              │
    │ GlobalBPM│  Live BPM    │  Current Key         │
    │ Sections │  Section     │  Global Key          │
    │ Cues     │  BPM Conf    │  Key Conf            │
    ├──────────┴──────────────┴──────────────────────┤
    │               BOTTOM STRIP                     │
    │  Section range │ State │ Review                │
    └───────────────────────────────────────────────-┘

    Usage:
      auto* dash = new AnalysisDashboardWidget(parent);
      dash->updatePanel(jsonObject);  // from AnalysisBridge
*/

#include <QFrame>
#include <QLabel>
#include <QGridLayout>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QJsonObject>
#include <QString>
#include <QFont>
#include <QSizePolicy>
#include <algorithm>

class AnalysisDashboardWidget : public QFrame
{
    Q_OBJECT

public:
    explicit AnalysisDashboardWidget(QWidget* parent = nullptr)
        : QFrame(parent)
    {
        setObjectName(QStringLiteral("analysisDashboard"));
        setFrameShape(QFrame::StyledPanel);
        setStyleSheet(QStringLiteral(
            "QFrame#analysisDashboard {"
            "  background: transparent;"
            "  border: none;"
            "  padding: 4px;"
            "}"
        ));
        setFrameShape(QFrame::NoFrame);
        setMinimumHeight(140);
        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);

        auto* root = new QVBoxLayout(this);
        root->setContentsMargins(8, 6, 8, 6);
        root->setSpacing(4);

        // ── HEADER ──
        auto* headerRow = new QHBoxLayout;
        headerRow->setSpacing(8);
        stateIcon_ = makeLabel(QStringLiteral("-"), 12, true, QStringLiteral("#888"));
        trackId_   = makeLabel(QStringLiteral("No Track"), 11, false, QStringLiteral("#ccc"));
        trackId_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        trackId_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
        statusText_ = makeLabel(QString(), 10, false, QStringLiteral("#888"));
        statusText_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);
        headerRow->addWidget(stateIcon_);
        headerRow->addWidget(trackId_);
        headerRow->addWidget(statusText_);
        root->addLayout(headerRow);

        // ── 3-column body ──
        auto* body = new QHBoxLayout;
        body->setSpacing(12);

        // LEFT column
        auto* leftCol = new QVBoxLayout;
        leftCol->setSpacing(2);
        leftTitle_ = makeLabel(QStringLiteral("GLOBAL"), 8, true, QStringLiteral("#888"));
        globalBpm_ = makeLabel(QStringLiteral("-- BPM"), 13, true, QStringLiteral("#66ccaa"));
        globalKey_ = makeLabel(QStringLiteral("--"), 11, false, QStringLiteral("#66ccaa"));
        sectionCount_ = makeLabel(QStringLiteral("Sections: --"), 9, false, QStringLiteral("#999"));
        cueCount_     = makeLabel(QStringLiteral("Cues: --"), 9, false, QStringLiteral("#999"));
        leftCol->addWidget(leftTitle_);
        leftCol->addWidget(globalBpm_);
        leftCol->addWidget(globalKey_);
        leftCol->addWidget(sectionCount_);
        leftCol->addWidget(cueCount_);
        leftCol->addStretch();

        // CENTER column
        auto* centerCol = new QVBoxLayout;
        centerCol->setSpacing(2);
        centerTitle_ = makeLabel(QStringLiteral("LIVE"), 8, true, QStringLiteral("#888"));
        liveBpm_     = makeLabel(QStringLiteral("-- BPM"), 14, true, QStringLiteral("#55ffaa"));
        liveBpmConf_ = makeLabel(QString(), 9, false, QStringLiteral("#888"));
        liveSection_ = makeLabel(QStringLiteral("Section: --"), 10, false, QStringLiteral("#aaa"));
        liveSectionRange_ = makeLabel(QString(), 9, false, QStringLiteral("#777"));
        centerCol->addWidget(centerTitle_);
        centerCol->addWidget(liveBpm_, 0, Qt::AlignRight);
        centerCol->addWidget(liveBpmConf_, 0, Qt::AlignRight);
        centerCol->addWidget(liveSection_, 0, Qt::AlignRight);
        centerCol->addWidget(liveSectionRange_, 0, Qt::AlignRight);
        centerCol->addStretch();

        // RIGHT column
        auto* rightCol = new QVBoxLayout;
        rightCol->setSpacing(2);
        rightTitle_ = makeLabel(QStringLiteral("KEY"), 8, true, QStringLiteral("#888"));
        liveKey_     = makeLabel(QStringLiteral("--"), 14, true, QStringLiteral("#66ccaa"));
        liveKeyConf_ = makeLabel(QString(), 9, false, QStringLiteral("#888"));
        globalKeyLabel_ = makeLabel(QStringLiteral("Global: --"), 10, false, QStringLiteral("#999"));
        readoutState_   = makeLabel(QString(), 9, false, QStringLiteral("#777"));
        rightCol->addWidget(rightTitle_);
        rightCol->addWidget(liveKey_);
        rightCol->addWidget(liveKeyConf_);
        rightCol->addWidget(globalKeyLabel_);
        rightCol->addWidget(readoutState_);
        rightCol->addStretch();

        body->addLayout(leftCol, 0);

        body->addStretch(1);

        body->addLayout(centerCol, 0);

        body->addLayout(rightCol, 0);

        root->addLayout(body);

        // ── BOTTOM strip ──
        auto* bottomRow = new QHBoxLayout;
        bottomRow->setSpacing(12);
        analysisState_ = makeLabel(QString(), 9, false, QStringLiteral("#888"));
        analysisState_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        analysisState_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Preferred);
        reviewLabel_ = makeLabel(QString(), 9, true, QStringLiteral("#ff8844"));
        reviewLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);
        bottomRow->addWidget(analysisState_);
        bottomRow->addWidget(reviewLabel_);
        root->addLayout(bottomRow);

        // ── Progress overlay (visible during RUNNING/QUEUED) ──
        progressLabel_ = makeLabel(QString(), 14, true, QStringLiteral("#ffcc44"));
        progressLabel_->setAlignment(Qt::AlignCenter);
        progressLabel_->hide();
        root->addWidget(progressLabel_);

        // ── Error overlay ──
        errorLabel_ = makeLabel(QString(), 11, false, QStringLiteral("#e94560"));
        errorLabel_->setAlignment(Qt::AlignCenter);
        errorLabel_->setWordWrap(true);
        errorLabel_->hide();
        root->addWidget(errorLabel_);

        // Initial state
        showNoTrack();
    }

    // ── Public update method ──
    void updatePanel(const QJsonObject& panel)
    {
        lastPanel_ = panel;
        const QString st = panel.value(QStringLiteral("state")).toString();

        if (st == QStringLiteral("NO_TRACK")) {
            showNoTrack();
            return;
        }
        if (st == QStringLiteral("ANALYSIS_QUEUED")) {
            showProgress(QStringLiteral("Analysis queued..."), QString());
            return;
        }
        if (st == QStringLiteral("ANALYSIS_RUNNING")) {
            const QString pt = panel.value(QStringLiteral("progress_text")).toString();
            const QString statusTxt = panel.value(QStringLiteral("status_text")).toString();
            const QString title = statusTxt.isEmpty() ? QStringLiteral("Analyzing...") : statusTxt;
            showProgress(title, pt);
            return;
        }
        if (st == QStringLiteral("ANALYSIS_FAILED")) {
            const QString err = panel.value(QStringLiteral("error_text")).toString();
            showError(err);
            return;
        }
        if (st == QStringLiteral("ANALYSIS_CANCELED")) {
            showError(QStringLiteral("Analysis canceled"));
            return;
        }

        // ANALYSIS_COMPLETE (or fallback) — full dashboard
        showComplete(panel);
    }

    const QJsonObject& lastPanel() const { return lastPanel_; }

    void setLiveVisible(bool v)
    {
        liveVisible_ = v;
        centerTitle_->setVisible(v);
        liveBpm_->setVisible(v);
        liveBpmConf_->setVisible(v);
        liveSection_->setVisible(v);
        liveSectionRange_->setVisible(v);
        rightTitle_->setVisible(v);
        liveKey_->setVisible(v);
        liveKeyConf_->setVisible(v);
        globalKeyLabel_->setVisible(v);
        readoutState_->setVisible(v);
    }

    bool liveVisible() const { return liveVisible_; }

    /// Show track metadata immediately while analysis runs.
    /// GLOBAL column populated from ID3 tags; LIVE and KEY show "Analyzing..."
    void showTrackLoading(const QString& title, const QString& artist,
                          const QString& bpm, const QString& key,
                          const QString& duration = QString())
    {
        progressLabel_->hide();
        errorLabel_->hide();
        setBodyVisible(true);

        // Header
        QString headerText = title;
        if (!artist.isEmpty())
            headerText = QStringLiteral("%1 - %2").arg(artist, title);
        stateIcon_->setText(QStringLiteral("~"));
        stateIcon_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));
        trackId_->setText(headerText.isEmpty() ? QStringLiteral("Loading...") : headerText);
        trackId_->setStyleSheet(QStringLiteral("color: #ccc; background: transparent; border: none;"));
        statusText_->setText(duration.isEmpty() ? QStringLiteral("LOADING") : duration);
        statusText_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));

        // GLOBAL column — from metadata
        globalBpm_->setText(bpm.isEmpty() ? QStringLiteral("-- BPM")
                                          : QStringLiteral("%1 BPM").arg(bpm));
        globalKey_->setText(key.isEmpty() ? QStringLiteral("--") : key);
        sectionCount_->setText(QStringLiteral("Sections: ..."));
        cueCount_->setText(QStringLiteral("Cues: ..."));

        // LIVE column — analyzing
        liveBpm_->setText(QStringLiteral("..."));
        liveBpm_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));
        liveBpmConf_->setText(QStringLiteral("Analyzing..."));
        liveSection_->setText(QStringLiteral("Section: --"));
        liveSectionRange_->clear();

        // KEY column — analyzing
        liveKey_->setText(QStringLiteral("..."));
        liveKey_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));
        liveKeyConf_->setText(QStringLiteral("Analyzing..."));
        globalKeyLabel_->setText(key.isEmpty() ? QStringLiteral("Global: --")
                                               : QStringLiteral("Global: %1").arg(key));
        readoutState_->clear();

        // Bottom
        analysisState_->setText(QStringLiteral("Waiting for analysis..."));
        reviewLabel_->clear();
    }

signals:

private:
    QJsonObject lastPanel_;
    bool liveVisible_{true};

    // Header
    QLabel* stateIcon_{};
    QLabel* trackId_{};
    QLabel* statusText_{};

    // Left
    QLabel* leftTitle_{};
    QLabel* globalBpm_{};
    QLabel* globalKey_{};
    QLabel* sectionCount_{};
    QLabel* cueCount_{};

    // Center
    QLabel* centerTitle_{};
    QLabel* liveBpm_{};
    QLabel* liveBpmConf_{};
    QLabel* liveSection_{};
    QLabel* liveSectionRange_{};

    // Right
    QLabel* rightTitle_{};
    QLabel* liveKey_{};
    QLabel* liveKeyConf_{};
    QLabel* globalKeyLabel_{};
    QLabel* readoutState_{};

    // Bottom
    QLabel* analysisState_{};
    QLabel* reviewLabel_{};

    // Overlays
    QLabel* progressLabel_{};
    QLabel* errorLabel_{};

    // ── Helpers ──

    QLabel* makeLabel(const QString& text, int ptSize, bool bold, const QString& color)
    {
        auto* lbl = new QLabel(text, this);
        QFont f = lbl->font();
        f.setPointSize(ptSize);
        f.setBold(bold);
        lbl->setFont(f);
        lbl->setStyleSheet(QStringLiteral("color: %1; background: transparent; border: none;").arg(color));
        return lbl;
    }

    static QString confBar(double c)
    {
        if (c >= 0.75) return QStringLiteral("=== HIGH");
        if (c >= 0.50) return QStringLiteral("==- MED");
        return QStringLiteral("=-- LOW");
    }

    static QString confColor(double c)
    {
        if (c >= 0.75) return QStringLiteral("#55ffaa");
        if (c >= 0.50) return QStringLiteral("#ffcc44");
        return QStringLiteral("#ff8844");
    }

    void setBodyVisible(bool v)
    {
        // Left
        leftTitle_->setVisible(v);
        globalBpm_->setVisible(v);
        globalKey_->setVisible(v);
        sectionCount_->setVisible(v);
        cueCount_->setVisible(v);
        // Center — respect liveVisible_ toggle
        const bool lv = v && liveVisible_;
        centerTitle_->setVisible(lv);
        liveBpm_->setVisible(lv);
        liveBpmConf_->setVisible(lv);
        liveSection_->setVisible(lv);
        liveSectionRange_->setVisible(lv);
        // Right — respect liveVisible_ toggle
        rightTitle_->setVisible(lv);
        liveKey_->setVisible(lv);
        liveKeyConf_->setVisible(lv);
        globalKeyLabel_->setVisible(lv);
        readoutState_->setVisible(lv);
        // Bottom
        analysisState_->setVisible(v);
        reviewLabel_->setVisible(v);
    }

    void showNoTrack()
    {
        stateIcon_->setText(QStringLiteral("-"));
        stateIcon_->setStyleSheet(QStringLiteral("color: #555; background: transparent; border: none;"));
        trackId_->setText(QStringLiteral("No Track"));
        trackId_->setStyleSheet(QStringLiteral("color: #666; background: transparent; border: none;"));
        statusText_->clear();
        setBodyVisible(false);
        progressLabel_->hide();
        errorLabel_->hide();
    }

    void showProgress(const QString& title, const QString& detail)
    {
        stateIcon_->setText(QStringLiteral("~"));
        stateIcon_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));
        statusText_->setText(title);
        statusText_->setStyleSheet(QStringLiteral("color: #ffcc44; background: transparent; border: none;"));
        setBodyVisible(false);
        errorLabel_->hide();
        if (!detail.isEmpty()) {
            progressLabel_->setText(detail);
            progressLabel_->show();
        } else {
            progressLabel_->hide();
        }
    }

    void showError(const QString& err)
    {
        stateIcon_->setText(QStringLiteral("X"));
        stateIcon_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent; border: none;"));
        statusText_->setText(QStringLiteral("FAILED"));
        statusText_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent; border: none;"));
        setBodyVisible(false);
        progressLabel_->hide();
        errorLabel_->setText(err.left(120));
        errorLabel_->show();
    }

    void showComplete(const QJsonObject& p)
    {
        progressLabel_->hide();
        errorLabel_->hide();
        setBodyVisible(true);

        // ── Header ──
        stateIcon_->setText(QStringLiteral("*"));
        stateIcon_->setStyleSheet(QStringLiteral("color: #55ffaa; background: transparent; border: none;"));
        const QString tid = p.value(QStringLiteral("track_id")).toString();
        trackId_->setText(tid.isEmpty() ? QStringLiteral("Track") : tid);
        trackId_->setStyleSheet(QStringLiteral("color: #ccc; background: transparent; border: none;"));
        const QString dur = p.value(QStringLiteral("duration_text")).toString();
        statusText_->setText(dur.isEmpty() ? QStringLiteral("COMPLETE") : dur);
        statusText_->setStyleSheet(QStringLiteral("color: #55ffaa; background: transparent; border: none;"));

        // ── Left — Global ──
        const QString gBpm = p.value(QStringLiteral("bpm_text")).toString();
        globalBpm_->setText(gBpm.isEmpty() ? QStringLiteral("-- BPM") : gBpm);

        const QString gKey = p.value(QStringLiteral("key_text")).toString();
        globalKey_->setText(gKey.isEmpty() ? QStringLiteral("--") : gKey);

        const int sc = p.value(QStringLiteral("section_count")).toInt();
        sectionCount_->setText(QStringLiteral("Sections: %1").arg(sc));

        const int cc = p.value(QStringLiteral("cue_count")).toInt();
        cueCount_->setText(QStringLiteral("Cues: %1").arg(cc));

        // ── Center — Live BPM ──
        const QString lb = p.value(QStringLiteral("live_bpm_text")).toString();
        const double bConf = p.value(QStringLiteral("live_bpm_confidence")).toDouble();
        liveBpm_->setText(lb.isEmpty() ? gBpm : lb);
        liveBpm_->setStyleSheet(QStringLiteral("color: %1; background: transparent; border: none;")
                                    .arg(confColor(bConf > 0 ? bConf : 0.75)));
        liveBpmConf_->setText(bConf > 0 ? QStringLiteral("BPM Conf: %1").arg(confBar(bConf)) : QString());

        const QString secLbl = p.value(QStringLiteral("live_section_label")).toString();
        liveSection_->setText(secLbl.isEmpty()
            ? QStringLiteral("Section: --")
            : QStringLiteral("Section: %1").arg(secLbl));
        const QString secRange = p.value(QStringLiteral("live_section_time_range")).toString();
        liveSectionRange_->setText(secRange);

        // ── Right — Key ──
        const QString lk = p.value(QStringLiteral("live_key_text")).toString();
        const double kConf = p.value(QStringLiteral("live_key_confidence")).toDouble();
        liveKey_->setText(lk.isEmpty() ? gKey : lk);
        liveKey_->setStyleSheet(QStringLiteral("color: %1; background: transparent; border: none;")
                                    .arg(confColor(kConf > 0 ? kConf : 0.75)));
        liveKeyConf_->setText(kConf > 0 ? QStringLiteral("Key Conf: %1").arg(confBar(kConf)) : QString());

        globalKeyLabel_->setText(gKey.isEmpty() ? QStringLiteral("Global: --")
                                                : QStringLiteral("Global: %1").arg(gKey));
        const QString rdSt = p.value(QStringLiteral("live_readout_state")).toString();
        readoutState_->setText(rdSt.isEmpty() ? QString() : QStringLiteral("Readout: %1").arg(rdSt));

        // ── Bottom strip ──
        const QString confTxt = p.value(QStringLiteral("confidence_text")).toString();
        const QString procTime = p.value(QStringLiteral("processing_time_text")).toString();
        QStringList bottom;
        if (!confTxt.isEmpty()) bottom << confTxt;
        if (!procTime.isEmpty()) bottom << QStringLiteral("Time: %1").arg(procTime);
        analysisState_->setText(bottom.join(QStringLiteral("  |  ")));

        const bool review = p.value(QStringLiteral("review_required")).toBool();
        if (review) {
            const QString reason = p.value(QStringLiteral("review_reason")).toString();
            reviewLabel_->setText(QStringLiteral("! REVIEW: %1").arg(reason.left(40)));
            reviewLabel_->show();
        } else {
            reviewLabel_->clear();
        }
    }
};
