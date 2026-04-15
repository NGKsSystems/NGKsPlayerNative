#include "ui/PlayerPage.h"

#include <QApplication>
#include <QComboBox>
#include <QFrame>
#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>
#include <QLineEdit>
#include <QMenu>
#include <QMessageBox>
#include <QMetaObject>
#include <QPushButton>
#include <QScrollArea>
#include <QSignalBlocker>
#include <QSlider>
#include <QSplitter>
#include <QStackedLayout>
#include <QTimer>
#include <QThread>
#include <QVBoxLayout>
#include <QWidget>

#include <algorithm>
#include <random>

#include "ui/EngineBridge.h"
#include "ui/EqPanel.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/DjLibraryWidget.h"
#include "ui/audio/AudioProfileStore.h"
#include "ui/diagnostics/DiagnosticsDialog.h"
#include "ui/widgets/VisualizerWidget.h"

PlayerPage::PlayerPage(EngineBridge& bridge, DjLibraryDatabase& db, QWidget* parent)
    : QWidget(parent), bridge_(bridge), db_(db)
{

    setStyleSheet(QStringLiteral(
        "QWidget { background: #0a0e27; color: #e0e0e0; }"
        "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 6px; padding: 8px 16px; font-size: 13px; }"
        "QPushButton:hover { background: #1a1a2e; border-color: #533483; }"
        "QPushButton:pressed { background: #533483; }"
        "QPushButton:disabled { background: #0d1117; color: #555; border-color: #1a1a2e; }"
        "QSlider::groove:horizontal { background: #1a1a2e; height: 8px; border-radius: 4px; }"
        "QSlider::handle:horizontal { background: #e94560; width: 16px; height: 16px;"
        "  margin: -4px 0; border-radius: 8px; }"
        "QSlider::sub-page:horizontal { background: #e94560; border-radius: 4px; min-width: 0px; }"
        "QListWidget { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 8px; outline: none; }"
        "QListWidget::item { padding: 8px 12px; border-bottom: 1px solid #0f3460; }"
        "QListWidget::item:selected { background: #533483; color: #ffffff; }"
        "QListWidget::item:hover { background: #1a1a2e; }"
        "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 4px; padding: 4px 8px; }"
        "QScrollBar:vertical { background: #0a0e27; width: 8px; }"
        "QScrollBar::handle:vertical { background: #533483; border-radius: 4px; min-height: 20px; }"
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"
    ));

    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(24, 16, 24, 16);
    layout->setSpacing(0);

    // ═══════════════════════════════════════════════════
    // A. Header row: Back + Title + Audio Profile
    // ═══════════════════════════════════════════════════
    auto* headerRow = new QHBoxLayout();
    headerRow->setSpacing(10);
    auto* backBtn = new QPushButton(QStringLiteral("<< Library"), this);
    backBtn->setMinimumHeight(34);
    backBtn->setCursor(Qt::PointingHandCursor);
    backBtn->setToolTip(QStringLiteral("Return to the library browser"));

    auto* titleLabel = new QLabel(QStringLiteral("Simple Player"), this);
    {
        QFont f = titleLabel->font();
        f.setPointSize(16);
        f.setBold(true);
        titleLabel->setFont(f);
        titleLabel->setStyleSheet(QStringLiteral("color: #e94560;"));
    }
    headerRow->addWidget(backBtn);
    headerRow->addSpacing(8);
    headerRow->addWidget(titleLabel);
    headerRow->addStretch(1);

    auto* profileLabel = new QLabel(QStringLiteral("Profile:"), this);
    profileLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
    audioProfileCombo_ = new QComboBox(this);
    audioProfileCombo_->setMinimumWidth(180);
    refreshAudioProfilesButton_ = new QPushButton(QStringLiteral("Refresh"), this);
    applyAudioProfileButton_ = new QPushButton(QStringLiteral("Apply"), this);
    headerRow->addWidget(profileLabel);
    headerRow->addWidget(audioProfileCombo_);
    headerRow->addWidget(refreshAudioProfilesButton_);
    headerRow->addWidget(applyAudioProfileButton_);
    layout->addLayout(headerRow);

    QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
        bridge_.leaveSimpleMode();
        emit backRequested();
    });
    QObject::connect(refreshAudioProfilesButton_, &QPushButton::clicked, this, [this]() {
        requestAudioProfilesRefresh(true);
    });
    QObject::connect(applyAudioProfileButton_, &QPushButton::clicked, this, &PlayerPage::applySelectedAudioProfile);
    requestAudioProfilesRefresh(true);

    const QString akApplyAutorun = qEnvironmentVariable("NGKS_AK_AUTORUN_APPLY").trimmed().toLower();
    if (akApplyAutorun == QStringLiteral("1") || akApplyAutorun == QStringLiteral("true") || akApplyAutorun == QStringLiteral("yes")) {
        QTimer::singleShot(200, this, &PlayerPage::applySelectedAudioProfile);
    }

    layout->addSpacing(14);

    // ═══════════════════════════════════════════════════
    // B. Hero / Now Playing panel with Visualizer
    //    Visualizer is the BACKGROUND layer; text overlays on top
    // ═══════════════════════════════════════════════════
    auto* heroFrame = new QFrame(this);
    heroFrame->setStyleSheet(QStringLiteral(
        "QFrame#heroFrame { background: #0a0e27; border: 1px solid #0f3460; border-radius: 12px; }"));
    heroFrame->setObjectName(QStringLiteral("heroFrame"));
    heroFrame->setMinimumHeight(220);
    heroFrame->setMaximumHeight(280);

    // QStackedLayout::StackAll shows all children simultaneously, stacked
    auto* heroStack = new QStackedLayout(heroFrame);
    heroStack->setStackingMode(QStackedLayout::StackAll);
    heroStack->setContentsMargins(0, 0, 0, 0);

    // B1. BACKGROUND (index 0): Visualizer fills the entire hero frame
    visualizer_ = new VisualizerWidget(heroFrame);
    visualizer_->setMinimumHeight(220);
    heroStack->addWidget(visualizer_);

    // B2. FOREGROUND: Transparent overlay with text + controls
    auto* foreground = new QWidget(heroFrame);
    foreground->setStyleSheet(QStringLiteral("background: transparent;"));
    foreground->setAttribute(Qt::WA_TransparentForMouseEvents, false);
    auto* fgLayout = new QVBoxLayout(foreground);
    fgLayout->setContentsMargins(28, 16, 28, 0);
    fgLayout->setSpacing(4);

    nowPlayingTag_ = new QLabel(QStringLiteral("NOW PLAYING"), foreground);
    {
        QFont f = nowPlayingTag_->font();
        f.setPointSize(9);
        f.setBold(true);
        f.setLetterSpacing(QFont::AbsoluteSpacing, 3.0);
        nowPlayingTag_->setFont(f);
    }
    nowPlayingTag_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent;"));
    nowPlayingTag_->setAlignment(Qt::AlignCenter);
    fgLayout->addWidget(nowPlayingTag_);

    fgLayout->addSpacing(2);

    playerTrackLabel_ = new QLabel(QStringLiteral("No track loaded"), foreground);
    {
        QFont f = playerTrackLabel_->font();
        f.setPointSize(20);
        f.setBold(true);
        playerTrackLabel_->setFont(f);
    }
    playerTrackLabel_->setAlignment(Qt::AlignCenter);
    playerTrackLabel_->setWordWrap(true);
    playerTrackLabel_->setStyleSheet(QStringLiteral(
        "color: #ffffff; background: transparent; border: none; padding: 4px 8px;"));
    fgLayout->addWidget(playerTrackLabel_);

    playerArtistLabel_ = new QLabel(QString(), foreground);
    {
        QFont f = playerArtistLabel_->font();
        f.setPointSize(13);
        playerArtistLabel_->setFont(f);
    }
    playerArtistLabel_->setAlignment(Qt::AlignCenter);
    playerArtistLabel_->setStyleSheet(QStringLiteral(
        "color: #cccccc; background: transparent; border: none; padding: 2px 6px;"));
    fgLayout->addWidget(playerArtistLabel_);

    playerMetaLabel_ = new QLabel(QString(), foreground);
    {
        QFont f = playerMetaLabel_->font();
        f.setPointSize(10);
        playerMetaLabel_->setFont(f);
    }
    playerMetaLabel_->setAlignment(Qt::AlignCenter);
    playerMetaLabel_->setStyleSheet(QStringLiteral(
        "color: #999999; background: transparent; border: none; padding: 2px 6px;"));
    fgLayout->addWidget(playerMetaLabel_);

    playerStateLabel_ = new QLabel(QStringLiteral("Stopped"), foreground);
    {
        QFont f = playerStateLabel_->font();
        f.setPointSize(10);
        f.setBold(true);
        playerStateLabel_->setFont(f);
    }
    playerStateLabel_->setAlignment(Qt::AlignCenter);
    playerStateLabel_->setStyleSheet(QStringLiteral("color: #e94560; background: transparent;"));
    fgLayout->addWidget(playerStateLabel_);

    // Up Next label
    upNextLabel_ = new QLabel(QStringLiteral("Up Next: \u2014"), foreground);
    {
        QFont f = upNextLabel_->font();
        f.setPointSize(9);
        f.setItalic(true);
        upNextLabel_->setFont(f);
    }
    upNextLabel_->setAlignment(Qt::AlignCenter);
    upNextLabel_->setStyleSheet(QStringLiteral("color: #888888; background: transparent;"));
    fgLayout->addWidget(upNextLabel_);

    fgLayout->addStretch(1);

    // B3. Control strip at bottom of foreground: [Pulse | Tune] ——— [Line | Bars | Circle | None]
    auto* vizControlRow = new QHBoxLayout();
    vizControlRow->setContentsMargins(0, 0, 0, 10);
    vizControlRow->setSpacing(6);

    // Left: Pulse ON/OFF
    pulseBtn_ = new QPushButton(QStringLiteral("Pulse: ON"), foreground);
    pulseBtn_->setMinimumSize(90, 28);
    pulseBtn_->setCursor(Qt::PointingHandCursor);
    pulseBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
        "  color: #e94560; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
        "QPushButton:hover { background: rgba(31,74,112,220); }"));
    vizControlRow->addWidget(pulseBtn_);

    QObject::connect(pulseBtn_, &QPushButton::clicked, this, [this]() {
        visualizer_->setPulseEnabled(!visualizer_->pulseEnabled());
        pulseBtn_->setText(visualizer_->pulseEnabled()
            ? QStringLiteral("Pulse: ON") : QStringLiteral("Pulse: OFF"));
        pulseBtn_->setStyleSheet(visualizer_->pulseEnabled()
            ? QStringLiteral(
                "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
                "  color: #e94560; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
                "QPushButton:hover { background: rgba(31,74,112,220); }")
            : QStringLiteral(
                "QPushButton { background: rgba(22,33,62,200); border: 1px solid #0f3460; border-radius: 4px;"
                "  color: #666; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
                "QPushButton:hover { background: rgba(26,26,46,220); }"));
        qInfo().noquote() << QStringLiteral("VIZ_PULSE=%1").arg(visualizer_->pulseEnabled() ? "ON" : "OFF");
    });

    // Tune button (cycles levels 0–4)
    tuneBtn_ = new QPushButton(QStringLiteral("Tune: 2"), foreground);
    tuneBtn_->setMinimumSize(80, 28);
    tuneBtn_->setCursor(Qt::PointingHandCursor);
    tuneBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460; border-radius: 4px;"
        "  color: #aaccee; font-size: 10px; font-weight: bold; padding: 2px 8px; }"
        "QPushButton:hover { background: rgba(31,74,112,220); }"));
    vizControlRow->addWidget(tuneBtn_);

    QObject::connect(tuneBtn_, &QPushButton::clicked, this, [this]() {
        int next = (visualizer_->tuneLevel() + 1) % 5;
        visualizer_->setTuneLevel(next);
        tuneBtn_->setText(QStringLiteral("Tune: %1").arg(next));
        qInfo().noquote() << QStringLiteral("VIZ_TUNE=%1").arg(next);
    });

    vizControlRow->addStretch(1);

    // Right: display mode buttons
    auto makeVizModeBtn = [&](const QString& label) -> QPushButton* {
        auto* btn = new QPushButton(label, foreground);
        btn->setMinimumSize(60, 28);
        btn->setCursor(Qt::PointingHandCursor);
        btn->setCheckable(true);
        btn->setStyleSheet(QStringLiteral(
            "QPushButton { background: rgba(22,33,62,200); border: 1px solid #0f3460; border-radius: 4px;"
            "  color: #888; font-size: 10px; padding: 2px 8px; }"
            "QPushButton:hover { background: rgba(26,26,46,220); color: #ccc; }"
            "QPushButton:checked { background: rgba(83,52,131,200); color: #fff; border-color: #e94560; }"));
        return btn;
    };

    vizLineBtn_   = makeVizModeBtn(QStringLiteral("Line"));
    vizBarsBtn_   = makeVizModeBtn(QStringLiteral("Bars"));
    vizCircleBtn_ = makeVizModeBtn(QStringLiteral("Circle"));
    vizNoneBtn_   = makeVizModeBtn(QStringLiteral("None"));

    // Default: Bars is active
    vizBarsBtn_->setChecked(true);

    vizControlRow->addWidget(vizLineBtn_);
    vizControlRow->addWidget(vizBarsBtn_);
    vizControlRow->addWidget(vizCircleBtn_);
    vizControlRow->addWidget(vizNoneBtn_);

    auto switchVizMode = [this](VisualizerWidget::DisplayMode mode) {
        visualizer_->setDisplayMode(mode);
        vizLineBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Line);
        vizBarsBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Bars);
        vizCircleBtn_->setChecked(mode == VisualizerWidget::DisplayMode::Circle);
        vizNoneBtn_->setChecked(mode == VisualizerWidget::DisplayMode::None);
        // (JUCE path: no audioBufferOutput_ to gate)
        const char* names[] = {"None", "Bars", "Line", "Circle"};
        qInfo().noquote() << QStringLiteral("VIZ_MODE=%1").arg(names[static_cast<int>(mode)]);
    };

    QObject::connect(vizLineBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Line); });
    QObject::connect(vizBarsBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Bars); });
    QObject::connect(vizCircleBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::Circle); });
    QObject::connect(vizNoneBtn_, &QPushButton::clicked, this, [=]() { switchVizMode(VisualizerWidget::DisplayMode::None); });

    fgLayout->addLayout(vizControlRow);

    // Add foreground as second layer (on top of visualizer)
    heroStack->addWidget(foreground);
    heroStack->setCurrentWidget(foreground); // ensure foreground is on top

    layout->addWidget(heroFrame);

    // Animation timer for visualizer (~30fps)
    vizTimer_ = new QTimer(this);
    vizTimer_->setInterval(33);
    QObject::connect(vizTimer_, &QTimer::timeout, this, [this]() {
        // Feed visualizer from bridge meters at 30fps (not 4Hz pollStatus)
        const float freshLevel = static_cast<float>(
            std::max(bridge_.meterL(), bridge_.meterR()));
        if (freshLevel > 0.0f || bridge_.running())
            visualizer_->setAudioLevel(freshLevel);

        // Title pulse envelope: fast attack, slow decay — all in JUCE data path
        if (bridge_.running()) {
            constexpr double kDecay = 0.88;
            constexpr double kMinThreshold = 0.015;
            const double rawLevel = static_cast<double>(freshLevel);
            if (rawLevel > titlePulseEnvelope_)
                titlePulseEnvelope_ = rawLevel;
            else
                titlePulseEnvelope_ *= kDecay;
            if (titlePulseEnvelope_ < kMinThreshold)
                titlePulseEnvelope_ = 0.0;
        } else {
            titlePulseEnvelope_ *= 0.85;
            if (titlePulseEnvelope_ < 0.001)
                titlePulseEnvelope_ = 0.0;
        }
        visualizer_->setTitlePulse(static_cast<float>(titlePulseEnvelope_));

        if (visualizer_->displayMode() != VisualizerWidget::DisplayMode::None)
            visualizer_->tick();
    });
    vizTimer_->start();

    layout->addSpacing(14);

    // ═══════════════════════════════════════════════════
    // C. Progress section: time | seek bar | time
    // ═══════════════════════════════════════════════════
    auto* timeRow = new QHBoxLayout();
    timeRow->setSpacing(12);

    playerTimeLabel_ = new QLabel(QStringLiteral("0:00"), this);
    {
        QFont f = playerTimeLabel_->font();
        f.setPointSize(11);
        playerTimeLabel_->setFont(f);
    }
    playerTimeLabel_->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
    playerTimeLabel_->setMinimumWidth(42);
    playerTimeLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);

    seekSlider_ = new QSlider(Qt::Horizontal, this);
    seekSlider_->setRange(0, 1);
    seekSlider_->setMinimumHeight(24);

    playerTimeTotalLabel_ = new QLabel(QStringLiteral("0:00"), this);
    {
        QFont f = playerTimeTotalLabel_->font();
        f.setPointSize(11);
        playerTimeTotalLabel_->setFont(f);
    }
    playerTimeTotalLabel_->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
    playerTimeTotalLabel_->setMinimumWidth(42);
    playerTimeTotalLabel_->setAlignment(Qt::AlignLeft | Qt::AlignVCenter);

    timeRow->addWidget(playerTimeLabel_);
    timeRow->addWidget(seekSlider_, 1);
    timeRow->addWidget(playerTimeTotalLabel_);
    layout->addLayout(timeRow);

    layout->addSpacing(10);

    // ═══════════════════════════════════════════════════
    // D. Transport section: |< | Play/Pause | >|
    // ═══════════════════════════════════════════════════
    auto* transportRow = new QHBoxLayout();
    transportRow->setSpacing(16);

    // Invisible spacer to balance the Mode button on the right
    auto* transportLeftSpacer = new QWidget(this);
    transportLeftSpacer->setFixedSize(160, 1);
    transportLeftSpacer->setStyleSheet(QStringLiteral("background: transparent;"));
    transportRow->addWidget(transportLeftSpacer);

    transportRow->addStretch(1);

    prevBtn_ = new QPushButton(QStringLiteral("|<  Prev"), this);
    prevBtn_->setToolTip(QStringLiteral("Previous track"));
    prevBtn_->setMinimumSize(90, 48);
    prevBtn_->setCursor(Qt::PointingHandCursor);
    {
        QFont f = prevBtn_->font();
        f.setPointSize(12);
        f.setBold(true);
        prevBtn_->setFont(f);
    }
    transportRow->addWidget(prevBtn_);

    playPauseBtn_ = new QPushButton(QStringLiteral("Play"), this);
    playPauseBtn_->setToolTip(QStringLiteral("Play / Pause"));
    playPauseBtn_->setMinimumSize(120, 56);
    playPauseBtn_->setCursor(Qt::PointingHandCursor);
    {
        QFont f = playPauseBtn_->font();
        f.setPointSize(15);
        f.setBold(true);
        playPauseBtn_->setFont(f);
    }
    playPauseBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: #e94560; border: none; border-radius: 28px;"
        "  font-size: 15px; font-weight: bold; color: #ffffff; padding: 0 24px; }"
        "QPushButton:hover { background: #d63851; }"
        "QPushButton:pressed { background: #c02a42; }"));
    transportRow->addWidget(playPauseBtn_);

    nextBtn_ = new QPushButton(QStringLiteral("Next  >|"), this);
    nextBtn_->setToolTip(QStringLiteral("Next track"));
    nextBtn_->setMinimumSize(90, 48);
    nextBtn_->setCursor(Qt::PointingHandCursor);
    {
        QFont f = nextBtn_->font();
        f.setPointSize(12);
        f.setBold(true);
        nextBtn_->setFont(f);
    }
    transportRow->addWidget(nextBtn_);

    transportRow->addStretch(1);

    // Play mode button — right-aligned in transport row
    playModeBtn_ = new QPushButton(QStringLiteral("Mode: In Order"), this);
    playModeBtn_->setToolTip(QStringLiteral("Click to cycle: Play Once / In Order / Repeat All / Shuffle / Smart Shuffle"));
    playModeBtn_->setMinimumSize(160, 36);
    playModeBtn_->setCursor(Qt::PointingHandCursor);
    {
        QFont f = playModeBtn_->font();
        f.setPointSize(10);
        playModeBtn_->setFont(f);
    }
    playModeBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: #2a2a3e; border: 1px solid #555580; border-radius: 6px;"
        "  color: #ccccee; padding: 4px 14px; }"
        "QPushButton:hover { background: #3a3a50; }"));
    transportRow->addWidget(playModeBtn_);

    layout->addLayout(transportRow);

    QObject::connect(playModeBtn_, &QPushButton::clicked, this, [this]() {
        switch (playMode_) {
        case PlayMode::PlayOnce:      playMode_ = PlayMode::PlayInOrder;   break;
        case PlayMode::PlayInOrder:   playMode_ = PlayMode::RepeatAll;     break;
        case PlayMode::RepeatAll:     playMode_ = PlayMode::Shuffle;       break;
        case PlayMode::Shuffle:       playMode_ = PlayMode::SmartShuffle;  break;
        case PlayMode::SmartShuffle:  playMode_ = PlayMode::PlayOnce;      break;
        }
        if (playMode_ == PlayMode::SmartShuffle) {
            rebuildSmartShufflePool();
        }
        updatePlayModeButton();
        updateUpNextLabel();
        qInfo().noquote() << QStringLiteral("PLAY_MODE_CHANGED=%1").arg(playModeLabel());
    });

    layout->addSpacing(10);

    // ═══════════════════════════════════════════════════
    // D2. Volume slider (below transport)
    // ═══════════════════════════════════════════════════
    auto* volRow = new QHBoxLayout();
    volRow->setSpacing(10);

    auto* volLabel = new QLabel(QStringLiteral("Vol:"), this);
    {
        QFont f = volLabel->font();
        f.setPointSize(11);
        volLabel->setFont(f);
    }
    volLabel->setStyleSheet(QStringLiteral("color: #aaaaaa;"));

    volumeSlider_ = new QSlider(Qt::Horizontal, this);
    volumeSlider_->setRange(0, 100);
    volumeSlider_->setValue(80);
    volumeSlider_->setMinimumWidth(180);
    volumeSlider_->setMaximumWidth(300);

    auto* volPercent = new QLabel(QStringLiteral("80%"), this);
    {
        QFont f = volPercent->font();
        f.setPointSize(11);
        volPercent->setFont(f);
    }
    volPercent->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
    volPercent->setMinimumWidth(36);

    volRow->addStretch(1);
    volRow->addWidget(volLabel);
    volRow->addWidget(volumeSlider_);
    volRow->addWidget(volPercent);
    volRow->addStretch(1);
    layout->addLayout(volRow);

    layout->addSpacing(10);

    // ═══════════════════════════════════════════════════
    // D3. 16-Band EQ Panel (modular widget)
    // ═══════════════════════════════════════════════════
    eqPanel_ = new EqPanel(&bridge_, this);
    layout->addWidget(eqPanel_);

    layout->addSpacing(14);

    // ═══════════════════════════════════════════════════
    // E. Library browser: search + sort + column tree
    // ═══════════════════════════════════════════════════
    auto* libHeaderRow = new QHBoxLayout();
    libHeaderRow->setSpacing(8);

    auto* libLabel = new QLabel(QStringLiteral("Library"), this);
    {
        QFont f = libLabel->font();
        f.setPointSize(13);
        f.setBold(true);
        libLabel->setFont(f);
    }
    libHeaderRow->addWidget(libLabel);

    libHeaderRow->addSpacing(12);

    playerSearchBar_ = new QLineEdit(this);
    playerSearchBar_->setPlaceholderText(QStringLiteral("Search tracks..."));
    playerSearchBar_->setClearButtonEnabled(true);
    playerSearchBar_->setMinimumHeight(28);
    playerSearchBar_->setStyleSheet(QStringLiteral(
        "QLineEdit { background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 4px; padding: 4px 8px; font-size: 12px; }"
        "QLineEdit:focus { border-color: #e94560; }"));
    libHeaderRow->addWidget(playerSearchBar_, 1);

    libHeaderRow->addSpacing(8);

    auto* sortLabel = new QLabel(QStringLiteral("Sort:"), this);
    sortLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
    libHeaderRow->addWidget(sortLabel);

    playerSortCombo_ = new QComboBox(this);
    playerSortCombo_->addItems({
        QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Album"),
        QStringLiteral("Duration"), QStringLiteral("BPM"), QStringLiteral("Key")
    });
    playerSortCombo_->setMinimumWidth(90);
    libHeaderRow->addWidget(playerSortCombo_);

    libHeaderRow->addSpacing(8);

    auto* libCountLabel = new QLabel(QStringLiteral("0 tracks"), this);
    libCountLabel->setStyleSheet(QStringLiteral("color: #666; font-size: 11px;"));
    libHeaderRow->addWidget(libCountLabel);

    layout->addLayout(libHeaderRow);
    layout->addSpacing(4);

    playerLibraryTree_ = new DjLibraryWidget(this);
    playerLibraryTree_->setDatabase(&db_);
    {
        QFont f = playerLibraryTree_->font();
        f.setPointSize(11);
        playerLibraryTree_->setViewFont(f);
    }
    playerLibraryTree_->setViewStyleSheet(QStringLiteral(
        "QTableView { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 8px; outline: none; alternate-background-color: #1a1a2e; }"
        "QTableView::item { padding: 4px 6px; }"
        "QTableView::item:selected { background: #533483; color: #ffffff; }"
        "QTableView::item:hover { background: #1a1a2e; }"
        "QHeaderView::section { background: #0f3460; color: #e0e0e0; border: none;"
        "  padding: 5px 8px; font-weight: bold; font-size: 11px; }"
        "QScrollBar:vertical { background: #0a0e27; width: 8px; }"
        "QScrollBar::handle:vertical { background: #533483; border-radius: 4px; min-height: 20px; }"
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"));
    playerLibraryTree_->header()->setSectionResizeMode(QHeaderView::Interactive);
    playerLibraryTree_->header()->resizeSection(0, 280);
    playerLibraryTree_->header()->resizeSection(1, 140);
    playerLibraryTree_->header()->resizeSection(2, 130);
    playerLibraryTree_->header()->resizeSection(3,  65);
    playerLibraryTree_->header()->resizeSection(4,  50);
    playerLibraryTree_->header()->resizeSection(5,  45);
    
    // Right click on track header
    playerLibraryTree_->header()->setContextMenuPolicy(Qt::CustomContextMenu);
    QObject::connect(playerLibraryTree_->header(), &QWidget::customContextMenuRequested, this,
        [this](const QPoint& pos) {
            QMenu menu(this);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
                "QMenu::item { padding: 6px 24px; }"
                "QMenu::item:selected { background: #533483; }"
            ));
            auto* headers = playerLibraryTree_->header();
            for (int i = 1; i < headers->count(); ++i) {
                QString colName = headers->model()->headerData(i, Qt::Horizontal).toString();
                QAction* a = menu.addAction(colName);
                a->setCheckable(true);
                a->setChecked(!headers->isSectionHidden(i));
                QObject::connect(a, &QAction::toggled, this, [=](bool checked) {
                    headers->setSectionHidden(i, !checked);
                });
            }
            menu.exec(headers->mapToGlobal(pos));
        });

    // Right click on track
    QObject::connect(playerLibraryTree_, &DjLibraryWidget::contextMenuRequested, this,
        [this](qint64 trackId, QPoint globalPos) {
            QMenu menu(this);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
                "QMenu::item { padding: 6px 24px; }"
                "QMenu::item:selected { background: #533483; }"
                "QMenu::separator { height: 1px; background: #0f3460; margin: 4px 8px; }"
            ));
            auto* loadA = menu.addAction(QStringLiteral("Load to Deck A"));
            auto* loadB = menu.addAction(QStringLiteral("Load to Deck B"));
            
            QAction* res = menu.exec(globalPos);
            if (res == loadA) {
                auto t = db_.trackById(trackId);
                if (t) bridge_.loadTrackToDeck(0, t->filePath);
            } else if (res == loadB) {
                auto t = db_.trackById(trackId);
                if (t) bridge_.loadTrackToDeck(1, t->filePath);
            }
        });

layout->addWidget(playerLibraryTree_, 1);

    playerLibCountLabel_ = libCountLabel;

    // ═══════════════════════════════════════════════════
    // Audio engine: JUCE via EngineBridge (all audio)
    // ═══════════════════════════════════════════════════
    // Visualizer audio level is now driven from JUCE engine meters
    // via pollStatus() → bridge_.meterL()/meterR()

    // ── Signal connections (JUCE bridge) ──

    // Running state → hero state label + play/pause button text
    QObject::connect(&bridge_, &EngineBridge::runningChanged, this, [this]() {
        if (bridge_.running()) {
            playerStateLabel_->setText(QStringLiteral("Playing"));
            playPauseBtn_->setText(QStringLiteral("Pause"));
            qInfo().noquote() << QStringLiteral("JUCE_PLAYBACK_STATE=PLAYING");
        } else {
            playerStateLabel_->setText(QStringLiteral("Stopped"));
            playPauseBtn_->setText(QStringLiteral("Play"));
            qInfo().noquote() << QStringLiteral("JUCE_PLAYBACK_STATE=STOPPED");
        }
    });

    // Duration → seek slider range + total time label (JUCE bridge)
    QObject::connect(&bridge_, &EngineBridge::durationChanged, this, [this](double seconds) {
        const uint64_t gen = bridge_.currentLoadGen();
        if (gen != uiTrackGen_) {
            qInfo().noquote() << QStringLiteral("TRC_UI durationChanged DROP gen=%1 uiGen=%2 dur=%3")
                .arg(gen).arg(uiTrackGen_).arg(seconds, 0, 'f', 2);
            return;
        }
        const int durSec = static_cast<int>(seconds);
        seekSlider_->setRange(0, durSec);
        playerTimeTotalLabel_->setText(QStringLiteral("%1:%2")
            .arg(durSec / 60).arg(durSec % 60, 2, 10, QChar('0')));
        qInfo().noquote() << QStringLiteral("TRC_UI durationChanged=%1 sliderMax=%2 gen=%3 IDX=%4 name=%5")
            .arg(seconds, 0, 'f', 2).arg(durSec).arg(gen).arg(currentTrackIndex_)
            .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(tracks_->size())
                 ? (*tracks_)[currentTrackIndex_].displayName : QStringLiteral("?"));
    });

    // Position → seek slider + current time label (JUCE bridge)
    QObject::connect(&bridge_, &EngineBridge::playheadChanged, this, [this](double seconds) {
        const uint64_t gen = bridge_.currentLoadGen();
        if (gen != uiTrackGen_) return; // stale generation
        const int posSec = static_cast<int>(seconds);
        if (!seekSliderPressed_) {
            seekSlider_->setValue(posSec);
        }
        playerTimeLabel_->setText(QStringLiteral("%1:%2")
            .arg(posSec / 60).arg(posSec % 60, 2, 10, QChar('0')));
    });

    // End of track (JUCE bridge)
    QObject::connect(&bridge_, &EngineBridge::endOfTrack, this, [this]() {
        const uint64_t gen = bridge_.currentLoadGen();
        if (gen != uiTrackGen_) {
            qInfo().noquote() << QStringLiteral("TRC_UI endOfTrack DROP gen=%1 uiGen=%2").arg(gen).arg(uiTrackGen_);
            return;
        }
        qInfo().noquote() << QStringLiteral("TRC_UI endOfTrack ACCEPT gen=%1 IDX=%2 name=%3 sliderVal=%4 sliderMax=%5")
            .arg(gen).arg(currentTrackIndex_)
            .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(tracks_->size())
                 ? (*tracks_)[currentTrackIndex_].displayName : QStringLiteral("?"))
            .arg(seekSlider_ ? seekSlider_->value() : -1)
            .arg(seekSlider_ ? seekSlider_->maximum() : -1);
        onEndOfMedia();
    });

    // Seek slider interaction
    QObject::connect(seekSlider_, &QSlider::sliderPressed, this, [this]() { seekSliderPressed_ = true; });
    QObject::connect(seekSlider_, &QSlider::sliderReleased, this, [this]() {
        seekSliderPressed_ = false;
        const int seekVal = seekSlider_->value();
        const int seekMax = seekSlider_->maximum();
        qInfo().noquote() << QStringLiteral("TRC_UI seekRelease val=%1 max=%2 IDX=%3 name=%4")
            .arg(seekVal).arg(seekMax).arg(currentTrackIndex_)
            .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(tracks_->size())
                 ? (*tracks_)[currentTrackIndex_].displayName : QStringLiteral("?"));
        bridge_.seek(static_cast<double>(seekVal));
    });

    // Play/Pause (JUCE path)
    QObject::connect(playPauseBtn_, &QPushButton::clicked, this, [this]() {
        if (bridge_.running()) {
            bridge_.pause();
            playerStateLabel_->setText(QStringLiteral("Paused"));
            playPauseBtn_->setText(QStringLiteral("Play"));
            qInfo().noquote() << QStringLiteral("JUCE_PAUSE=TRUE");
        } else {
            bridge_.start();
            playerStateLabel_->setText(QStringLiteral("Playing"));
            playPauseBtn_->setText(QStringLiteral("Pause"));
            qInfo().noquote() << QStringLiteral("JUCE_RESUME=TRUE");
        }
    });

    // Previous / Next
    QObject::connect(prevBtn_, &QPushButton::clicked, this, [this]() { playPrevTrack(); });
    QObject::connect(nextBtn_, &QPushButton::clicked, this, [this]() { playNextTrack(); });

    // Volume → JUCE master gain + percent label
    QObject::connect(volumeSlider_, &QSlider::valueChanged, this, [this, volPercent](int value) {
        bridge_.setMasterGain(static_cast<double>(value) / 100.0);
        volPercent->setText(QStringLiteral("%1%").arg(value));
    });

    // Library tree → double-click to play
    QObject::connect(playerLibraryTree_, &DjLibraryWidget::trackActivated, this, [this](qint64 trackId) {
        const int idx = static_cast<int>(trackId);
        if (idx >= 0 && idx < static_cast<int>(tracks_->size())) {
            currentTrackIndex_ = idx;
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("PLAYER_LIB_PLAY=%1").arg((*tracks_)[idx].displayName);
        }
    });

    // Search bar → filter player library
    QObject::connect(playerSearchBar_, &QLineEdit::textChanged, this, [this](const QString&) {
        refreshPlayerLibrary();
    });

    // Sort combo → re-sort player library
    QObject::connect(playerSortCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
        refreshPlayerLibrary();
    });

    // Bridge signal: audio profile apply result
    QObject::connect(&bridge_, &EngineBridge::audioProfileApplied, this,
        &PlayerPage::onAudioProfileApplied);
}
void PlayerPage::activateTrack(int trackIndex)
{
    refreshPlayerLibrary();
    loadAndPlayTrack(trackIndex);
}

void PlayerPage::refreshLibrary()
{
    refreshPlayerLibrary();
}

void PlayerPage::setAudioLevel(float level)
{
    if (visualizer_) {
        if (!meterDiagLogged_ && level > 0.0f) {
            qInfo().noquote() << QStringLiteral("DIAG_METER_FEED: feed=%1").arg(QString::number(level, 'f', 6));
            meterDiagLogged_ = true;
        }
        visualizer_->setAudioLevel(level);
    }
}

void PlayerPage::loadAndPlayTrack(int trackIndex)
{
    if (trackIndex < 0 || trackIndex >= static_cast<int>(tracks_->size())) return;
    currentTrackIndex_ = trackIndex;
    const TrackInfo& track = (*tracks_)[trackIndex];

    // Update hero labels
    playerTrackLabel_->setText(track.displayName.isEmpty()
        ? QStringLiteral("Unknown Track") : track.displayName);
    playerTrackLabel_->hide(); // Hide QLabel — visualizer paints pulsing title
    playerArtistLabel_->hide();
    playerMetaLabel_->hide();
    playerStateLabel_->hide();
    upNextLabel_->hide();
    if (nowPlayingTag_) nowPlayingTag_->hide();
    visualizer_->setTitleText(track.displayName.isEmpty()
        ? QStringLiteral("Unknown Track") : track.displayName);

    // Artist + Album line
    QStringList artistParts;
    if (!track.artist.isEmpty()) artistParts << track.artist;
    if (!track.album.isEmpty()) artistParts << track.album;
    playerArtistLabel_->setText(artistParts.isEmpty()
        ? QString() : artistParts.join(QStringLiteral("  |  ")));

    // Metadata line: BPM / Key / Duration
    QStringList metaParts;
    if (!track.bpm.isEmpty()) metaParts << QStringLiteral("BPM: %1").arg(track.bpm);
    if (!track.musicalKey.isEmpty()) metaParts << QStringLiteral("Key: %1").arg(track.musicalKey);
    if (!track.durationStr.isEmpty()) metaParts << track.durationStr;
    if (!track.genre.isEmpty()) metaParts << track.genre;
    playerMetaLabel_->setText(metaParts.join(QStringLiteral("   ")));

    // Highlight in library tree
    highlightPlayerLibraryItem(trackIndex);

    // JUCE playback path — load real file into engine deck
    bridge_.stop();
    if (!juceSimpleModeReady_) {
        bridge_.enterSimpleMode();
        juceSimpleModeReady_ = true;
    }

    // Reset UI transport state to zero BEFORE loading
    if (seekSlider_) {
        seekSlider_->setRange(0, 1);
        seekSlider_->setValue(0);
    }
    if (playerTimeLabel_) playerTimeLabel_->setText(QStringLiteral("0:00"));
    if (playerTimeTotalLabel_) playerTimeTotalLabel_->setText(QStringLiteral("0:00"));

    // Pre-set UI generation so the authoritative signals from
    // loadTrack() pass the gen check in durationChanged/playheadChanged.
    uiTrackGen_ = bridge_.currentLoadGen() + 1;
    qInfo().noquote() << QStringLiteral("TRC_UI loadAndPlay IDX=%1 name=%2 uiGen=%3")
        .arg(trackIndex).arg(track.displayName).arg(uiTrackGen_);

    const bool loaded = bridge_.loadTrack(track.filePath);
    // bridge_.loadTrack incremented gen → now bridge_.currentLoadGen() == uiTrackGen_
    if (loaded) {
        bridge_.start();
        if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Pause"));
        qInfo().noquote() << QStringLiteral("TRC_UI started gen=%1 IDX=%2 name=%3 sliderMax=%4")
            .arg(uiTrackGen_).arg(trackIndex).arg(track.displayName)
            .arg(seekSlider_ ? seekSlider_->maximum() : -1);
    } else {
        qWarning().noquote() << QStringLiteral("TRC_UI loadTrack FAILED IDX=%1 name=%2")
            .arg(trackIndex).arg(track.displayName);
    }

    // Update "Up Next" label
    updateUpNextLabel();

    qInfo().noquote() << QStringLiteral("LOAD_AND_PLAY=%1 IDX=%2").arg(track.displayName).arg(trackIndex);
}

void PlayerPage::playNextTrack()
{
    if ((!tracks_ || tracks_->empty()) || !playerLibraryTree_) return;
    const int count = playerLibraryTree_->totalFilteredCount();
    if (count == 0) return;

    if (playMode_ == PlayMode::Shuffle) {
        // Pure random from visible list
        std::uniform_int_distribution<int> dist(0, count - 1);
        const int ri = dist(shuffleRng_);
        const int idx = (int)playerLibraryTree_->trackIdAt(ri);
        loadAndPlayTrack(idx);
        qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SHUFFLE %1").arg((*tracks_)[idx].displayName);
        return;
    }

    if (playMode_ == PlayMode::SmartShuffle) {
        advanceSmartShuffle();
        return;
    }

    // Linear modes: find current position, advance
    int curPos = playerLibraryTree_->rowOfTrackId((qint64)currentTrackIndex_);

    int nextPos = curPos + 1;
    if (nextPos >= count) {
        if (playMode_ == PlayMode::RepeatAll) {
            nextPos = 0; // wrap
        } else {
            bridge_.stop();
            if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Play"));
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=END_OF_QUEUE");
            return;
        }
    }

    const int nextIdx = (int)playerLibraryTree_->trackIdAt(nextPos);
    if (nextIdx >= 0 && nextIdx < static_cast<int>(tracks_->size())) {
        loadAndPlayTrack(nextIdx);
        qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=%1").arg((*tracks_)[nextIdx].displayName);
    }
}

void PlayerPage::playPrevTrack()
{
    if ((!tracks_ || tracks_->empty()) || !playerLibraryTree_) return;
    const int count = playerLibraryTree_->totalFilteredCount();
    if (count == 0) return;

    if (playMode_ == PlayMode::Shuffle || playMode_ == PlayMode::SmartShuffle) {
        // In shuffle modes, prev picks random (no history)
        std::uniform_int_distribution<int> dist(0, count - 1);
        const int ri = dist(shuffleRng_);
        const int idx = (int)playerLibraryTree_->trackIdAt(ri);
        loadAndPlayTrack(idx);
        qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=SHUFFLE %1").arg((*tracks_)[idx].displayName);
        return;
    }

    int curPos = playerLibraryTree_->rowOfTrackId((qint64)currentTrackIndex_);

    int prevPos = curPos - 1;
    if (prevPos < 0) {
        if (playMode_ == PlayMode::RepeatAll) {
            prevPos = count - 1; // wrap
        } else {
            return; // already at start
        }
    }

    const int prevIdx = (int)playerLibraryTree_->trackIdAt(prevPos);
    if (prevIdx >= 0 && prevIdx < static_cast<int>(tracks_->size())) {
        loadAndPlayTrack(prevIdx);
        qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=%1").arg((*tracks_)[prevIdx].displayName);
    }
}

void PlayerPage::onEndOfMedia()
{
    switch (playMode_) {
    case PlayMode::PlayOnce:
        // Stop. Do not auto-advance.
        bridge_.stop();
        if (playPauseBtn_) playPauseBtn_->setText(QStringLiteral("Play"));
        if (playerTrackLabel_) playerTrackLabel_->show();
        if (playerArtistLabel_) playerArtistLabel_->show();
        if (playerMetaLabel_) playerMetaLabel_->show();
        if (playerStateLabel_) playerStateLabel_->show();
        if (upNextLabel_) upNextLabel_->show();
        if (nowPlayingTag_) nowPlayingTag_->show();
        visualizer_->setTitleText(QString());
        visualizer_->setUpNextText(QString());
        qInfo().noquote() << QStringLiteral("END_OF_MEDIA=PLAY_ONCE_STOP");
        break;
    case PlayMode::PlayInOrder:
    case PlayMode::RepeatAll:
    case PlayMode::Shuffle:
    case PlayMode::SmartShuffle:
        playNextTrack();
        break;
    }
}

QString PlayerPage::playModeLabel() const
{
    switch (playMode_) {
    case PlayMode::PlayOnce:      return QStringLiteral("Play Once");
    case PlayMode::PlayInOrder:   return QStringLiteral("In Order");
    case PlayMode::RepeatAll:     return QStringLiteral("Repeat All");
    case PlayMode::Shuffle:       return QStringLiteral("Shuffle");
    case PlayMode::SmartShuffle:  return QStringLiteral("Smart Shuffle");
    }
    return QStringLiteral("Unknown");
}

void PlayerPage::updatePlayModeButton()
{
    if (playModeBtn_)
        playModeBtn_->setText(QStringLiteral("Mode: %1").arg(playModeLabel()));
}

void PlayerPage::updateUpNextLabel()
{
    if (!upNextLabel_ || !playerLibraryTree_) return;
    const int count = playerLibraryTree_->totalFilteredCount();
    if (count == 0 || currentTrackIndex_ < 0) {
        upNextLabel_->setText(QStringLiteral("Up Next: \u2014"));
        if (visualizer_) visualizer_->setUpNextText(QString());
        return;
    }

    int nextIdx = -1;

    if (playMode_ == PlayMode::Shuffle) {
        upNextLabel_->setText(QStringLiteral("Up Next: (shuffle)"));
        if (visualizer_) visualizer_->setUpNextText(QStringLiteral("(shuffle)"));
        return;
    } else if (playMode_ == PlayMode::SmartShuffle) {
        // Show next from pool if available
        if (smartShufflePos_ >= 0 && smartShufflePos_ < static_cast<int>(smartShufflePool_.size())) {
            nextIdx = smartShufflePool_[smartShufflePos_];
        } else {
            upNextLabel_->setText(QStringLiteral("Up Next: (reshuffle)"));
            if (visualizer_) visualizer_->setUpNextText(QStringLiteral("(reshuffle)"));
            return;
        }
    } else {
        // Linear modes: find next in visible list
        const int curPos = playerLibraryTree_->rowOfTrackId((qint64)currentTrackIndex_);
        if (curPos >= 0) {
            if (curPos + 1 < count) {
                nextIdx = (int)playerLibraryTree_->trackIdAt(curPos + 1);
            } else if (playMode_ == PlayMode::RepeatAll) {
                nextIdx = (int)playerLibraryTree_->trackIdAt(0);
            }
        }
    }

    if (nextIdx >= 0 && nextIdx < static_cast<int>(tracks_->size())) {
        const auto& t = (*tracks_)[nextIdx];
        QString name = t.displayName.isEmpty() ? QStringLiteral("Unknown") : t.displayName;
        QString labelName = name;
        if (!t.artist.isEmpty()) labelName = QStringLiteral("%1 \u2013 %2").arg(t.artist, name);
        upNextLabel_->setText(QStringLiteral("Up Next: %1").arg(labelName));
        if (visualizer_) visualizer_->setUpNextText(name);
    } else {
        upNextLabel_->setText(QStringLiteral("Up Next: \u2014"));
        if (visualizer_) visualizer_->setUpNextText(QString());
    }
}

void PlayerPage::rebuildSmartShufflePool()
{
    if (!playerLibraryTree_) return;
    const int count = playerLibraryTree_->totalFilteredCount();
    smartShufflePool_.clear();
    smartShufflePool_.reserve(count);
    for (int i = 0; i < count; ++i)
        smartShufflePool_.push_back((int)playerLibraryTree_->trackIdAt(i));
    std::shuffle(smartShufflePool_.begin(), smartShufflePool_.end(), shuffleRng_);
    smartShufflePos_ = 0;
    qInfo().noquote() << QStringLiteral("SMART_SHUFFLE_POOL_REBUILT=%1").arg(count);
}

void PlayerPage::advanceSmartShuffle()
{
    if (smartShufflePool_.empty() || smartShufflePos_ >= static_cast<int>(smartShufflePool_.size())) {
        rebuildSmartShufflePool();
    }
    if (smartShufflePool_.empty()) return;
    const int idx = smartShufflePool_[smartShufflePos_];
    ++smartShufflePos_;
    if (idx >= 0 && idx < static_cast<int>(tracks_->size())) {
        loadAndPlayTrack(idx);
        qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SMART_SHUFFLE %1 pos=%2/%3")
            .arg((*tracks_)[idx].displayName).arg(smartShufflePos_).arg(smartShufflePool_.size());
    }
}

void PlayerPage::refreshPlayerLibrary()
{
    if (!playerLibraryTree_) return;
    const QString search = playerSearchBar_ ? playerSearchBar_->text().trimmed() : QString();
    const int sortCol    = playerSortCombo_ ? playerSortCombo_->currentIndex() : 0;
    playerLibraryTree_->applyFilter(search, 5, {}, sortCol);
    const int count = playerLibraryTree_->totalFilteredCount();
    if (playerLibCountLabel_)
        playerLibCountLabel_->setText(QStringLiteral("%1 tracks").arg(count));
    highlightPlayerLibraryItem(currentTrackIndex_);
    if (playMode_ == PlayMode::SmartShuffle && !smartShufflePool_.empty())
        rebuildSmartShufflePool();
    qInfo().noquote() << QStringLiteral("PLAYER_LIBRARY_REFRESHED=%1").arg(count);
}

void PlayerPage::highlightPlayerLibraryItem(int trackIndex)
{
    if (!playerLibraryTree_ || trackIndex < 0) return;
    playerLibraryTree_->setCurrentTrackId(static_cast<qint64>(trackIndex));
    playerLibraryTree_->scrollToTrackId(static_cast<qint64>(trackIndex));
}

void PlayerPage::requestAudioProfilesRefresh(bool logMarker)
{
    if (QThread::currentThread() != thread()) {
        QMetaObject::invokeMethod(this, [this, logMarker]() { requestAudioProfilesRefresh(logMarker); }, Qt::QueuedConnection);
        return;
    }

    if (audioApplyInProgress_.load(std::memory_order_acquire)) {
        qInfo().noquote() << QStringLiteral("RTAudioALRefreshDeferred=TRUE");
        pendingAudioProfilesRefresh_ = true;
        pendingAudioProfilesRefreshLogMarker_ = pendingAudioProfilesRefreshLogMarker_ || logMarker;
        return;
    }

    refreshAudioProfilesUi(logMarker);
}

void PlayerPage::refreshAudioProfilesUi(bool logMarker)
{
    UiAudioProfilesStore store {};
    QString loadError;
    const bool loaded = loadUiAudioProfiles(store, loadError);

    {
        const QSignalBlocker blocker(audioProfileCombo_);
        audioProfileCombo_->clear();
        if (loaded) {
            for (const auto& entry : store.profiles) {
                const QString& profileName = entry.first;
                const UiAudioProfile& profile = entry.second;
                const QString itemText = QStringLiteral("%1 (sr=%2, buf=%3, ch=%4)")
                                             .arg(profileName,
                                                  QString::number(profile.sampleRate),
                                                  QString::number(profile.bufferFrames),
                                                  QString::number(profile.channelsOut));
                audioProfileCombo_->addItem(itemText, profileName);
            }

            const int activeIndex = audioProfileCombo_->findData(store.activeProfile);
            if (activeIndex >= 0) {
                audioProfileCombo_->setCurrentIndex(activeIndex);
            }
        }
    }

    audioProfilesStore_ = store;
    const bool controlsEnabled = loaded && !audioProfilesStore_.profiles.empty();
    audioProfileCombo_->setEnabled(controlsEnabled);
    applyAudioProfileButton_->setEnabled(controlsEnabled);

    if (!controlsEnabled) {
        const QString reason = loadError.isEmpty() ? QStringLiteral("No profiles available") : loadError;
        qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(reason);
        if (logMarker) emit diagnosticsRefreshRequested();
        return;
    }

    if (logMarker || lastAkActiveProfileMarker_ != audioProfilesStore_.activeProfile) {
        qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(audioProfilesStore_.activeProfile);
        lastAkActiveProfileMarker_ = audioProfilesStore_.activeProfile;
    }
}

void PlayerPage::applySelectedAudioProfile()
{
    if (audioApplyInProgress_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    qInfo().noquote() << QStringLiteral("RTAudioALApplyBegin=1");

    const QString profileName = audioProfileCombo_->currentData().toString();
    const auto profileIt = audioProfilesStore_.profiles.find(profileName);
    if (profileName.isEmpty() || profileIt == audioProfilesStore_.profiles.end()) {
        qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=<invalid>");
        qInfo().noquote() << QStringLiteral("RTAudioALDeviceReopen=FALSE");
        qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
        QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Selected profile is not valid."));
        finishAudioApply();
        return;
    }

    qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(profileName);

    pendingApplyProfileName_ = profileName;

    const UiAudioProfile& profile = profileIt->second;
    bridge_.applyAudioProfile(profile.deviceId.toStdString(),
                              profile.deviceName.toStdString(),
                              profile.sampleRate,
                              profile.bufferFrames,
                              profile.channelsOut);
    // Result arrives via audioProfileApplied signal → onAudioProfileApplied()
}

void PlayerPage::onAudioProfileApplied(bool ok)
{
    const QString profileName = pendingApplyProfileName_;
    pendingApplyProfileName_.clear();

    if (!ok) {
        qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
        QMessageBox::warning(this, QStringLiteral("Audio Profile"), QStringLiteral("Failed to apply selected profile."));
        finishAudioApply();
        return;
    }

    QString saveError;
    if (!writeUiAudioProfilesActiveProfile(audioProfilesStore_, profileName, saveError)) {
        qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=FAIL");
        QMessageBox::warning(this,
                             QStringLiteral("Audio Profile"),
                             QStringLiteral("Profile applied, but active_profile was not persisted: %1").arg(saveError));
        finishAudioApply();
        return;
    }

    qInfo().noquote() << QStringLiteral("RTAudioALApplyResult=PASS");
    audioProfilesStore_.activeProfile = profileName;
    lastAkActiveProfileMarker_ = profileName;
    lastAgMarkerKey_.clear();
    finishAudioApply();
}

void PlayerPage::finishAudioApply()
{
    audioApplyInProgress_.store(false, std::memory_order_release);
    if (pendingAudioProfilesRefresh_) {
        const bool logMarker = pendingAudioProfilesRefreshLogMarker_;
        qInfo().noquote() << QStringLiteral("RTAudioALRefreshFlushed=TRUE");
        pendingAudioProfilesRefresh_ = false;
        pendingAudioProfilesRefreshLogMarker_ = false;
        QTimer::singleShot(0, this, [this, logMarker]() { requestAudioProfilesRefresh(logMarker); });
    }
}

