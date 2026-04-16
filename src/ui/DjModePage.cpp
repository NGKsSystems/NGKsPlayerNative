#include "ui/DjModePage.h"

#include <QDateTime>
#include <QDir>
#include <QFileInfo>
#include <QHBoxLayout>
#include <QLabel>
#include <QMenu>
#include <QMessageBox>
#include <QPushButton>
#include <QSlider>
#include <QSplitter>
#include <QTimer>
#include <QVBoxLayout>
#include <windows.h>  // GetCurrentThreadId

#include "ui/AnalysisBridge.h"
#include "ui/EngineBridge.h"
#include "ui/AncillaryScreensWidget.h"
#include "ui/library/DjBrowserPane.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryPersistence.h"
#include "ui/DeckStrip.h"  // includes LevelMeter

DjModePage::DjModePage(EngineBridge& bridge, DjLibraryDatabase& db,
                       QWidget* parent)
    : QWidget(parent)
    , bridge_(bridge)
    , db_(db)
{
    setStyleSheet(QStringLiteral("background: #080b10;"));
auto* layout = new QVBoxLayout(this);
layout->setContentsMargins(4, 0, 4, 4);
layout->setSpacing(2);

djUtilityMenu_ = new QMenu(this);
djUtilityMenu_->setStyleSheet(QStringLiteral(
    "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; padding: 4px 0; }"
    "QMenu::item { padding: 6px 24px; }"
    "QMenu::item:selected { background: #533483; }"
    "QMenu::separator { height: 1px; background: #0f3460; margin: 4px 8px; }"));

backAction_ = djUtilityMenu_->addAction(QStringLiteral("Back to Library"));
importFolderAction_ = djUtilityMenu_->addAction(QStringLiteral("Import Folder"));
importAnalysisAction_ = djUtilityMenu_->addAction(QStringLiteral("Run Analysis"));
djUtilityMenu_->addSeparator();
proAudioClipperAction_ = djUtilityMenu_->addAction(QStringLiteral("ProAudioClipper"));
ancillaryScreensAction_ = djUtilityMenu_->addAction(QStringLiteral("Ancillary Screens"));

QObject::connect(backAction_, &QAction::triggered, this, [this]() {
    bridge_.leaveDjMode();
    emit backRequested();
});
QObject::connect(importFolderAction_, &QAction::triggered, this, [this]() {
    emit importFolderRequested();
});
QObject::connect(importAnalysisAction_, &QAction::triggered, this, [this]() {
    emit importAnalysisRequested();
});
QObject::connect(proAudioClipperAction_, &QAction::triggered, this, [this]() {
    showProAudioClipperPlaceholder();
});
QObject::connect(ancillaryScreensAction_, &QAction::triggered, this, [this]() {
    openAncillaryScreens();
});

// ── Per-deck columns: Deck + Library side by side ──
auto* deckRow = new QHBoxLayout();
deckRow->setSpacing(6);

// ── Deck A column: strip + library ──
auto* colA = new QVBoxLayout();
colA->setSpacing(4);
djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, this);
colA->addWidget(djDeckA_, 1);

deckRow->addLayout(colA, 5);

// ── Master section column (center) ──
auto* masterCol = new QVBoxLayout();
masterCol->setSpacing(4);
masterCol->setContentsMargins(4, 0, 4, 0);

auto* masterLabel = new QLabel(QStringLiteral("MASTER"), this);
{
    QFont f = masterLabel->font(); f.setPointSizeF(7.5); f.setBold(true);
    masterLabel->setFont(f);
}
masterLabel->setAlignment(Qt::AlignCenter);
masterLabel->setStyleSheet(QStringLiteral(
    "color: #e0e0e0; background: transparent; padding: 2px 0;"));
masterCol->addWidget(masterLabel);

// Master L/R meters
auto* masterMeterRow = new QHBoxLayout();
masterMeterRow->setSpacing(2);
masterMeterRow->addStretch();
djMasterMeterL_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), this);
djMasterMeterR_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), this);
masterMeterRow->addWidget(djMasterMeterL_);
masterMeterRow->addWidget(djMasterMeterR_);
masterMeterRow->addStretch();
masterCol->addLayout(masterMeterRow, 1);

// CUE MIX label + slider (horizontal)
auto* cueMixLabel = new QLabel(QStringLiteral("CUE MIX"), this);
{
    QFont f = cueMixLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
    cueMixLabel->setFont(f);
}
cueMixLabel->setAlignment(Qt::AlignCenter);
cueMixLabel->setStyleSheet(QStringLiteral(
    "color: #aaa; background: transparent; padding: 1px 0;"));
masterCol->addWidget(cueMixLabel);

djCueMix_ = new QSlider(Qt::Horizontal, this);
djCueMix_->setRange(0, 1000);
djCueMix_->setValue(500);
djCueMix_->setFixedHeight(22);
djCueMix_->setStyleSheet(QStringLiteral(
    "QSlider::groove:horizontal {"
    "  background: #161616; height: 6px; border-radius: 3px;"
    "  border: 1px solid #333; }"
    "QSlider::handle:horizontal {"
    "  background: #d0d0d0; width: 14px; height: 14px;"
    "  margin: -5px 0; border-radius: 3px;"
    "  border: 1px solid #666; }"
    "QSlider::sub-page:horizontal {"
    "  background: #4070a0; border-radius: 3px; }"
    "QSlider::add-page:horizontal {"
    "  background: #333; border-radius: 3px; }"));
masterCol->addWidget(djCueMix_);

// CUE VOL label + slider (horizontal)
auto* cueVolLabel = new QLabel(QStringLiteral("CUE VOL"), this);
{
    QFont f = cueVolLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
    cueVolLabel->setFont(f);
}
cueVolLabel->setAlignment(Qt::AlignCenter);
cueVolLabel->setStyleSheet(QStringLiteral(
    "color: #aaa; background: transparent; padding: 1px 0;"));
masterCol->addWidget(cueVolLabel);

djCueVol_ = new QSlider(Qt::Horizontal, this);
djCueVol_->setRange(0, 1000);
djCueVol_->setValue(1000);
djCueVol_->setFixedHeight(22);
djCueVol_->setStyleSheet(QStringLiteral(
    "QSlider::groove:horizontal {"
    "  background: #161616; height: 6px; border-radius: 3px;"
    "  border: 1px solid #333; }"
    "QSlider::handle:horizontal {"
    "  background: #d0d0d0; width: 14px; height: 14px;"
    "  margin: -5px 0; border-radius: 3px;"
    "  border: 1px solid #666; }"
    "QSlider::sub-page:horizontal {"
    "  background: #4070a0; border-radius: 3px; }"
    "QSlider::add-page:horizontal {"
    "  background: #333; border-radius: 3px; }"));
masterCol->addWidget(djCueVol_);

// OUTPUT MODE label + toggle button
auto* outModeLabel = new QLabel(QStringLiteral("OUTPUT"), this);
{
    QFont f = outModeLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
    outModeLabel->setFont(f);
}
outModeLabel->setAlignment(Qt::AlignCenter);
outModeLabel->setStyleSheet(QStringLiteral(
    "color: #aaa; background: transparent; padding: 1px 0;"));
masterCol->addWidget(outModeLabel);

djOutputModeBtn_ = new QPushButton(QStringLiteral("Stereo"), this);
djOutputModeBtn_->setCursor(Qt::PointingHandCursor);
djOutputModeBtn_->setCheckable(true);
djOutputModeBtn_->setChecked(false);
djOutputModeBtn_->setFixedHeight(24);
djOutputModeBtn_->setStyleSheet(QStringLiteral(
    "QPushButton { background: #1a1a2a; border: 1px solid #444;"
    "  border-radius: 4px; color: #ccc; font-size: 8pt; font-weight: bold;"
    "  padding: 2px 6px; }"
    "QPushButton:checked { background: #2a4060; border: 1px solid #4090d0;"
    "  color: #60c0ff; }"
    "QPushButton:hover { background: #222240; }"));
masterCol->addWidget(djOutputModeBtn_);

masterCol->addStretch();
deckRow->addLayout(masterCol, 2);

// ── Deck B column: strip + library ──
auto* colB = new QVBoxLayout();
colB->setSpacing(4);
djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, this);
colB->addWidget(djDeckB_, 1);

deckAnalysisBridgeA_ = new AnalysisBridge(this);
deckAnalysisBridgeB_ = new AnalysisBridge(this);
wireDeckAnalysisBridge(deckAnalysisBridgeA_, djDeckA_, 0);
wireDeckAnalysisBridge(deckAnalysisBridgeB_, djDeckB_, 1);

deckRow->addLayout(colB, 5);



auto* pageSplitter = new QSplitter(Qt::Vertical, this);
djBrowser_ = new DjBrowserPane(&db_, this);

// -- Browser context menu -> load to deck --
QObject::connect(djBrowser_, &DjBrowserPane::loadToDeckRequested, this,
    [this](int deckIdx, const QString& path) {
        loadDeckTrack(deckIdx, path);
    });

QObject::connect(djBrowser_, &DjBrowserPane::importFolderRequested, this, [this]() {
    emit importFolderRequested();
});

QObject::connect(djBrowser_, &DjBrowserPane::importAnalysisRequested, this, [this]() {
    emit importAnalysisRequested();
});
QObject::connect(djBrowser_, &DjBrowserPane::backRequested, this, [this]() {
    bridge_.leaveDjMode();
    emit backRequested();
});
QObject::connect(djBrowser_, &DjBrowserPane::showProAudioClipperRequested, this, [this]() {
    showProAudioClipperPlaceholder();
});
QObject::connect(djBrowser_, &DjBrowserPane::showAncillaryScreensRequested, this, [this]() {
    openAncillaryScreens();
});

auto* deckWidget = new QWidget(this);
auto* deckRowLayout = new QVBoxLayout(deckWidget);
deckRowLayout->setContentsMargins(0,0,0,0);
deckRowLayout->addLayout(deckRow);

// ── Crossfader row ──
auto* xfadeRow = new QHBoxLayout();
xfadeRow->setSpacing(4);
xfadeRow->setContentsMargins(0, 1, 0, 1);

auto* xfadeLabel = new QLabel(QStringLiteral("A"), this);
{
    QFont f = xfadeLabel->font(); f.setPointSize(12); f.setBold(true);
    xfadeLabel->setFont(f);
}
xfadeLabel->setStyleSheet(QStringLiteral(
    "color: #e07020; background: transparent;"));
xfadeRow->addWidget(xfadeLabel);

djCrossfader_ = new QSlider(Qt::Horizontal, this);
djCrossfader_->setRange(0, 1000);
djCrossfader_->setValue(500);
djCrossfader_->setFixedHeight(32);
djCrossfader_->setStyleSheet(QStringLiteral(
    "QSlider::groove:horizontal {"
    "  background: qlineargradient(x1:0,x2:1,"
    "    stop:0 rgba(224,112,32,25), stop:0.48 #0a0a0a,"
    "    stop:0.5 #222, stop:0.52 #0a0a0a,"
    "    stop:1 rgba(32,128,224,25));"
    "  height: 8px; border-radius: 4px;"
    "  border: 1px solid #222; }"
    "QSlider::handle:horizontal {"
    "  background: qlineargradient(x1:0,x2:1, stop:0 #d0d0d0, stop:0.5 #ffffff, stop:1 #d0d0d0);"
    "  width: 28px; height: 28px;"
    "  margin: -10px 0; border-radius: 4px;"
    "  border: 1px solid #666; }"
    "QSlider::sub-page:horizontal {"
    "  background: qlineargradient(x1:0,x2:1, stop:0 #e07020, stop:1 #333);"
    "  border-radius: 4px; }"
    "QSlider::add-page:horizontal {"
    "  background: qlineargradient(x1:0,x2:1, stop:0 #333, stop:1 #2080e0);"
    "  border-radius: 4px; }"));
xfadeRow->addWidget(djCrossfader_, 1);

auto* xfadeLabelB = new QLabel(QStringLiteral("B"), this);
{
    QFont f = xfadeLabelB->font(); f.setPointSize(12); f.setBold(true);
    xfadeLabelB->setFont(f);
}
xfadeLabelB->setStyleSheet(QStringLiteral(
    "color: #2080e0; background: transparent;"));
xfadeRow->addWidget(xfadeLabelB);

deckRowLayout->addLayout(xfadeRow);

pageSplitter->addWidget(deckWidget);
pageSplitter->addWidget(djBrowser_);
pageSplitter->setSizes({760, 260});
layout->addWidget(pageSplitter, 1);

QObject::connect(djCrossfader_, &QSlider::valueChanged, this, [this](int value) {
    bridge_.setCrossfader(static_cast<double>(value) / 1000.0);
});

// ── Master section cue controls wiring ──
QObject::connect(djCueMix_, &QSlider::valueChanged, this, [this](int value) {
    bridge_.setCueMix(static_cast<double>(value) / 1000.0);
});

QObject::connect(djCueVol_, &QSlider::valueChanged, this, [this](int value) {
    bridge_.setCueVolume(static_cast<double>(value) / 1000.0);
});

// ── Output mode toggle ──
QObject::connect(djOutputModeBtn_, &QPushButton::toggled, this, [this](bool checked) {
    const int mode = checked ? 1 : 0;
    bridge_.setOutputMode(mode);
    djOutputModeBtn_->setText(checked
        ? QStringLiteral("Split Mono")
        : QStringLiteral("Stereo"));
    qInfo().noquote() << QStringLiteral("DJ_OUTPUT_MODE=%1").arg(mode);
});

// ── Device switch result logging (combo removed — signal still used for diagnostics) ──
QObject::connect(&bridge_, &EngineBridge::deviceSwitchFinished, this,
    [](bool ok, const QString& activeDevice, long long elapsedMs) {
    qInfo().noquote() << QStringLiteral("DJ_DEVICE_SWITCH_DONE ok=%1 active='%2' [%3ms]")
        .arg(ok).arg(activeDevice).arg(elapsedMs);
});

// Audio profile applied result is handled internally by PlayerPage
// via bridge_.audioProfileApplied connected in PlayerPage constructor.

// ── UI heartbeat — detects main-thread freezes ──
{
    auto* hb = new QTimer(this);
    auto* lastBeat = new qint64(QDateTime::currentMSecsSinceEpoch());
    connect(hb, &QTimer::timeout, this, [lastBeat]() {
        const qint64 now = QDateTime::currentMSecsSinceEpoch();
        const qint64 gap = now - *lastBeat;
        if (gap > 400) {
            const unsigned long tid = GetCurrentThreadId();
            qWarning().noquote() << QStringLiteral("UI_HEARTBEAT: FREEZE gap=%1ms tid=%2")
                .arg(gap).arg(tid);
        }
        *lastBeat = now;
    });
    hb->start(200);
}

// ── DeckStrip LOAD buttons: library is offline, no-op ──
QObject::connect(djDeckA_, &DeckStrip::loadRequested, this, [](int) {});
QObject::connect(djDeckB_, &DeckStrip::loadRequested, this, [](int) {});
// ── DeckStrip drag-to-deck: load track by track_id ──
QObject::connect(djDeckA_, &DeckStrip::loadTrackRequested, this,
    [this](int /*deckIndex*/, qint64 trackId) {
    auto optTrack = db_.trackById(trackId);
    if (optTrack.has_value()) {
        loadDeckTrack(0, optTrack->filePath);
    }
});
QObject::connect(djDeckB_, &DeckStrip::loadTrackRequested, this,
    [this](int /*deckIndex*/, qint64 trackId) {
    auto optTrack = db_.trackById(trackId);
    if (optTrack.has_value()) {
        loadDeckTrack(1, optTrack->filePath);
    }
});

// ── DeckStrip file-drag (from DjBrowserPane) ──
QObject::connect(djDeckA_, &DeckStrip::loadFileRequested, this,
    [this](int /*deckIndex*/, const QString& path) {
        loadDeckTrack(0, path);
    });
QObject::connect(djDeckB_, &DeckStrip::loadFileRequested, this,
    [this](int /*deckIndex*/, const QString& path) {
        loadDeckTrack(1, path);
    });

// Wire snapshot refresh
QObject::connect(&bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
    if (djDeckA_) djDeckA_->refreshFromSnapshot();
    if (djDeckB_) djDeckB_->refreshFromSnapshot();
    if (djMasterMeterL_) djMasterMeterL_->setLevel(static_cast<float>(bridge_.masterPeakL()));
    if (djMasterMeterR_) djMasterMeterR_->setLevel(static_cast<float>(bridge_.masterPeakR()));
    syncDeckLiveAnalysis(0);
    syncDeckLiveAnalysis(1);
});

// ── Device-lost overlay banner + Recover Audio button ──
djDeviceLostBanner_ = new QWidget(this);
djDeviceLostBanner_->setVisible(false);
djDeviceLostBanner_->setStyleSheet(QStringLiteral(
    "background: rgba(180,30,30,220); border: 2px solid #ff4444;"
    " border-radius: 6px;"));
auto* bannerLayout = new QVBoxLayout(djDeviceLostBanner_);
bannerLayout->setContentsMargins(12, 8, 12, 8);
bannerLayout->setSpacing(6);

djBannerTitleLabel_ = new QLabel(
    QStringLiteral("OUTPUT LOST!!!!   RECONNECT IMMEDIATELY!!!!!!!!!"), djDeviceLostBanner_);
{
    QFont f = djBannerTitleLabel_->font(); f.setPointSize(18); f.setBold(true);
    djBannerTitleLabel_->setFont(f);
}
djBannerTitleLabel_->setAlignment(Qt::AlignCenter);
djBannerTitleLabel_->setStyleSheet(QStringLiteral(
    "color: #ffffff; background: transparent;"));
bannerLayout->addWidget(djBannerTitleLabel_);

djRecoveryStatusLabel_ = new QLabel(QString(), djDeviceLostBanner_);
djRecoveryStatusLabel_->setVisible(false);
bannerLayout->addWidget(djRecoveryStatusLabel_);

djRecoverBtn_ = new QPushButton(QString(), djDeviceLostBanner_);
djRecoverBtn_->setVisible(false);
djRecoverBtn_->setFixedHeight(0);
bannerLayout->addWidget(djRecoverBtn_);

layout->addWidget(djDeviceLostBanner_);

// ── Wire djDeviceLost signal → show banner ──
QObject::connect(&bridge_, &EngineBridge::djDeviceLost, this, [this]() {
    // Stop any pending green-banner dismiss timer
    if (djBannerDismissTimer_) djBannerDismissTimer_->stop();
    if (djDeviceLostBanner_) {
        djDeviceLostBanner_->setVisible(true);
        djDeviceLostBanner_->setStyleSheet(QStringLiteral(
            "background: rgba(180,30,30,220); border: 2px solid #ff4444;"
            " border-radius: 6px;"));
    }
    if (djBannerTitleLabel_) djBannerTitleLabel_->setText(
        QStringLiteral("OUTPUT LOST!!!!   RECONNECT IMMEDIATELY!!!!!!!!!"));
    if (djRecoverBtn_) djRecoverBtn_->setVisible(false);
    if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setVisible(false);
});

// ── Wire Recover Audio button → attemptDjRecovery ──
QObject::connect(djRecoverBtn_, &QPushButton::clicked, this, [this]() {
    if (djRecoverBtn_) djRecoverBtn_->setEnabled(false);
    if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setText(
        QStringLiteral("Attempting recovery..."));
    bridge_.attemptDjRecovery();
});

// ── Wire recovery result signals ──
QObject::connect(&bridge_, &EngineBridge::djRecoverySuccess, this,
    [this](const QString& activeDevice) {
    if (djDeviceLostBanner_) djDeviceLostBanner_->setVisible(false);
    qInfo().noquote() << QStringLiteral("DJ_RECOVERY_UI: success device='%1'")
        .arg(activeDevice);
});

QObject::connect(&bridge_, &EngineBridge::djRecoveryFailed, this,
    [this](const QString& reason) {
    if (djRecoverBtn_) djRecoverBtn_->setEnabled(true);
    if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setText(
        QStringLiteral("Recovery failed:\n%1").arg(reason));
    qWarning().noquote() << QStringLiteral("DJ_RECOVERY_UI: failed reason='%1'")
        .arg(reason);
});

// ── Wire auto-recovery success → green banner + auto-dismiss ──
djBannerDismissTimer_ = new QTimer(this);
djBannerDismissTimer_->setSingleShot(true);
QObject::connect(djBannerDismissTimer_, &QTimer::timeout, this, [this]() {
    if (djDeviceLostBanner_) djDeviceLostBanner_->setVisible(false);
    qInfo().noquote() << QStringLiteral("DJ_BANNER_HIDE_GREEN");
});

QObject::connect(&bridge_, &EngineBridge::djAutoRecoverySuccess, this,
    [this](const QString& activeDevice, bool wasPlaying) {
    if (djDeviceLostBanner_) {
        djDeviceLostBanner_->setVisible(true);
        djDeviceLostBanner_->setStyleSheet(QStringLiteral(
            "background: rgba(40,100,50,220); border: 2px solid #44aa55;"
            " border-radius: 6px;"));
    }
    if (djBannerTitleLabel_) djBannerTitleLabel_->setText(
        QStringLiteral("CONNECTION RESTORED"));
    if (djRecoverBtn_) djRecoverBtn_->setVisible(false);
    if (djRecoveryStatusLabel_) djRecoveryStatusLabel_->setVisible(false);
    qInfo().noquote() << QStringLiteral("DJ_BANNER_SHOW_GREEN device='%1' wasPlaying=%2")
        .arg(activeDevice).arg(wasPlaying ? 1 : 0);

    // Auto-dismiss after 3 seconds
    djBannerDismissTimer_->start(3000);
});
}

void DjModePage::setBrowserRootFolder(const QString& folderPath)
{
    if (djBrowser_) djBrowser_->setBrowserRootFolder(folderPath);
}

void DjModePage::wireDeckAnalysisBridge(AnalysisBridge* analysisBridge, DeckStrip* deckWidget, int deckIndex)
{
    if (!analysisBridge || !deckWidget) return;

    QObject::connect(analysisBridge, &AnalysisBridge::bridgeReady, this,
        [this, analysisBridge, deckIndex]() {
            QString* pendingPath = pendingDeckAnalysisPath(deckIndex);
            if (!pendingPath || pendingPath->isEmpty()) return;
            analysisBridge->selectTrack(*pendingPath);
        });

    QObject::connect(analysisBridge, &AnalysisBridge::panelStateChanged, this,
        [deckWidget](const QJsonObject& panel) {
            deckWidget->updateAnalysisPanel(panel);
        });

    QObject::connect(analysisBridge, &AnalysisBridge::bridgeError, this,
        [deckIndex](const QString& error) {
            qWarning().noquote() << QStringLiteral("DJ_DECK_ANALYSIS_ERROR deck=%1 error=%2")
                .arg(deckIndex)
                .arg(error);
        });
}

void DjModePage::loadDeckTrack(int deckIndex, const QString& path)
{
    if (path.trimmed().isEmpty()) return;

    const QString normalizedPath = QDir::fromNativeSeparators(QFileInfo(path).absoluteFilePath());
    bridge_.loadTrackToDeck(deckIndex, normalizedPath);

    DeckStrip* deck = (deckIndex == 0) ? djDeckA_ : djDeckB_;
    if (deck) {
        const auto optTrack = db_.trackByPath(normalizedPath);
        if (optTrack.has_value()) {
            deck->setTrackMetadata(optTrack->title, optTrack->artist,
                                   optTrack->bpm, optTrack->musicalKey,
                                   optTrack->durationStr);
        } else {
            const QFileInfo info(normalizedPath);
            deck->setTrackMetadata(info.completeBaseName(), QString(), QString(), QString(), QString());
        }
    }

    startDeckLiveAnalysis(deckIndex, normalizedPath);
}

void DjModePage::startDeckLiveAnalysis(int deckIndex, const QString& path)
{
    AnalysisBridge* analysisBridge = deckAnalysisBridge(deckIndex);
    QString* pendingPath = pendingDeckAnalysisPath(deckIndex);
    if (!analysisBridge || !pendingPath) return;

    const QString normalizedPath = path.trimmed();
    *pendingPath = normalizedPath;
    if (normalizedPath.isEmpty()) {
        if (analysisBridge->isReady()) analysisBridge->unselectTrack();
        return;
    }

    if (analysisBridge->isReady()) {
        analysisBridge->selectTrack(normalizedPath);
        return;
    }

    analysisBridge->start();
}

void DjModePage::syncDeckLiveAnalysis(int deckIndex)
{
    AnalysisBridge* analysisBridge = deckAnalysisBridge(deckIndex);
    QString* pendingPath = pendingDeckAnalysisPath(deckIndex);
    if (!analysisBridge || !pendingPath) return;

    const QString rawPath = bridge_.deckFilePath(deckIndex).trimmed();
    if (rawPath.isEmpty()) {
        if (!pendingPath->isEmpty()) {
            startDeckLiveAnalysis(deckIndex, QString());
        }
        return;
    }

    if (pendingPath->isEmpty()) return;

    const QString currentPath = QDir::fromNativeSeparators(QFileInfo(rawPath).absoluteFilePath());
    if (*pendingPath != currentPath) return;

    if (analysisBridge->isReady()) {
        analysisBridge->resolvePlayhead(bridge_.deckPlayhead(deckIndex));
    }
}

AnalysisBridge* DjModePage::deckAnalysisBridge(int deckIndex) const
{
    return deckIndex == 0 ? deckAnalysisBridgeA_ : deckAnalysisBridgeB_;
}

QString* DjModePage::pendingDeckAnalysisPath(int deckIndex)
{
    return deckIndex == 0 ? &deckAnalysisPathA_ : &deckAnalysisPathB_;
}

void DjModePage::setImportUiState(const QString& title,
                                  const QString& detail,
                                  bool importEnabled,
                                  bool runAnalysisEnabled)
{
    if (!djBrowser_) return;
    djBrowser_->setImportUiState(title, detail, importEnabled, runAnalysisEnabled);
    if (importFolderAction_) {
        importFolderAction_->setEnabled(importEnabled);
        importFolderAction_->setToolTip(title + QStringLiteral("\n") + detail);
    }
    if (importAnalysisAction_) {
        importAnalysisAction_->setEnabled(runAnalysisEnabled);
        importAnalysisAction_->setToolTip(title + QStringLiteral("\n") + detail);
    }
}

void DjModePage::openAncillaryScreens()
{
    if (!ancillaryWidget_) {
        ancillaryWidget_ = new AncillaryScreensWidget(&bridge_);
        ancillaryWidget_->setWindowTitle(QStringLiteral("Ancillary Screens"));
        ancillaryWidget_->setAttribute(Qt::WA_DeleteOnClose, false);
    }
    ancillaryWidget_->show();
    ancillaryWidget_->raise();
    ancillaryWidget_->activateWindow();
}

void DjModePage::showProAudioClipperPlaceholder()
{
    QMessageBox::information(this, QStringLiteral("Coming Soon"), QStringLiteral("ProAudioClipper integration is a placeholder."));
}

void DjModePage::setTrackList(const std::vector<TrackInfo>* tracks)
{
    tracks_ = tracks;
}
