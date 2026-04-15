#include "ui/DjModePage.h"

#include <QDateTime>
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
layout->setContentsMargins(8, 8, 8, 8);
layout->setSpacing(4);

// ── Header row: Back + title ──
auto* headerRow = new QHBoxLayout();
headerRow->setSpacing(6);
auto* backBtn = new QPushButton(QStringLiteral("\u2190 Back"), this);
backBtn->setCursor(Qt::PointingHandCursor);
backBtn->setStyleSheet(QStringLiteral(
    "QPushButton { background: rgba(20,20,30,200); border: 1px solid #333;"
    "  border-radius: 4px; color: #aaa; font-size: 9px; padding: 4px 10px; }"
    "QPushButton:hover { background: rgba(40,40,60,220); color: #ddd; }"));
headerRow->addWidget(backBtn);

auto* title = new QLabel(QStringLiteral("DJ MIXER"), this);
{
    QFont f = title->font();
    f.setPointSize(14);
    f.setBold(true);
    title->setFont(f);
}
title->setStyleSheet(QStringLiteral("color: #e0e0e0; background: transparent;"));
title->setAlignment(Qt::AlignCenter);
headerRow->addWidget(title, 1);

auto* proAudioClipperBtn = new QPushButton(QStringLiteral("ProAudioClipper"), this);
proAudioClipperBtn->setStyleSheet(QStringLiteral(
    "QPushButton { background: rgba(20,20,30,200); border: 1px solid #444;"
    "border-radius: 4px; color: #888; font-size: 11px; padding: 6px 12px; }"
    "QPushButton:hover { background: rgba(40,40,60,220); color: #ddd; }"
));
proAudioClipperBtn->setMinimumHeight(28);
headerRow->addWidget(proAudioClipperBtn);
QObject::connect(proAudioClipperBtn, &QPushButton::clicked, this, [this]() {
    QMessageBox::information(this, QStringLiteral("Coming Soon"), QStringLiteral("ProAudioClipper integration is a placeholder."));
});

auto* ancillaryBtn = new QPushButton(QStringLiteral("Ancillary Screens"), this);
ancillaryBtn->setStyleSheet(QStringLiteral(
    "QPushButton { background: #b35900; border: 1px solid #ff8000;"
    "border-radius: 4px; color: #fff; font-size: 11px; padding: 6px 12px; }"
    "QPushButton:hover { background: #d96600; }"
));
ancillaryBtn->setMinimumHeight(28);
headerRow->addWidget(ancillaryBtn);
QObject::connect(ancillaryBtn, &QPushButton::clicked, this, [this]() {
    if (!ancillaryWidget_) {
        ancillaryWidget_ = new AncillaryScreensWidget(&bridge_);
        ancillaryWidget_->setWindowTitle(QStringLiteral("Ancillary Screens"));
        ancillaryWidget_->setAttribute(Qt::WA_DeleteOnClose, false);
    }
    ancillaryWidget_->show();
    ancillaryWidget_->raise();
    ancillaryWidget_->activateWindow();
});

headerRow->addSpacing(60);  // balance the back button
layout->addLayout(headerRow);

QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
    bridge_.leaveDjMode();
    emit backRequested();
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

deckRow->addLayout(colB, 5);



auto* deckSplitter = new QSplitter(Qt::Horizontal, this);
auto* djBrowser = new DjBrowserPane(&db_, this);
deckSplitter->addWidget(djBrowser);

// -- Browser context menu -> load to deck --
QObject::connect(djBrowser, &DjBrowserPane::loadToDeckRequested, this,
    [this](int deckIdx, const QString& path) {
        bridge_.loadTrackToDeck(deckIdx, path);
        auto optTrack = db_.trackByPath(path);
        if (optTrack.has_value()) {
            if (deckIdx == 0 && djDeckA_)
                djDeckA_->setTrackMetadata(optTrack->title, optTrack->artist,
                                           optTrack->bpm, optTrack->musicalKey,
                                           optTrack->durationStr);
            else if (deckIdx == 1 && djDeckB_)
                djDeckB_->setTrackMetadata(optTrack->title, optTrack->artist,
                                           optTrack->bpm, optTrack->musicalKey,
                                           optTrack->durationStr);
        }
    });

auto* deckWidget = new QWidget(this);
auto* deckRowLayout = new QVBoxLayout(deckWidget);
deckRowLayout->setContentsMargins(0,0,0,0);
deckRowLayout->addLayout(deckRow);

deckSplitter->addWidget(deckWidget);
deckSplitter->setSizes({250, 1000});
layout->addWidget(deckSplitter, 1);

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

layout->addLayout(xfadeRow);

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
        bridge_.loadTrackToDeck(0, optTrack->filePath);
        djDeckA_->setTrackMetadata(optTrack->title, optTrack->artist,
                                   optTrack->bpm, optTrack->musicalKey,
                                   optTrack->durationStr);
    }
});
QObject::connect(djDeckB_, &DeckStrip::loadTrackRequested, this,
    [this](int /*deckIndex*/, qint64 trackId) {
    auto optTrack = db_.trackById(trackId);
    if (optTrack.has_value()) {
        bridge_.loadTrackToDeck(1, optTrack->filePath);
        djDeckB_->setTrackMetadata(optTrack->title, optTrack->artist,
                                   optTrack->bpm, optTrack->musicalKey,
                                   optTrack->durationStr);
    }
});

// ── DeckStrip file-drag (from DjBrowserPane) ──
QObject::connect(djDeckA_, &DeckStrip::loadFileRequested, this,
    [this](int /*deckIndex*/, const QString& path) {
        bridge_.loadTrackToDeck(0, path);
        auto optTrack = db_.trackByPath(path);
        if (optTrack.has_value())
            djDeckA_->setTrackMetadata(optTrack->title, optTrack->artist,
                                       optTrack->bpm, optTrack->musicalKey,
                                       optTrack->durationStr);
    });
QObject::connect(djDeckB_, &DeckStrip::loadFileRequested, this,
    [this](int /*deckIndex*/, const QString& path) {
        bridge_.loadTrackToDeck(1, path);
        auto optTrack = db_.trackByPath(path);
        if (optTrack.has_value())
            djDeckB_->setTrackMetadata(optTrack->title, optTrack->artist,
                                       optTrack->bpm, optTrack->musicalKey,
                                       optTrack->durationStr);
    });

// Wire snapshot refresh
QObject::connect(&bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
    if (djDeckA_) djDeckA_->refreshFromSnapshot();
    if (djDeckB_) djDeckB_->refreshFromSnapshot();
    if (djMasterMeterL_) djMasterMeterL_->setLevel(static_cast<float>(bridge_.masterPeakL()));
    if (djMasterMeterR_) djMasterMeterR_->setLevel(static_cast<float>(bridge_.masterPeakR()));
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

void DjModePage::setTrackList(const std::vector<TrackInfo>* tracks)
{
    tracks_ = tracks;
}
