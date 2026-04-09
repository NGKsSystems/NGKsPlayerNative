#pragma once

#include <QApplication>
#include <QButtonGroup>
#include <QBrush>
#include <QCheckBox>
#include <QClipboard>
#include <QColor>
#include <QComboBox>
#include <QDateTime>
#include <QDesktopServices>
#include <QDialog>
#include <QDir>
#include <QFile>
#include <QFileDialog>
#include <QFormLayout>
#include <QFrame>
#include <QGridLayout>
#include <QGroupBox>
#include <QHBoxLayout>
#include <QHostInfo>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QLabel>
#include <QLineEdit>
#include <QListWidget>
#include <QMessageBox>
#include <QPixmap>
#include <QPlainTextEdit>
#include <QProcess>
#include <QProgressBar>
#include <QPushButton>
#include <QRadioButton>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QStackedWidget>
#include <QStandardPaths>
#include <QSignalBlocker>
#include <QTabWidget>
#include <QTimer>
#include <QToolButton>
#include <QTreeWidget>
#include <QUuid>
#include <QVariantMap>
#include <QVBoxLayout>
#include <QWidget>

#include <functional>
#include <map>

#include "ui/CrowdRequestBackend.h"
#include "ui/EngineBridge.h"

class AncillaryScreensWidget : public QWidget
{
public:
        void setBackRequestedHandler(std::function<void()> handler) { backRequestedHandler_ = std::move(handler); }
    using SearchProvider = std::function<QList<QVariantMap>(const QString&, const QString&)>;
    struct PendingHandoffVerification {
        QString requestId;
        QString requestedTitle;
        int deckIndex{0};
        QString expectedPath;
        qint64 startedAtMs{0};
        qint64 deadlineAtMs{0};
        bool pathSeen{false};
    };
    struct TrackLoadResult {
        bool dispatched{false};
        QString filePath;
        QString reason;
        QString title;
        QString artist;
    };
    using TrackLoader = std::function<TrackLoadResult(const QVariantMap&, int)>;
    using NowPlayingProvider = std::function<QVariantMap()>;

    ~AncillaryScreensWidget() override
    {
        shutdownRequestServer(true);
    }

    explicit AncillaryScreensWidget(EngineBridge* bridge, QWidget* parent = nullptr)
        : QWidget(parent), bridge_(bridge)
    {
        buildUi();
        crowdBackend_ = new CrowdRequestBackend(this);
        requestSettingsAutoSaveTimer_.setSingleShot(true);
        requestSettingsAutoSaveTimer_.setInterval(450);
        QObject::connect(&requestSettingsAutoSaveTimer_, &QTimer::timeout, this, [this]() {
            saveRequestSettings();
        });
        requestPollTimer_.setInterval(3000);
        QObject::connect(&requestPollTimer_, &QTimer::timeout, this, [this]() { refreshRequestQueue(); });
        requestRefreshDebounceTimer_.setSingleShot(true);
        requestRefreshDebounceTimer_.setInterval(120);
        QObject::connect(&requestRefreshDebounceTimer_, &QTimer::timeout, this, [this]() { refreshRequestQueue(false); });
        nowPlayingSyncDebounceTimer_.setSingleShot(true);
        nowPlayingSyncDebounceTimer_.setInterval(75);
        QObject::connect(&nowPlayingSyncDebounceTimer_, &QTimer::timeout, this, [this]() {
            if (syncGuestNowPlaying()) {
                scheduleRequestRefreshFromDeckEvent();
            }
        });
        handoffFallbackTimer_.setInterval(400);
        QObject::connect(&handoffFallbackTimer_, &QTimer::timeout, this, [this]() {
            processPendingHandoffVerifications(true);
        });
        if (bridge_) {
            QObject::connect(bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
                onDeckSnapshotUpdated();
            });
            QObject::connect(bridge_, &EngineBridge::deviceSwitchFinished, this,
                [this](bool ok, const QString& device, long long ms) {
                    if (hardwareSwitchStatusLabel_) {
                        hardwareSwitchStatusLabel_->setText(ok
                            ? QStringLiteral("Switched to %1 in %2 ms.").arg(device).arg(ms)
                            : QStringLiteral("Switch failed \u2014 active device: %1.").arg(device));
                    }
                    refreshHardwareDevices();
                });
        }
        refreshHardwareDevices();
        loadStreamingKeys();
        refreshStreamingServices();
        loadRequestSettings();
        loadHardwareSettings();
        loadLiveStreamSettings();
        refreshRequestQueue();
        refreshBroadcastStatus();
        switchSection(0);
    }

    void setSearchProvider(SearchProvider provider) { searchProvider_ = std::move(provider); }
    void setTrackLoader(TrackLoader loader) { trackLoader_ = std::move(loader); }
    void setNowPlayingProvider(NowPlayingProvider provider) { nowPlayingProvider_ = std::move(provider); }


    QPushButton* backButton_{nullptr};
    std::function<void()> backRequestedHandler_;

    QWidget* makeSectionCard(const QString& title, const QString& subtitle = QString())
    {
        auto* card = new QFrame(this);
        card->setObjectName(QStringLiteral("jukeboxSectionCard"));
        card->setStyleSheet(QStringLiteral(
            "QFrame#jukeboxSectionCard {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 rgba(54,20,8,240), stop:0.55 rgba(34,10,6,238), stop:1 rgba(18,6,4,244));"
            " border:2px solid rgba(208,167,94,170); border-radius:18px; }"
            "QLabel[role='title'] { color:#ffe0a3; font-size:16px; font-weight:800; letter-spacing:0.5px; }"
            "QLabel[role='subtitle'] { color:#f6c98a; font-size:11px; }"));
        auto* layout = new QVBoxLayout(card);
        layout->setContentsMargins(18, 16, 18, 16);
        layout->setSpacing(12);
        auto* titleLabel = new QLabel(title, card);
        titleLabel->setProperty("role", QStringLiteral("title"));
        layout->addWidget(titleLabel);
        if (!subtitle.isEmpty()) {
            auto* subtitleLabel = new QLabel(subtitle, card);
            subtitleLabel->setProperty("role", QStringLiteral("subtitle"));
            subtitleLabel->setWordWrap(true);
            layout->addWidget(subtitleLabel);
        }
        return card;
    }

    QPushButton* makeNavButton(const QString& label, int index)
    {
        auto* button = new QPushButton(label, this);
        button->setCheckable(true);
        button->setCursor(Qt::PointingHandCursor);
        button->setMinimumHeight(42);
        button->setStyleSheet(QStringLiteral(
            "QPushButton {"
            " background:#0f172a; color:#cbd5e1; border:1px solid #22304a; border-radius:10px;"
            " text-align:left; padding:10px 14px; font-size:12px; font-weight:600; }"
            "QPushButton:hover { background:#162238; border-color:#2dd4bf; }"
            "QPushButton:checked { background:#12303a; color:#ecfeff; border-color:#2dd4bf; }"));
        navGroup_->addButton(button, index);
        return button;
    }

    QPushButton* makeChoiceButton(const QString& label)
    {
        auto* button = new QPushButton(label, this);
        button->setCheckable(true);
        button->setCursor(Qt::PointingHandCursor);
        button->setMinimumHeight(54);
        button->setStyleSheet(QStringLiteral(
            "QPushButton { background:#0b1220; color:#dbe7f3; border:1px solid #22304a;"
            " border-radius:12px; padding:10px 12px; font-size:12px; font-weight:600; }"
            "QPushButton:hover { border-color:#38bdf8; }"
            "QPushButton:checked { background:#17304b; border-color:#38bdf8; color:#f8fafc; }"));
        return button;
    }

    void buildUi()
    {
        setStyleSheet(QStringLiteral(
            "QWidget { background:#120705; color:#ffe8bc; }"
            "QLabel { color:#ffe8bc; }"
            "QPushButton {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #f8bf55, stop:0.55 #e67716, stop:1 #8a2a12);"
            " color:#2f1208; border:2px solid #ffd27a; border-radius:11px; padding:8px 12px; font-weight:800; }"
            "QPushButton:hover { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #ffd27a, stop:0.6 #f08c22, stop:1 #a13414); }"
            "QPushButton:pressed { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #8a2a12, stop:1 #f08c22); color:#fff7e6; }"
            "QPushButton:disabled { background:#6f5147; color:#d7b58f; border-color:#a27c5c; }"
            "QLineEdit, QComboBox, QPlainTextEdit, QTreeWidget, QListWidget {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 rgba(58,22,14,235), stop:1 rgba(30,10,8,244));"
            " color:#fff3d2; border:2px solid rgba(215,170,96,180); border-radius:12px; }"
            "QLineEdit, QComboBox { padding:9px 12px; selection-background-color:#f59e0b; }"
            "QTreeWidget::item:selected, QListWidget::item:selected { background:rgba(249,168,37,70); }"
            "QHeaderView::section { background:#4f1d10; color:#ffe8bc; border:none; padding:8px 10px; font-weight:800; }"
            "QTabBar::tab { background:#4a170c; color:#f4c985; border:1px solid #a05a26; padding:8px 14px; margin-right:4px; border-top-left-radius:8px; border-top-right-radius:8px; }"
            "QTabBar::tab:selected { background:#8a2a12; color:#fff8e6; border-color:#ffd27a; }"
            "QCheckBox, QRadioButton { color:#ffe8bc; }"));

        auto* root = new QVBoxLayout(this);
        root->setContentsMargins(14, 10, 14, 14);
        root->setSpacing(0);

        auto* cabinet = new QFrame(this);
        cabinet->setObjectName(QStringLiteral("jukeboxCabinet"));
        cabinet->setStyleSheet(QStringLiteral(
            "QFrame#jukeboxCabinet {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #5b2212, stop:0.22 #2b0907, stop:0.78 #1b0605, stop:1 #6b2a16);"
            " border:3px solid #d5a35d; border-radius:38px; }"));
        auto* cabinetLayout = new QVBoxLayout(cabinet);
        cabinetLayout->setContentsMargins(18, 18, 18, 18);
        cabinetLayout->setSpacing(10);

        auto* marquee = new QFrame(cabinet);
        marquee->setObjectName(QStringLiteral("jukeboxMarquee"));
        marquee->setStyleSheet(QStringLiteral(
            "QFrame#jukeboxMarquee {"
            " background:qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 rgba(255,125,44,140), stop:0.5 rgba(255,205,105,180), stop:1 rgba(255,105,56,140));"
            " border:2px solid rgba(255,225,156,220); border-radius:28px; }"));
        auto* marqueeLayout = new QHBoxLayout(marquee);
        marqueeLayout->setContentsMargins(18, 10, 18, 10);
        auto* marqueeLeft = new QFrame(marquee);
        marqueeLeft->setMinimumHeight(10);
        marqueeLeft->setStyleSheet(QStringLiteral("background:qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 transparent, stop:0.45 #ff7b54, stop:1 transparent); border:none;"));
        auto* marqueeTitle = new QLabel(QStringLiteral("NGKs Jukebox Annex"), marquee);
        marqueeTitle->setStyleSheet(QStringLiteral("color:#fff7df; font-size:26px; font-weight:900; letter-spacing:1px; background:transparent;"));
        marqueeTitle->setAlignment(Qt::AlignCenter);
        auto* marqueeRight = new QFrame(marquee);
        marqueeRight->setMinimumHeight(10);
        marqueeRight->setStyleSheet(QStringLiteral("background:qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 transparent, stop:0.55 #4dd0e1, stop:1 transparent); border:none;"));
        marqueeLayout->addWidget(marqueeLeft, 1);
        marqueeLayout->addWidget(marqueeTitle, 0);
        marqueeLayout->addWidget(marqueeRight, 1);
        cabinetLayout->addWidget(marquee);

        auto* cabinetBody = new QFrame(cabinet);
        cabinetBody->setObjectName(QStringLiteral("jukeboxBody"));
        cabinetBody->setStyleSheet(QStringLiteral(
            "QFrame#jukeboxBody { background:rgba(24,8,6,210); border:2px solid rgba(212,169,100,120); border-radius:26px; }"));
        auto* bodyLayout = new QHBoxLayout(cabinetBody);
        bodyLayout->setContentsMargins(14, 14, 14, 14);
        bodyLayout->setSpacing(16);

        auto* rail = new QFrame(cabinetBody);
        rail->setMinimumWidth(260);
        rail->setMaximumWidth(320);
        rail->setObjectName(QStringLiteral("jukeboxRail"));
        rail->setStyleSheet(QStringLiteral(
            "QFrame#jukeboxRail {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 rgba(73,24,14,245), stop:1 rgba(26,8,6,244));"
            " border:2px solid rgba(215,170,96,160); border-radius:22px; }"));
        auto* railLayout = new QVBoxLayout(rail);
        railLayout->setContentsMargins(18, 18, 18, 18);
        railLayout->setSpacing(10);

        // Back button at the top
        backButton_ = new QPushButton(QStringLiteral("Back"), rail);
        backButton_->setCursor(Qt::PointingHandCursor);
        backButton_->setStyleSheet(QStringLiteral(
            "QPushButton { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #d7dee7, stop:1 #7b8797); color:#2a0d08;"
            " font-size:13px; font-weight:900; border:2px solid #f6f1de; border-radius:10px; padding:8px 16px; }"
            "QPushButton:hover { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #eff4f8, stop:1 #9aa6b7); }"));
        railLayout->addWidget(backButton_);
        QObject::connect(backButton_, &QPushButton::clicked, this, [this]() {
            if (backRequestedHandler_) {
                backRequestedHandler_();
            }
        });

        auto* title = new QLabel(QStringLiteral("Ancillary Screens"), rail);
        title->setStyleSheet(QStringLiteral("color:#fff1c1; font-size:24px; font-weight:900;"));
        railLayout->addWidget(title);

        auto* subtitle = new QLabel(QStringLiteral("Hardware integration, crowd requests, streaming services, and live streaming controls."), rail);
        subtitle->setWordWrap(true);
        subtitle->setStyleSheet(QStringLiteral("color:#f7c486; font-size:12px; line-height:1.4;"));
        railLayout->addWidget(subtitle);

        navGroup_ = new QButtonGroup(this);
        navGroup_->setExclusive(true);
        railLayout->addSpacing(8);
        railLayout->addWidget(makeNavButton(QStringLiteral("Hardware Integration"), 0));
        railLayout->addWidget(makeNavButton(QStringLiteral("Crowd Requests"), 1));
        railLayout->addWidget(makeNavButton(QStringLiteral("Streaming Music Services"), 2));
        railLayout->addWidget(makeNavButton(QStringLiteral("Live Streaming Setup"), 3));

        railLayout->addStretch(1);

        contentStack_ = new QStackedWidget(cabinetBody);
        contentStack_->setStyleSheet(QStringLiteral("QStackedWidget { background:transparent; }"));
        contentStack_->addWidget(buildHardwarePage());
        contentStack_->addWidget(buildCrowdRequestsPage());
        contentStack_->addWidget(buildStreamingPage());
        contentStack_->addWidget(buildLiveStreamingPage());

        QObject::connect(navGroup_, QOverload<int>::of(&QButtonGroup::idClicked), this,
                         [this](int id) { switchSection(id); });

        bodyLayout->addWidget(rail);
        bodyLayout->addWidget(contentStack_, 1);
        cabinetLayout->addWidget(cabinetBody, 1);
        root->addWidget(cabinet, 1);
    }

    QWidget* buildHardwarePage()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(14);

        auto* hero = makeSectionCard(
            QStringLiteral("Hardware Integration"),
            QStringLiteral("Audio device scanning, active output switching, MIDI device readiness, and DVS configuration."));
        auto* heroLayout = qobject_cast<QVBoxLayout*>(hero->layout());
        auto* metrics = new QGridLayout();
        metrics->setHorizontalSpacing(12);
        metrics->setVerticalSpacing(12);

        hardwareDeviceCountLabel_ = new QLabel(QStringLiteral("0"), hero);
        hardwareDeviceCountLabel_->setStyleSheet(QStringLiteral("color:#f8fafc; font-size:28px; font-weight:800;"));
        hardwareActiveDeviceLabel_ = new QLabel(QStringLiteral("No active device"), hero);
        hardwareActiveDeviceLabel_->setStyleSheet(QStringLiteral("color:#f8fafc; font-size:18px; font-weight:700;"));
        hardwareSwitchStatusLabel_ = new QLabel(QStringLiteral("Ready to scan for devices."), hero);
        hardwareSwitchStatusLabel_->setStyleSheet(QStringLiteral("color:#8aa0bf; font-size:12px;"));

        metrics->addWidget(new QLabel(QStringLiteral("Detected Outputs"), hero), 0, 0);
        metrics->addWidget(new QLabel(QStringLiteral("Active Device"), hero), 0, 1);
        metrics->addWidget(new QLabel(QStringLiteral("Switch Status"), hero), 0, 2);
        metrics->addWidget(hardwareDeviceCountLabel_, 1, 0);
        metrics->addWidget(hardwareActiveDeviceLabel_, 1, 1);
        metrics->addWidget(hardwareSwitchStatusLabel_, 1, 2);
        heroLayout->addLayout(metrics);
        layout->addWidget(hero);

        auto* tabs = new QTabWidget(page);
        tabs->addTab(buildHardwareControllersTab(), QStringLiteral("Controllers"));
        tabs->addTab(buildHardwareDVSTab(), QStringLiteral("DVS"));
        tabs->addTab(buildHardwareMIDITab(), QStringLiteral("MIDI"));
        tabs->addTab(buildHardwareSettingsTab(), QStringLiteral("Settings"));
        layout->addWidget(tabs, 1);
        return page;
    }

    QWidget* buildHardwareControllersTab()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 8, 0, 0);
        layout->setSpacing(10);

        // ── Activity metrics ──────────────────────────────────────────────
        auto* statsCard = makeSectionCard(
            QStringLiteral("Controller Activity"),
            QStringLiteral("Live metrics from the hardware detection engine."));
        auto* statsCardLayout = qobject_cast<QVBoxLayout*>(statsCard->layout());
        auto* statsRow = new QHBoxLayout();
        statsRow->setSpacing(10);

        auto makeStatBox = [](const QString& label, const QString& val,
                               QLabel*& outLabel, QWidget* parent) -> QFrame* {
            auto* box = new QFrame(parent);
            box->setStyleSheet(QStringLiteral(
                "QFrame { background:#111827; border:1px solid #1e3348; border-radius:8px; }"));
            auto* bl = new QVBoxLayout(box);
            bl->setContentsMargins(12, 8, 12, 8);
            bl->setSpacing(2);
            outLabel = new QLabel(val, box);
            outLabel->setStyleSheet(QStringLiteral(
                "color:#38bdf8; font-size:22px; font-weight:900;"
                " background:transparent; border:none;"));
            auto* lbl = new QLabel(label, box);
            lbl->setStyleSheet(QStringLiteral(
                "color:#6b7280; font-size:10px; font-weight:600;"
                " background:transparent; border:none;"));
            bl->addWidget(outLabel);
            bl->addWidget(lbl);
            return box;
        };

        statsRow->addWidget(makeStatBox(QStringLiteral("CONNECTED"),      QStringLiteral("0"),  hwCtrlConnectedLabel_,  statsCard));
        statsRow->addWidget(makeStatBox(QStringLiteral("MESSAGES / s"),   QStringLiteral("0"),  hwCtrlMsgLabel_,        statsCard));
        statsRow->addWidget(makeStatBox(QStringLiteral("MAPPINGS ACTIVE"), QStringLiteral("0"), hwCtrlMappingsLabel_,   statsCard));
        statsRow->addWidget(makeStatBox(QStringLiteral("LATENCY (ms)"),   QStringLiteral("--"), hwCtrlLatencyLabel_,    statsCard));
        statsCardLayout->addLayout(statsRow);
        layout->addWidget(statsCard);

        // ── Device selection & switch ─────────────────────────────────────
        auto* devCard = makeSectionCard(
            QStringLiteral("Audio Output Device"),
            QStringLiteral("Scan and switch the active engine output device."));
        auto* devLayout = qobject_cast<QVBoxLayout*>(devCard->layout());

        auto* actions = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("Scan for Devices"), devCard);
        auto* applyButton   = new QPushButton(QStringLiteral("Switch Active Device"), devCard);
        hardwareDeviceCombo_ = new QComboBox(devCard);
        hardwareDeviceCombo_->setMinimumWidth(260);
        actions->addWidget(refreshButton);
        actions->addWidget(hardwareDeviceCombo_, 1);
        actions->addWidget(applyButton);
        devLayout->addLayout(actions);

        hardwareDeviceList_ = new QListWidget(devCard);
        hardwareDeviceList_->setMinimumHeight(72);
        hardwareDeviceList_->setMaximumHeight(110);
        devLayout->addWidget(hardwareDeviceList_);
        layout->addWidget(devCard);

        // ── Supported controller library ──────────────────────────────────
        auto* libCard = makeSectionCard(
            QStringLiteral("Controller Library"),
            QStringLiteral("Known DJ controllers. Detection is via WASAPI/ASIO audio output enumeration -- no driver install or MIDI bridge available yet."));
        auto* libLayout = qobject_cast<QVBoxLayout*>(libCard->layout());

        const QList<QPair<QString, QStringList>> families = {
            { QStringLiteral("Pioneer"),
              { QStringLiteral("DDJ-SB3"), QStringLiteral("DDJ-FLX4"),
                QStringLiteral("DDJ-FLX6"), QStringLiteral("DDJ-SR2"),
                QStringLiteral("DDJ-800"), QStringLiteral("DDJ-1000") } },
            { QStringLiteral("Native Instruments"),
              { QStringLiteral("Traktor S2 MK3"), QStringLiteral("Traktor S4 MK3"),
                QStringLiteral("Kontrol X1"), QStringLiteral("Kontrol Z1") } },
            { QStringLiteral("Denon DJ"),
              { QStringLiteral("MC7000"), QStringLiteral("MC6000"),
                QStringLiteral("MC4000") } },
            { QStringLiteral("Numark"),
              { QStringLiteral("Mixtrack Pro III"), QStringLiteral("Mixtrack Platinum"),
                QStringLiteral("NV II"), QStringLiteral("NS7 III") } },
            { QStringLiteral("Other"),
              { QStringLiteral("Rane One"), QStringLiteral("Rane Twelve"),
                QStringLiteral("Any ASIO/WASAPI output") } }
        };

        auto* famGrid = new QGridLayout();
        famGrid->setHorizontalSpacing(16);
        famGrid->setVerticalSpacing(6);
        int famCol = 0;
        for (const auto& fam : families) {
            auto* famWidget = new QWidget(libCard);
            auto* famLayout = new QVBoxLayout(famWidget);
            famLayout->setContentsMargins(0, 0, 0, 0);
            famLayout->setSpacing(2);
            auto* famLabel = new QLabel(fam.first, famWidget);
            famLabel->setStyleSheet(QStringLiteral(
                "color:#60a5fa; font-size:11px; font-weight:800; background:transparent;"));
            famLayout->addWidget(famLabel);
            for (const QString& name : fam.second) {
                auto* tag = new QLabel(QStringLiteral("  ") + name, famWidget);
                tag->setStyleSheet(QStringLiteral(
                    "color:#94a3b8; font-size:10px; background:transparent;"));
                famLayout->addWidget(tag);
            }
            famGrid->addWidget(famWidget, 0, famCol++);
        }
        libLayout->addLayout(famGrid);
        layout->addWidget(libCard);

        QObject::connect(refreshButton, &QPushButton::clicked, this, [this]() {
            refreshHardwareDevices();
        });
        QObject::connect(applyButton, &QPushButton::clicked, this, [this]() {
            if (!bridge_) {
                hardwareSwitchStatusLabel_->setText(QStringLiteral("Engine bridge unavailable."));
                return;
            }
            const QString deviceName = hardwareDeviceCombo_->currentText().trimmed();
            if (deviceName.isEmpty()) {
                hardwareSwitchStatusLabel_->setText(QStringLiteral("Choose a device before switching."));
                return;
            }
            const bool ok = bridge_->switchAudioDevice(deviceName);
            hardwareSwitchStatusLabel_->setText(ok
                ? QStringLiteral("Switched active device to %1.").arg(deviceName)
                : QStringLiteral("Device switch failed for %1.").arg(deviceName));
            if (ok) { refreshHardwareDevices(); }
        });
        return page;
    }

    QWidget* buildHardwareDVSTab()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 8, 0, 0);
        layout->setSpacing(10);

        // ── Format selector ───────────────────────────────────────────────
        auto* fmtCard = makeSectionCard(
            QStringLiteral("Digital Vinyl System"),
            QStringLiteral("Select a timecode format and calibrate each deck for sub-1ms scratch detection."));
        auto* fmtLayout = qobject_cast<QVBoxLayout*>(fmtCard->layout());

        auto* fmtRow = new QHBoxLayout();
        auto* fmtLabel = new QLabel(QStringLiteral("Timecode Format:"), fmtCard);
        fmtLabel->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:12px; font-weight:700; background:transparent;"));
        dvsFormatCombo_ = new QComboBox(fmtCard);
        dvsFormatCombo_->addItems({
            QStringLiteral("Serato (1kHz / 1000 pos/rev)"),
            QStringLiteral("Traktor (2kHz / 1500 pos/rev)"),
            QStringLiteral("Virtual DJ (1.5kHz / 1200 pos/rev)"),
            QStringLiteral("NGKs Ultra (4kHz / 4000 pos/rev)")
        });
        dvsFormatCombo_->setMinimumWidth(240);
        fmtRow->addWidget(fmtLabel);
        fmtRow->addWidget(dvsFormatCombo_);
        fmtRow->addStretch(1);
        fmtLayout->addLayout(fmtRow);

        // Specs row
        auto* specsRow = new QHBoxLayout();
        specsRow->setSpacing(16);
        const QStringList specs = {
            QStringLiteral("Sub-1ms scratch latency"),
            QStringLiteral("96kHz interface support"),
            QStringLiteral("Auto-calibration workflow"),
            QStringLiteral("Signal-loss detection (100ms)")
        };
        for (const QString& s : specs) {
            auto* l = new QLabel(QStringLiteral("+ ") + s, fmtCard);
            l->setStyleSheet(QStringLiteral("color:#34d399; font-size:10px; font-weight:600; background:transparent;"));
            specsRow->addWidget(l);
        }
        specsRow->addStretch(1);
        fmtLayout->addLayout(specsRow);
        layout->addWidget(fmtCard);

        // ── Deck panels ───────────────────────────────────────────────────
        auto* deckRow = new QHBoxLayout();
        deckRow->setSpacing(10);

        auto makeDeckPanel = [&](const QString& deckName,
                                  QLabel*& outStatus,
                                  QLabel*& outPos,
                                  QLabel*& outSpeed,
                                  QLabel*& outDir,
                                  QProgressBar*& outSignal,
                                  bool isA) -> QFrame* {
            auto* panel = new QFrame(page);
            const QString borderCol = isA ? QStringLiteral("#2563eb") : QStringLiteral("#7c3aed");
            panel->setStyleSheet(QStringLiteral(
                "QFrame { background:#0a1628; border:2px solid %1; border-radius:12px; }").arg(borderCol));
            auto* pl = new QVBoxLayout(panel);
            pl->setContentsMargins(14, 12, 14, 12);
            pl->setSpacing(8);

            // Header
            auto* headRow = new QHBoxLayout();
            auto* deckLabel = new QLabel(deckName, panel);
            deckLabel->setStyleSheet(QStringLiteral(
                "color:#f1f5f9; font-size:15px; font-weight:900; background:transparent; border:none;"));
            outStatus = new QLabel(QStringLiteral("NO SIGNAL"), panel);
            outStatus->setStyleSheet(QStringLiteral(
                "color:#6b7280; font-size:10px; font-weight:700; background:#111827;"
                " border:1px solid #1e3348; border-radius:4px; padding:2px 7px;"));
            headRow->addWidget(deckLabel);
            headRow->addStretch(1);
            headRow->addWidget(outStatus);
            pl->addLayout(headRow);

            // Vinyl disc representation
            auto* disc = new QFrame(panel);
            disc->setFixedHeight(80);
            disc->setStyleSheet(QStringLiteral(
                "QFrame { background:qradialgradient(cx:0.5,cy:0.5,radius:0.5,"
                "stop:0 #1e293b, stop:0.35 #0f172a, stop:0.36 %1, stop:0.5 #0f172a,"
                "stop:0.9 #1e293b, stop:1 #0f172a);"
                " border:2px solid %1; border-radius:40px; }").arg(borderCol));
            auto* discInner = new QVBoxLayout(disc);
            discInner->setContentsMargins(0, 0, 0, 0);
            auto* discLabel = new QLabel(isA ? QStringLiteral("A") : QStringLiteral("B"), disc);
            discLabel->setAlignment(Qt::AlignCenter);
            discLabel->setStyleSheet(QStringLiteral(
                "color:%1; font-size:22px; font-weight:900; background:transparent; border:none;").arg(borderCol));
            discInner->addWidget(discLabel, 0, Qt::AlignCenter);
            pl->addWidget(disc);

            // Position + Speed + Direction
            auto* dataGrid = new QGridLayout();
            dataGrid->setHorizontalSpacing(8);
            dataGrid->setVerticalSpacing(3);

            auto addRow = [&](int row, const QString& lbl, QLabel*& outVal, const QString& initVal) {
                auto* key = new QLabel(lbl, panel);
                key->setStyleSheet(QStringLiteral("color:#6b7280; font-size:10px; background:transparent; border:none;"));
                outVal = new QLabel(initVal, panel);
                outVal->setStyleSheet(QStringLiteral("color:#e2e8f0; font-size:12px; font-weight:700; background:transparent; border:none;"));
                dataGrid->addWidget(key, row, 0);
                dataGrid->addWidget(outVal, row, 1);
            };
            addRow(0, QStringLiteral("Position"), outPos,   QStringLiteral("000.0 s"));
            addRow(1, QStringLiteral("Speed"),    outSpeed, QStringLiteral("+100.0%"));
            addRow(2, QStringLiteral("Direction"), outDir,  QStringLiteral("FWD"));
            pl->addLayout(dataGrid);

            // Signal quality
            auto* sigLabel = new QLabel(QStringLiteral("Signal Quality"), panel);
            sigLabel->setStyleSheet(QStringLiteral("color:#6b7280; font-size:10px; background:transparent; border:none;"));
            pl->addWidget(sigLabel);
            outSignal = new QProgressBar(panel);
            outSignal->setRange(0, 100);
            outSignal->setValue(0);
            outSignal->setTextVisible(true);
            outSignal->setFormat(QStringLiteral("%v%"));
            outSignal->setMinimumHeight(14);
            outSignal->setStyleSheet(QStringLiteral(
                "QProgressBar { background:#111827; border:1px solid #1e3348; border-radius:5px; text-align:center; font-size:10px; color:#94a3b8; }"
                "QProgressBar::chunk { background:%1; border-radius:4px; }").arg(borderCol));
            pl->addWidget(outSignal);

            // Calibrate button
            auto* calBtn = new QPushButton(QStringLiteral("Calibrate ") + deckName, panel);
            calBtn->setStyleSheet(QStringLiteral(
                "QPushButton { background:#0f1829; color:#94a3b8; border:1px solid #334155;"
                " border-radius:6px; padding:5px 10px; font-size:11px; font-weight:700; }"
                "QPushButton:hover { background:#1e293b; color:#f1f5f9; }"));
            calBtn->setCursor(Qt::PointingHandCursor);
            QObject::connect(calBtn, &QPushButton::clicked, this, [outStatus, outSignal]() {
                outStatus->setText(QStringLiteral("CALIBRATING"));
                outStatus->setStyleSheet(QStringLiteral(
                    "color:#f59e0b; font-size:10px; font-weight:700; background:#1c1608;"
                    " border:1px solid #92400e; border-radius:4px; padding:2px 7px;"));
                outSignal->setValue(0);
            });
            pl->addWidget(calBtn);
            return panel;
        };

        QLabel *dvsADir = nullptr, *dvsBDir = nullptr;
        deckRow->addWidget(makeDeckPanel(QStringLiteral("DECK A"),
            dvsDeckAStatus_, dvsDeckAPos_, dvsDeckASpeed_, dvsADir, dvsDeckASignal_, true));
        deckRow->addWidget(makeDeckPanel(QStringLiteral("DECK B"),
            dvsDeckBStatus_, dvsDeckBPos_, dvsDeckBSpeed_, dvsBDir, dvsDeckBSignal_, false));
        layout->addLayout(deckRow);

        // ── Performance stats ─────────────────────────────────────────────
        auto* perfCard = makeSectionCard(
            QStringLiteral("Processing Stats"),
            QStringLiteral("Real-time DSP performance metrics for the active timecode chain."));
        auto* perfLayout = qobject_cast<QVBoxLayout*>(perfCard->layout());
        auto* perfRow = new QHBoxLayout();
        perfRow->setSpacing(16);

        auto makePerfStat = [&](const QString& lbl, const QString& val, QLabel*& outLabel) {
            auto* col = new QWidget(perfCard);
            auto* cl = new QVBoxLayout(col);
            cl->setContentsMargins(0, 0, 0, 0);
            cl->setSpacing(1);
            outLabel = new QLabel(val, col);
            outLabel->setStyleSheet(QStringLiteral("color:#38bdf8; font-size:18px; font-weight:900; background:transparent;"));
            auto* ll = new QLabel(lbl, col);
            ll->setStyleSheet(QStringLiteral("color:#6b7280; font-size:10px; background:transparent;"));
            cl->addWidget(outLabel);
            cl->addWidget(ll);
            perfRow->addWidget(col);
        };
        makePerfStat(QStringLiteral("LATENCY (ms)"),  QStringLiteral("--"), dvsStatLatencyLabel_);
        makePerfStat(QStringLiteral("DROPOUTS"),       QStringLiteral("0"),  dvsStatDropoutsLabel_);
        makePerfStat(QStringLiteral("SAMPLE RATE"),    QStringLiteral("44100 Hz"), dvsStatSampleRateLabel_);
        makePerfStat(QStringLiteral("BUFFER SIZE"),    QStringLiteral("--"),  dvsStatBufferLabel_);
        perfRow->addStretch(1);
        perfLayout->addLayout(perfRow);
        layout->addWidget(perfCard);

        // ── Recommended hardware ──────────────────────────────────────────
        auto* hwCard = makeSectionCard(
            QStringLiteral("Recommended Hardware"),
            QStringLiteral("Tested turntables and interfaces for optimal DVS performance."));
        auto* hwLayout = qobject_cast<QVBoxLayout*>(hwCard->layout());
        auto* hwRow = new QHBoxLayout();
        hwRow->setSpacing(20);

        const QList<QPair<QString, QStringList>> hwGroups = {
            { QStringLiteral("Turntables"),
              { QStringLiteral("Technics SL-1200 MK7"),
                QStringLiteral("Pioneer PLX-1000"),
                QStringLiteral("Reloop RP-8000") } },
            { QStringLiteral("DVS Interfaces"),
              { QStringLiteral("Rane SL3 / SL4"),
                QStringLiteral("Traktor Audio 6/10"),
                QStringLiteral("Denon DS1") } }
        };
        for (const auto& grp : hwGroups) {
            auto* col = new QWidget(hwCard);
            auto* cl = new QVBoxLayout(col);
            cl->setContentsMargins(0, 0, 0, 0);
            cl->setSpacing(3);
            auto* hdr = new QLabel(grp.first, col);
            hdr->setStyleSheet(QStringLiteral("color:#60a5fa; font-size:11px; font-weight:800; background:transparent;"));
            cl->addWidget(hdr);
            for (const QString& item : grp.second) {
                auto* l = new QLabel(item, col);
                l->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:10px; background:transparent;"));
                cl->addWidget(l);
            }
            hwRow->addWidget(col);
        }
        hwRow->addStretch(1);
        hwLayout->addLayout(hwRow);
        layout->addWidget(hwCard);

        return page;
    }

    QWidget* buildHardwareMIDITab()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 8, 0, 0);
        layout->setSpacing(10);

        // ── MIDI device scan ──────────────────────────────────────────────
        auto* scanCard = makeSectionCard(
            QStringLiteral("MIDI Devices"),
            QStringLiteral("Scan connected hardware to check MIDI readiness and channel routing."));
        auto* scanLayout = qobject_cast<QVBoxLayout*>(scanCard->layout());

        auto* scanRow = new QHBoxLayout();
        auto* midiScanBtn = new QPushButton(QStringLiteral("Scan MIDI Devices"), scanCard);
        midiScanBtn->setCursor(Qt::PointingHandCursor);
        hardwareMidiStatusLabel_ = new QLabel(
            QStringLiteral("Run a device scan to refresh the current readiness summary."), scanCard);
        hardwareMidiStatusLabel_->setWordWrap(true);
        hardwareMidiStatusLabel_->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:11px; background:transparent;"));
        scanRow->addWidget(midiScanBtn);
        scanRow->addWidget(hardwareMidiStatusLabel_, 1);
        scanLayout->addLayout(scanRow);

        hardwareMidiDeviceList_ = new QListWidget(scanCard);
        hardwareMidiDeviceList_->setMinimumHeight(72);
        hardwareMidiDeviceList_->setMaximumHeight(110);
        hardwareMidiDeviceList_->setStyleSheet(QStringLiteral(
            "QListWidget { background:#0a1117; border:1px solid #1e3348;"
            " border-radius:7px; color:#e2e8f0; font-size:11px; }"
            "QListWidget::item { padding:5px 8px; }"
            "QListWidget::item:selected { background:#1e3a5f; }"));
        scanLayout->addWidget(hardwareMidiDeviceList_);
        layout->addWidget(scanCard);

        // ── Feature toggles ───────────────────────────────────────────────
        auto* featCard = makeSectionCard(
            QStringLiteral("MIDI Features"),
            QStringLiteral("Enable or disable individual MIDI capabilities for this session."));
        auto* featLayout = qobject_cast<QVBoxLayout*>(featCard->layout());

        midiLearnEnabled_       = new QCheckBox(QStringLiteral("MIDI Learn / custom mapping"), featCard);
        midiAutomationEnabled_  = new QCheckBox(QStringLiteral("Automation lanes (parameter recording)"), featCard);
        midiLedFeedbackEnabled_ = new QCheckBox(QStringLiteral("Controller LED feedback"), featCard);
        midiDualDeckEnabled_    = new QCheckBox(QStringLiteral("Dual-deck command parity"), featCard);
        midiLearnEnabled_->setChecked(true);
        midiAutomationEnabled_->setChecked(false);
        midiLedFeedbackEnabled_->setChecked(true);
        midiDualDeckEnabled_->setChecked(true);
        featLayout->addWidget(midiLearnEnabled_);
        featLayout->addWidget(midiAutomationEnabled_);
        featLayout->addWidget(midiLedFeedbackEnabled_);
        featLayout->addWidget(midiDualDeckEnabled_);
        QObject::connect(midiLearnEnabled_,       &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(midiAutomationEnabled_,  &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(midiLedFeedbackEnabled_, &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(midiDualDeckEnabled_,    &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        layout->addWidget(featCard);

        // ── Channel map info ──────────────────────────────────────────────
        auto* chanCard = makeSectionCard(
            QStringLiteral("Channel Routing"),
            QStringLiteral("Standard MIDI channel assignments for NGKs engine integration."));
        auto* chanLayout = qobject_cast<QVBoxLayout*>(chanCard->layout());
        auto* chanGrid = new QGridLayout();
        chanGrid->setHorizontalSpacing(20);
        chanGrid->setVerticalSpacing(4);
        const QList<QPair<QString, QString>> channels = {
            { QStringLiteral("Ch 1-2"),  QStringLiteral("Deck A -- transport, pitch, cue") },
            { QStringLiteral("Ch 3-4"),  QStringLiteral("Deck B -- transport, pitch, cue") },
            { QStringLiteral("Ch 5-6"),  QStringLiteral("Mixer -- EQ, filter, gains") },
            { QStringLiteral("Ch 7-8"),  QStringLiteral("FX units 1-2") },
            { QStringLiteral("Ch 9-10"), QStringLiteral("Loop / sampler pads") },
            { QStringLiteral("Ch 11"),   QStringLiteral("Automation / macro lanes") }
        };
        for (int i = 0; i < channels.size(); ++i) {
            auto* cLabel = new QLabel(channels[i].first, chanCard);
            cLabel->setStyleSheet(QStringLiteral("color:#60a5fa; font-size:11px; font-weight:700; background:transparent;"));
            auto* dLabel = new QLabel(channels[i].second, chanCard);
            dLabel->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:11px; background:transparent;"));
            chanGrid->addWidget(cLabel, i, 0);
            chanGrid->addWidget(dLabel, i, 1);
        }
        chanLayout->addLayout(chanGrid);
        layout->addWidget(chanCard);

        QObject::connect(midiScanBtn, &QPushButton::clicked, this, [this]() {
            refreshMidiDevices();
        });
        return page;
    }

    QWidget* buildHardwareSettingsTab()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 8, 0, 0);
        layout->setSpacing(10);

        // ── Behavior toggles ──────────────────────────────────────────────
        auto* behavCard = makeSectionCard(
            QStringLiteral("Hardware Behavior"),
            QStringLiteral("Device detection and feedback settings. Changes are saved automatically."));
        auto* behavLayout = qobject_cast<QVBoxLayout*>(behavCard->layout());

        hardwareAutoDetect_     = new QCheckBox(QStringLiteral("Auto-detect devices on connect"), behavCard);
        hardwareLedSync_        = new QCheckBox(QStringLiteral("Enable LED sync when supported"), behavCard);
        hardwareHaptics_        = new QCheckBox(QStringLiteral("Allow haptic feedback when available"), behavCard);
        hardwareMultiController_ = new QCheckBox(QStringLiteral("Multi-controller mode (up to 8 devices)"), behavCard);
        hardwareAutoDetect_->setChecked(true);
        hardwareLedSync_->setChecked(true);
        hardwareHaptics_->setChecked(true);
        hardwareMultiController_->setChecked(false);
        behavLayout->addWidget(hardwareAutoDetect_);
        behavLayout->addWidget(hardwareLedSync_);
        behavLayout->addWidget(hardwareHaptics_);
        behavLayout->addWidget(hardwareMultiController_);
        QObject::connect(hardwareAutoDetect_,     &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(hardwareLedSync_,        &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(hardwareHaptics_,        &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        QObject::connect(hardwareMultiController_, &QCheckBox::toggled, this, [this]() { saveHardwareSettings(); });
        layout->addWidget(behavCard);

        // ── Audio engine parameters ───────────────────────────────────────
        auto* audioCard = makeSectionCard(
            QStringLiteral("Audio Engine"),
            QStringLiteral("Low-level audio driver parameters. Requires engine restart to take effect."));
        auto* audioLayout = qobject_cast<QVBoxLayout*>(audioCard->layout());

        auto* audioGrid = new QGridLayout();
        audioGrid->setHorizontalSpacing(16);
        audioGrid->setVerticalSpacing(8);
        audioGrid->setColumnStretch(1, 1);

        auto addAudioRow = [&](int row, const QString& labelText, QComboBox*& outCombo,
                                const QStringList& items, int defaultIdx) {
            auto* lbl = new QLabel(labelText, audioCard);
            lbl->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:11px; font-weight:700; background:transparent;"));
            outCombo = new QComboBox(audioCard);
            outCombo->addItems(items);
            outCombo->setCurrentIndex(defaultIdx);
            audioGrid->addWidget(lbl, row, 0);
            audioGrid->addWidget(outCombo, row, 1);
        };

        addAudioRow(0, QStringLiteral("Latency Mode:"),
            hardwareLatencyCombo_,
            { QStringLiteral("Low Latency (< 10ms)"),
              QStringLiteral("Balanced (10-20ms)"),
              QStringLiteral("Studio Mode (< 3ms)") }, 1);

        addAudioRow(1, QStringLiteral("Sample Rate:"),
            hardwareSampleRateCombo_,
            { QStringLiteral("44100 Hz"),
              QStringLiteral("48000 Hz"),
              QStringLiteral("88200 Hz"),
              QStringLiteral("96000 Hz") }, 1);

        addAudioRow(2, QStringLiteral("Buffer Size:"),
            hardwareBufferSizeCombo_,
            { QStringLiteral("64 samples"),
              QStringLiteral("128 samples"),
              QStringLiteral("256 samples"),
              QStringLiteral("512 samples"),
              QStringLiteral("1024 samples") }, 2);

        audioLayout->addLayout(audioGrid);

        auto* restartNote = new QLabel(
            QStringLiteral("Note: audio engine parameter changes apply on next engine start."), audioCard);
        restartNote->setStyleSheet(QStringLiteral("color:#6b7280; font-size:10px; font-style:italic; background:transparent;"));
        audioLayout->addWidget(restartNote);
        layout->addWidget(audioCard);

        QObject::connect(hardwareLatencyCombo_,    QOverload<int>::of(&QComboBox::currentIndexChanged),
                         this, [this]() { saveHardwareSettings(); });
        QObject::connect(hardwareSampleRateCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
                         this, [this]() { saveHardwareSettings(); });
        QObject::connect(hardwareBufferSizeCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
                         this, [this]() { saveHardwareSettings(); });

        layout->addStretch(1);
        return page;
    }

    QWidget* buildCrowdRequestsPage()
    {
        auto* page = new QWidget(this);
        page->setObjectName(QStringLiteral("crowdRequestsPage"));
        page->setStyleSheet(QStringLiteral(
            "QWidget#crowdRequestsPage { background:transparent; }"
            "QFrame#jukeboxSelectionWindow, QFrame#jukeboxControlConsole, QFrame#jukeboxQueuePanel, QFrame#jukeboxCrowdMarquee {"
            " background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 rgba(67,24,14,245), stop:0.48 rgba(34,10,7,240), stop:1 rgba(20,7,6,244));"
            " border:2px solid rgba(220,176,97,170); border-radius:20px; }"
            "QFrame#jukeboxSelectionWindow { border-color:rgba(116,225,255,190); }"
            "QLabel[jukebox='display'] { color:#fff2cc; font-size:13px; font-weight:800; letter-spacing:0.6px; }"
            "QLabel[jukebox='small'] { color:#f7ca8f; font-size:11px; }"
            "QLabel[jukebox='counter'] { color:#ffe8b3; font-size:12px; font-weight:900; padding:4px 10px;"
            " background:rgba(104,39,20,160); border:1px solid rgba(244,191,117,130); border-radius:11px; }"
            "QPushButton[jukebox='control'] { min-height:34px; }"
            "QPushButton[jukebox='round'] { border-radius:17px; min-width:34px; min-height:34px; padding:0 10px; }"
            "QComboBox[jukebox='selector'] { min-width:130px; font-weight:800; }"
            "QLineEdit[jukebox='backlit'] { min-height:34px; }"
            "QTreeWidget#jukeboxQueueTree { background:rgba(13,6,5,215); alternate-background-color:rgba(78,28,16,120);"
            " border:2px solid rgba(218,170,92,170); border-radius:18px; }"
            "QTreeWidget#jukeboxQueueTree::item { padding:8px 10px; margin:3px 6px; border-radius:10px; }"
            "QTreeWidget#jukeboxQueueTree::item:hover { background:rgba(255,196,91,50); }"));
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(12);

        auto* marquee = new QFrame(page);
        marquee->setObjectName(QStringLiteral("jukeboxCrowdMarquee"));
        auto* marqueeLayout = new QVBoxLayout(marquee);
        marqueeLayout->setContentsMargins(18, 16, 18, 14);
        marqueeLayout->setSpacing(10);
        auto* marqueeTitle = new QLabel(QStringLiteral("CROWD REQUESTS JUKEBOX"), marquee);
        marqueeTitle->setStyleSheet(QStringLiteral("color:#fff1bd; font-size:24px; font-weight:900; letter-spacing:1px;"));
        marqueeLayout->addWidget(marqueeTitle);

        auto* statusRow = new QHBoxLayout();
        requestServerStateLabel_ = new QLabel(QStringLiteral("Queue offline"), marquee);
        requestServerStateLabel_->setProperty("jukebox", QStringLiteral("display"));
        requestUrlLabel_ = new QLabel(QStringLiteral("Click Start Request Server to expose the local join URL."), marquee);
        requestUrlLabel_->setProperty("jukebox", QStringLiteral("small"));
        requestPendingLabel_ = new QLabel(QStringLiteral("0 pending"), marquee);
        requestPendingLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestCountLabel_ = new QLabel(QStringLiteral("0 total"), marquee);
        requestCountLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestAcceptedLabel_ = new QLabel(QStringLiteral("0 accepted"), marquee);
        requestAcceptedLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestHandedOffLabel_ = new QLabel(QStringLiteral("0 handed off"), marquee);
        requestHandedOffLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestNowPlayingLabel_ = new QLabel(QStringLiteral("0 now playing"), marquee);
        requestNowPlayingLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestPlayedLabel_ = new QLabel(QStringLiteral("0 played"), marquee);
        requestPlayedLabel_->setProperty("jukebox", QStringLiteral("counter"));
        requestFailedLabel_ = new QLabel(QStringLiteral("0 failed"), marquee);
        requestFailedLabel_->setProperty("jukebox", QStringLiteral("counter"));
        statusRow->addWidget(requestServerStateLabel_);
        statusRow->addSpacing(18);
        statusRow->addWidget(requestPendingLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestCountLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestAcceptedLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestHandedOffLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestNowPlayingLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestPlayedLabel_);
        statusRow->addSpacing(10);
        statusRow->addWidget(requestFailedLabel_);
        auto* marqueeHint = new QLabel(QStringLiteral("Mechanical selector counters stay live while the queue is running."), marquee);
        marqueeHint->setProperty("jukebox", QStringLiteral("small"));
        statusRow->addStretch(1);
        statusRow->addWidget(marqueeHint);
        marqueeLayout->addLayout(statusRow);
        layout->addWidget(marquee);

        auto* topPanels = new QHBoxLayout();
        topPanels->setSpacing(12);

        auto* selectionCard = new QFrame(page);
        selectionCard->setObjectName(QStringLiteral("jukeboxSelectionWindow"));
        auto* selectionLayout = new QVBoxLayout(selectionCard);
        selectionLayout->setContentsMargins(18, 16, 18, 16);
        selectionLayout->setSpacing(10);
        auto* selectionTitle = new QLabel(QStringLiteral("Selection Window"), selectionCard);
        selectionTitle->setStyleSheet(QStringLiteral("color:#d7fbff; font-size:18px; font-weight:900; letter-spacing:0.8px;"));
        auto* selectionSubtitle = new QLabel(QStringLiteral("Guest-facing QR, join address, and server power controls."), selectionCard);
        selectionSubtitle->setProperty("jukebox", QStringLiteral("small"));
        selectionSubtitle->setWordWrap(true);
        selectionLayout->addWidget(selectionTitle);
        selectionLayout->addWidget(selectionSubtitle);

        auto* selectionGlass = new QFrame(selectionCard);
        selectionGlass->setStyleSheet(QStringLiteral(
            "QFrame { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 rgba(92,210,255,70), stop:0.4 rgba(255,255,255,18), stop:1 rgba(17,40,52,130));"
            " border:2px solid rgba(145,232,255,170); border-radius:18px; }"));
        auto* selectionGlassLayout = new QVBoxLayout(selectionGlass);
        selectionGlassLayout->setContentsMargins(16, 14, 16, 14);
        selectionGlassLayout->setSpacing(10);
        auto* joinLabel = new QLabel(QStringLiteral("JOIN ADDRESS"), selectionGlass);
        joinLabel->setStyleSheet(QStringLiteral("color:#d6fbff; font-size:12px; font-weight:900; letter-spacing:0.8px;"));
        selectionGlassLayout->addWidget(joinLabel);
        requestUrlDisplayLabel_ = new QLabel(QStringLiteral("Click Start Request Server to expose the local join URL."), selectionGlass);
        requestUrlDisplayLabel_->setProperty("jukebox", QStringLiteral("small"));
        requestUrlDisplayLabel_->setWordWrap(true);
        selectionGlassLayout->addWidget(requestUrlDisplayLabel_);
        auto* qrHint = new QLabel(QStringLiteral("Scan, open, or copy the guest page without leaving the operator view."), selectionGlass);
        qrHint->setProperty("jukebox", QStringLiteral("small"));
        qrHint->setWordWrap(true);
        selectionGlassLayout->addWidget(qrHint);
        selectionLayout->addWidget(selectionGlass, 1);

        auto* selectionButtons = new QGridLayout();
        requestStartButton_ = new QPushButton(QStringLiteral("Start Request Server"), selectionCard);
        requestStopButton_ = new QPushButton(QStringLiteral("Stop Request Server"), selectionCard);
        requestCopyButton_ = new QPushButton(QStringLiteral("Copy Join URL"), selectionCard);
        requestOpenGuestButton_ = new QPushButton(QStringLiteral("Open Guest Page"), selectionCard);
        requestQrButton_ = new QPushButton(QStringLiteral("Show QR"), selectionCard);
        auto* addManualButton = new QPushButton(QStringLiteral("Add Manual Request"), selectionCard);
        requestClearButton_ = new QPushButton(QStringLiteral("Clear Queue"), selectionCard);
        requestPolicyCombo_ = new QComboBox(selectionCard);
        requestPolicyCombo_->addItems({QStringLiteral("free"), QStringLiteral("paid"), QStringLiteral("either")});
        requestStartButton_->setProperty("jukebox", QStringLiteral("control"));
        requestStopButton_->setProperty("jukebox", QStringLiteral("control"));
        requestCopyButton_->setProperty("jukebox", QStringLiteral("control"));
        requestOpenGuestButton_->setProperty("jukebox", QStringLiteral("control"));
        requestQrButton_->setProperty("jukebox", QStringLiteral("control"));
        addManualButton->setProperty("jukebox", QStringLiteral("control"));
        requestClearButton_->setProperty("jukebox", QStringLiteral("control"));
        requestQrButton_->setProperty("jukebox", QStringLiteral("round"));
        requestPolicyCombo_->setProperty("jukebox", QStringLiteral("selector"));
        requestStartButton_->setStyleSheet(QStringLiteral("QPushButton { background:qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #b9ff6b, stop:1 #4d9e19); color:#173300; border:2px solid #ebffbd; font-weight:900; }"));
        selectionButtons->addWidget(requestStartButton_, 0, 0);
        selectionButtons->addWidget(requestStopButton_, 0, 1);
        selectionButtons->addWidget(requestOpenGuestButton_, 1, 0);
        selectionButtons->addWidget(requestCopyButton_, 1, 1);
        selectionButtons->addWidget(requestQrButton_, 2, 0);
        selectionButtons->addWidget(addManualButton, 2, 1);
        selectionButtons->addWidget(requestClearButton_, 3, 0);
        auto* policyWrap = new QHBoxLayout();
        auto* policyLabel = new QLabel(QStringLiteral("Selector Mode"), selectionCard);
        policyLabel->setProperty("jukebox", QStringLiteral("display"));
        policyWrap->addWidget(policyLabel);
        policyWrap->addWidget(requestPolicyCombo_, 1);
        selectionButtons->addLayout(policyWrap, 3, 1);
        selectionLayout->addLayout(selectionButtons);

        auto* controlCard = new QFrame(page);
        controlCard->setObjectName(QStringLiteral("jukeboxControlConsole"));
        auto* controlLayout = new QVBoxLayout(controlCard);
        controlLayout->setContentsMargins(18, 16, 18, 16);
        controlLayout->setSpacing(12);
        auto* controlTitle = new QLabel(QStringLiteral("Control Console"), controlCard);
        controlTitle->setStyleSheet(QStringLiteral("color:#ffe6b0; font-size:18px; font-weight:900; letter-spacing:0.8px;"));
        auto* controlSubtitle = new QLabel(QStringLiteral("Backlit payment selectors and operator settings for paid requests."), controlCard);
        controlSubtitle->setProperty("jukebox", QStringLiteral("small"));
        controlSubtitle->setWordWrap(true);
        controlLayout->addWidget(controlTitle);
        controlLayout->addWidget(controlSubtitle);

        auto* paymentCard = new QFrame(controlCard);
        paymentCard->setStyleSheet(QStringLiteral(
            "QFrame { background:rgba(20,8,7,165); border:1px solid rgba(219,166,86,120); border-radius:16px; }"));
        auto* paymentLayout = new QVBoxLayout(paymentCard);
        paymentLayout->setContentsMargins(14, 12, 14, 12);
        paymentLayout->setSpacing(10);
        auto* form = new QFormLayout();
        form->setLabelAlignment(Qt::AlignLeft | Qt::AlignVCenter);
        form->setFormAlignment(Qt::AlignTop);
        form->setHorizontalSpacing(12);
        form->setVerticalSpacing(10);
        requestVenmoEdit_ = new QLineEdit(paymentCard);
        requestCashAppEdit_ = new QLineEdit(paymentCard);
        requestPaypalEdit_ = new QLineEdit(paymentCard);
        requestZelleEdit_ = new QLineEdit(paymentCard);
        requestBuyMeACoffeeEdit_ = new QLineEdit(paymentCard);
        requestChimeEdit_ = new QLineEdit(paymentCard);
        requestCardUrlEdit_ = new QLineEdit(paymentCard);
        requestVenmoEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestCashAppEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestPaypalEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestZelleEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestBuyMeACoffeeEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestChimeEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestCardUrlEdit_->setProperty("jukebox", QStringLiteral("backlit"));
        requestVenmoEdit_->setPlaceholderText(QStringLiteral("@YourVenmo"));
        requestCashAppEdit_->setPlaceholderText(QStringLiteral("$YourCashApp"));
        requestPaypalEdit_->setPlaceholderText(QStringLiteral("paypal.me/you or your@email.com"));
        requestZelleEdit_->setPlaceholderText(QStringLiteral("your-zelle@example.com or 555-1234"));
        requestBuyMeACoffeeEdit_->setPlaceholderText(QStringLiteral("buymeacoffee.com/yourname"));
        requestChimeEdit_->setPlaceholderText(QStringLiteral("$YourChimeSign"));
        requestCardUrlEdit_->setPlaceholderText(QStringLiteral("https://checkout.stripe.com/... or your Square/PayPal card link"));
        form->addRow(QStringLiteral("Venmo"), requestVenmoEdit_);
        form->addRow(QStringLiteral("Cash App"), requestCashAppEdit_);
        form->addRow(QStringLiteral("PayPal"), requestPaypalEdit_);
        form->addRow(QStringLiteral("Zelle"), requestZelleEdit_);
        form->addRow(QStringLiteral("Buy Me a Coffee"), requestBuyMeACoffeeEdit_);
        form->addRow(QStringLiteral("Chime"), requestChimeEdit_);
        form->addRow(QStringLiteral("Debit / Credit Card Link"), requestCardUrlEdit_);
        paymentLayout->addLayout(form);
        auto* paymentActions = new QHBoxLayout();
        requestSaveSettingsButton_ = new QPushButton(QStringLiteral("Save Payment Settings"), paymentCard);
        requestSaveSettingsButton_->setProperty("jukebox", QStringLiteral("control"));
        requestSaveSettingsStatusLabel_ = new QLabel(QStringLiteral("Not saved in this session."), paymentCard);
        requestSaveSettingsStatusLabel_->setProperty("jukebox", QStringLiteral("small"));
        paymentActions->addWidget(requestSaveSettingsButton_);
        paymentActions->addWidget(requestSaveSettingsStatusLabel_, 1);
        paymentLayout->addLayout(paymentActions);
        auto* queueHint = new QLabel(QStringLiteral("Guests will see configured payment methods with QR codes on the request page when policy is paid or either. Leave a field blank to hide that option."), paymentCard);
        queueHint->setWordWrap(true);
        queueHint->setProperty("jukebox", QStringLiteral("small"));
        paymentLayout->addWidget(queueHint);
        controlLayout->addWidget(paymentCard, 1);

        topPanels->addWidget(selectionCard, 1);
        topPanels->addWidget(controlCard, 1);
        layout->addLayout(topPanels);

        auto* queueCard = new QFrame(page);
        queueCard->setObjectName(QStringLiteral("jukeboxQueuePanel"));
        auto* queueLayout = new QVBoxLayout(queueCard);
        queueLayout->setContentsMargins(18, 16, 18, 16);
        queueLayout->setSpacing(12);
        auto* queueTitle = new QLabel(QStringLiteral("Track List"), queueCard);
        queueTitle->setStyleSheet(QStringLiteral("color:#ffe7b0; font-size:20px; font-weight:900; letter-spacing:0.8px;"));
        auto* queueSubtitle = new QLabel(QStringLiteral("Scrolling jukebox strips for incoming, accepted, and live requests."), queueCard);
        queueSubtitle->setProperty("jukebox", QStringLiteral("small"));
        queueLayout->addWidget(queueTitle);
        queueLayout->addWidget(queueSubtitle);

        requestQueueTree_ = new QTreeWidget(queueCard);
        requestQueueTree_->setObjectName(QStringLiteral("jukeboxQueueTree"));
        requestQueueTree_->setHeaderLabels({QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Requester"), QStringLiteral("Votes"), QStringLiteral("Status")});
        requestQueueTree_->setRootIsDecorated(false);
        requestQueueTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        requestQueueTree_->setMinimumHeight(320);
        requestQueueTree_->setAlternatingRowColors(true);
        requestQueueTree_->setUniformRowHeights(false);
        requestQueueTree_->setIndentation(0);
        queueLayout->addWidget(requestQueueTree_);

        auto* queueButtons = new QHBoxLayout();
        auto* acceptButton = new QPushButton(QStringLiteral("Accept to Deck A"), queueCard);
        auto* acceptDeckBButton = new QPushButton(QStringLiteral("Accept to Deck B"), queueCard);
        auto* rejectButton = new QPushButton(QStringLiteral("Reject"), queueCard);
        auto* removeButton = new QPushButton(QStringLiteral("Remove"), queueCard);
        acceptButton->setProperty("jukebox", QStringLiteral("control"));
        acceptDeckBButton->setProperty("jukebox", QStringLiteral("control"));
        rejectButton->setProperty("jukebox", QStringLiteral("control"));
        removeButton->setProperty("jukebox", QStringLiteral("control"));
        queueButtons->addWidget(acceptButton);
        queueButtons->addWidget(acceptDeckBButton);
        queueButtons->addWidget(rejectButton);
        queueButtons->addWidget(removeButton);
        queueButtons->addStretch(1);
        queueLayout->addLayout(queueButtons);

        requestHandoffLabel_ = new QLabel(QStringLiteral("Accepted requests stay ACCEPTED until the target deck confirms the requested file path and decode readiness. Real playback promotes them to NOW_PLAYING, and moving away resolves them to PLAYED."), queueCard);
        requestHandoffLabel_->setWordWrap(true);
        requestHandoffLabel_->setProperty("jukebox", QStringLiteral("small"));
        queueLayout->addWidget(requestHandoffLabel_);
        layout->addWidget(queueCard, 1);

        QObject::connect(requestStartButton_, &QPushButton::clicked, this, [this]() {
            startRequestServer();
        });
        QObject::connect(requestStopButton_, &QPushButton::clicked, this, [this]() {
            shutdownRequestServer(false);
        });
        QObject::connect(requestCopyButton_, &QPushButton::clicked, this, [this]() {
            if (joinUrl_.trimmed().isEmpty()) {
                QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Start the request server before copying the join URL."));
                return;
            }
            QApplication::clipboard()->setText(joinUrl_);
            QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Join URL copied to clipboard."));
        });
        QObject::connect(requestOpenGuestButton_, &QPushButton::clicked, this, [this]() {
            if (joinUrl_.trimmed().isEmpty()) {
                QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Start the request server before opening the guest page."));
                return;
            }
            QDesktopServices::openUrl(QUrl(joinUrl_));
        });
        QObject::connect(requestQrButton_, &QPushButton::clicked, this, [this]() { showRequestQrDialog(); });
        QObject::connect(addManualButton, &QPushButton::clicked, this, [this]() { addManualRequest(); });
        QObject::connect(requestClearButton_, &QPushButton::clicked, this, [this]() {
            if (!crowdBackend_ || !crowdBackend_->isRunning()) {
                QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Start the request server before clearing the queue."));
                return;
            }
            if (QMessageBox::question(this, QStringLiteral("Crowd Requests"), QStringLiteral("Remove all queue items from the local request backend?")) != QMessageBox::Yes) {
                return;
            }
            QString error;
            if (!crowdBackend_->clearQueue(&error)) {
                QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Queue clear failed.") : error);
                return;
            }
            refreshRequestQueue();
        });
        QObject::connect(acceptButton, &QPushButton::clicked, this, [this]() { acceptSelectedRequest(0); });
        QObject::connect(acceptDeckBButton, &QPushButton::clicked, this, [this]() { acceptSelectedRequest(1); });
        QObject::connect(rejectButton, &QPushButton::clicked, this, [this]() { rejectSelectedRequest(); });
        QObject::connect(removeButton, &QPushButton::clicked, this, [this]() { removeSelectedRequest(); });
        QObject::connect(requestSaveSettingsButton_, &QPushButton::clicked, this, [this]() { saveRequestSettings(); });
        QObject::connect(requestPolicyCombo_, &QComboBox::currentTextChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestVenmoEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestCashAppEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestPaypalEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestZelleEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestBuyMeACoffeeEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestChimeEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        QObject::connect(requestCardUrlEdit_, &QLineEdit::textChanged, this, [this](const QString&) { scheduleRequestSettingsAutoSave(); });
        return page;
    }

    QWidget* buildStreamingPage()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(14);

        auto* hero = makeSectionCard(
            QStringLiteral("Streaming Music Services"),
            QStringLiteral("Connect streaming services and search your library to load tracks directly to a deck."));
        auto* heroLayout = qobject_cast<QVBoxLayout*>(hero->layout());
        auto* grid = new QGridLayout();
        grid->setHorizontalSpacing(10);
        grid->setVerticalSpacing(10);

        const QList<std::tuple<QString,QString,QString,QString,QString,QStringList>> services = {
            // key, display name, accent colour, signup URL, api-key URL, features
            {QStringLiteral("spotify"),
             QStringLiteral("Spotify"),
             QStringLiteral("#1DB954"),
             QStringLiteral("https://www.spotify.com/signup"),
             QStringLiteral("https://developer.spotify.com/dashboard"),
             {QStringLiteral("Browse tracks"), QStringLiteral("Create playlists"), QStringLiteral("Stream to decks")}},
            {QStringLiteral("soundcloud"),
             QStringLiteral("SoundCloud"),
             QStringLiteral("#FF5500"),
             QStringLiteral("https://soundcloud.com/register"),
             QStringLiteral("https://developers.soundcloud.com"),
             {QStringLiteral("Browse tracks"), QStringLiteral("Stream remixes"), QStringLiteral("DJ-friendly content")}},
            {QStringLiteral("tidal"),
             QStringLiteral("TIDAL"),
             QStringLiteral("#00FFFF"),
             QStringLiteral("https://tidal.com/register"),
             QStringLiteral("https://developer.tidal.com"),
             {QStringLiteral("HiFi audio"), QStringLiteral("Master quality"), QStringLiteral("Exclusive content")}},
            {QStringLiteral("beatport"),
             QStringLiteral("Beatport"),
             QStringLiteral("#92EF27"),
             QStringLiteral("https://www.beatport.com/account/register"),
             QStringLiteral("https://api.beatport.com"),
             {QStringLiteral("DJ charts"), QStringLiteral("BPM/key info"), QStringLiteral("Genre filters")}},
            {QStringLiteral("applemusic"),
             QStringLiteral("Apple Music"),
             QStringLiteral("#FC3C44"),
             QStringLiteral("https://music.apple.com"),
             QStringLiteral("https://developer.apple.com/musickit"),
             {QStringLiteral("iTunes sync"), QStringLiteral("Apple Music catalog"), QStringLiteral("Lossless audio")}},
            {QStringLiteral("youtubemusic"),
             QStringLiteral("YouTube Music"),
             QStringLiteral("#FF0000"),
             QStringLiteral("https://music.youtube.com"),
             QStringLiteral("https://developers.google.com/youtube/v3"),
             {QStringLiteral("Music videos"), QStringLiteral("Remixes"), QStringLiteral("Live performances")}},
            {QStringLiteral("deezer"),
             QStringLiteral("Deezer"),
             QStringLiteral("#EF5D17"),
             QStringLiteral("https://www.deezer.com/register"),
             QStringLiteral("https://developers.deezer.com/api"),
             {QStringLiteral("Flow recommendations"), QStringLiteral("HiFi audio"), QStringLiteral("Global catalog")}},
            {QStringLiteral("bandcamp"),
             QStringLiteral("Bandcamp"),
             QStringLiteral("#1DA0C3"),
             QStringLiteral("https://bandcamp.com/signup"),
             QStringLiteral("https://bandcamp.com/developer"),
             {QStringLiteral("Buy tracks"), QStringLiteral("Support artists"), QStringLiteral("Download files")}}
        };
        int serviceRow = 0, serviceCol = 0;
        for (const auto& entry : services) {
            const QString& key = std::get<0>(entry);
            const QString& name = std::get<1>(entry);
            const QString& accent = std::get<2>(entry);
            const QString& signupUrl = std::get<3>(entry);
            const QString& apiKeyUrl = std::get<4>(entry);
            const QStringList& features = std::get<5>(entry);
            streamingApiKeyUrls_[key] = {signupUrl, apiKeyUrl};
            streamingDisplayNames_[key] = name;
            auto* card = new QFrame(hero);
            card->setFixedHeight(170);
            card->setStyleSheet(QStringLiteral(
                "QFrame { background:#0f1829; border:2px solid %1; border-radius:12px; }"
            ).arg(accent));
            auto* cardLayout = new QVBoxLayout(card);
            cardLayout->setContentsMargins(12, 10, 12, 10);
            cardLayout->setSpacing(3);
            auto* nameLabel = new QLabel(name, card);
            nameLabel->setStyleSheet(QStringLiteral(
                "background:transparent; color:#f8fafc; font-size:14px; font-weight:800; border:none;"));
            cardLayout->addWidget(nameLabel);
            auto* featureLabel = new QLabel(
                QStringLiteral("\u2713 ") + features.join(QStringLiteral("  \u2713 ")), card);
            featureLabel->setStyleSheet(QStringLiteral(
                "background:transparent; color:#6b7280; font-size:10px; border:none;"));
            featureLabel->setWordWrap(false);
            cardLayout->addWidget(featureLabel);
            cardLayout->addStretch(1);
            auto* stateLabel = new QLabel(QStringLiteral("Disconnected"), card);
            stateLabel->setStyleSheet(QStringLiteral(
                "background:transparent; color:#6b7280; font-size:11px; border:none;"));
            cardLayout->addWidget(stateLabel);
            auto* button = new QPushButton(QStringLiteral("Connect %1").arg(name), card);
            button->setCursor(Qt::PointingHandCursor);
            button->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);
            button->setStyleSheet(QStringLiteral(
                "QPushButton { background:%1; color:#000; border:none; border-radius:7px;"
                " padding:6px 10px; font-size:12px; font-weight:700; }"
                "QPushButton:hover { background:%1; opacity:0.85; }"
            ).arg(accent));
            cardLayout->addWidget(button);
            auto* signupLink = new QPushButton(
                QStringLiteral("Don't have an account? Sign up \u2192"), card);
            signupLink->setCursor(Qt::PointingHandCursor);
            signupLink->setFlat(true);
            signupLink->setStyleSheet(QStringLiteral(
                "QPushButton { background:transparent; color:#64748b; border:none;"
                " font-size:10px; text-align:left; padding:0; }"
                "QPushButton:hover { color:#94a3b8; }"));
            cardLayout->addWidget(signupLink);
            grid->addWidget(card, serviceRow, serviceCol);
            if (++serviceCol == 4) { serviceCol = 0; ++serviceRow; }
            streamingStatusLabels_[key] = stateLabel;
            streamingToggleButtons_[key] = button;
            streamingConnected_[key] = !streamingApiKeys_[key].isEmpty();
            QObject::connect(signupLink, &QPushButton::clicked, this, [signupUrl]() {
                QDesktopServices::openUrl(QUrl(signupUrl));
            });
            QObject::connect(button, &QPushButton::clicked, this, [this, key, name, apiKeyUrl]() {
                if (streamingConnected_[key]) {
                    // Disconnect: clear stored key
                    streamingApiKeys_[key].clear();
                    streamingConnected_[key] = false;
                    saveStreamingKeys();
                    refreshStreamingServices();
                    return;
                }
                // Connect: need an API key
                auto* dlg = new QDialog(this);
                dlg->setWindowTitle(QStringLiteral("Connect %1").arg(name));
                dlg->setModal(true);
                auto* dlgLayout = new QVBoxLayout(dlg);
                dlgLayout->setSpacing(12);
                dlgLayout->setContentsMargins(20, 18, 20, 18);
                auto* prompt = new QLabel(
                    QStringLiteral("Enter your %1 API key to connect.").arg(name), dlg);
                prompt->setWordWrap(true);
                dlgLayout->addWidget(prompt);
                auto* keyEdit = new QLineEdit(dlg);
                keyEdit->setPlaceholderText(QStringLiteral("Paste API key here"));
                keyEdit->setEchoMode(QLineEdit::Password);
                dlgLayout->addWidget(keyEdit);
                auto* linkBtn = new QPushButton(
                    QStringLiteral("Don't have a key? Get one at %1 \u2192").arg(apiKeyUrl), dlg);
                linkBtn->setFlat(true);
                linkBtn->setCursor(Qt::PointingHandCursor);
                linkBtn->setStyleSheet(QStringLiteral("color:#38bdf8; text-align:left; font-size:11px;"));
                QObject::connect(linkBtn, &QPushButton::clicked, dlg, [apiKeyUrl]() {
                    QDesktopServices::openUrl(QUrl(apiKeyUrl));
                });
                dlgLayout->addWidget(linkBtn);
                auto* btnRow = new QHBoxLayout();
                auto* okBtn = new QPushButton(QStringLiteral("Connect"), dlg);
                auto* cancelBtn = new QPushButton(QStringLiteral("Cancel"), dlg);
                btnRow->addStretch(1);
                btnRow->addWidget(cancelBtn);
                btnRow->addWidget(okBtn);
                dlgLayout->addLayout(btnRow);
                QObject::connect(cancelBtn, &QPushButton::clicked, dlg, &QDialog::reject);
                QObject::connect(okBtn, &QPushButton::clicked, dlg, [this, dlg, keyEdit, key]() {
                    const QString k = keyEdit->text().trimmed();
                    if (k.isEmpty()) {
                        QMessageBox::warning(dlg, QStringLiteral("API Key Required"),
                            QStringLiteral("Please paste your API key before connecting."));
                        return;
                    }
                    streamingApiKeys_[key] = k;
                    streamingConnected_[key] = true;
                    saveStreamingKeys();
                    refreshStreamingServices();
                    dlg->accept();
                });
                dlg->exec();
            });
        }
        for (int c = 0; c < 4; ++c) { grid->setColumnStretch(c, 1); }
        auto* gridContainer = new QWidget(hero);
        gridContainer->setLayout(grid);
        heroLayout->addWidget(gridContainer);
        layout->addWidget(hero);

        auto* searchCard = makeSectionCard(
            QStringLiteral("Search and Load"),
            QStringLiteral("Search your imported library by title, artist, album, BPM, or key. Select a result to load it to a deck or add it to the crowd request queue."));
        auto* searchLayout = qobject_cast<QVBoxLayout*>(searchCard->layout());
        auto* searchRow = new QHBoxLayout();
        streamingSearchEdit_ = new QLineEdit(searchCard);
        streamingSearchEdit_->setPlaceholderText(QStringLiteral("Search titles, artists, albums, BPM, or keywords"));
        streamingServiceFilter_ = new QComboBox(searchCard);
        streamingServiceFilter_->addItem(QStringLiteral("all"));
        streamingServiceFilter_->addItem(QStringLiteral("spotify"));
        streamingServiceFilter_->addItem(QStringLiteral("soundcloud"));
        streamingServiceFilter_->addItem(QStringLiteral("tidal"));
        streamingServiceFilter_->addItem(QStringLiteral("beatport"));
        streamingServiceFilter_->addItem(QStringLiteral("applemusic"));
        streamingServiceFilter_->addItem(QStringLiteral("youtubemusic"));
        streamingServiceFilter_->addItem(QStringLiteral("deezer"));
        streamingServiceFilter_->addItem(QStringLiteral("bandcamp"));
        auto* searchButton = new QPushButton(QStringLiteral("Search"), searchCard);
        searchRow->addWidget(streamingSearchEdit_, 1);
        searchRow->addWidget(streamingServiceFilter_);
        searchRow->addWidget(searchButton);
        searchLayout->addLayout(searchRow);

        streamingResultsTree_ = new QTreeWidget(searchCard);
        streamingResultsTree_->setHeaderLabels({QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Source"), QStringLiteral("BPM"), QStringLiteral("Key")});
        streamingResultsTree_->setRootIsDecorated(false);
        streamingResultsTree_->setSelectionMode(QAbstractItemView::SingleSelection);
        searchLayout->addWidget(streamingResultsTree_, 1);

        auto* resultActions = new QHBoxLayout();
        auto* loadDeckAButton = new QPushButton(QStringLiteral("Load Deck A"), searchCard);
        auto* loadDeckBButton = new QPushButton(QStringLiteral("Load Deck B"), searchCard);
        auto* promoteButton = new QPushButton(QStringLiteral("Promote to Crowd Queue"), searchCard);
        resultActions->addWidget(loadDeckAButton);
        resultActions->addWidget(loadDeckBButton);
        resultActions->addWidget(promoteButton);
        resultActions->addStretch(1);
        searchLayout->addLayout(resultActions);
        layout->addWidget(searchCard, 1);

        QObject::connect(searchButton, &QPushButton::clicked, this, [this]() { runStreamingSearch(); });
        QObject::connect(streamingSearchEdit_, &QLineEdit::returnPressed, this, [this]() { runStreamingSearch(); });
        QObject::connect(loadDeckAButton, &QPushButton::clicked, this, [this]() { loadSelectedStreamingTrack(0); });
        QObject::connect(loadDeckBButton, &QPushButton::clicked, this, [this]() { loadSelectedStreamingTrack(1); });
        QObject::connect(promoteButton, &QPushButton::clicked, this, [this]() {
            const QVariantMap track = selectedStreamingTrack();
            if (track.isEmpty()) {
                QMessageBox::information(this, QStringLiteral("Streaming Music Services"), QStringLiteral("Select a search result first."));
                return;
            }
            addRequestFromTrack(track, QStringLiteral("Operator"));
        });
        return page;
    }

    QWidget* buildLiveStreamingPage()
    {
        auto* page = new QWidget(this);
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(14);

        // ── Software selection card ──────────────────────────────────────────
        auto* softwareCard = makeSectionCard(
            QStringLiteral("Live Streaming Setup"),
            QStringLiteral("Select your streaming software, choose an output theme, then open the broadcast window for capture."));
        auto* softwareCardLayout = qobject_cast<QVBoxLayout*>(softwareCard->layout());

        broadcastStatusLabel_ = new QLabel(QStringLiteral("Broadcast window is closed"), softwareCard);
        broadcastStatusLabel_->setStyleSheet(QStringLiteral(
            "color:#f8fafc; font-size:18px; font-weight:800; background:transparent;"));
        softwareCardLayout->addWidget(broadcastStatusLabel_);

        // Software selection — row 0: select buttons, row 1: launch buttons
        struct SoftwareEntry {
            QString key, name, desc;
        };
        const QList<SoftwareEntry> softwareChoices = {
            {QStringLiteral("obs"),        QStringLiteral("OBS Studio"),   QStringLiteral("Free & open source")},
            {QStringLiteral("streamlabs"), QStringLiteral("Streamlabs"),   QStringLiteral("Alerts & widgets")},
            {QStringLiteral("xsplit"),     QStringLiteral("XSplit"),       QStringLiteral("Pro broadcast suite")},
            {QStringLiteral("vmix"),       QStringLiteral("vMix"),         QStringLiteral("Multi-camera mixer")},
            {QStringLiteral("other"),      QStringLiteral("Other"),        QStringLiteral("Any capture app")}
        };

        static const QMap<QString, QStringList> knownRelPaths = {
            {QStringLiteral("obs"),        {QStringLiteral("obs-studio/bin/64bit/obs64.exe"),
                                            QStringLiteral("OBS Studio/bin/64bit/obs64.exe"),
                                            QStringLiteral("obs64.exe")}},
            {QStringLiteral("streamlabs"), {QStringLiteral("Streamlabs OBS/Streamlabs OBS.exe"),
                                            QStringLiteral("streamlabs-obs/Streamlabs OBS.exe"),
                                            QStringLiteral("Streamlabs OBS.exe")}},
            {QStringLiteral("xsplit"),     {QStringLiteral("SplitmediaLabs/XSplit Broadcaster/XSplit.Core.exe"),
                                            QStringLiteral("XSplit Broadcaster/XSplit.Core.exe")}},
            {QStringLiteral("vmix"),       {QStringLiteral("StudioCoast Pty Ltd/vMix/vMix64.exe"),
                                            QStringLiteral("vMix/vMix64.exe")}}
        };
        static const QMap<QString, QString> downloadUrls = {
            {QStringLiteral("obs"),        QStringLiteral("https://obsproject.com/download")},
            {QStringLiteral("streamlabs"), QStringLiteral("https://streamlabs.com/download")},
            {QStringLiteral("xsplit"),     QStringLiteral("https://www.xsplit.com")},
            {QStringLiteral("vmix"),       QStringLiteral("https://www.vmix.com/software/download.aspx")}
        };

        auto* swGrid = new QGridLayout();
        swGrid->setHorizontalSpacing(8);
        swGrid->setVerticalSpacing(6);
        softwareButtons_.clear();

        for (int i = 0; i < softwareChoices.size(); ++i) {
            const auto& sw = softwareChoices[i];

            // Row 0: selector button (shows guide text, stays highlighted)
            auto* selBtn = new QPushButton(
                sw.name + QStringLiteral("\n") + sw.desc, softwareCard);
            selBtn->setCheckable(true);
            selBtn->setCursor(Qt::PointingHandCursor);
            selBtn->setMinimumHeight(64);
            selBtn->setStyleSheet(QStringLiteral(
                "QPushButton { background:#0f1829; color:#f1f5f9; border:2px solid #22304a;"
                " border-radius:10px; padding:8px; font-size:11px; font-weight:700; text-align:left; }"
                "QPushButton:hover { background:#1e293b; border-color:#38bdf8; }"
                "QPushButton:checked { background:#17304b; border-color:#38bdf8; color:#7dd3fc; }"));
            swGrid->addWidget(selBtn, 0, i);
            swGrid->setColumnStretch(i, 1);
            softwareButtons_[sw.key] = selBtn;

            QObject::connect(selBtn, &QPushButton::clicked, this,
                [this, key = sw.key]() {
                    selectedSoftware_ = key;
                    saveLiveStreamSettings();
                    refreshBroadcastStatus();
                    refreshBroadcastGuide();
                });

            // Row 1: launch button (only for real apps, not "other")
            if (sw.key != QStringLiteral("other")) {
                auto* launchBtn = new QPushButton(
                    QStringLiteral("Launch ") + sw.name, softwareCard);
                launchBtn->setCursor(Qt::PointingHandCursor);
                launchBtn->setMinimumHeight(28);
                launchBtn->setStyleSheet(QStringLiteral(
                    "QPushButton { background:#1e3a5f; color:#93c5fd; border:1px solid #2563eb;"
                    " border-radius:7px; padding:4px 6px; font-size:10px; font-weight:700; }"
                    "QPushButton:hover { background:#2563eb; color:#fff; }"));
                swGrid->addWidget(launchBtn, 1, i);

                QObject::connect(launchBtn, &QPushButton::clicked, this,
                    [key = sw.key]() {
                        const QStringList roots = {
                            QStringLiteral("C:/Program Files"),
                            QStringLiteral("C:/Program Files (x86)"),
                            QStandardPaths::writableLocation(QStandardPaths::DesktopLocation),
                            QStandardPaths::writableLocation(QStandardPaths::HomeLocation),
                            QStandardPaths::writableLocation(QStandardPaths::AppLocalDataLocation)
                        };
                        for (const QString& root : roots) {
                            for (const QString& rel : knownRelPaths[key]) {
                                const QString full = root + QStringLiteral("/") + rel;
                                if (QFile::exists(full)) {
                                    QProcess::startDetached(full, {}, QFileInfo(full).absolutePath());
                                    return;
                                }
                            }
                        }
                        QDesktopServices::openUrl(QUrl(downloadUrls[key]));
                    });
            }
        }
        auto* swContainer = new QWidget(softwareCard);
        swContainer->setLayout(swGrid);
        softwareCardLayout->addWidget(swContainer);

        // Theme row
        auto* themeLabel = new QLabel(QStringLiteral("Output Theme:"), softwareCard);
        themeLabel->setStyleSheet(QStringLiteral(
            "color:#94a3b8; font-size:11px; font-weight:700; background:transparent;"));
        softwareCardLayout->addWidget(themeLabel);
        auto* themeRow = new QHBoxLayout();
        const QList<QPair<QString, QString>> themeChoices = {
            {QStringLiteral("default"), QStringLiteral("Professional")},
            {QStringLiteral("minimal"), QStringLiteral("Minimal")},
            {QStringLiteral("bar"),     QStringLiteral("Bottom Bar")},
            {QStringLiteral("vinyl"),   QStringLiteral("Vinyl")}
        };
        for (const auto& entry : themeChoices) {
            auto* button = makeChoiceButton(entry.second);
            themeButtons_[entry.first] = button;
            themeRow->addWidget(button);
            QObject::connect(button, &QPushButton::clicked, this, [this, key = entry.first]() {
                selectedTheme_ = key;
                saveLiveStreamSettings();
                refreshBroadcastGuide();
                refreshBroadcastPreview();
            });
        }
        themeRow->addStretch(1);
        softwareCardLayout->addLayout(themeRow);

        // Resolution + broadcast controls
        auto* controlsRow = new QHBoxLayout();
        auto* resLabel = new QLabel(QStringLiteral("Resolution:"), softwareCard);
        resLabel->setStyleSheet(QStringLiteral("background:transparent;"));
        broadcastResolutionCombo_ = new QComboBox(softwareCard);
        broadcastResolutionCombo_->addItems({
            QStringLiteral("1920x1080"),
            QStringLiteral("1280x720"),
            QStringLiteral("2560x1440"),
            QStringLiteral("3840x2160")
        });
        auto* openButton = new QPushButton(QStringLiteral("Open Broadcast Window"), softwareCard);
        auto* closeButton = new QPushButton(QStringLiteral("Close Broadcast Window"), softwareCard);
        controlsRow->addWidget(resLabel);
        controlsRow->addWidget(broadcastResolutionCombo_);
        controlsRow->addSpacing(12);
        controlsRow->addWidget(openButton);
        controlsRow->addWidget(closeButton);
        controlsRow->addStretch(1);
        softwareCardLayout->addLayout(controlsRow);
        layout->addWidget(softwareCard);

        // ── Setup Guide card ─────────────────────────────────────────────────
        auto* guideCard = makeSectionCard(
            QStringLiteral("Setup Guide"),
            QStringLiteral("Step-by-step instructions for first-time and recurring broadcasts."));
        auto* guideLayout = qobject_cast<QVBoxLayout*>(guideCard->layout());
        broadcastGuide_ = new QPlainTextEdit(guideCard);
        broadcastGuide_->setReadOnly(true);
        broadcastGuide_->setMinimumHeight(320);
        guideLayout->addWidget(broadcastGuide_);
        layout->addWidget(guideCard, 1);

        QObject::connect(openButton, &QPushButton::clicked, this,
            [this]() { openBroadcastWindow(); });
        QObject::connect(closeButton, &QPushButton::clicked, this,
            [this]() { closeBroadcastWindow(); });
        QObject::connect(broadcastResolutionCombo_, &QComboBox::currentTextChanged, this,
            [this]() { saveLiveStreamSettings(); refreshBroadcastPreview(); });

        selectedSoftware_ = QStringLiteral("obs");
        selectedTheme_ = QStringLiteral("default");
        refreshBroadcastGuide();
        return page;
    }

    void switchSection(int index)
    {
        contentStack_->setCurrentIndex(index);
        if (auto* button = navGroup_->button(index)) {
            button->setChecked(true);
        }
    }

    void refreshHardwareDevices()
    {
        QStringList devices;
        if (bridge_) {
            devices = bridge_->listAudioDeviceNames();
        }
        const QString activeDevice = bridge_ ? bridge_->activeAudioDeviceName() : QString();

        hardwareDeviceList_->clear();
        hardwareDeviceCombo_->clear();
        for (const QString& device : devices) {
            hardwareDeviceList_->addItem(device);
            hardwareDeviceCombo_->addItem(device);
        }
        if (!activeDevice.isEmpty()) {
            const int idx = hardwareDeviceCombo_->findText(activeDevice);
            if (idx >= 0) {
                hardwareDeviceCombo_->setCurrentIndex(idx);
            }
        }

        hardwareDeviceCountLabel_->setText(QString::number(devices.size()));
        hardwareActiveDeviceLabel_->setText(activeDevice.isEmpty() ? QStringLiteral("No active device") : activeDevice);
        if (hwCtrlConnectedLabel_) {
            hwCtrlConnectedLabel_->setText(QString::number(devices.size()));
        }
        hardwareMidiStatusLabel_->setText(devices.isEmpty()
            ? QStringLiteral("No engine-visible devices detected. Connect hardware and scan again.")
            : QStringLiteral("%1 output devices detected. MIDI readiness is tied to active engine audio routing.").arg(devices.size()));
    }

    void refreshMidiDevices()
    {
        if (!hardwareMidiDeviceList_ || !hardwareMidiStatusLabel_) return;
        hardwareMidiDeviceList_->clear();
        QStringList devices;
        if (bridge_) devices = bridge_->listMidiDeviceNames();
        for (const QString& d : devices) {
            auto* item = new QListWidgetItem(d, hardwareMidiDeviceList_);
            item->setForeground(QColor(QStringLiteral("#60a5fa")));
            hardwareMidiDeviceList_->addItem(item);
        }
        if (devices.isEmpty()) {
            auto* noItem = new QListWidgetItem(
                QStringLiteral("No MIDI devices found. Connect a controller and scan again."),
                hardwareMidiDeviceList_);
            noItem->setForeground(QColor(QStringLiteral("#6b7280")));
            hardwareMidiDeviceList_->addItem(noItem);
        }
        hardwareMidiStatusLabel_->setText(devices.isEmpty()
            ? QStringLiteral("No MIDI devices detected.")
            : QStringLiteral("%1 MIDI device(s) found.").arg(devices.size()));
    }

    void refreshStreamingServices()
    {
        int connectedCount = 0;
        for (auto it = streamingConnected_.begin(); it != streamingConnected_.end(); ++it) {
            const bool connected = it->second;
            if (connected) { ++connectedCount; }
            const QString displayName = streamingDisplayNames_.count(it->first)
                ? streamingDisplayNames_[it->first] : it->first;
            if (streamingStatusLabels_.count(it->first)) {
                streamingStatusLabels_[it->first]->setText(
                    connected ? QStringLiteral("Connected") : QStringLiteral("Disconnected"));
                streamingStatusLabels_[it->first]->setStyleSheet(
                    QStringLiteral("color:%1; font-size:12px;")
                        .arg(connected ? QStringLiteral("#2dd4bf") : QStringLiteral("#94a3b8")));
            }
            if (streamingToggleButtons_.count(it->first)) {
                streamingToggleButtons_[it->first]->setText(
                    connected ? QStringLiteral("Disconnect") : QStringLiteral("Connect %1").arg(displayName));
            }
        }
        setWindowTitle(QStringLiteral("Ancillary Screens (%1 services connected)").arg(connectedCount));
    }

    void runStreamingSearch()
    {
        const QString query = streamingSearchEdit_->text().trimmed();
        const QString source = streamingServiceFilter_->currentText().trimmed();
        QList<QVariantMap> results;
        if (searchProvider_) {
            results = searchProvider_(query, source);
        }
        if (results.isEmpty()) {
            streamingResultsTree_->clear();
            auto* noResults = new QTreeWidgetItem(streamingResultsTree_);
            noResults->setText(0, query.isEmpty()
                ? QStringLiteral("Type a search term above and press Search.")
                : QStringLiteral("No results found for \u201c%1\u201d.").arg(query));
            noResults->setFlags(noResults->flags() & ~Qt::ItemIsSelectable);
            return;
        }

        streamingResultsTree_->clear();
        for (const QVariantMap& result : results) {
            auto* item = new QTreeWidgetItem(streamingResultsTree_);
            item->setText(0, result.value(QStringLiteral("title")).toString());
            item->setText(1, result.value(QStringLiteral("artist")).toString());
            item->setText(2, result.value(QStringLiteral("service")).toString());
            item->setText(3, result.value(QStringLiteral("bpm")).toString());
            item->setText(4, result.value(QStringLiteral("key")).toString());
            item->setData(0, Qt::UserRole, result);
        }
        if (!results.isEmpty()) {
            streamingResultsTree_->setCurrentItem(streamingResultsTree_->topLevelItem(0));
        }
    }

    QVariantMap selectedStreamingTrack() const
    {
        if (auto* item = streamingResultsTree_->currentItem()) {
            return item->data(0, Qt::UserRole).toMap();
        }
        return {};
    }

    void loadSelectedStreamingTrack(int deckIndex)
    {
        const QVariantMap track = selectedStreamingTrack();
        if (track.isEmpty()) {
            QMessageBox::information(this, QStringLiteral("Streaming Music Services"), QStringLiteral("Select a search result first."));
            return;
        }
        if (!trackLoader_) {
            QMessageBox::information(this, QStringLiteral("Streaming Music Services"), QStringLiteral("Track loading is not wired in this build."));
            return;
        }
        const TrackLoadResult result = trackLoader_(track, deckIndex);
        if (!result.dispatched) {
            QMessageBox::warning(this, QStringLiteral("Streaming Music Services"), result.reason.isEmpty() ? QStringLiteral("Track load failed.") : result.reason);
        }
    }

    void addManualRequest()
    {
        QDialog dialog(this);
        dialog.setWindowTitle(QStringLiteral("Add Crowd Request"));
        dialog.setStyleSheet(styleSheet());
        auto* layout = new QVBoxLayout(&dialog);
        auto* requesterEdit = new QLineEdit(&dialog);
        auto* titleEdit = new QLineEdit(&dialog);
        auto* artistEdit = new QLineEdit(&dialog);
        requesterEdit->setPlaceholderText(QStringLiteral("Requester"));
        titleEdit->setPlaceholderText(QStringLiteral("Track title"));
        artistEdit->setPlaceholderText(QStringLiteral("Artist"));
        layout->addWidget(requesterEdit);
        layout->addWidget(titleEdit);
        layout->addWidget(artistEdit);
        auto* buttonRow = new QHBoxLayout();
        auto* cancelButton = new QPushButton(QStringLiteral("Cancel"), &dialog);
        auto* addButton = new QPushButton(QStringLiteral("Add"), &dialog);
        buttonRow->addStretch(1);
        buttonRow->addWidget(cancelButton);
        buttonRow->addWidget(addButton);
        layout->addLayout(buttonRow);
        QObject::connect(cancelButton, &QPushButton::clicked, &dialog, &QDialog::reject);
        QObject::connect(addButton, &QPushButton::clicked, &dialog, &QDialog::accept);

        if (dialog.exec() != QDialog::Accepted) {
            return;
        }

        QVariantMap track;
        track.insert(QStringLiteral("title"), titleEdit->text().trimmed());
        track.insert(QStringLiteral("artist"), artistEdit->text().trimmed());
        track.insert(QStringLiteral("service"), QStringLiteral("manual"));
        if (track.value(QStringLiteral("title")).toString().isEmpty()) {
            QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Track title is required."));
            return;
        }
        addRequestFromTrack(track, requesterEdit->text().trimmed().isEmpty() ? QStringLiteral("Anonymous") : requesterEdit->text().trimmed());
    }

    void addRequestFromTrack(const QVariantMap& track, const QString& requester)
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning()) {
            QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Start the request server before adding requests."));
            return;
        }
        QJsonObject payload;
        payload.insert(QStringLiteral("requested_title"), track.value(QStringLiteral("title")).toString().trimmed());
        payload.insert(QStringLiteral("requested_artist"), track.value(QStringLiteral("artist")).toString().trimmed());
        payload.insert(QStringLiteral("requester_name"), requester.trimmed().isEmpty() ? QStringLiteral("Anonymous") : requester.trimmed());
        if (track.contains(QStringLiteral("filePath"))) {
            payload.insert(QStringLiteral("file_path"), track.value(QStringLiteral("filePath")).toString());
        }
        if (track.contains(QStringLiteral("file_path_normalized"))) {
            payload.insert(QStringLiteral("file_path_normalized"), track.value(QStringLiteral("file_path_normalized")).toString());
        }
        if (track.contains(QStringLiteral("stable_identity_key"))) {
            payload.insert(QStringLiteral("stable_identity_key"), track.value(QStringLiteral("stable_identity_key")).toString());
        }
        if (track.contains(QStringLiteral("identity_confidence"))) {
            payload.insert(QStringLiteral("identity_confidence"), track.value(QStringLiteral("identity_confidence")).toString());
        }
        if (track.contains(QStringLiteral("identity_match_basis"))) {
            payload.insert(QStringLiteral("identity_match_basis"), track.value(QStringLiteral("identity_match_basis")).toString());
        }
        if (track.contains(QStringLiteral("track_id"))) {
            payload.insert(QStringLiteral("track_id"), track.value(QStringLiteral("track_id")).toString());
        }
        if (track.contains(QStringLiteral("authority_track_id"))) {
            payload.insert(QStringLiteral("authority_track_id"), track.value(QStringLiteral("authority_track_id")).toString());
        }
        QString error;
        if (!crowdBackend_->submitRequest(payload, &error)) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Request submission failed.") : error);
            return;
        }
        refreshRequestQueue();
        switchSection(1);
    }

    QString selectedRequestId() const
    {
        if (auto* item = requestQueueTree_->currentItem()) {
            return item->data(0, Qt::UserRole).toString();
        }
        return {};
    }

    void acceptSelectedRequest(int deckIndex)
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning()) {
            return;
        }
        const QString requestId = selectedRequestId();
        if (requestId.isEmpty()) {
            return;
        }
        QVariantMap request;
        QString error;
        if (!crowdBackend_->operatorAction(QStringLiteral("accept"), requestId, &request, &error)) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Accept action failed.") : error);
            return;
        }
        const QString filePath = request.value(QStringLiteral("file_path")).toString().trimmed();
        const QString deckLabel = deckIndex == 0 ? QStringLiteral("A") : QStringLiteral("B");
        if (!trackLoader_) {
            finalizeRequestHandoff(requestId, deckLabel, QStringLiteral("HANDOFF_FAILED"), QStringLiteral("Deck handoff is not wired in this build."), filePath);
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), QStringLiteral("Deck handoff is not wired in this build."));
            return;
        }
        if (filePath.isEmpty()) {
            finalizeRequestHandoff(requestId, deckLabel, QStringLiteral("HANDOFF_FAILED"), QStringLiteral("No local file path was available for direct deck loading."), QString());
            QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Request accepted, but this item is not mapped to a local library file yet."));
            return;
        }
        QVariantMap track;
        track.insert(QStringLiteral("title"), request.value(QStringLiteral("requested_title")).toString());
        track.insert(QStringLiteral("artist"), request.value(QStringLiteral("requested_artist")).toString());
        track.insert(QStringLiteral("filePath"), filePath);
        const TrackLoadResult result = trackLoader_(track, deckIndex);
        if (!result.dispatched) {
            finalizeRequestHandoff(requestId, deckLabel, QStringLiteral("HANDOFF_FAILED"), result.reason.isEmpty() ? QStringLiteral("Deck handoff dispatch failed.") : result.reason, filePath);
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), result.reason.isEmpty() ? QStringLiteral("Deck handoff dispatch failed.") : result.reason);
            return;
        }
        if (requestHandoffLabel_) {
            requestHandoffLabel_->setText(QStringLiteral("Accepted '%1' to Deck %2. Waiting for real deck confirmation...").arg(request.value(QStringLiteral("requested_title")).toString(), deckLabel));
        }
        beginHandoffVerification(requestId, request.value(QStringLiteral("requested_title")).toString(), deckIndex, filePath);
        refreshRequestQueue();
    }

    void rejectSelectedRequest()
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning()) {
            return;
        }
        const QString requestId = selectedRequestId();
        if (requestId.isEmpty()) {
            return;
        }
        QString error;
        QVariantMap ignored;
        if (!crowdBackend_->operatorAction(QStringLiteral("reject"), requestId, &ignored, &error)) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Reject action failed.") : error);
            return;
        }
        refreshRequestQueue();
    }

    void removeSelectedRequest()
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning()) {
            return;
        }
        const QString requestId = selectedRequestId();
        if (requestId.isEmpty()) {
            return;
        }
        QString error;
        QVariantMap ignored;
        if (!crowdBackend_->operatorAction(QStringLiteral("remove"), requestId, &ignored, &error)) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Remove action failed.") : error);
            return;
        }
        refreshRequestQueue();
    }

    void startRequestServer()
    {
        if (!crowdBackend_) {
            return;
        }
        // Persist any pending policy/payment edits before launching the sidecar.
        saveRequestSettings();
        if (!crowdBackend_->start()) {
            const auto status = crowdBackend_->status(false);
            requestServerStateLabel_->setText(QStringLiteral("Queue failed"));
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), status.lastError.isEmpty() ? QStringLiteral("Failed to start the local request server.") : status.lastError);
            refreshRequestQueue();
            return;
        }
        loadRequestSettings();
        requestPollTimer_.start();
        refreshRequestQueue();
    }

    void shutdownRequestServer(bool quiet)
    {
        requestPollTimer_.stop();
        if (crowdBackend_) {
            crowdBackend_->stop(quiet);
        }
        requestItems_.clear();
        refreshRequestQueue();
    }

    void saveRequestSettings()
    {
        if (!crowdBackend_ || !requestPolicyCombo_ || !requestVenmoEdit_) {
            return;
        }
        if (requestSettingsAutoSaveTimer_.isActive()) {
            requestSettingsAutoSaveTimer_.stop();
        }
        QJsonObject handles;
        handles.insert(QStringLiteral("venmo"), requestVenmoEdit_->text().trimmed());
        handles.insert(QStringLiteral("cashapp"), requestCashAppEdit_ ? requestCashAppEdit_->text().trimmed() : QString());
        handles.insert(QStringLiteral("paypal"), requestPaypalEdit_ ? requestPaypalEdit_->text().trimmed() : QString());
        handles.insert(QStringLiteral("zelle"), requestZelleEdit_ ? requestZelleEdit_->text().trimmed() : QString());
        handles.insert(QStringLiteral("buymeacoffee"), requestBuyMeACoffeeEdit_ ? requestBuyMeACoffeeEdit_->text().trimmed() : QString());
        handles.insert(QStringLiteral("chime"), requestChimeEdit_ ? requestChimeEdit_->text().trimmed() : QString());
        handles.insert(QStringLiteral("card_url"), requestCardUrlEdit_ ? requestCardUrlEdit_->text().trimmed() : QString());
        QJsonObject payload;
        const QString policyToSave = requestPolicyCombo_->currentText();
        payload.insert(QStringLiteral("request_policy"), policyToSave);
        payload.insert(QStringLiteral("payment_handles"), handles);
        payload.insert(QStringLiteral("updated_at"), QDateTime::currentDateTimeUtc().toString(Qt::ISODate));
        qDebug() << "[AncillaryScreensWidget] Saving request_policy:" << policyToSave;
        QString error;
        if (!crowdBackend_->saveSettings(payload, &error)) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("Failed to save payment settings.") : error);
            if (requestSaveSettingsStatusLabel_) {
                requestSaveSettingsStatusLabel_->setText(QStringLiteral("Save failed."));
            }
            return;
        }
        if (requestSaveSettingsStatusLabel_) {
            requestSaveSettingsStatusLabel_->setText(requestServerRunning_
                ? QStringLiteral("Saved. Guests will see the updated payment options on the request page.")
                : QStringLiteral("Saved locally. Start the request server to publish the updated payment options on the request page."));
        }
        loadRequestSettings();
    }

    void scheduleRequestSettingsAutoSave()
    {
        if (!crowdBackend_) {
            return;
        }
        if (requestSaveSettingsStatusLabel_) {
            requestSaveSettingsStatusLabel_->setText(QStringLiteral("Saving changes..."));
        }
        requestSettingsAutoSaveTimer_.start();
    }

    void showRequestQrDialog()
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning()) {
            QMessageBox::information(this, QStringLiteral("Crowd Requests"), QStringLiteral("Start the request server before opening the QR dialog."));
            return;
        }
        QString error;
        const QByteArray png = crowdBackend_->fetchQrPng(&error);
        if (png.isEmpty()) {
            QMessageBox::warning(this, QStringLiteral("Crowd Requests"), error.isEmpty() ? QStringLiteral("QR generation failed.") : error);
            return;
        }

        QDialog dialog(this);
        dialog.setWindowTitle(QStringLiteral("Crowd Requests QR"));
        dialog.setStyleSheet(styleSheet());
        auto* layout = new QVBoxLayout(&dialog);
        auto* intro = new QLabel(QStringLiteral("Guests can scan this QR code to open the local Crowd Requests page."), &dialog);
        intro->setWordWrap(true);
        layout->addWidget(intro);

        QPixmap pixmap;
        pixmap.loadFromData(png, "PNG");
        auto* qrLabel = new QLabel(&dialog);
        qrLabel->setAlignment(Qt::AlignCenter);
        qrLabel->setPixmap(pixmap.scaled(320, 320, Qt::KeepAspectRatio, Qt::SmoothTransformation));
        layout->addWidget(qrLabel, 0, Qt::AlignCenter);

        auto* urlLabel = new QLabel(joinUrl_, &dialog);
        urlLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
        urlLabel->setWordWrap(true);
        layout->addWidget(urlLabel);

        auto* actions = new QHBoxLayout();
        auto* copyUrlButton = new QPushButton(QStringLiteral("Copy URL"), &dialog);
        auto* copyImageButton = new QPushButton(QStringLiteral("Copy QR"), &dialog);
        auto* saveButton = new QPushButton(QStringLiteral("Save PNG"), &dialog);
        auto* closeButton = new QPushButton(QStringLiteral("Close"), &dialog);
        actions->addWidget(copyUrlButton);
        actions->addWidget(copyImageButton);
        actions->addWidget(saveButton);
        actions->addStretch(1);
        actions->addWidget(closeButton);
        layout->addLayout(actions);

        QObject::connect(copyUrlButton, &QPushButton::clicked, &dialog, [this]() {
            QApplication::clipboard()->setText(joinUrl_);
        });
        QObject::connect(copyImageButton, &QPushButton::clicked, &dialog, [pixmap]() {
            QApplication::clipboard()->setPixmap(pixmap);
        });
        QObject::connect(saveButton, &QPushButton::clicked, &dialog, [this, png]() {
            const QString target = QFileDialog::getSaveFileName(this, QStringLiteral("Save Crowd Requests QR"), QStringLiteral("crowd_requests_qr.png"), QStringLiteral("PNG Image (*.png)"));
            if (target.isEmpty()) {
                return;
            }
            QFile file(target);
            if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
                return;
            }
            file.write(png);
            file.close();
        });
        QObject::connect(closeButton, &QPushButton::clicked, &dialog, &QDialog::accept);
        dialog.exec();
    }

    void loadRequestSettings()
    {
        if (!crowdBackend_) {
            return;
        }
        QString error;
        const QJsonObject settings = crowdBackend_->loadSettings(&error);
        if (settings.isEmpty()) {
            return;
        }
        const QJsonObject handles = settings.value(QStringLiteral("payment_handles")).toObject();
        const QSignalBlocker blocker1(requestPolicyCombo_);
        const QSignalBlocker blocker2(requestVenmoEdit_);
        const QSignalBlocker blocker3(requestCashAppEdit_);
        const QSignalBlocker blocker4(requestPaypalEdit_);
        const QSignalBlocker blocker5(requestZelleEdit_);
        const QSignalBlocker blocker6(requestBuyMeACoffeeEdit_);
        const QSignalBlocker blocker7(requestChimeEdit_);
        const QSignalBlocker blocker8(requestCardUrlEdit_);
        if (requestPolicyCombo_) {
            const QString policy = settings.value(QStringLiteral("request_policy")).toString(QStringLiteral("free"));
            qDebug() << "[AncillaryScreensWidget] Loaded request_policy:" << policy;
            const int idx = requestPolicyCombo_->findText(policy);
            if (idx >= 0) {
                requestPolicyCombo_->setCurrentIndex(idx);
            }
        }
        if (requestVenmoEdit_) requestVenmoEdit_->setText(handles.value(QStringLiteral("venmo")).toString());
        if (requestCashAppEdit_) requestCashAppEdit_->setText(handles.value(QStringLiteral("cashapp")).toString());
        if (requestPaypalEdit_) requestPaypalEdit_->setText(handles.value(QStringLiteral("paypal")).toString());
        if (requestZelleEdit_) requestZelleEdit_->setText(handles.value(QStringLiteral("zelle")).toString());
        if (requestBuyMeACoffeeEdit_) requestBuyMeACoffeeEdit_->setText(handles.value(QStringLiteral("buymeacoffee")).toString());
        if (requestChimeEdit_) requestChimeEdit_->setText(handles.value(QStringLiteral("chime")).toString());
        if (requestCardUrlEdit_) requestCardUrlEdit_->setText(handles.value(QStringLiteral("card_url")).toString());
    }

    void refreshRequestQueue(bool syncBeforeFetch = true)
    {
        requestItems_.clear();
        if (crowdBackend_ && crowdBackend_->isRunning()) {
            processPendingHandoffVerifications(true);
            if (syncBeforeFetch) {
                syncGuestNowPlaying(true);
            }
            QString error;
            requestItems_ = crowdBackend_->fetchQueue(&error);
        }
        const auto status = crowdBackend_ ? crowdBackend_->status(crowdBackend_->isRunning()) : CrowdRequestBackend::StatusSnapshot{};
        requestServerRunning_ = status.running;
        joinUrl_ = status.joinUrl;
        if (requestUrlLabel_) {
            requestUrlLabel_->setText(joinUrl_.isEmpty() ? QStringLiteral("Click Start Request Server to expose the local join URL.") : joinUrl_);
        }
        if (requestUrlDisplayLabel_) {
            requestUrlDisplayLabel_->setText(joinUrl_.isEmpty() ? QStringLiteral("Click Start Request Server to expose the local join URL.") : joinUrl_);
        }
        if (requestServerStateLabel_) {
            requestServerStateLabel_->setText(requestServerRunning_ ? QStringLiteral("Queue online") : QStringLiteral("Queue offline"));
        }
        if (requestStartButton_) {
            requestStartButton_->setEnabled(!requestServerRunning_);
        }
        if (requestStopButton_) {
            requestStopButton_->setEnabled(requestServerRunning_);
        }
        if (requestCopyButton_) {
            requestCopyButton_->setEnabled(requestServerRunning_ && !joinUrl_.trimmed().isEmpty());
        }
        if (requestOpenGuestButton_) {
            requestOpenGuestButton_->setEnabled(requestServerRunning_ && !joinUrl_.trimmed().isEmpty());
        }
        if (requestQrButton_) {
            requestQrButton_->setEnabled(requestServerRunning_ && !joinUrl_.trimmed().isEmpty());
        }
        if (requestClearButton_) {
            requestClearButton_->setEnabled(requestServerRunning_);
        }
        if (requestSaveSettingsButton_) {
            requestSaveSettingsButton_->setEnabled(crowdBackend_ != nullptr);
        }
        const int pending = std::count_if(requestItems_.cbegin(), requestItems_.cend(), [](const QVariantMap& entry) {
            return entry.value(QStringLiteral("status")).toString() == QStringLiteral("PENDING");
        });
        if (requestPendingLabel_) {
            requestPendingLabel_->setText(QStringLiteral("%1 pending").arg(pending));
        }
        if (requestCountLabel_) {
            requestCountLabel_->setText(QStringLiteral("%1 total").arg(status.requestCount));
        }
        if (requestAcceptedLabel_) {
            requestAcceptedLabel_->setText(QStringLiteral("%1 accepted").arg(status.acceptedCount));
        }
        if (requestHandedOffLabel_) {
            requestHandedOffLabel_->setText(QStringLiteral("%1 handed off").arg(status.handedOffCount));
        }
        if (requestNowPlayingLabel_) {
            requestNowPlayingLabel_->setText(QStringLiteral("%1 now playing").arg(status.nowPlayingCount));
        }
        if (requestPlayedLabel_) {
            requestPlayedLabel_->setText(QStringLiteral("%1 played").arg(status.playedCount));
        }
        if (requestFailedLabel_) {
            requestFailedLabel_->setText(QStringLiteral("%1 failed").arg(status.handoffFailedCount));
        }
        requestQueueTree_->clear();
        for (const QVariantMap& entry : requestItems_) {
            auto* item = new QTreeWidgetItem(requestQueueTree_);
            item->setText(0, entry.value(QStringLiteral("requested_title")).toString());
            item->setText(1, entry.value(QStringLiteral("requested_artist")).toString());
            item->setText(2, entry.value(QStringLiteral("requester_name")).toString());
            item->setText(3, QString::number(entry.value(QStringLiteral("votes")).toInt()));
            const QString requestStatus = entry.value(QStringLiteral("status")).toString();
            item->setText(4, requestStatus);
            const QBrush textBrush = statusBrush(requestStatus);
            const QBrush backgroundBrush = statusBackgroundBrush(requestStatus);
            QFont rowFont = requestQueueTree_->font();
            rowFont.setBold(requestStatus == QStringLiteral("NOW_PLAYING") || requestStatus == QStringLiteral("ACCEPTED"));
            if (requestStatus == QStringLiteral("NOW_PLAYING")) {
                rowFont.setPointSize(rowFont.pointSize() + 1);
            }
            item->setSizeHint(0, QSize(0, requestStatus == QStringLiteral("NOW_PLAYING") ? 44 : 38));
            for (int column = 0; column < 5; ++column) {
                item->setForeground(column, textBrush);
                item->setBackground(column, backgroundBrush);
                item->setFont(column, rowFont);
            }
            const QString detail = entry.value(QStringLiteral("handoff_detail")).toString().trimmed();
            if (!detail.isEmpty()) {
                item->setToolTip(4, detail);
            }
            item->setData(0, Qt::UserRole, entry.value(QStringLiteral("request_id")).toString());
        }
    }

    QString nowPlayingEventFingerprint(const QVariantMap& nowPlaying) const
    {
        QStringList parts;
        const QVariantList activeDecks = nowPlaying.value(QStringLiteral("active_decks")).toList();
        for (const QVariant& entryValue : activeDecks) {
            const QVariantMap entry = entryValue.toMap();
            parts << QStringLiteral("%1|%2|%3|%4|%5|%6")
                         .arg(entry.value(QStringLiteral("deck")).toString(),
                              entry.value(QStringLiteral("is_playing")).toBool() ? QStringLiteral("1") : QStringLiteral("0"),
                              QDir::cleanPath(entry.value(QStringLiteral("file_path")).toString()),
                              QString::number(entry.value(QStringLiteral("signal_bucket")).toInt()),
                              entry.value(QStringLiteral("title")).toString(),
                              entry.value(QStringLiteral("artist")).toString());
        }
        parts << QStringLiteral("summary|%1|%2|%3|%4")
                     .arg(nowPlaying.value(QStringLiteral("title")).toString(),
                          nowPlaying.value(QStringLiteral("artist")).toString(),
                          nowPlaying.value(QStringLiteral("deck")).toString(),
                          QDir::cleanPath(nowPlaying.value(QStringLiteral("file_path")).toString()));
        return parts.join(QStringLiteral("||"));
    }

    QString nowPlayingPeakFingerprint(const QVariantMap& nowPlaying) const
    {
        QStringList parts;
        const QVariantList activeDecks = nowPlaying.value(QStringLiteral("active_decks")).toList();
        for (const QVariant& entryValue : activeDecks) {
            const QVariantMap entry = entryValue.toMap();
            parts << QStringLiteral("%1|%2")
                         .arg(entry.value(QStringLiteral("deck")).toString(),
                              QString::number(entry.value(QStringLiteral("peak_level")).toDouble(), 'f', 2));
        }
        return parts.join(QStringLiteral("||"));
    }

    bool syncGuestNowPlaying(bool force = false)
    {
        if (!crowdBackend_ || !crowdBackend_->isRunning() || !nowPlayingProvider_) {
            return false;
        }
        const QVariantMap nowPlaying = nowPlayingProvider_();
        const QString eventFingerprint = nowPlayingEventFingerprint(nowPlaying);
        const QString peakFingerprint = nowPlayingPeakFingerprint(nowPlaying);
        const int activeDeckCount = nowPlaying.value(QStringLiteral("active_decks")).toList().size();
        const qint64 nowMs = QDateTime::currentMSecsSinceEpoch();
        const bool overlapRefreshDue = activeDeckCount > 1
            && peakFingerprint != lastNowPlayingPeakFingerprint_
            && (nowMs - lastNowPlayingSyncMs_) >= 120;
        if (!force && eventFingerprint == lastNowPlayingEventFingerprint_ && !overlapRefreshDue) {
            return false;
        }
        const QJsonObject payload = QJsonObject::fromVariantMap(nowPlaying);
        QString ignored;
        crowdBackend_->saveNowPlaying(payload, &ignored);
        lastNowPlayingEventFingerprint_ = eventFingerprint;
        lastNowPlayingPeakFingerprint_ = peakFingerprint;
        lastNowPlayingSyncMs_ = nowMs;
        return true;
    }

    void scheduleRequestRefreshFromDeckEvent()
    {
        if (!requestRefreshDebounceTimer_.isActive()) {
            requestRefreshDebounceTimer_.start();
        }
    }

    void onDeckSnapshotUpdated()
    {
        lastDeckSnapshotAtMs_ = QDateTime::currentMSecsSinceEpoch();
        processPendingHandoffVerifications(false);
        if (!crowdBackend_ || !crowdBackend_->isRunning() || !nowPlayingProvider_) {
            return;
        }
        if (!nowPlayingSyncDebounceTimer_.isActive()) {
            nowPlayingSyncDebounceTimer_.start();
        }
    }

    void finalizeRequestHandoff(const QString& requestId,
                                const QString& deckLabel,
                                const QString& status,
                                const QString& detail,
                                const QString& targetPath)
    {
        pendingHandoffVerifications_.erase(requestId);
        if (pendingHandoffVerifications_.empty()) {
            handoffFallbackTimer_.stop();
        }
        if (!crowdBackend_) {
            return;
        }
        QVariantMap updated;
        QString error;
        if (!crowdBackend_->reportHandoff(requestId, status, deckLabel, detail, targetPath, &updated, &error)) {
            if (requestHandoffLabel_) {
                requestHandoffLabel_->setText(QStringLiteral("Handoff state update failed: %1").arg(error.isEmpty() ? QStringLiteral("unknown error") : error));
            }
            refreshRequestQueue();
            return;
        }
        if (requestHandoffLabel_) {
            requestHandoffLabel_->setText(QStringLiteral("Deck %1 %2: %3").arg(deckLabel, status, detail));
        }
        refreshRequestQueue(false);
    }

    void beginHandoffVerification(const QString& requestId,
                                  const QString& requestedTitle,
                                  int deckIndex,
                                  const QString& expectedPath)
    {
        PendingHandoffVerification pending;
        pending.requestId = requestId;
        pending.requestedTitle = requestedTitle;
        pending.deckIndex = deckIndex;
        pending.expectedPath = expectedPath;
        pending.startedAtMs = QDateTime::currentMSecsSinceEpoch();
        pending.deadlineAtMs = pending.startedAtMs + 6000;
        pendingHandoffVerifications_[requestId] = pending;
        if (!handoffFallbackTimer_.isActive()) {
            handoffFallbackTimer_.start();
        }
        processPendingHandoffVerifications(false);
    }

    void processPendingHandoffVerifications(bool fromFallbackTimer)
    {
        if (pendingHandoffVerifications_.empty()) {
            handoffFallbackTimer_.stop();
            return;
        }
        if (!bridge_ || !crowdBackend_) {
            const auto pending = pendingHandoffVerifications_.begin()->second;
            finalizeRequestHandoff(pending.requestId,
                                   pending.deckIndex == 0 ? QStringLiteral("A") : QStringLiteral("B"),
                                   QStringLiteral("HANDOFF_FAILED"),
                                   QStringLiteral("Deck bridge was unavailable during handoff verification."),
                                   pending.expectedPath);
            return;
        }
        const qint64 nowMs = QDateTime::currentMSecsSinceEpoch();
        std::vector<QString> completedIds;
        for (auto& entry : pendingHandoffVerifications_) {
            PendingHandoffVerification& pending = entry.second;
            const QString deckLabel = pending.deckIndex == 0 ? QStringLiteral("A") : QStringLiteral("B");
            const QString livePath = QDir::cleanPath(bridge_->deckFilePath(pending.deckIndex));
            const QString targetPath = QDir::cleanPath(pending.expectedPath);
            const bool pathMatches = !targetPath.isEmpty() && livePath.compare(targetPath, Qt::CaseInsensitive) == 0;
            if (pathMatches) {
                pending.pathSeen = true;
            }
            if (pathMatches && bridge_->isDeckFullyDecoded(pending.deckIndex)) {
                const qint64 elapsedMs = nowMs - pending.startedAtMs;
                finalizeRequestHandoff(pending.requestId,
                                       deckLabel,
                                       QStringLiteral("HANDED_OFF"),
                                       QStringLiteral("%1 reached Deck %2 and decode completed in %3 ms.").arg(pending.requestedTitle, deckLabel).arg(elapsedMs),
                                       pending.expectedPath);
                completedIds.push_back(pending.requestId);
                continue;
            }
            if (nowMs < pending.deadlineAtMs) {
                continue;
            }
            const QString detail = pending.pathSeen
                ? QStringLiteral("Deck %1 received the target path but decode readiness did not complete before timeout.").arg(deckLabel)
                : QStringLiteral("Deck %1 never reported the requested file path.").arg(deckLabel);
            finalizeRequestHandoff(pending.requestId,
                                   deckLabel,
                                   QStringLiteral("HANDOFF_FAILED"),
                                   detail,
                                   pending.expectedPath);
            completedIds.push_back(pending.requestId);
        }
        for (const QString& requestId : completedIds) {
            pendingHandoffVerifications_.erase(requestId);
        }
        if (pendingHandoffVerifications_.empty()) {
            handoffFallbackTimer_.stop();
        } else if (fromFallbackTimer && (nowMs - lastDeckSnapshotAtMs_) > 600) {
            if (!nowPlayingSyncDebounceTimer_.isActive()) {
                nowPlayingSyncDebounceTimer_.start();
            }
        }
    }

    void saveStreamingKeys()
    {
        const QString connName = QStringLiteral("ngks_skeys_w_%1")
            .arg(QUuid::createUuid().toString(QUuid::WithoutBraces));
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
        db.setDatabaseName(streamingKeysDbPath());
        if (!db.open()) {
            QSqlDatabase::removeDatabase(connName);
            return;
        }
        QSqlQuery q(db);
        q.exec(QStringLiteral(
            "CREATE TABLE IF NOT EXISTS streaming_api_keys "
            "(service TEXT PRIMARY KEY NOT NULL, api_key TEXT NOT NULL)"));
        for (const auto& kv : streamingApiKeys_) {
            if (!kv.second.isEmpty()) {
                q.prepare(QStringLiteral(
                    "INSERT OR REPLACE INTO streaming_api_keys(service, api_key) VALUES(?,?)"));
                q.addBindValue(kv.first);
                q.addBindValue(kv.second);
                q.exec();
            } else {
                q.prepare(QStringLiteral(
                    "DELETE FROM streaming_api_keys WHERE service = ?"));
                q.addBindValue(kv.first);
                q.exec();
            }
        }
        db.close();
        QSqlDatabase::removeDatabase(connName);
    }

    void loadStreamingKeys()
    {
        const QString connName = QStringLiteral("ngks_skeys_r_%1")
            .arg(QUuid::createUuid().toString(QUuid::WithoutBraces));
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
        db.setDatabaseName(streamingKeysDbPath());
        if (!db.open()) {
            QSqlDatabase::removeDatabase(connName);
            return;
        }
        QSqlQuery q(db);
        q.exec(QStringLiteral(
            "CREATE TABLE IF NOT EXISTS streaming_api_keys "
            "(service TEXT PRIMARY KEY NOT NULL, api_key TEXT NOT NULL)"));
        if (q.exec(QStringLiteral("SELECT service, api_key FROM streaming_api_keys"))) {
            while (q.next()) {
                const QString svc = q.value(0).toString();
                const QString key = q.value(1).toString();
                if (!svc.isEmpty() && !key.isEmpty()) {
                    streamingApiKeys_[svc] = key;
                    streamingConnected_[svc] = true;
                }
            }
        }
        db.close();
        QSqlDatabase::removeDatabase(connName);
    }

    QString streamingKeysDbPath() const
    {
        const QString exeDir = QCoreApplication::applicationDirPath();
        const QString root = QDir::cleanPath(QDir(exeDir).filePath(QStringLiteral("../../.."))); 
        return QDir(root).filePath(QStringLiteral("data/runtime/crowd_requests_local.db"));
    }

    void saveHardwareSettings()
    {
        const QString dir = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
        QDir().mkpath(dir);
        QJsonObject obj;
        obj[QLatin1String("auto_detect")] = hardwareAutoDetect_ ? hardwareAutoDetect_->isChecked() : true;
        obj[QLatin1String("led_sync")] = hardwareLedSync_ ? hardwareLedSync_->isChecked() : true;
        obj[QLatin1String("haptics")] = hardwareHaptics_ ? hardwareHaptics_->isChecked() : true;
        obj[QLatin1String("multi_controller")] = hardwareMultiController_ ? hardwareMultiController_->isChecked() : false;
        obj[QLatin1String("midi_learn")] = midiLearnEnabled_ ? midiLearnEnabled_->isChecked() : true;
        obj[QLatin1String("midi_automation")] = midiAutomationEnabled_ ? midiAutomationEnabled_->isChecked() : false;
        obj[QLatin1String("midi_led_feedback")] = midiLedFeedbackEnabled_ ? midiLedFeedbackEnabled_->isChecked() : true;
        obj[QLatin1String("midi_dual_deck")] = midiDualDeckEnabled_ ? midiDualDeckEnabled_->isChecked() : true;
        obj[QLatin1String("latency_mode")] = hardwareLatencyCombo_ ? hardwareLatencyCombo_->currentIndex() : 1;
        obj[QLatin1String("sample_rate")] = hardwareSampleRateCombo_ ? hardwareSampleRateCombo_->currentIndex() : 1;
        obj[QLatin1String("buffer_size")] = hardwareBufferSizeCombo_ ? hardwareBufferSizeCombo_->currentIndex() : 2;
        obj[QLatin1String("dvs_format")] = dvsFormatCombo_ ? dvsFormatCombo_->currentIndex() : 0;
        QFile f(dir + QLatin1String("/hardware_settings.json"));
        if (f.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
            f.write(QJsonDocument(obj).toJson());
        }
    }

    void loadHardwareSettings()
    {
        const QString path = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation)
                             + QLatin1String("/hardware_settings.json");
        QFile f(path);
        if (!f.open(QIODevice::ReadOnly)) { return; }
        const QJsonObject obj = QJsonDocument::fromJson(f.readAll()).object();
        if (hardwareAutoDetect_) {
            QSignalBlocker sb(hardwareAutoDetect_);
            hardwareAutoDetect_->setChecked(obj.value(QLatin1String("auto_detect")).toBool(true));
        }
        if (hardwareLedSync_) {
            QSignalBlocker sb(hardwareLedSync_);
            hardwareLedSync_->setChecked(obj.value(QLatin1String("led_sync")).toBool(true));
        }
        if (hardwareHaptics_) {
            QSignalBlocker sb(hardwareHaptics_);
            hardwareHaptics_->setChecked(obj.value(QLatin1String("haptics")).toBool(true));
        }
        if (hardwareMultiController_) {
            QSignalBlocker sb(hardwareMultiController_);
            hardwareMultiController_->setChecked(obj.value(QLatin1String("multi_controller")).toBool(false));
        }
        if (midiLearnEnabled_) {
            QSignalBlocker sb(midiLearnEnabled_);
            midiLearnEnabled_->setChecked(obj.value(QLatin1String("midi_learn")).toBool(true));
        }
        if (midiAutomationEnabled_) {
            QSignalBlocker sb(midiAutomationEnabled_);
            midiAutomationEnabled_->setChecked(obj.value(QLatin1String("midi_automation")).toBool(false));
        }
        if (midiLedFeedbackEnabled_) {
            QSignalBlocker sb(midiLedFeedbackEnabled_);
            midiLedFeedbackEnabled_->setChecked(obj.value(QLatin1String("midi_led_feedback")).toBool(true));
        }
        if (midiDualDeckEnabled_) {
            QSignalBlocker sb(midiDualDeckEnabled_);
            midiDualDeckEnabled_->setChecked(obj.value(QLatin1String("midi_dual_deck")).toBool(true));
        }
        if (hardwareLatencyCombo_) {
            QSignalBlocker sb(hardwareLatencyCombo_);
            hardwareLatencyCombo_->setCurrentIndex(obj.value(QLatin1String("latency_mode")).toInt(1));
        }
        if (hardwareSampleRateCombo_) {
            QSignalBlocker sb(hardwareSampleRateCombo_);
            hardwareSampleRateCombo_->setCurrentIndex(obj.value(QLatin1String("sample_rate")).toInt(1));
        }
        if (hardwareBufferSizeCombo_) {
            QSignalBlocker sb(hardwareBufferSizeCombo_);
            hardwareBufferSizeCombo_->setCurrentIndex(obj.value(QLatin1String("buffer_size")).toInt(2));
        }
        if (dvsFormatCombo_) {
            QSignalBlocker sb(dvsFormatCombo_);
            dvsFormatCombo_->setCurrentIndex(obj.value(QLatin1String("dvs_format")).toInt(0));
        }
    }

    void saveLiveStreamSettings()
    {
        const QString dir = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
        QDir().mkpath(dir);
        QJsonObject obj;
        obj[QLatin1String("software")] = selectedSoftware_;
        obj[QLatin1String("theme")] = selectedTheme_;
        if (broadcastResolutionCombo_) {
            obj[QLatin1String("resolution")] = broadcastResolutionCombo_->currentText();
        }
        QFile f(dir + QLatin1String("/livestream_settings.json"));
        if (f.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
            f.write(QJsonDocument(obj).toJson());
        }
    }

    void loadLiveStreamSettings()
    {
        const QString path = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation)
                             + QLatin1String("/livestream_settings.json");
        QFile f(path);
        if (!f.open(QIODevice::ReadOnly)) { return; }
        const QJsonObject obj = QJsonDocument::fromJson(f.readAll()).object();
        const QString sw = obj.value(QLatin1String("software")).toString();
        if (!sw.isEmpty()) { selectedSoftware_ = sw; }
        const QString th = obj.value(QLatin1String("theme")).toString();
        if (!th.isEmpty()) { selectedTheme_ = th; }
        const QString res = obj.value(QLatin1String("resolution")).toString();
        if (!res.isEmpty() && broadcastResolutionCombo_) {
            const int idx = broadcastResolutionCombo_->findText(res);
            if (idx >= 0) {
                QSignalBlocker sb(broadcastResolutionCombo_);
                broadcastResolutionCombo_->setCurrentIndex(idx);
            }
        }
        refreshBroadcastGuide();
    }

    QBrush statusBrush(const QString& status) const
    {
        if (status == QStringLiteral("NOW_PLAYING")) {
            return QBrush(QColor(QStringLiteral("#fff7d6")));
        }
        if (status == QStringLiteral("PLAYED")) {
            return QBrush(QColor(QStringLiteral("#d9c4aa")));
        }
        if (status == QStringLiteral("HANDED_OFF")) {
            return QBrush(QColor(QStringLiteral("#cceeff")));
        }
        if (status == QStringLiteral("HANDOFF_FAILED")) {
            return QBrush(QColor(QStringLiteral("#ffd1bf")));
        }
        if (status == QStringLiteral("ACCEPTED")) {
            return QBrush(QColor(QStringLiteral("#fff1bf")));
        }
        if (status == QStringLiteral("REJECTED")) {
            return QBrush(QColor(QStringLiteral("#f4d7ca")));
        }
        return QBrush(QColor(QStringLiteral("#ffe6b8")));
    }

    QBrush statusBackgroundBrush(const QString& status) const
    {
        if (status == QStringLiteral("NOW_PLAYING")) {
            return QBrush(QColor(QStringLiteral("#b64719")));
        }
        if (status == QStringLiteral("ACCEPTED")) {
            return QBrush(QColor(QStringLiteral("#7b4318")));
        }
        if (status == QStringLiteral("HANDED_OFF")) {
            return QBrush(QColor(QStringLiteral("#4b2a18")));
        }
        if (status == QStringLiteral("HANDOFF_FAILED")) {
            return QBrush(QColor(QStringLiteral("#5a2219")));
        }
        if (status == QStringLiteral("PLAYED")) {
            return QBrush(QColor(QStringLiteral("#3c2118")));
        }
        if (status == QStringLiteral("REJECTED")) {
            return QBrush(QColor(QStringLiteral("#2d1814")));
        }
        return QBrush(QColor(QStringLiteral("#5a3018")));
    }

    void refreshBroadcastStatus()
    {
        if (broadcastStatusLabel_) {
            broadcastStatusLabel_->setText(broadcastWindow_ ? QStringLiteral("Broadcast window is open") : QStringLiteral("Broadcast window is closed"));
        }
        for (auto& entry : softwareButtons_) {
            entry.second->setChecked(entry.first == selectedSoftware_);
        }
        for (auto& entry : themeButtons_) {
            entry.second->setChecked(entry.first == selectedTheme_);
        }
    }

    QString broadcastGuideText() const
    {
        if (selectedSoftware_ == QStringLiteral("obs")) {
            return QStringLiteral(
                "OBS Studio -- First-time setup\n"
                "--------------------------------\n"
                "1. Launch OBS from this page (or from your desktop).\n"
                "2. Click the + button under Sources -> choose Window Capture.\n"
                "3. Name it 'NGKs Broadcast' and click OK.\n"
                "4. In the Window dropdown, select [NGKsPlayerNative.exe]: NGKs Player.\n"
                "5. Tick 'Capture Cursor' OFF (cleaner output).\n"
                "6. Resize the source to fill your canvas: right-click -> Fit to screen.\n"
                "7. (Optional) Add a Video Capture Device source for your camera on top.\n"
                "8. Go to Settings -> Stream, paste your Twitch/YouTube stream key.\n"
                "9. Settings -> Output -> set Bitrate to 4500-6000 Kbps for 1080p.\n"
                "10. Settings -> Audio -> set Sample Rate 48 kHz, match your DJ mixer.\n"
                "11. Click Start Recording once to verify the output looks correct.\n"
                "12. Save the scene -- you reuse it every show, no reconfiguring needed.\n\n"
                "Every stream after setup\n"
                "------------------------\n"
                "1. Open the Broadcast Window (button above).\n"
                "2. In OBS, click Start Streaming.\n"
                "3. To end: Stop Streaming in OBS, then close the broadcast window.\n\n"
                "Audio routing tip\n"
                "-----------------\n"
                "Use VB-Cable or Voicemeeter as a virtual audio cable so OBS captures\n"
                "your DJ mix without also capturing system sounds. Set NGKs Player's\n"
                "audio output device to the virtual cable, then add that cable as a\n"
                "Desktop Audio source in OBS."
            );
        }
        if (selectedSoftware_ == QStringLiteral("streamlabs")) {
            return QStringLiteral(
                "Streamlabs -- First-time setup\n"
                "--------------------------------\n"
                "1. Launch Streamlabs from this page.\n"
                "2. In the Editor, click the + under Sources -> Window Capture.\n"
                "3. Select [NGKsPlayerNative.exe]: NGKs Player from the window list.\n"
                "4. Right-click the source -> Transform -> Fit to Screen.\n"
                "5. (Optional) Add a Webcam source above the capture layer.\n"
                "6. Go to Settings -> Stream -> connect your Twitch or YouTube account\n"
                "   using the Login button -- no manual stream key needed.\n"
                "7. Settings -> Output -> Video Bitrate: 4500-6000 Kbps for 1080p.\n"
                "8. Settings -> Audio -> set your DJ mixer as the Audio Input Device.\n"
                "9. Click Go Live to test before your first real show.\n"
                "10. Save your Scene Collection (Manage -> Export) as a backup.\n\n"
                "Every stream after setup\n"
                "------------------------\n"
                "1. Open the Broadcast Window.\n"
                "2. Click Go Live in Streamlabs, fill in the title/game.\n"
                "3. To end: End Stream in Streamlabs, then close the broadcast window.\n\n"
                "Alerts & widgets tip\n"
                "--------------------\n"
                "Streamlabs widgets (Alert Box, Tip Jar, etc.) sit as Browser Sources\n"
                "above your NGKs capture layer. Keep them docked to the bottom so\n"
                "they don't cover the track metadata display."
            );
        }
        if (selectedSoftware_ == QStringLiteral("xsplit")) {
            return QStringLiteral(
                "XSplit Broadcaster -- First-time setup\n"
                "---------------------------------------\n"
                "1. Launch XSplit from this page.\n"
                "2. Click Add Source -> Screen Capture -> Window.\n"
                "3. Pick NGKsPlayerNative from the application list.\n"
                "4. Drag the source to fill the scene canvas.\n"
                "5. (Optional) Add a webcam via Add Source -> Devices -> Camera.\n"
                "6. Go to Broadcast -> Set up a new output, choose Twitch or YouTube.\n"
                "7. Sign in with your streaming account (OAuth -- no manual key needed).\n"
                "8. In Output settings set Quality to High, Bitrate 4500-6000 Kbps.\n"
                "9. Tools -> Settings -> Audio -> select your DJ mixer as the input.\n"
                "10. Test with Record Locally first to verify the capture looks clean.\n"
                "11. Save your scene (File -> Save Presentation) for reuse.\n\n"
                "Every stream after setup\n"
                "------------------------\n"
                "1. Open the Broadcast Window.\n"
                "2. Press the Broadcast button next to your configured output.\n"
                "3. To end: Stop the broadcast in XSplit, close the broadcast window.\n\n"
                "Resolution tip\n"
                "--------------\n"
                "Match the XSplit scene resolution to the resolution selected above\n"
                "(e.g. 1920x1080). XSplit will downscale cleanly if your output\n"
                "is set to 720p for lower-bandwidth venues."
            );
        }
        if (selectedSoftware_ == QStringLiteral("vmix")) {
            return QStringLiteral(
                "vMix -- First-time setup\n"
                "------------------------\n"
                "1. Launch vMix from this page.\n"
                "2. Click Add Input -> Desktop Capture.\n"
                "3. Choose Window and select NGKsPlayerNative from the list.\n"
                "4. The input appears as a tile -- double-click to preview full screen.\n"
                "5. (Optional) Add a camera input: Add Input -> Camera, select webcam.\n"
                "6. In the streaming bar at the bottom, click the cog next to Stream.\n"
                "7. Set Destination to Twitch or YouTube, paste your stream key.\n"
                "8. Set Quality: H.264, Bitrate 4500-6000 Kbps, Keyframe 2s.\n"
                "9. Audio -> Mixer: route your DJ mixer to Bus A (master output).\n"
                "10. Click the red REC button to do a local test recording first.\n"
                "11. File -> Save Preset to reuse this configuration every show.\n\n"
                "Every stream after setup\n"
                "------------------------\n"
                "1. Open the Broadcast Window.\n"
                "2. Click Stream in vMix to go live.\n"
                "3. To end: click Stream again to stop, close the broadcast window.\n\n"
                "Multi-camera tip\n"
                "----------------\n"
                "vMix supports multiple camera inputs on the free tier (up to 2).\n"
                "Use the MultiView to monitor your NGKs capture and camera side by side,\n"
                "then cut between them live using the T-bar or keyboard shortcuts."
            );
        }
        // "other"
        return QStringLiteral(
            "Other software -- General setup\n"
            "--------------------------------\n"
            "Any software that supports window or screen capture works.\n\n"
            "1. Open the Broadcast Window using the button above.\n"
            "2. In your streaming software, add a Window Capture source.\n"
            "3. Select NGKsPlayerNative as the target window.\n"
            "4. Size the capture to fill your output canvas.\n"
            "5. Set your stream destination (Twitch/YouTube/etc.) and paste your\n"
            "   stream key from your platform dashboard.\n"
            "6. Set video bitrate: 4500-6000 Kbps for 1080p, 2500-3500 for 720p.\n"
            "7. Set audio: 48 kHz sample rate, 160-320 Kbps bitrate.\n"
            "8. Route your DJ mixer audio through a virtual cable (VB-Cable or\n"
            "   Voicemeeter) so only the mix is captured, not the whole desktop.\n"
            "9. Do a test recording locally before going live.\n\n"
            "Every stream after setup\n"
            "------------------------\n"
            "1. Open the Broadcast Window.\n"
            "2. Start your stream in the software.\n"
            "3. To end: stop the stream, then close the broadcast window."
        );
    }

    void refreshBroadcastGuide()
    {
        if (broadcastGuide_) {
            broadcastGuide_->setPlainText(broadcastGuideText());
        }
        refreshBroadcastStatus();
    }

    void refreshBroadcastPreview()
    {
        if (!broadcastWindow_) {
            return;
        }

        QString bg = QStringLiteral("background:qlineargradient(x1:0,y1:0,x2:1,y2:1,stop:0 #0f172a, stop:1 #1d4ed8);");
        if (selectedTheme_ == QStringLiteral("minimal")) {
            bg = QStringLiteral("background:rgba(3,7,18,230);");
        } else if (selectedTheme_ == QStringLiteral("bar")) {
            bg = QStringLiteral("background:qlineargradient(x1:0,y1:0,x2:1,y2:0,stop:0 #111827, stop:1 #0f766e);");
        } else if (selectedTheme_ == QStringLiteral("vinyl")) {
            bg = QStringLiteral("background:qradialgradient(cx:0.5, cy:0.5, radius:0.7, stop:0 #1f2937, stop:0.4 #0f172a, stop:1 #030712);");
        }
        broadcastWindow_->setStyleSheet(QStringLiteral("QDialog { %1 color:#f8fafc; border:1px solid #22304a; }").arg(bg));
        const QVariantMap nowPlaying = nowPlayingProvider_ ? nowPlayingProvider_() : QVariantMap{};
        broadcastTrackLabel_->setText(nowPlaying.value(QStringLiteral("title")).toString().trimmed().isEmpty()
            ? QStringLiteral("No track loaded") : nowPlaying.value(QStringLiteral("title")).toString());
        broadcastArtistLabel_->setText(nowPlaying.value(QStringLiteral("artist")).toString().trimmed());
        broadcastMetaLabel_->setText(QStringLiteral("Theme: %1  |  Software: %2  |  Resolution: %3")
            .arg(selectedTheme_, selectedSoftware_, broadcastResolutionCombo_ ? broadcastResolutionCombo_->currentText() : QStringLiteral("1920x1080")));
    }

    void openBroadcastWindow()
    {
        if (!broadcastWindow_) {
            broadcastWindow_ = new QDialog(this, Qt::Window);
            broadcastWindow_->setAttribute(Qt::WA_DeleteOnClose, false);
            broadcastWindow_->setWindowTitle(QStringLiteral("NGKs Player - Broadcast Output"));
            auto* layout = new QVBoxLayout(broadcastWindow_);
            layout->setContentsMargins(30, 30, 30, 30);
            layout->setSpacing(10);
            auto* header = new QLabel(QStringLiteral("NGKs Player Broadcast Output"), broadcastWindow_);
            header->setStyleSheet(QStringLiteral("color:#2dd4bf; font-size:14px; font-weight:800; letter-spacing:1px;"));
            broadcastTrackLabel_ = new QLabel(QStringLiteral("No track loaded"), broadcastWindow_);
            broadcastTrackLabel_->setStyleSheet(QStringLiteral("color:#f8fafc; font-size:34px; font-weight:800;"));
            broadcastTrackLabel_->setWordWrap(true);
            broadcastArtistLabel_ = new QLabel(QString(), broadcastWindow_);
            broadcastArtistLabel_->setStyleSheet(QStringLiteral("color:#cbd5e1; font-size:20px; font-weight:600;"));
            broadcastMetaLabel_ = new QLabel(QString(), broadcastWindow_);
            broadcastMetaLabel_->setStyleSheet(QStringLiteral("color:#94a3b8; font-size:12px;"));
            layout->addWidget(header);
            layout->addStretch(1);
            layout->addWidget(broadcastTrackLabel_);
            layout->addWidget(broadcastArtistLabel_);
            layout->addWidget(broadcastMetaLabel_);
            layout->addStretch(2);

            QObject::connect(&broadcastRefreshTimer_, &QTimer::timeout, this, [this]() { refreshBroadcastPreview(); });
            broadcastRefreshTimer_.start(500);
        }

        const QString resolution = broadcastResolutionCombo_ ? broadcastResolutionCombo_->currentText() : QStringLiteral("1920x1080");
        const QStringList parts = resolution.split('x');
        if (parts.size() == 2) {
            broadcastWindow_->resize(parts[0].toInt(), parts[1].toInt());
        }
        refreshBroadcastPreview();
        broadcastWindow_->show();
        broadcastWindow_->raise();
        broadcastWindow_->activateWindow();
        refreshBroadcastStatus();
    }

    void closeBroadcastWindow()
    {
        if (broadcastWindow_) {
            broadcastWindow_->close();
            broadcastWindow_->hide();
        }
        refreshBroadcastStatus();
    }

    EngineBridge* bridge_{nullptr};
    SearchProvider searchProvider_;
    TrackLoader trackLoader_;
    NowPlayingProvider nowPlayingProvider_;
    QButtonGroup* navGroup_{nullptr};
    QStackedWidget* contentStack_{nullptr};

    QLabel* hardwareDeviceCountLabel_{nullptr};
    QLabel* hardwareActiveDeviceLabel_{nullptr};
    QLabel* hardwareSwitchStatusLabel_{nullptr};
    QLabel* hardwareMidiStatusLabel_{nullptr};
    QComboBox* hardwareDeviceCombo_{nullptr};
    QListWidget* hardwareDeviceList_{nullptr};
    // Controller stats labels
    QLabel* hwCtrlConnectedLabel_{nullptr};
    QLabel* hwCtrlMsgLabel_{nullptr};
    QLabel* hwCtrlMappingsLabel_{nullptr};
    QLabel* hwCtrlLatencyLabel_{nullptr};
    // DVS tab
    QComboBox* dvsFormatCombo_{nullptr};
    QLabel* dvsDeckAStatus_{nullptr};
    QLabel* dvsDeckBStatus_{nullptr};
    QLabel* dvsDeckAPos_{nullptr};
    QLabel* dvsDeckBPos_{nullptr};
    QLabel* dvsDeckASpeed_{nullptr};
    QLabel* dvsDeckBSpeed_{nullptr};
    QProgressBar* dvsDeckASignal_{nullptr};
    QProgressBar* dvsDeckBSignal_{nullptr};
    QLabel* dvsStatLatencyLabel_{nullptr};
    QLabel* dvsStatDropoutsLabel_{nullptr};
    QLabel* dvsStatSampleRateLabel_{nullptr};
    QLabel* dvsStatBufferLabel_{nullptr};
    // MIDI tab
    QListWidget* hardwareMidiDeviceList_{nullptr};
    QCheckBox* midiLearnEnabled_{nullptr};
    QCheckBox* midiAutomationEnabled_{nullptr};
    QCheckBox* midiLedFeedbackEnabled_{nullptr};
    QCheckBox* midiDualDeckEnabled_{nullptr};
    // Settings tab - audio engine
    QComboBox* hardwareLatencyCombo_{nullptr};
    QComboBox* hardwareSampleRateCombo_{nullptr};
    QComboBox* hardwareBufferSizeCombo_{nullptr};

    std::map<QString, QLabel*> streamingStatusLabels_;
    std::map<QString, QPushButton*> streamingToggleButtons_;
    std::map<QString, bool> streamingConnected_;
    std::map<QString, QString> streamingApiKeys_;
    std::map<QString, QString> streamingDisplayNames_;
    std::map<QString, std::pair<QString,QString>> streamingApiKeyUrls_;
    QLineEdit* streamingSearchEdit_{nullptr};
    QComboBox* streamingServiceFilter_{nullptr};
    QTreeWidget* streamingResultsTree_{nullptr};

    QLabel* requestServerStateLabel_{nullptr};
    QLabel* requestUrlLabel_{nullptr};
    QLabel* requestUrlDisplayLabel_{nullptr};
    QPushButton* requestStartButton_{nullptr};
    QPushButton* requestStopButton_{nullptr};
    QPushButton* requestCopyButton_{nullptr};
    QPushButton* requestOpenGuestButton_{nullptr};
    QPushButton* requestQrButton_{nullptr};
    QPushButton* requestClearButton_{nullptr};
    QPushButton* requestSaveSettingsButton_{nullptr};
    QLabel* requestSaveSettingsStatusLabel_{nullptr};
    QLabel* requestPendingLabel_{nullptr};
    QLabel* requestCountLabel_{nullptr};
    QLabel* requestAcceptedLabel_{nullptr};
    QLabel* requestHandedOffLabel_{nullptr};
    QLabel* requestNowPlayingLabel_{nullptr};
    QLabel* requestPlayedLabel_{nullptr};
    QLabel* requestFailedLabel_{nullptr};
    QComboBox* requestPolicyCombo_{nullptr};
    QTreeWidget* requestQueueTree_{nullptr};
    QLineEdit* requestVenmoEdit_{nullptr};
    QLineEdit* requestCashAppEdit_{nullptr};
    QLineEdit* requestPaypalEdit_{nullptr};
    QLineEdit* requestZelleEdit_{nullptr};
    QLineEdit* requestBuyMeACoffeeEdit_{nullptr};
    QLineEdit* requestChimeEdit_{nullptr};
    QLineEdit* requestCardUrlEdit_{nullptr};
    QLabel* requestHandoffLabel_{nullptr};
    QList<QVariantMap> requestItems_;
    bool requestServerRunning_{false};
    QString joinUrl_;
    CrowdRequestBackend* crowdBackend_{nullptr};
    QTimer requestPollTimer_;
    QTimer requestRefreshDebounceTimer_;
    QTimer requestSettingsAutoSaveTimer_;
    QTimer nowPlayingSyncDebounceTimer_;
    QTimer handoffFallbackTimer_;
    std::map<QString, PendingHandoffVerification> pendingHandoffVerifications_;
    QString lastNowPlayingEventFingerprint_;
    QString lastNowPlayingPeakFingerprint_;
    qint64 lastNowPlayingSyncMs_{0};
    qint64 lastDeckSnapshotAtMs_{0};

    QCheckBox* hardwareAutoDetect_{nullptr};
    QCheckBox* hardwareLedSync_{nullptr};
    QCheckBox* hardwareHaptics_{nullptr};
    QCheckBox* hardwareMultiController_{nullptr};

    std::map<QString, QPushButton*> softwareButtons_;
    std::map<QString, QPushButton*> themeButtons_;
    QString selectedSoftware_{QStringLiteral("obs")};
    QString selectedTheme_{QStringLiteral("default")};
    QLabel* broadcastStatusLabel_{nullptr};
    QComboBox* broadcastResolutionCombo_{nullptr};
    QPlainTextEdit* broadcastGuide_{nullptr};
    QDialog* broadcastWindow_{nullptr};
    QLabel* broadcastTrackLabel_{nullptr};
    QLabel* broadcastArtistLabel_{nullptr};
    QLabel* broadcastMetaLabel_{nullptr};
    QTimer broadcastRefreshTimer_;
};