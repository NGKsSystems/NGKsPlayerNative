#include <QAction>
#include <QApplication>
#include <QComboBox>
#include <QDialog>
#include <QHBoxLayout>
#include <QGridLayout>
#include <QStackedLayout>
#include <QLabel>
#include <QMainWindow>
#include <QMenu>
#include <QMenuBar>
#include <QMessageLogContext>
#include <QMessageBox>
#include <QMutex>
#include <QMutexLocker>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QScrollArea>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QGuiApplication>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QSaveFile>
#include <QShortcut>
#include <QSignalBlocker>
#include <QStringList>
#include <QSysInfo>
#include <QTimer>
#include <QThread>
#include <QVBoxLayout>
#include <QDateTime>
#include <QWidget>
#include <QClipboard>
#include <QDesktopServices>
#include <QInputDialog>
#include <QUrl>
#include <QLineEdit>
#include <QListWidget>
#include <QHeaderView>
#include <QSplitter>
#include <QSlider>
#include <QStackedWidget>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QElapsedTimer>
#include <QFileDialog>
#include <QDirIterator>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <csignal>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <atomic>
#include <functional>
#include <map>
#include <random>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

#include "ui/EngineBridge.h"
#include "ui/EqPanel.h"
#include "ui/AncillaryScreensWidget.h"
#include "ui/DeckStrip.h"
#include "engine/DiagLog.h"

#include "ui/diagnostics/RuntimeLogSupport.h"
#include "ui/library/LibraryPersistence.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryBrowserWidget.h"
#include "ui/library/DjLibraryWidget.h"
#include "ui/library/DjBrowserPane.h"
#include "ui/library/LibraryScanner.h"
#include "ui/library/LegacyLibraryImport.h"
#include "ui/audio/AudioProfileStore.h"
#include "ui/diagnostics/DiagnosticsDialog.h"
#include "ui/widgets/VisualizerWidget.h"
#include "ui/TrackDetailPanel.h"
#include "ui/PlayerPage.h"
#include "ui/DjModePage.h"
#include "ui/DjIntroOverlay.h"

#ifndef NGKS_BUILD_STAMP
#define NGKS_BUILD_STAMP "unknown"
#endif

#ifndef NGKS_GIT_SHA
#define NGKS_GIT_SHA "unknown"
#endif

namespace {

class MainWindow : public QMainWindow {
public:
    explicit MainWindow(EngineBridge& engineBridge)
        : bridge_(engineBridge)
    {
        setWindowTitle(QStringLiteral("NGKsPlayerNative"));
        resize(640, 480);

        auto* root = new QWidget(this);
        auto* rootLayout = new QVBoxLayout(root);
        rootLayout->setContentsMargins(0, 0, 0, 0);
        rootLayout->setSpacing(0);

        // ── Stacked pages ──
        stack_ = new QStackedWidget(root);
        stack_->addWidget(buildSplashPage());    // 0
        stack_->addWidget(buildLandingPage());   // 1
        playerPage_ = new PlayerPage(bridge_, djDb_);
        stack_->addWidget(playerPage_);          // 2
        djModePage_ = new DjModePage(bridge_, djDb_);
        djModePage_->setTrackList(&allTracks_);
        stack_->addWidget(djModePage_);          // 3
        connect(djModePage_, &DjModePage::backRequested, this, [this]() {
            stack_->setCurrentIndex(1);
        });
        djIntroOverlay_ = new DjIntroOverlay();
        stack_->addWidget(djIntroOverlay_);      // 4
        connect(djIntroOverlay_, &DjIntroOverlay::finished, this, [this]() {
            stack_->setCurrentIndex(3);
        });
        stack_->setCurrentIndex(0);
        rootLayout->addWidget(stack_, 1);

        // ── Persistent status strip ──
        auto* statusStrip = new QWidget(root);
        statusStrip->setStyleSheet(QStringLiteral("background:#222; color:#ccc; font-size:11px;"));
        auto* stripLayout = new QHBoxLayout(statusStrip);
        stripLayout->setContentsMargins(8, 4, 8, 4);
        engineStatusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), statusStrip);
        runningLabel_ = new QLabel(QStringLiteral("Running: NO"), statusStrip);
        meterLabel_ = new QLabel(QStringLiteral("MeterL: 0.000  MeterR: 0.000"), statusStrip);
        stripLayout->addWidget(engineStatusLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(runningLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(meterLabel_);
        stripLayout->addStretch(1);
        rootLayout->addWidget(statusStrip);

        setCentralWidget(root);

        // ── Menu bar ──
        auto* diagnosticsAction = menuBar()->addAction(QStringLiteral("Diagnostics"));
        QObject::connect(diagnosticsAction, &QAction::triggered, this, &MainWindow::showDiagnostics);
        auto* shortcut = new QShortcut(QKeySequence(QStringLiteral("Ctrl+D")), this);
        QObject::connect(shortcut, &QShortcut::activated, this, &MainWindow::showDiagnostics);

        // Wire PlayerPage navigation signals
        QObject::connect(playerPage_, &PlayerPage::backRequested, this, [this]() {
            stack_->setCurrentIndex(1);
        });
        QObject::connect(playerPage_, &PlayerPage::diagnosticsRefreshRequested, this, [this]() {
            if (diagnosticsDialog_) diagnosticsDialog_->refreshLogTail();
        });

        // ── Poll timer ──
        pollTimer_.setInterval(250);
        QObject::connect(&pollTimer_, &QTimer::timeout, this, &MainWindow::pollStatus);
        pollTimer_.start();

        // ── Autorun flags ──
        const QString autorun = qEnvironmentVariable("NGKS_SELFTEST_AUTORUN").trimmed().toLower();
        selfTestAutorun_ = (autorun == QStringLiteral("1") || autorun == QStringLiteral("true") || autorun == QStringLiteral("yes"));
        const QString rtAutorun = qEnvironmentVariable("NGKS_RT_AUDIO_AUTORUN").trimmed().toLower();
        rtProbeAutorun_ = (rtAutorun == QStringLiteral("1") || rtAutorun == QStringLiteral("true") || rtAutorun == QStringLiteral("yes"));

        // ── Open SQLite library database ──
        djDb_.open(runtimePath("data/runtime/ngks_library.db"));

        // ── Restore persisted library ──
        {
            QString restoredFolder;
            std::vector<TrackInfo> restoredTracks;
            if (loadLibraryJson(restoredTracks, restoredFolder)) {
                allTracks_ = std::move(restoredTracks);
                importedFolderPath_ = restoredFolder;

                // Auto-merge legacy DB on restore if not already imported
                bool anyLegacy = false;
                for (const auto& t : allTracks_) {
                    if (t.legacyImported) { anyLegacy = true; break; }
                }
                if (!anyLegacy) {
                    const QString dbPath = findLegacyDbPath();
                    if (!dbPath.isEmpty()) {
                        const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
                        if (res.matched > 0) {
                            saveLibraryJson(allTracks_, importedFolderPath_);
                            qInfo().noquote() << QStringLiteral("LEGACY_DB_AUTO_IMPORT matched=%1 total=%2")
                                .arg(res.matched).arg(res.totalDbRows);
                        }
                    }
                }

                applyCoreDurationPatch(allTracks_);
djDb_.bulkInsert(allTracks_);
                refreshLibraryList();
                playerPage_->setTrackList(&allTracks_);
                playerPage_->refreshLibrary();
                qInfo().noquote() << QStringLiteral("LIBRARY_RESTORED=%1").arg(allTracks_.size());
            }
        }

        // ── Restore persisted playlists ──
        loadPlaylists(playlists_);
        qInfo().noquote() << QStringLiteral("PLAYLISTS_RESTORED=%1").arg(playlists_.size());

        // ── Splash auto-transition (2 s) ──
        QTimer::singleShot(2000, this, [this]() { stack_->setCurrentIndex(1); });


        // ── Refresh player library every time the player page becomes visible ──
        QObject::connect(stack_, &QStackedWidget::currentChanged, this, [this](int index) {
            if (index == 2) {
                playerPage_->refreshLibrary();
            }
        });

        qInfo() << "MainWindowConstructed=PASS";

        if (selfTestAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::runFoundationSelfTests);
        }
        if (rtProbeAutorun_) {
            QTimer::singleShot(0, this, &MainWindow::startRtProbeAutorun);
        }
    }

private:
    // ── Page builders ──
    QWidget* buildSplashPage()
    {
        auto* page = new QWidget();
        auto* layout = new QVBoxLayout(page);
        layout->addStretch(2);

        auto* title = new QLabel(QStringLiteral("NGKsPlayerNative"), page);
        {
            QFont f = title->font();
            f.setPointSize(28);
            f.setBold(true);
            title->setFont(f);
        }
        title->setAlignment(Qt::AlignCenter);
        layout->addWidget(title);

        auto* subtitle = new QLabel(QStringLiteral("Audio Engine Platform"), page);
        subtitle->setAlignment(Qt::AlignCenter);
        {
            QFont f = subtitle->font();
            f.setPointSize(12);
            subtitle->setFont(f);
        }
        layout->addWidget(subtitle);

        layout->addStretch(3);
        return page;
    }

    QWidget* buildLandingPage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral(
            "QWidget { background: #1a1a2e; color: #e0e0e0; }"
            "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 6px 14px; font-size: 12px; }"
            "QPushButton:hover { background: #0f3460; }"
            "QPushButton:pressed { background: #533483; }"
            "QLineEdit { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 6px; padding: 6px 10px; font-size: 12px; }"
            "QTreeWidget { background: #16213e; alternate-background-color: #1a1a3e;"
            "  color: #e0e0e0; border: 1px solid #0f3460; border-radius: 6px;"
            "  font-size: 12px; outline: none; }"
            "QTreeWidget::item { padding: 4px 6px; border-bottom: 1px solid #0f3460; }"
            "QTreeWidget::item:selected { background: #533483; color: #fff; }"
            "QTreeWidget::item:hover { background: #1f2b4d; }"
            "QHeaderView::section { background: #0f3460; color: #ccc; padding: 5px 8px;"
            "  border: none; border-right: 1px solid #1a1a2e; font-weight: bold; font-size: 11px; }"
            "QLabel#detailTitle { font-size: 15px; font-weight: bold; color: #e94560; }"
            "QLabel#detailField { font-size: 11px; color: #aaa; }"
            "QLabel#detailValue { font-size: 12px; color: #e0e0e0; }"
        ));

        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(16, 12, 16, 8);
        layout->setSpacing(8);

        // ── Header ──
        auto* title = new QLabel(QStringLiteral("Library"), page);
        {
            QFont f = title->font();
            f.setPointSize(16);
            f.setBold(true);
            title->setFont(f);
        }
        title->setStyleSheet(QStringLiteral("color: #e94560; margin-bottom: 2px;"));
        layout->addWidget(title);

        // ── Action row 1: library actions + nav ──
        auto* actionRow = new QHBoxLayout();
        actionRow->setSpacing(6);

        auto* importBtn = new QPushButton(QStringLiteral("Add Folder"), page);
        importBtn->setMinimumHeight(32);
        importBtn->setCursor(Qt::PointingHandCursor);
        importBtn->setToolTip(QStringLiteral("Import a folder of audio files into the library"));
        actionRow->addWidget(importBtn);

        auto* legacyDbBtn = new QPushButton(QStringLiteral("Import Legacy DB"), page);
        legacyDbBtn->setMinimumHeight(32);
        legacyDbBtn->setCursor(Qt::PointingHandCursor);
        legacyDbBtn->setToolTip(QStringLiteral("Merge BPM, key, LUFS and analysis fields from the legacy ngksplayer database"));
        actionRow->addWidget(legacyDbBtn);

        auto* playAllBtn = new QPushButton(QStringLiteral("Play All"), page);
        playAllBtn->setMinimumHeight(32);
        playAllBtn->setCursor(Qt::PointingHandCursor);
        playAllBtn->setToolTip(QStringLiteral("Play all visible tracks sequentially"));
        actionRow->addWidget(playAllBtn);

        auto* nowPlayingBtn = new QPushButton(QStringLiteral("Now Playing"), page);
        nowPlayingBtn->setMinimumHeight(32);
        nowPlayingBtn->setCursor(Qt::PointingHandCursor);
        nowPlayingBtn->setToolTip(QStringLiteral("Jump to the currently playing track"));
        actionRow->addWidget(nowPlayingBtn);

        auto* playlistsBtn = new QPushButton(QStringLiteral("Playlists"), page);
        playlistsBtn->setMinimumHeight(32);
        playlistsBtn->setCursor(Qt::PointingHandCursor);
        playlistsBtn->setToolTip(QStringLiteral("Browse, create, and filter by playlists"));
        actionRow->addWidget(playlistsBtn);

        actionRow->addStretch(1);

        auto* playerBtn = new QPushButton(QStringLiteral("Player"), page);
        auto* djBtn = new QPushButton(QStringLiteral("DJ Mode"), page);
        playerBtn->setMinimumHeight(32);
        djBtn->setMinimumHeight(32);
        playerBtn->setCursor(Qt::PointingHandCursor);
        djBtn->setCursor(Qt::PointingHandCursor);
        actionRow->addWidget(playerBtn);
        actionRow->addWidget(djBtn);
        layout->addLayout(actionRow);

        // ── Action row 2: tools (some wired, some coming-soon) ──
        auto* toolRow = new QHBoxLayout();
        toolRow->setSpacing(6);

        auto* tagEditorBtn = new QPushButton(QStringLiteral("Tag Editor"), page);
        tagEditorBtn->setMinimumHeight(30);
        tagEditorBtn->setEnabled(false);
        tagEditorBtn->setToolTip(QStringLiteral("Edit track tags (coming soon)"));
        toolRow->addWidget(tagEditorBtn);

        auto* settingsBtn = new QPushButton(QStringLiteral("Settings"), page);
        settingsBtn->setMinimumHeight(30);
        settingsBtn->setEnabled(false);
        settingsBtn->setToolTip(QStringLiteral("Application settings (coming soon)"));
        toolRow->addWidget(settingsBtn);

        auto* normalizeBtn = new QPushButton(QStringLiteral("Normalize"), page);
        normalizeBtn->setMinimumHeight(30);
        normalizeBtn->setEnabled(false);
        normalizeBtn->setToolTip(QStringLiteral("Normalize loudness across tracks (coming soon)"));
        toolRow->addWidget(normalizeBtn);

        auto* layerRemoverBtn = new QPushButton(QStringLiteral("Layer Remover"), page);
        layerRemoverBtn->setMinimumHeight(30);
        layerRemoverBtn->setEnabled(false);
        layerRemoverBtn->setToolTip(QStringLiteral("AI stem separation (coming soon)"));
        toolRow->addWidget(layerRemoverBtn);

        auto* clipperBtn = new QPushButton(QStringLiteral("Clipper V3"), page);
        clipperBtn->setMinimumHeight(30);
        clipperBtn->setEnabled(false);
        clipperBtn->setToolTip(QStringLiteral("Soft-clip mastering (coming soon)"));
        toolRow->addWidget(clipperBtn);

        toolRow->addStretch(1);
        layout->addLayout(toolRow);

        // ── Main content: splitter (library browser | detail panel) ──
        auto* splitter = new QSplitter(Qt::Horizontal, page);
        splitter->setHandleWidth(2);
        splitter->setStyleSheet(QStringLiteral("QSplitter::handle { background: #0f3460; }"));

        libraryTree_ = new LibraryBrowserWidget(LibraryBrowserWidget::Mode::MainPanel, splitter);
        libraryTree_->setDatabase(&djDb_);

        // ── Detail panel (scrollable) ──
        auto* detailScroll = new QScrollArea(splitter);
        detailScroll->setWidgetResizable(true);
        detailScroll->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
        detailScroll->setMinimumWidth(200);
        detailScroll->setMaximumWidth(280);
        detailScroll->setStyleSheet(QStringLiteral(
            "QScrollArea { background: #16213e; border-left: 1px solid #0f3460;"
            "  border-radius: 6px; }"
            "QScrollBar:vertical { background: #16213e; width: 6px; }"
            "QScrollBar::handle:vertical { background: #533483; border-radius: 3px; }"
            "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }"
        ));
        detailPanel_ = new TrackDetailPanel();
        detailScroll->setWidget(detailPanel_);
        splitter->addWidget(libraryTree_);
        splitter->addWidget(detailScroll);
        splitter->setSizes({500, 240});
        layout->addWidget(splitter, 1);

        // ── Bottom bar: track count ──
        auto* bottomRow = new QHBoxLayout();
        bottomRow->setSpacing(8);
        trackCountLabel_ = new QLabel(QStringLiteral("0 tracks"), page);
        trackCountLabel_->setStyleSheet(QStringLiteral("color: #888; font-size: 11px;"));
        bottomRow->addWidget(trackCountLabel_);
        bottomRow->addStretch(1);
        layout->addLayout(bottomRow);

        // ── Connections ──

        // Import folder
        QObject::connect(importBtn, &QPushButton::clicked, this, [this]() {
            const QString dir = QFileDialog::getExistingDirectory(this, QStringLiteral("Select Music Folder"));
            if (dir.isEmpty()) return;
            qInfo().noquote() << QStringLiteral("LIBRARY_SCAN_STARTED=%1").arg(dir);

            allTracks_ = scanFolderForTracks(dir);
            importedFolderPath_ = dir;
            qInfo().noquote() << QStringLiteral("FILES_FOUND=%1").arg(allTracks_.size());
            qInfo().noquote() << QStringLiteral("TRACKS_INDEXED=%1").arg(allTracks_.size());

            applyCoreDurationPatch(allTracks_);
djDb_.bulkInsert(allTracks_);

            // Clear detail panel before rebuild
            clearTrackDetail();
            refreshLibraryList();

            // Save library to disk (metadata already extracted during scan)
            saveLibraryJson(allTracks_, importedFolderPath_);
            qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_SCAN");
        });

        // Import legacy DB
        QObject::connect(legacyDbBtn, &QPushButton::clicked, this, [this]() {
            if (allTracks_.empty()) {
                QMessageBox::information(this, QStringLiteral("Import Legacy DB"),
                    QStringLiteral("Import a music folder first, then merge legacy analysis data."));
                return;
            }
            const QString dbPath = findLegacyDbPath();
            if (dbPath.isEmpty()) {
                QMessageBox::warning(this, QStringLiteral("Legacy DB Not Found"),
                    QStringLiteral("Could not locate library.db in AppData/Roaming."));
                return;
            }
            qInfo().noquote() << QStringLiteral("LEGACY_DB_IMPORT_STARTED=%1").arg(dbPath);
            const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
            qInfo().noquote() << QStringLiteral("LEGACY_DB_IMPORT_DONE matched=%1 unmatched=%2 total=%3")
                .arg(res.matched).arg(res.unmatched).arg(res.totalDbRows);

            applyCoreDurationPatch(allTracks_);
djDb_.bulkInsert(allTracks_);
            refreshLibraryList();
            saveLibraryJson(allTracks_, importedFolderPath_);
            qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_LEGACY_IMPORT");

            QMessageBox::information(this, QStringLiteral("Legacy DB Imported"),
                QStringLiteral("Matched %1 of %2 DB tracks.\n%3 unmatched.")
                    .arg(res.matched).arg(res.totalDbRows).arg(res.unmatched));
        });

        // Single-click → show detail
        QObject::connect(libraryTree_, &LibraryBrowserWidget::trackSelected, this,
            [this](qint64 trackId) {
            const int trackIdx = static_cast<int>(trackId);
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            showTrackDetail(trackIdx);
        });

        // Double-click track → play
        QObject::connect(libraryTree_, &LibraryBrowserWidget::trackActivated, this,
            [this](qint64 trackId) {
            const int trackIdx = static_cast<int>(trackId);
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            qInfo().noquote() << QStringLiteral("TRACK_SELECTED=%1").arg(allTracks_[trackIdx].displayName);
            playerPage_->setTrackList(&allTracks_);
            playerPage_->activateTrack(trackIdx);
            qInfo().noquote() << QStringLiteral("PLAYER_OPENED=TRUE");
            stack_->setCurrentIndex(2);
        });

        // ── Right-click context menu on library tree ──
        QObject::connect(libraryTree_, &LibraryBrowserWidget::contextMenuRequested, this,
            [this](qint64 trackId, QPoint globalPos) {
            const int trackIdx = static_cast<int>(trackId);
            if (trackIdx < 0 || trackIdx >= static_cast<int>(allTracks_.size())) return;
            libraryTree_->setCurrentTrackId(trackId);
            const TrackInfo& t = allTracks_[trackIdx];

            QMenu menu(libraryTree_);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
                "  padding: 4px 0; }"
                "QMenu::item { padding: 6px 24px; }"
                "QMenu::item:selected { background: #533483; }"
                "QMenu::item:disabled { color: #666; }"
                "QMenu::separator { height: 1px; background: #0f3460; margin: 4px 8px; }"
            ));

            // ─ Playback actions ─
            auto* playAction = menu.addAction(QStringLiteral("Play"));
            auto* openInPlayerAction = menu.addAction(QStringLiteral("Open in Player"));
            menu.addSeparator();
            auto* playNextAction = menu.addAction(QStringLiteral("Play Next"));
            playNextAction->setEnabled(false);
            playNextAction->setToolTip(QStringLiteral("Queue system coming soon"));
            auto* addToQueueAction = menu.addAction(QStringLiteral("Add to Queue"));
            addToQueueAction->setEnabled(false);
            addToQueueAction->setToolTip(QStringLiteral("Queue system coming soon"));
            menu.addSeparator();

            // ─ Info & edit ─
            auto* trackInfoAction = menu.addAction(QStringLiteral("Track Info"));
            auto* editTagsAction = menu.addAction(QStringLiteral("Edit Tags"));
            editTagsAction->setEnabled(false);
            editTagsAction->setToolTip(QStringLiteral("Tag editor coming soon"));
            menu.addSeparator();

            // ─ Analysis ─
            auto* analyzeAction = menu.addAction(QStringLiteral("Analyze Track"));
            analyzeAction->setEnabled(false);
            analyzeAction->setToolTip(QStringLiteral("Analysis pipeline coming soon"));
            auto* refreshMetaAction = menu.addAction(QStringLiteral("Refresh Metadata"));
            menu.addSeparator();

            // ─ Playlist ─
            auto* playlistMenu = menu.addMenu(QStringLiteral("Add to Playlist"));
            playlistMenu->setStyleSheet(menu.styleSheet());
            std::vector<QAction*> playlistActions;
            for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                auto* a = playlistMenu->addAction(playlists_[pi].name);
                a->setData(static_cast<int>(pi));
                playlistActions.push_back(a);
            }
            if (!playlists_.empty()) playlistMenu->addSeparator();
            auto* newPlaylistAction = playlistMenu->addAction(QStringLiteral("New Playlist..."));
            menu.addSeparator();

            // ─ File operations ─
            auto* showInFolderAction = menu.addAction(QStringLiteral("Show in Folder"));
            auto* copyPathAction = menu.addAction(QStringLiteral("Copy File Path"));
            menu.addSeparator();

            // ─ Destructive ─
            auto* removeAction = menu.addAction(QStringLiteral("Remove from Library"));
            auto* deleteFromDiskAction = menu.addAction(QStringLiteral("Delete from Disk..."));

            // ── Execute selected action ──
            auto* chosen = menu.exec(globalPos);
            if (!chosen) return;

            if (chosen == playAction || chosen == openInPlayerAction) {
                playerPage_->setTrackList(&allTracks_);
                playerPage_->activateTrack(trackIdx);
                qInfo().noquote() << QStringLiteral("CTX_PLAY=%1").arg(t.displayName);
                stack_->setCurrentIndex(2);
            } else if (chosen == trackInfoAction) {
                showTrackDetail(trackIdx);
                qInfo().noquote() << QStringLiteral("CTX_TRACK_INFO=%1").arg(t.displayName);
            } else if (chosen == refreshMetaAction) {
                readId3Tags(allTracks_[trackIdx]);
                updateTreeItemForTrack(trackIdx);
                saveLibraryJson(allTracks_, importedFolderPath_);
                qInfo().noquote() << QStringLiteral("CTX_REFRESH_META=%1").arg(allTracks_[trackIdx].displayName);
            } else if (chosen == showInFolderAction) {
                const QFileInfo fi(t.filePath);
                QDesktopServices::openUrl(QUrl::fromLocalFile(fi.absolutePath()));
                qInfo().noquote() << QStringLiteral("CTX_SHOW_FOLDER=%1").arg(fi.absolutePath());
            } else if (chosen == copyPathAction) {
                QGuiApplication::clipboard()->setText(t.filePath);
                qInfo().noquote() << QStringLiteral("CTX_COPY_PATH=%1").arg(t.filePath);
            } else if (chosen == removeAction) {
                qInfo().noquote() << QStringLiteral("CTX_REMOVE=%1").arg(t.displayName);
                allTracks_.erase(allTracks_.begin() + trackIdx);
                saveLibraryJson(allTracks_, importedFolderPath_);
                refreshLibraryList();
            } else if (chosen == deleteFromDiskAction) {
                // Warning dialog before permanent file deletion
                QMessageBox warning(libraryTree_);
                warning.setWindowTitle(QStringLiteral("Delete from Disk"));
                warning.setIcon(QMessageBox::Warning);
                warning.setText(QStringLiteral("Permanently delete this file?"));
                warning.setInformativeText(
                    QStringLiteral("Track: %1\nPath: %2\n\nThis cannot be undone.")
                        .arg(t.displayName, t.filePath));
                warning.setStandardButtons(QMessageBox::Cancel);
                auto* deleteBtn = warning.addButton(QStringLiteral("Delete"), QMessageBox::DestructiveRole);
                warning.setDefaultButton(QMessageBox::Cancel);
                warning.setStyleSheet(QStringLiteral(
                    "QMessageBox { background: #1a1a2e; color: #e0e0e0; }"
                    "QPushButton { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
                    "  border-radius: 4px; padding: 6px 16px; }"
                    "QPushButton:hover { background: #533483; }"
                ));
                warning.exec();
                if (warning.clickedButton() == deleteBtn) {
                    const QString path = t.filePath;
                    const QString name = t.displayName;
                    if (QFile::remove(path)) {
                        qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_OK=%1 PATH=%2").arg(name, path);
                        allTracks_.erase(allTracks_.begin() + trackIdx);
                        saveLibraryJson(allTracks_, importedFolderPath_);
                        refreshLibraryList();
                    } else {
                        qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_FAIL=%1 PATH=%2").arg(name, path);
                        QMessageBox::critical(libraryTree_, QStringLiteral("Delete Failed"),
                            QStringLiteral("Could not delete:\n%1\n\nThe file may be in use or read-only.").arg(path));
                    }
                } else {
                    qInfo().noquote() << QStringLiteral("CTX_DELETE_DISK_CANCELLED=%1").arg(t.displayName);
                }
            } else if (chosen == newPlaylistAction) {
                bool ok = false;
                const QString name = QInputDialog::getText(libraryTree_,
                    QStringLiteral("New Playlist"),
                    QStringLiteral("Playlist name:"),
                    QLineEdit::Normal, QString(), &ok);
                if (ok && !name.trimmed().isEmpty()) {
                    Playlist pl;
                    pl.name = name.trimmed();
                    pl.trackPaths.append(t.filePath);
                    playlists_.push_back(std::move(pl));
                    savePlaylists(playlists_);
                    qInfo().noquote() << QStringLiteral("CTX_NEW_PLAYLIST=%1 TRACK=%2").arg(name.trimmed(), t.displayName);
                }
            } else {
                // Check if an existing playlist was selected
                for (auto* pa : playlistActions) {
                    if (chosen == pa) {
                        const int pi = pa->data().toInt();
                        if (pi >= 0 && pi < static_cast<int>(playlists_.size())) {
                            playlists_[pi].trackPaths.append(t.filePath);
                            savePlaylists(playlists_);
                            qInfo().noquote() << QStringLiteral("CTX_ADD_TO_PLAYLIST=%1 TRACK=%2")
                                .arg(playlists_[pi].name, t.displayName);
                        }
                        break;
                    }
                }
            }
        });

        // Track count → update bottom bar label
        QObject::connect(libraryTree_, &LibraryBrowserWidget::trackCountChanged, this,
            [this](int count) {
            if (trackCountLabel_)
                trackCountLabel_->setText(QStringLiteral("%1 tracks").arg(count));
        });

        // ── Play All button ──
        QObject::connect(playAllBtn, &QPushButton::clicked, this, [this]() {
            if (allTracks_.empty()) return;
            const qint64 firstId = libraryTree_->firstVisibleTrackId();
            if (firstId < 0) return;
            const int idx = static_cast<int>(firstId);
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
                playerPage_->setTrackList(&allTracks_);
                playerPage_->activateTrack(idx);
                qInfo().noquote() << QStringLiteral("PLAY_ALL_START=%1").arg(allTracks_[idx].displayName);
                stack_->setCurrentIndex(2);
            }
        });

        // ── Now Playing button — scroll to current track ──
        QObject::connect(nowPlayingBtn, &QPushButton::clicked, this, [this]() {
            const int idx = playerPage_ ? playerPage_->currentTrackIndex() : -1;
            if (idx < 0 || idx >= static_cast<int>(allTracks_.size())) return;
            libraryTree_->scrollToTrackId(static_cast<qint64>(idx));
            qInfo().noquote() << QStringLiteral("NOW_PLAYING_SCROLL=%1").arg(allTracks_[idx].displayName);
        });

        // ── Playlists button — popup menu to browse / filter by playlist ──
        QObject::connect(playlistsBtn, &QPushButton::clicked, this, [this, playlistsBtn]() {
            QMenu menu(playlistsBtn);
            menu.setStyleSheet(QStringLiteral(
                "QMenu { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460; }"
                "QMenu::item:selected { background: #533483; }"));

            // "Show All Library" — clears playlist filter
            auto* showAllAction = menu.addAction(QStringLiteral("Show All Library"));
            showAllAction->setEnabled(activePlaylistIndex_ >= 0);

            menu.addSeparator();

            // List existing playlists
            std::vector<QAction*> plActions;
            for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                const QString label = QStringLiteral("%1 (%2)")
                    .arg(playlists_[pi].name)
                    .arg(playlists_[pi].trackPaths.size());
                auto* a = menu.addAction(label);
                a->setCheckable(true);
                a->setChecked(static_cast<int>(pi) == activePlaylistIndex_);
                a->setData(static_cast<int>(pi));
                plActions.push_back(a);
            }

            if (!playlists_.empty()) menu.addSeparator();

            auto* newPlAction = menu.addAction(QStringLiteral("New Playlist..."));
            QAction* deletePlAction = nullptr;
            if (!playlists_.empty()) {
                deletePlAction = menu.addAction(QStringLiteral("Delete Playlist..."));
            }

            auto* chosen = menu.exec(playlistsBtn->mapToGlobal(
                QPoint(0, playlistsBtn->height())));
            if (!chosen) return;

            if (chosen == showAllAction) {
                activePlaylistIndex_ = -1;
                playlistsBtn->setText(QStringLiteral("Playlists"));
                refreshLibraryList();
                qInfo().noquote() << QStringLiteral("PLAYLIST_FILTER=ALL");
            } else if (chosen == newPlAction) {
                bool ok = false;
                const QString name = QInputDialog::getText(playlistsBtn,
                    QStringLiteral("New Playlist"),
                    QStringLiteral("Playlist name:"),
                    QLineEdit::Normal, QString(), &ok);
                if (ok && !name.trimmed().isEmpty()) {
                    Playlist pl;
                    pl.name = name.trimmed();
                    playlists_.push_back(std::move(pl));
                    savePlaylists(playlists_);
                    qInfo().noquote() << QStringLiteral("PLAYLIST_CREATED=%1").arg(name.trimmed());
                }
            } else if (chosen == deletePlAction) {
                // Show a second menu to pick which playlist to delete
                QMenu delMenu(playlistsBtn);
                delMenu.setStyleSheet(menu.styleSheet());
                std::vector<QAction*> delActions;
                for (size_t pi = 0; pi < playlists_.size(); ++pi) {
                    auto* a = delMenu.addAction(playlists_[pi].name);
                    a->setData(static_cast<int>(pi));
                    delActions.push_back(a);
                }
                auto* delChosen = delMenu.exec(QCursor::pos());
                if (delChosen) {
                    const int di = delChosen->data().toInt();
                    if (di >= 0 && di < static_cast<int>(playlists_.size())) {
                        const QString deletedName = playlists_[di].name;
                        playlists_.erase(playlists_.begin() + di);
                        savePlaylists(playlists_);
                        if (activePlaylistIndex_ == di) {
                            activePlaylistIndex_ = -1;
                            playlistsBtn->setText(QStringLiteral("Playlists"));
                            refreshLibraryList();
                        } else if (activePlaylistIndex_ > di) {
                            --activePlaylistIndex_;
                        }
                        qInfo().noquote() << QStringLiteral("PLAYLIST_DELETED=%1").arg(deletedName);
                    }
                }
            } else {
                // Check if an existing playlist was selected to filter
                for (auto* pa : plActions) {
                    if (chosen == pa) {
                        const int pi = pa->data().toInt();
                        if (pi >= 0 && pi < static_cast<int>(playlists_.size())) {
                            activePlaylistIndex_ = pi;
                            playlistsBtn->setText(QStringLiteral("Playlists: %1").arg(playlists_[pi].name));
                            refreshLibraryList();
                            qInfo().noquote() << QStringLiteral("PLAYLIST_FILTER=%1").arg(playlists_[pi].name);
                        }
                        break;
                    }
                }
            }
        });

        // Nav buttons
        QObject::connect(playerBtn, &QPushButton::clicked, this, [this]() {
            playerPage_->refreshLibrary();
            stack_->setCurrentIndex(2);
        });
        QObject::connect(djBtn, &QPushButton::clicked, this, [this]() {
            bridge_.enterDjMode();
            djModePage_->refreshLibrary();
            const QString intro = QCoreApplication::applicationDirPath()
                                  + QStringLiteral("/../../../assets/Video Project 1.mp4");
            djIntroOverlay_->play(intro);
            stack_->setCurrentIndex(4);
        });

        return page;
    }



    // ── Library helpers ──
    void refreshLibraryList()
    {
        if (!libraryTree_) return;
        QStringList playlistPaths;
        if (activePlaylistIndex_ >= 0
                && activePlaylistIndex_ < static_cast<int>(playlists_.size())) {
            for (const QString& p : playlists_[activePlaylistIndex_].trackPaths)
                playlistPaths << p;
        }
        libraryTree_->setPlaylistFilter(playlistPaths);
        libraryTree_->refresh();
    }

    void updateTreeItemForTrack(int trackIndex)
    {
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        djDb_.upsertTrack(static_cast<qint64>(trackIndex), allTracks_[trackIndex]);
        // Re-apply filter so the updated row appears with fresh data
        refreshLibraryList();
        // If this track is currently selected in the detail panel, refresh the detail
        if (libraryTree_ && libraryTree_->currentTrackId() == static_cast<qint64>(trackIndex))
            showTrackDetail(trackIndex);
    }

    void showTrackDetail(int trackIndex)
    {
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        if (!detailPanel_) return;
        detailPanel_->display(allTracks_[trackIndex]);
    }

    void clearTrackDetail()
    {
        if (detailPanel_) detailPanel_->clear();
    }


















    void showDiagnostics()
    {
        if (!diagnosticsDialog_) {
            diagnosticsDialog_ = new DiagnosticsDialog(bridge_, this);
        }
        if (!lastStatus_.lastUpdateUtc.empty()) {
            diagnosticsDialog_->setStatus(lastStatus_);
        }
        diagnosticsDialog_->setHealth(lastHealth_);
        diagnosticsDialog_->setTelemetry(lastTelemetry_);
        diagnosticsDialog_->setFoundation(lastFoundation_, selfTestsRan_ ? &lastSelfTests_ : nullptr);
        diagnosticsDialog_->setRtAudio(lastTelemetry_);
        diagnosticsDialog_->refreshLogTail();
        diagnosticsDialog_->show();
        diagnosticsDialog_->raise();
        diagnosticsDialog_->activateWindow();
    }

    void pollStatus()
    {
        UIStatus status {};
        status.buildStamp = NGKS_BUILD_STAMP;
        status.gitSha = NGKS_GIT_SHA;
        status.lastUpdateUtc = utcNowIso().toStdString();
        const bool ready = bridge_.tryGetStatus(status);

        if (!ready) {
            status.engineReady = false;
        }

        UIHealthSnapshot health {};
        const bool healthReady = bridge_.tryGetHealth(health);
        if (!healthReady) {
            health.engineInitialized = false;
            health.audioDeviceReady = false;
            health.lastRenderCycleOk = false;
            health.renderCycleCounter = 0;
        }

        UIEngineTelemetrySnapshot telemetry {};
        const bool telemetryReady = bridge_.tryGetTelemetry(telemetry);
        if (!telemetryReady) {
            telemetry = {};
        }

        int64_t stallMs = 0;
        const bool watchdogOk = bridge_.pollRtWatchdog(500, stallMs);
        telemetry.rtWatchdogOk = watchdogOk;

        UIFoundationSnapshot foundation {};
        const bool foundationReady = bridge_.tryGetFoundation(foundation);
        if (!foundationReady) {
            foundation = {};
        }

        lastStatus_ = status;
        lastHealth_ = health;
        lastTelemetry_ = telemetry;
        lastFoundation_ = foundation;

        // ── JUCE engine status ──
        const bool juceReady = status.engineReady;
        engineStatusLabel_->setText(juceReady
            ? QStringLiteral("Engine: READY") : QStringLiteral("Engine: NOT_READY"));

        const bool effectiveRunning = bridge_.running();
        runningLabel_->setText(effectiveRunning
            ? QStringLiteral("Running: YES") : QStringLiteral("Running: NO"));

        // Meters: JUCE engine only
        const double meterL = bridge_.meterL();
        const double meterR = bridge_.meterR();
        meterLabel_->setText(QStringLiteral("MeterL: %1  MeterR: %2")
            .arg(QString::number(meterL, 'f', 3),
                 QString::number(meterR, 'f', 3)));

        // Feed visualizer from JUCE engine meters
        if (playerPage_) {
            playerPage_->setAudioLevel(static_cast<float>(std::max(meterL, meterR)));
        }

        if (diagnosticsDialog_) {
            diagnosticsDialog_->setStatus(status);
            diagnosticsDialog_->setHealth(health);
            diagnosticsDialog_->setTelemetry(telemetry);
            diagnosticsDialog_->setFoundation(foundation, selfTestsRan_ ? &lastSelfTests_ : nullptr);
            diagnosticsDialog_->setRtAudio(telemetry);
        }

        if (!statusTickLogged_) {
            qInfo().noquote() << QStringLiteral("StatusPollTick=PASS %1").arg(statusSummaryLine(status));
            statusTickLogged_ = true;
        }

        if (!healthTickLogged_) {
            qInfo() << "HealthPollTick=PASS";
            qInfo().noquote() << QStringLiteral("HealthEngineInit=%1").arg(boolToFlag(health.engineInitialized));
            qInfo().noquote() << QStringLiteral("HealthAudioReady=%1").arg(boolToFlag(health.audioDeviceReady));
            qInfo().noquote() << QStringLiteral("HealthRenderOK=%1").arg(boolToFlag(health.lastRenderCycleOk));
            qInfo().noquote() << QStringLiteral("RenderCycleCounter=%1").arg(QString::number(static_cast<qulonglong>(health.renderCycleCounter)));
            healthTickLogged_ = true;
        }

        if (!telemetryTickLogged_) {
            qInfo() << "TelemetryPollTick=PASS";
            qInfo().noquote() << QStringLiteral("TelemetryRenderCycles=%1").arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)));
            qInfo().noquote() << QStringLiteral("TelemetryAudioCallbacks=%1").arg(QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)));
            qInfo().noquote() << QStringLiteral("TelemetryXRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("TelemetryLastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryMaxRenderUs=%1").arg(QString::number(telemetry.maxRenderDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryLastCallbackUs=%1").arg(QString::number(telemetry.lastCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetryMaxCallbackUs=%1").arg(QString::number(telemetry.maxCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("TelemetrySparkline=%1").arg(telemetrySparkline(telemetry));
            qInfo() << "=== Telemetry Snapshot ===";
            qInfo().noquote() << QStringLiteral("RenderCycles=%1").arg(QString::number(static_cast<qulonglong>(telemetry.renderCycles)));
            qInfo().noquote() << QStringLiteral("AudioCallbacks=%1").arg(QString::number(static_cast<qulonglong>(telemetry.audioCallbacks)));
            qInfo().noquote() << QStringLiteral("XRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.xruns)));
            qInfo().noquote() << QStringLiteral("LastRenderUs=%1").arg(QString::number(telemetry.lastRenderDurationUs));
            qInfo().noquote() << QStringLiteral("MaxRenderUs=%1").arg(QString::number(telemetry.maxRenderDurationUs));
            qInfo().noquote() << QStringLiteral("LastCallbackUs=%1").arg(QString::number(telemetry.lastCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("MaxCallbackUs=%1").arg(QString::number(telemetry.maxCallbackDurationUs));
            qInfo().noquote() << QStringLiteral("Sparkline=%1").arg(telemetrySparkline(telemetry));
            qInfo() << "==========================";
            telemetryTickLogged_ = true;
        }

        if (!foundationTickLogged_) {
            qInfo() << "FoundationPollTick=PASS";
            qInfo().noquote() << QStringLiteral("FoundationReportLine=%1").arg(foundationReportLine(foundation));
            qInfo().noquote() << QStringLiteral("FoundationTelemetryRenderCycles=%1").arg(QString::number(static_cast<qulonglong>(foundation.telemetryRenderCycles)));
            qInfo().noquote() << QStringLiteral("FoundationHealthRenderOK=%1").arg(boolToFlag(foundation.healthRenderOk));
            foundationTickLogged_ = true;
        }

        if (selfTestsRan_ && !foundationSelfTestLogged_) {
            qInfo().noquote() << QStringLiteral("FoundationSelfTestSummary=%1").arg(passFail(lastSelfTests_.allPass));
            foundationSelfTestLogged_ = true;
        }

        qInfo() << "RTAudioPollTick=PASS";
        qInfo().noquote() << QStringLiteral("RTAudioDeviceOpen=%1").arg(boolToFlag(telemetry.rtDeviceOpenOk));
        qInfo().noquote() << QStringLiteral("RTAudioCallbackCount=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtCallbackCount)));
        qInfo().noquote() << QStringLiteral("RTAudioXRuns=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCount)));
        qInfo().noquote() << QStringLiteral("RTAudioXRunsTotal=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCountTotal)));
        qInfo().noquote() << QStringLiteral("RTAudioXRunsWindow=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtXRunCountWindow)));
        qInfo().noquote() << QStringLiteral("RTAudioJitterMaxNsWindow=%1").arg(QString::number(static_cast<qulonglong>(telemetry.rtJitterAbsNsMaxWindow)));
        qInfo().noquote() << QStringLiteral("RTAudioDeviceRestartCount=%1").arg(QString::number(telemetry.rtDeviceRestartCount));
        qInfo().noquote() << QStringLiteral("RTAudioWatchdogState=%1").arg(rtWatchdogStateText(telemetry.rtWatchdogStateCode));
        qInfo().noquote() << QStringLiteral("RTAudioPeakDb=%1").arg(QString::number(static_cast<double>(telemetry.rtMeterPeakDb10) / 10.0, 'f', 1));
        qInfo().noquote() << QStringLiteral("RTAudioWatchdog=%1").arg(boolToFlag(telemetry.rtWatchdogOk));
        if (!telemetry.rtWatchdogOk) {
            qInfo().noquote() << QStringLiteral("RTAudioWatchdogStallMs=%1").arg(QString::number(stallMs));
        }

        if (telemetry.rtDeviceOpenOk) {
            const QString markerKey = QStringLiteral("%1|%2|%3|%4|%5|%6|%7")
                .arg(QString::fromUtf8(telemetry.rtDeviceId),
                     QString::number(telemetry.rtRequestedSampleRate),
                     QString::number(telemetry.rtRequestedBufferFrames),
                     QString::number(telemetry.rtRequestedChannelsOut),
                     QString::number(telemetry.rtSampleRate),
                     QString::number(telemetry.rtBufferFrames),
                     QString::number(telemetry.rtChannelsOut));
            if (markerKey != lastAgMarkerKey_) {
                qInfo().noquote() << QStringLiteral("RTAudioAGRequestedSR=%1").arg(QString::number(telemetry.rtRequestedSampleRate));
                qInfo().noquote() << QStringLiteral("RTAudioAGAppliedSR=%1").arg(QString::number(telemetry.rtSampleRate));
                qInfo().noquote() << QStringLiteral("RTAudioAGFallback=%1").arg(telemetry.rtAgFallback ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));
                lastAgMarkerKey_ = markerKey;
            }
        }
    }

    void runFoundationSelfTests()
    {
        UISelfTestSnapshot selfTests {};
        bridge_.runSelfTests(selfTests);
        lastSelfTests_ = selfTests;
        selfTestsRan_ = true;

        qInfo() << "SelfTestSuite=BEGIN";
        qInfo().noquote() << QStringLiteral("SelfTest_TelemetryReadable=%1").arg(passFail(selfTests.telemetryReadable));
        qInfo().noquote() << QStringLiteral("SelfTest_HealthReadable=%1").arg(passFail(selfTests.healthReadable));
        qInfo().noquote() << QStringLiteral("SelfTest_OfflineRenderPasses=%1").arg(passFail(selfTests.offlineRenderPasses));
        qInfo() << "SelfTestSuite=END";
        qInfo().noquote() << QStringLiteral("FoundationSelfTestSummary=%1").arg(passFail(selfTests.allPass));
        foundationSelfTestLogged_ = true;

        UIFoundationSnapshot foundation {};
        if (bridge_.tryGetFoundation(foundation)) {
            lastFoundation_ = foundation;
            if (diagnosticsDialog_ != nullptr) {
                diagnosticsDialog_->setFoundation(lastFoundation_, &lastSelfTests_);
            }
        }
    }

    void startRtProbeAutorun()
    {
        bridge_.startRtProbe(440.0, -12.0);
        QTimer::singleShot(5000, this, [this]() { bridge_.stopRtProbe(); });
    }

public:
    void autoShowDiagnosticsIfRequested()
    {
        const QString autoshow = qEnvironmentVariable("NGKS_DIAG_AUTOSHOW").trimmed().toLower();
        if (autoshow == QStringLiteral("1") || autoshow == QStringLiteral("true") || autoshow == QStringLiteral("yes")) {
            showDiagnostics();
        }
    }

private:
    EngineBridge& bridge_;
    QTimer pollTimer_;
    DiagnosticsDialog* diagnosticsDialog_{nullptr};
    QStackedWidget* stack_{nullptr};
    LibraryBrowserWidget* libraryTree_{nullptr};
    QLabel* trackCountLabel_{nullptr};
    TrackDetailPanel* detailPanel_{nullptr};
    PlayerPage* playerPage_{nullptr};
    DjModePage* djModePage_{nullptr};
    DjIntroOverlay* djIntroOverlay_{nullptr};
    DjLibraryDatabase djDb_;
    std::vector<TrackInfo> allTracks_;
    std::vector<Playlist> playlists_;
    int activePlaylistIndex_{-1}; // -1 = show all library
    QString importedFolderPath_;


    QLabel* engineStatusLabel_{nullptr};
    QLabel* runningLabel_{nullptr};
    QLabel* meterLabel_{nullptr};
    UIStatus lastStatus_ {};
    UIHealthSnapshot lastHealth_ {};
    UIEngineTelemetrySnapshot lastTelemetry_ {};
    UIFoundationSnapshot lastFoundation_ {};
    UISelfTestSnapshot lastSelfTests_ {};
    bool selfTestsRan_{false};
    bool selfTestAutorun_{false};
    bool rtProbeAutorun_{false};
    bool statusTickLogged_{false};
    bool healthTickLogged_{false};
    bool telemetryTickLogged_{false};
    bool foundationTickLogged_{false};
    bool foundationSelfTestLogged_{false};
    QString lastAgMarkerKey_{};
};

} // namespace

int main(int argc, char* argv[])
{
    initializeUiRuntimeLog();
    installCrashCaptureHandlers();

    QApplication app(argc, argv);

    const QString smokeFlag = qEnvironmentVariable("NGKS_UI_SMOKE").trimmed().toLower();
    const bool smokeMode = (smokeFlag == QStringLiteral("1") || smokeFlag == QStringLiteral("true") || smokeFlag == QStringLiteral("yes"));
    int smokeSeconds = 5;
    if (smokeMode) {
        bool ok = false;
        const int parsed = qEnvironmentVariable("NGKS_UI_SMOKE_SECONDS").toInt(&ok);
        if (ok && parsed > 0) {
            smokeSeconds = parsed;
        }
        writeLine(QStringLiteral("=== UI Smoke Harness ENABLED seconds=%1 ===").arg(smokeSeconds));
        QJsonObject smokePayload;
        smokePayload.insert(QStringLiteral("enabled"), true);
        smokePayload.insert(QStringLiteral("seconds"), smokeSeconds);
        writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("ui_smoke"), smokePayload);
    }

    const QStringList pluginPaths = QCoreApplication::libraryPaths();
    writeLine(QStringLiteral("QtPluginPaths=%1").arg(pluginPaths.join(';')));
    writeLine(QStringLiteral("EnvReport PlatformName=%1").arg(QGuiApplication::platformName()));
    QJsonObject pathsPayload;
    pathsPayload.insert(QStringLiteral("plugin_paths"), pluginPaths.join(';'));
    pathsPayload.insert(QStringLiteral("platform_name"), QGuiApplication::platformName());
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("qt_paths"), pathsPayload);

    const QString exePath = QCoreApplication::applicationFilePath();
    const QString cwd = QDir::currentPath();
    const bool depSnapshotOk = writeDependencySnapshot(exePath, cwd, gPathSnapshot, pluginPaths);
    if (depSnapshotOk) {
        writeLine(QStringLiteral("DepSnapshot=PASS path=%1").arg(QString::fromStdString(gDepsSnapshotPath)));
    } else {
        writeLine(QStringLiteral("DepSnapshot=FAIL path=%1").arg(QString::fromStdString(gDepsSnapshotPath)));
    }
    QJsonObject depPayload;
    depPayload.insert(QStringLiteral("pass"), depSnapshotOk);
    depPayload.insert(QStringLiteral("path"), QString::fromStdString(gDepsSnapshotPath));
    writeJsonEvent(depSnapshotOk ? QStringLiteral("INFO") : QStringLiteral("ERROR"), QStringLiteral("dep_snapshot"), depPayload);

    const bool uiSelfCheckPass = gRuntimeDirReady && gLogWritable && gDllProbePass;
    if (uiSelfCheckPass) {
        writeLine(QStringLiteral("UiSelfCheck=PASS"));
    } else {
        QStringList reasons;
        if (!gRuntimeDirReady) {
            reasons.push_back(QStringLiteral("runtime_dir_missing"));
        }
        if (!gLogWritable) {
            reasons.push_back(QStringLiteral("log_not_writable"));
        }
        if (!gDllProbePass) {
            reasons.push_back(QStringLiteral("dll_probe_failed"));
        }
        writeLine(QStringLiteral("UiSelfCheck=FAIL reasons=%1").arg(reasons.join(',')));
        QJsonObject selfCheckPayload;
        selfCheckPayload.insert(QStringLiteral("pass"), false);
        selfCheckPayload.insert(QStringLiteral("reasons"), reasons.join(','));
        writeJsonEvent(QStringLiteral("ERROR"), QStringLiteral("self_check"), selfCheckPayload);
        return 2;
    }
    QJsonObject selfCheckPayload;
    selfCheckPayload.insert(QStringLiteral("pass"), true);
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("self_check"), selfCheckPayload);

    writeLine(QStringLiteral("UI app initialized pid=%1").arg(QString::number(QCoreApplication::applicationPid())));
    QJsonObject initPayload;
    initPayload.insert(QStringLiteral("pid"), static_cast<qint64>(QCoreApplication::applicationPid()));
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("app_init"), initPayload);

    EngineBridge engineBridge;

    // ── Dump previous ring buffer if crash left one ──
    // On startup, check if data/runtime/trace_ring_dump.txt exists from a previous frozen session
    {
        const QString ringDumpPath = runtimePath("data/runtime/trace_ring_dump.txt");
        if (QFileInfo::exists(ringDumpPath)) {
            std::fprintf(stderr, "[AUDIO_TRACE] Previous session ring dump found at %s\n",
                         ringDumpPath.toUtf8().constData());
        }
    }

    // ── UI thread heartbeat timer ──
    // Prints to stderr every 500ms so we can detect UI thread stalls during unplug
    QTimer uiHeartbeatTimer;
    uiHeartbeatTimer.setInterval(500);
    uint64_t uiHeartbeatCount = 0;
    QObject::connect(&uiHeartbeatTimer, &QTimer::timeout, [&uiHeartbeatCount]() {
        ++uiHeartbeatCount;
        // Only log every 4th beat (~2 seconds) to keep noise down, but still detect stalls
        if ((uiHeartbeatCount % 4) == 0) {
            ngks::audioTrace("UI_HEARTBEAT", "beat=%llu", static_cast<unsigned long long>(uiHeartbeatCount));
        }
    });
    uiHeartbeatTimer.start();

    // ── Freeze-detect timer: dump ring buffer if UI heartbeat stalls ──
    QElapsedTimer uiAliveTimer;
    uiAliveTimer.start();
    QTimer freezeDetectTimer;
    freezeDetectTimer.setInterval(3000); // check every 3s 
    QObject::connect(&freezeDetectTimer, &QTimer::timeout, [&uiAliveTimer]() {
        // If this fires, the UI thread is alive (the timer ran).
        // Reset the alive timer.
        uiAliveTimer.restart();
    });
    freezeDetectTimer.start();

    MainWindow window(engineBridge);
    window.show();
    writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("window_show"), QJsonObject());
    window.autoShowDiagnosticsIfRequested();

    QObject::connect(&app, &QCoreApplication::aboutToQuit, [&]() {
        uiHeartbeatTimer.stop();
        freezeDetectTimer.stop();
        // Dump ring buffer on exit for post-mortem analysis
        ngks::traceRing().dumpToFile(
            runtimePath("data/runtime/trace_ring_dump.txt").toUtf8().constData());
        writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("shutdown"), QJsonObject());
        if (smokeMode) {
            writeLine(QStringLiteral("UiSmokeExit=PASS seconds=%1").arg(smokeSeconds));
            QJsonObject smokeExitPayload;
            smokeExitPayload.insert(QStringLiteral("pass"), true);
            smokeExitPayload.insert(QStringLiteral("seconds"), smokeSeconds);
            writeJsonEvent(QStringLiteral("INFO"), QStringLiteral("ui_smoke_exit"), smokeExitPayload);
        }
    });

    if (smokeMode) {
        QTimer::singleShot(smokeSeconds * 1000, &app, &QCoreApplication::quit);
    }

    // ── Bench-load: auto-load a track for deterministic timing capture ──
    // Usage: native.exe --bench-load "C:\path\to\file.mp3"
    //        native.exe --bench-load "C:\path\to\file.mp3" --bench-deck 1
    {
        const QStringList args = QCoreApplication::arguments();
        const int blIdx = args.indexOf(QStringLiteral("--bench-load"));
        if (blIdx >= 0 && blIdx + 1 < args.size()) {
            const QString benchFile = args.at(blIdx + 1);
            int benchDeck = 0;
            const int bdIdx = args.indexOf(QStringLiteral("--bench-deck"));
            if (bdIdx >= 0 && bdIdx + 1 < args.size())
                benchDeck = args.at(bdIdx + 1).toInt();

            ngks::audioTrace("BENCH_LOAD_SCHEDULED", "deck=%d path=%s",
                             benchDeck, benchFile.toStdString().c_str());

            // Delay 2s to let audio device initialize
            QTimer::singleShot(2000, [&engineBridge, benchFile, benchDeck]() {
                ngks::audioTrace("BENCH_LOAD_FIRE", "deck=%d path=%s",
                                 benchDeck, benchFile.toStdString().c_str());
                engineBridge.loadTrackToDeck(benchDeck, benchFile);
            });

            // Second load of same file after 5s for warm-load measurement
            QTimer::singleShot(5000, [&engineBridge, benchFile, benchDeck]() {
                ngks::audioTrace("BENCH_WARM_LOAD_FIRE", "deck=%d path=%s",
                                 benchDeck, benchFile.toStdString().c_str());
                engineBridge.loadTrackToDeck(benchDeck, benchFile);
            });

            // Auto-quit after 10s
            QTimer::singleShot(10000, &app, &QCoreApplication::quit);
        }
    }

    return app.exec();
}

