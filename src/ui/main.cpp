#include <QAction>
#include <QApplication>
#include "library/DjBrowserPane.h"
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
#include "ui/DeckStrip.h"
#include "engine/DiagLog.h"

#include "ui/diagnostics/RuntimeLogSupport.h"
#include "ui/library/LibraryPersistence.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryBrowserWidget.h"
#include "ui/library/DjLibraryWidget.h"
#include "ui/library/LibraryScanner.h"
#include "ui/library/LegacyLibraryImport.h"
#include "ui/audio/AudioProfileStore.h"
#include "ui/diagnostics/DiagnosticsDialog.h"
#include "ui/widgets/VisualizerWidget.h"

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
        stack_->addWidget(buildPlayerPage());    // 2
        stack_->addWidget(buildDjModePage());    // 3
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

                
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db directly
                  {
                      QString coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      }

                      if (QFile::exists(coreDbPath)) {
                          const QString connName = QStringLiteral("dj_core_duration_fix");
                          {
                              QSqlDatabase coreDb = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
                              coreDb.setDatabaseName(coreDbPath);
                              coreDb.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
                              int matchCore = 0;
                              if (coreDb.open()) {
                                  std::map<QString, size_t> pathIndex;
                                  for (size_t i = 0; i < allTracks_.size(); ++i) {
                                      pathIndex[QDir::fromNativeSeparators(allTracks_[i].filePath).trimmed().toLower()] = i;
                                  }
                                  
                                  QSqlQuery q(coreDb);
                                  q.setForwardOnly(true);
                                  if (q.exec(QStringLiteral("SELECT file_path, duration FROM tracks LIMIT 9999999"))) {
                                      while (q.next()) {
                                          QString fp = QDir::fromNativeSeparators(q.value(0).toString()).trimmed().toLower();
                                          double dur = q.value(1).toDouble();
                                          if (dur > 0) {
                                              auto it = pathIndex.find(fp);
                                              if (it != pathIndex.end()) {
                                                  TrackInfo& t = allTracks_[it->second];
                                                  if (t.durationMs <= 0 || t.durationStr == QStringLiteral("--:--") || t.durationStr.isEmpty()) {
                                                      t.durationMs = static_cast<qint64>(dur * 1000.0);
                                                      const int totalSec = static_cast<int>(dur);
                                                      t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                                                      matchCore++;
                                                  }
                                              }
                                          }
                                      }
                                      qInfo().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH matched=%1").arg(matchCore);
                                  } else {
                                      qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_QUERY ") << q.lastError().text();
                                  }
                                  coreDb.close();
                              } else {
                                  qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_OPEN ") << coreDb.lastError().text();
                              }
                          }
                          QSqlDatabase::removeDatabase(connName);
                      } else {
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND ") << coreDbPath;
                      }
                  }
                  

                  // Fallback: For any tracks without duration, estimate it from MP3 filesize
                  for (auto& t : allTracks_) {
                      if (t.durationMs <= 0 && t.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
                          QFile fFallback(t.filePath);
                          if (fFallback.open(QIODevice::ReadOnly)) {
                              QByteArray hdr = fFallback.read(10);
                              qint64 dataOff = 0;
                              if (hdr.size() == 10 && hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
                                  dataOff = 10 + ((quint32(static_cast<unsigned char>(hdr[6])) << 21)
                                               | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                                               | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                                               | quint32(static_cast<unsigned char>(hdr[9])));
                              }
                              fFallback.seek(dataOff);
                              QByteArray buf = fFallback.read(8192);
                              int bitrate = 0;
                              for (int i = 0; i < buf.size() - 3; ++i) {
                                  if ((static_cast<unsigned char>(buf[i]) == 0xFF) && ((static_cast<unsigned char>(buf[i+1]) & 0xE0) == 0xE0)) {
                                      int bitrateIdx = (static_cast<unsigned char>(buf[i+2]) >> 4) & 0x0F;
                                      int layer = (static_cast<unsigned char>(buf[i+1]) >> 1) & 0x03;
                                      int ver = (static_cast<unsigned char>(buf[i+1]) >> 3) & 0x03;
                                      static const int bitrates[2][3][16] = {
                                          { {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,32,48,56,64,80,96,112,128,144,160,176,192,224,256,0} },
                                          { {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0}, {0,32,48,56,64,80,96,112,128,160,192,224,256,320,384,0}, {0,32,64,96,128,160,192,224,256,288,320,352,384,416,448,0} }
                                      };
                                      int mpegIdx = (ver == 3) ? 1 : 0;
                                      int layerIdx = (layer == 1) ? 0 : ((layer == 2) ? 1 : 2);
                                      bitrate = bitrates[mpegIdx][layerIdx][bitrateIdx];
                                      break;
                                  }
                              }
                              if (bitrate <= 0) bitrate = 256;
                              t.durationMs = static_cast<qint64>(((fFallback.size() - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
                              int totalSec = static_cast<int>(t.durationMs / 1000);
                              t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                          }
                      }
                  }

djDb_.bulkInsert(allTracks_);
                refreshLibraryList();
                rebuildPlayerQueue();
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
                refreshPlayerLibrary();
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
        auto* detailPanel = new QWidget();
        detailPanel->setStyleSheet(QStringLiteral("background: transparent;"));
        auto* detailLayout = new QVBoxLayout(detailPanel);
        detailLayout->setContentsMargins(12, 12, 12, 12);
        detailLayout->setSpacing(6);

        detailTitleLabel_ = new QLabel(QStringLiteral("Track Info"), detailPanel);
        detailTitleLabel_->setObjectName(QStringLiteral("detailTitle"));
        detailLayout->addWidget(detailTitleLabel_);

        auto addDetailRow = [&](const QString& label, QLabel*& valueLabel) {
            auto* fieldLabel = new QLabel(label, detailPanel);
            fieldLabel->setObjectName(QStringLiteral("detailField"));
            detailLayout->addWidget(fieldLabel);
            valueLabel = new QLabel(QStringLiteral("-"), detailPanel);
            valueLabel->setObjectName(QStringLiteral("detailValue"));
            valueLabel->setWordWrap(true);
            detailLayout->addWidget(valueLabel);
        };

        addDetailRow(QStringLiteral("TITLE"), detailTrackTitle_);
        addDetailRow(QStringLiteral("ARTIST"), detailTrackArtist_);
        addDetailRow(QStringLiteral("ALBUM"), detailTrackAlbum_);
        addDetailRow(QStringLiteral("GENRE"), detailTrackGenre_);
        addDetailRow(QStringLiteral("DURATION"), detailTrackDuration_);
        addDetailRow(QStringLiteral("BPM"), detailTrackBpm_);
        addDetailRow(QStringLiteral("KEY"), detailTrackKey_);
        addDetailRow(QStringLiteral("CAMELOT"), detailTrackCamelot_);
        addDetailRow(QStringLiteral("ENERGY"), detailTrackEnergy_);
        addDetailRow(QStringLiteral("LUFS"), detailTrackLufs_);
        addDetailRow(QStringLiteral("CUE IN/OUT"), detailTrackCue_);
        addDetailRow(QStringLiteral("DANCEABILITY"), detailTrackDance_);
        addDetailRow(QStringLiteral("FILE SIZE"), detailTrackSize_);
        addDetailRow(QStringLiteral("FILE PATH"), detailTrackPath_);

        detailLayout->addStretch(1);
        detailScroll->setWidget(detailPanel);
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

            
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db directly
                  {
                      QString coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      }

                      if (QFile::exists(coreDbPath)) {
                          const QString connName = QStringLiteral("dj_core_duration_fix");
                          {
                              QSqlDatabase coreDb = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
                              coreDb.setDatabaseName(coreDbPath);
                              coreDb.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
                              int matchCore = 0;
                              if (coreDb.open()) {
                                  std::map<QString, size_t> pathIndex;
                                  for (size_t i = 0; i < allTracks_.size(); ++i) {
                                      pathIndex[QDir::fromNativeSeparators(allTracks_[i].filePath).trimmed().toLower()] = i;
                                  }
                                  
                                  QSqlQuery q(coreDb);
                                  q.setForwardOnly(true);
                                  if (q.exec(QStringLiteral("SELECT file_path, duration FROM tracks LIMIT 9999999"))) {
                                      while (q.next()) {
                                          QString fp = QDir::fromNativeSeparators(q.value(0).toString()).trimmed().toLower();
                                          double dur = q.value(1).toDouble();
                                          if (dur > 0) {
                                              auto it = pathIndex.find(fp);
                                              if (it != pathIndex.end()) {
                                                  TrackInfo& t = allTracks_[it->second];
                                                  if (t.durationMs <= 0 || t.durationStr == QStringLiteral("--:--") || t.durationStr.isEmpty()) {
                                                      t.durationMs = static_cast<qint64>(dur * 1000.0);
                                                      const int totalSec = static_cast<int>(dur);
                                                      t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                                                      matchCore++;
                                                  }
                                              }
                                          }
                                      }
                                      qInfo().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH matched=%1").arg(matchCore);
                                  } else {
                                      qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_QUERY ") << q.lastError().text();
                                  }
                                  coreDb.close();
                              } else {
                                  qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_OPEN ") << coreDb.lastError().text();
                              }
                          }
                          QSqlDatabase::removeDatabase(connName);
                      } else {
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND ") << coreDbPath;
                      }
                  }
                  

                  // Fallback: For any tracks without duration, estimate it from MP3 filesize
                  for (auto& t : allTracks_) {
                      if (t.durationMs <= 0 && t.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
                          QFile fFallback(t.filePath);
                          if (fFallback.open(QIODevice::ReadOnly)) {
                              QByteArray hdr = fFallback.read(10);
                              qint64 dataOff = 0;
                              if (hdr.size() == 10 && hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
                                  dataOff = 10 + ((quint32(static_cast<unsigned char>(hdr[6])) << 21)
                                               | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                                               | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                                               | quint32(static_cast<unsigned char>(hdr[9])));
                              }
                              fFallback.seek(dataOff);
                              QByteArray buf = fFallback.read(8192);
                              int bitrate = 0;
                              for (int i = 0; i < buf.size() - 3; ++i) {
                                  if ((static_cast<unsigned char>(buf[i]) == 0xFF) && ((static_cast<unsigned char>(buf[i+1]) & 0xE0) == 0xE0)) {
                                      int bitrateIdx = (static_cast<unsigned char>(buf[i+2]) >> 4) & 0x0F;
                                      int layer = (static_cast<unsigned char>(buf[i+1]) >> 1) & 0x03;
                                      int ver = (static_cast<unsigned char>(buf[i+1]) >> 3) & 0x03;
                                      static const int bitrates[2][3][16] = {
                                          { {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,32,48,56,64,80,96,112,128,144,160,176,192,224,256,0} },
                                          { {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0}, {0,32,48,56,64,80,96,112,128,160,192,224,256,320,384,0}, {0,32,64,96,128,160,192,224,256,288,320,352,384,416,448,0} }
                                      };
                                      int mpegIdx = (ver == 3) ? 1 : 0;
                                      int layerIdx = (layer == 1) ? 0 : ((layer == 2) ? 1 : 2);
                                      bitrate = bitrates[mpegIdx][layerIdx][bitrateIdx];
                                      break;
                                  }
                              }
                              if (bitrate <= 0) bitrate = 256;
                              t.durationMs = static_cast<qint64>(((fFallback.size() - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
                              int totalSec = static_cast<int>(t.durationMs / 1000);
                              t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                          }
                      }
                  }

djDb_.bulkInsert(allTracks_);

            
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db directly
                  {
                      QString coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      }

                      if (QFile::exists(coreDbPath)) {
                          const QString connName = QStringLiteral("dj_core_duration_fix");
                          {
                              QSqlDatabase coreDb = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
                              coreDb.setDatabaseName(coreDbPath);
                              coreDb.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
                              int matchCore = 0;
                              if (coreDb.open()) {
                                  std::map<QString, size_t> pathIndex;
                                  for (size_t i = 0; i < allTracks_.size(); ++i) {
                                      pathIndex[QDir::fromNativeSeparators(allTracks_[i].filePath).trimmed().toLower()] = i;
                                  }
                                  
                                  QSqlQuery q(coreDb);
                                  q.setForwardOnly(true);
                                  if (q.exec(QStringLiteral("SELECT file_path, duration FROM tracks LIMIT 9999999"))) {
                                      while (q.next()) {
                                          QString fp = QDir::fromNativeSeparators(q.value(0).toString()).trimmed().toLower();
                                          double dur = q.value(1).toDouble();
                                          if (dur > 0) {
                                              auto it = pathIndex.find(fp);
                                              if (it != pathIndex.end()) {
                                                  TrackInfo& t = allTracks_[it->second];
                                                  if (t.durationMs <= 0 || t.durationStr == QStringLiteral("--:--") || t.durationStr.isEmpty()) {
                                                      t.durationMs = static_cast<qint64>(dur * 1000.0);
                                                      const int totalSec = static_cast<int>(dur);
                                                      t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                                                      matchCore++;
                                                  }
                                              }
                                          }
                                      }
                                      qInfo().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH matched=%1").arg(matchCore);
                                  } else {
                                      qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_QUERY ") << q.lastError().text();
                                  }
                                  coreDb.close();
                              } else {
                                  qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_OPEN ") << coreDb.lastError().text();
                              }
                          }
                          QSqlDatabase::removeDatabase(connName);
                      } else {
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND ") << coreDbPath;
                      }
                  }
                  

                  // Fallback: For any tracks without duration, estimate it from MP3 filesize
                  for (auto& t : allTracks_) {
                      if (t.durationMs <= 0 && t.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
                          QFile fFallback(t.filePath);
                          if (fFallback.open(QIODevice::ReadOnly)) {
                              QByteArray hdr = fFallback.read(10);
                              qint64 dataOff = 0;
                              if (hdr.size() == 10 && hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
                                  dataOff = 10 + ((quint32(static_cast<unsigned char>(hdr[6])) << 21)
                                               | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                                               | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                                               | quint32(static_cast<unsigned char>(hdr[9])));
                              }
                              fFallback.seek(dataOff);
                              QByteArray buf = fFallback.read(8192);
                              int bitrate = 0;
                              for (int i = 0; i < buf.size() - 3; ++i) {
                                  if ((static_cast<unsigned char>(buf[i]) == 0xFF) && ((static_cast<unsigned char>(buf[i+1]) & 0xE0) == 0xE0)) {
                                      int bitrateIdx = (static_cast<unsigned char>(buf[i+2]) >> 4) & 0x0F;
                                      int layer = (static_cast<unsigned char>(buf[i+1]) >> 1) & 0x03;
                                      int ver = (static_cast<unsigned char>(buf[i+1]) >> 3) & 0x03;
                                      static const int bitrates[2][3][16] = {
                                          { {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,32,48,56,64,80,96,112,128,144,160,176,192,224,256,0} },
                                          { {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0}, {0,32,48,56,64,80,96,112,128,160,192,224,256,320,384,0}, {0,32,64,96,128,160,192,224,256,288,320,352,384,416,448,0} }
                                      };
                                      int mpegIdx = (ver == 3) ? 1 : 0;
                                      int layerIdx = (layer == 1) ? 0 : ((layer == 2) ? 1 : 2);
                                      bitrate = bitrates[mpegIdx][layerIdx][bitrateIdx];
                                      break;
                                  }
                              }
                              if (bitrate <= 0) bitrate = 256;
                              t.durationMs = static_cast<qint64>(((fFallback.size() - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
                              int totalSec = static_cast<int>(t.durationMs / 1000);
                              t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                          }
                      }
                  }

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

            
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db directly
                  {
                      QString coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      }

                      if (QFile::exists(coreDbPath)) {
                          const QString connName = QStringLiteral("dj_core_duration_fix");
                          {
                              QSqlDatabase coreDb = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
                              coreDb.setDatabaseName(coreDbPath);
                              coreDb.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
                              int matchCore = 0;
                              if (coreDb.open()) {
                                  std::map<QString, size_t> pathIndex;
                                  for (size_t i = 0; i < allTracks_.size(); ++i) {
                                      pathIndex[QDir::fromNativeSeparators(allTracks_[i].filePath).trimmed().toLower()] = i;
                                  }
                                  
                                  QSqlQuery q(coreDb);
                                  q.setForwardOnly(true);
                                  if (q.exec(QStringLiteral("SELECT file_path, duration FROM tracks LIMIT 9999999"))) {
                                      while (q.next()) {
                                          QString fp = QDir::fromNativeSeparators(q.value(0).toString()).trimmed().toLower();
                                          double dur = q.value(1).toDouble();
                                          if (dur > 0) {
                                              auto it = pathIndex.find(fp);
                                              if (it != pathIndex.end()) {
                                                  TrackInfo& t = allTracks_[it->second];
                                                  if (t.durationMs <= 0 || t.durationStr == QStringLiteral("--:--") || t.durationStr.isEmpty()) {
                                                      t.durationMs = static_cast<qint64>(dur * 1000.0);
                                                      const int totalSec = static_cast<int>(dur);
                                                      t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                                                      matchCore++;
                                                  }
                                              }
                                          }
                                      }
                                      qInfo().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH matched=%1").arg(matchCore);
                                  } else {
                                      qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_QUERY ") << q.lastError().text();
                                  }
                                  coreDb.close();
                              } else {
                                  qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_OPEN ") << coreDb.lastError().text();
                              }
                          }
                          QSqlDatabase::removeDatabase(connName);
                      } else {
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND ") << coreDbPath;
                      }
                  }
                  

                  // Fallback: For any tracks without duration, estimate it from MP3 filesize
                  for (auto& t : allTracks_) {
                      if (t.durationMs <= 0 && t.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) {
                          QFile fFallback(t.filePath);
                          if (fFallback.open(QIODevice::ReadOnly)) {
                              QByteArray hdr = fFallback.read(10);
                              qint64 dataOff = 0;
                              if (hdr.size() == 10 && hdr[0] == 'I' && hdr[1] == 'D' && hdr[2] == '3') {
                                  dataOff = 10 + ((quint32(static_cast<unsigned char>(hdr[6])) << 21)
                                               | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                                               | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                                               | quint32(static_cast<unsigned char>(hdr[9])));
                              }
                              fFallback.seek(dataOff);
                              QByteArray buf = fFallback.read(8192);
                              int bitrate = 0;
                              for (int i = 0; i < buf.size() - 3; ++i) {
                                  if ((static_cast<unsigned char>(buf[i]) == 0xFF) && ((static_cast<unsigned char>(buf[i+1]) & 0xE0) == 0xE0)) {
                                      int bitrateIdx = (static_cast<unsigned char>(buf[i+2]) >> 4) & 0x0F;
                                      int layer = (static_cast<unsigned char>(buf[i+1]) >> 1) & 0x03;
                                      int ver = (static_cast<unsigned char>(buf[i+1]) >> 3) & 0x03;
                                      static const int bitrates[2][3][16] = {
                                          { {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,8,16,24,32,40,48,56,64,80,96,112,128,144,160,0}, {0,32,48,56,64,80,96,112,128,144,160,176,192,224,256,0} },
                                          { {0,32,40,48,56,64,80,96,112,128,160,192,224,256,320,0}, {0,32,48,56,64,80,96,112,128,160,192,224,256,320,384,0}, {0,32,64,96,128,160,192,224,256,288,320,352,384,416,448,0} }
                                      };
                                      int mpegIdx = (ver == 3) ? 1 : 0;
                                      int layerIdx = (layer == 1) ? 0 : ((layer == 2) ? 1 : 2);
                                      bitrate = bitrates[mpegIdx][layerIdx][bitrateIdx];
                                      break;
                                  }
                              }
                              if (bitrate <= 0) bitrate = 256;
                              t.durationMs = static_cast<qint64>(((fFallback.size() - dataOff) * 8.0) / (bitrate * 1000.0) * 1000.0);
                              int totalSec = static_cast<int>(t.durationMs / 1000);
                              t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                          }
                      }
                  }

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
            currentTrackIndex_ = trackIdx;
            qInfo().noquote() << QStringLiteral("TRACK_SELECTED=%1").arg(allTracks_[trackIdx].displayName);
            rebuildPlayerQueue();
            loadAndPlayTrack(trackIdx);
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
                currentTrackIndex_ = trackIdx;
                rebuildPlayerQueue();
                loadAndPlayTrack(trackIdx);
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
                currentTrackIndex_ = idx;
                rebuildPlayerQueue();
                loadAndPlayTrack(idx);
                qInfo().noquote() << QStringLiteral("PLAY_ALL_START=%1").arg(allTracks_[idx].displayName);
                stack_->setCurrentIndex(2);
            }
        });

        // ── Now Playing button — scroll to current track ──
        QObject::connect(nowPlayingBtn, &QPushButton::clicked, this, [this]() {
            if (currentTrackIndex_ < 0 || currentTrackIndex_ >= static_cast<int>(allTracks_.size())) return;
            libraryTree_->scrollToTrackId(static_cast<qint64>(currentTrackIndex_));
            qInfo().noquote() << QStringLiteral("NOW_PLAYING_SCROLL=%1").arg(allTracks_[currentTrackIndex_].displayName);
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
            rebuildPlayerQueue();
            stack_->setCurrentIndex(2);
        });
        QObject::connect(djBtn, &QPushButton::clicked, this, [this]() {
            bridge_.enterDjMode();
            populateDjLibraryTrees();
            stack_->setCurrentIndex(3);
        });

        return page;
    }

    QWidget* buildPlayerPage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral(
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

        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(24, 16, 24, 16);
        layout->setSpacing(0);

        // ═══════════════════════════════════════════════════
        // A. Header row: Back + Title + Audio Profile
        // ═══════════════════════════════════════════════════
        auto* headerRow = new QHBoxLayout();
        headerRow->setSpacing(10);
        auto* backBtn = new QPushButton(QStringLiteral("<< Library"), page);
        backBtn->setMinimumHeight(34);
        backBtn->setCursor(Qt::PointingHandCursor);
        backBtn->setToolTip(QStringLiteral("Return to the library browser"));

        auto* titleLabel = new QLabel(QStringLiteral("Simple Player"), page);
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

        auto* profileLabel = new QLabel(QStringLiteral("Profile:"), page);
        profileLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
        audioProfileCombo_ = new QComboBox(page);
        audioProfileCombo_->setMinimumWidth(180);
        refreshAudioProfilesButton_ = new QPushButton(QStringLiteral("Refresh"), page);
        applyAudioProfileButton_ = new QPushButton(QStringLiteral("Apply"), page);
        headerRow->addWidget(profileLabel);
        headerRow->addWidget(audioProfileCombo_);
        headerRow->addWidget(refreshAudioProfilesButton_);
        headerRow->addWidget(applyAudioProfileButton_);
        layout->addLayout(headerRow);

        QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
            bridge_.leaveSimpleMode();
            stack_->setCurrentIndex(1);
        });
        QObject::connect(refreshAudioProfilesButton_, &QPushButton::clicked, this, [this]() {
            requestAudioProfilesRefresh(true);
        });
        QObject::connect(applyAudioProfileButton_, &QPushButton::clicked, this, &MainWindow::applySelectedAudioProfile);
        requestAudioProfilesRefresh(true);

        const QString akApplyAutorun = qEnvironmentVariable("NGKS_AK_AUTORUN_APPLY").trimmed().toLower();
        if (akApplyAutorun == QStringLiteral("1") || akApplyAutorun == QStringLiteral("true") || akApplyAutorun == QStringLiteral("yes")) {
            QTimer::singleShot(200, this, &MainWindow::applySelectedAudioProfile);
        }

        layout->addSpacing(14);

        // ═══════════════════════════════════════════════════
        // B. Hero / Now Playing panel with Visualizer
        //    Visualizer is the BACKGROUND layer; text overlays on top
        // ═══════════════════════════════════════════════════
        auto* heroFrame = new QFrame(page);
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

        playerTimeLabel_ = new QLabel(QStringLiteral("0:00"), page);
        {
            QFont f = playerTimeLabel_->font();
            f.setPointSize(11);
            playerTimeLabel_->setFont(f);
        }
        playerTimeLabel_->setStyleSheet(QStringLiteral("color: #aaaaaa;"));
        playerTimeLabel_->setMinimumWidth(42);
        playerTimeLabel_->setAlignment(Qt::AlignRight | Qt::AlignVCenter);

        seekSlider_ = new QSlider(Qt::Horizontal, page);
        seekSlider_->setRange(0, 1);
        seekSlider_->setMinimumHeight(24);

        playerTimeTotalLabel_ = new QLabel(QStringLiteral("0:00"), page);
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
        auto* transportLeftSpacer = new QWidget(page);
        transportLeftSpacer->setFixedSize(160, 1);
        transportLeftSpacer->setStyleSheet(QStringLiteral("background: transparent;"));
        transportRow->addWidget(transportLeftSpacer);

        transportRow->addStretch(1);

        prevBtn_ = new QPushButton(QStringLiteral("|<  Prev"), page);
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

        playPauseBtn_ = new QPushButton(QStringLiteral("Play"), page);
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

        nextBtn_ = new QPushButton(QStringLiteral("Next  >|"), page);
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
        playModeBtn_ = new QPushButton(QStringLiteral("Mode: In Order"), page);
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

        auto* volLabel = new QLabel(QStringLiteral("Vol:"), page);
        {
            QFont f = volLabel->font();
            f.setPointSize(11);
            volLabel->setFont(f);
        }
        volLabel->setStyleSheet(QStringLiteral("color: #aaaaaa;"));

        volumeSlider_ = new QSlider(Qt::Horizontal, page);
        volumeSlider_->setRange(0, 100);
        volumeSlider_->setValue(80);
        volumeSlider_->setMinimumWidth(180);
        volumeSlider_->setMaximumWidth(300);

        auto* volPercent = new QLabel(QStringLiteral("80%"), page);
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
        eqPanel_ = new EqPanel(&bridge_, page);
        layout->addWidget(eqPanel_);

        layout->addSpacing(14);

        // ═══════════════════════════════════════════════════
        // E. Library browser: search + sort + column tree
        // ═══════════════════════════════════════════════════
        auto* libHeaderRow = new QHBoxLayout();
        libHeaderRow->setSpacing(8);

        auto* libLabel = new QLabel(QStringLiteral("Library"), page);
        {
            QFont f = libLabel->font();
            f.setPointSize(13);
            f.setBold(true);
            libLabel->setFont(f);
        }
        libHeaderRow->addWidget(libLabel);

        libHeaderRow->addSpacing(12);

        playerSearchBar_ = new QLineEdit(page);
        playerSearchBar_->setPlaceholderText(QStringLiteral("Search tracks..."));
        playerSearchBar_->setClearButtonEnabled(true);
        playerSearchBar_->setMinimumHeight(28);
        playerSearchBar_->setStyleSheet(QStringLiteral(
            "QLineEdit { background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460;"
            "  border-radius: 4px; padding: 4px 8px; font-size: 12px; }"
            "QLineEdit:focus { border-color: #e94560; }"));
        libHeaderRow->addWidget(playerSearchBar_, 1);

        libHeaderRow->addSpacing(8);

        auto* sortLabel = new QLabel(QStringLiteral("Sort:"), page);
        sortLabel->setStyleSheet(QStringLiteral("color: #888; font-size: 12px;"));
        libHeaderRow->addWidget(sortLabel);

        playerSortCombo_ = new QComboBox(page);
        playerSortCombo_->addItems({
            QStringLiteral("Title"), QStringLiteral("Artist"), QStringLiteral("Album"),
            QStringLiteral("Duration"), QStringLiteral("BPM"), QStringLiteral("Key")
        });
        playerSortCombo_->setMinimumWidth(90);
        libHeaderRow->addWidget(playerSortCombo_);

        libHeaderRow->addSpacing(8);

        auto* libCountLabel = new QLabel(QStringLiteral("0 tracks"), page);
        libCountLabel->setStyleSheet(QStringLiteral("color: #666; font-size: 11px;"));
        libHeaderRow->addWidget(libCountLabel);

        layout->addLayout(libHeaderRow);
        layout->addSpacing(4);

        playerLibraryTree_ = new DjLibraryWidget(page);
        playerLibraryTree_->setDatabase(&djDb_);
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
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"));
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
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"))
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
                .arg(currentTrackIndex_ >= 0 && currentTrackIndex_ < static_cast<int>(allTracks_.size())
                     ? allTracks_[currentTrackIndex_].displayName : QStringLiteral("?"));
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
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
                currentTrackIndex_ = idx;
                loadAndPlayTrack(idx);
                qInfo().noquote() << QStringLiteral("PLAYER_LIB_PLAY=%1").arg(allTracks_[idx].displayName);
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

        return page;
    }

    QWidget* buildDjModePage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral("background: #080b10;"));
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(8, 8, 8, 8);
        layout->setSpacing(4);

        // ── Header row: Back + title ──
        auto* headerRow = new QHBoxLayout();
        headerRow->setSpacing(6);
        auto* backBtn = new QPushButton(QStringLiteral("\u2190 Back"), page);
        backBtn->setCursor(Qt::PointingHandCursor);
        backBtn->setStyleSheet(QStringLiteral(
            "QPushButton { background: rgba(20,20,30,200); border: 1px solid #333;"
            "  border-radius: 4px; color: #aaa; font-size: 9px; padding: 4px 10px; }"
            "QPushButton:hover { background: rgba(40,40,60,220); color: #ddd; }"));
        headerRow->addWidget(backBtn);

        auto* title = new QLabel(QStringLiteral("DJ MIXER"), page);
        {
            QFont f = title->font();
            f.setPointSize(14);
            f.setBold(true);
            title->setFont(f);
        }
        title->setStyleSheet(QStringLiteral("color: #e0e0e0; background: transparent;"));
        title->setAlignment(Qt::AlignCenter);
        headerRow->addWidget(title, 1);

        headerRow->addSpacing(60);  // balance the back button
        layout->addLayout(headerRow);

        QObject::connect(backBtn, &QPushButton::clicked, this, [this]() {
            bridge_.leaveDjMode();
            stack_->setCurrentIndex(1);
        });

        // ── Per-deck columns: Deck + Library side by side ──
        auto* deckRow = new QHBoxLayout();
        deckRow->setSpacing(6);

        // ── Deck A column: strip + library ──
        auto* colA = new QVBoxLayout();
        colA->setSpacing(4);
        djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, page);
        colA->addWidget(djDeckA_, 1);

        // ── DJ Library A placeholder (library not initialized) ──
        auto* libPlaceholderA = new QLabel(QStringLiteral("DJ Library not initialized"), page);
        libPlaceholderA->setAlignment(Qt::AlignCenter);
        libPlaceholderA->setFixedHeight(100);
        libPlaceholderA->setStyleSheet(QStringLiteral(
            "color: #3a3a3a; background: #0a0c12; border: 1px solid #1a1e28;"
            " font-size: 8pt; border-radius: 3px;"));
        /* colA->addWidget(libPlaceholderA); */
        colA->addStretch();
        deckRow->addLayout(colA, 5);

        // ── Master section column (center) ──
        auto* masterCol = new QVBoxLayout();
        masterCol->setSpacing(4);
        masterCol->setContentsMargins(4, 0, 4, 0);

        auto* masterLabel = new QLabel(QStringLiteral("MASTER"), page);
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
        djMasterMeterL_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), page);
        djMasterMeterR_ = new LevelMeter(QColor(0xc0, 0xc0, 0xc0), page);
        masterMeterRow->addWidget(djMasterMeterL_);
        masterMeterRow->addWidget(djMasterMeterR_);
        masterMeterRow->addStretch();
        masterCol->addLayout(masterMeterRow, 1);

        // CUE MIX label + slider (horizontal)
        auto* cueMixLabel = new QLabel(QStringLiteral("CUE MIX"), page);
        {
            QFont f = cueMixLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            cueMixLabel->setFont(f);
        }
        cueMixLabel->setAlignment(Qt::AlignCenter);
        cueMixLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(cueMixLabel);

        djCueMix_ = new QSlider(Qt::Horizontal, page);
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
        auto* cueVolLabel = new QLabel(QStringLiteral("CUE VOL"), page);
        {
            QFont f = cueVolLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            cueVolLabel->setFont(f);
        }
        cueVolLabel->setAlignment(Qt::AlignCenter);
        cueVolLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(cueVolLabel);

        djCueVol_ = new QSlider(Qt::Horizontal, page);
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
        auto* outModeLabel = new QLabel(QStringLiteral("OUTPUT"), page);
        {
            QFont f = outModeLabel->font(); f.setPointSizeF(6.5); f.setBold(true);
            outModeLabel->setFont(f);
        }
        outModeLabel->setAlignment(Qt::AlignCenter);
        outModeLabel->setStyleSheet(QStringLiteral(
            "color: #aaa; background: transparent; padding: 1px 0;"));
        masterCol->addWidget(outModeLabel);

        djOutputModeBtn_ = new QPushButton(QStringLiteral("Stereo"), page);
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
        djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, page);
        colB->addWidget(djDeckB_, 1);

        // ── DJ Library B placeholder (library not initialized) ──
        auto* libPlaceholderB = new QLabel(QStringLiteral("DJ Library not initialized"), page);
        libPlaceholderB->setAlignment(Qt::AlignCenter);
        libPlaceholderB->setFixedHeight(100);
        libPlaceholderB->setStyleSheet(QStringLiteral(
            "color: #3a3a3a; background: #0a0c12; border: 1px solid #1a1e28;"
            " font-size: 8pt; border-radius: 3px;"));
        /* colB->addWidget(libPlaceholderB); */
        colB->addStretch();
        deckRow->addLayout(colB, 5);

        auto* deckSplitter = new QSplitter(Qt::Horizontal, page);
        auto* djBrowser = new DjBrowserPane(page);
        djBrowser->setDatabase(&djDb_);
        deckSplitter->addWidget(djBrowser);

        QObject::connect(djBrowser, &DjBrowserPane::requestLoadDeck, this, [this](const QString& path, int deck) {
            if (deck == 1 && djDeckA_) djDeckA_->loadTrack(path);
            else if (deck == 2 && djDeckB_) djDeckB_->loadTrack(path);
        });

        QObject::connect(djBrowser, &DjBrowserPane::requestAnalyze, this, [](const QString& path, bool live) {
            qInfo() << "Requested offline/Live analyze:" << path << live;
        });

        QObject::connect(djBrowser, &DjBrowserPane::requestEnqueue, this, [](const QString& path) {
            qInfo() << "Enqueue requested (stub):" << path;
        });

        auto* deckWidget = new QWidget(page);
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

        auto* xfadeLabel = new QLabel(QStringLiteral("A"), page);
        {
            QFont f = xfadeLabel->font(); f.setPointSize(12); f.setBold(true);
            xfadeLabel->setFont(f);
        }
        xfadeLabel->setStyleSheet(QStringLiteral(
            "color: #e07020; background: transparent;"));
        xfadeRow->addWidget(xfadeLabel);

        djCrossfader_ = new QSlider(Qt::Horizontal, page);
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

        auto* xfadeLabelB = new QLabel(QStringLiteral("B"), page);
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

        // ── Audio profile applied result (async) ──
        QObject::connect(&bridge_, &EngineBridge::audioProfileApplied, this,
            &MainWindow::onAudioProfileApplied);

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
            const int idx = static_cast<int>(trackId);    
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size()))
                loadAndPlayTrack(idx);
        });
        QObject::connect(djDeckB_, &DeckStrip::loadTrackRequested, this,
            [this](int /*deckIndex*/, qint64 trackId) {   
            const int idx = static_cast<int>(trackId);    
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size()))
                loadAndPlayTrack(idx);
        });

        // ── DeckStrip drag-to-deck: load file path ──
        QObject::connect(djDeckA_, &DeckStrip::loadFileRequested, this,
            [this](int deckIndex, const QString& path) {   
            bridge_.loadTrackToDeck(deckIndex, path);
        });
        QObject::connect(djDeckB_, &DeckStrip::loadFileRequested, this,
            [this](int deckIndex, const QString& path) {   
            bridge_.loadTrackToDeck(deckIndex, path);
        });
        QObject::connect(djDeckB_, &DeckStrip::loadTrackRequested, this,
            [this](int /*deckIndex*/, qint64 trackId) {
            const int idx = static_cast<int>(trackId);
            if (idx >= 0 && idx < static_cast<int>(allTracks_.size()))
                loadAndPlayTrack(idx);
        });

        // Wire snapshot refresh
        QObject::connect(&bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
            if (djDeckA_) djDeckA_->refreshFromSnapshot();
            if (djDeckB_) djDeckB_->refreshFromSnapshot();
            if (djMasterMeterL_) djMasterMeterL_->setLevel(static_cast<float>(bridge_.masterPeakL()));
            if (djMasterMeterR_) djMasterMeterR_->setLevel(static_cast<float>(bridge_.masterPeakR()));
        });

        // ── Device-lost overlay banner + Recover Audio button ──
        djDeviceLostBanner_ = new QWidget(page);
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

        return page;
    }

    /// DJ library is offline — stub to prevent call-site errors.
    void populateDjLibraryTrees() { /* library not initialized */ }

    /// DJ library is offline — stub to prevent call-site errors.
    void refreshDjLibraryHighlights() { /* library not initialized */ }

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
        if (!detailTitleLabel_) return; // guard against pre-init calls
        const TrackInfo& t = allTracks_[trackIndex];
        detailTitleLabel_->setText(t.title.isEmpty() ? QStringLiteral("Unknown Track") : t.title);
        detailTrackTitle_->setText(t.title.isEmpty() ? QStringLiteral("-") : t.title);
        detailTrackArtist_->setText(t.artist.isEmpty() ? QStringLiteral("-") : t.artist);
        detailTrackAlbum_->setText(t.album.isEmpty() ? QStringLiteral("-") : t.album);
        detailTrackGenre_->setText(t.genre.isEmpty() ? QStringLiteral("-") : t.genre);
        detailTrackDuration_->setText(t.durationStr.isEmpty() ? QStringLiteral("--:--") : t.durationStr);
        detailTrackBpm_->setText(t.bpm.isEmpty() ? QStringLiteral("-") : t.bpm);
        detailTrackKey_->setText(t.musicalKey.isEmpty() ? QStringLiteral("-") : t.musicalKey);
        detailTrackCamelot_->setText(t.camelotKey.isEmpty() ? QStringLiteral("-") : t.camelotKey);
        detailTrackEnergy_->setText(t.energy >= 0 ? QString::number(t.energy, 'f', 1) : QStringLiteral("-"));
        detailTrackLufs_->setText(t.loudnessLUFS != 0.0
            ? QStringLiteral("%1 LUFS (range %2)").arg(QString::number(t.loudnessLUFS, 'f', 1), QString::number(t.loudnessRange, 'f', 1))
            : QStringLiteral("-"));
        {
            QString cueStr;
            if (!t.cueIn.isEmpty() || !t.cueOut.isEmpty())
                cueStr = QStringLiteral("%1 / %2").arg(t.cueIn.isEmpty() ? QStringLiteral("-") : t.cueIn,
                                                        t.cueOut.isEmpty() ? QStringLiteral("-") : t.cueOut);
            detailTrackCue_->setText(cueStr.isEmpty() ? QStringLiteral("-") : cueStr);
        }
        detailTrackDance_->setText(t.danceability >= 0 ? QString::number(t.danceability, 'f', 1) : QStringLiteral("-"));
        detailTrackSize_->setText(t.fileSize > 0 ? formatFileSize(t.fileSize) : QStringLiteral("-"));
        detailTrackPath_->setText(t.filePath);
    }

    void clearTrackDetail()
    {
        if (!detailTitleLabel_) return;
        detailTitleLabel_->setText(QStringLiteral("Track Info"));
        detailTrackTitle_->setText(QStringLiteral("-"));
        detailTrackArtist_->setText(QStringLiteral("-"));
        detailTrackAlbum_->setText(QStringLiteral("-"));
        detailTrackGenre_->setText(QStringLiteral("-"));
        detailTrackDuration_->setText(QStringLiteral("--:--"));
        detailTrackBpm_->setText(QStringLiteral("-"));
        detailTrackKey_->setText(QStringLiteral("-"));
        detailTrackCamelot_->setText(QStringLiteral("-"));
        detailTrackEnergy_->setText(QStringLiteral("-"));
        detailTrackLufs_->setText(QStringLiteral("-"));
        detailTrackCue_->setText(QStringLiteral("-"));
        detailTrackDance_->setText(QStringLiteral("-"));
        detailTrackSize_->setText(QStringLiteral("-"));
        detailTrackPath_->setText(QStringLiteral("-"));
    }

    void loadAndPlayTrack(int trackIndex)
    {
        if (trackIndex < 0 || trackIndex >= static_cast<int>(allTracks_.size())) return;
        currentTrackIndex_ = trackIndex;
        const TrackInfo& track = allTracks_[trackIndex];

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

    void playNextTrack()
    {
        if (allTracks_.empty() || !playerLibraryTree_) return;
        const int count = playerLibraryTree_->totalFilteredCount();
        if (count == 0) return;

        if (playMode_ == PlayMode::Shuffle) {
            // Pure random from visible list
            std::uniform_int_distribution<int> dist(0, count - 1);
            const int ri = dist(shuffleRng_);
            const int idx = (int)playerLibraryTree_->trackIdAt(ri);
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SHUFFLE %1").arg(allTracks_[idx].displayName);
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
        if (nextIdx >= 0 && nextIdx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(nextIdx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=%1").arg(allTracks_[nextIdx].displayName);
        }
    }

    void playPrevTrack()
    {
        if (allTracks_.empty() || !playerLibraryTree_) return;
        const int count = playerLibraryTree_->totalFilteredCount();
        if (count == 0) return;

        if (playMode_ == PlayMode::Shuffle || playMode_ == PlayMode::SmartShuffle) {
            // In shuffle modes, prev picks random (no history)
            std::uniform_int_distribution<int> dist(0, count - 1);
            const int ri = dist(shuffleRng_);
            const int idx = (int)playerLibraryTree_->trackIdAt(ri);
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=SHUFFLE %1").arg(allTracks_[idx].displayName);
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
        if (prevIdx >= 0 && prevIdx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(prevIdx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_PREV=%1").arg(allTracks_[prevIdx].displayName);
        }
    }

    void onEndOfMedia()
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

    QString playModeLabel() const
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

    void updatePlayModeButton()
    {
        if (playModeBtn_)
            playModeBtn_->setText(QStringLiteral("Mode: %1").arg(playModeLabel()));
    }

    void updateUpNextLabel()
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

        if (nextIdx >= 0 && nextIdx < static_cast<int>(allTracks_.size())) {
            const auto& t = allTracks_[nextIdx];
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

    void rebuildSmartShufflePool()
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

    void advanceSmartShuffle()
    {
        if (smartShufflePool_.empty() || smartShufflePos_ >= static_cast<int>(smartShufflePool_.size())) {
            rebuildSmartShufflePool();
        }
        if (smartShufflePool_.empty()) return;
        const int idx = smartShufflePool_[smartShufflePos_];
        ++smartShufflePos_;
        if (idx >= 0 && idx < static_cast<int>(allTracks_.size())) {
            loadAndPlayTrack(idx);
            qInfo().noquote() << QStringLiteral("TRANSPORT_NEXT=SMART_SHUFFLE %1 pos=%2/%3")
                .arg(allTracks_[idx].displayName).arg(smartShufflePos_).arg(smartShufflePool_.size());
        }
    }

    void rebuildPlayerQueue()
    {
        refreshPlayerLibrary();
    }

    void refreshPlayerLibrary()
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

    void highlightPlayerLibraryItem(int trackIndex)
    {
        if (!playerLibraryTree_ || trackIndex < 0) return;
        playerLibraryTree_->setCurrentTrackId(static_cast<qint64>(trackIndex));
        playerLibraryTree_->scrollToTrackId(static_cast<qint64>(trackIndex));
    }

    void requestAudioProfilesRefresh(bool logMarker)
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

    void refreshAudioProfilesUi(bool logMarker)
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
            if (logMarker && diagnosticsDialog_ != nullptr) {
                diagnosticsDialog_->refreshLogTail();
            }
            return;
        }

        if (logMarker || lastAkActiveProfileMarker_ != audioProfilesStore_.activeProfile) {
            qInfo().noquote() << QStringLiteral("RTAudioAKActiveProfile=%1").arg(audioProfilesStore_.activeProfile);
            lastAkActiveProfileMarker_ = audioProfilesStore_.activeProfile;
        }
    }

    void applySelectedAudioProfile()
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
        const bool hadOpenDevice = lastTelemetry_.rtDeviceOpenOk;
        qInfo().noquote() << QStringLiteral("RTAudioALDeviceReopen=%1").arg(hadOpenDevice ? QStringLiteral("TRUE") : QStringLiteral("FALSE"));

        pendingApplyProfileName_ = profileName;

        const UiAudioProfile& profile = profileIt->second;
        bridge_.applyAudioProfile(profile.deviceId.toStdString(),
                                  profile.deviceName.toStdString(),
                                  profile.sampleRate,
                                  profile.bufferFrames,
                                  profile.channelsOut);
        // Result arrives via audioProfileApplied signal → onAudioProfileApplied()
    }

    void onAudioProfileApplied(bool ok)
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

    void finishAudioApply()
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
        if (visualizer_) {
            const float feedLevel = static_cast<float>(std::max(meterL, meterR));
            if (!meterDiagLogged_ && feedLevel > 0.0f) {
                qInfo().noquote() << QStringLiteral("DIAG_METER_FEED: L=%1 R=%2 feed=%3")
                    .arg(QString::number(meterL, 'f', 6),
                         QString::number(meterR, 'f', 6),
                         QString::number(feedLevel, 'f', 6));
                meterDiagLogged_ = true;
            }
            visualizer_->setAudioLevel(feedLevel);
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
    // Detail panel labels
    QLabel* detailTitleLabel_{nullptr};
    QLabel* detailTrackTitle_{nullptr};
    QLabel* detailTrackArtist_{nullptr};
    QLabel* detailTrackAlbum_{nullptr};
    QLabel* detailTrackGenre_{nullptr};
    QLabel* detailTrackDuration_{nullptr};
    QLabel* detailTrackBpm_{nullptr};
    QLabel* detailTrackKey_{nullptr};
    QLabel* detailTrackCamelot_{nullptr};
    QLabel* detailTrackEnergy_{nullptr};
    QLabel* detailTrackLufs_{nullptr};
    QLabel* detailTrackCue_{nullptr};
    QLabel* detailTrackDance_{nullptr};
    QLabel* detailTrackSize_{nullptr};
    QLabel* detailTrackPath_{nullptr};
    QLabel* playerTrackLabel_{nullptr};
    QLabel* nowPlayingTag_{nullptr};
    double titlePulseEnvelope_{0.0};
    QLabel* playerArtistLabel_{nullptr};
    QLabel* playerMetaLabel_{nullptr};
    QLabel* playerStateLabel_{nullptr};
    QLabel* playerTimeLabel_{nullptr};
    QLabel* playerTimeTotalLabel_{nullptr};
    QSlider* seekSlider_{nullptr};
    QSlider* volumeSlider_{nullptr};
    QPushButton* playPauseBtn_{nullptr};
    QPushButton* prevBtn_{nullptr};
    QPushButton* nextBtn_{nullptr};
    DjLibraryWidget* playerLibraryTree_{nullptr};
    QLabel* playerLibCountLabel_{nullptr};
    QLineEdit* playerSearchBar_{nullptr};
    QComboBox* playerSortCombo_{nullptr};
    DjLibraryDatabase djDb_;
    std::vector<TrackInfo> allTracks_;
    bool juceSimpleModeReady_{false};
    std::vector<Playlist> playlists_;
    int activePlaylistIndex_{-1}; // -1 = show all library
    QString importedFolderPath_;
    int currentTrackIndex_{-1};
    bool seekSliderPressed_{false};
    uint64_t uiTrackGen_{0};    // must match bridge_.currentLoadGen() for UI updates

    // Play mode
    enum class PlayMode { PlayOnce, PlayInOrder, RepeatAll, Shuffle, SmartShuffle };
    PlayMode playMode_{PlayMode::PlayInOrder};
    std::vector<int> smartShufflePool_;
    int smartShufflePos_{-1};
    std::mt19937 shuffleRng_{std::random_device{}()};
    QPushButton* playModeBtn_{nullptr};

    // Visualizer / display surface
    VisualizerWidget* visualizer_{nullptr};
    QTimer* vizTimer_{nullptr};

    QPushButton* pulseBtn_{nullptr};
    QPushButton* tuneBtn_{nullptr};
    QPushButton* vizLineBtn_{nullptr};
    QPushButton* vizBarsBtn_{nullptr};
    QPushButton* vizCircleBtn_{nullptr};
    QPushButton* vizNoneBtn_{nullptr};
    QLabel* upNextLabel_{nullptr};

    // 16-band EQ panel
    EqPanel* eqPanel_{nullptr};

    // DJ mode widgets
    DeckStrip* djDeckA_{nullptr};
    DeckStrip* djDeckB_{nullptr};
    QSlider* djCrossfader_{nullptr};
    // djLibTreeA_/B_ removed — library stripped for rebuild
    LevelMeter* djMasterMeterL_{nullptr};
    LevelMeter* djMasterMeterR_{nullptr};
    QSlider* djCueMix_{nullptr};
    QSlider* djCueVol_{nullptr};
    QPushButton* djOutputModeBtn_{nullptr};
    QWidget* djDeviceLostBanner_{nullptr};
    QLabel* djBannerTitleLabel_{nullptr};
    QLabel* djRecoveryStatusLabel_{nullptr};
    QPushButton* djRecoverBtn_{nullptr};
    QTimer* djBannerDismissTimer_{nullptr};
    // djLibHighlight* removed — library stripped for rebuild
    int lastActiveDjDeck_{0};

    QLabel* engineStatusLabel_{nullptr};
    QLabel* runningLabel_{nullptr};
    QLabel* meterLabel_{nullptr};
    QComboBox* audioProfileCombo_{nullptr};
    QPushButton* refreshAudioProfilesButton_{nullptr};
    QPushButton* applyAudioProfileButton_{nullptr};
    UIStatus lastStatus_ {};
    UIHealthSnapshot lastHealth_ {};
    UIEngineTelemetrySnapshot lastTelemetry_ {};
    UIFoundationSnapshot lastFoundation_ {};
    UISelfTestSnapshot lastSelfTests_ {};
    UiAudioProfilesStore audioProfilesStore_ {};
    bool selfTestsRan_{false};
    bool selfTestAutorun_{false};
    bool rtProbeAutorun_{false};
    bool statusTickLogged_{false};
    bool meterDiagLogged_{false};
    bool healthTickLogged_{false};
    bool telemetryTickLogged_{false};
    bool foundationTickLogged_{false};
    bool foundationSelfTestLogged_{false};
    std::atomic<bool> audioApplyInProgress_ { false };
    bool pendingAudioProfilesRefresh_{false};
    bool pendingAudioProfilesRefreshLogMarker_{false};
    QString lastAgMarkerKey_ {};
    QString lastAkActiveProfileMarker_ {};
    QString pendingApplyProfileName_ {};
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


