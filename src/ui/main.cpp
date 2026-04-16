#include <QAction>
#include <QApplication>
#include <QComboBox>
#include <QDialog>
#include <QDialogButtonBox>
#include <QEvent>
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
#include <QSet>
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
#include <QRandomGenerator>
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
#include <QToolButton>
#include <QStyle>
#include <QPainter>
#include <QPainterPath>
#include <QPen>
#include <QElapsedTimer>
#include <QFileDialog>
#include <QDirIterator>
#include <QFormLayout>
#include <QHash>
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
#include "ui/library/LibraryImportCoordinator.h"
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

enum class DjIntroMode {
    Always,
    FirstLaunchOnly,
    Off,
};

QString djIntroModeKey()
{
    return QStringLiteral("settings/dj_intro_mode");
}

QString djIntroSeenKey()
{
    return QStringLiteral("settings/dj_intro_seen");
}

QByteArray blobForText(const QString& text)
{
    return text.toUtf8();
}

QString textFromBlob(const QByteArray& blob)
{
    return QString::fromUtf8(blob).trimmed();
}

struct ImportFolderSelection {
    QString sourceFolder;
    QString destinationFolder;
};

struct ManagedImportCopyResult {
    int copiedCount{0};
    int reusedCount{0};
    QStringList failedFiles;
};

class ImportFoldersDialog final : public QDialog {
public:
    ImportFoldersDialog(const QString& initialDestination, QWidget* parent = nullptr)
        : QDialog(parent)
    {
        setWindowTitle(QStringLiteral("Import Music"));
        setModal(true);

        auto* layout = new QVBoxLayout(this);
        auto* form = new QFormLayout();
        sourceEdit_ = new QLineEdit(this);
        destinationEdit_ = new QLineEdit(this);
        destinationEdit_->setText(initialDestination);

        auto* sourceRow = new QWidget(this);
        auto* sourceLayout = new QHBoxLayout(sourceRow);
        sourceLayout->setContentsMargins(0, 0, 0, 0);
        auto* sourceBrowse = new QPushButton(QStringLiteral("Browse"), sourceRow);
        sourceLayout->addWidget(sourceEdit_);
        sourceLayout->addWidget(sourceBrowse);

        auto* destinationRow = new QWidget(this);
        auto* destinationLayout = new QHBoxLayout(destinationRow);
        destinationLayout->setContentsMargins(0, 0, 0, 0);
        auto* destinationBrowse = new QPushButton(QStringLiteral("Browse"), destinationRow);
        destinationLayout->addWidget(destinationEdit_);
        destinationLayout->addWidget(destinationBrowse);

        form->addRow(QStringLiteral("Source Folder"), sourceRow);
        form->addRow(QStringLiteral("Destination Folder"), destinationRow);
        layout->addLayout(form);

        auto* hint = new QLabel(
            QStringLiteral("Audio files are copied into the managed destination folder. Existing files with identical content are reused instead of duplicated."),
            this);
        hint->setWordWrap(true);
        layout->addWidget(hint);

        auto* buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, this);
        layout->addWidget(buttons);

        QObject::connect(sourceBrowse, &QPushButton::clicked, this, [this]() {
            const QString selected = QFileDialog::getExistingDirectory(this, QStringLiteral("Select Source Folder"), sourceEdit_->text());
            if (!selected.isEmpty()) sourceEdit_->setText(selected);
        });
        QObject::connect(destinationBrowse, &QPushButton::clicked, this, [this]() {
            const QString selected = QFileDialog::getExistingDirectory(this, QStringLiteral("Select Destination Folder"), destinationEdit_->text());
            if (!selected.isEmpty()) destinationEdit_->setText(selected);
        });
        QObject::connect(buttons, &QDialogButtonBox::accepted, this, [this]() {
            if (sourceEdit_->text().trimmed().isEmpty() || destinationEdit_->text().trimmed().isEmpty()) {
                QMessageBox::warning(this, QStringLiteral("Import Music"),
                    QStringLiteral("Choose both a source folder and a destination folder before importing."));
                return;
            }
            accept();
        });
        QObject::connect(buttons, &QDialogButtonBox::rejected, this, &QDialog::reject);
    }

    ImportFolderSelection selection() const
    {
        return { sourceEdit_->text().trimmed(), destinationEdit_->text().trimmed() };
    }

private:
    QLineEdit* sourceEdit_{nullptr};
    QLineEdit* destinationEdit_{nullptr};
};

qint64 generateMediaId()
{
    qint64 mediaId = 0;
    while (mediaId <= 0) {
        mediaId = static_cast<qint64>(QRandomGenerator::global()->generate64() & 0x7fffffffffffffffULL);
    }
    return mediaId;
}

QString uniqueImportPath(const QString& desiredPath)
{
    QFileInfo info(desiredPath);
    const QString stem = info.completeBaseName();
    const QString suffix = info.suffix().isEmpty() ? QString() : QStringLiteral(".") + info.suffix();
    QDir dir = info.dir();

    QString candidate = desiredPath;
    for (int counter = 2; QFileInfo::exists(candidate); ++counter) {
        candidate = dir.filePath(QStringLiteral("%1 (%2)%3").arg(stem).arg(counter).arg(suffix));
    }
    return candidate;
}

ManagedImportCopyResult copyManagedImportLibrary(const QString& sourceFolder, const QString& destinationFolder)
{
    ManagedImportCopyResult result;
    const QDir sourceRoot(sourceFolder);
    const QDir destinationRoot(destinationFolder);
    QDir().mkpath(destinationRoot.absolutePath());

    QDirIterator it(sourceFolder, supportedAudioFileFilters(), QDir::Files, QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        const QString sourcePath = it.filePath();
        QString relativePath = sourceRoot.relativeFilePath(sourcePath);
        if (relativePath.startsWith(QStringLiteral(".."))) {
            relativePath = QFileInfo(sourcePath).fileName();
        }

        QString destinationPath = destinationRoot.filePath(relativePath);
        QFileInfo destinationInfo(destinationPath);
        QDir().mkpath(destinationInfo.dir().absolutePath());

        const QString normalizedSource = QFileInfo(sourcePath).absoluteFilePath();
        const QString normalizedDestination = QFileInfo(destinationPath).absoluteFilePath();
        if (normalizedSource.compare(normalizedDestination, Qt::CaseInsensitive) == 0) {
            ++result.reusedCount;
            continue;
        }

        const QString sourceFingerprint = computeTrackFingerprint(sourcePath);
        if (QFileInfo::exists(destinationPath)) {
            const QString destinationFingerprint = computeTrackFingerprint(destinationPath);
            if (!sourceFingerprint.isEmpty() && sourceFingerprint == destinationFingerprint) {
                ++result.reusedCount;
                continue;
            }
            destinationPath = uniqueImportPath(destinationPath);
            destinationInfo = QFileInfo(destinationPath);
            QDir().mkpath(destinationInfo.dir().absolutePath());
        }

        if (!QFile::copy(sourcePath, destinationPath)) {
            result.failedFiles << sourcePath;
            continue;
        }
        ++result.copiedCount;
    }

    return result;
}

DjIntroMode djIntroModeFromString(const QString& value)
{
    const QString lower = value.trimmed().toLower();
    if (lower == QStringLiteral("off")) return DjIntroMode::Off;
    if (lower == QStringLiteral("first-launch-only")) return DjIntroMode::FirstLaunchOnly;
    return DjIntroMode::Always;
}

QString djIntroModeToString(DjIntroMode mode)
{
    switch (mode) {
    case DjIntroMode::Off:
        return QStringLiteral("off");
    case DjIntroMode::FirstLaunchOnly:
        return QStringLiteral("first-launch-only");
    case DjIntroMode::Always:
    default:
        return QStringLiteral("always");
    }
}

class MainWindow : public QMainWindow {
public:
    explicit MainWindow(EngineBridge& engineBridge)
        : bridge_(engineBridge)
    {
        setWindowFlag(Qt::FramelessWindowHint, true);
        setWindowTitle(QStringLiteral("NGKsPlayerNative"));
        resize(640, 480);

        auto* root = new QWidget(this);
        auto* rootLayout = new QVBoxLayout(root);
        rootLayout->setContentsMargins(0, 0, 0, 0);
        rootLayout->setSpacing(0);

        topChrome_ = new QWidget(root);
        topChrome_->setObjectName(QStringLiteral("topChrome"));
        topChrome_->setFixedHeight(34);
        topChrome_->setStyleSheet(QStringLiteral(
            "QWidget#topChrome { background: #121722; border-bottom: 1px solid #222a36; }"
            "QLabel { color: #d7deea; background: transparent; }"
            "QLabel#topTitleLabel { color: #dfe8ff; font-size: 15px; font-weight: 700; letter-spacing: 3px; padding-left: 6px; }"
            "QToolButton { background: #1a2434; color: #e4edf8; border: 1px solid #2f4f88; border-radius: 4px; padding: 1px 10px; }"
            "QToolButton:hover { background: #22314b; }"
            "QToolButton:pressed { background: #16213e; }"
            "QToolButton#closeWindowBtn { border-color: #8f313d; background: #2a1b22; }"
            "QToolButton#closeWindowBtn:hover { background: #5a2432; }"));
        auto* topChromeLayout = new QHBoxLayout(topChrome_);
        topChromeLayout->setContentsMargins(8, 4, 6, 4);
        topChromeLayout->setSpacing(6);

        auto* leftChromeSlot = new QWidget(topChrome_);
        leftChromeSlot->setFixedWidth(140);
        auto* leftChromeLayout = new QHBoxLayout(leftChromeSlot);
        leftChromeLayout->setContentsMargins(0, 0, 0, 0);
        leftChromeLayout->setSpacing(6);

        djUtilityMenuBtn_ = new QToolButton(topChrome_);
        djUtilityMenuBtn_->setText(QStringLiteral("Menu"));
        djUtilityMenuBtn_->setPopupMode(QToolButton::InstantPopup);
        djUtilityMenuBtn_->setToolButtonStyle(Qt::ToolButtonTextOnly);
        djUtilityMenuBtn_->setVisible(false);
        leftChromeLayout->addWidget(djUtilityMenuBtn_, 0);

        diagnosticsBtn_ = new QToolButton(topChrome_);
        diagnosticsBtn_->setText(QStringLiteral("Diagnostics"));
        diagnosticsBtn_->setToolButtonStyle(Qt::ToolButtonTextOnly);
        leftChromeLayout->addWidget(diagnosticsBtn_, 0);
        leftChromeLayout->addStretch(1);
        topChromeLayout->addWidget(leftChromeSlot, 0);

        topTitleLabel_ = new QLabel(QStringLiteral("KAELIX"), topChrome_);
        topTitleLabel_->setObjectName(QStringLiteral("topTitleLabel"));
        topTitleLabel_->setAlignment(Qt::AlignCenter);
        topChromeLayout->addWidget(topTitleLabel_, 1);

        auto* rightChromeSlot = new QWidget(topChrome_);
        rightChromeSlot->setFixedWidth(110);
        auto* rightChromeLayout = new QHBoxLayout(rightChromeSlot);
        rightChromeLayout->setContentsMargins(0, 0, 0, 0);
        rightChromeLayout->setSpacing(6);
        rightChromeLayout->addStretch(1);

        minimizeWindowBtn_ = new QToolButton(topChrome_);
        minimizeWindowBtn_->setText(QStringLiteral("-"));
        minimizeWindowBtn_->setFixedWidth(28);
        rightChromeLayout->addWidget(minimizeWindowBtn_, 0);

        maximizeWindowBtn_ = new QToolButton(topChrome_);
        maximizeWindowBtn_->setText(QStringLiteral("[]"));
        maximizeWindowBtn_->setFixedWidth(34);
        rightChromeLayout->addWidget(maximizeWindowBtn_, 0);

        closeWindowBtn_ = new QToolButton(topChrome_);
        closeWindowBtn_->setObjectName(QStringLiteral("closeWindowBtn"));
        closeWindowBtn_->setText(QStringLiteral("X"));
        closeWindowBtn_->setFixedWidth(28);
        rightChromeLayout->addWidget(closeWindowBtn_, 0);

        topChromeLayout->addWidget(rightChromeSlot, 0);

        rootLayout->addWidget(topChrome_);

        // ── Stacked pages ──
        stack_ = new QStackedWidget(root);
        stack_->addWidget(buildSplashPage());    // 0
        stack_->addWidget(buildLandingPage());   // 1
        playerPage_ = new PlayerPage(bridge_, djDb_);
        playerPage_->setTrackList(&allTracks_);
        stack_->addWidget(playerPage_);          // 2
        djModePage_ = new DjModePage(bridge_, djDb_);
        djModePage_->setTrackList(&allTracks_);
        if (!importedFolderPath_.trimmed().isEmpty()) {
            djModePage_->setBrowserRootFolder(importedFolderPath_);
        }
        stack_->addWidget(djModePage_);          // 3
        connect(djModePage_, &DjModePage::backRequested, this, [this]() {
            if (djIntroOverlay_) djIntroOverlay_->stop();
            stack_->setCurrentIndex(1);
        });
        connect(djModePage_, &DjModePage::importFolderRequested, this, [this]() {
            importMusicFolder();
        });
        connect(djModePage_, &DjModePage::importAnalysisRequested, this, [this]() {
            startImportAnalysisBatch();
        });
        djIntroOverlay_ = new DjIntroOverlay(stack_);
        djIntroOverlay_->setGeometry(stack_->rect());
        djIntroOverlay_->hide();
        stack_->installEventFilter(this);
        connect(&bridge_, &EngineBridge::audioHotReady, this, [this](bool ok) {
            if (ok && djIntroOverlay_) djIntroOverlay_->duckForEngineReady();
        });
        stack_->setCurrentIndex(0);
        rootLayout->addWidget(stack_, 1);

        // ── Persistent status strip ──
        statusStrip_ = new QWidget(root);
        statusStrip_->setStyleSheet(QStringLiteral("background:#222; color:#ccc; font-size:11px;"));
        auto* stripLayout = new QHBoxLayout(statusStrip_);
        stripLayout->setContentsMargins(8, 4, 8, 4);
        engineStatusLabel_ = new QLabel(QStringLiteral("Engine: NOT_READY"), statusStrip_);
        runningLabel_ = new QLabel(QStringLiteral("Running: NO"), statusStrip_);
        meterLabel_ = new QLabel(QStringLiteral("MeterL: 0.000  MeterR: 0.000"), statusStrip_);
        stripLayout->addWidget(engineStatusLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(runningLabel_);
        stripLayout->addSpacing(16);
        stripLayout->addWidget(meterLabel_);
        stripLayout->addStretch(1);
        rootLayout->addWidget(statusStrip_);

        setCentralWidget(root);

        if (menuBar()) menuBar()->hide();
        if (djModePage_) djUtilityMenuBtn_->setMenu(djModePage_->utilityMenu());

        QObject::connect(diagnosticsBtn_, &QToolButton::clicked, this, &MainWindow::showDiagnostics);
        QObject::connect(minimizeWindowBtn_, &QToolButton::clicked, this, &QWidget::showMinimized);
        QObject::connect(maximizeWindowBtn_, &QToolButton::clicked, this, [this]() {
            isMaximized() ? showNormal() : showMaximized();
        });
        QObject::connect(closeWindowBtn_, &QToolButton::clicked, this, &QWidget::close);
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

        loadDjIntroSettings();

        // ── Open SQLite library database ──
        djDb_.open(runtimePath("data/runtime/ngks_library.db"));

        importCoordinator_ = new LibraryImportCoordinator(&djDb_, this);
        importCoordinator_->setStatusCallback([this](const QString& text) {
            latestImportStatusText_ = text;
            refreshImportSystemSummary();
            qInfo().noquote() << text;
        });
        importCoordinator_->setTrackCallback([this](const QString& filePath, const TrackInfo& track) {
            applyImportedTrackUpdate(filePath, track);
        });
        importCoordinator_->setBatchFinishedCallback([this]() {
            importAnalysisRunning_ = false;
            latestImportStatusText_ = QStringLiteral("Import analysis complete.");
            refreshImportSystemSummary();
            saveLibraryJson(allTracks_, importedFolderPath_);
            qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_IMPORT_ANALYSIS");
        });

        // ── Restore persisted library ──
        {
            QString restoredFolder;
            std::vector<TrackInfo> restoredTracks;
            if (loadLibraryJson(restoredTracks, restoredFolder)) {
                allTracks_ = std::move(restoredTracks);
                importedFolderPath_ = restoredFolder;
                playerPage_->setTrackList(&allTracks_);
                if (djModePage_ && !importedFolderPath_.trimmed().isEmpty()) {
                    djModePage_->setBrowserRootFolder(importedFolderPath_);
                }
                latestImportStatusText_ = QStringLiteral("Library restored from disk.");

                bool libraryUpgraded = false;
                for (TrackInfo& track : allTracks_) {
                    if (track.mediaId <= 0) {
                        track.mediaId = generateMediaId();
                        libraryUpgraded = true;
                    }
                    if (track.fileFingerprint.isEmpty() && QFileInfo::exists(track.filePath)) {
                        track.fileFingerprint = computeTrackFingerprint(track.filePath);
                        libraryUpgraded = true;
                    }
                }

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

                if (libraryUpgraded) {
                    saveLibraryJson(allTracks_, importedFolderPath_);
                }

djDb_.bulkInsert(allTracks_);
                refreshImportSystemSummary();
                restoreLibraryUiPending_ = true;
            }
        }

        // ── Restore persisted playlists ──
        loadPlaylists(playlists_);
        qInfo().noquote() << QStringLiteral("PLAYLISTS_RESTORED=%1").arg(playlists_.size());

        // ── Splash auto-transition (2 s) ──
        QTimer::singleShot(2000, this, [this]() { stack_->setCurrentIndex(1); });


        // ── Refresh player library every time the player page becomes visible ──
        QObject::connect(stack_, &QStackedWidget::currentChanged, this, [this](int index) {
            const bool djModeActive = (index == 3);
            if (diagnosticsBtn_) diagnosticsBtn_->setVisible(!djModeActive);
            if (djUtilityMenuBtn_) djUtilityMenuBtn_->setVisible(djModeActive);
            if (statusStrip_) statusStrip_->setVisible(!djModeActive);
            if (index == 1) {
                if (!landingLibraryBound_ && libraryTree_) {
                    libraryTree_->setDatabase(&djDb_);
                    landingLibraryBound_ = true;
                }
                if (restoreLibraryUiPending_) {
                    restoreLibraryUiPending_ = false;
                    refreshLibraryList();
                    playerPage_->refreshLibrary();
                    qInfo().noquote() << QStringLiteral("LIBRARY_RESTORED=%1").arg(allTracks_.size());
                }
            }
            if (index == 2) {
                playerPage_->bindLibraryDatabase();
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
    bool eventFilter(QObject* watched, QEvent* event) override
    {
        if (watched == stack_ && djIntroOverlay_ &&
            (event->type() == QEvent::Resize || event->type() == QEvent::Show)) {
            djIntroOverlay_->setGeometry(stack_->rect());
        }

        return QMainWindow::eventFilter(watched, event);
    }

    bool nativeEvent(const QByteArray& eventType, void* message, qintptr* result) override
    {
#ifdef _WIN32
        Q_UNUSED(eventType);
        MSG* msg = static_cast<MSG*>(message);
        if (msg->message == WM_NCHITTEST && !isMaximized()) {
            const LONG border = 8;
            RECT windowRect {};
            GetWindowRect(HWND(winId()), &windowRect);

            const LONG x = static_cast<short>(LOWORD(msg->lParam));
            const LONG y = static_cast<short>(HIWORD(msg->lParam));

            const bool onLeft = x >= windowRect.left && x < windowRect.left + border;
            const bool onRight = x < windowRect.right && x >= windowRect.right - border;
            const bool onTop = y >= windowRect.top && y < windowRect.top + border;
            const bool onBottom = y < windowRect.bottom && y >= windowRect.bottom - border;

            if (onTop && onLeft) { *result = HTTOPLEFT; return true; }
            if (onTop && onRight) { *result = HTTOPRIGHT; return true; }
            if (onBottom && onLeft) { *result = HTBOTTOMLEFT; return true; }
            if (onBottom && onRight) { *result = HTBOTTOMRIGHT; return true; }
            if (onLeft) { *result = HTLEFT; return true; }
            if (onRight) { *result = HTRIGHT; return true; }
            if (onTop) { *result = HTTOP; return true; }
            if (onBottom) { *result = HTBOTTOM; return true; }

            if (topChrome_) {
                const QPoint globalPos(x, y);
                const QPoint localPos = mapFromGlobal(globalPos);
                const QRect chromeRect(topChrome_->mapTo(this, QPoint(0, 0)), topChrome_->size());
                if (chromeRect.contains(localPos)) {
                    QWidget* hovered = childAt(localPos);
                    const bool draggableArea = (hovered == nullptr || hovered == topChrome_ || hovered == topTitleLabel_);
                    if (draggableArea) {
                        *result = HTCAPTION;
                        return true;
                    }
                }
            }
        }
#endif
        return QMainWindow::nativeEvent(eventType, message, result);
    }

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

        importFolderBtn_ = new QPushButton(QStringLiteral("Import Music Folder"), page);
        auto* importBtn = importFolderBtn_;
        importBtn->setMinimumHeight(32);
        importBtn->setCursor(Qt::PointingHandCursor);
        importBtn->setToolTip(QStringLiteral("Scan a folder, add tracks to the library, and start background dual analysis"));
        actionRow->addWidget(importBtn);

        runImportAnalysisBtn_ = new QPushButton(QStringLiteral("Run Import Analysis"), page);
        runImportAnalysisBtn_->setMinimumHeight(32);
        runImportAnalysisBtn_->setCursor(Qt::PointingHandCursor);
        runImportAnalysisBtn_->setToolTip(QStringLiteral("Run the regular and live analysis import pipeline on the current library"));
        actionRow->addWidget(runImportAnalysisBtn_);

        legacyImportBtn_ = new QPushButton(QStringLiteral("Import Legacy DB"), page);
        auto* legacyDbBtn = legacyImportBtn_;
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
        settingsBtn->setToolTip(QStringLiteral("Application settings"));
        toolRow->addWidget(settingsBtn);
        QObject::connect(settingsBtn, &QPushButton::clicked, this, [this]() {
            showAppSettingsDialog();
        });

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

        auto* importStatusCard = new QWidget(page);
        importStatusCard->setStyleSheet(QStringLiteral(
            "background: #101a33; border: 1px solid #0f3460; border-radius: 8px;"
        ));
        auto* importStatusLayout = new QVBoxLayout(importStatusCard);
        importStatusLayout->setContentsMargins(12, 10, 12, 10);
        importStatusLayout->setSpacing(4);

        importStatusTitleLabel_ = new QLabel(QStringLiteral("Import System Ready"), importStatusCard);
        importStatusTitleLabel_->setStyleSheet(QStringLiteral("color: #e94560; font-size: 13px; font-weight: bold;"));
        importStatusLayout->addWidget(importStatusTitleLabel_);

        importStatusDetailLabel_ = new QLabel(importStatusCard);
        importStatusDetailLabel_->setWordWrap(true);
        importStatusDetailLabel_->setStyleSheet(QStringLiteral("color: #b8c2d8; font-size: 11px;"));
        importStatusLayout->addWidget(importStatusDetailLabel_);

        layout->addWidget(importStatusCard);

        // ── Main content: splitter (library browser | detail panel) ──
        auto* splitter = new QSplitter(Qt::Horizontal, page);
        splitter->setHandleWidth(2);
        splitter->setStyleSheet(QStringLiteral("QSplitter::handle { background: #0f3460; }"));

        libraryTree_ = new LibraryBrowserWidget(LibraryBrowserWidget::Mode::MainPanel, splitter);

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

        refreshImportSystemSummary();

        // ── Connections ──

        // Import folder
        QObject::connect(importBtn, &QPushButton::clicked, this, [this]() {
            importMusicFolder();
        });

        QObject::connect(runImportAnalysisBtn_, &QPushButton::clicked, this, [this]() {
            startImportAnalysisBatch();
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
            latestImportStatusText_ = QStringLiteral("Legacy database merged into the current library.");
            refreshImportSystemSummary();
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
            qInfo().noquote() << QStringLiteral("DJ_MODE_REQUEST overlayActive=%1")
                .arg((djIntroOverlay_ && djIntroOverlay_->isActive()) ? 1 : 0);
            if (djIntroOverlay_ && djIntroOverlay_->isActive()) {
                djIntroOverlay_->skip();
                return;
            }

            const bool enterOk = bridge_.enterDjMode();
            qInfo().noquote() << QStringLiteral("DJ_MODE_ENTER dispatched=%1").arg(enterOk ? 1 : 0);
            djModePage_->refreshLibrary();
            stack_->setCurrentIndex(3);
            qInfo().noquote() << QStringLiteral("DJ_MODE_STACK_INDEX=%1").arg(stack_->currentIndex());

            if (shouldPlayDjIntro()) {
                const QString intro = chooseRandomDjIntroVideo();
                if (!intro.isEmpty()) {
                    markDjIntroSeen();
                    djIntroOverlay_->setGeometry(stack_->rect());
                    djIntroOverlay_->play(intro);
                } else {
                    qInfo().noquote() << QStringLiteral("DJ_MODE_INTRO_SKIPPED reason=no_video");
                }
            } else {
                qInfo().noquote() << QStringLiteral("DJ_MODE_INTRO_SKIPPED reason=mode_setting");
            }
        });

        return page;
    }

    void showAppSettingsDialog()
    {
        QDialog dialog(this);
        dialog.setWindowTitle(QStringLiteral("Settings"));
        dialog.setModal(true);
        dialog.resize(360, 160);

        auto* layout = new QVBoxLayout(&dialog);
        auto* introLabel = new QLabel(
            QStringLiteral("DJ mode intro video behavior:"),
            &dialog);
        layout->addWidget(introLabel);

        auto* introModeCombo = new QComboBox(&dialog);
        introModeCombo->addItem(QStringLiteral("Always"), static_cast<int>(DjIntroMode::Always));
        introModeCombo->addItem(QStringLiteral("First Launch Only"), static_cast<int>(DjIntroMode::FirstLaunchOnly));
        introModeCombo->addItem(QStringLiteral("Off"), static_cast<int>(DjIntroMode::Off));
        introModeCombo->setCurrentIndex(introModeCombo->findData(static_cast<int>(djIntroMode_)));
        layout->addWidget(introModeCombo);

        auto* help = new QLabel(
            QStringLiteral("Click the intro video or press Space, Enter, or Esc to skip it. Audio ducks automatically when DJ audio becomes ready."),
            &dialog);
        help->setWordWrap(true);
        layout->addWidget(help);

        auto* buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, &dialog);
        layout->addWidget(buttons);
        QObject::connect(buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
        QObject::connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);

        if (dialog.exec() != QDialog::Accepted) return;

        djIntroMode_ = static_cast<DjIntroMode>(introModeCombo->currentData().toInt());
        saveUiStateBlob(djIntroModeKey(), blobForText(djIntroModeToString(djIntroMode_)));
    }

    void loadDjIntroSettings()
    {
        QByteArray introModeBlob;
        if (loadUiStateBlob(djIntroModeKey(), introModeBlob)) {
            djIntroMode_ = djIntroModeFromString(textFromBlob(introModeBlob));
        }

        QByteArray introSeenBlob;
        if (loadUiStateBlob(djIntroSeenKey(), introSeenBlob)) {
            const QString text = textFromBlob(introSeenBlob).toLower();
            djIntroSeen_ = (text == QStringLiteral("1") || text == QStringLiteral("true") || text == QStringLiteral("yes"));
        }
    }

    bool shouldPlayDjIntro() const
    {
        switch (djIntroMode_) {
        case DjIntroMode::Off:
            return false;
        case DjIntroMode::FirstLaunchOnly:
            return !djIntroSeen_;
        case DjIntroMode::Always:
        default:
            return true;
        }
    }

    void markDjIntroSeen()
    {
        if (djIntroSeen_) return;
        djIntroSeen_ = true;
        saveUiStateBlob(djIntroSeenKey(), blobForText(QStringLiteral("1")));
    }

    QString chooseRandomDjIntroVideo()
    {
        const QString assetsPath = QCoreApplication::applicationDirPath()
                                   + QStringLiteral("/../../../assets");
        QDir assetsDir(assetsPath);
        const QStringList names = assetsDir.entryList(
            {QStringLiteral("*.mp4"), QStringLiteral("*.mov"), QStringLiteral("*.m4v"),
             QStringLiteral("*.avi"), QStringLiteral("*.webm"), QStringLiteral("*.mkv")},
            QDir::Files | QDir::Readable,
            QDir::Name);
        if (names.isEmpty()) return {};

        QStringList candidates;
        candidates.reserve(names.size());
        for (const QString& name : names) {
            candidates.push_back(assetsDir.absoluteFilePath(name));
        }

        if (candidates.size() > 1 && !lastDjIntroVideoPath_.isEmpty()) {
            candidates.removeAll(lastDjIntroVideoPath_);
            if (candidates.isEmpty()) {
                for (const QString& name : names) {
                    candidates.push_back(assetsDir.absoluteFilePath(name));
                }
            }
        }

        const int index = QRandomGenerator::global()->bounded(candidates.size());
        lastDjIntroVideoPath_ = candidates.at(index);
        return lastDjIntroVideoPath_;
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

    void refreshImportSystemSummary()
    {
        const bool canImportFolder = !importAnalysisRunning_;
        const bool canRunAnalysis = !allTracks_.empty() && !importAnalysisRunning_;

        if (importFolderBtn_) {
            importFolderBtn_->setEnabled(canImportFolder);
        }
        if (runImportAnalysisBtn_) {
            runImportAnalysisBtn_->setEnabled(canRunAnalysis);
        }
        if (legacyImportBtn_) {
            legacyImportBtn_->setEnabled(!allTracks_.empty() && !importAnalysisRunning_);
        }

        QString titleText;
        QString detailText;

        if (allTracks_.empty()) {
            titleText = QStringLiteral("Import System Ready");
            detailText =
                QStringLiteral("Use Import Music Folder to scan tracks. Songs will appear in the library immediately while regular and live analysis run in the background and persist to the database.");
            if (importStatusTitleLabel_) importStatusTitleLabel_->setText(titleText);
            if (importStatusDetailLabel_) importStatusDetailLabel_->setText(detailText);
            if (djModePage_) djModePage_->setImportUiState(titleText, detailText, canImportFolder, canRunAnalysis);
            return;
        }

        int regularComplete = 0;
        int regularRunning = 0;
        int regularFailed = 0;
        int regularWaiting = 0;
        int liveComplete = 0;
        int liveQueued = 0;
        int liveRunning = 0;
        int liveFailed = 0;
        int liveWaiting = 0;

        for (const TrackInfo& track : allTracks_) {
            const QString regularState = track.regularAnalysisState.trimmed();
            if (regularState == QStringLiteral("ANALYSIS_COMPLETE")) {
                ++regularComplete;
            } else if (regularState == QStringLiteral("ANALYSIS_RUNNING")) {
                ++regularRunning;
            } else if (regularState == QStringLiteral("ANALYSIS_FAILED") ||
                       regularState == QStringLiteral("ANALYSIS_CANCELED")) {
                ++regularFailed;
            } else {
                ++regularWaiting;
            }

            const QString liveState = track.liveAnalysisState.trimmed();
            if (liveState == QStringLiteral("ANALYSIS_COMPLETE")) {
                ++liveComplete;
            } else if (liveState == QStringLiteral("ANALYSIS_QUEUED")) {
                ++liveQueued;
            } else if (liveState == QStringLiteral("ANALYSIS_RUNNING")) {
                ++liveRunning;
            } else if (liveState == QStringLiteral("ANALYSIS_FAILED") ||
                       liveState == QStringLiteral("ANALYSIS_CANCELED")) {
                ++liveFailed;
            } else {
                ++liveWaiting;
            }
        }

        const bool workRemaining = regularRunning > 0 || regularWaiting > 0 || liveQueued > 0 || liveRunning > 0 || liveWaiting > 0;
        titleText = importAnalysisRunning_ || workRemaining
            ? QStringLiteral("Import System Running")
            : QStringLiteral("Import System Ready");

        QStringList detailParts;
        if (!latestImportStatusText_.trimmed().isEmpty()) {
            detailParts << latestImportStatusText_.trimmed();
        }
        detailParts << QStringLiteral("Tracks: %1").arg(allTracks_.size());
        detailParts << QStringLiteral("Regular %1 done, %2 running, %3 failed, %4 waiting")
            .arg(regularComplete)
            .arg(regularRunning)
            .arg(regularFailed)
            .arg(regularWaiting);
        detailParts << QStringLiteral("Live %1 done, %2 queued, %3 running, %4 failed, %5 waiting")
            .arg(liveComplete)
            .arg(liveQueued)
            .arg(liveRunning)
            .arg(liveFailed)
            .arg(liveWaiting);
        detailText = detailParts.join(QStringLiteral("  |  "));

        if (importStatusTitleLabel_) importStatusTitleLabel_->setText(titleText);
        if (importStatusDetailLabel_) importStatusDetailLabel_->setText(detailText);
        if (djModePage_) djModePage_->setImportUiState(titleText, detailText, canImportFolder, canRunAnalysis);
    }

    void carryForwardTrackedState(TrackInfo& target, const TrackInfo& existing) const
    {
        if (target.fileFingerprint.isEmpty()) target.fileFingerprint = existing.fileFingerprint;
        target.regularAnalysisState = existing.regularAnalysisState;
        target.regularAnalysisJson = existing.regularAnalysisJson;
        target.liveAnalysisState = existing.liveAnalysisState;
        target.liveAnalysisJson = existing.liveAnalysisJson;
        target.cueIn = existing.cueIn;
        target.cueOut = existing.cueOut;
        target.rating = existing.rating;
        target.comments = existing.comments;
        target.legacyImported = existing.legacyImported;

        if (!existing.bpm.trimmed().isEmpty()) target.bpm = existing.bpm;
        if (!existing.musicalKey.trimmed().isEmpty()) target.musicalKey = existing.musicalKey;
        if (!existing.camelotKey.trimmed().isEmpty()) target.camelotKey = existing.camelotKey;
        if (existing.energy >= 0.0) target.energy = existing.energy;
        if (existing.danceability >= 0.0) target.danceability = existing.danceability;
        if (existing.loudnessLUFS != 0.0) target.loudnessLUFS = existing.loudnessLUFS;
        if (target.durationMs <= 0 && existing.durationMs > 0) target.durationMs = existing.durationMs;
        if (target.durationStr.trimmed().isEmpty() && !existing.durationStr.trimmed().isEmpty()) target.durationStr = existing.durationStr;
    }

    std::vector<TrackInfo> reconcileManagedLibraryTracks(const std::vector<TrackInfo>& scannedTracks) const
    {
        QHash<QString, int> existingByPath;
        QHash<QString, int> existingByFingerprint;
        for (int index = 0; index < static_cast<int>(allTracks_.size()); ++index) {
            const TrackInfo& existing = allTracks_[static_cast<size_t>(index)];
            existingByPath.insert(existing.filePath, index);
            if (!existing.fileFingerprint.isEmpty() && !existingByFingerprint.contains(existing.fileFingerprint)) {
                existingByFingerprint.insert(existing.fileFingerprint, index);
            }
        }

        std::vector<TrackInfo> reconciled;
        reconciled.reserve(scannedTracks.size());
        QSet<qint64> assignedMediaIds;

        for (const TrackInfo& scanned : scannedTracks) {
            TrackInfo merged = scanned;
            const TrackInfo* existing = nullptr;

            const auto pathIt = existingByPath.constFind(scanned.filePath);
            if (pathIt != existingByPath.cend()) {
                existing = &allTracks_[static_cast<size_t>(pathIt.value())];
            } else if (!scanned.fileFingerprint.isEmpty()) {
                const auto fingerprintIt = existingByFingerprint.constFind(scanned.fileFingerprint);
                if (fingerprintIt != existingByFingerprint.cend()) {
                    existing = &allTracks_[static_cast<size_t>(fingerprintIt.value())];
                }
            }

            if (existing != nullptr) {
                carryForwardTrackedState(merged, *existing);
                if (existing->mediaId > 0 && !assignedMediaIds.contains(existing->mediaId)) {
                    merged.mediaId = existing->mediaId;
                }
            }

            if (merged.mediaId <= 0 || assignedMediaIds.contains(merged.mediaId)) {
                merged.mediaId = generateMediaId();
            }
            assignedMediaIds.insert(merged.mediaId);
            reconciled.push_back(std::move(merged));
        }

        return reconciled;
    }

    void importMusicFolder()
    {
        const QString initialDestination = importedFolderPath_.trimmed().isEmpty()
            ? QDir::home().filePath(QStringLiteral("NGKsPlayerNativeLibrary"))
            : importedFolderPath_;
        ImportFoldersDialog dialog(initialDestination, this);
        if (dialog.exec() != QDialog::Accepted) return;

        const ImportFolderSelection selection = dialog.selection();
        if (selection.sourceFolder.isEmpty() || selection.destinationFolder.isEmpty()) return;

        qInfo().noquote() << QStringLiteral("LIBRARY_IMPORT_STARTED source=%1 dest=%2")
            .arg(selection.sourceFolder, selection.destinationFolder);

        const ManagedImportCopyResult copyResult = copyManagedImportLibrary(selection.sourceFolder, selection.destinationFolder);
        std::vector<TrackInfo> scannedTracks = scanFolderForTracks(selection.destinationFolder);
        allTracks_ = reconcileManagedLibraryTracks(scannedTracks);
        importedFolderPath_ = selection.destinationFolder;

        latestImportStatusText_ = QStringLiteral("Imported from %1 to %2. %3 copied, %4 reused, %5 failed. Background analysis starting.")
            .arg(QFileInfo(selection.sourceFolder).fileName().isEmpty() ? selection.sourceFolder : QFileInfo(selection.sourceFolder).fileName())
            .arg(QFileInfo(selection.destinationFolder).fileName().isEmpty() ? selection.destinationFolder : QFileInfo(selection.destinationFolder).fileName())
            .arg(copyResult.copiedCount)
            .arg(copyResult.reusedCount)
            .arg(copyResult.failedFiles.size());
        qInfo().noquote() << QStringLiteral("FILES_FOUND=%1").arg(allTracks_.size());
        qInfo().noquote() << QStringLiteral("TRACKS_INDEXED=%1").arg(allTracks_.size());

        applyCoreDurationPatch(allTracks_);
        djDb_.bulkInsert(allTracks_);

        clearTrackDetail();
        refreshLibraryList();
        playerPage_->setTrackList(&allTracks_);
        playerPage_->refreshLibrary();
        if (djModePage_) {
            djModePage_->setBrowserRootFolder(importedFolderPath_);
            djModePage_->refreshLibrary();
        }
        refreshImportSystemSummary();

        saveLibraryJson(allTracks_, importedFolderPath_);
        qInfo().noquote() << QStringLiteral("LIBRARY_PERSISTED=POST_SCAN");

        if (!copyResult.failedFiles.isEmpty()) {
            QMessageBox::warning(this, QStringLiteral("Import Completed With Errors"),
                QStringLiteral("%1 files could not be copied into the managed library folder. The library was refreshed with the files that did import.")
                    .arg(copyResult.failedFiles.size()));
        }

        startImportAnalysisBatch();
    }

    void startImportAnalysisBatch()
    {
        if (!importCoordinator_ || allTracks_.empty()) {
            refreshImportSystemSummary();
            return;
        }
        if (importAnalysisRunning_) {
            QMessageBox::information(this, QStringLiteral("Import Analysis Running"),
                QStringLiteral("The import analysis pipeline is already running for this library."));
            return;
        }

        importAnalysisRunning_ = true;
        if (latestImportStatusText_.trimmed().isEmpty()) {
            latestImportStatusText_ = QStringLiteral("Background analysis starting.");
        }
        refreshImportSystemSummary();
        importCoordinator_->startImportBatch(allTracks_);
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

    void applyImportedTrackUpdate(const QString& filePath, const TrackInfo& track)
    {
        if (filePath.isEmpty()) return;

        for (size_t index = 0; index < allTracks_.size(); ++index) {
            if (allTracks_[index].filePath != filePath) continue;

            allTracks_[index] = track;
            refreshLibraryList();
            playerPage_->refreshLibrary();
            if (djModePage_) djModePage_->refreshLibrary();
            if (libraryTree_ && libraryTree_->currentTrackId() == static_cast<qint64>(index)) {
                showTrackDetail(static_cast<int>(index));
            }
            refreshImportSystemSummary();
            saveLibraryJson(allTracks_, importedFolderPath_);
            return;
        }
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
    QPushButton* importFolderBtn_{nullptr};
    QPushButton* runImportAnalysisBtn_{nullptr};
    QPushButton* legacyImportBtn_{nullptr};
    QLabel* trackCountLabel_{nullptr};
    QLabel* importStatusTitleLabel_{nullptr};
    QLabel* importStatusDetailLabel_{nullptr};
    TrackDetailPanel* detailPanel_{nullptr};
    PlayerPage* playerPage_{nullptr};
    DjModePage* djModePage_{nullptr};
    DjIntroOverlay* djIntroOverlay_{nullptr};
    LibraryImportCoordinator* importCoordinator_{nullptr};
    QString lastDjIntroVideoPath_;
    DjIntroMode djIntroMode_{DjIntroMode::Always};
    bool djIntroSeen_{false};
    DjLibraryDatabase djDb_;
    std::vector<TrackInfo> allTracks_;
    std::vector<Playlist> playlists_;
    int activePlaylistIndex_{-1}; // -1 = show all library
    QString importedFolderPath_;
    QString latestImportStatusText_;
    bool importAnalysisRunning_{false};
    bool restoreLibraryUiPending_{false};
    bool landingLibraryBound_{false};


    QLabel* engineStatusLabel_{nullptr};
    QLabel* runningLabel_{nullptr};
    QLabel* meterLabel_{nullptr};
    QWidget* statusStrip_{nullptr};
    QWidget* topChrome_{nullptr};
    QLabel* topTitleLabel_{nullptr};
    QToolButton* diagnosticsBtn_{nullptr};
    QToolButton* djUtilityMenuBtn_{nullptr};
    QToolButton* minimizeWindowBtn_{nullptr};
    QToolButton* maximizeWindowBtn_{nullptr};
    QToolButton* closeWindowBtn_{nullptr};
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

