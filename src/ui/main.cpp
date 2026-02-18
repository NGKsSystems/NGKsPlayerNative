#include <QAction>
#include <QApplication>
#include <QDialog>
#include <QHBoxLayout>
#include <QLabel>
#include <QMainWindow>
#include <QMenuBar>
#include <QMessageLogContext>
#include <QMutex>
#include <QMutexLocker>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QShortcut>
#include <QStringList>
#include <QVBoxLayout>
#include <QDateTime>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "ui/EngineBridge.h"

#ifndef NGKS_BUILD_STAMP
#define NGKS_BUILD_STAMP "unknown"
#endif

#ifndef NGKS_GIT_SHA
#define NGKS_GIT_SHA "unknown"
#endif

namespace {

QMutex gLogMutex;
std::string gLogPath;
bool gConsoleEcho = false;

QString uiLogAbsolutePath()
{
    return QString::fromStdString(std::filesystem::absolute(gLogPath).string());
}

const char* levelToText(QtMsgType type)
{
    switch (type) {
    case QtDebugMsg:
        return "DEBUG";
    case QtInfoMsg:
        return "INFO";
    case QtWarningMsg:
        return "WARN";
    case QtCriticalMsg:
        return "CRIT";
    case QtFatalMsg:
        return "FATAL";
    default:
        return "UNKNOWN";
    }
}

void writeLine(const QString& line)
{
    QMutexLocker locker(&gLogMutex);
    if (!gLogPath.empty()) {
        std::ofstream stream(gLogPath, std::ios::app);
        if (stream.is_open()) {
            stream << line.toStdString() << '\n';
            stream.flush();
        }
    }
    if (gConsoleEcho) {
        std::cerr << line.toStdString() << std::endl;
    }
}

void qtRuntimeMessageHandler(QtMsgType type, const QMessageLogContext& context, const QString& msg)
{
    const QString ts = QDateTime::currentDateTimeUtc().toString(Qt::ISODateWithMs);
    const QString category = context.category ? QString::fromUtf8(context.category) : QStringLiteral("qt");
    const QString file = context.file ? QString::fromUtf8(context.file) : QStringLiteral("?");
    const int line = context.line;
    const QString text = QStringLiteral("%1 [%2] [%3] %4:%5 %6")
                             .arg(ts,
                                  QString::fromUtf8(levelToText(type)),
                                  category,
                                  file,
                                  QString::number(line),
                                  msg);
    writeLine(text);

    if (type == QtFatalMsg) {
        abort();
    }
}

void initializeUiRuntimeLog()
{
    std::filesystem::create_directories("data/runtime");
    gLogPath = "data/runtime/ui_qt.log";

    const QString echoValue = qEnvironmentVariable("NGKS_UI_LOG_ECHO").trimmed().toLower();
    gConsoleEcho = (echoValue == QStringLiteral("1") || echoValue == QStringLiteral("true") || echoValue == QStringLiteral("yes"));

    qInstallMessageHandler(qtRuntimeMessageHandler);

    const QString banner = QStringLiteral("=== UI bootstrap BuildStamp=%1 GitSHA=%2 ===")
                               .arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA));
    writeLine(banner);
}

class DiagnosticsDialog : public QDialog {
public:
    explicit DiagnosticsDialog(QWidget* parent = nullptr)
        : QDialog(parent)
    {
        setWindowTitle(QStringLiteral("Diagnostics"));
        resize(760, 360);

        auto* layout = new QVBoxLayout(this);

        auto* pathLabel = new QLabel(QStringLiteral("ui_qt.log: %1").arg(uiLogAbsolutePath()), this);
        pathLabel->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(pathLabel);

        auto* row = new QHBoxLayout();
        auto* refreshButton = new QPushButton(QStringLiteral("Refresh Log Tail"), this);
        row->addWidget(refreshButton);
        row->addStretch(1);
        layout->addLayout(row);

        logTailBox_ = new QPlainTextEdit(this);
        logTailBox_->setReadOnly(true);
        layout->addWidget(logTailBox_);

        QObject::connect(refreshButton, &QPushButton::clicked, this, &DiagnosticsDialog::refreshLogTail);

        qInfo() << "DiagnosticsDialogConstructed";
        refreshLogTail();
    }

    void refreshLogTail()
    {
        std::ifstream stream(gLogPath);
        if (!stream.is_open()) {
            logTailBox_->setPlainText(QStringLiteral("log missing"));
            return;
        }

        std::vector<std::string> lines;
        std::string line;
        while (std::getline(stream, line)) {
            lines.push_back(line);
        }

        const size_t start = (lines.size() > 20u) ? (lines.size() - 20u) : 0u;
        QStringList tail;
        for (size_t i = start; i < lines.size(); ++i) {
            tail.push_back(QString::fromStdString(lines[i]));
        }

        if (tail.isEmpty()) {
            logTailBox_->setPlainText(QStringLiteral("log missing"));
        } else {
            logTailBox_->setPlainText(tail.join('\n'));
        }
    }

private:
    QPlainTextEdit* logTailBox_{nullptr};
};

class MainWindow : public QMainWindow {
public:
    explicit MainWindow(EngineBridge& engineBridge)
    {
        setWindowTitle(QStringLiteral("NGKsPlayerNative (Dev)"));
        resize(760, 300);

        auto* root = new QWidget(this);
        auto* layout = new QVBoxLayout(root);

        auto* title = new QLabel(QStringLiteral("NGKsPlayerNative (Dev)"), root);
        layout->addWidget(title);

        auto* buildInfo = new QLabel(
            QStringLiteral("BuildStamp=%1  GitSHA=%2").arg(QStringLiteral(NGKS_BUILD_STAMP), QStringLiteral(NGKS_GIT_SHA)),
            root);
        buildInfo->setTextInteractionFlags(Qt::TextSelectableByMouse);
        layout->addWidget(buildInfo);

        auto* bridgeStatus = new QLabel(QStringLiteral("EngineBridge: OK"), root);
        layout->addWidget(bridgeStatus);

        auto* placeholderState = new QLabel(QStringLiteral("State: UI_SANITY_V1"), root);
        layout->addWidget(placeholderState);

        layout->addStretch(1);
        setCentralWidget(root);

        auto* diagnosticsAction = menuBar()->addAction(QStringLiteral("Diagnostics"));
        QObject::connect(diagnosticsAction, &QAction::triggered, this, &MainWindow::showDiagnostics);

        auto* shortcut = new QShortcut(QKeySequence(QStringLiteral("Ctrl+D")), this);
        QObject::connect(shortcut, &QShortcut::activated, this, &MainWindow::showDiagnostics);

        Q_UNUSED(engineBridge);
        qInfo() << "MainWindowConstructed";
    }

private:
    void showDiagnostics()
    {
        if (!diagnosticsDialog_) {
            diagnosticsDialog_ = new DiagnosticsDialog(this);
        }
        diagnosticsDialog_->refreshLogTail();
        diagnosticsDialog_->show();
        diagnosticsDialog_->raise();
        diagnosticsDialog_->activateWindow();
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
    DiagnosticsDialog* diagnosticsDialog_{nullptr};
};

} // namespace

int main(int argc, char* argv[])
{
    initializeUiRuntimeLog();

    QApplication app(argc, argv);
    writeLine(QStringLiteral("UI app initialized pid=%1").arg(QString::number(QCoreApplication::applicationPid())));

    EngineBridge engineBridge;

    MainWindow window(engineBridge);
    window.show();
    window.autoShowDiagnosticsIfRequested();

    return app.exec();
}