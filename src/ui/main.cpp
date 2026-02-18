#include <QGuiApplication>
#include <QDir>
#include <QMessageLogContext>
#include <QMutex>
#include <QMutexLocker>
#include <QQmlApplicationEngine>
#include <QQmlContext>
#include <QDateTime>
#include <QUrl>

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

} // namespace

int main(int argc, char* argv[])
{
    initializeUiRuntimeLog();

    QGuiApplication app(argc, argv);
    writeLine(QStringLiteral("UI app initialized pid=%1").arg(QString::number(QCoreApplication::applicationPid())));

    EngineBridge engineBridge;

    QQmlApplicationEngine engine;
    const QString qmlPath = QDir(app.applicationDirPath()).filePath("qml/Main.qml");
    engine.rootContext()->setContextProperty("engine", &engineBridge);

    QObject::connect(
        &engine,
        &QQmlApplicationEngine::objectCreationFailed,
        &app,
        []() { QCoreApplication::exit(-1); },
        Qt::QueuedConnection);

    engine.load(QUrl::fromLocalFile(qmlPath));

    if (engine.rootObjects().isEmpty()) {
        return -1;
    }

    return app.exec();
}