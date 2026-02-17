#include <QGuiApplication>
#include <QQmlApplicationEngine>
#include <QQmlContext>
#include <QDir>
#include <QUrl>

#include "ui/EngineBridge.h"

int main(int argc, char* argv[])
{
    QGuiApplication app(argc, argv);

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