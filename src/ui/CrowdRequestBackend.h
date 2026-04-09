#pragma once

#include <QByteArray>
#include <QCoreApplication>
#include <QDir>
#include <QElapsedTimer>
#include <QEventLoop>
#include <QFile>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QNetworkAccessManager>
#include <QNetworkReply>
#include <QNetworkRequest>
#include <QObject>
#include <QProcess>
#include <QProcessEnvironment>
#include <QSqlDatabase>
#include <QSqlError>
#include <QSqlQuery>
#include <QThread>
#include <QTimer>
#include <QUrl>
#include <QUuid>
#include <QVariantMap>

class CrowdRequestBackend : public QObject
{
public:
    struct StatusSnapshot {
        bool running{false};
        int pendingCount{0};
        int requestCount{0};
        int acceptedCount{0};
        int handedOffCount{0};
        int nowPlayingCount{0};
        int playedCount{0};
        int handoffFailedCount{0};
        QString joinUrl;
        QString lastError;
    };

    explicit CrowdRequestBackend(QObject* parent = nullptr)
        : QObject(parent)
    {
    }

    ~CrowdRequestBackend() override
    {
        stop(true);
    }

    bool start()
    {
        if (proc_ && proc_->state() != QProcess::NotRunning) {
            refreshStatus(true);
            return snapshot_.running;
        }

        const QString pythonPath = workspacePath(QStringLiteral(".venv/Scripts/python.exe"));
        const QString scriptPath = workspacePath(QStringLiteral("src/analysis/crowd_request_server.py"));
        if (!QFile::exists(pythonPath)) {
            snapshot_.lastError = QStringLiteral("Python venv not found: %1").arg(pythonPath);
            return false;
        }
        if (!QFile::exists(scriptPath)) {
            snapshot_.lastError = QStringLiteral("Crowd request server script not found: %1").arg(scriptPath);
            return false;
        }

        operatorToken_ = QUuid::createUuid().toString(QUuid::WithoutBraces);
        proc_ = new QProcess(this);
        proc_->setProcessChannelMode(QProcess::SeparateChannels);
        proc_->setWorkingDirectory(workspaceRoot());
        QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
        env.insert(QStringLiteral("PYTHONIOENCODING"), QStringLiteral("utf-8"));
        proc_->setProcessEnvironment(env);

        connect(proc_, &QProcess::readyReadStandardOutput, this, [this]() {
            stdoutBuf_.append(proc_->readAllStandardOutput());
            trimBuffer(stdoutBuf_);
        });
        connect(proc_, &QProcess::readyReadStandardError, this, [this]() {
            stderrBuf_.append(proc_->readAllStandardError());
            trimBuffer(stderrBuf_);
        });
        connect(proc_, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished), this,
                [this](int, QProcess::ExitStatus) { snapshot_.running = false; });

        const QStringList args = {
            scriptPath,
            QStringLiteral("--bind"), QStringLiteral("0.0.0.0"),
            QStringLiteral("--port"), QString::number(port_),
            QStringLiteral("--library-json"), workspacePath(QStringLiteral("data/runtime/library.json")),
            QStringLiteral("--db-path"), workspacePath(QStringLiteral("data/runtime/crowd_requests_local.db")),
            QStringLiteral("--log-path"), workspacePath(QStringLiteral("data/runtime/crowd_requests_server.log")),
            QStringLiteral("--parent-pid"), QString::number(QCoreApplication::applicationPid()),
            QStringLiteral("--operator-token"), operatorToken_,
        };
        proc_->start(pythonPath, args);
        if (!proc_->waitForStarted(5000)) {
            snapshot_.lastError = QStringLiteral("Failed to start crowd request server process");
            proc_->deleteLater();
            proc_ = nullptr;
            return false;
        }
        if (!waitForHealth(5000)) {
            return false;
        }

        QString operatorError;
        const QJsonObject settingsReply = getJson(QStringLiteral("/settings"), &operatorError, true);
        if (settingsReply.isEmpty() || !settingsReply.value(QStringLiteral("ok")).toBool()) {
            snapshot_.running = false;
            snapshot_.lastError = operatorError.isEmpty()
                ? QStringLiteral("Crowd request server ownership check failed")
                : QStringLiteral("Crowd request server is already running under a different operator token. Stop the existing server and retry. Details: %1").arg(operatorError);
            if (proc_) {
                proc_->terminate();
                proc_->waitForFinished(1500);
                if (proc_->state() != QProcess::NotRunning) {
                    proc_->kill();
                    proc_->waitForFinished(1500);
                }
                proc_->deleteLater();
                proc_ = nullptr;
            }
            return false;
        }
        return true;
    }

    bool stop(bool quiet = false)
    {
        if (!proc_) {
            snapshot_ = StatusSnapshot{};
            return true;
        }

        if (proc_->state() != QProcess::NotRunning) {
            QString ignored;
            postJson(QStringLiteral("/operator/shutdown"), QJsonObject{}, true, &ignored);
            proc_->waitForFinished(3000);
            if (proc_->state() != QProcess::NotRunning) {
                proc_->terminate();
                proc_->waitForFinished(1500);
            }
            if (proc_->state() != QProcess::NotRunning) {
                proc_->kill();
                proc_->waitForFinished(1500);
            }
        }
        proc_->deleteLater();
        proc_ = nullptr;
        snapshot_ = StatusSnapshot{};
        if (!quiet) {
            snapshot_.lastError.clear();
        }
        return true;
    }

    StatusSnapshot status(bool refresh = true)
    {
        refreshStatus(refresh);
        return snapshot_;
    }

    QList<QVariantMap> fetchQueue(QString* error = nullptr)
    {
        const QJsonObject reply = getJson(QStringLiteral("/queue"), error);
        QList<QVariantMap> items;
        const QJsonArray rows = reply.value(QStringLiteral("requests")).toArray();
        for (const QJsonValue& row : rows) {
            items.push_back(row.toObject().toVariantMap());
        }
        snapshot_.pendingCount = reply.value(QStringLiteral("pending_count")).toInt(snapshot_.pendingCount);
        snapshot_.requestCount = items.size();
        snapshot_.running = true;
        return items;
    }

    bool submitRequest(const QJsonObject& payload, QString* error = nullptr)
    {
        const QJsonObject reply = postJson(QStringLiteral("/request"), payload, false, error);
        return !reply.isEmpty() && reply.value(QStringLiteral("ok")).toBool();
    }

    bool operatorAction(const QString& action, const QString& requestId, QVariantMap* outRequest, QString* error = nullptr)
    {
        const QString path = QStringLiteral("/operator/%1").arg(action);
        const QJsonObject reply = postJson(path, QJsonObject{{QStringLiteral("request_id"), requestId}}, true, error);
        if (reply.isEmpty() || !reply.value(QStringLiteral("ok")).toBool()) {
            return false;
        }
        if (outRequest) {
            *outRequest = reply.value(QStringLiteral("request")).toObject().toVariantMap();
        }
        return true;
    }

    bool reportHandoff(const QString& requestId,
                       const QString& status,
                       const QString& deck,
                       const QString& detail,
                       const QString& targetPath,
                       QVariantMap* outRequest,
                       QString* error = nullptr)
    {
        QJsonObject payload {
            {QStringLiteral("request_id"), requestId},
            {QStringLiteral("status"), status},
            {QStringLiteral("deck"), deck},
            {QStringLiteral("detail"), detail},
            {QStringLiteral("target_path"), targetPath},
        };
        const QJsonObject reply = postJson(QStringLiteral("/operator/handoff"), payload, true, error);
        if (reply.isEmpty() || !reply.value(QStringLiteral("ok")).toBool()) {
            return false;
        }
        if (outRequest) {
            *outRequest = reply.value(QStringLiteral("request")).toObject().toVariantMap();
        }
        return true;
    }

    bool saveNowPlaying(const QJsonObject& payload, QString* error = nullptr)
    {
        const QJsonObject reply = postJson(QStringLiteral("/operator/now-playing"), payload, true, error);
        return !reply.isEmpty() && reply.value(QStringLiteral("ok")).toBool();
    }

    bool clearQueue(QString* error = nullptr)
    {
        const QJsonObject reply = postJson(QStringLiteral("/operator/clear"), QJsonObject{}, true, error);
        return !reply.isEmpty() && reply.value(QStringLiteral("ok")).toBool();
    }

    QJsonObject loadSettings(QString* error = nullptr)
    {
        if (!proc_ || proc_->state() == QProcess::NotRunning) {
            return loadSettingsOffline(error);
        }
        return getJson(QStringLiteral("/settings"), error, true);
    }

    bool saveSettings(const QJsonObject& payload, QString* error = nullptr)
    {
        if (!proc_ || proc_->state() == QProcess::NotRunning) {
            return saveSettingsOffline(payload, error);
        }
        const QJsonObject reply = postJson(QStringLiteral("/operator/settings"), payload, true, error);
        return !reply.isEmpty() && reply.value(QStringLiteral("ok")).toBool();
    }

    bool isRunning() const
    {
        return snapshot_.running;
    }

    QByteArray fetchQrPng(QString* error = nullptr)
    {
        return requestBytes(QStringLiteral("/qr.png"), error);
    }

private:
    QString settingsDbPath() const
    {
        return workspacePath(QStringLiteral("data/runtime/crowd_requests_local.db"));
    }

    QString workspaceRoot() const
    {
        const QString exeDir = QCoreApplication::applicationDirPath();
        return QDir::cleanPath(QDir(exeDir).filePath(QStringLiteral("../../..")));
    }

    QString workspacePath(const QString& relativePath) const
    {
        return QDir(workspaceRoot()).filePath(relativePath);
    }

    QJsonObject defaultSettingsObject() const
    {
        QJsonObject handles;
        handles.insert(QStringLiteral("venmo"), QString());
        handles.insert(QStringLiteral("cashapp"), QString());
        handles.insert(QStringLiteral("paypal"), QString());
        handles.insert(QStringLiteral("zelle"), QString());
        handles.insert(QStringLiteral("buymeacoffee"), QString());
        handles.insert(QStringLiteral("chime"), QString());
        handles.insert(QStringLiteral("card_url"), QString());

        QJsonObject result;
        result.insert(QStringLiteral("ok"), true);
        result.insert(QStringLiteral("request_policy"), QStringLiteral("free"));
        result.insert(QStringLiteral("payment_handles"), handles);
        result.insert(QStringLiteral("updated_at"), QString());
        return result;
    }

    bool ensureSettingsTable(QSqlDatabase& db, QString* error)
    {
        QSqlQuery query(db);
        if (!query.exec(QStringLiteral(
                "CREATE TABLE IF NOT EXISTS crowd_request_settings ("
                "settings_id INTEGER PRIMARY KEY CHECK(settings_id = 1),"
                "request_policy TEXT NOT NULL,"
                "venmo TEXT NOT NULL,"
                "cashapp TEXT NOT NULL,"
                "paypal TEXT NOT NULL,"
                "zelle TEXT NOT NULL,"
                "buymeacoffee TEXT NOT NULL DEFAULT '',"
                "chime TEXT NOT NULL DEFAULT '',"
                "card_url TEXT NOT NULL DEFAULT '',"
                "updated_at TEXT NOT NULL)"))) {
            if (error) {
                *error = query.lastError().text();
            }
            return false;
        }

        if (!query.exec(QStringLiteral(
                "INSERT OR IGNORE INTO crowd_request_settings ("
                "settings_id, request_policy, venmo, cashapp, paypal, zelle, buymeacoffee, chime, card_url, updated_at) "
                "VALUES (1, 'free', '', '', '', '', '', '', '', '')"))) {
            if (error) {
                *error = query.lastError().text();
            }
            return false;
        }
        return true;
    }

    QJsonObject loadSettingsOffline(QString* error)
    {
        if (error) {
            error->clear();
        }

        QFileInfo dbInfo(settingsDbPath());
        dbInfo.dir().mkpath(QStringLiteral("."));

        const QString connectionName = QStringLiteral("crowd_request_backend_load_%1")
            .arg(QUuid::createUuid().toString(QUuid::WithoutBraces));
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connectionName);
        db.setDatabaseName(settingsDbPath());
        if (!db.open()) {
            if (error) {
                *error = db.lastError().text();
            }
            QSqlDatabase::removeDatabase(connectionName);
            return {};
        }

        QJsonObject result = defaultSettingsObject();
        {
            if (!ensureSettingsTable(db, error)) {
                db.close();
                QSqlDatabase::removeDatabase(connectionName);
                return {};
            }

            QSqlQuery query(db);
            if (!query.exec(QStringLiteral(
                    "SELECT request_policy, venmo, cashapp, paypal, zelle, buymeacoffee, chime, card_url, updated_at "
                    "FROM crowd_request_settings WHERE settings_id = 1"))) {
                if (error) {
                    *error = query.lastError().text();
                }
                db.close();
                QSqlDatabase::removeDatabase(connectionName);
                return {};
            }

            if (query.next()) {
                QJsonObject handles = result.value(QStringLiteral("payment_handles")).toObject();
                result.insert(QStringLiteral("request_policy"), query.value(0).toString());
                handles.insert(QStringLiteral("venmo"), query.value(1).toString());
                handles.insert(QStringLiteral("cashapp"), query.value(2).toString());
                handles.insert(QStringLiteral("paypal"), query.value(3).toString());
                handles.insert(QStringLiteral("zelle"), query.value(4).toString());
                handles.insert(QStringLiteral("buymeacoffee"), query.value(5).toString());
                handles.insert(QStringLiteral("chime"), query.value(6).toString());
                handles.insert(QStringLiteral("card_url"), query.value(7).toString());
                result.insert(QStringLiteral("payment_handles"), handles);
                result.insert(QStringLiteral("updated_at"), query.value(8).toString());
            }
        }

        db.close();
        QSqlDatabase::removeDatabase(connectionName);
        return result;
    }

    bool saveSettingsOffline(const QJsonObject& payload, QString* error)
    {
        if (error) {
            error->clear();
        }

        QFileInfo dbInfo(settingsDbPath());
        dbInfo.dir().mkpath(QStringLiteral("."));

        const QJsonObject handles = payload.value(QStringLiteral("payment_handles")).toObject();
        const QString requestPolicy = payload.value(QStringLiteral("request_policy")).toString(QStringLiteral("free"));
        const QString updatedAt = payload.value(QStringLiteral("updated_at")).toString();

        const QString connectionName = QStringLiteral("crowd_request_backend_save_%1")
            .arg(QUuid::createUuid().toString(QUuid::WithoutBraces));
        QSqlDatabase db = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connectionName);
        db.setDatabaseName(settingsDbPath());
        if (!db.open()) {
            if (error) {
                *error = db.lastError().text();
            }
            QSqlDatabase::removeDatabase(connectionName);
            return false;
        }

        bool ok = false;
        {
            if (!ensureSettingsTable(db, error)) {
                db.close();
                QSqlDatabase::removeDatabase(connectionName);
                return false;
            }

            QSqlQuery query(db);
            query.prepare(QStringLiteral(
                "UPDATE crowd_request_settings SET "
                "request_policy = ?, venmo = ?, cashapp = ?, paypal = ?, zelle = ?, buymeacoffee = ?, chime = ?, card_url = ?, updated_at = ? "
                "WHERE settings_id = 1"));
            query.addBindValue(requestPolicy);
            query.addBindValue(handles.value(QStringLiteral("venmo")).toString());
            query.addBindValue(handles.value(QStringLiteral("cashapp")).toString());
            query.addBindValue(handles.value(QStringLiteral("paypal")).toString());
            query.addBindValue(handles.value(QStringLiteral("zelle")).toString());
            query.addBindValue(handles.value(QStringLiteral("buymeacoffee")).toString());
            query.addBindValue(handles.value(QStringLiteral("chime")).toString());
            query.addBindValue(handles.value(QStringLiteral("card_url")).toString());
            query.addBindValue(updatedAt);
            ok = query.exec();
            if (!ok && error) {
                *error = query.lastError().text();
            }
        }

        db.close();
        QSqlDatabase::removeDatabase(connectionName);
        return ok;
    }

    void trimBuffer(QByteArray& buffer)
    {
        constexpr int kMaxBuffer = 16384;
        if (buffer.size() > kMaxBuffer) {
            buffer = buffer.right(kMaxBuffer);
        }
    }

    bool waitForHealth(int timeoutMs)
    {
        QElapsedTimer timer;
        timer.start();
        while (timer.elapsed() < timeoutMs) {
            refreshStatus(true);
            if (snapshot_.running) {
                return true;
            }
            QCoreApplication::processEvents(QEventLoop::AllEvents, 50);
            QThread::msleep(150);
        }
        if (snapshot_.lastError.isEmpty()) {
            snapshot_.lastError = QStringLiteral("Crowd request server health check timed out");
        }
        return false;
    }

    void refreshStatus(bool refresh)
    {
        if (!refresh) {
            if (!proc_ || proc_->state() == QProcess::NotRunning) {
                snapshot_.running = false;
            }
            return;
        }
        if (!proc_ || proc_->state() == QProcess::NotRunning) {
            snapshot_.running = false;
            if (snapshot_.joinUrl.isEmpty()) {
                snapshot_.joinUrl.clear();
            }
            return;
        }
        QString error;
        const QJsonObject reply = getJson(QStringLiteral("/health"), &error);
        if (reply.isEmpty() || !reply.value(QStringLiteral("ok")).toBool()) {
            snapshot_.running = false;
            snapshot_.lastError = error;
            return;
        }
        snapshot_.running = reply.value(QStringLiteral("running")).toBool();
        snapshot_.pendingCount = reply.value(QStringLiteral("pending_count")).toInt();
        snapshot_.requestCount = reply.value(QStringLiteral("request_count")).toInt();
        snapshot_.acceptedCount = reply.value(QStringLiteral("accepted_count")).toInt();
        snapshot_.handedOffCount = reply.value(QStringLiteral("handed_off_count")).toInt();
        snapshot_.nowPlayingCount = reply.value(QStringLiteral("now_playing_count")).toInt();
        snapshot_.playedCount = reply.value(QStringLiteral("played_count")).toInt();
        snapshot_.handoffFailedCount = reply.value(QStringLiteral("handoff_failed_count")).toInt();
        snapshot_.joinUrl = reply.value(QStringLiteral("join_url")).toString();
        snapshot_.lastError.clear();
    }

    QJsonObject getJson(const QString& path, QString* error = nullptr, bool operatorAuth = false)
    {
        return requestJson(QStringLiteral("GET"), path, QJsonObject{}, operatorAuth, error);
    }

    QJsonObject postJson(const QString& path, const QJsonObject& payload, bool operatorAuth, QString* error = nullptr)
    {
        return requestJson(QStringLiteral("POST"), path, payload, operatorAuth, error);
    }

    QJsonObject requestJson(const QString& method,
                            const QString& path,
                            const QJsonObject& payload,
                            bool operatorAuth,
                            QString* error)
    {
        if (error) {
            error->clear();
        }
        QNetworkRequest request(QUrl(QStringLiteral("http://127.0.0.1:%1%2").arg(port_).arg(path)));
        request.setRawHeader("Accept", "application/json");
        request.setRawHeader("Content-Type", "application/json");
        if (operatorAuth && !operatorToken_.isEmpty()) {
            request.setRawHeader("X-Operator-Token", operatorToken_.toUtf8());
        }

        QNetworkReply* reply = nullptr;
        if (method == QStringLiteral("GET")) {
            reply = network_.get(request);
        } else {
            reply = network_.post(request, QJsonDocument(payload).toJson(QJsonDocument::Compact));
        }

        QEventLoop loop;
        QTimer timer;
        timer.setSingleShot(true);
        connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
        connect(&timer, &QTimer::timeout, &loop, &QEventLoop::quit);
        timer.start(1500);
        loop.exec();

        if (!reply->isFinished()) {
            reply->abort();
            if (error) {
                *error = QStringLiteral("Request to crowd request server timed out");
            }
            reply->deleteLater();
            return {};
        }

        const QByteArray bytes = reply->readAll();
        const int httpStatus = reply->attribute(QNetworkRequest::HttpStatusCodeAttribute).toInt();
        QJsonParseError parseError{};
        const QJsonDocument doc = QJsonDocument::fromJson(bytes, &parseError);
        const QJsonObject obj = doc.isObject() ? doc.object() : QJsonObject{};

        if (reply->error() != QNetworkReply::NoError || httpStatus >= 400) {
            if (error) {
                *error = obj.value(QStringLiteral("error")).toString(reply->errorString());
            }
            reply->deleteLater();
            return obj;
        }
        if (parseError.error != QJsonParseError::NoError) {
            if (error) {
                *error = QStringLiteral("Invalid JSON reply from crowd request server");
            }
            reply->deleteLater();
            return {};
        }
        reply->deleteLater();
        return obj;
    }

    QByteArray requestBytes(const QString& path, QString* error)
    {
        if (error) {
            error->clear();
        }
        QNetworkRequest request(QUrl(QStringLiteral("http://127.0.0.1:%1%2").arg(port_).arg(path)));
        QNetworkReply* reply = network_.get(request);

        QEventLoop loop;
        QTimer timer;
        timer.setSingleShot(true);
        connect(reply, &QNetworkReply::finished, &loop, &QEventLoop::quit);
        connect(&timer, &QTimer::timeout, &loop, &QEventLoop::quit);
        timer.start(1500);
        loop.exec();

        if (!reply->isFinished()) {
            reply->abort();
            if (error) {
                *error = QStringLiteral("Request to crowd request server timed out");
            }
            reply->deleteLater();
            return {};
        }

        const QByteArray bytes = reply->readAll();
        if (reply->error() != QNetworkReply::NoError) {
            if (error) {
                *error = reply->errorString();
            }
            reply->deleteLater();
            return {};
        }
        reply->deleteLater();
        return bytes;
    }

    QProcess* proc_{nullptr};
    QNetworkAccessManager network_;
    QByteArray stdoutBuf_;
    QByteArray stderrBuf_;
    QString operatorToken_;
    int port_{3000};
    StatusSnapshot snapshot_;
};