#pragma once
/*  AnalysisBridge  —  QProcess-based IPC between the Qt UI and
    the Python analysis layer (analysis_ipc_server.py).

    Usage:
      AnalysisBridge bridge(this);
      bridge.start();                               // spawns Python
      bridge.selectTrack("C:/Music/song.mp3");      // notifies analysis
      bridge.resolvePlayhead(45.0);                  // playhead update
      // connect to panelStateChanged() for UI updates
*/

#include <QObject>
#include <QProcess>
#include <QTimer>
#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QByteArray>
#include <QCoreApplication>
#include <QDir>
#include <QFile>
#include <functional>

class AnalysisBridge : public QObject
{
    Q_OBJECT

public:
    explicit AnalysisBridge(QObject* parent = nullptr)
        : QObject(parent)
    {
        pollTimer_.setInterval(500);  // 500ms panel state poll
        connect(&pollTimer_, &QTimer::timeout, this, &AnalysisBridge::poll);
    }

    ~AnalysisBridge() override
    {
        shutdown();
    }

    // ── Lifecycle ──────────────────────────────────────────

    bool start()
    {
        if (proc_) return true;  // already running

        proc_ = new QProcess(this);
        proc_->setProcessChannelMode(QProcess::SeparateChannels);

        connect(proc_, &QProcess::readyReadStandardOutput,
                this, &AnalysisBridge::onStdout);
        connect(proc_, &QProcess::readyReadStandardError,
                this, &AnalysisBridge::onStderr);
        connect(proc_, QOverload<int, QProcess::ExitStatus>::of(&QProcess::finished),
                this, &AnalysisBridge::onFinished);

        // Find Python & script relative to the exe
        const QString exeDir = QCoreApplication::applicationDirPath();
        // Walk up to workspace root (build_graph/release/bin → workspace)
        const QString workspace = QDir(exeDir).filePath(QStringLiteral("../../.."));
        const QString canonWs = QDir::cleanPath(workspace);

        pythonPath_ = QDir(canonWs).filePath(QStringLiteral(".venv/Scripts/python.exe"));
        scriptPath_ = QDir(canonWs).filePath(QStringLiteral("src/analysis/analysis_ipc_server.py"));

        if (!QFile::exists(pythonPath_)) {
            qWarning() << "AnalysisBridge: python not found:" << pythonPath_;
            emit bridgeError(QStringLiteral("Python venv not found"));
            delete proc_;
            proc_ = nullptr;
            return false;
        }
        if (!QFile::exists(scriptPath_)) {
            qWarning() << "AnalysisBridge: IPC script not found:" << scriptPath_;
            emit bridgeError(QStringLiteral("IPC server script not found"));
            delete proc_;
            proc_ = nullptr;
            return false;
        }

        QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
        env.insert(QStringLiteral("PYTHONIOENCODING"), QStringLiteral("utf-8"));
        proc_->setProcessEnvironment(env);
        proc_->setWorkingDirectory(canonWs);
        QTimer::singleShot(0, this, [this]() { proc_->start(pythonPath_, {scriptPath_}); });

        if (false) {
            qWarning() << "AnalysisBridge: failed to start Python process";
            emit bridgeError(QStringLiteral("Failed to start Python process"));
            delete proc_;
            proc_ = nullptr;
            return false;
        }

        ready_ = false;
        qInfo() << "AnalysisBridge: Python IPC process started, PID=" << proc_->processId();
        return true;
    }

    void shutdown()
    {
        pollTimer_.stop();
        if (!proc_) return;
        sendCommand({{"cmd", "shutdown"}});
        proc_->waitForFinished(3000);
        if (proc_->state() != QProcess::NotRunning) {
            proc_->kill();
            proc_->waitForFinished(2000);
        }
        delete proc_;
        proc_ = nullptr;
        ready_ = false;
        qInfo() << "AnalysisBridge: shutdown complete";
    }

    bool isReady() const { return ready_; }

    // ── Commands ───────────────────────────────────────────

    void selectTrack(const QString& filepath)
    {
        if (!proc_ || !ready_) return;
        activeTrackPath_ = filepath;
        sendCommand({
            {"cmd", "track_selected"},
            {"filepath", filepath}
        });
        // Start polling for state updates
        if (!pollTimer_.isActive()) pollTimer_.start();
    }

    void unselectTrack()
    {
        if (!proc_ || !ready_) return;
        activeTrackPath_.clear();
        pollTimer_.stop();
        sendCommand({{"cmd", "track_unselected"}});
    }

    void poll()
    {
        if (!proc_ || !ready_) return;
        sendCommand({{"cmd", "poll"}});
    }

    void resolvePlayhead(double seconds)
    {
        if (!proc_ || !ready_ || activeTrackPath_.isEmpty()) return;
        sendCommand({
            {"cmd", "resolve"},
            {"time_s", seconds}
        });
    }

    // ── Accessors ──────────────────────────────────────────

    QJsonObject lastPanel() const { return lastPanel_; }
    QString lastState() const { return lastPanel_.value(QStringLiteral("state")).toString(); }
    QString bpmText() const { return lastPanel_.value(QStringLiteral("bpm_text")).toString(); }
    QString keyText() const { return lastPanel_.value(QStringLiteral("key_text")).toString(); }
    QString durationText() const { return lastPanel_.value(QStringLiteral("duration_text")).toString(); }
    QString confidenceText() const { return lastPanel_.value(QStringLiteral("confidence_text")).toString(); }
    QString progressText() const { return lastPanel_.value(QStringLiteral("progress_text")).toString(); }
    double progress() const { return lastPanel_.value(QStringLiteral("progress")).toDouble(); }
    QString statusText() const { return lastPanel_.value(QStringLiteral("status_text")).toString(); }
    int sectionCount() const { return lastPanel_.value(QStringLiteral("section_count")).toInt(); }
    bool reviewRequired() const { return lastPanel_.value(QStringLiteral("review_required")).toBool(); }
    QString reviewReason() const { return lastPanel_.value(QStringLiteral("review_reason")).toString(); }

    // Live readout
    QString liveBpmText() const { return lastPanel_.value(QStringLiteral("live_bpm_text")).toString(); }
    QString liveKeyText() const { return lastPanel_.value(QStringLiteral("live_key_text")).toString(); }
    QString liveSectionLabel() const { return lastPanel_.value(QStringLiteral("live_section_label")).toString(); }
    double liveBpmConfidence() const { return lastPanel_.value(QStringLiteral("live_bpm_confidence")).toDouble(); }
    double liveKeyConfidence() const { return lastPanel_.value(QStringLiteral("live_key_confidence")).toDouble(); }

signals:
    void panelStateChanged(const QJsonObject& panel);
    void bridgeReady();
    void bridgeError(const QString& error);
    void bridgeClosed();

private slots:
    void onStdout()
    {
        stdoutBuf_.append(proc_->readAllStandardOutput());
        // Process complete JSON lines
        while (true) {
            const int nl = stdoutBuf_.indexOf('\n');
            if (nl < 0) break;
            const QByteArray line = stdoutBuf_.left(nl).trimmed();
            stdoutBuf_.remove(0, nl + 1);
            if (line.isEmpty()) continue;
            processResponse(line);
        }
    }

    void onStderr()
    {
        const QByteArray err = proc_->readAllStandardError();
        // Log Python stderr for diagnostics
        const QString errStr = QString::fromUtf8(err).trimmed();
        if (!errStr.isEmpty()) {
            qInfo().noquote() << "AnalysisBridge[py]:" << errStr;
        }
    }

    void onFinished(int exitCode, QProcess::ExitStatus status)
    {
        qInfo() << "AnalysisBridge: Python process finished, exit=" << exitCode
                << "status=" << status;
        ready_ = false;
        pollTimer_.stop();
        emit bridgeClosed();
    }

private:
    void sendCommand(const QJsonObject& cmd)
    {
        if (!proc_ || proc_->state() != QProcess::Running) return;
        const QByteArray data = QJsonDocument(cmd).toJson(QJsonDocument::Compact) + "\n";
        proc_->write(data);
    }

    void processResponse(const QByteArray& line)
    {
        QJsonParseError err;
        const QJsonDocument doc = QJsonDocument::fromJson(line, &err);
        if (doc.isNull()) {
            qWarning() << "AnalysisBridge: JSON parse error:" << err.errorString();
            return;
        }
        const QJsonObject obj = doc.object();
        if (!obj.value(QStringLiteral("ok")).toBool()) {
            qWarning() << "AnalysisBridge: error from Python:"
                       << obj.value(QStringLiteral("error")).toString();
            return;
        }

        // Handle ready signal
        if (obj.contains(QStringLiteral("ready"))) {
            ready_ = true;
            qInfo() << "AnalysisBridge: Python IPC ready, remote PID="
                     << obj.value(QStringLiteral("pid")).toInt();
            emit bridgeReady();
            return;
        }

        // Handle pong
        if (obj.contains(QStringLiteral("pong"))) return;

        // Handle panel state update
        if (obj.contains(QStringLiteral("panel"))) {
            lastPanel_ = obj.value(QStringLiteral("panel")).toObject();
            emit panelStateChanged(lastPanel_);
        }
    }

    QProcess* proc_{nullptr};
    QTimer pollTimer_;
    bool ready_{false};
    QString pythonPath_;
    QString scriptPath_;
    QString activeTrackPath_;
    QByteArray stdoutBuf_;
    QJsonObject lastPanel_;
};
