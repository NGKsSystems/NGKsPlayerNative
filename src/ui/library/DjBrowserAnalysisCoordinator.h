#pragma once

#include "ui/AnalysisBridge.h"
#include "ui/AnalysisResult.h"
#include "ui/AudioAnalysisService.h"
#include "ui/library/DjBrowserTrackOps.h"
#include "ui/library/DjLibraryDatabase.h"

#include <QFileInfo>
#include <QJsonObject>
#include <QObject>
#include <QThread>
#include <QString>
#include <QStringList>

#include <exception>
#include <functional>
#include <memory>
#include <utility>

class DjBrowserAnalysisCoordinator : public QObject {
public:
    using FooterCallback = std::function<void(const QString&, const QString&)>;
    using RefreshCallback = std::function<void()>;

    explicit DjBrowserAnalysisCoordinator(DjLibraryDatabase* db, QObject* parent = nullptr)
        : QObject(parent)
        , db_(db)
        , analysisBridge_(new AnalysisBridge(this))
    {
        connectBridgeSignals();
    }

    void setFooterCallback(FooterCallback callback)
    {
        footerCallback_ = std::move(callback);
    }

    void setRefreshCallback(RefreshCallback callback)
    {
        refreshCallback_ = std::move(callback);
    }

    void startRegularAnalysis(const QString& filePath)
    {
        if (regularAnalysisThread_) {
            updateFooter(QStringLiteral("Regular analyzer is already running."), QStringLiteral("busy"));
            return;
        }

        QString genreHint;
        if (db_) {
            const auto track = db_->trackByPath(filePath);
            if (track) genreHint = track->genre;
        }

        auto result = std::make_shared<AnalysisResult>();
        auto error = std::make_shared<QString>();
        updateFooter(
            QStringLiteral("Regular analyzer running: %1").arg(QFileInfo(filePath).fileName()),
            QStringLiteral("busy"));

        regularAnalysisThread_ = QThread::create([filePath, genreHint, result, error]() {
            try {
                AudioAnalysisService service;
                *result = service.analyzeFile(filePath, genreHint);
            } catch (const std::exception& ex) {
                *error = QString::fromUtf8(ex.what());
            } catch (...) {
                *error = QStringLiteral("Unexpected analyzer failure");
            }
        });

        QThread* worker = regularAnalysisThread_;
        QObject::connect(worker, &QThread::finished, worker, &QObject::deleteLater);
        QObject::connect(worker, &QThread::finished, this, [this, worker, filePath, result, error]() {
            if (regularAnalysisThread_ == worker) regularAnalysisThread_ = nullptr;

            const QString fileName = QFileInfo(filePath).fileName();
            if (!error->isEmpty()) {
                updateFooter(
                    QStringLiteral("Regular analyzer failed: %1").arg(*error),
                    QStringLiteral("error"));
                return;
            }

            if (!DjBrowserTrackOps::persistAnalysisResult(db_, filePath, *result)) {
                updateFooter(
                    QStringLiteral("Regular analyzer finished but metadata could not be saved: %1").arg(fileName),
                    QStringLiteral("error"));
                return;
            }

            refreshModel();
            QStringList parts;
            const QString bpm = formatBpm(result->bpm);
            if (!bpm.isEmpty()) parts << (bpm + QStringLiteral(" BPM"));
            if (!result->camelotKey.isEmpty()) parts << result->camelotKey;
            updateFooter(
                QStringLiteral("Regular analyzer complete: %1")
                    .arg(parts.isEmpty() ? fileName : parts.join(QStringLiteral(" | "))),
                QStringLiteral("success"));
        });

        worker->start();
    }

    void startBackgroundAnalysis(const QString& filePath)
    {
        backgroundAnalysisPath_ = filePath;
        updateFooter(
            QStringLiteral("Live analyzer queued: %1").arg(QFileInfo(filePath).fileName()),
            QStringLiteral("busy"));

        if (analysisBridge_->isReady()) {
            analysisBridge_->selectTrack(filePath);
            return;
        }

        if (!analysisBridge_->start()) {
            updateFooter(
                QStringLiteral("Live analyzer failed to start: %1").arg(QFileInfo(filePath).fileName()),
                QStringLiteral("error"));
        }
    }

private:
    static QString formatBpm(double bpm)
    {
        if (bpm <= 0.0) return {};
        QString text = QString::number(bpm, 'f', 1);
        if (text.endsWith(QStringLiteral(".0"))) text.chop(2);
        return text;
    }

    void updateFooter(const QString& text, const QString& tone) const
    {
        if (footerCallback_) footerCallback_(text, tone);
    }

    void refreshModel() const
    {
        if (refreshCallback_) refreshCallback_();
    }

    QString takeBackgroundAnalysisPath()
    {
        const QString path = backgroundAnalysisPath_;
        backgroundAnalysisPath_.clear();
        return path;
    }

    void connectBridgeSignals()
    {
        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeReady, this, [this]() {
            if (backgroundAnalysisPath_.isEmpty()) return;
            analysisBridge_->selectTrack(backgroundAnalysisPath_);
            updateFooter(
                QStringLiteral("Live analyzer started: %1").arg(QFileInfo(backgroundAnalysisPath_).fileName()),
                QStringLiteral("busy"));
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::panelStateChanged, this, [this](const QJsonObject& panel) {
            const QString state = panel.value(QStringLiteral("state")).toString();
            if (state.isEmpty() || backgroundAnalysisPath_.isEmpty()) return;

            const QString status = panel.value(QStringLiteral("status_text")).toString().trimmed();
            const QString progress = panel.value(QStringLiteral("progress_text")).toString().trimmed();
            const QString bpm = panel.value(QStringLiteral("bpm_text")).toString().trimmed();
            const QString key = panel.value(QStringLiteral("key_text")).toString().trimmed();

            if (state == QStringLiteral("ANALYSIS_QUEUED") || state == QStringLiteral("ANALYSIS_RUNNING")) {
                QStringList parts;
                if (!status.isEmpty()) parts << status;
                if (!progress.isEmpty()) parts << progress;
                if (parts.isEmpty()) parts << QStringLiteral("Analyzing...");
                updateFooter(
                    QStringLiteral("Live analyzer: %1").arg(parts.join(QStringLiteral(" | "))),
                    QStringLiteral("busy"));
                return;
            }

            if (state == QStringLiteral("ANALYSIS_FAILED") || state == QStringLiteral("ANALYSIS_CANCELED")) {
                const QString detail = !status.isEmpty()
                    ? status
                    : panel.value(QStringLiteral("review_reason")).toString().trimmed();
                takeBackgroundAnalysisPath();
                updateFooter(
                    QStringLiteral("Live analyzer failed: %1").arg(detail.isEmpty() ? QStringLiteral("Unknown error") : detail),
                    QStringLiteral("error"));
                return;
            }

            QStringList parts;
            if (!bpm.isEmpty()) parts << bpm;
            if (!key.isEmpty()) parts << key;
            if (parts.isEmpty() && !status.isEmpty()) parts << status;

            const QString completedPath = takeBackgroundAnalysisPath();
            refreshModel();
            updateFooter(
                QStringLiteral("Live analyzer complete: %1")
                    .arg(parts.isEmpty() ? QFileInfo(completedPath).fileName() : parts.join(QStringLiteral(" | "))),
                QStringLiteral("success"));
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeError, this, [this](const QString& error) {
            takeBackgroundAnalysisPath();
            updateFooter(
                QStringLiteral("Live analyzer error: %1").arg(error.isEmpty() ? QStringLiteral("Unknown error") : error),
                QStringLiteral("error"));
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeClosed, this, [this]() {
            if (backgroundAnalysisPath_.isEmpty()) return;
            const QString closedPath = takeBackgroundAnalysisPath();
            updateFooter(
                QStringLiteral("Live analyzer closed: %1").arg(QFileInfo(closedPath).fileName()),
                QStringLiteral("info"));
        });
    }

    DjLibraryDatabase* db_;
    AnalysisBridge* analysisBridge_;
    QThread* regularAnalysisThread_{nullptr};
    QString backgroundAnalysisPath_;
    FooterCallback footerCallback_;
    RefreshCallback refreshCallback_;
};