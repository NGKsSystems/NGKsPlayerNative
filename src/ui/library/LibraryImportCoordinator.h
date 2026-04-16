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
#include <QStringList>

#include <exception>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

class LibraryImportCoordinator : public QObject {
public:
    using StatusCallback = std::function<void(const QString&)>;
    using TrackCallback = std::function<void(const QString&, const TrackInfo&)>;
    using BatchFinishedCallback = std::function<void()>;

    explicit LibraryImportCoordinator(DjLibraryDatabase* db, QObject* parent = nullptr)
        : QObject(parent)
        , db_(db)
    {
    }

    ~LibraryImportCoordinator() override
    {
        if (analysisBridge_) analysisBridge_->shutdown();
    }

    void setStatusCallback(StatusCallback callback)
    {
        statusCallback_ = std::move(callback);
    }

    void setTrackCallback(TrackCallback callback)
    {
        trackCallback_ = std::move(callback);
    }

    void setBatchFinishedCallback(BatchFinishedCallback callback)
    {
        batchFinishedCallback_ = std::move(callback);
    }

    void startImportBatch(const std::vector<TrackInfo>& tracks)
    {
        ++activeBatchId_;
        pendingRegularPaths_.clear();
        pendingLivePaths_.clear();

        for (const TrackInfo& track : tracks) {
            if (track.filePath.trimmed().isEmpty()) continue;
            pendingRegularPaths_.append(track.filePath);
            pendingLivePaths_.append(track.filePath);
        }

        totalTracks_ = pendingRegularPaths_.size();
        regularCompleted_ = 0;
        liveCompleted_ = 0;
        currentLivePath_.clear();
        liveBusy_ = false;

        resetLiveBridge();

        if (totalTracks_ <= 0) {
            emitStatus(QStringLiteral("Import analysis idle."));
            if (batchFinishedCallback_) batchFinishedCallback_();
            return;
        }

        emitProgress();
        startNextRegularAnalysis();
        startNextLiveAnalysis();
    }

private:
    void resetLiveBridge()
    {
        if (analysisBridge_) {
            analysisBridge_->shutdown();
            analysisBridge_->deleteLater();
            analysisBridge_ = nullptr;
        }

        analysisBridge_ = new AnalysisBridge(this);
        connectLiveBridgeSignals();
    }

    void ensureLiveBridge()
    {
        if (!analysisBridge_) resetLiveBridge();
    }

    void emitStatus(const QString& text) const
    {
        if (statusCallback_) statusCallback_(text);
    }

    void emitProgress() const
    {
        emitStatus(
            QStringLiteral("Import analysis: regular %1/%2 | live %3/%4")
                .arg(regularCompleted_)
                .arg(totalTracks_)
                .arg(liveCompleted_)
                .arg(totalTracks_));
    }

    void notifyTrackChanged(const QString& filePath) const
    {
        if (!trackCallback_ || !db_) return;
        const auto track = db_->trackByPath(filePath);
        if (track) trackCallback_(filePath, *track);
    }

    void maybeFinishBatch()
    {
        const bool regularDone = pendingRegularPaths_.isEmpty() && !regularAnalysisThread_;
        const bool liveDone = pendingLivePaths_.isEmpty() && !liveBusy_;
        if (!regularDone || !liveDone) return;

        emitStatus(QStringLiteral("Import analysis complete."));
        if (batchFinishedCallback_) batchFinishedCallback_();
    }

    void startNextRegularAnalysis()
    {
        if (regularAnalysisThread_ || pendingRegularPaths_.isEmpty()) {
            maybeFinishBatch();
            return;
        }

        const QString filePath = pendingRegularPaths_.takeFirst();
        const int batchId = activeBatchId_;
        const QString fileName = QFileInfo(filePath).fileName();

        if (!DjBrowserTrackOps::persistRegularAnalysisState(
                db_, filePath, QStringLiteral("ANALYSIS_RUNNING"), QJsonObject{{QStringLiteral("status"), QStringLiteral("running")}})) {
            emitStatus(QStringLiteral("Import regular analysis state could not be saved: %1").arg(fileName));
        }

        auto result = std::make_shared<AnalysisResult>();
        auto error = std::make_shared<QString>();
        regularAnalysisThread_ = QThread::create([filePath, result, error]() {
            try {
                AudioAnalysisService service;
                *result = service.analyzeFile(filePath, QString());
            } catch (const std::exception& ex) {
                *error = QString::fromUtf8(ex.what());
            } catch (...) {
                *error = QStringLiteral("Unexpected analyzer failure");
            }
        });

        QThread* worker = regularAnalysisThread_;
        QObject::connect(worker, &QThread::finished, worker, &QObject::deleteLater);
        QObject::connect(worker, &QThread::finished, this, [this, worker, batchId, filePath, fileName, result, error]() {
            if (regularAnalysisThread_ == worker) regularAnalysisThread_ = nullptr;
            if (batchId != activeBatchId_) return;

            AnalysisResult finalResult = *result;
            if (!error->isEmpty()) {
                finalResult.valid = false;
                finalResult.errorMsg = *error;
            }

            if (!DjBrowserTrackOps::persistAnalysisResult(db_, filePath, finalResult)) {
                emitStatus(QStringLiteral("Import regular analysis save failed: %1").arg(fileName));
            } else {
                notifyTrackChanged(filePath);
            }

            ++regularCompleted_;
            emitProgress();
            startNextRegularAnalysis();
            maybeFinishBatch();
        });

        worker->start();
    }

    void startNextLiveAnalysis()
    {
        if (liveBusy_ || pendingLivePaths_.isEmpty()) {
            maybeFinishBatch();
            return;
        }

        ensureLiveBridge();

        currentLivePath_ = pendingLivePaths_.takeFirst();
        liveBusy_ = true;

        DjBrowserTrackOps::persistLiveAnalysisPanel(
            db_, currentLivePath_, QJsonObject{
                {QStringLiteral("state"), QStringLiteral("ANALYSIS_QUEUED")},
                {QStringLiteral("status_text"), QStringLiteral("Queued for import")}
            });

        if (analysisBridge_->isReady()) {
            analysisBridge_->selectTrack(currentLivePath_);
            return;
        }

        if (!analysisBridge_->start()) {
            completeLiveAnalysisWithError(currentLivePath_, QStringLiteral("Live analyzer failed to start"));
        }
    }

    void completeLiveAnalysisWithError(const QString& filePath, const QString& error)
    {
        if (filePath.isEmpty()) return;

        DjBrowserTrackOps::persistLiveAnalysisPanel(
            db_, filePath, QJsonObject{
                {QStringLiteral("state"), QStringLiteral("ANALYSIS_FAILED")},
                {QStringLiteral("status_text"), error}
            });
        notifyTrackChanged(filePath);
        currentLivePath_.clear();
        liveBusy_ = false;
        ++liveCompleted_;
        emitProgress();
        startNextLiveAnalysis();
        maybeFinishBatch();
    }

    void connectLiveBridgeSignals()
    {
        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeReady, this, [this]() {
            if (!liveBusy_ || currentLivePath_.isEmpty()) return;
            analysisBridge_->selectTrack(currentLivePath_);
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::panelStateChanged, this, [this](const QJsonObject& panel) {
            if (!liveBusy_ || currentLivePath_.isEmpty()) return;

            DjBrowserTrackOps::persistLiveAnalysisPanel(db_, currentLivePath_, panel);

            const QString state = panel.value(QStringLiteral("state")).toString();
            if (state == QStringLiteral("ANALYSIS_COMPLETE") ||
                state == QStringLiteral("ANALYSIS_FAILED") ||
                state == QStringLiteral("ANALYSIS_CANCELED")) {
                const QString finishedPath = currentLivePath_;
                currentLivePath_.clear();
                liveBusy_ = false;
                analysisBridge_->unselectTrack();
                notifyTrackChanged(finishedPath);
                ++liveCompleted_;
                emitProgress();
                startNextLiveAnalysis();
                maybeFinishBatch();
            }
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeError, this, [this](const QString& error) {
            if (!liveBusy_ || currentLivePath_.isEmpty()) return;
            completeLiveAnalysisWithError(currentLivePath_, error.isEmpty() ? QStringLiteral("Unknown live analysis error") : error);
        });

        QObject::connect(analysisBridge_, &AnalysisBridge::bridgeClosed, this, [this]() {
            if (!liveBusy_ || currentLivePath_.isEmpty()) return;
            completeLiveAnalysisWithError(currentLivePath_, QStringLiteral("Live analyzer closed unexpectedly"));
        });
    }

    DjLibraryDatabase* db_{nullptr};
    AnalysisBridge* analysisBridge_{nullptr};
    QThread* regularAnalysisThread_{nullptr};
    QStringList pendingRegularPaths_;
    QStringList pendingLivePaths_;
    QString currentLivePath_;
    bool liveBusy_{false};
    int activeBatchId_{0};
    int totalTracks_{0};
    int regularCompleted_{0};
    int liveCompleted_{0};
    StatusCallback statusCallback_;
    TrackCallback trackCallback_;
    BatchFinishedCallback batchFinishedCallback_;
};