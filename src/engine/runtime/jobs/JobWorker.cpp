#include "engine/runtime/jobs/JobWorker.h"

#include <chrono>

namespace ngks {

namespace {
constexpr int analyzeSteps = 5;
constexpr int stemsSteps = 10;
constexpr auto stepSleep = std::chrono::milliseconds(10);
}

JobWorker::JobWorker(JobQueue& queueRef, ResultCallback callback, void* context)
    : queue(queueRef)
    , onResult(callback)
    , callbackContext(context)
{
}

JobWorker::~JobWorker()
{
    stop();
}

void JobWorker::start()
{
    if (running.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    thread = std::thread([this]() {
        run();
    });
}

void JobWorker::stop()
{
    if (!running.exchange(false, std::memory_order_acq_rel)) {
        return;
    }

    queue.notifyAll();
    if (thread.joinable()) {
        thread.join();
    }
}

void JobWorker::run()
{
    while (running.load(std::memory_order_acquire)) {
        JobRequest request {};
        if (!queue.waitPop(request, running)) {
            continue;
        }

        if (queue.isCancelled(request.jobId)) {
            JobResult cancelled {};
            cancelled.jobId = request.jobId;
            cancelled.deckId = request.deckId;
            cancelled.trackId = request.trackId;
            cancelled.type = request.type;
            cancelled.status = JobStatus::Cancelled;
            cancelled.progress0_100 = 100;
            onResult(callbackContext, cancelled);
            continue;
        }

        const int totalSteps = (request.type == JobType::AnalyzeTrack) ? analyzeSteps : stemsSteps;
        for (int step = 1; step <= totalSteps; ++step) {
            std::this_thread::sleep_for(stepSleep);

            if (queue.isCancelled(request.jobId)) {
                JobResult cancelled {};
                cancelled.jobId = request.jobId;
                cancelled.deckId = request.deckId;
                cancelled.trackId = request.trackId;
                cancelled.type = request.type;
                cancelled.status = JobStatus::Cancelled;
                cancelled.progress0_100 = static_cast<uint8_t>((step * 100) / totalSteps);
                onResult(callbackContext, cancelled);
                goto next_request;
            }

            emitProgress(request, static_cast<uint8_t>((step * 100) / totalSteps));
        }

        {
            JobResult completed {};
            completed.jobId = request.jobId;
            completed.deckId = request.deckId;
            completed.trackId = request.trackId;
            completed.type = request.type;
            completed.status = JobStatus::Complete;
            completed.progress0_100 = 100;
            if (request.type == JobType::AnalyzeTrack) {
                completed.bpmFixed = 12800;
                completed.loudness = -1400;
                completed.deadAirMs = 200;
                completed.stemsReady = 0;
            } else {
                completed.stemsReady = 1;
            }
            onResult(callbackContext, completed);
        }

    next_request:
        continue;
    }
}

void JobWorker::emitProgress(const JobRequest& request, uint8_t progress)
{
    JobResult progressResult {};
    progressResult.jobId = request.jobId;
    progressResult.deckId = request.deckId;
    progressResult.trackId = request.trackId;
    progressResult.type = request.type;
    progressResult.status = JobStatus::Running;
    progressResult.progress0_100 = progress;
    onResult(callbackContext, progressResult);
}

}
