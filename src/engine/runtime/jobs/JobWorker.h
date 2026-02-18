#pragma once

#include <atomic>
#include <thread>

#include "engine/runtime/jobs/JobQueue.h"
#include "engine/runtime/jobs/JobResult.h"

namespace ngks {

class JobWorker {
public:
    using ResultCallback = void(*)(void*, const JobResult&);

    JobWorker(JobQueue& queue, ResultCallback onResult, void* context);
    ~JobWorker();

    void start();
    void stop();

private:
    void run();
    void emitProgress(const JobRequest& request, uint8_t progress);

    JobQueue& queue;
    ResultCallback onResult;
    void* callbackContext;
    std::atomic<bool> running { false };
    std::thread thread;
};

}
