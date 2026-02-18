#include "engine/runtime/jobs/JobSystem.h"

namespace ngks {

JobSystem::JobSystem()
    : worker(queue, &JobSystem::onWorkerResult, this)
{
}

JobSystem::~JobSystem()
{
    stop();
}

void JobSystem::start()
{
    if (started.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    worker.start();
}

void JobSystem::stop()
{
    if (!started.exchange(false, std::memory_order_acq_rel)) {
        return;
    }

    worker.stop();
}

bool JobSystem::enqueue(const JobRequest& request)
{
    return queue.enqueue(request);
}

void JobSystem::cancel(uint32_t jobId) noexcept
{
    queue.cancel(jobId);
}

bool JobSystem::tryPopResult(JobResult& out) noexcept
{
    return results.pop(out);
}

void JobSystem::onWorkerResult(void* context, const JobResult& result)
{
    auto* self = static_cast<JobSystem*>(context);
    if (self != nullptr) {
        self->results.push(result);
    }
}

}
