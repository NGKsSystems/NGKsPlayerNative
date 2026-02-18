#include "engine/runtime/jobs/JobQueue.h"

namespace ngks {

bool JobQueue::enqueue(const JobRequest& request)
{
    {
        std::lock_guard<std::mutex> guard(mutex);
        requests.push_back(request);
    }
    condition.notify_one();
    return true;
}

bool JobQueue::waitPop(JobRequest& out, const std::atomic<bool>& running)
{
    std::unique_lock<std::mutex> lock(mutex);
    condition.wait(lock, [&]() {
        return !running.load(std::memory_order_acquire) || !requests.empty();
    });

    if (requests.empty()) {
        return false;
    }

    out = requests.front();
    requests.pop_front();
    return true;
}

void JobQueue::notifyAll()
{
    condition.notify_all();
}

void JobQueue::cancel(uint32_t jobId) noexcept
{
    const size_t slot = static_cast<size_t>(jobId % kCancelSlots);
    cancelledJobTokens[slot].store(jobId + 1u, std::memory_order_release);
}

bool JobQueue::isCancelled(uint32_t jobId) const noexcept
{
    const size_t slot = static_cast<size_t>(jobId % kCancelSlots);
    const uint32_t token = cancelledJobTokens[slot].load(std::memory_order_acquire);
    return token == (jobId + 1u);
}

}
