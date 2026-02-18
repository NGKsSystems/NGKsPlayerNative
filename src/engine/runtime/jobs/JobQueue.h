#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>

#include "engine/runtime/jobs/JobRequest.h"

namespace ngks {

class JobQueue {
public:
    bool enqueue(const JobRequest& request);
    bool waitPop(JobRequest& out, const std::atomic<bool>& running);
    void notifyAll();

    void cancel(uint32_t jobId) noexcept;
    bool isCancelled(uint32_t jobId) const noexcept;

private:
    static constexpr size_t kCancelSlots = 1024;

    mutable std::mutex mutex;
    std::condition_variable condition;
    std::deque<JobRequest> requests;
    std::array<std::atomic<uint32_t>, kCancelSlots> cancelledJobTokens {};
};

}
