#pragma once

#include <array>
#include <atomic>
#include <cstdint>

#include "engine/runtime/jobs/JobQueue.h"
#include "engine/runtime/jobs/JobResult.h"
#include "engine/runtime/jobs/JobWorker.h"

namespace ngks {

template <size_t Capacity>
class JobResultRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power-of-two");

public:
    bool push(const JobResult& result) noexcept
    {
        const uint32_t write = writeIndex.load(std::memory_order_relaxed);
        const uint32_t nextWrite = (write + 1u) & mask;
        if (nextWrite == readIndex.load(std::memory_order_acquire)) {
            return false;
        }

        buffer[write] = result;
        writeIndex.store(nextWrite, std::memory_order_release);
        return true;
    }

    bool pop(JobResult& out) noexcept
    {
        const uint32_t read = readIndex.load(std::memory_order_relaxed);
        if (read == writeIndex.load(std::memory_order_acquire)) {
            return false;
        }

        out = buffer[read];
        readIndex.store((read + 1u) & mask, std::memory_order_release);
        return true;
    }

private:
    static constexpr uint32_t mask = static_cast<uint32_t>(Capacity - 1u);
    std::array<JobResult, Capacity> buffer {};
    std::atomic<uint32_t> writeIndex { 0 };
    std::atomic<uint32_t> readIndex { 0 };
};

class JobSystem {
public:
    JobSystem();
    ~JobSystem();

    void start();
    void stop();

    bool enqueue(const JobRequest& request);
    void cancel(uint32_t jobId) noexcept;
    void publishSyntheticResult(const JobResult& result) noexcept;

    bool tryPopResult(JobResult& out) noexcept;

private:
    static void onWorkerResult(void* context, const JobResult& result);

    JobQueue queue;
    JobResultRing<256> results;
    JobWorker worker;
    std::atomic<bool> started { false };
};

}
