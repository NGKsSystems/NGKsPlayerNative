#pragma once

#include <array>
#include <atomic>
#include <cstdint>

#include "engine/command/Command.h"

namespace ngks {

template <size_t Capacity>
class SPSCCommandRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power-of-two");

public:
    bool push(const Command& command) noexcept
    {
        const uint32_t write = writeIndex.load(std::memory_order_relaxed);
        const uint32_t nextWrite = (write + 1u) & mask;
        if (nextWrite == readIndex.load(std::memory_order_acquire)) {
            return false;
        }

        buffer[write] = command;
        writeIndex.store(nextWrite, std::memory_order_release);
        return true;
    }

    bool pop(Command& out) noexcept
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
    std::array<Command, Capacity> buffer {};
    std::atomic<uint32_t> writeIndex { 0 };
    std::atomic<uint32_t> readIndex { 0 };
};

}