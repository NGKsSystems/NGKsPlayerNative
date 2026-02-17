#pragma once
#include <queue>
#include <mutex>
#include "../command/Command.h"

namespace ngks {

class CommandQueue {
public:
    void enqueue(const Command& cmd) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(cmd);
    }

    bool tryDequeue(Command& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return false;
        out = queue_.front();
        queue_.pop();
        return true;
    }

private:
    std::queue<Command> queue_;
    std::mutex mutex_;
};

}