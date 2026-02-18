#pragma once

#include <cstdint>

namespace ngks {

enum class JobType : uint8_t {
    AnalyzeTrack = 0,
    StemsOffline = 1
};

enum class JobStatus : uint8_t {
    Pending = 0,
    Running = 1,
    Complete = 2,
    Cancelled = 3
};

}
