#pragma once

#include <cstdint>

#include "engine/domain/DeckId.h"
#include "engine/runtime/jobs/JobTypes.h"

namespace ngks {

struct JobRequest {
    uint32_t jobId{0};
    DeckId deckId{0};
    JobType type{JobType::AnalyzeTrack};
    uint64_t trackId{0};
    uint32_t param0{0};
    uint32_t param1{0};
};

}
