#pragma once

#include <cstdint>

#include "engine/domain/DeckId.h"
#include "engine/runtime/jobs/JobTypes.h"

namespace ngks {

struct JobResult {
    uint32_t jobId{0};
    DeckId deckId{0};
    JobType type{JobType::AnalyzeTrack};
    JobStatus status{JobStatus::Pending};
    uint8_t progress0_100{0};
    int32_t bpmFixed{0};
    int32_t loudness{0};
    int32_t deadAirMs{0};
    uint8_t stemsReady{0};
};

}
