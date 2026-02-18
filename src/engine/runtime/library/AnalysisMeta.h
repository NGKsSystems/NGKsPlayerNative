#pragma once

#include <cstdint>

namespace ngks {

struct AnalysisMeta {
    int32_t bpmFixed{0};
    int32_t loudnessCentiDb{0};
    uint32_t deadAirMs{0};
    uint8_t stemsReady{0};
    uint32_t lastJobId{0};
    uint32_t status{0};
};

}
