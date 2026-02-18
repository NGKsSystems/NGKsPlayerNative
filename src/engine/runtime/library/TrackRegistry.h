#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "engine/runtime/library/AnalysisMeta.h"
#include "engine/runtime/library/TrackMeta.h"

namespace ngks {

struct RegistryEntrySnapshot {
    TrackMeta track{};
    AnalysisMeta analysis{};
    uint8_t hasAnalysis{0};
};

class TrackRegistry {
public:
    void upsertTrackMeta(uint64_t trackId, const TrackMeta& meta);
    void updateAnalysis(uint64_t trackId, const AnalysisMeta& analysis);
    bool getAnalysis(uint64_t trackId, AnalysisMeta& out) const;

    void importEntry(const RegistryEntrySnapshot& entry);
    std::vector<RegistryEntrySnapshot> exportEntries() const;
    size_t count() const;

private:
    struct Entry {
        TrackMeta track{};
        AnalysisMeta analysis{};
        bool hasAnalysis{false};
    };

    mutable std::mutex mutex;
    std::unordered_map<uint64_t, Entry> entries;
};

}
