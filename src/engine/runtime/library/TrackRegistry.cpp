#include "engine/runtime/library/TrackRegistry.h"

namespace ngks {

void TrackRegistry::upsertTrackMeta(uint64_t trackId, const TrackMeta& meta)
{
    std::lock_guard<std::mutex> guard(mutex);
    auto& entry = entries[trackId];
    entry.track = meta;
    entry.track.trackId = trackId;
}

void TrackRegistry::updateAnalysis(uint64_t trackId, const AnalysisMeta& analysis)
{
    std::lock_guard<std::mutex> guard(mutex);
    auto& entry = entries[trackId];
    entry.track.trackId = trackId;
    entry.analysis = analysis;
    entry.hasAnalysis = true;
}

bool TrackRegistry::getAnalysis(uint64_t trackId, AnalysisMeta& out) const
{
    std::lock_guard<std::mutex> guard(mutex);
    const auto it = entries.find(trackId);
    if (it == entries.end() || !it->second.hasAnalysis) {
        return false;
    }

    out = it->second.analysis;
    return true;
}

void TrackRegistry::importEntry(const RegistryEntrySnapshot& entry)
{
    std::lock_guard<std::mutex> guard(mutex);
    auto& target = entries[entry.track.trackId];
    target.track = entry.track;
    target.analysis = entry.analysis;
    target.hasAnalysis = (entry.hasAnalysis != 0);
}

std::vector<RegistryEntrySnapshot> TrackRegistry::exportEntries() const
{
    std::vector<RegistryEntrySnapshot> out;
    std::lock_guard<std::mutex> guard(mutex);
    out.reserve(entries.size());

    for (const auto& pair : entries) {
        RegistryEntrySnapshot snapshot {};
        snapshot.track = pair.second.track;
        snapshot.analysis = pair.second.analysis;
        snapshot.hasAnalysis = pair.second.hasAnalysis ? 1 : 0;
        out.push_back(snapshot);
    }

    return out;
}

size_t TrackRegistry::count() const
{
    std::lock_guard<std::mutex> guard(mutex);
    return entries.size();
}

}
