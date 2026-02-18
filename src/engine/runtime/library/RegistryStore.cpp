#include "engine/runtime/library/RegistryStore.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace ngks {

namespace {
constexpr const char* kRegistryRelativePath = "data/runtime/track_registry_v1.txt";
constexpr const char* kRegistryTempSuffix = ".tmp";
}

RegistryStore::RegistryStore()
    : storePath(kRegistryRelativePath)
{
}

size_t RegistryStore::load(TrackRegistry& registry) const
{
    namespace fs = std::filesystem;

    std::error_code ec;
    if (!fs::exists(storePath, ec)) {
        return 0;
    }

    std::ifstream input(storePath);
    if (!input.is_open()) {
        return 0;
    }

    size_t imported = 0;
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        std::stringstream ss(line);
        RegistryEntrySnapshot entry {};
        std::string label;
        std::string token;

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.track.trackId = static_cast<uint64_t>(std::stoull(token));

        if (!std::getline(ss, label, '|')) {
            continue;
        }
        for (size_t i = 0; i < sizeof(entry.track.label) - 1 && i < label.size(); ++i) {
            entry.track.label[i] = label[i];
        }

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.track.durationMs = static_cast<uint32_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.track.flags = static_cast<uint32_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.bpmFixed = static_cast<int32_t>(std::stoi(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.loudnessCentiDb = static_cast<int32_t>(std::stoi(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.deadAirMs = static_cast<uint32_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.stemsReady = static_cast<uint8_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.lastJobId = static_cast<uint32_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.analysis.status = static_cast<uint32_t>(std::stoul(token));

        if (!std::getline(ss, token, '|')) {
            continue;
        }
        entry.hasAnalysis = static_cast<uint8_t>(std::stoul(token));

        registry.importEntry(entry);
        ++imported;
    }

    return imported;
}

bool RegistryStore::save(const TrackRegistry& registry) const
{
    namespace fs = std::filesystem;

    std::error_code ec;
    fs::create_directories(fs::path(storePath).parent_path(), ec);

    const std::string tempPath = storePath + kRegistryTempSuffix;
    std::ofstream output(tempPath, std::ios::trunc);
    if (!output.is_open()) {
        return false;
    }

    const auto entries = registry.exportEntries();
    for (const auto& entry : entries) {
        output
            << entry.track.trackId << '|'
            << entry.track.label << '|'
            << entry.track.durationMs << '|'
            << entry.track.flags << '|'
            << entry.analysis.bpmFixed << '|'
            << entry.analysis.loudnessCentiDb << '|'
            << entry.analysis.deadAirMs << '|'
            << static_cast<uint32_t>(entry.analysis.stemsReady) << '|'
            << entry.analysis.lastJobId << '|'
            << entry.analysis.status << '|'
            << static_cast<uint32_t>(entry.hasAnalysis)
            << '\n';
    }

    output.close();

    fs::rename(tempPath, storePath, ec);
    if (ec) {
        fs::remove(storePath, ec);
        ec.clear();
        fs::rename(tempPath, storePath, ec);
    }

    return !ec;
}

const std::string& RegistryStore::pathString() const noexcept
{
    return storePath;
}

}
