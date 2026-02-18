#pragma once

#include <cstddef>
#include <string>

#include "engine/runtime/library/TrackRegistry.h"

namespace ngks {

class RegistryStore {
public:
    RegistryStore();

    size_t load(TrackRegistry& registry) const;
    bool save(const TrackRegistry& registry) const;
    const std::string& pathString() const noexcept;

private:
    std::string storePath;
};

}
