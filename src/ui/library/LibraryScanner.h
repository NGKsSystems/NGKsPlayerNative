#pragma once

#include "ui/library/LibraryPersistence.h"
#include <vector>

// ── ID3 tag reader ────────────────────────────────────────────────────────────
void readId3Tags(TrackInfo& track);

// ── Folder scanner ────────────────────────────────────────────────────────────
std::vector<TrackInfo> scanFolderForTracks(const QString& folderPath);
