#pragma once

#include "ui/library/LibraryPersistence.h"
#include <vector>

// ── Normalize file path for matching ─────────────────────────────────────────
QString normalizePath(const QString& raw);

// ── Locate the legacy ngksplayer library.db ───────────────────────────────────
QString findLegacyDbPath();

// ── LegacyImportResult ────────────────────────────────────────────────────────
struct LegacyImportResult {
    int     matched{0};
    int     unmatched{0};
    int     totalDbRows{0};
    QString dbPath;
};

// ── Import a legacy SQLite library into a track list ─────────────────────────
LegacyImportResult importLegacyDb(std::vector<TrackInfo>& tracks, const QString& dbPath);
// ── Phase 3 Core DB Duration Patch ───────────────────────────────────
void applyCoreDurationPatch(std::vector<TrackInfo>& tracks);