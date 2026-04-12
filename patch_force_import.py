import sys

file_path = r"c:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\src\ui\main.cpp"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

old_code = """                  // Auto-merge legacy DB on restore if not already imported
                  bool anyLegacy = false;
                  for (const auto& t : allTracks_) {
                      if (t.legacyImported) { anyLegacy = true; break; }
                  }
                  if (!anyLegacy) {
                      const QString dbPath = findLegacyDbPath();
                      if (!dbPath.isEmpty()) {
                          const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
                          if (res.matched > 0) {
                              saveLibraryJson(allTracks_, importedFolderPath_);
                              qInfo().noquote() << QStringLiteral("LEGACY_DB_AUTO_IMPORT matched=%1 total=%2")
                                  .arg(res.matched).arg(res.totalDbRows);
                          }
                      }
                  }"""

new_code = """                  // Auto-merge legacy DB on restore if not already imported
                  bool anyLegacy = false;
                  for (const auto& t : allTracks_) {
                      if (t.legacyImported) { anyLegacy = true; break; }
                  }
                  // Temporary bypass to force duration update from DB without deleting JSON
                  {
                      const QString dbPath = findLegacyDbPath();
                      if (!dbPath.isEmpty()) {
                          const LegacyImportResult res = importLegacyDb(allTracks_, dbPath);
                          if (res.matched > 0) {
                              saveLibraryJson(allTracks_, importedFolderPath_);
                              qInfo().noquote() << QStringLiteral("LEGACY_DB_AUTO_IMPORT matched=%1 total=%2")
                                  .arg(res.matched).arg(res.totalDbRows);
                          }
                      }
                  }"""

content = content.replace(old_code, new_code)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("done")
