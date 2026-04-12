import sys

file_path = r"c:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\src\ui\main.cpp"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# We'll inject a direct read from dj_library_core.db right after importLegacyDb
old_code = """                  // Temporary bypass to force duration update from DB without deleting JSON
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

new_code = """                  // Temporary bypass to force duration update from DB without deleting JSON
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
                  }
                  
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db
                  {
                      QString coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../../../data/dj_library_core.db");
                      }
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QDir::currentPath() + QStringLiteral("/../data/dj_library_core.db");
                      }
                      // Just in case we're deep in the build folder:
                      if (!QFile::exists(coreDbPath)) {
                          coreDbPath = QStringLiteral("C:/Users/suppo/Desktop/NGKsSystems/NGKsPlayerNative/data/dj_library_core.db");
                      }

                      if (QFile::exists(coreDbPath)) {
                          const QString connName = QStringLiteral("dj_core_duration_fix");
                          {
                              QSqlDatabase coreDb = QSqlDatabase::addDatabase(QStringLiteral("QSQLITE"), connName);
                              coreDb.setDatabaseName(coreDbPath);
                              coreDb.setConnectOptions(QStringLiteral("QSQLITE_OPEN_READONLY"));
                              int matchCore = 0;
                              if (coreDb.open()) {
                                  std::map<QString, size_t> pathIndex;
                                  for (size_t i = 0; i < allTracks_.size(); ++i) {
                                      pathIndex[QDir::fromNativeSeparators(allTracks_[i].filePath).trimmed().toLower()] = i;
                                  }
                                  
                                  QSqlQuery q(coreDb);
                                  q.setForwardOnly(true);
                                  if (q.exec(QStringLiteral("SELECT file_path, duration FROM tracks LIMIT 9999999"))) {
                                      while (q.next()) {
                                          QString fp = QDir::fromNativeSeparators(q.value(0).toString()).trimmed().toLower();
                                          double dur = q.value(1).toDouble();
                                          if (dur > 0) {
                                              auto it = pathIndex.find(fp);
                                              if (it != pathIndex.end()) {
                                                  TrackInfo& t = allTracks_[it->second];
                                                  if (t.durationMs <= 0 || t.durationStr == QStringLiteral("--:--") || t.durationStr.isEmpty()) {
                                                      t.durationMs = static_cast<qint64>(dur * 1000.0);
                                                      const int totalSec = static_cast<int>(dur);
                                                      t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                                                      matchCore++;
                                                  }
                                              }
                                          }
                                      }
                                      qInfo().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH matched=%1").arg(matchCore);
                                      if (matchCore > 0) {
                                          saveLibraryJson(allTracks_, importedFolderPath_);
                                      }
                                  } else {
                                      qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_QUERY ") << q.lastError().text();
                                  }
                                  coreDb.close();
                              } else {
                                  qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH FAILED_OPEN ") << coreDb.lastError().text();
                              }
                          }
                          QSqlDatabase::removeDatabase(connName);
                      } else {
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND");
                      }
                  }"""

content = content.replace(old_code, new_code)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("done_patch_duration")
