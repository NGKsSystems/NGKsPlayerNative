import re

file_path = r'src\ui\main.cpp'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

PATCH = """
                  // Phase 3 Core DB Duration Patch: Load duration from dj_library_core.db directly
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
                          qWarning().noquote() << QStringLiteral("CORE_DB_DURATION_PATCH DB_NOT_FOUND ") << coreDbPath;
                      }
                  }
                  
"""

# Inject before djDb_.bulkInsert(allTracks_);
if 'CORE_DB_DURATION_PATCH' not in content:
    parts = content.split('djDb_.bulkInsert(allTracks_);')
    if len(parts) > 1:
        new_content = parts[0]
        for i in range(1, len(parts)):
            new_content += PATCH + 'djDb_.bulkInsert(allTracks_);' + parts[i]
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Patched {len(parts)-1} occurrences of bulkInsert!")
    else:
        print("bulkInsert not found.")
else:
    print("Already patched.")
