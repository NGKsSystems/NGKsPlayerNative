import sys

file_path = r"c:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\src\ui\library\LegacyLibraryImport.cpp"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

old_query = """        QSqlQuery q(db);
        q.setForwardOnly(true);
        if (!q.exec(QStringLiteral(
            "SELECT filePath, genre, bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange, "
            "cueIn, cueOut, danceability, acousticness, instrumentalness, liveness, "
            "year, rating, comments, album, artist, title "
            "FROM tracks"))) {
            qWarning().noquote() << QStringLiteral("LEGACY_DB_QUERY_FAIL=%1").arg(q.lastError().text());
            db.close();
            return result;
        }

        while (q.next()) {"""

new_query = """        QSqlQuery q(db);
        q.setForwardOnly(true);
        bool hasDuration = false;
        
        if (!q.exec(QStringLiteral(
            "SELECT filePath, genre, bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange, "
            "cueIn, cueOut, danceability, acousticness, instrumentalness, liveness, "
            "year, rating, comments, album, artist, title, duration "
            "FROM tracks"))) {
            // Fallback for older DB versions without 'duration'
            if (!q.exec(QStringLiteral(
                "SELECT filePath, genre, bpm, key, camelotKey, energy, loudnessLUFS, loudnessRange, "
                "cueIn, cueOut, danceability, acousticness, instrumentalness, liveness, "
                "year, rating, comments, album, artist, title "
                "FROM tracks"))) {
                qWarning().noquote() << QStringLiteral("LEGACY_DB_QUERY_FAIL=%1").arg(q.lastError().text());
                db.close();
                return result;
            }
        } else {
            hasDuration = true;
        }

        while (q.next()) {"""

old_assign = """            if (t.rating  == 0)   t.rating  = q.value(15).toInt();
            if (t.comments.isEmpty()) t.comments = q.value(16).toString().trimmed();
            if (t.album.isEmpty())    t.album    = q.value(17).toString().trimmed();
            if (t.artist.isEmpty())   t.artist   = q.value(18).toString().trimmed();
            if (t.title.isEmpty())    t.title    = q.value(19).toString().trimmed();
        }"""

new_assign = """            if (t.rating  == 0)   t.rating  = q.value(15).toInt();
            if (t.comments.isEmpty()) t.comments = q.value(16).toString().trimmed();
            if (t.album.isEmpty())    t.album    = q.value(17).toString().trimmed();
            if (t.artist.isEmpty())   t.artist   = q.value(18).toString().trimmed();
            if (t.title.isEmpty())    t.title    = q.value(19).toString().trimmed();
            
            if (hasDuration && t.durationMs <= 0) {
                const double durSec = q.value(20).toDouble();
                if (durSec > 0.0) {
                    t.durationMs = static_cast<qint64>(durSec * 1000.0);
                    const int totalSec = static_cast<int>(t.durationMs / 1000);
                    t.durationStr = QStringLiteral("%1:%2").arg(totalSec / 60).arg(totalSec % 60, 2, 10, QLatin1Char('0'));
                }
            }
        }"""

content = content.replace(old_query, new_query)
content = content.replace(old_assign, new_assign)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("done")
