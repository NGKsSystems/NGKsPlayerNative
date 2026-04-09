#include "ui/library/LibraryScanner.h"

#include <QDir>
#include <QDirIterator>
#include <QFile>
#include <QLoggingCategory>

// ── readId3Tags ───────────────────────────────────────────────────────────────
void readId3Tags(TrackInfo& track)
{
    if (!track.filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive)) return;
    QFile f(track.filePath);
    if (!f.open(QIODevice::ReadOnly)) return;
    const QByteArray hdr = f.read(10);
    if (hdr.size() < 10 || hdr[0] != 'I' || hdr[1] != 'D' || hdr[2] != '3') return;
    const int ver = static_cast<unsigned char>(hdr[3]);
    const quint32 tagSz = (quint32(static_cast<unsigned char>(hdr[6])) << 21)
                         | (quint32(static_cast<unsigned char>(hdr[7])) << 14)
                         | (quint32(static_cast<unsigned char>(hdr[8])) << 7)
                         | quint32(static_cast<unsigned char>(hdr[9]));
    const QByteArray tag = f.read(qMin(qint64(tagSz), qint64(256 * 1024)));
    int pos = 0;
    while (pos + 10 <= tag.size()) {
        const QByteArray fid = tag.mid(pos, 4);
        if (fid[0] == '\0') break;
        quint32 fsz;
        if (ver >= 4) {
            fsz = (quint32(static_cast<unsigned char>(tag[pos+4])) << 21)
                | (quint32(static_cast<unsigned char>(tag[pos+5])) << 14)
                | (quint32(static_cast<unsigned char>(tag[pos+6])) << 7)
                | quint32(static_cast<unsigned char>(tag[pos+7]));
        } else {
            fsz = (quint32(static_cast<unsigned char>(tag[pos+4])) << 24)
                | (quint32(static_cast<unsigned char>(tag[pos+5])) << 16)
                | (quint32(static_cast<unsigned char>(tag[pos+6])) << 8)
                | quint32(static_cast<unsigned char>(tag[pos+7]));
        }
        pos += 10;
        if (fsz == 0 || pos + static_cast<int>(fsz) > tag.size()) break;
        const bool isTextFrame = (fid == "TBPM" || fid == "TKEY" || fid == "TIT2"
                                  || fid == "TPE1" || fid == "TALB" || fid == "TLEN");
        if (isTextFrame && fsz > 1) {
            const unsigned char enc = static_cast<unsigned char>(tag[pos]);
            QString val;
            if (enc == 0 || enc == 3) {
                val = QString::fromUtf8(tag.mid(pos + 1, static_cast<int>(fsz) - 1)).trimmed();
                val.remove(QChar('\0'));
            } else if (enc == 1 || enc == 2) {
                const char* raw = tag.constData() + pos + 1;
                const int rawLen = static_cast<int>(fsz) - 1;
                if (rawLen >= 2) {
                    const auto b0 = static_cast<unsigned char>(raw[0]);
                    const auto b1 = static_cast<unsigned char>(raw[1]);
                    if (enc == 1 && b0 == 0xFF && b1 == 0xFE) {
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw + 2), (rawLen - 2) / 2).trimmed();
                    } else if (enc == 1 && b0 == 0xFE && b1 == 0xFF) {
                        QByteArray swapped(rawLen - 2, '\0');
                        for (int i = 0; i < rawLen - 2; i += 2) {
                            swapped[i] = raw[2 + i + 1];
                            swapped[i + 1] = raw[2 + i];
                        }
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(swapped.constData()), swapped.size() / 2).trimmed();
                    } else {
                        val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw), rawLen / 2).trimmed();
                    }
                    val.remove(QChar('\0'));
                }
            }
            if (!val.isEmpty()) {
                if (fid == "TBPM" && track.bpm.isEmpty()) {
                    track.bpm = val;
                    qInfo().noquote() << QStringLiteral("BPM_ANALYSIS_END source=id3_TBPM bpm=%1 path=%2")
                        .arg(val, track.filePath);
                }
                else if (fid == "TKEY" && track.musicalKey.isEmpty()) track.musicalKey = val;
                else if (fid == "TIT2" && track.title.isEmpty())       track.title = val;
                else if (fid == "TPE1" && track.artist.isEmpty())      track.artist = val;
                else if (fid == "TALB" && track.album.isEmpty())       track.album = val;
                else if (fid == "TLEN" && track.durationMs <= 0) {
                    bool ok = false;
                    const qint64 ms = val.toLongLong(&ok);
                    if (ok && ms > 0) {
                        track.durationMs  = ms;
                        track.durationStr = formatDurationMs(ms);
                    }
                }
            }
        }
        pos += static_cast<int>(fsz);
    }
    if (!track.artist.isEmpty() && !track.title.isEmpty()) {
        track.displayName = track.artist + QStringLiteral(" \u2014 ") + track.title;
    } else if (!track.title.isEmpty()) {
        track.displayName = track.title;
    }
}

// ── scanFolderForTracks ───────────────────────────────────────────────────────
std::vector<TrackInfo> scanFolderForTracks(const QString& folderPath)
{
    std::vector<TrackInfo> tracks;
    static const QStringList filters = {
        QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"),
        QStringLiteral("*.ogg"), QStringLiteral("*.aac"), QStringLiteral("*.m4a"),
        QStringLiteral("*.wma")
    };

    QDirIterator it(folderPath, filters, QDir::Files, QDirIterator::Subdirectories);
    while (it.hasNext()) {
        it.next();
        TrackInfo info;
        info.filePath = it.filePath();
        info.fileSize = it.fileInfo().size();
        const QString baseName = it.fileInfo().completeBaseName();

        const int dashPos = baseName.indexOf(QStringLiteral(" - "));
        if (dashPos > 0) {
            info.artist      = baseName.left(dashPos).trimmed();
            info.title       = baseName.mid(dashPos + 3).trimmed();
            info.displayName = info.artist + QStringLiteral(" \u2014 ") + info.title;
        } else {
            info.title       = baseName;
            info.displayName = baseName;
        }
        readId3Tags(info);
        tracks.push_back(std::move(info));
    }
    return tracks;
}
