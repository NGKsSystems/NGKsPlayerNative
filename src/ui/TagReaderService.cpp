#include "TagReaderService.h"
#include <QFile>
#include <QFileInfo>

// ── ID3v2 helpers ──────────────────────────────────────────────────
static quint32 syncsafeDecode(const QByteArray& d, int off)
{
    return (quint32(static_cast<unsigned char>(d[off + 0])) << 21)
         | (quint32(static_cast<unsigned char>(d[off + 1])) << 14)
         | (quint32(static_cast<unsigned char>(d[off + 2])) << 7)
         |  quint32(static_cast<unsigned char>(d[off + 3]));
}

static quint32 bigEndian32(const QByteArray& d, int off)
{
    return (quint32(static_cast<unsigned char>(d[off + 0])) << 24)
         | (quint32(static_cast<unsigned char>(d[off + 1])) << 16)
         | (quint32(static_cast<unsigned char>(d[off + 2])) << 8)
         |  quint32(static_cast<unsigned char>(d[off + 3]));
}

static QString decodeId3Text(const QByteArray& data, int offset, int length)
{
    if (length < 1) return {};
    const auto enc = static_cast<unsigned char>(data[offset]);
    const char* raw = data.constData() + offset + 1;
    const int rawLen = length - 1;
    if (rawLen <= 0) return {};

    QString val;
    if (enc == 0 || enc == 3) {
        // ISO-8859-1 or UTF-8
        val = (enc == 3) ? QString::fromUtf8(raw, rawLen)
                         : QString::fromLatin1(raw, rawLen);
    } else if ((enc == 1 || enc == 2) && rawLen >= 2) {
        // UTF-16
        const auto b0 = static_cast<unsigned char>(raw[0]);
        const auto b1 = static_cast<unsigned char>(raw[1]);
        if (enc == 1 && b0 == 0xFF && b1 == 0xFE) {
            val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw + 2),
                                     (rawLen - 2) / 2);
        } else if (enc == 1 && b0 == 0xFE && b1 == 0xFF) {
            QByteArray swapped(rawLen - 2, '\0');
            for (int i = 0; i < rawLen - 2; i += 2) {
                swapped[i]     = raw[2 + i + 1];
                swapped[i + 1] = raw[2 + i];
            }
            val = QString::fromUtf16(reinterpret_cast<const char16_t*>(swapped.constData()),
                                     swapped.size() / 2);
        } else {
            val = QString::fromUtf16(reinterpret_cast<const char16_t*>(raw), rawLen / 2);
        }
    }
    val.remove(QChar('\0'));
    return val.trimmed();
}

// ── Public API ─────────────────────────────────────────────────────
TrackTagData TagReaderService::loadTagsForFile(const QString& filePath)
{
    return loadTagsForFile(filePath, false);
}

TrackTagData TagReaderService::loadTagsForFile(const QString& filePath, bool skipAlbumArt)
{
    TrackTagData data;
    data.sourceFilePath = filePath;

    if (!filePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive))
        return data;

    QFile f(filePath);
    if (!f.open(QIODevice::ReadOnly))
        return data;

    const QByteArray hdr = f.read(10);
    if (hdr.size() < 10 || hdr[0] != 'I' || hdr[1] != 'D' || hdr[2] != '3')
        return data;

    const int ver = static_cast<unsigned char>(hdr[3]);
    const quint32 tagSz = syncsafeDecode(hdr, 6);
    const QByteArray tag = f.read(qMin(qint64(tagSz), qint64(1024 * 1024)));
    f.close();

    int pos = 0;
    while (pos + 10 <= tag.size()) {
        const QByteArray fid = tag.mid(pos, 4);
        if (fid[0] == '\0') break;

        quint32 fsz;
        if (ver >= 4)
            fsz = syncsafeDecode(tag, pos + 4);
        else
            fsz = bigEndian32(tag, pos + 4);

        pos += 10; // skip frame header (4 id + 4 size + 2 flags)
        if (fsz == 0 || pos + static_cast<int>(fsz) > tag.size()) break;

        // ── Text frames ──
        if (fid == "TIT2" && data.title.isEmpty())
            data.title = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TPE1" && data.artist.isEmpty())
            data.artist = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TPE2" && data.albumArtist.isEmpty())
            data.albumArtist = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TALB" && data.album.isEmpty())
            data.album = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TCON" && data.genre.isEmpty())
            data.genre = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if ((fid == "TYER" || fid == "TDRC") && data.year.isEmpty())
            data.year = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TRCK" && data.trackNumber.isEmpty())
            data.trackNumber = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TPOS" && data.discNumber.isEmpty())
            data.discNumber = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TBPM" && data.bpm.isEmpty())
            data.bpm = decodeId3Text(tag, pos, static_cast<int>(fsz));
        else if (fid == "TKEY" && data.musicalKey.isEmpty())
            data.musicalKey = decodeId3Text(tag, pos, static_cast<int>(fsz));
        // ── COMM (comment) ──
        else if (fid == "COMM" && data.comments.isEmpty() && fsz > 4) {
            // encoding(1) + language(3) + short_desc(null-term) + text
            int cpos = pos + 1 + 3; // skip encoding + language
            const int cend = pos + static_cast<int>(fsz);
            // skip short content description
            while (cpos < cend && tag[cpos] != '\0') cpos++;
            cpos++; // skip null
            if (cpos < cend) {
                const auto enc = static_cast<unsigned char>(tag[pos]);
                if (enc == 0 || enc == 3)
                    data.comments = QString::fromUtf8(tag.constData() + cpos, cend - cpos).trimmed();
                else if (enc == 1 || enc == 2)
                    data.comments = QString::fromUtf16(
                        reinterpret_cast<const char16_t*>(tag.constData() + cpos),
                        (cend - cpos) / 2).trimmed();
                data.comments.remove(QChar('\0'));
            }
        }
        // ── APIC (album art) ──
        else if (fid == "APIC" && !data.hasAlbumArt && fsz > 10 && !skipAlbumArt) {
            int apos = pos;
            const int aend = pos + static_cast<int>(fsz);
            apos++; // skip encoding byte
            // skip MIME type string
            while (apos < aend && tag[apos] != '\0') apos++;
            apos++; // skip null
            apos++; // skip picture type byte
            // skip description
            while (apos < aend && tag[apos] != '\0') apos++;
            apos++; // skip null
            if (apos < aend) {
                const int imgLen = aend - apos;
                QPixmap pm;
                if (pm.loadFromData(reinterpret_cast<const uchar*>(tag.constData() + apos), imgLen)) {
                    data.albumArt = pm;
                    data.hasAlbumArt = true;
                }
            }
        }

        pos += static_cast<int>(fsz);
    }

    // Build display name from filename if title/artist empty
    if (data.title.isEmpty()) {
        QFileInfo fi(filePath);
        data.title = fi.completeBaseName();
    }

    return data;
}
