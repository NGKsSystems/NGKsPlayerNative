#include "TagWriterService.h"
#include <QFile>
#include <QBuffer>
#include <QDir>
#include <QImage>
#include <cstring>

// ── ID3v2 frame/tag building helpers ───────────────────────────────

// Syncsafe-encode a 28-bit value into 4 bytes
static QByteArray syncsafeEncode(quint32 value)
{
    QByteArray out(4, '\0');
    out[0] = static_cast<char>((value >> 21) & 0x7F);
    out[1] = static_cast<char>((value >> 14) & 0x7F);
    out[2] = static_cast<char>((value >>  7) & 0x7F);
    out[3] = static_cast<char>( value        & 0x7F);
    return out;
}

// Big-endian encode 4 bytes (ID3v2.3 frame size)
static QByteArray bigEndian32(quint32 value)
{
    QByteArray out(4, '\0');
    out[0] = static_cast<char>((value >> 24) & 0xFF);
    out[1] = static_cast<char>((value >> 16) & 0xFF);
    out[2] = static_cast<char>((value >>  8) & 0xFF);
    out[3] = static_cast<char>( value        & 0xFF);
    return out;
}

// Syncsafe-decode for reading existing tags
static quint32 syncsafeDecode(const QByteArray& d, int off)
{
    return (quint32(static_cast<unsigned char>(d[off + 0])) << 21)
         | (quint32(static_cast<unsigned char>(d[off + 1])) << 14)
         | (quint32(static_cast<unsigned char>(d[off + 2])) << 7)
         |  quint32(static_cast<unsigned char>(d[off + 3]));
}

static quint32 bigEndianDecode(const QByteArray& d, int off)
{
    return (quint32(static_cast<unsigned char>(d[off + 0])) << 24)
         | (quint32(static_cast<unsigned char>(d[off + 1])) << 16)
         | (quint32(static_cast<unsigned char>(d[off + 2])) << 8)
         |  quint32(static_cast<unsigned char>(d[off + 3]));
}

// Build a text frame (ID3v2.3, encoding=3 UTF-8)
static QByteArray buildTextFrame(const QByteArray& frameId, const QString& text)
{
    if (text.isEmpty()) return {};
    const QByteArray utf8 = text.toUtf8();
    const quint32 dataSz = 1 + static_cast<quint32>(utf8.size()); // encoding + text

    QByteArray frame;
    frame.append(frameId);               // 4 bytes: frame ID
    frame.append(bigEndian32(dataSz));    // 4 bytes: frame size (v2.3 big-endian)
    frame.append(2, '\0');               // 2 bytes: flags
    frame.append(char(3));               // 1 byte: encoding = UTF-8
    frame.append(utf8);                  // text
    return frame;
}

// Build COMM frame (comment)
static QByteArray buildCommentFrame(const QString& text)
{
    if (text.isEmpty()) return {};
    const QByteArray utf8 = text.toUtf8();
    // encoding(1) + language(3) + short_desc(1 null) + text
    const quint32 dataSz = 1 + 3 + 1 + static_cast<quint32>(utf8.size());

    QByteArray frame;
    frame.append("COMM", 4);
    frame.append(bigEndian32(dataSz));
    frame.append(2, '\0');             // flags
    frame.append(char(3));             // encoding = UTF-8
    frame.append("eng", 3);           // language
    frame.append(char('\0'));          // empty short description
    frame.append(utf8);
    return frame;
}

// Build APIC frame (album art as front cover JPEG)
static QByteArray buildApicFrame(const QPixmap& art)
{
    if (art.isNull()) return {};

    QByteArray jpegData;
    QBuffer buf(&jpegData);
    buf.open(QIODevice::WriteOnly);
    art.toImage().save(&buf, "JPEG", 90);
    buf.close();

    if (jpegData.isEmpty()) return {};

    // encoding(1) + "image/jpeg\0" + picType(1) + desc("\0") + imageData
    const QByteArray mime("image/jpeg");
    const quint32 dataSz = 1 + static_cast<quint32>(mime.size()) + 1 + 1 + 1
                         + static_cast<quint32>(jpegData.size());

    QByteArray frame;
    frame.append("APIC", 4);
    frame.append(bigEndian32(dataSz));
    frame.append(2, '\0');             // flags
    frame.append(char(0));             // encoding = ISO-8859-1
    frame.append(mime);
    frame.append(char('\0'));          // MIME null terminator
    frame.append(char(3));             // picture type = front cover
    frame.append(char('\0'));          // empty description
    frame.append(jpegData);
    return frame;
}

// ── Known frame IDs that we manage ──
static const QList<QByteArray> kManagedFrameIds = {
    "TIT2", "TPE1", "TPE2", "TALB", "TCON", "TYER", "TDRC",
    "TRCK", "TPOS", "TBPM", "TKEY", "COMM", "APIC"
};

static bool isManagedFrame(const QByteArray& id)
{
    return kManagedFrameIds.contains(id);
}

// Collect unknown (passthrough) frames from the original tag
static QByteArray collectPassthroughFrames(const QByteArray& tag, int ver)
{
    QByteArray result;
    int pos = 0;
    while (pos + 10 <= tag.size()) {
        const QByteArray fid = tag.mid(pos, 4);
        if (fid[0] == '\0') break;

        quint32 fsz;
        if (ver >= 4)
            fsz = syncsafeDecode(tag, pos + 4);
        else
            fsz = bigEndianDecode(tag, pos + 4);

        if (fsz == 0 || pos + 10 + static_cast<int>(fsz) > tag.size()) break;

        if (!isManagedFrame(fid)) {
            // Copy entire frame (header + data) verbatim
            result.append(tag.mid(pos, 10 + static_cast<int>(fsz)));
        }
        pos += 10 + static_cast<int>(fsz);
    }
    return result;
}

// ── Public API ─────────────────────────────────────────────────────

bool TagWriterService::saveTagsToFile(const TrackTagData& data)
{
    if (data.sourceFilePath.isEmpty()) return false;
    if (!data.sourceFilePath.endsWith(QStringLiteral(".mp3"), Qt::CaseInsensitive))
        return false;

    // 1. Read entire original file
    QFile srcFile(data.sourceFilePath);
    if (!srcFile.open(QIODevice::ReadOnly))
        return false;
    const QByteArray original = srcFile.readAll();
    srcFile.close();

    if (original.isEmpty()) return false;

    // 2. Locate audio data start (skip existing ID3v2 tag)
    int audioStart = 0;
    int originalVer = 3;
    QByteArray originalTagBody;
    if (original.size() >= 10 && original[0] == 'I' && original[1] == 'D' && original[2] == '3') {
        originalVer = static_cast<unsigned char>(original[3]);
        const quint32 tagSz = syncsafeDecode(original, 6);
        audioStart = 10 + static_cast<int>(tagSz);
        originalTagBody = original.mid(10, static_cast<int>(tagSz));
    }

    // 3. Build new frames from TrackTagData
    QByteArray newFrames;
    newFrames.append(buildTextFrame("TIT2", data.title));
    newFrames.append(buildTextFrame("TPE1", data.artist));
    newFrames.append(buildTextFrame("TPE2", data.albumArtist));
    newFrames.append(buildTextFrame("TALB", data.album));
    newFrames.append(buildTextFrame("TCON", data.genre));
    newFrames.append(buildTextFrame("TYER", data.year));
    newFrames.append(buildTextFrame("TRCK", data.trackNumber));
    newFrames.append(buildTextFrame("TPOS", data.discNumber));
    newFrames.append(buildTextFrame("TBPM", data.bpm));
    newFrames.append(buildTextFrame("TKEY", data.musicalKey));
    newFrames.append(buildCommentFrame(data.comments));
    if (data.hasAlbumArt && !data.albumArt.isNull())
        newFrames.append(buildApicFrame(data.albumArt));

    // 4. Preserve unknown frames from original tag
    newFrames.append(collectPassthroughFrames(originalTagBody, originalVer));

    // 5. Add 1024 bytes padding
    const int padding = 1024;
    const quint32 totalTagBody = static_cast<quint32>(newFrames.size()) + padding;

    // 6. Build ID3v2.3 header
    QByteArray header(10, '\0');
    header[0] = 'I'; header[1] = 'D'; header[2] = '3';
    header[3] = char(3); // version 2.3
    header[4] = char(0); // revision
    header[5] = char(0); // flags
    const QByteArray szEnc = syncsafeEncode(totalTagBody);
    header[6] = szEnc[0]; header[7] = szEnc[1];
    header[8] = szEnc[2]; header[9] = szEnc[3];

    // 7. Write to temp file then replace
    const QString tmpPath = data.sourceFilePath + QStringLiteral(".ngks_tmp");
    {
        QFile tmp(tmpPath);
        if (!tmp.open(QIODevice::WriteOnly))
            return false;
        tmp.write(header);
        tmp.write(newFrames);
        tmp.write(QByteArray(padding, '\0'));
        if (audioStart < original.size())
            tmp.write(original.mid(audioStart));
        tmp.close();
    }

    // 8. Replace original: remove original, rename temp
    if (!QFile::remove(data.sourceFilePath)) {
        QFile::remove(tmpPath);
        return false;
    }
    if (!QFile::rename(tmpPath, data.sourceFilePath)) {
        return false;
    }

    return true;
}
