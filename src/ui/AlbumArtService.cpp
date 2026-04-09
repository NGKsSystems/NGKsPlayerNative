#include "AlbumArtService.h"
#include "TagReaderService.h"
#include "TagWriterService.h"
#include <QFile>
#include <QFileInfo>
#include <QDir>
#include <QBuffer>
#include <QImage>

// ── extractEmbeddedAlbumArt ────────────────────────────────────────
QPixmap AlbumArtService::extractEmbeddedAlbumArt(const QString& filePath)
{
    TrackTagData data = TagReaderService::loadTagsForFile(filePath);
    return data.albumArt; // may be null if no embedded art
}

// ── findFolderAlbumArt ─────────────────────────────────────────────
// Strict order: cover.jpg → folder.jpg → front.jpg → .png variants
QPixmap AlbumArtService::findFolderAlbumArt(const QString& filePath)
{
    const QDir dir = QFileInfo(filePath).absoluteDir();
    static const QStringList candidates = {
        QStringLiteral("cover.jpg"),
        QStringLiteral("folder.jpg"),
        QStringLiteral("front.jpg"),
        QStringLiteral("cover.png"),
        QStringLiteral("folder.png"),
        QStringLiteral("front.png"),
    };

    for (const auto& name : candidates) {
        const QString path = dir.filePath(name);
        if (QFile::exists(path)) {
            QPixmap pm;
            if (pm.load(path))
                return pm;
        }
    }

    // Case-insensitive fallback: scan directory for cover/folder/front images
    const QStringList entries = dir.entryList(
        {QStringLiteral("*.jpg"), QStringLiteral("*.jpeg"), QStringLiteral("*.png"),
         QStringLiteral("*.bmp")},
        QDir::Files);
    for (const auto& entry : entries) {
        const QString lower = entry.toLower();
        if (lower.startsWith(QStringLiteral("cover")) ||
            lower.startsWith(QStringLiteral("folder")) ||
            lower.startsWith(QStringLiteral("front"))) {
            QPixmap pm;
            if (pm.load(dir.filePath(entry)))
                return pm;
        }
    }

    return {}; // no folder art found
}

// ── writeAlbumArt ──────────────────────────────────────────────────
bool AlbumArtService::writeAlbumArt(const QString& filePath, const QPixmap& art)
{
    if (art.isNull()) return false;

    TrackTagData data = TagReaderService::loadTagsForFile(filePath);
    data.albumArt = art;
    data.hasAlbumArt = true;
    return TagWriterService::saveTagsToFile(data);
}

// ── removeAlbumArt ─────────────────────────────────────────────────
bool AlbumArtService::removeAlbumArt(const QString& filePath)
{
    TrackTagData data = TagReaderService::loadTagsForFile(filePath);
    data.albumArt = QPixmap();
    data.hasAlbumArt = false;
    return TagWriterService::saveTagsToFile(data);
}
