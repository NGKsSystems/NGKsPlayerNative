#pragma once
#include <QPixmap>
#include <QString>

class AlbumArtService
{
public:
    static QPixmap extractEmbeddedAlbumArt(const QString& filePath);
    static QPixmap findFolderAlbumArt(const QString& filePath);
    static bool    writeAlbumArt(const QString& filePath, const QPixmap& art);
    static bool    removeAlbumArt(const QString& filePath);
};
