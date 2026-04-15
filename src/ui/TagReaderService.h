#pragma once
#include "TrackTagData.h"

class TagReaderService
{
public:
    static TrackTagData loadTagsForFile(const QString& filePath);
    static TrackTagData loadTagsForFile(const QString& filePath, bool skipAlbumArt);
};
