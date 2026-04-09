#pragma once
#include "TrackTagData.h"

class TagWriterService
{
public:
    static bool saveTagsToFile(const TrackTagData& data);
};
