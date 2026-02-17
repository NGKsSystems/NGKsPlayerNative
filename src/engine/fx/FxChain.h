#pragma once

#include "engine/fx/FxNode.h"

class FxChain
{
public:
    void process(float* left, float* right, int numSamples) noexcept
    {
        (void) left;
        (void) right;
        (void) numSamples;
    }
};