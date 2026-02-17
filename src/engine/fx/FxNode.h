#pragma once

class FxNode
{
public:
    virtual ~FxNode() = default;
    virtual void process(float*, float*, int) noexcept {}
};