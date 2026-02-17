#pragma once

class Limiter
{
public:
    float processSample(float sample) const noexcept;
};