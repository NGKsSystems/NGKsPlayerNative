#pragma once

#include <cstdio>
#include <cstdarg>
#include <mutex>

namespace ngks {

inline void diagLog(const char* fmt, ...) noexcept
{
    static std::mutex mtx;
    std::lock_guard<std::mutex> lock(mtx);
    FILE* f = fopen("data/runtime/diag_juce.log", "a");
    if (!f) return;
    va_list args;
    va_start(args, fmt);
    vfprintf(f, fmt, args);
    va_end(args);
    fputc('\n', f);
    fflush(f);
    fclose(f);
}

} // namespace ngks
