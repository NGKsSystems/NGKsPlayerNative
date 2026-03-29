#pragma once

#include <atomic>
#include <cstdio>
#include <cstdarg>
#include <cstring>
#include <mutex>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace ngks {

// ── Ring buffer for last-gasp trace capture ──
// Stores the last kTraceRingSize trace lines in memory so that
// if the app freezes/crashes we can dump them on next launch.
struct TraceRingBuffer {
    static constexpr int kTraceRingSize = 128;
    static constexpr int kTraceLineLen = 1200;

    char lines[kTraceRingSize][kTraceLineLen]{};
    std::atomic<int> writeIndex{0};
    std::atomic<bool> frozen{false}; // set true on freeze detection to stop overwriting

    void push(const char* line) noexcept {
        if (frozen.load(std::memory_order_relaxed)) return;
        const int idx = writeIndex.fetch_add(1, std::memory_order_relaxed) % kTraceRingSize;
        std::strncpy(lines[idx], line, kTraceLineLen - 1);
        lines[idx][kTraceLineLen - 1] = '\0';
    }

    void freeze() noexcept { frozen.store(true, std::memory_order_relaxed); }

    void dump(FILE* out) const noexcept {
        const int w = writeIndex.load(std::memory_order_relaxed);
        const int count = (w < kTraceRingSize) ? w : kTraceRingSize;
        const int start = (w < kTraceRingSize) ? 0 : (w % kTraceRingSize);
        std::fprintf(out, "=== TRACE RING DUMP (%d entries) ===\n", count);
        for (int i = 0; i < count; ++i) {
            const int idx = (start + i) % kTraceRingSize;
            if (lines[idx][0] != '\0') {
                std::fprintf(out, "  [%d] %s\n", i, lines[idx]);
            }
        }
        std::fprintf(out, "=== END TRACE RING DUMP ===\n");
        std::fflush(out);
    }

    void dumpToFile(const char* path) const noexcept {
        FILE* f = fopen(path, "w");
        if (!f) return;
        dump(f);
        fclose(f);
    }
};

inline TraceRingBuffer& traceRing() noexcept {
    static TraceRingBuffer ring;
    return ring;
}

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

/// Terminal-visible structured trace log for device disconnect diagnostics.
/// Writes to BOTH stderr (immediate flush for terminal visibility) and diag_juce.log.
/// Also pushes into the in-memory ring buffer for last-gasp capture.
/// Format: [AUDIO_TRACE ts=HH:MM:SS.mmm tid=####] EVENT key=value ...
inline void audioTrace(const char* event, const char* fmt, ...) noexcept
{
    static std::mutex mtx;

    // Timestamp
    char tsBuf[16];
#ifdef _WIN32
    SYSTEMTIME st;
    GetLocalTime(&st);
    snprintf(tsBuf, sizeof(tsBuf), "%02d:%02d:%02d.%03d",
             st.wHour, st.wMinute, st.wSecond, st.wMilliseconds);
    unsigned long tid = GetCurrentThreadId();
#else
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);
    snprintf(tsBuf, sizeof(tsBuf), "%02d:%02d:%02d.%03d",
             tm.tm_hour, tm.tm_min, tm.tm_sec, (int)(ts.tv_nsec / 1000000));
    unsigned long tid = (unsigned long)pthread_self();
#endif

    // Format user message
    char msgBuf[1024];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msgBuf, sizeof(msgBuf), fmt, args);
    va_end(args);

    // Build full line
    char lineBuf[1200];
    snprintf(lineBuf, sizeof(lineBuf),
             "[AUDIO_TRACE ts=%s tid=%lu] %s %s",
             tsBuf, tid, event, msgBuf);

    // Push into ring buffer (lock-free write index)
    traceRing().push(lineBuf);

    std::lock_guard<std::mutex> lock(mtx);

    // stderr — terminal-visible, immediate flush
    std::fprintf(stderr, "%s\n", lineBuf);
    std::fflush(stderr);

    // Mirror to file
    FILE* f = fopen("data/runtime/diag_juce.log", "a");
    if (f) {
        std::fprintf(f, "%s\n", lineBuf);
        std::fflush(f);
        fclose(f);
    }
}

} // namespace ngks
