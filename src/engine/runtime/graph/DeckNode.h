#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <juce_audio_formats/juce_audio_formats.h>

#include "engine/runtime/EngineSnapshot.h"

namespace ngks {

class DeckNode {
public:
    DeckNode();
    ~DeckNode();

    void prepare(double sampleRate);
    void beginStopFade(int fadeSamples) noexcept;
    bool isStopFadeActive() const noexcept;

    // Load a real audio file. Called from UI thread, NOT RT thread.
    // Returns true on success and fills outDurationSeconds.
    bool loadFile(const std::string& path, double& outDurationSeconds);

    // Unload any loaded audio buffer.
    void unloadFile() noexcept;

    // Seek to a position in the loaded file.
    void seekTo(double seconds) noexcept;

    void render(const DeckSnapshot& deck,
                int numSamples,
                float* outLeft,
                float* outRight,
                float& outRms,
                float& outPeak) noexcept;

    // Returns the playhead position in seconds based on the read cursor.
    double getPlayheadSeconds() const noexcept;

private:
    juce::AudioFormatManager formatManager_;

    // Decoded audio buffer (stereo interleaved: L0 R0 L1 R1 ...)
    std::vector<float> decodedL_;
    std::vector<float> decodedR_;
    int64_t totalDecodedFrames_{0};
    double fileSampleRate_{0.0};
    double fileDurationSeconds_{0.0};

    // RT-safe read position
    std::atomic<int64_t> readPosition_{0};

    // Resample ratio (file sample rate / device sample rate)
    double resampleRatio_{1.0};
    double fractionalReadPos_{0.0};

    double deviceSampleRate_{48000.0};
    int stopFadeSamplesRemaining{0};
    int stopFadeSamplesTotal{1};

    // Mutex protects buffer swap during loadFile/unloadFile
    std::mutex bufferMutex_;
    bool hasAudioData_{false};
    bool diagFirstNonzero_{false}; // diagnostic: log first nonzero render
};

}