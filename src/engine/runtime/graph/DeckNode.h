#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include <juce_audio_formats/juce_audio_formats.h>

#include "engine/runtime/EngineSnapshot.h"

namespace ngks {

/// Min/max + RMS waveform bucket — true audio shape per time slice.
/// lo/hi preserve transient peaks; rms shows energy envelope.
struct WaveMinMax {
    float lo;   ///< most negative sample value in bucket
    float hi;   ///< most positive sample value in bucket
    float rms;  ///< root-mean-square energy of bucket
};

/// Broad frequency-band energy per time slice.
/// Lightweight analysis — NOT stem separation. Uses time-domain
/// inter-sample-difference technique to estimate spectral distribution.
struct BandEnergy {
    float low;      ///< low-frequency energy (bass / sub)
    float lowMid;   ///< low-mid energy (body / warmth)
    float highMid;  ///< high-mid energy (vocals / presence)
    float high;     ///< high-frequency energy (hats / cymbals / air)
};

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

    /// Generate a downsampled waveform overview using true min/max buckets.
    /// Returns a vector of `numBins` WaveMinMax pairs preserving peak/valley shape.
    /// Thread-safe: acquires shared_lock on bufferMutex_.
    std::vector<WaveMinMax> generateWaveformOverview(int numBins) const;

    /// Generate broad frequency-band energy overview.
    /// Lightweight time-domain analysis: inter-sample-difference partitioning.
    /// Thread-safe: acquires shared_lock on bufferMutex_.
    std::vector<BandEnergy> generateBandEnergyOverview(int numBins) const;

    /// Returns true once the full file (not just preload) is decoded.
    bool isFullyDecoded() const noexcept;

    /// Returns the file path currently loaded (empty if none).
    std::string loadedFilePath() const;

private:
    void cancelStreamDecode();

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

    // Shared mutex protects deck buffer state shared between control/UI and audio render paths.
    mutable std::shared_mutex bufferMutex_;
    bool hasAudioData_{false};
    bool diagFirstNonzero_{false}; // diagnostic: log first nonzero render

    // Streaming decode: background thread continues decoding after initial preload
    std::atomic<int64_t> streamDecodedFrames_{0};
    std::atomic<bool> streamCancelled_{false};
    std::thread streamDecodeThread_;

    // Track identity: file path of currently loaded audio
    std::string loadedFilePath_;
};

}