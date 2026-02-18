#include "engine/runtime/offline/WavWriter.h"

#include <algorithm>

namespace ngks {

namespace {

void writeU16(std::ofstream& stream, uint16_t value)
{
    stream.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

void writeU32(std::ofstream& stream, uint32_t value)
{
    stream.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

} // namespace

bool WavWriter::open(const std::string& path, uint32_t sampleRate, uint16_t channels, OfflineWavFormat format)
{
    stream_.open(path, std::ios::binary | std::ios::trunc);
    if (!stream_.is_open()) {
        return false;
    }

    dataBytesWritten_ = 0;
    channels_ = channels;
    sampleRate_ = sampleRate;

    format_ = format;
    bitsPerSample_ = (format_ == OfflineWavFormat::Float32) ? 32 : 16;

    const uint16_t blockAlign = static_cast<uint16_t>(channels_ * (bitsPerSample_ / 8));
    const uint32_t byteRate = sampleRate_ * static_cast<uint32_t>(blockAlign);

    stream_.write("RIFF", 4);
    writeU32(stream_, 0);
    stream_.write("WAVE", 4);

    stream_.write("fmt ", 4);
    writeU32(stream_, 16);
    writeU16(stream_, formatCode());
    writeU16(stream_, channels_);
    writeU32(stream_, sampleRate_);
    writeU32(stream_, byteRate);
    writeU16(stream_, blockAlign);
    writeU16(stream_, bitsPerSample_);

    stream_.write("data", 4);
    writeU32(stream_, 0);

    return stream_.good();
}

bool WavWriter::writeInterleaved(const float* interleaved, uint32_t frames)
{
    if (!stream_.is_open() || interleaved == nullptr || channels_ != 2) {
        return false;
    }

    for (uint32_t frame = 0; frame < frames; ++frame) {
        for (uint16_t channel = 0; channel < channels_; ++channel) {
            const float input = interleaved[frame * channels_ + channel];
            if (format_ == OfflineWavFormat::Float32) {
                const float clamped = std::clamp(input, -1.0f, 1.0f);
                stream_.write(reinterpret_cast<const char*>(&clamped), sizeof(clamped));
                dataBytesWritten_ += sizeof(clamped);
            } else {
                const float clamped = std::clamp(input, -1.0f, 1.0f);
                const int16_t pcm = static_cast<int16_t>(clamped * 32767.0f);
                stream_.write(reinterpret_cast<const char*>(&pcm), sizeof(pcm));
                dataBytesWritten_ += sizeof(pcm);
            }
        }
    }

    return stream_.good();
}

bool WavWriter::finalize()
{
    if (!stream_.is_open()) {
        return false;
    }

    const uint32_t riffChunkSize = 36u + dataBytesWritten_;
    stream_.seekp(4, std::ios::beg);
    writeU32(stream_, riffChunkSize);
    stream_.seekp(40, std::ios::beg);
    writeU32(stream_, dataBytesWritten_);
    stream_.close();
    return true;
}

uint16_t WavWriter::formatCode() const noexcept
{
    return static_cast<uint16_t>(format_);
}

uint16_t WavWriter::bitsPerSample() const noexcept
{
    return bitsPerSample_;
}

uint16_t WavWriter::blockAlign() const noexcept
{
    return static_cast<uint16_t>(channels_ * (bitsPerSample_ / 8));
}

}
