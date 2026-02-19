#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <juce_audio_devices/juce_audio_devices.h>

class EngineCore;

class AudioIOJuce final : public juce::AudioIODeviceCallback
{
public:
    struct DeviceInfo
    {
        std::string deviceId;
        std::string deviceName;
        std::string backendType;
        int inputChannels = 0;
        int outputChannels = 0;
    };

    struct StartRequest
    {
        std::string preferredDeviceId;
        std::string preferredDeviceName;
        int preferredBufferSize = 128;
        double preferredSampleRate = 0.0;
        int preferredInputChannels = 0;
        int preferredOutputChannels = 2;
    };

    struct StartResult
    {
        bool ok = false;
        std::string message;
        std::string deviceId;
        std::string deviceName;
        double requestedSampleRate = 0.0;
        int requestedOutputChannels = 2;
        int requestedBufferSize = 128;
        bool fallbackUsed = false;
        int actualBufferSize = 0;
        int inputChannels = 0;
        int outputChannels = 0;
        double sampleRate = 0.0;
        uint64_t deviceIdHash = 0;
    };

    static std::vector<DeviceInfo> listAudioDevices();
    static uint64_t hashDeviceId(const std::string& value) noexcept;

    explicit AudioIOJuce(EngineCore& engineCoreRef);
    ~AudioIOJuce() override;

    StartResult start(const StartRequest& request = {});
    void stop();

    void audioDeviceIOCallbackWithContext(const float* const* inputChannelData,
                                          int numInputChannels,
                                          float* const* outputChannelData,
                                          int numOutputChannels,
                                          int numSamples,
                                          const juce::AudioIODeviceCallbackContext& context) override;
    void audioDeviceAboutToStart(juce::AudioIODevice* device) override;
    void audioDeviceStopped() override;

private:
    EngineCore& engineCore;
    juce::AudioDeviceManager deviceManager;
    bool callbackAdded = false;
};