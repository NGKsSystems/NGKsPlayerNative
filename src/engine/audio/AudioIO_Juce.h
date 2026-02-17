#pragma once

#include <string>

#include <juce_audio_devices/juce_audio_devices.h>

class EngineCore;

class AudioIOJuce final : public juce::AudioIODeviceCallback
{
public:
    struct StartResult
    {
        bool ok = false;
        std::string message;
        int requestedBufferSize = 128;
        int actualBufferSize = 0;
        double sampleRate = 0.0;
    };

    explicit AudioIOJuce(EngineCore& engineCoreRef);
    ~AudioIOJuce() override;

    StartResult start();
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