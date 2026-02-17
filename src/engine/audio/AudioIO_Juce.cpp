#include "engine/audio/AudioIO_Juce.h"

#include <algorithm>

AudioIOJuce::AudioIOJuce(EngineCore& engineCoreRef)
    : engineCore(engineCoreRef)
{
}

AudioIOJuce::~AudioIOJuce()
{
    stop();
}

AudioIOJuce::StartResult AudioIOJuce::start()
{
    StartResult result;

    if (const juce::String initError = deviceManager.initialise(0, 2, nullptr, true); initError.isNotEmpty()) {
        result.ok = false;
        result.message = initError.toStdString();
        return result;
    }

    juce::AudioDeviceManager::AudioDeviceSetup setup;
    deviceManager.getAudioDeviceSetup(setup);
    setup.bufferSize = result.requestedBufferSize;
    deviceManager.setAudioDeviceSetup(setup, true);

    auto* device = deviceManager.getCurrentAudioDevice();
    if (device == nullptr) {
        result.ok = false;
        result.message = "No audio device opened";
        return result;
    }

    result.actualBufferSize = device->getCurrentBufferSizeSamples();
    result.sampleRate = device->getCurrentSampleRate();
    result.ok = true;
    result.message = "OK";

    deviceManager.addAudioCallback(this);
    return result;
}

void AudioIOJuce::stop()
{
    deviceManager.removeAudioCallback(this);
    deviceManager.closeAudioDevice();
}

void AudioIOJuce::audioDeviceIOCallbackWithContext(const float* const*,
                                                   int,
                                                   float* const* outputChannelData,
                                                   int numOutputChannels,
                                                   int numSamples,
                                                   const juce::AudioIODeviceCallbackContext&)
{
    if (numOutputChannels <= 0 || outputChannelData == nullptr || numSamples <= 0) {
        return;
    }

    float* left = outputChannelData[0];
    float* right = (numOutputChannels > 1 && outputChannelData[1] != nullptr) ? outputChannelData[1] : outputChannelData[0];

    if (left == nullptr || right == nullptr) {
        return;
    }

    engineCore.process(left, right, numSamples);

    for (int channel = 2; channel < numOutputChannels; ++channel) {
        if (outputChannelData[channel] != nullptr) {
            std::copy(left, left + numSamples, outputChannelData[channel]);
        }
    }
}

void AudioIOJuce::audioDeviceAboutToStart(juce::AudioIODevice* device)
{
    if (device != nullptr) {
        engineCore.prepare(device->getCurrentSampleRate(), device->getCurrentBufferSizeSamples());
    }
}

void AudioIOJuce::audioDeviceStopped()
{
}