#include "engine/audio/AudioIO_Juce.h"

#include "engine/EngineCore.h"

#include <algorithm>
#include <limits>

namespace
{
std::string makeDeviceId(const juce::String& backendType, const juce::String& deviceName)
{
    return backendType.toStdString() + "|" + deviceName.toStdString();
}
}

std::vector<AudioIOJuce::DeviceInfo> AudioIOJuce::listAudioDevices()
{
    std::vector<DeviceInfo> devices;

    juce::AudioDeviceManager manager;
    juce::OwnedArray<juce::AudioIODeviceType> types;
    manager.createAudioDeviceTypes(types);

    for (auto* type : types) {
        if (type == nullptr) {
            continue;
        }

        type->scanForDevices();
        const juce::String backendType = type->getTypeName();
        const juce::StringArray outputNames = type->getDeviceNames(false);
        for (const auto& outputName : outputNames) {
            DeviceInfo info {};
            info.deviceName = outputName.toStdString();
            info.backendType = backendType.toStdString();
            info.deviceId = makeDeviceId(backendType, outputName);

            std::unique_ptr<juce::AudioIODevice> device(type->createDevice({}, outputName));
            if (device != nullptr) {
                info.inputChannels = device->getInputChannelNames().size();
                info.outputChannels = device->getOutputChannelNames().size();
            }

            devices.push_back(info);
        }
    }

    std::sort(devices.begin(), devices.end(), [](const DeviceInfo& a, const DeviceInfo& b) {
        if (a.backendType == b.backendType) {
            return a.deviceName < b.deviceName;
        }
        return a.backendType < b.backendType;
    });
    return devices;
}

uint64_t AudioIOJuce::hashDeviceId(const std::string& value) noexcept
{
    constexpr uint64_t kOffset = 1469598103934665603ull;
    constexpr uint64_t kPrime = 1099511628211ull;
    uint64_t hash = kOffset;
    for (unsigned char c : value) {
        hash ^= static_cast<uint64_t>(c);
        hash *= kPrime;
    }
    return hash;
}

AudioIOJuce::AudioIOJuce(EngineCore& engineCoreRef)
    : engineCore(engineCoreRef)
{
}

AudioIOJuce::~AudioIOJuce()
{
    stop();
}

AudioIOJuce::StartResult AudioIOJuce::start(const StartRequest& request)
{
    StartResult result;

    const auto devices = listAudioDevices();
    std::string resolvedDeviceName;
    std::string resolvedDeviceId;

    if (!request.preferredDeviceId.empty()) {
        for (const auto& device : devices) {
            if (device.deviceId == request.preferredDeviceId) {
                resolvedDeviceId = device.deviceId;
                resolvedDeviceName = device.deviceName;
                break;
            }
        }
        if (resolvedDeviceName.empty()) {
            result.ok = false;
            result.message = "Preferred device_id not found";
            return result;
        }
    } else if (!request.preferredDeviceName.empty()) {
        for (const auto& device : devices) {
            if (device.deviceName == request.preferredDeviceName) {
                resolvedDeviceId = device.deviceId;
                resolvedDeviceName = device.deviceName;
                break;
            }
        }
        if (resolvedDeviceName.empty()) {
            result.ok = false;
            result.message = "Preferred device_name not found";
            return result;
        }
    }

    auto* currentDevice = deviceManager.getCurrentAudioDevice();
    if (currentDevice == nullptr) {
        if (const juce::String initError = deviceManager.initialise(0, 2, nullptr, true); initError.isNotEmpty()) {
            result.ok = false;
            result.message = initError.toStdString();
            return result;
        }

        juce::AudioDeviceManager::AudioDeviceSetup setup;
        deviceManager.getAudioDeviceSetup(setup);
        setup.bufferSize = (request.preferredBufferSize > 0) ? request.preferredBufferSize : result.requestedBufferSize;
        if (request.preferredSampleRate > 0.0) {
            setup.sampleRate = request.preferredSampleRate;
        }
        if (!resolvedDeviceName.empty()) {
            setup.outputDeviceName = juce::String(resolvedDeviceName);
        }
        const juce::String setupError = deviceManager.setAudioDeviceSetup(setup, true);
        if (setupError.isNotEmpty()) {
            result.ok = false;
            result.message = setupError.toStdString();
            return result;
        }
        currentDevice = deviceManager.getCurrentAudioDevice();
    }

    if (currentDevice == nullptr) {
        result.ok = false;
        result.message = "No audio device opened";
        return result;
    }

    result.actualBufferSize = currentDevice->getCurrentBufferSizeSamples();
    result.sampleRate = currentDevice->getCurrentSampleRate();
    result.inputChannels = currentDevice->getInputChannelNames().size();
    result.outputChannels = currentDevice->getOutputChannelNames().size();
    result.deviceName = currentDevice->getName().toStdString();
    if (!resolvedDeviceId.empty()) {
        result.deviceId = resolvedDeviceId;
    } else {
        result.deviceId = makeDeviceId(currentDevice->getTypeName(), currentDevice->getName());
    }
    result.deviceIdHash = hashDeviceId(result.deviceId);
    result.ok = true;
    result.message = "OK";

    if (!callbackAdded) {
        deviceManager.addAudioCallback(this);
        callbackAdded = true;
    }

    return result;
}

void AudioIOJuce::stop()
{
    if (callbackAdded) {
        deviceManager.removeAudioCallback(this);
        callbackAdded = false;
    }

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