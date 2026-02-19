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

template <typename T>
void appendUnique(std::vector<T>& values, T value)
{
    for (const auto& existing : values) {
        if (existing == value) {
            return;
        }
    }
    values.push_back(value);
}

int chooseOutputChannels(int requested, int maxOutput)
{
    int normalizedRequested = (requested > 0) ? requested : 2;
    if (maxOutput <= 0) {
        return std::clamp(normalizedRequested, 1, 2);
    }

    if (normalizedRequested <= maxOutput) {
        return normalizedRequested;
    }

    if (maxOutput >= 2) {
        return 2;
    }
    return 1;
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

    if (const juce::String initError = deviceManager.initialise(0, 2, nullptr, true); initError.isNotEmpty()) {
        result.ok = false;
        result.message = initError.toStdString();
        return result;
    }

    juce::AudioDeviceManager::AudioDeviceSetup baseSetup;
    deviceManager.getAudioDeviceSetup(baseSetup);

    result.requestedSampleRate = request.preferredSampleRate;
    result.requestedBufferSize = (request.preferredBufferSize > 0) ? request.preferredBufferSize : baseSetup.bufferSize;
    result.requestedOutputChannels = (request.preferredOutputChannels > 0) ? request.preferredOutputChannels : 2;

    const int deviceMaxOut = [&]() {
        for (const auto& d : devices) {
            if ((!resolvedDeviceId.empty() && d.deviceId == resolvedDeviceId)
                || (!resolvedDeviceName.empty() && d.deviceName == resolvedDeviceName)) {
                return d.outputChannels;
            }
        }
        return 2;
    }();

    const int desiredOutputChannels = chooseOutputChannels(result.requestedOutputChannels, deviceMaxOut);

    std::vector<double> sampleRateCandidates;
    if (result.requestedSampleRate > 0.0) {
        appendUnique(sampleRateCandidates, result.requestedSampleRate);
    }
    appendUnique(sampleRateCandidates, 48000.0);
    appendUnique(sampleRateCandidates, 44100.0);
    appendUnique(sampleRateCandidates, baseSetup.sampleRate);
    if (sampleRateCandidates.empty()) {
        sampleRateCandidates.push_back(44100.0);
    }

    std::vector<int> bufferCandidates;
    appendUnique(bufferCandidates, result.requestedBufferSize);
    appendUnique(bufferCandidates, 512);
    appendUnique(bufferCandidates, 256);
    appendUnique(bufferCandidates, 128);
    appendUnique(bufferCandidates, baseSetup.bufferSize);

    juce::String lastSetupError;
    auto* currentDevice = static_cast<juce::AudioIODevice*>(nullptr);
    for (double sr : sampleRateCandidates) {
        if (sr <= 0.0) {
            continue;
        }
        for (int buffer : bufferCandidates) {
            if (buffer <= 0) {
                continue;
            }

            juce::AudioDeviceManager::AudioDeviceSetup setup = baseSetup;
            setup.sampleRate = sr;
            setup.bufferSize = buffer;
            if (!resolvedDeviceName.empty()) {
                setup.outputDeviceName = juce::String(resolvedDeviceName);
            }
            setup.outputChannels.clear();
            for (int ch = 0; ch < desiredOutputChannels; ++ch) {
                setup.outputChannels.setBit(ch);
            }

            lastSetupError = deviceManager.setAudioDeviceSetup(setup, true);
            if (lastSetupError.isNotEmpty()) {
                continue;
            }

            currentDevice = deviceManager.getCurrentAudioDevice();
            if (currentDevice != nullptr) {
                break;
            }
        }
        if (currentDevice != nullptr) {
            break;
        }
    }

    if (currentDevice == nullptr) {
        result.ok = false;
        result.message = lastSetupError.isNotEmpty() ? lastSetupError.toStdString() : "No audio device opened";
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
    result.fallbackUsed = (result.requestedSampleRate > 0.0 && result.sampleRate != result.requestedSampleRate)
        || (result.requestedBufferSize > 0 && result.actualBufferSize != result.requestedBufferSize)
        || (result.outputChannels != result.requestedOutputChannels);
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