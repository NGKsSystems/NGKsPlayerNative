#include "engine/audio/AudioIO_Juce.h"

#include "engine/EngineCore.h"
#include "engine/DiagLog.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#include <objbase.h>
#endif

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
    deviceManager.addChangeListener(this);
}

AudioIOJuce::~AudioIOJuce()
{
    deviceManager.removeChangeListener(this);
    stop();
}

AudioIOJuce::StartResult AudioIOJuce::start(const StartRequest& request)
{
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto elapsedMs = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    };

    StartResult result;

    ngks::audioTrace("START_ENTER", "prefId=\"%s\" prefName=\"%s\" sr=%.0f buf=%d ch=%d",
                     request.preferredDeviceId.c_str(), request.preferredDeviceName.c_str(),
                     request.preferredSampleRate, request.preferredBufferSize,
                     request.preferredOutputChannels);
    ngks::diagLog("AUDIO_IO: start — preferred_id='%s' preferred_name='%s' sr=%.0f buf=%d ch=%d [t=0ms]",
                  request.preferredDeviceId.c_str(), request.preferredDeviceName.c_str(),
                  request.preferredSampleRate, request.preferredBufferSize,
                  request.preferredOutputChannels);

    // ── Log current active device BEFORE switch ──
    if (auto* prevDevice = deviceManager.getCurrentAudioDevice()) {
        ngks::diagLog("AUDIO_IO: BEFORE switch — active='%s' type='%s' sr=%.0f buf=%d [t=%lldms]",
                      prevDevice->getName().toRawUTF8(),
                      prevDevice->getTypeName().toRawUTF8(),
                      prevDevice->getCurrentSampleRate(),
                      prevDevice->getCurrentBufferSizeSamples(), elapsedMs());
    } else {
        ngks::diagLog("AUDIO_IO: BEFORE switch — no active device [t=%lldms]", elapsedMs());
    }

    // ── Resolve device target ──
    // FAST PATH: when we have a device name (the UI switch case), skip the
    // expensive listAudioDevices() scan.  That static method creates a temporary
    // AudioDeviceManager, calls scanForDevices() on every WASAPI/DirectSound
    // backend, and probes every endpoint via COM — which serialises with the
    // main thread's STA message pump and freezes the UI for 3-10 s.
    std::string resolvedDeviceName;
    std::string resolvedDeviceId;

    if (!request.preferredDeviceId.empty()) {
        // ID-based lookup requires scanning to resolve ID → name
        ngks::diagLog("AUDIO_IO: ID-based lookup, scanning devices… [t=%lldms]", elapsedMs());
        const auto devices = listAudioDevices();
        ngks::diagLog("AUDIO_IO: scan complete (%zu devices) [t=%lldms]",
                      devices.size(), elapsedMs());
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
            ngks::diagLog("AUDIO_IO: start FAILED — %s (id='%s') [t=%lldms]",
                          result.message.c_str(), request.preferredDeviceId.c_str(), elapsedMs());
            return result;
        }
    } else if (!request.preferredDeviceName.empty()) {
        // NAME-based: use the name directly — no scan needed
        resolvedDeviceName = request.preferredDeviceName;
        ngks::diagLog("AUDIO_IO: using device name directly (no scan) — '%s' [t=%lldms]",
                      resolvedDeviceName.c_str(), elapsedMs());
    }

    ngks::diagLog("AUDIO_IO: resolved target — name='%s' id='%s' [t=%lldms]",
                  resolvedDeviceName.c_str(), resolvedDeviceId.c_str(), elapsedMs());

    // ── Initialise device manager ONCE only ──
    if (!initialized_) {
        ngks::audioTrace("START_INIT_BEGIN", "");
        ngks::diagLog("AUDIO_IO: first-time initialise of device manager [t=%lldms]", elapsedMs());
        const auto tInit = Clock::now();
        if (const juce::String initError = deviceManager.initialise(0, 2, nullptr, true); initError.isNotEmpty()) {
            const auto initMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tInit).count();
            ngks::audioTrace("START_INIT_FAIL", "err=\"%s\" elapsedMs=%lld", initError.toRawUTF8(), initMs);
            result.ok = false;
            result.message = initError.toStdString();
            ngks::diagLog("AUDIO_IO: initialise FAILED — %s [t=%lldms]", result.message.c_str(), elapsedMs());
            return result;
        }
        const auto initMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tInit).count();
        initialized_ = true;
        ngks::audioTrace("START_INIT_END", "elapsedMs=%lld", initMs);
        if (initMs > 1000) ngks::audioTrace("STALL_WARNING", "step=initialise elapsedMs=%lld", initMs);
        if (initMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=initialise elapsedMs=%lld", initMs);
        ngks::diagLog("AUDIO_IO: initialise done [t=%lldms]", elapsedMs());
    }

    // ── If no device preference, resolve to the current Windows default ──
    if (resolvedDeviceName.empty()) {
        // Ask JUCE's cached device type for the current Windows default.
        // WASAPI scan() places the default endpoint at index 0.
        auto* deviceType = deviceManager.getCurrentDeviceTypeObject();
        if (deviceType != nullptr) {
            const auto names = deviceType->getDeviceNames(false);
            const int defaultIdx = deviceType->getDefaultDeviceIndex(false);
            if (defaultIdx >= 0 && defaultIdx < names.size()) {
                resolvedDeviceName = names[defaultIdx].toStdString();
                ngks::diagLog("AUDIO_IO: no preference — Windows default is '%s' [t=%lldms]",
                              resolvedDeviceName.c_str(), elapsedMs());
            }
        }
        // Fallback: if device type not ready yet, try whatever is open/available
        if (resolvedDeviceName.empty()) {
            if (auto* openDev = deviceManager.getCurrentAudioDevice()) {
                resolvedDeviceName = openDev->getName().toStdString();
                ngks::diagLog("AUDIO_IO: no preference — fallback to already-open device '%s' [t=%lldms]",
                              resolvedDeviceName.c_str(), elapsedMs());
            } else {
                juce::AudioDeviceManager::AudioDeviceSetup currentSetup;
                deviceManager.getAudioDeviceSetup(currentSetup);
                if (!currentSetup.outputDeviceName.isEmpty()) {
                    resolvedDeviceName = currentSetup.outputDeviceName.toStdString();
                    ngks::diagLog("AUDIO_IO: no preference — fallback to setup device '%s' [t=%lldms]",
                                  resolvedDeviceName.c_str(), elapsedMs());
                } else {
                    const auto devices = listAudioDevices();
                    ngks::diagLog("AUDIO_IO: no preference — fallback scanned %zu devices [t=%lldms]",
                                  devices.size(), elapsedMs());
                    for (const auto& d : devices) {
                        if (d.outputChannels > 0) {
                            resolvedDeviceName = d.deviceName;
                            ngks::diagLog("AUDIO_IO: no preference — fallback picked '%s' [t=%lldms]",
                                          resolvedDeviceName.c_str(), elapsedMs());
                            break;
                        }
                    }
                    if (resolvedDeviceName.empty()) {
                        ngks::diagLog("AUDIO_IO: no preference — no output devices found! [t=%lldms]", elapsedMs());
                    }
                }
            }
        }
    }

    juce::AudioDeviceManager::AudioDeviceSetup baseSetup;
    deviceManager.getAudioDeviceSetup(baseSetup);

    result.requestedSampleRate = request.preferredSampleRate;
    result.requestedBufferSize = (request.preferredBufferSize > 0) ? request.preferredBufferSize : baseSetup.bufferSize;
    result.requestedOutputChannels = (request.preferredOutputChannels > 0) ? request.preferredOutputChannels : 2;

    const int desiredOutputChannels = chooseOutputChannels(result.requestedOutputChannels, 2);

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

            ngks::diagLog("AUDIO_IO: setAudioDeviceSetup begin tid=%lu sr=%.0f buf=%d dev='%s' [t=%lldms]",
                          static_cast<unsigned long>(GetCurrentThreadId()), sr, buffer,
                          resolvedDeviceName.c_str(), elapsedMs());
            ngks::audioTrace("START_SETUP_BEGIN", "sr=%.0f buf=%d dev=\"%s\"", sr, buffer, resolvedDeviceName.c_str());
            const auto tSetup = Clock::now();
            lastSetupError = deviceManager.setAudioDeviceSetup(setup, true);
            const auto setupMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                Clock::now() - tSetup).count();
            if (lastSetupError.isNotEmpty()) {
                ngks::audioTrace("START_SETUP_FAIL", "sr=%.0f buf=%d err=\"%s\" elapsedMs=%lld",
                                 sr, buffer, lastSetupError.toRawUTF8(), setupMs);
                ngks::diagLog("AUDIO_IO: setAudioDeviceSetup FAILED — sr=%.0f buf=%d err='%s' [took %lldms, t=%lldms]",
                              sr, buffer, lastSetupError.toRawUTF8(), setupMs, elapsedMs());
                continue;
            }

            ngks::audioTrace("START_SETUP_OK", "sr=%.0f buf=%d elapsedMs=%lld", sr, buffer, setupMs);
            if (setupMs > 1000) ngks::audioTrace("STALL_WARNING", "step=setAudioDeviceSetup sr=%.0f buf=%d elapsedMs=%lld", sr, buffer, setupMs);
            if (setupMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=setAudioDeviceSetup sr=%.0f buf=%d elapsedMs=%lld", sr, buffer, setupMs);
            ngks::diagLog("AUDIO_IO: setAudioDeviceSetup OK — sr=%.0f buf=%d [took %lldms, t=%lldms]",
                          sr, buffer, setupMs, elapsedMs());
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
        // Direct setup failed — run diagnostic scan (off the hot path now)
        if (!request.preferredDeviceName.empty() && request.preferredDeviceId.empty()) {
            ngks::diagLog("AUDIO_IO: direct setup failed, scanning for diagnostics… [t=%lldms]", elapsedMs());
            const auto diagDevices = listAudioDevices();
            ngks::diagLog("AUDIO_IO: diagnostic scan (%zu devices) [t=%lldms]",
                          diagDevices.size(), elapsedMs());
            bool nameFound = false;
            for (const auto& d : diagDevices) {
                ngks::diagLog("  [%s] '%s' out_ch=%d",
                              d.backendType.c_str(), d.deviceName.c_str(), d.outputChannels);
                if (d.deviceName == request.preferredDeviceName) nameFound = true;
            }
            if (!nameFound) {
                result.ok = false;
                result.message = "Device name not found: '" + request.preferredDeviceName + "'";
                ngks::diagLog("AUDIO_IO: FAILED — %s [t=%lldms]", result.message.c_str(), elapsedMs());
                return result;
            }
        }
        result.ok = false;
        result.message = lastSetupError.isNotEmpty() ? lastSetupError.toStdString() : "No audio device opened";
        ngks::diagLog("AUDIO_IO: start FAILED — %s [t=%lldms]", result.message.c_str(), elapsedMs());
        return result;
    }

    // ── Verify the active device is actually the one we requested ──
    const juce::String activeDeviceName = currentDevice->getName();
    if (!resolvedDeviceName.empty()
        && activeDeviceName != juce::String(resolvedDeviceName)) {
        ngks::diagLog("AUDIO_IO: DEVICE MISMATCH — requested='%s' active='%s' [t=%lldms]",
                      resolvedDeviceName.c_str(), activeDeviceName.toRawUTF8(), elapsedMs());
        result.ok = false;
        result.message = "Device mismatch: active='" + activeDeviceName.toStdString()
                        + "' requested='" + resolvedDeviceName + "'";
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

    ngks::audioTrace("START_OK", "device=\"%s\" sr=%.0f buf=%d ch=%d totalMs=%lld",
                     result.deviceName.c_str(), result.sampleRate,
                     result.actualBufferSize, result.outputChannels, elapsedMs());
    ngks::diagLog("AUDIO_IO: start OK — active='%s' type='%s' sr=%.0f buf=%d ch_out=%d fallback=%d [t=%lldms]",
                  result.deviceName.c_str(),
                  currentDevice->getTypeName().toRawUTF8(),
                  result.sampleRate, result.actualBufferSize,
                  result.outputChannels, result.fallbackUsed ? 1 : 0,
                  elapsedMs());

    return result;
}

void AudioIOJuce::stop()
{
    using Clock = std::chrono::steady_clock;
    const auto t0 = Clock::now();
    auto elapsedMs = [&t0]() -> long long {
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
    };

    ngks::audioTrace("STOP_ENTER", "callbackAdded=%d audioOpened=%d",
                     callbackAdded ? 1 : 0,
                     engineCore.audioOpened.load(std::memory_order_acquire) ? 1 : 0);
    ngks::diagLog("AUDIO_IO: stop — begin tid=%lu [t=0ms]", static_cast<unsigned long>(GetCurrentThreadId()));

    if (callbackAdded) {
        ngks::audioTrace("STOP_REMOVE_CB_BEGIN", "");
        const auto tStep = Clock::now();
        deviceManager.removeAudioCallback(this);
        callbackAdded = false;
        const auto stepMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tStep).count();
        ngks::audioTrace("STOP_REMOVE_CB_END", "elapsedMs=%lld", stepMs);
        if (stepMs > 1000) ngks::audioTrace("STALL_WARNING", "step=removeAudioCallback elapsedMs=%lld", stepMs);
        if (stepMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=removeAudioCallback elapsedMs=%lld", stepMs);
    }

    {
        ngks::audioTrace("STOP_CLOSE_BEGIN", "");
        const auto tStep = Clock::now();
        deviceManager.closeAudioDevice();
        const auto stepMs = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tStep).count();
        ngks::audioTrace("STOP_CLOSE_END", "elapsedMs=%lld", stepMs);
        if (stepMs > 1000) ngks::audioTrace("STALL_WARNING", "step=closeAudioDevice elapsedMs=%lld", stepMs);
        if (stepMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=closeAudioDevice elapsedMs=%lld", stepMs);
    }

    ngks::audioTrace("STOP_EXIT", "totalMs=%lld", elapsedMs());
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

    const uint64_t cc = callbackCounter_.fetch_add(1, std::memory_order_relaxed);

    // First callback after start/resume — log transition
    if (cc == 0) {
        callbackActive_.store(true, std::memory_order_relaxed);
        ngks::audioTrace("CB_FIRST", "numSamples=%d numOutCh=%d", numSamples, numOutputChannels);
    }

    // Heartbeat every 2000 callbacks (~every ~5-10 seconds depending on buffer size)
    if ((cc % 2000) == 0 && cc > 0) {
        ngks::audioTrace("CB_HEARTBEAT", "count=%llu numSamples=%d",
                         static_cast<unsigned long long>(cc), numSamples);
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
        ngks::audioTrace("ABOUT_TO_START", "device=\"%s\" sr=%.0f buf=%d",
                         device->getName().toRawUTF8(),
                         device->getCurrentSampleRate(),
                         device->getCurrentBufferSizeSamples());
        engineCore.prepare(device->getCurrentSampleRate(), device->getCurrentBufferSizeSamples());
    }
}

void AudioIOJuce::audioDeviceStopped()
{
    const uint64_t finalCount = callbackCounter_.load(std::memory_order_relaxed);
    const bool wasActive = callbackActive_.exchange(false, std::memory_order_relaxed);
    callbackCounter_.store(0, std::memory_order_relaxed); // reset for next session
    ngks::audioTrace("DEVICE_STOPPED", "audioOpened=%d switchInFlight=%d recoveryInFlight=%d cbCount=%llu wasActive=%d",
                     engineCore.audioOpened.load(std::memory_order_acquire) ? 1 : 0,
                     engineCore.deviceSwitchInFlight_.load(std::memory_order_acquire) ? 1 : 0,
                     recoveryInFlight_.load(std::memory_order_acquire) ? 1 : 0,
                     static_cast<unsigned long long>(finalCount),
                     wasActive ? 1 : 0);
    ngks::diagLog("AUDIO_IO: audioDeviceStopped callback fired");
    engineCore.notifyDeviceStopped();
}

std::string AudioIOJuce::getDefaultOutputDeviceName() const
{
    auto* deviceType = deviceManager.getCurrentDeviceTypeObject();
    if (deviceType == nullptr) return {};

    const auto names = deviceType->getDeviceNames(false);
    const int defaultIdx = deviceType->getDefaultDeviceIndex(false);
    if (defaultIdx >= 0 && defaultIdx < names.size()) {
        return names[defaultIdx].toStdString();
    }
    if (!names.isEmpty()) {
        return names[0].toStdString();
    }
    return {};
}

// ── DJ output validity enforcer queries ──

bool AudioIOJuce::isOutputDevicePresent(const std::string& deviceName) const
{
    auto* deviceType = deviceManager.getCurrentDeviceTypeObject();
    if (deviceType == nullptr) return false;
    // Rescan the current backend to detect newly added/removed endpoints.
    // This is cheaper than listAudioDevices() which creates a temp manager
    // and scans ALL backends.
    deviceType->scanForDevices();
    const auto names = deviceType->getDeviceNames(false);
    for (const auto& name : names) {
        if (name.toStdString() == deviceName)
            return true;
    }
    return false;
}

bool AudioIOJuce::isCurrentDeviceOpen() const
{
    auto* device = deviceManager.getCurrentAudioDevice();
    return device != nullptr && device->isOpen();
}

std::vector<std::string> AudioIOJuce::listOutputDeviceNames() const
{
    std::vector<std::string> names;
    auto* deviceType = deviceManager.getCurrentDeviceTypeObject();
    if (deviceType == nullptr) return names;
    deviceType->scanForDevices();
    const auto juceNames = deviceType->getDeviceNames(false);
    names.reserve(static_cast<size_t>(juceNames.size()));
    for (const auto& n : juceNames)
        names.push_back(n.toStdString());
    return names;
}

void AudioIOJuce::changeListenerCallback(juce::ChangeBroadcaster*)
{
    // ── Entry trace ──
    auto* currentDevice = deviceManager.getCurrentAudioDevice();
    const std::string activeName = currentDevice ? currentDevice->getName().toStdString() : "(none)";
    const std::string defaultName = getDefaultOutputDeviceName();
    const bool wasOpen = engineCore.audioOpened.load(std::memory_order_acquire);
    const bool switchInFlight = engineCore.deviceSwitchInFlight_.load(std::memory_order_acquire);
    const bool recoveryActive = recoveryInFlight_.load(std::memory_order_acquire);
    const uint64_t cbCount = engineCore.telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
    const bool deviceGone = (currentDevice == nullptr);

    ngks::audioTrace("CHANGE_CB", "active=\"%s\" default=\"%s\" audioOpened=%d switch=%d recovery=%d cbCount=%llu deviceGone=%d",
                     activeName.c_str(), defaultName.c_str(), wasOpen ? 1 : 0,
                     switchInFlight ? 1 : 0, recoveryActive ? 1 : 0,
                     static_cast<unsigned long long>(cbCount), deviceGone ? 1 : 0);

    // Device list changed (disconnect/connect/default changed).
    // Skip if an intentional device switch is already in progress.
    if (switchInFlight) {
        ngks::audioTrace("CHANGE_CB_SKIP", "reason=deviceSwitchInFlight");
        ngks::diagLog("DEVICE_CHANGE: skipped — deviceSwitchInFlight");
        return;
    }

    ngks::diagLog("DEVICE_CHANGE: callback fired — wasOpen=%d deviceGone=%d",
                  wasOpen ? 1 : 0, deviceGone ? 1 : 0);

    // Detect hard device-loss: we thought audio was open but JUCE says no device
    if (wasOpen && deviceGone) {
        ngks::audioTrace("CHANGE_CB_DEVICE_LOST", "wasOpen=1 deviceGone=1 active=\"%s\"", activeName.c_str());
        ngks::diagLog("DEVICE_CHANGE: device LOST (was open, now gone) — forcing audioOpened=false");
        engineCore.notifyDeviceStopped();
    }

    // ── DJ mode: NO auto-recovery, NO default-follow ──
    if (engineCore.djMode_.load(std::memory_order_acquire)) {
        ngks::audioTrace("CHANGE_CB_DJ_BLOCK", "djMode=1 djDeviceLost=%d — recovery BLOCKED",
                         engineCore.djDeviceLost_.load(std::memory_order_acquire) ? 1 : 0);
        ngks::diagLog("DEVICE_CHANGE: DJ mode active — auto-recovery BLOCKED, user must re-enter DJ mode");
        return;
    }

    const bool needsRecovery = !engineCore.audioOpened.load(std::memory_order_acquire);

    // Check if Windows default changed while we're still "open"
    bool defaultChanged = false;
    if (!needsRecovery && currentDevice != nullptr) {
        if (!defaultName.empty() && defaultName != activeName) {
            defaultChanged = true;
            ngks::audioTrace("CHANGE_CB_DEFAULT_CHANGED", "active=\"%s\" newDefault=\"%s\"",
                             activeName.c_str(), defaultName.c_str());
            ngks::diagLog("DEVICE_CHANGE: Windows default changed — active='%s' default='%s'",
                          activeName.c_str(), defaultName.c_str());
        }
    }

    // ── Classification ──
    const char* classification = "neither";
    if (needsRecovery && defaultChanged)       classification = "both";
    else if (needsRecovery)                    classification = "device_lost";
    else if (defaultChanged)                   classification = "default_changed";

    ngks::audioTrace("CHANGE_CB_CLASSIFY", "class=%s needsRecovery=%d defaultChanged=%d prev=\"%s\" newDefault=\"%s\"",
                     classification, needsRecovery ? 1 : 0, defaultChanged ? 1 : 0,
                     activeName.c_str(), defaultName.c_str());

    if (!needsRecovery && !defaultChanged) {
        ngks::audioTrace("CHANGE_CB_NOACTION", "");
        ngks::diagLog("DEVICE_CHANGE: no action needed");
        return;
    }

    // Prevent multiple concurrent recovery threads
    bool expected = false;
    if (!recoveryInFlight_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        ngks::audioTrace("CHANGE_CB_SKIP", "reason=recoveryAlreadyInFlight");
        ngks::diagLog("DEVICE_CHANGE: recovery already in-flight, skipping");
        return;
    }

    ngks::audioTrace("RECOVERY_DISPATCH", "reason=%s prev=\"%s\" nextDefault=\"%s\"",
                     classification, activeName.c_str(), defaultName.c_str());
    ngks::diagLog("DEVICE_CHANGE: dispatching recovery — reason=%s",
                  needsRecovery ? "device-loss" : "default-changed");

    // Capture names for the recovery thread
    const std::string capturedPrev = activeName;
    const std::string capturedDefault = defaultName;
    const char* capturedReason = classification;

    std::thread([this, capturedPrev, capturedDefault, capturedReason]() {
#ifdef _WIN32
        CoInitializeEx(nullptr, COINIT_MULTITHREADED);
#endif
        ngks::audioTrace("RECOVERY_START", "reason=\"%s\" prev=\"%s\" nextDefault=\"%s\"",
                         capturedReason, capturedPrev.c_str(), capturedDefault.c_str());

        if (engineCore.deviceSwitchInFlight_.load(std::memory_order_acquire)) {
            ngks::audioTrace("RECOVERY_CANCEL", "reason=deviceSwitchInFlight");
            ngks::diagLog("DEVICE_CHANGE_BG: cancelled — deviceSwitchInFlight");
#ifdef _WIN32
            CoUninitialize();
#endif
            recoveryInFlight_.store(false, std::memory_order_release);
            return;
        }

        // Always clear preferred device — we want to land on the current
        // Windows default, not retry a dead/stale endpoint.
        ngks::audioTrace("RECOVERY_CLEAR_PREF", "clearing preferredDevice");
        engineCore.clearPreferredAudioDevice();

        bool recovered = false;
        constexpr int kMaxAttempts = 3;
        constexpr int kRetryDelayMs = 500;

        for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
            if (attempt > 1) {
                ngks::audioTrace("RECOVERY_RETRY", "attempt=%d/%d delayMs=%d",
                                 attempt, kMaxAttempts, kRetryDelayMs);
                ngks::diagLog("DEVICE_CHANGE_BG: retry attempt %d/%d (waiting %dms)",
                              attempt, kMaxAttempts, kRetryDelayMs);
                std::this_thread::sleep_for(std::chrono::milliseconds(kRetryDelayMs));

                if (engineCore.deviceSwitchInFlight_.load(std::memory_order_acquire)) {
                    ngks::audioTrace("RECOVERY_CANCEL", "reason=deviceSwitchInFlight_during_retry attempt=%d", attempt);
                    ngks::diagLog("DEVICE_CHANGE_BG: cancelled during retry — deviceSwitchInFlight");
                    break;
                }
                engineCore.clearPreferredAudioDevice();
            }

            ngks::audioTrace("RECOVERY_REOPEN_BEGIN", "attempt=%d/%d", attempt, kMaxAttempts);
            const auto tReopen = std::chrono::steady_clock::now();
            const bool reopenOk = engineCore.reopenAudioWithPreferredConfig();
            const auto reopenMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - tReopen).count();

            ngks::audioTrace("RECOVERY_REOPEN_END", "attempt=%d ok=%d elapsedMs=%lld",
                             attempt, reopenOk ? 1 : 0, reopenMs);
            if (reopenMs > 1000) ngks::audioTrace("STALL_WARNING", "step=reopenAudioWithPreferredConfig elapsedMs=%lld attempt=%d", reopenMs, attempt);
            if (reopenMs > 3000) ngks::audioTrace("STALL_CRITICAL", "step=reopenAudioWithPreferredConfig elapsedMs=%lld attempt=%d", reopenMs, attempt);

            if (reopenOk) {
                // Verify: audio callback must actually be flowing
                const uint64_t countBefore = engineCore.telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
                ngks::audioTrace("RECOVERY_VERIFY_BEGIN", "cbCountBefore=%llu",
                                 static_cast<unsigned long long>(countBefore));
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                const uint64_t countAfter = engineCore.telemetry_.rtCallbackCount.load(std::memory_order_relaxed);
                const bool callbacksFlowing = (countAfter > countBefore);

                ngks::audioTrace("RECOVERY_VERIFY_END", "cbBefore=%llu cbAfter=%llu flowing=%d",
                                 static_cast<unsigned long long>(countBefore),
                                 static_cast<unsigned long long>(countAfter),
                                 callbacksFlowing ? 1 : 0);

                if (callbacksFlowing) {
                    const std::string newActive = engineCore.getActiveDeviceName();
                    ngks::audioTrace("RECOVERY_SUCCESS", "attempt=%d device=\"%s\" cbCount=%llu",
                                     attempt, newActive.c_str(),
                                     static_cast<unsigned long long>(countAfter));
                    ngks::diagLog("DEVICE_CHANGE_BG: recovery VERIFIED attempt %d — callbacks %llu→%llu",
                                  attempt,
                                  static_cast<unsigned long long>(countBefore),
                                  static_cast<unsigned long long>(countAfter));
                    recovered = true;
                    break;
                } else {
                    ngks::audioTrace("RECOVERY_VERIFY_FAIL", "attempt=%d cbStuck=%llu",
                                     attempt, static_cast<unsigned long long>(countBefore));
                    ngks::diagLog("DEVICE_CHANGE_BG: reopen OK but callbacks NOT flowing (%llu→%llu) — treating as failure",
                                  static_cast<unsigned long long>(countBefore),
                                  static_cast<unsigned long long>(countAfter));
                }
            } else {
                ngks::audioTrace("RECOVERY_ATTEMPT_FAIL", "attempt=%d/%d", attempt, kMaxAttempts);
                ngks::diagLog("DEVICE_CHANGE_BG: recovery FAILED attempt %d/%d",
                              attempt, kMaxAttempts);
            }
        }

        if (!recovered) {
            ngks::audioTrace("RECOVERY_ALL_FAILED", "attempts=%d prev=\"%s\"",
                             kMaxAttempts, capturedPrev.c_str());
            ngks::diagLog("DEVICE_CHANGE_BG: ALL %d recovery attempts FAILED", kMaxAttempts);
        }

#ifdef _WIN32
        CoUninitialize();
#endif
        recoveryInFlight_.store(false, std::memory_order_release);
        ngks::audioTrace("RECOVERY_EXIT", "recovered=%d", recovered ? 1 : 0);
    }).detach();
}