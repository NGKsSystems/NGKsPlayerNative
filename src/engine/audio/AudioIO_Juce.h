#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <juce_audio_devices/juce_audio_devices.h>

class EngineCore;

class AudioIOJuce final : public juce::AudioIODeviceCallback,
                          public juce::ChangeListener
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

    /// Return the current Windows default output device name via JUCE's
    /// cached device list (updated automatically by systemDeviceChanged).
    std::string getDefaultOutputDeviceName() const;

    void audioDeviceIOCallbackWithContext(const float* const* inputChannelData,
                                          int numInputChannels,
                                          float* const* outputChannelData,
                                          int numOutputChannels,
                                          int numSamples,
                                          const juce::AudioIODeviceCallbackContext& context) override;
    void audioDeviceAboutToStart(juce::AudioIODevice* device) override;
    void audioDeviceStopped() override;
    void changeListenerCallback(juce::ChangeBroadcaster* source) override;

    // ── DJ output validity enforcer queries ──
    // Fast check: does deviceName still appear in the current backend's output device list?
    // Calls scanForDevices() internally — must be called from message/UI thread.
    bool isOutputDevicePresent(const std::string& deviceName) const;
    // Returns true if JUCE's current audio device is non-null and isOpen()
    bool isCurrentDeviceOpen() const;
    // Return all output device names from the current backend (rescans).
    std::vector<std::string> listOutputDeviceNames() const;
    // Callback flow state
    bool isCallbackFlowing() const noexcept { return callbackActive_.load(std::memory_order_acquire); }
    uint64_t callbackCount() const noexcept { return callbackCounter_.load(std::memory_order_relaxed); }

private:
    EngineCore& engineCore;
    juce::AudioDeviceManager deviceManager;
    bool callbackAdded = false;
    bool initialized_ = false;
    std::atomic<bool> recoveryInFlight_{false};
    std::atomic<uint64_t> callbackCounter_{0};    // heartbeat: total audioDeviceIOCallback invocations
    std::atomic<bool> callbackActive_{false};      // true while callbacks are flowing, false after audioDeviceStopped
};