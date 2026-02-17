#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"
#include "engine/audio/AudioIO_Juce.h"

int main()
{
    EngineCore engine;
    AudioIOJuce audio(engine);

    const auto startResult = audio.start();
    std::cout << "RequestedBuffer=128" << std::endl;
    std::cout << "ActualBuffer=" << startResult.actualBufferSize << std::endl;
    std::cout << "SampleRate=" << startResult.sampleRate << std::endl;

    if (!startResult.ok) {
        std::cout << "AudioStart=FAIL" << std::endl;
        std::cout << "Error=" << startResult.message << std::endl;
        return 1;
    }

    std::cout << "AudioStart=OK" << std::endl;

    std::thread meterThread([&engine]() {
        for (int second = 1; second <= 10; ++second) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            const auto snapshot = engine.consumeMeterSnapshot();
            std::cout << "t=" << second << "s"
                      << " meterL=" << snapshot.left
                      << " meterR=" << snapshot.right << std::endl;
        }
    });

    meterThread.join();
    audio.stop();

    std::cout << "RunResult=PASS" << std::endl;
    return 0;
}