#include <chrono>
#include <iostream>
#include <thread>

#include "engine/EngineCore.h"

int main()
{
    EngineCore engine;

    if (!engine.startAudioIfNeeded()) {
        std::cout << "AudioStart=FAIL" << std::endl;
        return 1;
    }

    std::cout << "RequestedBuffer=" << engine.getRequestedBufferSize() << std::endl;
    std::cout << "ActualBuffer=" << engine.getActualBufferSize() << std::endl;
    std::cout << "SampleRate=" << engine.getSampleRate() << std::endl;

    std::cout << "AudioStart=OK" << std::endl;

    std::thread meterThread([&engine]() {
        for (int second = 1; second <= 10; ++second) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            const auto snapshot = engine.getSnapshot();
            std::cout << "t=" << second << "s"
                      << " meterL=" << snapshot.left
                      << " meterR=" << snapshot.right << std::endl;
        }
    });

    meterThread.join();
    engine.stopWithFade();
    std::this_thread::sleep_for(std::chrono::milliseconds(220));

    std::cout << "RunResult=PASS" << std::endl;
    return 0;
}