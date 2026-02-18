#include "engine/runtime/offline/OfflineRenderer.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <thread>
#include <vector>

#include "engine/EngineCore.h"
#include "engine/command/Command.h"
#include "engine/runtime/MasterBus.h"
#include "engine/runtime/offline/WavWriter.h"

namespace ngks {

bool OfflineRenderer::renderToWav(const OfflineRenderConfig& config,
                                  const std::string& outputPath,
                                  OfflineRenderResult& result)
{
    result = {};
    if (config.channels != 2 || config.sampleRate == 0 || config.blockSize == 0 || config.secondsToRender <= 0.0f) {
        return false;
    }

    std::filesystem::create_directories(std::filesystem::path(outputPath).parent_path());

    EngineCore engine(true);
    engine.prepare(static_cast<double>(config.sampleRate), static_cast<int>(config.blockSize));

    uint32_t seq = 1;

    ngks::Command setTrack {};
    setTrack.type = ngks::CommandType::SetDeckTrack;
    setTrack.deck = ngks::DECK_A;
    setTrack.seq = seq++;
    setTrack.trackUidHash = 4001ULL;
    std::memcpy(setTrack.trackLabel, "OfflineTone", 11);
    engine.enqueueCommand(setTrack);

    engine.enqueueCommand({ ngks::CommandType::RequestAnalyzeTrack, ngks::DECK_A, seq++, 4001ULL, 0.0f, 0, 0, 401u });

    {
        std::vector<float> warmupInterleaved(static_cast<size_t>(config.blockSize) * 2u, 0.0f);
        for (int i = 0; i < 200; ++i) {
            engine.renderOfflineBlock(warmupInterleaved.data(), config.blockSize);
            const auto snapshot = engine.getSnapshot();
            if (snapshot.decks[ngks::DECK_A].lifecycle == DeckLifecycleState::Analyzed) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    engine.enqueueCommand({ ngks::CommandType::SetCue, ngks::DECK_A, seq++, 0, 0.0f, 1, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetMasterGain, ngks::DECK_A, seq++, 0, config.masterGain, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::SetDeckGain, ngks::DECK_A, seq++, 0, 12.0f, 0, 0 });
    engine.enqueueCommand({ ngks::CommandType::Play, ngks::DECK_A, seq++, 0, 0.0f, 0, 0 });

    const uint32_t totalFrames = static_cast<uint32_t>(std::round(config.secondsToRender * static_cast<float>(config.sampleRate)));

    WavWriter writer;
    if (!writer.open(outputPath, config.sampleRate, static_cast<uint16_t>(config.channels))) {
        return false;
    }

    std::vector<float> blockInterleaved(static_cast<size_t>(config.blockSize) * 2u, 0.0f);
    uint32_t renderedFrames = 0;
    float peakAbs = 0.0f;

    while (renderedFrames < totalFrames) {
        const uint32_t framesThisBlock = std::min(config.blockSize, totalFrames - renderedFrames);
        if (!engine.renderOfflineBlock(blockInterleaved.data(), framesThisBlock)) {
            return false;
        }

        for (uint32_t i = 0; i < framesThisBlock * 2u; ++i) {
            peakAbs = std::max(peakAbs, std::abs(blockInterleaved[i]));
        }

        if (!writer.writeInterleaved(blockInterleaved.data(), framesThisBlock)) {
            return false;
        }
        renderedFrames += framesThisBlock;
    }

    if (!writer.finalize()) {
        return false;
    }

    result.success = true;
    result.renderedFrames = renderedFrames;
    result.peakAbs = peakAbs;
    return true;
}

}
