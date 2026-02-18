cmake_minimum_required(VERSION 3.22)
project(NGKsPlayerNative LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

set(CMAKE_PREFIX_PATH "C:/Qt/6.10.2/msvc2022_64" CACHE PATH "Qt 6.10.2 MSVC 2022 path" FORCE)

find_package(Qt6 6.10 REQUIRED COMPONENTS Quick)
set(JUCE_DIR "C:/JUCE")
add_subdirectory(${JUCE_DIR} JUCE)

qt_standard_project_setup(REQUIRES 6.10)

add_library(NGKsEngine STATIC
    src/engine/EngineCore.cpp
    src/engine/audio/AudioIO_Juce.cpp
    src/engine/dsp/Meter.cpp
    src/engine/dsp/Limiter.cpp
    src/engine/runtime/fx/DummyGainFx.cpp
    src/engine/runtime/fx/FxChain.cpp
    src/engine/runtime/jobs/JobQueue.cpp
    src/engine/runtime/jobs/JobWorker.cpp
    src/engine/runtime/jobs/JobSystem.cpp
    src/engine/runtime/library/TrackRegistry.cpp
    src/engine/runtime/library/RegistryStore.cpp
    src/engine/runtime/MasterBus.cpp
    src/engine/runtime/offline/WavWriter.cpp
    src/engine/runtime/offline/OfflineRenderer.cpp
    src/engine/runtime/graph/AudioGraph.cpp
    src/engine/runtime/graph/DeckNode.cpp
    src/engine/runtime/graph/MasterMixNode.cpp
    src/engine/runtime/graph/CueMixNode.cpp
    src/engine/runtime/graph/OutputNode.cpp
)

target_include_directories(NGKsEngine
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(NGKsEngine
    PUBLIC
        juce::juce_audio_basics
        juce::juce_audio_devices
        juce::juce_dsp
        juce::juce_core
        juce::juce_events
)

qt_add_executable(NGKsPlayerNative
    src/ui/main.cpp
    src/ui/EngineBridge.h
    src/ui/EngineBridge.cpp
)

string(TIMESTAMP NGKS_BUILD_STAMP_UTC "%Y-%m-%dT%H:%M:%SZ" UTC)
execute_process(
    COMMAND git rev-parse --short=12 HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE NGKS_GIT_SHA
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
if(NOT NGKS_GIT_SHA)
    set(NGKS_GIT_SHA "unknown")
endif()

target_compile_definitions(NGKsPlayerNative
    PRIVATE
        NGKS_BUILD_STAMP="${NGKS_BUILD_STAMP_UTC}"
        NGKS_GIT_SHA="${NGKS_GIT_SHA}"
)

target_link_libraries(NGKsPlayerNative
    PRIVATE
        Qt6::Quick
        NGKsEngine
)

add_custom_command(TARGET NGKsPlayerNative POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:NGKsPlayerNative>/qml"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/src/ui/qml/Main.qml"
        "$<TARGET_FILE_DIR:NGKsPlayerNative>/qml/Main.qml"
)

add_executable(NGKsPlayerHeadless
    tools/headless_main.cpp
)

target_link_libraries(NGKsPlayerHeadless
    PRIVATE
        NGKsEngine
)
