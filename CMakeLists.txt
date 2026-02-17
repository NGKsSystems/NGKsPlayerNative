cmake_minimum_required(VERSION 3.22)
project(NGKsPlayerNative LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
set(CMAKE_AUTOUIC ON)

set(CMAKE_PREFIX_PATH "C:/Qt/6.10.2/msvc2022_64" CACHE PATH "Qt 6.10.2 MSVC 2022 path" FORCE)

find_package(Qt6 6.10 REQUIRED COMPONENTS Quick)

qt_standard_project_setup(REQUIRES 6.10)

qt_add_executable(NGKsPlayerNative
    src/ui/main.cpp
)

target_link_libraries(NGKsPlayerNative
    PRIVATE
        Qt6::Quick
)

add_custom_command(TARGET NGKsPlayerNative POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "$<TARGET_FILE_DIR:NGKsPlayerNative>/qml"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${CMAKE_CURRENT_SOURCE_DIR}/src/ui/qml/Main.qml"
        "$<TARGET_FILE_DIR:NGKsPlayerNative>/qml/Main.qml"
)
