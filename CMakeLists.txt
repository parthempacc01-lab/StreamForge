cmake_minimum_required(VERSION 3.25)

project(StreamForge VERSION 0.1.0 LANGUAGES CXX)

# Visual Studio compiles sources in parallel by default. /FS serializes shared
# debug-database writes so multi-file test targets build reliably on MSVC.
if(MSVC)
  add_compile_options(/FS)
endif()

option(STREAMFORGE_BUILD_TESTING "Build StreamForge tests" ON)
option(STREAMFORGE_BUILD_FUZZERS "Build libFuzzer targets with Clang" OFF)
option(STREAMFORGE_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(STREAMFORGE_ENABLE_TSAN "Enable ThreadSanitizer on Linux with Clang or GCC" OFF)

if(STREAMFORGE_ENABLE_SANITIZERS AND STREAMFORGE_ENABLE_TSAN)
  message(FATAL_ERROR "STREAMFORGE_ENABLE_SANITIZERS and STREAMFORGE_ENABLE_TSAN require separate builds")
endif()

add_library(streamforge_core
  src/protocol/frame_codec.cpp
  src/network/tcp_server.cpp
  src/storage/record_codec.cpp
  src/storage/segment_log.cpp
  src/broker/topic_catalog.cpp
  src/broker/offset_store.cpp
  src/broker/group_coordinator.cpp
  src/broker/replication_cluster.cpp
  src/benchmark/benchmark.cpp
  src/broker/broker_service.cpp
)
add_library(StreamForge::core ALIAS streamforge_core)

target_compile_features(streamforge_core PUBLIC cxx_std_20)
target_include_directories(streamforge_core
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

find_package(Threads REQUIRED)
target_link_libraries(streamforge_core PUBLIC Threads::Threads)
if(WIN32)
  target_link_libraries(streamforge_core PUBLIC ws2_32)
endif()

add_executable(eventstream_server apps/server_main.cpp)
target_link_libraries(eventstream_server PRIVATE StreamForge::core)
add_executable(eventstream_benchmark apps/benchmark_main.cpp)
target_link_libraries(eventstream_benchmark PRIVATE StreamForge::core)
target_compile_definitions(eventstream_benchmark PRIVATE STREAMFORGE_BUILD_TYPE="$<CONFIG>" STREAMFORGE_COMPILER="${CMAKE_CXX_COMPILER_ID}-${CMAKE_CXX_COMPILER_VERSION}" STREAMFORGE_OS="${CMAKE_SYSTEM_NAME}")
add_executable(eventstream_tcp_benchmark apps/tcp_benchmark_main.cpp)
target_link_libraries(eventstream_tcp_benchmark PRIVATE StreamForge::core)
add_executable(eventstream_client apps/client_main.cpp)
target_link_libraries(eventstream_client PRIVATE StreamForge::core)

if(MSVC)
  target_compile_options(streamforge_core PRIVATE /W4 /permissive-)
else()
  target_compile_options(streamforge_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

if(STREAMFORGE_ENABLE_TSAN)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "STREAMFORGE_ENABLE_TSAN is supported on Linux only")
  endif()
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    message(FATAL_ERROR "STREAMFORGE_ENABLE_TSAN requires Clang or GCC")
  endif()
  target_compile_options(streamforge_core PUBLIC -fsanitize=thread -fno-omit-frame-pointer -g)
  target_link_options(streamforge_core PUBLIC -fsanitize=thread)
elseif(STREAMFORGE_ENABLE_SANITIZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    message(FATAL_ERROR "STREAMFORGE_ENABLE_SANITIZERS requires Clang or GCC")
  endif()
  target_compile_options(streamforge_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(streamforge_core PUBLIC -fsanitize=address,undefined)
endif()

if(STREAMFORGE_BUILD_TESTING)
  include(CTest)
  enable_testing()
  add_subdirectory(tests)
endif()
