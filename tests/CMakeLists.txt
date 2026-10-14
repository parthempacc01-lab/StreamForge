include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
  URL_HASH SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(streamforge_smoke_test
  smoke_test.cpp
  frame_codec_test.cpp
  record_codec_test.cpp
  segment_log_test.cpp
  topic_catalog_test.cpp
  broker_service_test.cpp
  tcp_server_test.cpp
  group_coordinator_test.cpp
  replication_cluster_test.cpp
  benchmark_test.cpp
)
target_link_libraries(streamforge_smoke_test PRIVATE StreamForge::core GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(streamforge_smoke_test)

if(STREAMFORGE_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "STREAMFORGE_BUILD_FUZZERS requires a Clang compiler")
  endif()
  add_executable(streamforge_frame_codec_fuzz fuzz/frame_codec_fuzz.cpp)
  target_link_libraries(streamforge_frame_codec_fuzz PRIVATE StreamForge::core)
  target_compile_options(streamforge_frame_codec_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(streamforge_frame_codec_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)

  add_executable(streamforge_record_codec_fuzz fuzz/record_codec_fuzz.cpp)
  target_link_libraries(streamforge_record_codec_fuzz PRIVATE StreamForge::core)
  target_compile_options(streamforge_record_codec_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
  target_link_options(streamforge_record_codec_fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
endif()
