# Licensed under the Apache License, Version 2.0.
cmake_minimum_required(VERSION 3.18)

# Also usable as a dependency-free toolchain smoke test on the Linux runner.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(milvus_tsan_smoke LANGUAGES C CXX)
    option(USE_TSAN "Enable ThreadSanitizer" ON)
    option(USE_ASAN "Enable AddressSanitizer" OFF)
    include("${CMAKE_CURRENT_LIST_DIR}/../../cmake/Sanitizers.cmake")
    enable_testing()
endif()

find_package(Threads REQUIRED)
add_library(tsan_probe_access SHARED access.c access.cpp)
add_executable(tsan_probe probe.cpp)
target_compile_features(tsan_probe PRIVATE cxx_std_11)
target_link_libraries(tsan_probe PRIVATE tsan_probe_access Threads::Threads)

foreach(mode clean race-c race-cxx)
    add_test(NAME tsan-${mode}
        COMMAND "${CMAKE_COMMAND}"
            "-DPROBE=$<TARGET_FILE:tsan_probe>"
            "-DMODE=${mode}"
            -P "${CMAKE_CURRENT_LIST_DIR}/CheckReport.cmake")
    set_tests_properties(tsan-${mode} PROPERTIES LABELS tsan TIMEOUT 60)
endforeach()

# Do not install an intentionally racy executable into output/unittest: legacy
# runners execute every file in that directory and expect each one to pass.
