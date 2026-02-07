# Copyright (C) 2019-2025 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

# MinHash module build configuration

# Find required packages (cmake_find_package modules from conan)
find_package(xxHash REQUIRED)
find_package(OpenSSL REQUIRED)

add_source_at_current_directory_recursively()

# Separate SIMD implementation files from main source
list(FILTER SOURCE_FILES EXCLUDE REGEX "fusion_compute/x86/.*")
list(FILTER SOURCE_FILES EXCLUDE REGEX "fusion_compute/arm/.*")

add_library(milvus_minhash OBJECT ${SOURCE_FILES})

# Build strategy: Compile all SIMD variants unconditionally
# This ensures build/deploy machine separation - binary contains all variants
# Runtime CPU detection will select the optimal implementation based on actual hardware

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    # Unconditionally compile AVX2 variant
    set(AVX2_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx2.cpp
    )
    set_source_files_properties(${AVX2_SOURCES} PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")
    target_sources(milvus_minhash PRIVATE ${AVX2_SOURCES})
    message(STATUS "Building with AVX2 support")

    # Unconditionally compile AVX512 variant
    set(AVX512_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx512.cpp
    )
    set_source_files_properties(${AVX512_SOURCES} PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512dq -mavx512bw -mavx512vl")
    target_sources(milvus_minhash PRIVATE ${AVX512_SOURCES})
    message(STATUS "Building with AVX512 support")

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(aarch64)|(ARM64)|(arm64)")
    # Unconditionally compile NEON variant
    set(NEON_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_neon.cpp
    )
    target_sources(milvus_minhash PRIVATE ${NEON_SOURCES})
    message(STATUS "Building with NEON support")

    # SVE is only supported on Linux aarch64, not on macOS (Apple Silicon doesn't have SVE)
    if(NOT APPLE)
        set(SVE_SOURCES
            ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_sve.cpp
        )
        set_source_files_properties(${SVE_SOURCES} PROPERTIES COMPILE_FLAGS "-march=armv8-a+sve")
        target_sources(milvus_minhash PRIVATE ${SVE_SOURCES})
        message(STATUS "Building with SVE support")
    else()
        message(STATUS "SVE support disabled on macOS (Apple Silicon doesn't have SVE)")
    endif()
endif()

target_link_libraries(milvus_minhash PUBLIC
    xxHash::xxhash
    OpenSSL::SSL
)

target_include_directories(milvus_minhash PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute
)
