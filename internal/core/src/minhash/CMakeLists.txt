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
add_source_at_current_directory_recursively()

# Separate SIMD implementation files from main source
list(FILTER SOURCE_FILES EXCLUDE REGEX "fusion_compute/x86/.*")
list(FILTER SOURCE_FILES EXCLUDE REGEX "fusion_compute/arm/.*")

add_library(milvus_minhash OBJECT ${SOURCE_FILES})

include(CheckCXXCompilerFlag)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    CHECK_CXX_COMPILER_FLAG("-mavx2" COMPILER_SUPPORTS_AVX2)
    if(COMPILER_SUPPORTS_AVX2)
        set(AVX2_SOURCES
            ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx2.cpp
        )
        if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx2.cpp)
            file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx2.cpp
                "// This file intentionally left empty - implementation is in header\n")
        endif()
        set_source_files_properties(${AVX2_SOURCES} PROPERTIES COMPILE_FLAGS "-mavx2 -mfma")
        target_sources(milvus_minhash PRIVATE ${AVX2_SOURCES})
    else()
        message(WARNING "Compiler does not support -mavx2 flag. AVX2 optimization will not be available.")
    endif()

    CHECK_CXX_COMPILER_FLAG("-mavx512f" COMPILER_SUPPORTS_AVX512F)
    if(COMPILER_SUPPORTS_AVX512F)
        set(AVX512_SOURCES
            ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx512.cpp
        )
        if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx512.cpp)
            file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/x86/fusion_compute_avx512.cpp
                "// This file intentionally left empty - implementation is in header\n")
        endif()
        set_source_files_properties(${AVX512_SOURCES} PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512dq -mavx512bw -mavx512vl")
        target_sources(milvus_minhash PRIVATE ${AVX512_SOURCES})
    else()
        message(WARNING "Compiler does not support -mavx512f flag. AVX512 optimization will not be available.")
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(aarch64)|(ARM64)")
    set(NEON_SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_neon.cpp
    )
    if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_neon.cpp)
        file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_neon.cpp
            "// This file intentionally left empty - implementation is in header\n")
    endif()
    target_sources(milvus_minhash PRIVATE ${NEON_SOURCES})

    CHECK_CXX_COMPILER_FLAG("-march=armv8-a+sve" COMPILER_SUPPORTS_SVE)
    if(COMPILER_SUPPORTS_SVE)
        set(SVE_SOURCES
            ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_sve.cpp
        )
        if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_sve.cpp)
            file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute/arm/fusion_compute_sve.cpp
                "// This file intentionally left empty - implementation is in header\n")
        endif()
        set_source_files_properties(${SVE_SOURCES} PROPERTIES COMPILE_FLAGS "-march=armv8-a+sve")
        target_sources(milvus_minhash PRIVATE ${SVE_SOURCES})
    endif()
endif()

target_link_libraries(milvus_minhash PUBLIC
    xxhash::xxhash
    OpenSSL::SSL
)

target_include_directories(milvus_minhash PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute
)
