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
add_library(milvus_minhash OBJECT ${SOURCE_FILES})

# Platform-specific SIMD optimizations
# NOTE: We use target_compile_options to only apply flags to milvus_minhash, not to segcore or other modules
include(CheckCXXCompilerFlag)
target_compile_options(milvus_minhash PRIVATE -march=native)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    CHECK_CXX_COMPILER_FLAG("-mavx512f" COMPILER_SUPPORTS_AVX512F)
    CHECK_CXX_COMPILER_FLAG("-mavx2" COMPILER_SUPPORTS_AVX2)

    if(COMPILER_SUPPORTS_AVX512F)
        target_compile_options(milvus_minhash PRIVATE
            -mavx512f
            -mavx512dq
            -mavx512bw
            -mavx512vl
        )
    elseif(COMPILER_SUPPORTS_AVX2)
        target_compile_options(milvus_minhash PRIVATE -mavx2)
    endif()
endif()

# Dependencies
# Note: xxhash and OpenSSL are required for hash computation
target_link_libraries(milvus_minhash PUBLIC
    xxhash::xxhash
    OpenSSL::SSL
)

target_include_directories(milvus_minhash PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/fusion_compute
)
