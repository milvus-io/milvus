# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

set(MILVUS_SIMD_SRCS
    ref.cpp
    hook.cpp
)

if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    # x86 cpu simd
    message ("simd using x86_64 mode")
    list(APPEND MILVUS_SIMD_SRCS
                sse2.cpp
                sse4.cpp
                avx2.cpp
                avx512.cpp
    )
    set_source_files_properties(sse4.cpp PROPERTIES COMPILE_FLAGS "-msse4.2")
    set_source_files_properties(avx2.cpp PROPERTIES COMPILE_FLAGS "-mavx2")
    set_source_files_properties(avx512.cpp PROPERTIES COMPILE_FLAGS "-mavx512f  -mavx512dq -mavx512bw")
elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm*")
    # TODO: add arm cpu simd
endif()

add_library(milvus_simd ${MILVUS_SIMD_SRCS})

# Link the milvus_simd library with other libraries as needed
target_link_libraries(milvus_simd milvus_log)