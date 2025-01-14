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

set(BITSET_SRCS
    detail/platform/dynamic.cpp
)



if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    list(APPEND BITSET_SRCS
        detail/platform/x86/avx2-inst.cpp
        detail/platform/x86/avx512-inst.cpp
        detail/platform/x86/instruction_set.cpp
    )

    set_source_files_properties(detail/platform/x86/avx512-inst.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq -mavx512cd -mbmi")
    set_source_files_properties(detail/platform/x86/avx2-inst.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mavx -mfma -mbmi")

    # set_source_files_properties(detail/platform/dynamic.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq")
    # set_source_files_properties(detail/platform/dynamic.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mavx -mfma")
elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm*")
    list(APPEND BITSET_SRCS
        detail/platform/arm/neon-inst.cpp
        detail/platform/arm/sve-inst.cpp
        detail/platform/arm/instruction_set.cpp
    )

    # targeting AWS graviton, 
    #  https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md

    #set_source_files_properties(detail/platform/arm/sve-inst.cpp PROPERTIES COMPILE_FLAGS "-mcpu=neoverse-v1")
endif()

add_library(milvus_bitset OBJECT ${BITSET_SRCS})
