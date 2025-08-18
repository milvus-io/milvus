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

add_library(milvus_bitset)

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

    # Perform a test for the ARM SVE command set.
    # Sometimes, a compiler may support it, but there are no headers available.
    include(CheckCXXCompilerFlag)
    include(CheckIncludeFileCXX)

    check_cxx_compiler_flag("-march=armv8-a+sve" COMPILER_SUPPORTS_SVE)
    check_include_file_cxx("arm_sve.h" COMPILER_HAS_ARM_SVE_HEADER)

    if(COMPILER_SUPPORTS_SVE)
        message(STATUS "Compiler supports SVE")
    else()
        message(STATUS "Compiler does NOT support SVE")
    endif()

    if(COMPILER_HAS_ARM_SVE_HEADER)
        message(STATUS "Compiler has arm_sve.h")
    else()
        message(STATUS "Compiler has no arm_sve.h")
    endif()

    # targeting AWS graviton, 
    #  https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md

    if (COMPILER_SUPPORTS_SVE AND COMPILER_HAS_ARM_SVE_HEADER)
        message(STATUS "SVE support for the bitset library is enabled")
        target_compile_definitions(milvus_bitset PRIVATE BITSET_ENABLE_SVE_SUPPORT=1)

        set_source_files_properties(detail/platform/arm/sve-inst.cpp PROPERTIES COMPILE_FLAGS "-march=armv8-a+sve")
    endif()
endif()

target_sources(milvus_bitset PRIVATE ${BITSET_SRCS})
