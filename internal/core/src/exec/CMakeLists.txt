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

add_source_at_current_directory_recursively()
add_library(milvus_exec OBJECT ${SOURCE_FILES})

# ── Per-arch SIMD compile flags for runtime dispatch ──
# Each SimdFilter*.cpp is compiled with arch-specific flags so xsimd
# generates the correct instructions. The dispatcher (SimdFilter.cpp)
# is compiled with default flags and selects the best impl at runtime.
if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    # Baseline must stay SSE2-only even if project adds global -mavx2.
    # -mno-avx prevents auto-vectorization from emitting AVX instructions.
    # SKIP_PRECOMPILE_HEADERS disables PCH for these files because the PCH
    # is built with different target features, causing clang to reject the
    # AST file.
    set_source_files_properties(
        expression/SimdFilter.cpp
        PROPERTIES COMPILE_FLAGS "-mno-avx" SKIP_PRECOMPILE_HEADERS ON)
    set_source_files_properties(
        expression/SimdFilterAvx2.cpp
        PROPERTIES COMPILE_FLAGS "-mavx2 -mfma" SKIP_PRECOMPILE_HEADERS ON)
    set_source_files_properties(
        expression/SimdFilterAvx512.cpp
        PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq" SKIP_PRECOMPILE_HEADERS ON)
endif()
