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

# Standalone library tests: avoids building Milvus callsites during API migration.
cmake_minimum_required(VERSION 3.20)
project(milvus_bitset_tests LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(GTEST_PREFIX "" CACHE PATH "GoogleTest installation")
option(SANITIZE "Enable ASan and UBSan" OFF)
option(SANITIZER_TEST_O1 "Use O1 for the template-heavy test TU" OFF)
if(SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()
find_package(Threads REQUIRED)
find_path(GTEST_INCLUDE_DIR gtest/gtest.h HINTS "${GTEST_PREFIX}/include" REQUIRED)
find_library(GTEST_LIBRARY gtest HINTS "${GTEST_PREFIX}/lib" REQUIRED)
get_filename_component(core "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)
set(platform "${core}/bitset/detail/platform")
add_library(actual_bitset STATIC "${platform}/dynamic.cpp" "${core}/bitset/detail/count_bulk.cpp")
target_include_directories(actual_bitset PUBLIC "${core}")
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
  target_sources(actual_bitset PRIVATE "${platform}/x86/avx2-inst.cpp" "${platform}/x86/avx512-inst.cpp" "${platform}/x86/instruction_set.cpp")
  set_source_files_properties("${platform}/x86/avx2-inst.cpp" PROPERTIES COMPILE_OPTIONS "-mavx2;-mavx;-mfma;-mbmi")
  set_source_files_properties("${platform}/x86/avx512-inst.cpp" PROPERTIES COMPILE_OPTIONS "-mavx512f;-mavx512bw;-mavx512vl;-mavx512dq;-mavx512cd;-mbmi")
else()
  target_sources(actual_bitset PRIVATE "${platform}/arm/neon-inst.cpp" "${platform}/arm/sve-inst.cpp" "${platform}/arm/instruction_set.cpp")
endif()
add_executable(bitset_tests "${core}/bitset/BitsetTest.cpp")
target_include_directories(bitset_tests PRIVATE "${GTEST_INCLUDE_DIR}")
target_link_libraries(bitset_tests PRIVATE actual_bitset "${GTEST_LIBRARY}" Threads::Threads)
if(SANITIZE AND SANITIZER_TEST_O1)
  target_compile_options(bitset_tests PRIVATE -O1)
endif()
enable_testing()
add_test(NAME bitset_tests COMMAND bitset_tests)
add_executable(bitset_header_only header_only.cpp)
target_include_directories(bitset_header_only PRIVATE "${core}")
add_test(NAME bitset_header_only COMMAND bitset_header_only)
