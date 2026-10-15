# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)
project(milvus_grep_standalone_test LANGUAGES CXX)
enable_testing()

set(GREP_RUST_ARCHIVE "" CACHE FILEPATH "Built libtantivy_binding.a")
if(NOT EXISTS "${GREP_RUST_ARCHIVE}")
    message(FATAL_ERROR "Set GREP_RUST_ARCHIVE to the built Rust binding archive")
endif()
get_filename_component(CORE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
find_package(Threads REQUIRED)
if(POLICY CMP0144)
    cmake_policy(SET CMP0144 NEW)
endif()
find_package(GTest REQUIRED)

add_library(tantivy_binding STATIC IMPORTED GLOBAL)
set_target_properties(tantivy_binding PROPERTIES
    IMPORTED_LOCATION "${GREP_RUST_ARCHIVE}"
    INTERFACE_INCLUDE_DIRECTORIES "${CORE_ROOT}/thirdparty/grep/include")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/.." grep)
target_compile_options(milvus_grep PRIVATE -Wall -Wextra -Werror -pedantic)

# Include the production C++ object directly, as milvus_core does. No -u or
# whole-archive flags: its ordinary C references must pull in all eight exports.
add_library(grep_shared_probe SHARED $<TARGET_OBJECTS:milvus_grep>)
target_link_libraries(grep_shared_probe PRIVATE tantivy_binding Threads::Threads ${CMAKE_DL_LIBS})
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(grep_shared_probe PRIVATE m rt util)
    target_link_options(grep_shared_probe PRIVATE "LINKER:--no-undefined")
endif()
add_executable(grep_test ../GrepTest.cpp)
target_compile_features(grep_test PRIVATE cxx_std_17)
target_include_directories(grep_test PRIVATE "${CORE_ROOT}/src")
target_link_libraries(grep_test PRIVATE grep_shared_probe GTest::gtest_main Threads::Threads)
add_test(NAME grep_test COMMAND grep_test)

# Same production object, but resolve its C calls against controlled failures.
# Do not link this target to the Rust archive or include it in all_tests.
add_executable(grep_fault_test ffi_faults.cc $<TARGET_OBJECTS:milvus_grep>)
target_compile_features(grep_fault_test PRIVATE cxx_std_17)
target_include_directories(grep_fault_test PRIVATE "${CORE_ROOT}/src"
    "${CORE_ROOT}/thirdparty/grep/include")
target_link_libraries(grep_fault_test PRIVATE GTest::gtest_main Threads::Threads)
add_test(NAME grep_fault_test COMMAND grep_fault_test)

option(GREP_SANITIZE "Instrument the C++ wrapper/tests with ASan and UBSan" OFF)
if(GREP_SANITIZE)
    foreach(target milvus_grep grep_test grep_fault_test)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    endforeach()
    foreach(target grep_shared_probe grep_test grep_fault_test)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endforeach()
endif()
