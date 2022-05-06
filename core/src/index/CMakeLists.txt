#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------


cmake_minimum_required(VERSION 3.12)
message(STATUS "------------------------------KNOWHERE-----------------------------------")
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(KNOWHERE_VERSION "0.6.0")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" KNOWHERE_BASE_VERSION "${KNOWHERE_VERSION}")
project(knowhere VERSION "${KNOWHERE_BASE_VERSION}" LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)

set(KNOWHERE_VERSION_MAJOR "${knowhere_VERSION_MAJOR}")
set(KNOWHERE_VERSION_MINOR "${knowhere_VERSION_MINOR}")
set(KNOWHERE_VERSION_PATCH "${knowhere_VERSION_PATCH}")
if (KNOWHERE_VERSION_MAJOR STREQUAL ""
        OR KNOWHERE_VERSION_MINOR STREQUAL ""
        OR KNOWHERE_VERSION_PATCH STREQUAL "")
    message(FATAL_ERROR "Failed to determine Knowhere version from '${KNOWHERE_VERSION}'")
endif ()

message(STATUS "Knowhere version: "
        "${KNOWHERE_VERSION_MAJOR}.${KNOWHERE_VERSION_MINOR}.${KNOWHERE_VERSION_PATCH} "
        "(full: '${KNOWHERE_VERSION}')")

# if no build build type is specified, default to release builds
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif (NOT CMAKE_BUILD_TYPE)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message(STATUS "building milvus_engine on x86 architecture")
    set(KNOWHERE_BUILD_ARCH x86_64)
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message(STATUS "building milvus_engine on ppc architecture")
    set(KNOWHERE_BUILD_ARCH ppc64le)
else ()
    message(WARNING "unknown processor type")
    message(WARNING "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(KNOWHERE_BUILD_ARCH unknown)
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "release")
else ()
    set(BUILD_TYPE "debug")
endif ()
message(STATUS "Build type = ${BUILD_TYPE}")

set(INDEX_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(INDEX_BINARY_DIR ${PROJECT_BINARY_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${INDEX_SOURCE_DIR}/cmake")

include(ExternalProject)
include(DefineOptionsCore)
include(BuildUtilsCore)
if (MILVUS_FPGA_VERSION OR KNOWHERE_FPGA_VERSION)
    add_compile_definitions("MILVUS_FPGA_VERSION")
else ()
  message(STATUS "Building Knowhere CPU version")
endif ()
if (MILVUS_APU_VERSION OR KNOWHERE_APU_VERSION)
    add_compile_definitions("MILVUS_APU_VERSION")
else ()
  message(STATUS "Building Knowhere CPU version")
endif ()
if (MILVUS_GPU_VERSION OR KNOWHERE_GPU_VERSION)
    message(STATUS "Building Knowhere GPU version")
    add_compile_definitions("MILVUS_GPU_VERSION")
    enable_language(CUDA)
    find_package(CUDA 11 REQUIRED)
    set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES --expt-extended-lambda")
else ()
    message(STATUS "Building Knowhere CPU version")
endif ()

include(ThirdPartyPackagesCore)

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -DELPP_THREAD_SAFE -fopenmp")
    if (KNOWHERE_GPU_VERSION)
        set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O3")
    endif ()
else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -DELPP_THREAD_SAFE -fopenmp")
    if (KNOWHERE_GPU_VERSION)
        set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")
    endif ()
endif ()

add_subdirectory(knowhere)

if (BUILD_COVERAGE STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif ()

set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)

if (KNOWHERE_BUILD_TESTS)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DELPP_DISABLE_LOGS")
    add_subdirectory(unittest)
endif ()

config_summary()
