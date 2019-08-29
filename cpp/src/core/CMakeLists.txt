#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.14)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(KNOWHERE_VERSION "0.1.0")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" KNOWHERE_BASE_VERSION "${KNOWHERE_VERSION}")
project(knowhere VERSION "${KNOWHERE_BASE_VERSION}" LANGUAGES CUDA C CXX)
set(CMAKE_CXX_STANDARD 14)

set(KNOWHERE_VERSION_MAJOR "${knowhere_VERSION_MAJOR}")
set(KNOWHERE_VERSION_MINOR "${knowhere_VERSION_MINOR}")
set(KNOWHERE_VERSION_PATCH "${knowhere_VERSION_PATCH}")
if(KNOWHERE_VERSION_MAJOR STREQUAL ""
        OR KNOWHERE_VERSION_MINOR STREQUAL ""
        OR KNOWHERE_VERSION_PATCH STREQUAL "")
    message(FATAL_ERROR "Failed to determine Knowhere version from '${KNOWHERE_VERSION}'")
endif()

message(STATUS "Knowhere version: "
        "${KNOWHERE_VERSION_MAJOR}.${KNOWHERE_VERSION_MINOR}.${KNOWHERE_VERSION_PATCH} "
        "(full: '${KNOWHERE_VERSION}')")

# if no build build type is specified, default to release builds
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif(NOT CMAKE_BUILD_TYPE)

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -fopenmp")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -fopenmp")
endif()
MESSAGE(STATUS "CMAKE_CXX_FLAGS" ${CMAKE_CXX_FLAGS})

find_package(CUDA)
#set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES --expt-extended-lambda")
#set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message("building milvus_engine on x86 architecture")
    set(KNOWHERE_BUILD_ARCH x86_64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message("building milvus_engine on ppc architecture")
    set(KNOWHERE_BUILD_ARCH ppc64le)
else()
    message("unknown processor type")
    message("CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(KNOWHERE_BUILD_ARCH unknown)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "release")
else()
    set(BUILD_TYPE "debug")
endif()
message(STATUS "Build type = ${BUILD_TYPE}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(ExternalProject)
include(DefineOptions)
include(BuildUtils)
include(ThirdPartyPackages)

add_subdirectory(src)

if(BUILD_UNIT_TEST STREQUAL "ON")
    add_subdirectory(test)
endif()

config_summary()
