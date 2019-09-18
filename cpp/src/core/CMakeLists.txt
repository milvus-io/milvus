#-------------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#-------------------------------------------------------------------------------


cmake_minimum_required(VERSION 3.14)
message(STATUS "---------------core--------------")
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

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message(STATUS "building milvus_engine on x86 architecture")
    set(KNOWHERE_BUILD_ARCH x86_64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message(STATUS "building milvus_engine on ppc architecture")
    set(KNOWHERE_BUILD_ARCH ppc64le)
else()
    message(WARNING "unknown processor type")
    message(WARNING "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(KNOWHERE_BUILD_ARCH unknown)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "release")
else()
    set(BUILD_TYPE "debug")
endif()
message(STATUS "Build type = ${BUILD_TYPE}")

set(CORE_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(CORE_BINARY_DIR ${PROJECT_BINARY_DIR})
message(STATUS "Core source dir: ${PROJECT_SOURCE_DIR}")
message(STATUS "Core binary dir: ${PROJECT_BINARY_DIR}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CORE_SOURCE_DIR}/cmake")

include(ExternalProject)
include(DefineOptionsCore)
include(BuildUtilsCore)
include(ThirdPartyPackagesCore)

add_subdirectory(knowhere)

if (BUILD_COVERAGE STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

set(CORE_INCLUDE_DIRS ${CORE_INCLUDE_DIRS} PARENT_SCOPE)

if(BUILD_UNIT_TEST STREQUAL "ON")
    add_subdirectory(test)
endif()

config_summary()
