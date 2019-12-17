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
cmake_minimum_required(VERSION 3.12)
project(milvus_sdk LANGUAGES CXX C)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ExternalProject)
include(DefineOptions)
include(BuildUtils)
include(ThirdPartyPackages)

include_directories(include)
include_directories(examples)
include_directories(grpc-gen/gen-milvus)
include_directories(grpc-gen/gen-status)

# set build type
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "Release")
else ()
    set(BUILD_TYPE "Debug")
endif ()
message(STATUS "Build type = ${BUILD_TYPE}")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC")
else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC")
endif()
message(STATUS "Build type = ${BUILD_TYPE}")

unset(CMAKE_EXPORT_COMPILE_COMMANDS CACHE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED on)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message(STATUS "Building milvus_sdk on x86 architecture")
    set(MILVUS_BUILD_ARCH x86_64)
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message(STATUS "Building milvus_sdk on ppc architecture")
    set(MILVUS_BUILD_ARCH ppc64le)
else ()
    message(WARNING "Unknown processor type")
    message(WARNING "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(MILVUS_BUILD_ARCH unknown)
endif ()

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    if (NOT MSVC)
        find_program(MAKE make)
    endif ()
endif ()

aux_source_directory(interface interface_files)
aux_source_directory(grpc grpc_client_files)

set(grpc_service_files
        grpc-gen/gen-milvus/milvus.grpc.pb.cc
        grpc-gen/gen-milvus/milvus.pb.cc
        grpc-gen/gen-status/status.grpc.pb.cc
        grpc-gen/gen-status/status.pb.cc
        )

set(grpc_lib
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc
        )

add_library(milvus_sdk SHARED
        ${interface_files}
        ${grpc_client_files}
        ${grpc_service_files}
        )

target_link_libraries(milvus_sdk
        ${grpc_lib}
        zlib
        dl
        z
        )

install(TARGETS milvus_sdk DESTINATION lib)

add_subdirectory(examples)
