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

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(include)
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

find_package(Python COMPONENTS Interpreter Development)
find_package(ClangTools)
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

#
# "make lint" target
#
if (NOT MILVUS_VERBOSE_LINT)
    set(MILVUS_LINT_QUIET "--quiet")
endif ()

if (NOT LINT_EXCLUSIONS_FILE)
    # source files matching a glob from a line in this file
    # will be excluded from linting (cpplint, clang-tidy, clang-format)
    set(LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt)
endif ()

find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

#
# "make lint" targets
#
add_custom_target(lint
        ${PYTHON_EXECUTABLE}
        ${BUILD_SUPPORT_DIR}/run_cpplint.py
        --cpplint_binary
        ${CPPLINT_BIN}
        --exclude_globs
        ${LINT_EXCLUSIONS_FILE}
        --source_dir
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${MILVUS_LINT_QUIET})

#
# "make clang-format" and "make check-clang-format" targets
#
if (${CLANG_FORMAT_FOUND})
    # runs clang format and updates files in place.
    add_custom_target(clang-format
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_format.py
            --clang_format_binary
            ${CLANG_FORMAT_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            --fix
            ${MILVUS_LINT_QUIET})

    # runs clang format and exits with a non-zero exit code if any files need to be reformatted
    add_custom_target(check-clang-format
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_format.py
            --clang_format_binary
            ${CLANG_FORMAT_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MILVUS_LINT_QUIET})
endif ()

#
# "make clang-tidy" and "make check-clang-tidy" targets
#
if (${CLANG_TIDY_FOUND})
    # runs clang-tidy and attempts to fix any warning automatically
    add_custom_target(clang-tidy
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
            --clang_tidy_binary
            ${CLANG_TIDY_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --compile_commands
            ${CMAKE_BINARY_DIR}/compile_commands.json
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            --fix
            ${MILVUS_LINT_QUIET})

    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
    add_custom_target(check-clang-tidy
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
            --clang_tidy_binary
            ${CLANG_TIDY_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --compile_commands
            ${CMAKE_BINARY_DIR}/compile_commands.json
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MILVUS_LINT_QUIET})
endif ()

