# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required( VERSION 3.18 )

if ( APPLE )
    set( CMAKE_CROSSCOMPILING TRUE )
    set( RUN_HAVE_GNU_POSIX_REGEX 0 )
endif ()

add_definitions(-DELPP_THREAD_SAFE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
message( STATUS "Building using CMake version: ${CMAKE_VERSION}" )

if ( MILVUS_GPU_VERSION )
    add_definitions(-DMILVUS_GPU_VERSION)
endif ()

if ( USE_DYNAMIC_SIMD ) 
    add_definitions(-DUSE_DYNAMIC_SIMD)
endif()

project(core)
include(CheckCXXCompilerFlag)
if ( APPLE )
    message(STATUS "==============Darwin Environment==============")
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    message(STATUS "==============Linux Environment===============")
    set(LINUX TRUE)
elseif ( MSYS )
    message( STATUS "==============MSYS Environment===============" )
else ()
    message(FATAL_ERROR "Unsupported platform!" )
endif ()

if (CMAKE_COMPILER_IS_GNUCC)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 11.99)
        # ignore deprecated declarations for gcc>=12
        # TODO: this workaround may removed when protobuf upgraded
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-declarations")
    endif ()
endif ()

set( CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake" )
include( Utils )

# **************************** Build time, type and code version ****************************
get_current_time( BUILD_TIME )
message( STATUS "Build time = ${BUILD_TIME}" )

get_build_type( TARGET BUILD_TYPE DEFAULT "Release" )
message( STATUS "Build type = ${BUILD_TYPE}" )

get_milvus_version( TARGET MILVUS_VERSION DEFAULT "2.0" )
message( STATUS "Build version = ${MILVUS_VERSION}" )

get_last_commit_id( LAST_COMMIT_ID )
message( STATUS "LAST_COMMIT_ID = ${LAST_COMMIT_ID}" )

set( CMAKE_EXPORT_COMPILE_COMMANDS ON )

# **************************** Project ****************************
project( milvus VERSION "${MILVUS_VERSION}" )

set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )

set( MILVUS_SOURCE_DIR      ${PROJECT_SOURCE_DIR} )
set( MILVUS_BINARY_DIR      ${PROJECT_BINARY_DIR} )
set( MILVUS_ENGINE_SRC      ${PROJECT_SOURCE_DIR}/src )
set( MILVUS_THIRDPARTY_SRC  ${PROJECT_SOURCE_DIR}/thirdparty )

# This will set RPATH to all excutable TARGET
# self-installed dynamic libraries will be correctly linked by excutable
set( CMAKE_INSTALL_RPATH "/usr/lib" "${CMAKE_INSTALL_PREFIX}/lib" "${CMAKE_INSTALL_PREFIX}/lib64" )
set( CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE )

# **************************** Dependencies ****************************
list( APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/conan )
include( ${CMAKE_BINARY_DIR}/conan/conanbuildinfo.cmake )
set( CONAN_DISABLE_CHECK_COMPILER ON )
conan_basic_setup( KEEP_RPATHS )

set( CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CONAN_PROTOBUF_ROOT})

include( CTest )
include( BuildUtils )
include( DefineOptions )
using_ccache_if_defined(MILVUS_USE_CCACHE)

include( ExternalProject )
include( GNUInstallDirs )
include( FetchContent )

# this MACRO is used to show knowhere prometheus metrics
add_definitions(-DNOT_COMPILE_FOR_SWIG)
include_directories( thirdparty )

set( FETCHCONTENT_BASE_DIR ${MILVUS_BINARY_DIR}/3rdparty_download )
set( FETCHCONTENT_QUIET OFF )
include( ThirdPartyPackages )

# **************************** Compiler arguments ****************************
message( STATUS "Building Milvus CPU version" )

if (LINUX OR MSYS)
    append_flags( CMAKE_CXX_FLAGS
                  FLAGS
                  "-fPIC"
                  "-DELPP_THREAD_SAFE"
                  "-fopenmp"
                  "-Werror"
                  "-O3"
                  )
endif ()

if ( APPLE )
    append_flags( CMAKE_CXX_FLAGS
            FLAGS
            "-fPIC"
            "-DELPP_THREAD_SAFE"
            "-fopenmp"
            "-Wno-error"
            "-Wsign-compare"
            "-Wall"
            "-pedantic"
            "-Wno-unused-command-line-argument"
            "-Wextra"
            "-Wno-unused-parameter"
            "-Wno-deprecated"
            "-DBOOST_STACKTRACE_GNU_SOURCE_NOT_REQUIRED=1"
            #"-fvisibility=hidden"
            #"-fvisibility-inlines-hidden"
            )
endif ()

# Set SIMD to CMAKE_CXX_FLAGS
if (OPEN_SIMD) 
    message(STATUS "open simd function, CPU_ARCH:${CPU_ARCH}")
    if (${CPU_ARCH} STREQUAL "avx")
      #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -ftree-vectorize -mavx2 -mfma -mavx -mf16c ")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma -mavx -mf16c ")
    elseif (${CPU_ARCH} STREQUAL "sse")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 ")
    elseif (${CPU_ARCH} STREQUAL "arm64")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=apple-m1+crc ")
    endif()
endif ()

# **************************** Coding style check tools ****************************
find_package( ClangTools )
set( BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support" )
message(STATUS "CMAKE_SOURCE_DIR is at ${CMAKE_SOURCE_DIR}" )

if("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

#
# "make lint" target
#
if ( NOT MILVUS_VERBOSE_LINT )
    set( MILVUS_LINT_QUIET "--quiet" )
endif ()

if ( NOT LINT_EXCLUSIONS_FILE )
    # source files matching a glob from a line in this file
    # will be excluded from linting (cpplint, clang-tidy, clang-format)
    set( LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt )
endif ()

add_custom_target(lint
    ${PYTHON_EXECUTABLE}    ${BUILD_SUPPORT_DIR}/run_cpplint.py
                            --cpplint_binary    ${CPPLINT_BIN}
                            --exclude_globs     ${LINT_EXCLUSIONS_FILE}
                            --source_dir            ${CMAKE_CURRENT_SOURCE_DIR}/src
                            ${MILVUS_LINT_QUIET}
                            )


find_program( CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR} )
message( STATUS "Found cpplint executable at ${CPPLINT_BIN}" )

# Use the first Python installation on PATH, not the newest one
set(Python3_FIND_STRATEGY "LOCATION")
# On Windows, use registry last, not first
set(Python3_FIND_REGISTRY "LAST")
# On macOS, use framework last, not first
set(Python3_FIND_FRAMEWORK "LAST")
find_package(Python3)
set(PYTHON_EXECUTABLE ${Python3_EXECUTABLE})

message( STATUS "Found Python at ${PYTHON_EXECUTABLE}")

#
# "make clang-format" and "make check-clang-format" targets
#
if ( ${CLANG_FORMAT_FOUND} )
    # runs clang format and updates files in place.
    add_custom_target( clang-format
        ${PYTHON_EXECUTABLE}    ${BUILD_SUPPORT_DIR}/run_clang_format.py
                                --clang_format_binary   ${CLANG_FORMAT_BIN}
                                --exclude_globs         ${LINT_EXCLUSIONS_FILE}
                                --source_dir            ${CMAKE_CURRENT_SOURCE_DIR}/src
                                --fix
                                ${MILVUS_LINT_QUIET} )

    # runs clang format and exits with a non-zero exit code if any files need to be reformatted
    add_custom_target( check-clang-format
        ${PYTHON_EXECUTABLE}    ${BUILD_SUPPORT_DIR}/run_clang_format.py
                                --clang_format_binary   ${CLANG_FORMAT_BIN}
                                --exclude_globs         ${LINT_EXCLUSIONS_FILE}
                                --source_dir            ${CMAKE_CURRENT_SOURCE_DIR}/src
                                ${MILVUS_LINT_QUIET} )
endif ()

#
# "make clang-tidy" and "make check-clang-tidy" targets
#
if ( ${CLANG_TIDY_FOUND} )
    # runs clang-tidy and attempts to fix any warning automatically
    add_custom_target( clang-tidy
        ${PYTHON_EXECUTABLE}    ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                                --clang_tidy_binary ${CLANG_TIDY_BIN}
                                --exclude_globs     ${LINT_EXCLUSIONS_FILE}
                                --compile_commands  ${CMAKE_BINARY_DIR}/compile_commands.json
                                --source_dir        ${CMAKE_CURRENT_SOURCE_DIR}/src
                                --fix
                                ${MILVUS_LINT_QUIET} )

    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
    add_custom_target( check-clang-tidy
        ${PYTHON_EXECUTABLE}    ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
                                --clang_tidy_binary ${CLANG_TIDY_BIN}
                                --exclude_globs     ${LINT_EXCLUSIONS_FILE}
                                --compile_commands  ${CMAKE_BINARY_DIR}/compile_commands.json
                                --source_dir        ${CMAKE_CURRENT_SOURCE_DIR}/src
                                ${MILVUS_LINT_QUIET} )
endif ()

#
# Validate and print out Milvus configuration options
#
config_summary()

# **************************** Source files ****************************
if ( BUILD_UNIT_TEST STREQUAL "ON" AND BUILD_COVERAGE STREQUAL "ON" )
    append_flags( CMAKE_CXX_FLAGS
                  FLAGS
                        "-fprofile-arcs"
                        "-ftest-coverage"
                        )
endif ()

if ( BUILD_DISK_ANN STREQUAL "ON" )
    ADD_DEFINITIONS(-DBUILD_DISK_ANN=${BUILD_DISK_ANN})
endif ()

# Warning: add_subdirectory(src) must be after append_flags("-ftest-coverage"),
# otherwise cpp code coverage tool will miss src folder
add_subdirectory( thirdparty )
add_subdirectory( src )

# Unittest lib
if ( BUILD_UNIT_TEST STREQUAL "ON" )
    append_flags( CMAKE_CXX_FLAGS FLAGS "-DELPP_DISABLE_LOGS")
    add_subdirectory(unittest)
endif ()

add_custom_target( Clean-All COMMAND ${CMAKE_BUILD_TOOL} clean )

# **************************** Install ****************************

# Install storage
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/storage/
        DESTINATION include/storage
        FILES_MATCHING PATTERN "*_c.h"
)

# Install segcore
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/segcore/
        DESTINATION include/segcore
        FILES_MATCHING PATTERN "*_c.h"
)

# Install indexbuilder
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/indexbuilder/
        DESTINATION include/indexbuilder
        FILES_MATCHING PATTERN "*_c.h"
)

# Install common
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/common/
        DESTINATION include/common
        FILES_MATCHING PATTERN "*_c.h"
)

install(DIRECTORY ${CMAKE_BINARY_DIR}/lib/
        DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)

