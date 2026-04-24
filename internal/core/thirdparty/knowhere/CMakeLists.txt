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

# Update KNOWHERE_VERSION for the first occurrence
milvus_add_pkg_config("knowhere")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( KNOWHERE_VERSION 868634bc )
set( GIT_REPOSITORY  "https://github.com/zilliztech/knowhere.git")

message(STATUS "Knowhere repo: ${GIT_REPOSITORY}")
message(STATUS "Knowhere version: ${KNOWHERE_VERSION}")

message(STATUS "Building knowhere-${KNOWHERE_SOURCE_VER} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

if ( BUILD_DISK_ANN STREQUAL "ON" )
    set(WITH_DISKANN ON CACHE BOOL "" FORCE )
else ()
    set(WITH_DISKANN OFF CACHE BOOL "" FORCE )
endif ()

if ( INDEX_ENGINE STREQUAL "cardinal" )
    set(WITH_CARDINAL ON CACHE BOOL "" FORCE )
else ()
    set(WITH_CARDINAL OFF CACHE BOOL "" FORCE )
endif()

if ( MILVUS_GPU_VERSION STREQUAL "ON" )
    set(WITH_CUVS ON CACHE BOOL "" FORCE )
endif ()

if ( WITH_SVS STREQUAL "ON" )
    set(WITH_SVS ON CACHE BOOL "" FORCE )
else ()
    set(WITH_SVS OFF CACHE BOOL "" FORCE )
endif()

# Conan 2.x: CMAKE_PREFIX_PATH is set by conan_toolchain.cmake
FetchContent_Declare(
        knowhere
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${KNOWHERE_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( knowhere )
if ( NOT knowhere_POPULATED )
    FetchContent_Populate( knowhere )

    # Adding the following target:
    # knowhere
    add_subdirectory( ${knowhere_SOURCE_DIR}
                      ${knowhere_BINARY_DIR} )
endif()

# Install the SVS runtime shared library alongside libknowhere.so so that
# the Go linker (and the final binary at runtime) can find it.
# svs_SOURCE_DIR is set inside knowhere's subdirectory scope and does not
# propagate here, so we derive the path from FETCHCONTENT_BASE_DIR which
# is in the CMake cache (set by internal/core/CMakeLists.txt).
if ( WITH_SVS STREQUAL "ON" )
    set( _SVS_LIB_DIR "${FETCHCONTENT_BASE_DIR}/svs-src/lib" )
    if ( EXISTS "${_SVS_LIB_DIR}" )
        install(DIRECTORY ${_SVS_LIB_DIR}/
                DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING PATTERN "libsvs_runtime.so*")
    else()
        message(WARNING "SVS enabled but ${_SVS_LIB_DIR} not found \u2014 libsvs_runtime.so will not be installed")
    endif()
endif()

# get prometheus COMPILE_OPTIONS
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "knowhere src compile options: ${var}" )

set( KNOWHERE_INCLUDE_DIR ${knowhere_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory" )
