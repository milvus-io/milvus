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
# Using default c and cxx compiler in our build tree
# Thirdpart cxx and c flags
append_flags(CMAKE_CXX_FLAGS FLAGS "-O3 -fPIC -Wno-error -fopenmp -Wno-macro-redefined")

if (NOT KNOWHERE_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

set(MAKE_BUILD_ARGS "-j6")

include(FetchContent)
set(FETCHCONTENT_BASE_DIR  ${MILVUS_BINARY_DIR}/3rdparty_download)
set(FETCHCONTENT_QUIET     OFF)

function(add_milvus_common)
    milvus_add_pkg_config("milvus-common")
    set( milvus_common_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/milvus-common )

    if ( NOT EXISTS "${milvus_common_SOURCE_DIR}/CMakeLists.txt" )
        message(FATAL_ERROR "The milvus-common submodule is not initialized. Please run:\n"
                            "  git submodule update --init internal/core/thirdparty/milvus-common")
    endif()

    set(WITH_JAEGER ON CACHE BOOL "" FORCE )
    set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )

    add_subdirectory( ${milvus_common_SOURCE_DIR}
                      ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-build )

    set( MILVUS_COMMON_INCLUDE_DIR ${milvus_common_SOURCE_DIR}/include CACHE INTERNAL "Path to milvus-common include directory" )

    # Mark as populated so knowhere's FetchContent_GetProperties skips its own milvus-common.
    # CMake 3.28 FetchContent checks DEFINED (via define_property), not just the value.
    set(__prefix "_FetchContent_milvus-common")
    define_property(GLOBAL PROPERTY ${__prefix}_sourceDir)
    set_property(GLOBAL PROPERTY ${__prefix}_sourceDir "${milvus_common_SOURCE_DIR}")
    define_property(GLOBAL PROPERTY ${__prefix}_binaryDir)
    set_property(GLOBAL PROPERTY ${__prefix}_binaryDir "${CMAKE_CURRENT_BINARY_DIR}/milvus-common-build")
    define_property(GLOBAL PROPERTY ${__prefix}_populated)
    set_property(GLOBAL PROPERTY ${__prefix}_populated TRUE)
endfunction()

function(add_knowhere)
    milvus_add_pkg_config("knowhere")
    set( knowhere_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/knowhere )

    if ( NOT EXISTS "${knowhere_SOURCE_DIR}/CMakeLists.txt" )
        message(FATAL_ERROR "The knowhere submodule is not initialized. Please run:\n"
                            "  git submodule update --init internal/core/thirdparty/knowhere")
    endif()

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

    set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )
    add_subdirectory( ${knowhere_SOURCE_DIR}
                      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build )

    get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
    message( STATUS "knowhere src compile options: ${var}" )
    set( KNOWHERE_INCLUDE_DIR ${knowhere_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory" )
endfunction()

# Find pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
add_milvus_common()
add_knowhere()

add_subdirectory(boost_ext)
add_subdirectory(rocksdb)
add_subdirectory(rdkafka)
add_subdirectory(simdjson)
add_subdirectory(bsoncxx)
if (USE_OPENDAL)
    add_subdirectory(opendal)
endif()
add_subdirectory(tantivy)

if (LINUX)
    add_subdirectory(jemalloc)
endif()

add_subdirectory(milvus-storage)
