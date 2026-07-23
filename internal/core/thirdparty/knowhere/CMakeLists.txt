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
# Pinned to an immutable commit SHA on 6si/knowhere@gpu-hnsw rather than the
# branch name: a branch is mutable and the build could change with no milvus PR.
# Repoint to a tagged 6si/knowhere release before merging to production. This SHA
# is 6si/knowhere@gpu-hnsw merged up to knowhere main, so it INCLUDES the
# expected<T> iterator error-code interface (knowhere #1699) that the segcore
# adaptation on milvus master now depends on. It re-vendors the
# vanilla-faiss-structured GpuIndexHNSW (built on faiss::IndexHNSW,
# cppcontrib-free) and rewires GpuHnswIndexNode to be a pure consumer of it: the
# deserialized cppcontrib index is converted to a vanilla faiss::IndexHNSW and
# uploaded via copyFrom(), cosine folded into normalize+IP. It carries GPU-side
# filtered search, warp-cooperative layer-0 distance, and nq-chunked
# visited-bitmap VRAM bounding. Quantized-cosine numerics differ from the legacy
# inverse-norm path (recall-gated; may require re-ingest).
# Accurate int8 cosine: QT_8bit_direct_signed collapses L2-normalized vectors
# (components ~1/sqrt(d)) onto a few of the 256 levels, so int8 cosine is
# re-encoded as fp16 at load (validated fp16 GPU path; the int8 query is widened
# to fp32 through searchHost). int8 L2/IP keep direct_signed + DP4A.
# Includes the faiss review fixes: upload the filter bitset in
# GpuIndexHNSW::searchImpl_ (standard search() path); guard extract_hnsw_layers
# against an OOB nb_neighbors(1) read on a layer-0-only index; and select the
# int8 DP4A layer-0 path on a per-search staged flag (not a leftover pooled
# d_queries_i8 buffer) so a fp32-query search reusing the slot can't score
# against stale int8 data. Also converts the standard search() label output
# on-device (convert_labels_kernel) instead of a D2H->host->H2D round-trip;
# milvus uses the searchHost path so this is a no-op for production but keeps
# the vendored tree in sync with faiss.
set( KNOWHERE_VERSION 2959fe3dead76ca5d11f1c2611d60d1af9f48932 )
set( GIT_REPOSITORY  "https://github.com/6si/knowhere.git")

message(STATUS "Knowhere repo: ${GIT_REPOSITORY}")
message(STATUS "Knowhere version: ${KNOWHERE_VERSION}")

message(STATUS "Building knowhere-${KNOWHERE_VERSION} from source")
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
    set(raft_DIR "${FETCHCONTENT_BASE_DIR}/raft-build" CACHE PATH "Path to raft build directory" FORCE)
    set(rmm_DIR "${FETCHCONTENT_BASE_DIR}/rmm-build" CACHE PATH "Path to rmm build directory" FORCE)
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
        message(WARNING "SVS enabled but ${_SVS_LIB_DIR} not found -- libsvs_runtime.so will not be installed")
    endif()
endif()

if ( WITH_CUVS STREQUAL "ON" )
    set( _RMM_LIB_DIR "${FETCHCONTENT_BASE_DIR}/rmm-build" )
    if ( EXISTS "${_RMM_LIB_DIR}" )
        install(DIRECTORY ${_RMM_LIB_DIR}/
                DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING PATTERN "librmm.so*")
    else()
        message(WARNING "cuVS enabled but ${_RMM_LIB_DIR} not found -- librmm.so will not be installed")
    endif()

    set( _RAPIDS_LOGGER_LIB_DIR "${FETCHCONTENT_BASE_DIR}/rapids_logger-build" )
    if ( EXISTS "${_RAPIDS_LOGGER_LIB_DIR}" )
        install(DIRECTORY ${_RAPIDS_LOGGER_LIB_DIR}/
                DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING PATTERN "librapids_logger.so*")
    else()
        message(WARNING "cuVS enabled but ${_RAPIDS_LOGGER_LIB_DIR} not found -- librapids_logger.so will not be installed")
    endif()
endif()

# get prometheus COMPILE_OPTIONS
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "knowhere src compile options: ${var}" )

set( KNOWHERE_INCLUDE_DIR ${knowhere_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory" )
