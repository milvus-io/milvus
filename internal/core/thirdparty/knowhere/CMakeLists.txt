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
set( KNOWHERE_VERSION 3cade7e )
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

set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )
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

    # Patch knowhere cmake for macOS ARM64 - include hook.cc but exclude SVE sources
    # SVE (Scalable Vector Extension) is not supported on Apple Silicon
    # But hook.cc contains required symbols (fvec_hook, fvec_madd) needed for linking
    if(APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        set(_KNOWHERE_FAISS_CMAKE "${knowhere_SOURCE_DIR}/cmake/libs/libfaiss.cmake")
        file(READ "${_KNOWHERE_FAISS_CMAKE}" _FAISS_CMAKE_CONTENT)

        # Verify patch targets exist (detect upstream changes early)
        string(FIND "${_FAISS_CMAKE_CONTENT}" "set(UTILS_SRC src/simd/distances_ref.cc src/simd/distances_neon.cc)" _UTILS_POS)
        if(_UTILS_POS EQUAL -1)
            message(FATAL_ERROR "knowhere faiss patch failed: UTILS_SRC pattern not found in ${_KNOWHERE_FAISS_CMAKE}. "
                "Upstream cmake may have changed (version: ${KNOWHERE_VERSION}).")
        endif()
        string(FIND "${_FAISS_CMAKE_CONTENT}" "set(UTILS_SVE_SRC src/simd/hook.cc src/simd/distances_sve.cc)" _SVE_POS)
        if(_SVE_POS EQUAL -1)
            message(FATAL_ERROR "knowhere faiss patch failed: UTILS_SVE_SRC pattern not found in ${_KNOWHERE_FAISS_CMAKE}. "
                "Upstream cmake may have changed (version: ${KNOWHERE_VERSION}).")
        endif()
        string(FIND "${_FAISS_CMAKE_CONTENT}" "set(ALL_UTILS_SRC \${UTILS_SRC} \${UTILS_SVE_SRC})" _ALL_UTILS_POS)
        if(_ALL_UTILS_POS EQUAL -1)
            message(FATAL_ERROR "knowhere faiss patch failed: ALL_UTILS_SRC pattern not found in ${_KNOWHERE_FAISS_CMAKE}. "
                "Upstream cmake may have changed (version: ${KNOWHERE_VERSION}).")
        endif()

        # Step 1: Add hook.cc to UTILS_SRC for Apple (contains required symbols)
        string(REPLACE
            "set(UTILS_SRC src/simd/distances_ref.cc src/simd/distances_neon.cc)"
            "set(UTILS_SRC src/simd/distances_ref.cc src/simd/distances_neon.cc src/simd/hook.cc)"
            _FAISS_CMAKE_CONTENT "${_FAISS_CMAKE_CONTENT}")

        # Step 2: On Apple, exclude only distances_sve.cc from UTILS_SVE_SRC
        string(REPLACE
            "set(UTILS_SVE_SRC src/simd/hook.cc src/simd/distances_sve.cc)"
            "set(UTILS_SVE_SRC src/simd/distances_sve.cc)"
            _FAISS_CMAKE_CONTENT "${_FAISS_CMAKE_CONTENT}")

        # Step 3: Exclude SVE sources on Apple
        string(REPLACE
            "set(ALL_UTILS_SRC \${UTILS_SRC} \${UTILS_SVE_SRC})"
            "if(APPLE)\n    set(ALL_UTILS_SRC \${UTILS_SRC})\n    message(STATUS \"Excluding SVE sources on Apple (hook.cc included in UTILS_SRC)\")\n  else()\n    set(ALL_UTILS_SRC \${UTILS_SRC} \${UTILS_SVE_SRC})\n  endif()"
            _FAISS_CMAKE_CONTENT "${_FAISS_CMAKE_CONTENT}")

        file(WRITE "${_KNOWHERE_FAISS_CMAKE}" "${_FAISS_CMAKE_CONTENT}")
        message(STATUS "Patched knowhere cmake: hook.cc included, SVE excluded on Apple")
    endif()

    # Adding the following target:
    # knowhere
    add_subdirectory( ${knowhere_SOURCE_DIR}
                      ${knowhere_BINARY_DIR} )
endif()

# get prometheus COMPILE_OPTIONS
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "knowhere src compile options: ${var}" )

set( KNOWHERE_INCLUDE_DIR ${knowhere_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory" )
