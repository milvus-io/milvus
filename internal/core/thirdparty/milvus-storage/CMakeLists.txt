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

# Update milvus-storage_VERSION for the first occurrence
milvus_add_pkg_config("milvus-storage")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( milvus-storage_VERSION 0a9cb7ebe8a9bcd1be15a59c7fa870aff7554593)
set( GIT_REPOSITORY  "https://github.com/milvus-io/milvus-storage.git")
message(STATUS "milvus-storage repo: ${GIT_REPOSITORY}")
message(STATUS "milvus-storage version: ${milvus-storage_VERSION}")

message(STATUS "Building milvus-storage-${milvus-storage_SOURCE_VER} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

if ( ENABLE_AZURE_FS STREQUAL "ON" )
    set(WITH_AZURE_FS ON CACHE BOOL "" FORCE )
else ()
    set(WITH_AZURE_FS OFF CACHE BOOL "" FORCE )
endif ()

set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )
FetchContent_Declare(
        milvus-storage
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${milvus-storage_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-build
        SOURCE_SUBDIR   cpp
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( milvus-storage )
if ( NOT milvus-storage_POPULATED )
    FetchContent_Populate( milvus-storage )

    if(APPLE)
        # On macOS, Conan's aws-c-* packages use deprecated Security framework APIs
        # removed in macOS 15+. Instead, use Homebrew's aws-sdk-cpp (1.11.735+)
        # which has the compatibility fixes.
        #
        # Strategy: create an IMPORTED target with the Conan target name
        # (AWS::aws-sdk-cpp-identity-management) that wraps Homebrew's AWS SDK.
        # This way milvus-storage's unmodified CMakeLists.txt links correctly.
        execute_process(
            COMMAND brew --prefix
            OUTPUT_VARIABLE _HOMEBREW_PREFIX
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(NOT _HOMEBREW_PREFIX)
            set(_HOMEBREW_PREFIX "/opt/homebrew")
        endif()
        if(EXISTS "${_HOMEBREW_PREFIX}/lib/cmake/AWSSDK")
            set(AWSSDK_DIR "${_HOMEBREW_PREFIX}/lib/cmake/AWSSDK")
        else()
            message(FATAL_ERROR "AWS SDK cmake config not found in Homebrew "
                "(searched ${_HOMEBREW_PREFIX}/lib/cmake/AWSSDK). "
                "Install with: brew install aws-sdk-cpp")
        endif()
        find_package(AWSSDK REQUIRED COMPONENTS core s3 identity-management)

        # aws-sdk-cpp 1.11.750 had a packaging bug: the core CMakeLists.txt
        # was missing the install() rule for pagination headers, so
        # Paginator.h is absent from the installation even though the STS
        # module's STSPaginationBase.h references it.  Homebrew backported
        # the fix (commit 175e803) with rebuild 1.  Fail early with a clear
        # message so developers know to reinstall.
        if(NOT EXISTS "${_HOMEBREW_PREFIX}/include/aws/core/utils/pagination/Paginator.h")
            message(FATAL_ERROR
                "aws/core/utils/pagination/Paginator.h is missing from "
                "Homebrew's aws-sdk-cpp installation.  This is a known "
                "packaging bug in aws-sdk-cpp 1.11.750.  "
                "Fix with:  brew reinstall aws-sdk-cpp")
        endif()

        if(NOT TARGET AWS::aws-sdk-cpp-identity-management)
            add_library(AWS::aws-sdk-cpp-identity-management INTERFACE IMPORTED)
            set_target_properties(AWS::aws-sdk-cpp-identity-management PROPERTIES
                INTERFACE_LINK_LIBRARIES "${AWSSDK_LINK_LIBRARIES}")
        endif()

        # Ignore Homebrew paths so milvus-storage's find_package(Boost) uses
        # Conan's Boost 1.83, not Homebrew's Boost 1.90+ (ABI incompatible).
        set(_SAVED_CMAKE_IGNORE_PATH "${CMAKE_IGNORE_PATH}")
        list(APPEND CMAKE_IGNORE_PATH
            "/opt/homebrew" "/opt/homebrew/include" "/opt/homebrew/lib"
            "/usr/local" "/usr/local/include" "/usr/local/lib")
    endif()

    # Adding the following target:
    # milvus-storage
    add_subdirectory( ${milvus-storage_SOURCE_DIR}/cpp
                      ${milvus-storage_BINARY_DIR} )

    # Enable S3 filesystem support on all platforms
    target_compile_definitions(milvus-storage PRIVATE MILVUS_S3_FS)

    if(APPLE)
        set(CMAKE_IGNORE_PATH "${_SAVED_CMAKE_IGNORE_PATH}")
    endif()
endif()

set( MILVUS_STORAGE_INCLUDE_DIR ${milvus-storage_SOURCE_DIR}/cpp/include CACHE INTERNAL "Path to milvus-storage include directory" )

# Add the milvus-storage include directory to the target's interface include directories
# This ensures that consumers of milvus-storage can find <arrow/c/abi.h> which is bundled
# inside the milvus-storage include directory
target_include_directories(milvus-storage PUBLIC ${MILVUS_STORAGE_INCLUDE_DIR})
