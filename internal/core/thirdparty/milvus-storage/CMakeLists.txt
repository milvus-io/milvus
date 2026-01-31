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
set( milvus-storage_VERSION 88c2551)
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

    # Patch milvus-storage CMakeLists.txt to enable S3 support on all platforms
    # On macOS, we use Homebrew's aws-sdk-cpp (1.11.735+) which has macOS 15+ compatibility
    # On Linux, we use Conan's aws-sdk-cpp
    set(_STORAGE_CMAKE "${milvus-storage_SOURCE_DIR}/cpp/CMakeLists.txt")
    file(READ "${_STORAGE_CMAKE}" _STORAGE_CMAKE_CONTENT)

    # Enable S3 support on all platforms (including macOS with Homebrew)
    # The original line links AWS SDK unconditionally, we add MILVUS_S3_FS definition
    # IMPORTANT: On macOS, we must use Conan's Boost headers to match the linked library.
    # Homebrew has Boost 1.90 with different symbols (e.g. absolute_v3) than Conan's Boost 1.83.
    # We ignore Homebrew's Boost to ensure Conan's FindBoost.cmake is used.
    string(REPLACE
        "find_package(Boost REQUIRED)"
        "# Ignore Homebrew Boost - we must use Conan's Boost 1.83 to match linked library\nlist(APPEND CMAKE_IGNORE_PATH \"/opt/homebrew\" \"/opt/homebrew/include\" \"/opt/homebrew/lib\")\nfind_package(Boost REQUIRED COMPONENTS filesystem system)"
        _STORAGE_CMAKE_CONTENT "${_STORAGE_CMAKE_CONTENT}")

    string(REPLACE
        "list(APPEND LINK_LIBS arrow::arrow Boost::boost protobuf::protobuf AWS::aws-sdk-cpp-identity-management google-cloud-cpp::storage libavrocpp::libavrocpp Folly::folly fmt::fmt-header-only)"
        "list(APPEND LINK_LIBS arrow::arrow Boost::filesystem Boost::system protobuf::protobuf google-cloud-cpp::storage libavrocpp::libavrocpp Folly::folly fmt::fmt-header-only)\n# Enable S3 support on all platforms\nadd_compile_definitions(MILVUS_S3_FS)\nif(APPLE)\n  # On macOS, use Homebrew's AWS SDK (1.11.735+ with macOS 15+ compatibility)\n  # Remove Homebrew from ignore list just for AWS SDK lookup\n  list(REMOVE_ITEM CMAKE_IGNORE_PATH \"/opt/homebrew\" \"/opt/homebrew/include\" \"/opt/homebrew/lib\")\n  set(AWSSDK_DIR \"/opt/homebrew/lib/cmake/AWSSDK\")\n  find_package(AWSSDK REQUIRED COMPONENTS core s3 identity-management)\n  list(APPEND LINK_LIBS \${AWSSDK_LINK_LIBRARIES})\nelse()\n  # On Linux, use Conan's AWS SDK\n  list(APPEND LINK_LIBS AWS::aws-sdk-cpp-identity-management)\nendif()"
        _STORAGE_CMAKE_CONTENT "${_STORAGE_CMAKE_CONTENT}")

    file(WRITE "${_STORAGE_CMAKE}" "${_STORAGE_CMAKE_CONTENT}")

    message(STATUS "Patched milvus-storage CMakeLists.txt for cross-platform S3 support")

    # Adding the following target:
    # milvus-storage
    add_subdirectory( ${milvus-storage_SOURCE_DIR}/cpp
                      ${milvus-storage_BINARY_DIR} )
endif()

set( MILVUS_STORAGE_INCLUDE_DIR ${milvus-storage_SOURCE_DIR}/cpp/include CACHE INTERNAL "Path to milvus-storage include directory" )
