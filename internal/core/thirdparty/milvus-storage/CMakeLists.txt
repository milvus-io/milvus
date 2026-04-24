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

set( milvus-storage_VERSION 6f38add)
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

# Disable benchmark for milvus-storage (we don't need it and it may have missing deps)
set(WITH_BENCHMARK OFF CACHE BOOL "" FORCE)

# Conan 2.x: CMAKE_PREFIX_PATH is set by conan_toolchain.cmake.
# find_package(Protobuf) sets Protobuf_PROTOC_EXECUTABLE automatically.
# Fallback: search CMAKE_BINARY_DIR/bin for protoc installed by conan imports.
find_program(_PROTOC_PROGRAM protoc
    PATHS "${CMAKE_BINARY_DIR}/bin"
    NO_DEFAULT_PATH)
if(_PROTOC_PROGRAM)
    message(STATUS "Setting Protobuf_PROTOC_EXECUTABLE for milvus-storage: ${_PROTOC_PROGRAM}")
    set(Protobuf_PROTOC_EXECUTABLE "${_PROTOC_PROGRAM}" CACHE FILEPATH "" FORCE)
endif()

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

    # Adding the following target:
    # milvus-storage
    add_subdirectory( ${milvus-storage_SOURCE_DIR}/cpp
                      ${milvus-storage_BINARY_DIR} )

    # Enable S3 filesystem support on all platforms
    target_compile_definitions(milvus-storage PRIVATE MILVUS_S3_FS)

    # milvus-storage uses Aws::S3::S3Client and Aws::S3::Model::* directly but only
    # links AWS::aws-sdk-cpp-identity-management in its own CMakeLists.txt. The S3
    # symbols happen to resolve through Arrow's transitive dependency on aws-sdk-cpp-s3,
    # but this is fragile (depends on Arrow package config). Explicitly link the S3
    # component so the symbols are always resolved in libmilvus-storage.so.
    # Conan's cmake_find_package generator creates FindAWSSDK.cmake but it must be
    # explicitly loaded to create the AWS::* targets.
    find_package(AWSSDK REQUIRED)
    if(TARGET AWS::aws-sdk-cpp-s3)
        target_link_libraries(milvus-storage PUBLIC AWS::aws-sdk-cpp-s3)
    elseif(TARGET AWS::s3)
        target_link_libraries(milvus-storage PUBLIC AWS::s3)
    else()
        message(WARNING "Neither AWS::aws-sdk-cpp-s3 nor AWS::s3 target found after find_package(AWSSDK)")
    endif()

    # On macOS, aws-c-io uses Apple's Network.framework for socket operations
    if(APPLE)
        target_link_libraries(milvus-storage PUBLIC "-framework Network")
    endif()
endif()

set( MILVUS_STORAGE_INCLUDE_DIR ${milvus-storage_SOURCE_DIR}/cpp/include CACHE INTERNAL "Path to milvus-storage include directory" )

# Add the milvus-storage include directory to the target's interface include directories
# This ensures that consumers of milvus-storage can find <arrow/c/abi.h> which is bundled
# inside the milvus-storage include directory
target_include_directories(milvus-storage PUBLIC ${MILVUS_STORAGE_INCLUDE_DIR})
