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

set( MILVUS_STORAGE_VERSION 9d1ad9c)

message(STATUS "Building milvus-storage-${MILVUS_STORAGE_VERSION} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

# message(FATAL_ERROR ${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage.patch)
# set(milvus-storage-patch git apply --ignore-whitespace ${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage.patch)
set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )
FetchContent_Declare(
        milvus-storage
        GIT_REPOSITORY  "https://github.com/milvus-io/milvus-storage.git"
        GIT_TAG         ${MILVUS_STORAGE_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-storage-build
        SOURCE_SUBDIR   cpp
        PATCH_COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage_CMakeLists.txt <SOURCE_DIR>/cpp/CMakeLists.txt
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_MakeAvailable(milvus-storage)
# target_compile_features(milvus-storage PUBLIC cxx_std_20)

# FetchContent_GetProperties( milvus-storage )
# if ( NOT milvus-storage_POPULATED )
#     FetchContent_Populate( milvus-storage)

#     # Adding the following target:
#     add_subdirectory( ${milvus-storage_SOURCE_DIR}/cpp
#                       ${milvus-storage_BINARY_DIR} )
# endif()

# message(FATAL_ERROR ${milvus-storage_SOURCE_DIR}  ${milvus-storage_BINARY_DIR})
# get prometheus COMPILE_OPTIONS
# get_property( var DIRECTORY "${milvus-storage_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "milvus-storage src compile options: ${var}" )
# unset(CMAKE_CXX_STANDARD)
