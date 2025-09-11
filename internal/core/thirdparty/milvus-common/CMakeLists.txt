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

milvus_add_pkg_config("milvus-common")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( MILVUS-COMMON-VERSION aaff302 )
set( GIT_REPOSITORY "https://github.com/zilliztech/milvus-common.git")

message(STATUS "milvus-common repo: ${GIT_REPOSITORY}")
message(STATUS "milvus-common version: ${MILVUS-COMMON-VERSION}")

message(STATUS "Building milvus-common-${MILVUS-COMMON-VERSION} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

set(WITH_JAEGER ON CACHE BOOL "" FORCE )

set( CMAKE_PREFIX_PATH ${CONAN_BOOST_ROOT} )

FetchContent_Declare(
        milvus-common
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${MILVUS-COMMON-VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-build
        SOURCE_SUBDIR   cpp
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( milvus-common )
if ( NOT milvus-common_POPULATED )
    FetchContent_Populate( milvus-common )
    # Adding the following target:
    # milvus-common
    add_subdirectory( ${milvus-common_SOURCE_DIR}
                      ${milvus-common_BINARY_DIR} )
endif()

set( MILVUS_COMMON_INCLUDE_DIR ${milvus-common_SOURCE_DIR}/include CACHE INTERNAL "Path to milvus-common include directory" )
