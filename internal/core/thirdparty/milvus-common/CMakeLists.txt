#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

milvus_add_pkg_config("milvus-common")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( MILVUS-COMMON-VERSION 60f998e4494ad1630ef5ce58a06cc26565ed5bc1 )
set( GIT_REPOSITORY "https://github.com/zilliztech/milvus-common.git")

message(STATUS "milvus-common repo: ${GIT_REPOSITORY}")
message(STATUS "milvus-common version: ${MILVUS-COMMON-VERSION}")

message(STATUS "Building milvus-common-${MILVUS-COMMON-VERSION} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

FetchContent_Declare(
        milvus-common
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${MILVUS-COMMON-VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( milvus-common )
FetchContent_MakeAvailable( milvus-common )

set( MILVUS_COMMON_INCLUDE_DIR ${milvus-common_SOURCE_DIR}/include CACHE INTERNAL "Path to milvus-common include directory" )

# The source-built target does not carry interface include directories (its own
# CMakeLists uses directory-level include_directories), but every consumer of
# the target needs them to resolve milvus-common's public headers. Dependency
# header paths (fmt, glog, ...) keep flowing through the target's PUBLIC
# linkage, matching what the Conan config exposed via transitive_headers.
target_include_directories(milvus-common INTERFACE ${MILVUS_COMMON_INCLUDE_DIR})

# Compatibility shim: the vendored knowhere and milvus-storage checkouts still
# call find_package(milvus-common) and link milvus-common::milvus-common, which
# the Conan CMakeDeps config used to provide. Route those lookups to the
# source-built target instead. This only takes effect when no real
# milvus-common config (Conan CMakeDeps or a user-provided one) is already
# visible: a project-level milvus-common_DIR must not be silently clobbered,
# so we only write the cache entry when it is not already defined.
set( MILVUS_COMMON_COMPAT_DIR ${CMAKE_CURRENT_BINARY_DIR}/milvus-common-compat )
configure_file( milvus-commonConfig.cmake.in
                "${MILVUS_COMMON_COMPAT_DIR}/milvus-commonConfig.cmake" @ONLY )
if( NOT DEFINED CACHE{milvus-common_DIR} )
    set( milvus-common_DIR "${MILVUS_COMMON_COMPAT_DIR}" CACHE PATH
         "Compat config for the source-built milvus-common" )
endif()
