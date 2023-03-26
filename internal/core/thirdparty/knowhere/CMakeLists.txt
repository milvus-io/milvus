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

set( KNOWHERE_VERSION fbf2b6e6 )

message(STATUS "Building knowhere-${KNOWHERE_SOURCE_VER} from source")
set(MILVUS_BUILD_TYPE ${CMAKE_BUILD_TYPE})

set(WITH_DISKANN ${MILVUS_DISKANN} CACHE BOOL "" FORCE )
set(WITH_ASAN ${MILVUS_ASAN} CACHE BOOL "" FORCE )

if (MILVUS_GPU_VERSION STREQUAL "ON")
    set(USE_CUDA ON CACHE BOOL "" FORCE )
    set(WITH_RAFT ON CACHE BOOL "" FORCE )
endif ()

set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
FetchContent_Declare(
        knowhere
        GIT_REPOSITORY  "https://github.com/milvus-io/knowhere.git"
        GIT_TAG         ${KNOWHERE_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH})

FetchContent_GetProperties(knowhere)
if (NOT knowhere_POPULATED)
    FetchContent_Populate(knowhere)

    # Adding the following target:
    # knowhere
    add_subdirectory(${knowhere_SOURCE_DIR} ${knowhere_BINARY_DIR})
endif()

# get prometheus COMPILE_OPTIONS
get_property(var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS)

set(CMAKE_BUILD_TYPE ${MILVUS_BUILD_TYPE} CACHE STRING "" FORCE)
message(STATUS "knowhere src compile options: ${var} ${CMAKE_BUILD_TYPE}")
