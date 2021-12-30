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

if(NOT DEFINED KNOWHERE_VERSION)
   set(KNOWHERE_VERSION 1.00)
endif()

if ( DEFINED ENV{KNOWHERE_GIT_URL} )
    set(KNOWHERE_GIT_URL "$ENV{KNOWHERE_URL}" )
else ()
    set(KNOWHERE_GIT_URL "https://github.com/milvus-io/knowhere.git" )
endif ()

message( STATUS "Building knowhere-${KNOWHERE_VERSION} from source" )
FetchContent_Declare(
    knowhere
    GIT_REPOSITORY         ${KNOWHERE_GIT_URL}
    GIT_TAG                 main
    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
    )

set(CMAKE_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}")
FetchContent_GetProperties(knowhere)
if ( NOT knowhere_POPULATED )
    FetchContent_Populate( knowhere )
    add_subdirectory( ${knowhere_SOURCE_DIR}
                        ${knowhere_BINARY_DIR}
                        EXCLUDE_FROM_ALL )
endif()

get_target_property( KNOWHERE_INCLUDE_DIR knowhere INCLUDE_DIRECTORIES )
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "== Knowhere compile options: ${var}" )

