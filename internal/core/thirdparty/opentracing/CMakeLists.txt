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

if ( DEFINED ENV{MILVUS_OPENTRACING_URL} )
    set(OPENTRACING_SOURCE_URL "$ENV{MILVUS_OPENTRACING_URL}")
else ()
    set(OPENTRACING_SOURCE_URL
        "https://github.com/opentracing/opentracing-cpp/archive/${OPENTRACING_VERSION}.tar.gz" )
endif ()

message(STATUS "Building OPENTRACING-${OPENTRACING_VERSION} from source")

FetchContent_Declare(
    opentracing
    URL         ${OPENTRACING_SOURCE_URL}
    URL_MD5     "e598ba4b81ae8e1ceed8cd8bbf86f2fd"
    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/opentracing-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/opentracing-build
    )

set( BUILD_SHARED_LIBS  CACHE BOOL OFF FORCE)
set( ENABLE_LINTING     CACHE BOOL OFF FORCE)
set( BUILD_TESTING      CACHE BOOL OFF FORCE )
FetchContent_GetProperties( opentracing )
if ( NOT opentracing_POPULATED )

    FetchContent_Populate( opentracing )

    # Adding the following targets:
    # opentracing-static
    # opentracing_mocktracer-static
    add_subdirectory( ${opentracing_SOURCE_DIR}
                      ${opentracing_BINARY_DIR}
                      EXCLUDE_FROM_ALL )

    # Opentracing-cpp CMakeLists.txt file didn't give a
    # correct interface directories
    target_include_directories( opentracing-static
        INTERFACE   $<BUILD_INTERFACE:${opentracing_SOURCE_DIR}/include>
                    $<BUILD_INTERFACE:${opentracing_BINARY_DIR}/include>
                    $<BUILD_INTERFACE:${opentracing_SOURCE_DIR}/3rd_party/include> )
    target_include_directories( opentracing_mocktracer-static
        INTERFACE  $<BUILD_INTERFACE:${opentracing_SOURCE_DIR}/mocktracer/include> )
    # Adding the following ALIAS Targets:
    # opentracing::opentracing
    # opentracing::mocktracer
    if ( NOT TARGET opentracing::opentracing )
        add_library( opentracing::opentracing ALIAS opentracing-static )
    endif()
    if ( NOT TARGET opentracing::mocktracer )
        add_library( opentracing::mocktracer ALIAS opentracing_mocktracer-static )
    endif()

endif()

get_property( var DIRECTORY "${opentracing_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "opentracing compile options: ${var}" )
