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

set( KNOWHERE_VERSION v1.0.1 )
set( KNOWHERE_SOURCE_MD5 "0ac8a0802f94c2370f13a40527c57ced" )

if ( DEFINED ENV{MILVUS_KNOWHERE_URL} )
    set( KNOWHERE_SOURCE_URL "$ENV{MILVUS_KNOWHERE_URL}" )
else ()
    set( KNOWHERE_SOURCE_URL
        "https://github.com/milvus-io/knowhere/archive/refs/tags/${KNOWHERE_VERSION}.tar.gz" )
endif ()

#message( STATUS "Building knowhere-${KNOWHERE_VERSION} from source" )
#FetchContent_Declare(
#    knowhere
#    URL             ${KNOWHERE_SOURCE_URL}
#    URL_MD5         ${KNOWHERE_SOURCE_MD5}
#    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
#    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
#    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
#    )
#
#FetchContent_GetProperties(knowhere)
#if ( NOT knowhere_POPULATED )
#    FetchContent_Populate( knowhere )
#    add_subdirectory( ${knowhere_SOURCE_DIR}
#                      ${knowhere_BINARY_DIR}
#                      EXCLUDE_FROM_ALL )
#endif()
#
#get_target_property( KNOWHERE_INCLUDE_DIR knowhere INTERFACE_INCLUDE_DIRECTORIES )
#message( STATUS ${KNOWHERE_INCLUDE_DIR} )
#
#get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
#message( STATUS "knowhere compile options: ${var}" )
#

macro(build_knowhere)
    message(STATUS "Building knowhere-${KNOWHERE_VERSION} from source")
    set (KNOWHERE_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    set(KNOWHERE_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_BUILD_TYPE=Release
            "-DCMAKE_INSTALL_PREFIX=${KNOWHERE_INSTALL_PREFIX}"
            )

    externalproject_add(knowhere_ep
            URL             ${KNOWHERE_SOURCE_URL}
            URL_MD5         ${KNOWHERE_SOURCE_MD5}
            CMAKE_ARGS      ${KNOWHERE_CMAKE_ARGS}
            BUILD_COMMAND   ${MAKE} ${MAKE_BUILD_ARGS}
            PREFIX          ${CMAKE_BINARY_DIR}/3rdparty_download/knowhere-subbuild
            BINARY_DIR      knowhere-bin
            INSTALL_DIR     ${KNOWHERE_INSTALL_PREFIX}
            )

    ExternalProject_Get_Property(knowhere_ep INSTALL_DIR)
    message(STATUS ${INSTALL_DIR})

    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()

    add_library(knowhere SHARED IMPORTED)
    set_target_properties( knowhere
            PROPERTIES
                IMPORTED_GLOBAL     TRUE
                IMPORTED_LOCATION   ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}knowhere${CMAKE_SHARED_LIBRARY_SUFFIX}
                INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})

    add_dependencies(knowhere knowhere_ep)
endmacro()

set(KNOWHERE_SOURCE "AUTO")
if (KNOWHERE_SOURCE STREQUAL "AUTO")
    find_package(knowhere)
    message(STATUS "knowhere libraries: ${KNOWHERE_LIBRARIES}")
    message(STATUS "knowhere found: ${KNOWHERE_FOUND}")

    if (KNOWHERE_FOUND)
        add_library(knowhere)
    else()
        build_knowhere()
    endif()
elseif (KNOWHERE_SOURCE STREQUAL "BUNDLED")
    build_knowhere()
elseif (KNOWHERE_SOURCE STREQUAL "SYSTEM")
    find_package(knowhere REQUIRED)
    add_library(knowhere)
endif ()
