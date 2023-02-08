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

set( KNOWHERE_VERSION v1.3.7 )
set( KNOWHERE_SOURCE_MD5 "a418d9c8e507d2fed22dc7c3cfa8b703")

if ( DEFINED ENV{MILVUS_KNOWHERE_URL} )
    set( KNOWHERE_SOURCE_URL "$ENV{MILVUS_KNOWHERE_URL}" )
else ()
    set( KNOWHERE_SOURCE_URL
        "https://github.com/milvus-io/knowhere/archive/refs/tags/${KNOWHERE_VERSION}.tar.gz" )
endif ()

macro(build_knowhere)
    message(STATUS "Building knowhere-${KNOWHERE_VERSION} from source")
    set (KNOWHERE_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

    message(STATUS ${CMAKE_BUILD_TYPE})
    set(KNOWHERE_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_INSTALL_PREFIX=${KNOWHERE_INSTALL_PREFIX}
            )

    if ( BUILD_DISK_ANN STREQUAL "ON" )
        set(KNOWHERE_CMAKE_ARGS
                ${KNOWHERE_CMAKE_ARGS}
            -DKNOWHERE_WITH_DISKANN=true)
    else ()
        set(KNOWHERE_CMAKE_ARGS
                ${KNOWHERE_CMAKE_ARGS}
            -DKNOWHERE_WITH_DISKANN=false)
    endif ()

    externalproject_add(knowhere_ep
            # GIT_REPOSITORY  "https://github.com/milvus-io/knowhere.git"
            # GIT_TAG         main
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

    if (MSYS)
        set_target_properties( knowhere
            PROPERTIES
                IMPORTED_GLOBAL     TRUE
                IMPORTED_IMPLIB     ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}knowhere${CMAKE_SHARED_LIBRARY_SUFFIX}.a
                INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})
    else ()
        set_target_properties( knowhere
                PROPERTIES
                IMPORTED_GLOBAL     TRUE
                IMPORTED_LOCATION   ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/${CMAKE_SHARED_LIBRARY_PREFIX}knowhere${CMAKE_SHARED_LIBRARY_SUFFIX}
                INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})
    endif ()

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
