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

if ( DEFINED ENV{KNOWHERE_LIBUNWIND_URL} )
    set( LIBUNWIND_SOURCE_URL "$ENV{KNOWHERE_LIBUNWIND_URL}" )
else ()
    set( LIBUNWIND_SOURCE_URL
         "https://github.com/libunwind/libunwind/releases/download/v${LIBUNWIND_VERSION}/libunwind-${LIBUNWIND_VERSION}.tar.gz" )
endif ()

if ( DEFINED ENV{KNOWHERE_GPERFTOOLS_URL} )
    set( GPERFTOOLS_SOURCE_URL "$ENV{KNOWHERE_GPERFTOOLS_URL}" )
else ()
    set( GPERFTOOLS_SOURCE_URL
         "https://github.com/gperftools/gperftools/releases/download/gperftools-${GPERFTOOLS_VERSION}/gperftools-${GPERFTOOLS_VERSION}.tar.gz" )
endif ()

# ----------------------------------------------------------------------
# libunwind

macro( build_libunwind )
    message( STATUS "Building libunwind-${LIBUNWIND_VERSION} from source" )

    set( LIBUNWIND_PREFIX  ${CMAKE_CURRENT_BINARY_DIR}/libunwind)
    ExternalProject_Add(
        libunwind_ep
            DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR     ${CMAKE_CURRENT_BINARY_DIR}/libunwind
            URL             ${LIBUNWIND_SOURCE_URL}
            URL_MD5         f625b6a98ac1976116c71708a73dc44a
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure
                                    "--prefix=<INSTALL_DIR>"
                                    "--quiet"
                                    "--disable-tests"
                                    "cc=${EP_C_COMPILER}"
                                    "cxx=${EP_CXX_COMPILER}"
            BUILD_COMMAND       ${MAKE}    ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND     ${MAKE}    install
            ${EP_LOG_OPTIONS} )

    ExternalProject_Get_Property( libunwind_ep INSTALL_DIR )
    file(MAKE_DIRECTORY ${INSTALL_DIR}/include)
    add_library( libunwind SHARED IMPORTED )
    set_target_properties(
        libunwind   PROPERTIES
                    IMPORTED_GLOBAL                 TRUE
                    IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libunwind.so"
                    INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include" )

    add_dependencies( libunwind libunwind_ep )
endmacro()


# ----------------------------------------------------------------------
# gperftools

macro( build_gperftools )
    message( STATUS "Building gperftools-${GPERFTOOLS_VERSION} from source" )

    ExternalProject_Add(
        gperftools_ep
            DEPENDS             libunwind_ep
            DOWNLOAD_DIR        ${CMAKE_BINARY_DIR}/3rdparty_download/download
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}/gperftools
            URL                 ${GPERFTOOLS_SOURCE_URL}
            URL_MD5             cb21f2ebe71bbc8d5ad101b310be980a
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure
                                    "--prefix=<INSTALL_DIR>"
                                    "--quiet"
                                    "cc=${EP_C_COMPILER}"
                                    "cxx=${EP_CXX_COMPILER}"
            BUILD_COMMAND       ${MAKE}     ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND     ${MAKE}     install
            ${EP_LOG_OPTIONS} )

    ExternalProject_Get_Property( gperftools_ep INSTALL_DIR )
    file(MAKE_DIRECTORY ${INSTALL_DIR}/include)
    # libprofiler.so
    add_library( gperftools SHARED IMPORTED )
    set_target_properties( gperftools
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libtcmalloc_and_profiler.so"
                INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
                INTERFACE_LINK_LIBRARIES        libunwind
                )
    add_dependencies( gperftools gperftools_ep )
endmacro()


build_libunwind()
build_gperftools()
get_target_property( GPERFTOOLS_LIB gperftools LOCATION )
install(FILES ${GPERFTOOLS_LIB} DESTINATION ${CMAKE_INSTALL_PREFIX})

