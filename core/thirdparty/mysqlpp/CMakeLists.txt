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
if (DEFINED ENV{MILVUS_MYSQLPP_URL})
    set(MYSQLPP_SOURCE_URL "$ENV{MILVUS_MYSQLPP_URL}")
else ()
    set(MYSQLPP_SOURCE_URL "https://tangentsoft.com/mysqlpp/releases/mysql++-${MYSQLPP_VERSION}.tar.gz")
endif ()

macro( build_mysqlpp )
    message(STATUS "Building MySQL++-${MYSQLPP_VERSION} from source")

    # set(MYSQLPP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep")
    # set(MYSQLPP_INCLUDE_DIR "${MYSQLPP_PREFIX}/include")
    # set(MYSQLPP_SHARED_LIB
    #         "${MYSQLPP_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}")

    # set(MYSQLPP_CONFIGURE_ARGS
    #         "--prefix=${MYSQLPP_PREFIX}"
    #         "--enable-thread-check"
    #         "CFLAGS=${EP_C_FLAGS}"
    #         "CXXFLAGS=${EP_CXX_FLAGS}"
    #         "LDFLAGS=-pthread"
    #         )

    ExternalProject_Add(
        mysqlpp_ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/mysqlpp-subbuild
            DOWNLOAD_DIR        ${CMAKE_BINARY_DIR}/3rdparty_download/download
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
            URL                 ${MYSQLPP_SOURCE_URL}
            URL_MD5             "cda38b5ecc0117de91f7c42292dd1e79"
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure
                                    "--prefix=<INSTALL_DIR>"
                                    "--srcdir=<SOURCE_DIR>"
                                    "--enable-thread-check"
                                    "CFLAGS=${EP_C_FLAGS}"
                                    "CXXFLAGS=${EP_CXX_FLAGS}"
                                    "LDFLAGS=-pthread"
                                    "cc=${CCACHE_FOUND} ${CMAKE_C_COMPILER}"
                                    "cxx=${CCACHE_FOUND} ${CMAKE_CXX_COMPILER}"
            BUILD_COMMAND           ${MAKE} ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND ${MAKE} install
            ${EP_LOG_OPTIONS}
            BUILD_IN_SOURCE     1
            )

    ExternalProject_Get_Property( mysqlpp_ep SOURCE_DIR INSTALL_DIR )
    # message(FATAL_ERROR "${SOURCE_DIR}======${INSTALL_DIR}")
    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file(MAKE_DIRECTORY "${INSTALL_DIR}/include")
    endif()
    add_library(mysqlpp SHARED IMPORTED)
    set_target_properties( mysqlpp
            PROPERTIES
                IMPORTED_GLOBAL TRUE
                IMPORTED_LOCATION ${INSTALL_DIR}/lib/libmysqlpp.so 
                INTERFACE_INCLUDE_DIRECTORIES ${INSTALL_DIR}/include )

    add_dependencies(mysqlpp mysqlpp_ep)

endmacro()


build_mysqlpp()
install( FILES
    ${INSTALL_DIR}/lib/libmysqlpp.so
    ${INSTALL_DIR}/lib/libmysqlpp.so.3
    ${INSTALL_DIR}/lib/libmysqlpp.so.3.2.4
    DESTINATION lib )
# resolve_dependency(MySQLPP)
# get_target_property(MYSQLPP_INCLUDE_DIR mysqlpp INTERFACE_INCLUDE_DIRECTORIES)
# include_directories(SYSTEM "${MYSQLPP_INCLUDE_DIR}")
# link_directories(SYSTEM ${MYSQLPP_PREFIX}/lib)
