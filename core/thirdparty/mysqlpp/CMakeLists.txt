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
if ( DEFINED ENV{MILVUS_MYSQLPP_URL} )
    set( MYSQLPP_SOURCE_URL "$ENV{MILVUS_MYSQLPP_URL}" )
else ()
    set( MYSQLPP_SOURCE_URL "https://tangentsoft.com/mysqlpp/releases/mysql++-${MYSQLPP_VERSION}.tar.gz" )
endif ()

macro( build_mysqlpp )
    message( STATUS "Building MySQL++-${MYSQLPP_VERSION} from source" )

    set( MYSQLPP_CXX_FLAGS "-O3 -fPIC -Wno-deprecated -Wno-format-truncation -fopenmp" )
    ExternalProject_Add(
            mysqlpp_ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/mysqlpp-subbuild
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
            URL                 ${MYSQLPP_SOURCE_URL}
            URL_MD5             "707a37cfcb93246570449bddd637210a"
            CONFIGURE_COMMAND   <SOURCE_DIR>/configure
                                    "--prefix=<INSTALL_DIR>"
                                    "--enable-thread-check"
                                    "CXXFLAGS=${MYSQLPP_CXX_FLAGS}"
                                    "LDFLAGS=-pthread"
                                    "cc=${EP_C_COMPILER}"
                                    "cxx=${EP_CXX_COMPILER}"
            BUILD_COMMAND       ${MAKE} ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND     ${MAKE} install
            BUILD_IN_SOURCE     1
            ${EP_LOG_OPTIONS} )

    ExternalProject_Get_Property( mysqlpp_ep INSTALL_DIR )
    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()

    add_library(mysqlpp SHARED IMPORTED)
    set_target_properties( mysqlpp
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/lib/libmysqlpp.so 
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )

    add_dependencies(mysqlpp mysqlpp_ep)

endmacro()


build_mysqlpp()
install( FILES  ${INSTALL_DIR}/lib/libmysqlpp.so
                ${INSTALL_DIR}/lib/libmysqlpp.so.3
                ${INSTALL_DIR}/lib/libmysqlpp.so.3.2.4
        DESTINATION lib )
