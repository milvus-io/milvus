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
if(NOT DEFINED FIU_VERSION)
   set(FIU_VERSION 1.00)
endif()

if ( DEFINED ENV{Knowhere_FIU_URL} )
    set( FIU_SOURCE_URL "$ENV{MILVUS_FIU_URL}" )
else ()
    set( FIU_SOURCE_URL "https://github.com.cnpmjs.org/albertito/libfiu/archive/${FIU_VERSION}.tar.gz" )
endif ()

macro( build_fiu )
    message( STATUS "Building FIU-${FIU_VERSION} from source" )
    ExternalProject_Add(
            fiu_ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/fiu-subbuild
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
            URL                 ${FIU_SOURCE_URL}
            URL_MD5             "75f9d076daf964c9410611701f07c61b"
            CONFIGURE_COMMAND   ""
            BUILD_IN_SOURCE     1
            BUILD_COMMAND       ${MAKE}
            INSTALL_COMMAND     ${MAKE} "PREFIX=<INSTALL_DIR>" install
            ${EP_LOG_OPTIONS}
            )

    ExternalProject_Get_Property( fiu_ep INSTALL_DIR )
    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()
    add_library( fiu SHARED IMPORTED )
    set_target_properties( fiu
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/lib/libfiu.so
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies(fiu fiu_ep)
endmacro()

build_fiu()

install( FILES  ${INSTALL_DIR}/lib/libfiu.so
                ${INSTALL_DIR}/lib/libfiu.so.0
                ${INSTALL_DIR}/lib/libfiu.so.1.00
        DESTINATION lib )

get_target_property( var fiu INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${var} )
set_directory_properties( PROPERTY INCLUDE_DIRECTORIES ${var} )
