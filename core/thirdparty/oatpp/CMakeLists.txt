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
if ( DEFINED ENV{MILVUS_OATPP_URL} )
    set( OATPP_SOURCE_URL "$ENV{MILVUS_OATPP_URL}" )
else()
#    set( OATPP_SOURCE_URL "https://github.com/oatpp/oatpp/archive/${OATPP_VERSION}.tar.gz" )
    set( OATPP_SOURCE_URL "https://github.com/BossZou/oatpp/archive/${OATPP_VERSION}.tar.gz" )
endif()

message( STATUS "Building oatpp-${OATPP_VERSION} from source" )
FetchContent_Declare(
        oatpp
        URL             ${OATPP_SOURCE_URL}
        URL_MD5         "5a59afb847e93e2ec4c71025f91590c7"
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/oatpp-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/oatpp-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )


FetchContent_GetProperties( oatpp )
if ( NOT oatpp_POPULATED )

    FetchContent_Populate( oatpp )

    set( OATPP_BUILD_TESTS  CACHE BOOL OFF FORCE )
    set( OATPP_INSTALL      CACHE BOOL OFF FORCE )
    set( OATPP_LINK_ATOMIC  CACHE BOOL OFF FORCE )
    set( OATPP_DISABLE_LOGV CACHE BOOL ON  FORCE )
    set( OATPP_DISABLE_LOGD CACHE BOOL ON  FORCE )
    set( OATPP_DISABLE_LOGI CACHE BOOL ON  FORCE )
    # Adding the following targets:
    # oatpp, oatpp-test
    add_subdirectory( ${oatpp_SOURCE_DIR}
                      ${oatpp_BINARY_DIR}
                      EXCLUDE_FROM_ALL )

endif()

get_target_property( OATPP_INCLUDE_DIR oatpp INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${OATPP_INCLUDE_DIR} )

get_property( var DIRECTORY "${OATPP_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "oatpp compile options: ${var}" )
