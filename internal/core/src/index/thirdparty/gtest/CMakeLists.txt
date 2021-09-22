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
set( GTEST_VERSION 1.8.1 )

if ( DEFINED ENV{MILVUS_GTEST_URL} )
    set( GTEST_SOURCE_URL "$ENV{MILVUS_GTEST_URL}" )
else()
    set( GTEST_SOURCE_URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.zip" )
endif()

message( STATUS "Building gtest-${GTEST_VERSION} from source" )

set( CMAKE_POLICY_DEFAULT_CMP0022 NEW ) # for googletest only

FetchContent_Declare(
        googletest
        URL             ${GTEST_SOURCE_URL} 
        URL_MD5         "ad6868782b5952b7476a7c1c72d5a714"
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/googletest-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( googletest )
if ( NOT googletest_POPULATED )

    FetchContent_Populate( googletest )

    # Adding the following targets:
    # gtest, gtest_main, gmock, gmock_main
    add_subdirectory( ${googletest_SOURCE_DIR}
                      ${googletest_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()
# include(GoogleTest)

# ****************************************************************
# Create ALIAS Target
# ****************************************************************
# if (NOT TARGET GTest:gtest)
#     add_library( GTest::gtest ALIAS gtest )
# endif()
# if (NOT TARGET GTest:main)
#     add_library( GTest::main ALIAS gtest_main )
# endif()
# if (NOT TARGET GMock:gmock)
#     target_link_libraries( gmock INTERFACE GTest::gtest )
#     add_library( GMock::gmock ALIAS gmock )
# endif()
# if (NOT TARGET GMock:main)
#     target_link_libraries( gmock_main INTERFACE GTest::gtest )
#     add_library( GMock::main ALIAS gmock_main )
# endif()


get_property( var DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/googletest-src" PROPERTY COMPILE_OPTIONS )
message( STATUS "gtest compile options: ${var}" )
