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

# *********************************** FetchContent ***********************************
# Cons: force install at INSTALL_DIRS, EXCLUDE_FROM_ALL can aviod install
# Pros: Simple, done at configure time, can be used immediately
# ************************************************************************************
# include( FetchContent )
# set( CMAKE_POLICY_DEFAULT_CMP0022 NEW ) # for googletest only
#
# FetchContent_Declare(
#     googletest
#     URL     "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
#             "https://gitee.com/quicksilver/googletest/repository/archive/release-${GTEST_VERSION}.zip"
#     URL_MD5 2e6fbeb6a91310a16efe181886c59596
# )
#
# if ( NOT googletest_POPULATED )
#     FetchContent_Populate( googletest )
#
#     add_subdirectory( ${googletest_SOURCE_DIR}
#                       ${googletest_BINARY_DIR}
#                       )
# endif()

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


message( STATUS "Building gtest-${GTEST_VERSION} from source" )
set( GTEST_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS} )
set( gtest_DOWNLOAD_DIR ${MILVUS_BINARY_DIR}/3rdparty_download/googletest_download )

# Download and unpack googletest at configure time
configure_file( CMakeLists.txt.in ${gtest_DOWNLOAD_DIR}/CMakeLists.txt )


execute_process(COMMAND             "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
                WORKING_DIRECTORY   ${gtest_DOWNLOAD_DIR}
)
execute_process(COMMAND             "${CMAKE_COMMAND}" --build .
                WORKING_DIRECTORY   ${gtest_DOWNLOAD_DIR}
)

# Add googletest directly to our build. This adds the following targets:
# gtest, gtest_main, gmock and gmock_main
add_subdirectory("${CMAKE_CURRENT_BINARY_DIR}/googletest-src"
                 "${CMAKE_CURRENT_BINARY_DIR}/googletest-build"
                 EXCLUDE_FROM_ALL
)
