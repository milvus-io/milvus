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
if ( DEFINED ENV{MILVUS_GTEST_URL} )
    set( GTEST_SOURCE_URL "$ENV{MILVUS_GTEST_URL}" )
else ()
    set( GTEST_SOURCE_URL
            "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
            "https://gitee.com/quicksilver/googletest/repository/archive/release-${GTEST_VERSION}.zip" )
endif ()

macro ( build_gtest )
    message( STATUS "Building gtest-${GTEST_VERSION} from source" )
    set( GTEST_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS} )

    ExternalProject_Add(
        googletest_ep
        INSTALL_DIR     ${CMAKE_CURRENT_BINARY_DIR}/gtest_install
        URL             ${GTEST_SOURCE_URL}
        URL_MD5         "2e6fbeb6a91310a16efe181886c59596"
        BUILD_COMMAND   ${MAKE} ${MAKE_BUILD_ARGS}
        CMAKE_ARGS      ${EP_COMMON_CMAKE_ARGS}
                        -D CMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
                        -D CMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS}
                        -D CMAKE_BUILD_TYPE=Release
        ${EP_LOG_OPTIONS}
    )

    ExternalProject_Get_Property( googletest_ep INSTALL_DIR )
    message( STATUS "gtest install directory: " ${INSTALL_DIR} )

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY ${INSTALL_DIR}/include)
    # Add Target gtest, gtest_main, gmock, gmock_main
    add_library( gtest STATIC IMPORTED GLOBAL )
    set_target_properties(
        gtest
        PROPERTIES
        IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libgtest.a"
        INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
    )

    add_library( gtest_main STATIC IMPORTED GLOBAL )
    set_target_properties(
        gtest_main
        PROPERTIES
        IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libgtest_main.a"
        INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
    )

    add_library( gmock STATIC IMPORTED GLOBAL )
    set_target_properties(
        gmock
        PROPERTIES 
        IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libgmock.a"
        INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
    )

    add_library( gmock_main STATIC IMPORTED GLOBAL )
    set_target_properties(
        gmock_main
        PROPERTIES 
        IMPORTED_LOCATION               "${INSTALL_DIR}/lib/libgmock_main.a"
        INTERFACE_INCLUDE_DIRECTORIES   "${INSTALL_DIR}/include"
    )

    add_dependencies( gtest      googletest_ep )
    add_dependencies( gtest_main googletest_ep )
    add_dependencies( gmock      googletest_ep )
    add_dependencies( gmock_main googletest_ep )
endmacro()

if (MILVUS_BUILD_TESTS)
    build_gtest()
endif ()
# *********************************** find gtest packages ***********************************
# set( GTest_USE_STATIC_LIBS TRUE )
# set( GTest_USR_SHARED_LIBS FALSE )
# find_package( GTest CONFIG
#                     QUIET
#                     REQUIRED
#                     COMPONENTS gtest gmock gtest_main gmock_main
#                     PATHS ${INSTALL_DIR}/lib
#                     NO_DEFAULT_PATH
#                     NO_SYSTEM_ENVIRONMENT_PATH )
# if (GTest_FOUND)
#     set_target_properties(  GTest::gtest
#                             GTest::gmock
#                             GTest::gtest_main
#                             GTest::gmock_main
#           PROPERTIES IMPORTED_GLOBAL TRUE)
# else ()
#     message(FATAL_ERROR "Not find GTest")
# endif()


# *********************************** FetchContent ***********************************
# Cons: force install at INSTALL_DIRS, #TODO figureout how to change install_dir
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
#
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
