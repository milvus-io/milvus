# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(GTEST_VERSION "1.8.0")

message(MEGASEARCH_BUILD_TESTS ${MEGASEARCH_BUILD_TESTS})

if(MEGASEARCH_BUILD_TESTS)
    add_custom_target(unittest ctest -L unittest)

    if("$ENV{GTEST_HOME}" STREQUAL "")
        message("Yes")
        set(GTEST_CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

        set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest/src/googletest")
        set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
        set(GTEST_STATIC_LIB
                "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(GTEST_MAIN_STATIC_LIB
                "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")

        set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

        ExternalProject_Add(googletest
                URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
                BUILD_BYPRODUCTS "${GTEST_STATIC_LIB}" "${GTEST_MAIN_STATIC_LIB}"
                CMAKE_ARGS ${GTEST_CMAKE_ARGS}
                ${EP_LOG_OPTIONS})
        set(GTEST_VENDORED 1)
    else()
        find_package(GTest REQUIRED)
        set(GTEST_VENDORED 0)
    endif()

    message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
    message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})

    add_library(gtest STATIC IMPORTED)
    set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${GTEST_STATIC_LIB})

    add_library(gtest_main STATIC IMPORTED)
    set_target_properties(gtest_main PROPERTIES IMPORTED_LOCATION
            ${GTEST_MAIN_STATIC_LIB})

    if(GTEST_VENDORED)
        add_dependencies(gtest googletest)
        add_dependencies(gtest_main googletest)
    endif()
endif()