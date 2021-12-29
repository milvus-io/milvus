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

# include(ExternalProject)

# if(NOT DEFINED KNOWHERE_VERSION)
#    set(KNOWHERE_VERSION 1.00)
# endif()

# if ( DEFINED ENV{KNOWHERE_GIT_URL} )
#     set(KNOWHERE_GIT_URL "$ENV{KNOWHERE_URL}" )
# else ()
#     set(KNOWHERE_GIT_URL "https://github.com/jaime0815/knowhere.git" )
# endif ()

# message( STATUS "Building knowhere-${KNOWHERE_VERSION} from source" )
# set( KNOWHERE_CMAKE_ARGS
#     "-DARROW_WITH_LZ4=OFF"
#     "-DCMAKE_VERBOSE_MAKEFILE=ON"
#     "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
#     "-DCMAKE_INCLUDE_PATH=${Boost_INCLUDE_DIRS}"
#     )

# ExternalProject_Add(knowhere-ep
#         PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/knowhere
#         CONFIGURE_COMMAND   ""
#         DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
#         INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
#         GIT_REPOSITORY      ${KNOWHERE_GIT_URL}
#         GIT_TAG             main
#  #       PATCH_COMMAND       mkdir -p cmake_build && cd cmake_build
#  #       BUILD_COMMAND       ${CMAKE} -DKNOWHERE_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR} -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CUDA_COMPILER=/usr/local/cuda/bin/nvcc -DMILVUS_ENABLE_PROFILING=OFF ..
#         PATCH_COMMAND  bash build.sh -p ${CMAKE_CURRENT_BINARY_DIR}
#         )

# ExternalProject_Get_Property(knowhere-ep INSTALL_DIR )
# if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
#     file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
# endif()

# message(STATUS "------------------------- ${CMAKE_CURRENT_BINARY_DIR}/lib --------------------6----------6-6")
# link_directories(${CMAKE_CURRENT_BINARY_DIR}/lib)
# include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)


#  add_subdirectory(${CMAKE_BINARY_DIR}/3rdparty_download/knowhere/src/knowhere-ep ${CMAKE_CURRENT_BINARY_DIR})
# message(STATUS "-----------=================${CMAKE_CURRENT_BINARY_DIR}/include=============-----------------sss----------- ${INSTALL_DIR}")


include(FetchContent)

if(NOT DEFINED KNOWHERE_VERSION)
   set(KNOWHERE_VERSION 1.00)
endif()

if ( DEFINED ENV{KNOWHERE_GIT_URL} )
    set(KNOWHERE_GIT_URL "$ENV{KNOWHERE_URL}" )
else ()
    set(KNOWHERE_GIT_URL "https://github.com/jaime0815/knowhere.git" )
endif ()

message( STATUS "Building knowhere-${KNOWHERE_VERSION} from source" )
set( KNOWHERE_CMAKE_ARGS
    "-DARROW_WITH_LZ4=OFF"
    "-DCMAKE_VERBOSE_MAKEFILE=ON"
    "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
    "-DCMAKE_INCLUDE_PATH=${Boost_INCLUDE_DIRS}"
    )

FetchContent_Declare(
    knowhere
    GIT_REPOSITORY         ${KNOWHERE_GIT_URL}
    GIT_TAG                 main
    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
    )

set(CMAKE_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}")

FetchContent_GetProperties(knowhere)
if ( NOT knowhere_POPULATED )

    FetchContent_Populate( knowhere )
    add_subdirectory( ${knowhere_SOURCE_DIR}
                        ${knowhere_BINARY_DIR}
                        EXCLUDE_FROM_ALL )
endif()

get_target_property( KNOWHERE_INCLUDE_DIR knowhere INCLUDE_DIRECTORIES )
#set(KNOWHERE_INCLUDE_DIR ${KNOWHERE_INCLUDE_DIR} PARENT_SCOPE)

get_target_property(INCLUDE_DIR knowhere INTERFACE_INCLUDE_DIRECTORIES )
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "== Knowhere compile options: ${var}" )

