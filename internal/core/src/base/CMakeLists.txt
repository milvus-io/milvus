# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

cmake_minimum_required( VERSION 3.18 )
project(milvus_common CXX C)

set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set( CMAKE_EXPORT_COMPILE_COMMANDS ON )

set(MILVUS_COMMON_WORKSPACE ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${MILVUS_COMMON_WORKSPACE}/include)

list( APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/conan )
include( ${CMAKE_BINARY_DIR}/conan/conanbuildinfo.cmake )
set( CONAN_DISABLE_CHECK_COMPILER ON )
conan_basic_setup( KEEP_RPATHS )

file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.cc)
add_library(milvus_base SHARED ${SRC_FILES})

function(add_pkg_config module)
  configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/${module}.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/${module}.pc
    @ONLY
  )
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/${module}.pc"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/"
  )
endfunction()

add_pkg_config(milvus_base)

list(APPEND MILVUS_BASE_LINKER_LIBS ${CONAN_LIBS})

target_link_libraries(milvus_base PRIVATE ${MILVUS_BASE_LINKER_LIBS})

install(TARGETS milvus_base
        DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR})

install(DIRECTORY "${MILVUS_COMMON_WORKSPACE}/include/"
        DESTINATION "${CMAKE_INSTALL_PREFIX}/include")