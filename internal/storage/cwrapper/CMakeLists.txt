# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required( VERSION 3.18 )
project(wrapper)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include( GNUInstallDirs )
include( ExternalProject )
include( CheckCXXCompilerFlag )

set(ARROW_OUTPUT_PREFIX ${CMAKE_INSTALL_PREFIX}/../../../core/output)
add_library(wrapper STATIC)
target_sources(wrapper PUBLIC ParquetWrapper.cpp PayloadStream.cpp)

if ( NOT MSYS )
    add_library(parquet STATIC IMPORTED)
    set_target_properties(parquet PROPERTIES
            IMPORTED_LOCATION "${ARROW_OUTPUT_PREFIX}/lib/libparquet.a"
            INTERFACE_INCLUDE_DIRECTORIES "${ARROW_OUTPUT_PREFIX}/include")
endif()

if ( EMBEDDED_MILVUS )
    message ( STATUS "Turning on fPIC while building embedded Milvus" )
    set_target_properties( wrapper PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR} POSITION_INDEPENDENT_CODE ON )
else()
    set_target_properties( wrapper PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR} )
endif()

target_link_libraries( wrapper PUBLIC parquet pthread)

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()

install( TARGETS wrapper )

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
