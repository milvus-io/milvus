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

cmake_minimum_required(VERSION 3.14...3.17 FATAL_ERROR)
project(wrapper)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

macro( import_arrow )
    message( STATUS "Importing ARROW from ${CORE_OUTPUT_LIB}" )

    add_library( arrow SHARED IMPORTED )
    set_target_properties( arrow
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${CORE_OUTPUT_LIB}/lib/libarrow.so
                INTERFACE_INCLUDE_DIRECTORIES   ${CORE_OUTPUT_LIB}/include )
    target_link_libraries(arrow INTERFACE arrow_bundled_dependencies)

    add_library( parquet SHARED IMPORTED )
    set_target_properties( parquet
            PROPERTIES
            IMPORTED_GLOBAL                 TRUE
            IMPORTED_LOCATION               ${CORE_OUTPUT_LIB}/lib/libparquet.so
            INTERFACE_INCLUDE_DIRECTORIES   ${CORE_OUTPUT_LIB}/include )
    target_link_libraries(parquet INTERFACE arrow)

endmacro()

import_arrow()

add_library( wrapper STATIC )
target_sources( wrapper PUBLIC ParquetWrapper.cpp PayloadStream.cpp )
set_target_properties( wrapper
            PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES ${CORE_OUTPUT_LIB}/include 
            INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR} )
target_link_libraries( wrapper PUBLIC parquet pthread )

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()

install(TARGETS wrapper DESTINATION ${CMAKE_INSTALL_PREFIX})

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
