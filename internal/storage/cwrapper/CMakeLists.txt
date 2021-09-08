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

include( ExternalProject )
set( ARROW_VERSION "2.0.0" )
set( ARROW_SOURCE_URL
    "https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz")

if( CUSTOM_THIRDPARTY_DOWNLOAD_PATH )
    set( THIRDPARTY_DOWNLOAD_PATH ${CUSTOM_THIRDPARTY_DOWNLOAD_PATH} )
else()
    set( THIRDPARTY_DOWNLOAD_PATH ${CMAKE_BINARY_DIR}/3rdparty_download/download )
endif()
message( STATUS "Thirdparty downloaded file path: ${THIRDPARTY_DOWNLOAD_PATH}" )

# These three components is required by arrow
find_package(Boost REQUIRED COMPONENTS regex system filesystem)
message( STATUS "Find Boost: include dirs-${Boost_INCLUDE_DIRS}, version-${Boost_VERSION}")

macro( build_arrow )
    message( STATUS "Building ARROW-${ARROW_VERSION} from source" )

    set( ARROW_CMAKE_ARGS
        "-DARROW_WITH_LZ4=OFF"
        "-DARROW_WITH_ZSTD=OFF"
        "-DARROW_WITH_BROTLI=OFF"
        "-DARROW_WITH_SNAPPY=OFF"
        "-DARROW_WITH_ZLIB=OFF"
        "-DARROW_BUILD_STATIC=ON"
        "-DARROW_BUILD_SHARED=OFF"
        "-DARROW_BOOST_USE_SHARED=OFF"
        "-DARROW_BUILD_TESTS=OFF"
        "-DARROW_TEST_MEMCHECK=OFF"
        "-DARROW_BUILD_BENCHMARKS=OFF"
        "-DARROW_CUDA=OFF"
        "-DARROW_JEMALLOC=OFF"
        "-DARROW_PYTHON=OFF"
        "-DARROW_BUILD_UTILITIES=OFF"
        "-DARROW_PARQUET=ON"
        "-DPARQUET_BUILD_SHARED=OFF"
        "-DThrift_SOURCE=BUNDLED"
        "-Dutf8proc_SOURCE=BUNDLED"
        "-DARROW_S3=OFF"
        "-DCMAKE_VERBOSE_MAKEFILE=ON"
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}"
        "-DCMAKE_INCLUDE_PATH=${Boost_INCLUDE_DIRS}"
        )

    ExternalProject_Add(
            arrow-ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/arrow-subbuild
            BINARY_DIR          arrow-bin
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
            SOURCE_SUBDIR       "cpp"
            URL                 ${ARROW_SOURCE_URL}
            URL_MD5             "37fbe42e7f155dd76cf6f63257373b24"
            CMAKE_ARGS          ${ARROW_CMAKE_ARGS}
            ${EP_LOG_OPTIONS}
            )

    ExternalProject_Get_Property( arrow-ep INSTALL_DIR )
    ExternalProject_Get_Property( arrow-ep BINARY_DIR )
    set( THRIFT_LOCATION ${BINARY_DIR}/thrift_ep-install )
    set( UTF8PROC_LOCATION ${BINARY_DIR}/utf8proc_ep-install )

    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()
    if( NOT IS_DIRECTORY ${THRIFT_LOCATION}/include )
        file( MAKE_DIRECTORY "${THRIFT_LOCATION}/include" )
    endif()
    if( NOT IS_DIRECTORY ${UTF8PROC_LOCATION}/include )
        file( MAKE_DIRECTORY "${UTF8PROC_LOCATION}/include" )
    endif()

    include(GNUInstallDirs)

    add_library( thrift STATIC IMPORTED )
    set_target_properties( thrift
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${THRIFT_LOCATION}/lib/libthrift.a
                INTERFACE_INCLUDE_DIRECTORIES   ${THRIFT_LOCATION}/include )
    add_dependencies(thrift arrow-ep)

    add_library( utf8proc STATIC IMPORTED )
    set_target_properties( utf8proc
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${UTF8PROC_LOCATION}/lib/libutf8proc.a
                INTERFACE_INCLUDE_DIRECTORIES   ${UTF8PROC_LOCATION}/include )
    add_dependencies(utf8proc arrow-ep)

    add_library( arrow STATIC IMPORTED )
    set_target_properties( arrow
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libarrow.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies(arrow arrow-ep )

    add_library( parquet STATIC IMPORTED )
    set_target_properties( parquet
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libparquet.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies(parquet arrow-ep)
    target_link_libraries(parquet INTERFACE arrow thrift utf8proc)
endmacro()

build_arrow()

add_library(wrapper STATIC)
target_sources(wrapper PUBLIC ParquetWrapper.cpp PayloadStream.cpp
    
    )
set_target_properties( wrapper PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR} )
target_link_libraries(wrapper PUBLIC parquet pthread)

if(NOT CMAKE_INSTALL_PREFIX)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR})
endif()

get_target_property( THRIFT_LIB thrift LOCATION )
get_target_property( ARROW_LIB  arrow LOCATION )
get_target_property( PARQUET_LIB  parquet LOCATION )
get_target_property( UTF8PROC_LIB  utf8proc LOCATION )
install(TARGETS wrapper DESTINATION ${CMAKE_INSTALL_PREFIX})
install(
    FILES ${ARROW_LIB} ${PARQUET_LIB} ${THRIFT_LIB} ${UTF8PROC_LIB} DESTINATION ${CMAKE_INSTALL_PREFIX})

if (BUILD_TESTING)
    add_subdirectory(test)
endif()
