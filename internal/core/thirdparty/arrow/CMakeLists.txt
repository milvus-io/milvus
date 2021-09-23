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
if( NOT DEFINED ARROW_VERSION )
   set( ARROW_VERSION 5.0.0 )
endif()

if ( DEFINED ENV{MILVUS_ARROW_URL} )
    set( ARROW_SOURCE_URL "$ENV{MILVUS_ARROW_URL}" )
else ()
    set( ARROW_SOURCE_URL "https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz" )
endif ()

macro( build_arrow )
    message( STATUS "Building ARROW-${ARROW_VERSION} from source" )

    set( ARROW_CMAKE_ARGS
        "-DARROW_PARQUET=ON"
        "-DARROW_COMPUTE=ON"
        "-DARROW_BUILD_STATIC=OFF"
        "-DARROW_BUILD_SHARED=ON"
        "-DARROW_BUILD_TESTS=OFF"
        "-DARROW_TEST_MEMCHECK=OFF"
        "-DARROW_BUILD_BENCHMARKS=OFF"
        "-DARROW_CUDA=OFF"
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}"
        )

    ExternalProject_Add(
            arrow_ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/arrow-subbuild
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_CURRENT_BINARY_DIR}
            URL                 ${ARROW_SOURCE_URL}
            URL_MD5             "9caf5dbd36ef4972c3a591bcfeaf59c8"
            CMAKE_ARGS          ${ARROW_CMAKE_ARGS}
            SOURCE_SUBDIR       "cpp"
            BUILD_IN_SOURCE     1
            BUILD_COMMAND       ${MAKE}
            INSTALL_COMMAND     ${MAKE} install
    )

    ExternalProject_Get_Property( arrow_ep INSTALL_DIR )
    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()

    add_library( arrow SHARED IMPORTED )
    set_target_properties( arrow
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/lib/libarrow.so
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies( arrow arrow_ep )

    add_library( parquet SHARED IMPORTED )
    set_target_properties( parquet
            PROPERTIES
            IMPORTED_GLOBAL                 TRUE
            IMPORTED_LOCATION               ${INSTALL_DIR}/lib/libparquet.so
            INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies( parquet arrow-ep )
    target_link_libraries( parquet INTERFACE arrow )

endmacro()

build_arrow()

install( FILES ${INSTALL_DIR}/lib/libarrow.so
               ${INSTALL_DIR}/lib/libarrow.so.500
               ${INSTALL_DIR}/lib/libarrow.so.500.0.0
               ${INSTALL_DIR}/lib/libparquet.so
               ${INSTALL_DIR}/lib/libparquet.so.500
               ${INSTALL_DIR}/lib/libparquet.so.500.0.0
        DESTINATION lib )

install( DIRECTORY ${INSTALL_DIR}/include/arrow
                   ${INSTALL_DIR}/include/parquet
        DESTINATION include )

get_target_property( var arrow INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${var} )
set_directory_properties( PROPERTY INCLUDE_DIRECTORIES ${var} )
