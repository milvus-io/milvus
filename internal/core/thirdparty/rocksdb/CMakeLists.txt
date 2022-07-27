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

set( ROCKSDB_VERSION "6.27.3" )
set( ROCKSDB_SOURCE_URL
        "https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz")

macro( build_rocksdb )
    message( STATUS "Building ROCKSDB-${ROCKSDB_VERSION} from source" )

    set ( ROCKSDB_MD5 "e4a0625f0cec82060e62c81b787a1124" )

    if ( EMBEDDED_MILVUS )
        message ( STATUS "Turning on fPIC while building embedded Milvus" )
        set( FPIC_ARG "-DCMAKE_POSITION_INDEPENDENT_CODE=ON" )
    endif()
    set( ROCKSDB_CMAKE_ARGS
         "-DWITH_GFLAGS=OFF"
         "-DROCKSDB_BUILD_SHARED=OFF"
         "-DWITH_TESTS=OFF"
         "-DWITH_BENCHMARK_TOOLS=OFF"
         "-DWITH_CORE_TOOLS=OFF"
         "-DWITH_TOOLS=OFF"
         "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
         "-DROCKSDB_INSTALL_ON_WINDOWS=ON"
         "-DFAIL_ON_WARNINGS=OFF"
         "-DWITH_ZSTD=ON"
         ${FPIC_ARG}
         #This is used to solve 'illegal instruction' problem in some machine
         "-DPORTABLE=ON"
        )

    ExternalProject_Add(
            rocksdb-ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/rocksdb-subbuild
            BINARY_DIR          rocksdb-bin
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_INSTALL_PREFIX}
            URL                 ${ROCKSDB_SOURCE_URL}
            URL_MD5             ${ROCKSDB_MD5}
            CMAKE_ARGS          ${ROCKSDB_CMAKE_ARGS}
            )

    ExternalProject_Get_Property( rocksdb-ep INSTALL_DIR )
    ExternalProject_Get_Property( rocksdb-ep BINARY_DIR )

    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()

    find_path(ZSTD_INCLUDE_DIR
      NAMES zstd.h
      HINTS ${zstd_ROOT_DIR}/include)

    find_library(ZSTD_LIBRARY
      NAMES zstd
      HINTS ${zstd_ROOT_DIR}/lib)
    message ( STATUS "FIND ZSTD LIBRARY ${ZSTD_LIBRARY}" )
    message ( STATUS "FIND ZSTD INCLUDE ${ZSTD_INCLUDE_DIR}" )

    milvus_add_pkg_config("rocksdb")
    add_library( rocksdb STATIC IMPORTED )
    set_target_properties( rocksdb
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/librocksdb.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )

endmacro()

if (MSYS)
    message(STATUS "Using rocksdb form system")
    configure_file(rocksdb-win.pc.in "${CMAKE_CURRENT_BINARY_DIR}/rocksdb.pc" @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/rocksdb.pc" DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
else (MSYS)
    build_rocksdb()
endif (MSYS)
