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

set( ARROW_VERSION "8.0.0" )
set( ARROW_SOURCE_URL
    "https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz")
set( ARROW_MD5 "b1d285d2b90feceb61155baf9a21f81c" )

# These three components is required by arrow
find_package(Boost REQUIRED COMPONENTS regex system filesystem)
message( STATUS "Find Boost: include dirs-${Boost_INCLUDE_DIRS}, version-${Boost_VERSION}")

macro( build_arrow )
    message( STATUS "Building ARROW-${ARROW_VERSION} from source" )

    set( ARROW_CMAKE_ARGS
        "-DARROW_WITH_LZ4=OFF"
        "-DARROW_WITH_ZSTD=ON"
        "-Dzstd_SOURCE=BUNDLED"
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
        "-DARROW_PYTHON=OFF"
        "-DARROW_WITH_RE2=OFF"
        "-DARROW_BUILD_UTILITIES=OFF"
        "-DARROW_PARQUET=ON"
        "-DPARQUET_BUILD_SHARED=OFF"
        "-DThrift_SOURCE=BUNDLED"
        "-Dutf8proc_SOURCE=BUNDLED"
        "-DARROW_S3=OFF"
        "-DCMAKE_VERBOSE_MAKEFILE=ON"
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}"
        "-DCMAKE_INCLUDE_PATH=${Boost_INCLUDE_DIRS}"
        )
    if ( WIN32 )
        set( ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS} "-DARROW_JEMALLOC=OFF" )
    else ()
        set( ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS} "-DARROW_JEMALLOC=ON" )
    endif ()

    ExternalProject_Add(
            arrow-ep
            PREFIX              ${CMAKE_BINARY_DIR}/3rdparty_download/arrow-subbuild
            BINARY_DIR          arrow-bin
            DOWNLOAD_DIR        ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR         ${CMAKE_INSTALL_PREFIX}
            SOURCE_SUBDIR       "cpp"
            URL                 ${ARROW_SOURCE_URL}
            URL_MD5             ${ARROW_MD5}
            CMAKE_ARGS          ${ARROW_CMAKE_ARGS}
            ${EP_LOG_OPTIONS}
            )

    ExternalProject_Get_Property( arrow-ep INSTALL_DIR )
    if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
        file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
    endif()

    add_library( arrow_bundled STATIC IMPORTED )
    set_target_properties( arrow_bundled
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libarrow_bundled_dependencies.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies(arrow_bundled arrow-ep )

    add_library( arrow STATIC IMPORTED )
    set_target_properties( arrow
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libarrow.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies(arrow arrow_bundled)
    target_link_libraries(arrow INTERFACE arrow_bundled)


    add_library( parquet STATIC IMPORTED )
    set_target_properties( parquet
            PROPERTIES
                IMPORTED_GLOBAL                 TRUE
                IMPORTED_LOCATION               ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libparquet.a
                INTERFACE_INCLUDE_DIRECTORIES   ${INSTALL_DIR}/include )
    add_dependencies( parquet arrow-ep )
    target_link_libraries( parquet INTERFACE arrow )
endmacro()

if (MSYS)
    message(STATUS "Using arrow form system")
else (MSYS)
    build_arrow()
endif (MSYS)
