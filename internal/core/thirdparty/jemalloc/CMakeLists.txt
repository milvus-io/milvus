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

if (DEFINED ENV{MILVUS_JEMALLOC_URL})
    set(JEMALLOC_SOURCE_URL "$ENV{MILVUS_JEMALLOC_URL}")
else ()
    set(JEMALLOC_SOURCE_URL
            "https://github.com/jemalloc/jemalloc/releases/download/${MILVUS_JEMALLOC_BUILD_VERSION}/jemalloc-${MILVUS_JEMALLOC_BUILD_VERSION}.tar.bz2")
endif ()

# ----------------------------------------------------------------------
# jemalloc - Unix-only high-performance allocator
message(STATUS "Building (vendored) jemalloc from source")
# We only use a vendored jemalloc as we want to control its version.
# Also our build of jemalloc is specially prefixed so that it will not
# conflict with the default allocator as well as other jemalloc
# installations.
# find_package(jemalloc)

include(CheckSymbolExists)

macro(detect_aarch64_target_arch)
  check_symbol_exists(__aarch64__ "" __AARCH64)
endmacro()
detect_aarch64_target_arch()

set(JEMALLOC_PREFIX "${CMAKE_INSTALL_PREFIX}")
set(JEMALLOC_LIB_DIR "${JEMALLOC_PREFIX}/lib")
set(JEMALLOC_STATIC_LIB "${JEMALLOC_LIB_DIR}/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(JEMALLOC_CONFIGURE_COMMAND ./configure "AR=${CMAKE_AR}" "CC=${CMAKE_C_COMPILER}")

message("CMAKE_OSX_SYSROOT: ${CMAKE_OSX_SYSROOT}")
if (CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
endif ()

if (DEFINED __AARCH64)
    #aarch64 platform use 64k pagesize.
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "--with-lg-page=16")
endif ()

list(APPEND
        JEMALLOC_CONFIGURE_COMMAND
        "--prefix=${JEMALLOC_PREFIX}"
        "--libdir=${JEMALLOC_LIB_DIR}"
        "--enable-prof")
if (CMAKE_BUILD_TYPE EQUAL "DEBUG")
    # Enable jemalloc debug checks when Milvus itself has debugging enabled
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "--enable-debug")
endif ()
set(JEMALLOC_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
if (CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_BUILD_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
endif ()

ExternalProject_Add(
        jemalloc_ep
        PREFIX ${CMAKE_BINARY_DIR}/3rdparty_download/jemalloc-subbuild
        URL ${JEMALLOC_SOURCE_URL}
        PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
        CONFIGURE_COMMAND ${JEMALLOC_CONFIGURE_COMMAND}
        BUILD_IN_SOURCE 1
        BUILD_COMMAND ${JEMALLOC_BUILD_COMMAND}
        BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
        INSTALL_COMMAND ${MAKE} install)

add_library(jemalloc SHARED IMPORTED)
set_target_properties(jemalloc
        PROPERTIES INTERFACE_LINK_LIBRARIES Threads::Threads
        IMPORTED_LOCATION "${JEMALLOC_PREFIX}/${CMAKE_INSTALL_LIBDIR}/libjemalloc.so"
        INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_PREFIX}/jemalloc/include")
add_dependencies(jemalloc jemalloc_ep)

get_target_property(JEMALLOC_IMPORTED_LOCATION jemalloc IMPORTED_LOCATION)
get_target_property(JEMALLOC_INTERFACE_INCLUDE_DIRECTORIES jemalloc INTERFACE_INCLUDE_DIRECTORIES)
message("JEMALLOC_IMPORTED_LOCATION: ${JEMALLOC_IMPORTED_LOCATION}")
message("JEMALLOC_INTERFACE_INCLUDE_DIRECTORIES: ${JEMALLOC_INTERFACE_INCLUDE_DIRECTORIES}")
