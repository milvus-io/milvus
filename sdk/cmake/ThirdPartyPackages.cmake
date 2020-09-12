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

set(MILVUS_THIRDPARTY_DEPENDENCIES

        GRPC
        ZLIB
        )

# For each dependency, set dependency source to global default, if unset
foreach (DEPENDENCY ${MILVUS_THIRDPARTY_DEPENDENCIES})
    if ("${${DEPENDENCY}_SOURCE}" STREQUAL "")
        set(${DEPENDENCY}_SOURCE ${MILVUS_DEPENDENCY_SOURCE})
    endif ()
endforeach ()

macro(build_dependency DEPENDENCY_NAME)
    if("${DEPENDENCY_NAME}" STREQUAL "GRPC")
        build_grpc()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "ZLIB")
        build_zlib()
    endif()
endmacro()
#
#define_option(MILVUS_WITH_GRPC "Build with GRPC" ON)
#
#define_option(MILVUS_WITH_ZLIB "Build with zlib compression" ON)

# ----------------------------------------------------------------------
# Identify OS
if (UNIX)
    if (APPLE)
        set(CMAKE_OS_NAME "osx" CACHE STRING "Operating system name" FORCE)
    else (APPLE)
        ## Check for Debian GNU/Linux ________________
        find_file(DEBIAN_FOUND debian_version debconf.conf
                PATHS /etc
                )
        if (DEBIAN_FOUND)
            set(CMAKE_OS_NAME "debian" CACHE STRING "Operating system name" FORCE)
        endif (DEBIAN_FOUND)
        ##  Check for Fedora _________________________
        find_file(FEDORA_FOUND fedora-release
                PATHS /etc
                )
        if (FEDORA_FOUND)
            set(CMAKE_OS_NAME "fedora" CACHE STRING "Operating system name" FORCE)
        endif (FEDORA_FOUND)
        ##  Check for RedHat _________________________
        find_file(REDHAT_FOUND redhat-release inittab.RH
                PATHS /etc
                )
        if (REDHAT_FOUND)
            set(CMAKE_OS_NAME "redhat" CACHE STRING "Operating system name" FORCE)
        endif (REDHAT_FOUND)
        ## Extra check for Ubuntu ____________________
        if (DEBIAN_FOUND)
            ## At its core Ubuntu is a Debian system, with
            ## a slightly altered configuration; hence from
            ## a first superficial inspection a system will
            ## be considered as Debian, which signifies an
            ## extra check is required.
            find_file(UBUNTU_EXTRA legal issue
                    PATHS /etc
                    )
            if (UBUNTU_EXTRA)
                ## Scan contents of file
                file(STRINGS ${UBUNTU_EXTRA} UBUNTU_FOUND
                        REGEX Ubuntu
                        )
                ## Check result of string search
                if (UBUNTU_FOUND)
                    set(CMAKE_OS_NAME "ubuntu" CACHE STRING "Operating system name" FORCE)
                    set(DEBIAN_FOUND FALSE)

                    find_program(LSB_RELEASE_EXEC lsb_release)
                    execute_process(COMMAND ${LSB_RELEASE_EXEC} -rs
                            OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT
                            OUTPUT_STRIP_TRAILING_WHITESPACE
                            )
                    STRING(REGEX REPLACE "\\." "_" UBUNTU_VERSION "${LSB_RELEASE_ID_SHORT}")
                endif (UBUNTU_FOUND)
            endif (UBUNTU_EXTRA)
        endif (DEBIAN_FOUND)
    endif (APPLE)
endif (UNIX)

# ----------------------------------------------------------------------
# thirdparty directory
set(THIRDPARTY_DIR "${MILVUS_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
macro(resolve_dependency DEPENDENCY_NAME)
    if (${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
        find_package(${DEPENDENCY_NAME} MODULE)
        if (NOT ${${DEPENDENCY_NAME}_FOUND})
            build_dependency(${DEPENDENCY_NAME})
        endif ()
    elseif (${DEPENDENCY_NAME}_SOURCE STREQUAL "BUNDLED")
        build_dependency(${DEPENDENCY_NAME})
    elseif (${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
        find_package(${DEPENDENCY_NAME} REQUIRED)
    endif ()
endmacro()

# ----------------------------------------------------------------------
# ExternalProject options

#string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

# Set -fPIC on all external projects
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")

# CC/CXX environment variables are captured on the first invocation of the
# builder (e.g make or ninja) instead of when CMake is invoked into to build
# directory. This leads to issues if the variables are exported in a subshell
# and the invocation of make/ninja is in distinct subshell without the same
# environment (CC/CXX).
set(EP_COMMON_TOOLCHAIN -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

if (CMAKE_AR)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_AR=${CMAKE_AR})
endif ()

if (CMAKE_RANLIB)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_RANLIB=${CMAKE_RANLIB})
endif ()

if (NOT MILVUS_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

# External projects are still able to override the following declarations.
# cmake command line will favor the last defined variable when a duplicate is
# encountered. This requires that `EP_COMMON_CMAKE_ARGS` is always the first
# argument.
set(EP_COMMON_CMAKE_ARGS
        ${EP_COMMON_TOOLCHAIN}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_C_FLAGS=${EP_C_FLAGS}
        -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
        -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS})

if (NOT MILVUS_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    find_program(MAKE make)
endif ()

if (NOT DEFINED MAKE_BUILD_ARGS)
    set(MAKE_BUILD_ARGS "-j8")
endif ()
message(STATUS "Third Party MAKE_BUILD_ARGS = ${MAKE_BUILD_ARGS}")

# ----------------------------------------------------------------------
# Find pthreads

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------------------------------------------------------------------

if (DEFINED ENV{MILVUS_GRPC_URL})
    set(GRPC_SOURCE_URL "$ENV{MILVUS_GRPC_URL}")
else ()
    set(GRPC_SOURCE_URL
            "https://github.com/youny626/grpc-milvus/archive/master.zip")
endif ()

if (DEFINED ENV{MILVUS_ZLIB_URL})
    set(ZLIB_SOURCE_URL "$ENV{MILVUS_ZLIB_URL}")
else ()
    set(ZLIB_SOURCE_URL "https://github.com/madler/zlib/archive/v1.2.11.tar.gz")
endif ()

# ----------------------------------------------------------------------
# GRPC

macro(build_grpc)
    message(STATUS "Building GRPC-master from source")
    set(GRPC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-prefix/src/grpc_ep/install")
    set(GRPC_INCLUDE_DIR "${GRPC_PREFIX}/include")
    set(GRPC_STATIC_LIB "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GRPC++_STATIC_LIB "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GRPCPP_CHANNELZ_STATIC_LIB "${GRPC_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}grpcpp_channelz${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GRPC_PROTOBUF_LIB_DIR "${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-prefix/src/grpc_ep/libs/opt/protobuf")
    set(GRPC_PROTOBUF_STATIC_LIB "${GRPC_PROTOBUF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GRPC_PROTOC_STATIC_LIB "${GRPC_PROTOBUF_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}")

    externalproject_add(grpc_ep
            URL
            ${GRPC_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CONFIGURE_COMMAND
            ""
            BUILD_IN_SOURCE
            1
            BUILD_COMMAND
            ${MAKE} ${MAKE_BUILD_ARGS} prefix=${GRPC_PREFIX}
            INSTALL_COMMAND
            ${MAKE} install prefix=${GRPC_PREFIX}
            BUILD_BYPRODUCTS
            ${GRPC_STATIC_LIB}
            ${GRPC++_STATIC_LIB}
            ${GRPCPP_CHANNELZ_STATIC_LIB}
            ${GRPC_PROTOBUF_STATIC_LIB}
            ${GRPC_PROTOC_STATIC_LIB})

    ExternalProject_Add_StepDependencies(grpc_ep build zlib_ep)

    file(MAKE_DIRECTORY "${GRPC_INCLUDE_DIR}")

    add_library(grpc STATIC IMPORTED)
    set_target_properties(grpc
            PROPERTIES IMPORTED_LOCATION "${GRPC_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "zlib")

    add_library(grpc++ STATIC IMPORTED)
    set_target_properties(grpc++
            PROPERTIES IMPORTED_LOCATION "${GRPC++_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "zlib")

    add_library(grpcpp_channelz STATIC IMPORTED)
    set_target_properties(grpcpp_channelz
            PROPERTIES IMPORTED_LOCATION "${GRPCPP_CHANNELZ_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "zlib")

    add_library(grpc_protobuf STATIC IMPORTED)
    set_target_properties(grpc_protobuf
            PROPERTIES IMPORTED_LOCATION "${GRPC_PROTOBUF_STATIC_LIB}"
            INTERFACE_LINK_LIBRARIES "zlib")

    add_library(grpc_protoc STATIC IMPORTED)
    set_target_properties(grpc_protoc
            PROPERTIES IMPORTED_LOCATION "${GRPC_PROTOC_STATIC_LIB}"
            INTERFACE_LINK_LIBRARIES "zlib")

    add_dependencies(grpc grpc_ep)
    add_dependencies(grpc++ grpc_ep)
    add_dependencies(grpcpp_channelz grpc_ep)
    add_dependencies(grpc_protobuf grpc_ep)
    add_dependencies(grpc_protoc grpc_ep)
endmacro()

if (MILVUS_WITH_GRPC)
    resolve_dependency(GRPC)

    get_target_property(GRPC_INCLUDE_DIR grpc INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${GRPC_INCLUDE_DIR})
    link_directories(SYSTEM ${GRPC_PREFIX}/lib)

    set(GRPC_THIRD_PARTY_DIR ${CMAKE_CURRENT_BINARY_DIR}/grpc_ep-prefix/src/grpc_ep/third_party)
    include_directories(SYSTEM ${GRPC_THIRD_PARTY_DIR}/protobuf/src)
    link_directories(SYSTEM ${GRPC_PROTOBUF_LIB_DIR})
endif ()

# ----------------------------------------------------------------------
# zlib

macro(build_zlib)
    message(STATUS "Building ZLIB-v1.2.11 from source")
    set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep-prefix/src/zlib_ep")
    set(ZLIB_STATIC_LIB_NAME libz.a)
    set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
    set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
    set(ZLIB_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}"
            -DBUILD_SHARED_LIBS=OFF)

    externalproject_add(zlib_ep
            URL
            ${ZLIB_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            BUILD_COMMAND
            ${MAKE}
            ${MAKE_BUILD_ARGS}
            BUILD_BYPRODUCTS
            "${ZLIB_STATIC_LIB}"
            CMAKE_ARGS
            ${ZLIB_CMAKE_ARGS})

    file(MAKE_DIRECTORY "${ZLIB_INCLUDE_DIR}")
    add_library(zlib STATIC IMPORTED)
    set_target_properties(zlib
            PROPERTIES IMPORTED_LOCATION "${ZLIB_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ZLIB_INCLUDE_DIR}")

    add_dependencies(zlib zlib_ep)
endmacro()

if (MILVUS_WITH_ZLIB)
    resolve_dependency(ZLIB)

    get_target_property(ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
endif ()

# ----------------------------------------------------------------------
