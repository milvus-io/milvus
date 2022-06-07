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

set(KNOWHERE_THIRDPARTY_DEPENDENCIES
        Arrow
        FAISS
        GTest
        OpenBLAS
        MKL
        )

message(STATUS "Using ${KNOWHERE_DEPENDENCY_SOURCE} approach to find dependencies")

# For each dependency, set dependency source to global default, if unset
foreach (DEPENDENCY ${KNOWHERE_THIRDPARTY_DEPENDENCIES})
    if ("${${DEPENDENCY}_SOURCE}" STREQUAL "")
        set(${DEPENDENCY}_SOURCE ${KNOWHERE_DEPENDENCY_SOURCE})
    endif ()
endforeach ()

macro(build_dependency DEPENDENCY_NAME)
    if ("${DEPENDENCY_NAME}" STREQUAL "Arrow")
        build_arrow()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "GTest")
        build_gtest()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "OpenBLAS")
        build_openblas()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "FAISS")
        build_faiss()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "MKL")
        build_mkl()
    else ()
        message(FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
    endif ()
endmacro()

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
                endif (UBUNTU_FOUND)
            endif (UBUNTU_EXTRA)
        endif (DEBIAN_FOUND)
    endif (APPLE)
endif (UNIX)


# ----------------------------------------------------------------------
# thirdparty directory
set(THIRDPARTY_DIR "${INDEX_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
# ExternalProject options

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (NOT MSVC)
    # Set -fPIC on all external projects
    set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
    set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")
endif ()

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

if (NOT KNOWHERE_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    if (NOT MSVC)
        find_program(MAKE make)
    endif ()
endif ()

set(MAKE_BUILD_ARGS "-j8")


# ----------------------------------------------------------------------
# Find pthreads

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds

# Read toolchain versions from cpp/thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach (_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
    # Exclude comments
    if (NOT _VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
        continue()
    endif ()

    string(REGEX MATCH "^[^=]*" _LIB_NAME ${_VERSION_ENTRY})
    string(REPLACE "${_LIB_NAME}=" "" _LIB_VERSION ${_VERSION_ENTRY})

    # Skip blank or malformed lines
    if (${_LIB_VERSION} STREQUAL "")
        continue()
    endif ()

    # For debugging
    #message(STATUS "${_LIB_NAME}: ${_LIB_VERSION}")

    set(${_LIB_NAME} "${_LIB_VERSION}")
endforeach ()

set(FAISS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/faiss)

if (DEFINED ENV{KNOWHERE_ARROW_URL})
    set(ARROW_SOURCE_URL "$ENV{KNOWHERE_ARROW_URL}")
else ()
    set(ARROW_SOURCE_URL
            "https://github.com/apache/arrow.git"
            )
endif ()

if (DEFINED ENV{KNOWHERE_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{KNOWHERE_GTEST_URL}")
else ()
    set(GTEST_SOURCE_URL
            "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif ()

if (DEFINED ENV{KNOWHERE_OPENBLAS_URL})
    set(OPENBLAS_SOURCE_URL "$ENV{KNOWHERE_OPENBLAS_URL}")
else ()
    set(OPENBLAS_SOURCE_URL
            "https://github.com/xianyi/OpenBLAS/archive/v${OPENBLAS_VERSION}.tar.gz")
endif ()

# ----------------------------------------------------------------------
# ARROW
set(ARROW_PREFIX "${INDEX_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")

macro(build_arrow)
    message(STATUS "Building Apache ARROW-${ARROW_VERSION} from source")
    set(ARROW_STATIC_LIB_NAME arrow)
    set(ARROW_LIB_DIR "${ARROW_PREFIX}/lib")
    set(ARROW_STATIC_LIB
            "${ARROW_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DARROW_BUILD_STATIC=ON
            -DARROW_BUILD_SHARED=OFF
            -DARROW_USE_GLOG=OFF
            -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
            -DCMAKE_INSTALL_LIBDIR=${ARROW_LIB_DIR}
            -DARROW_CUDA=OFF
            -DARROW_FLIGHT=OFF
            -DARROW_GANDIVA=OFF
            -DARROW_GANDIVA_JAVA=OFF
            -DARROW_HDFS=OFF
            -DARROW_HIVESERVER2=OFF
            -DARROW_ORC=OFF
            -DARROW_PARQUET=OFF
            -DARROW_PLASMA=OFF
            -DARROW_PLASMA_JAVA_CLIENT=OFF
            -DARROW_PYTHON=OFF
            -DARROW_WITH_BZ2=OFF
            -DARROW_WITH_ZLIB=OFF
            -DARROW_WITH_LZ4=OFF
            -DARROW_WITH_SNAPPY=OFF
            -DARROW_WITH_ZSTD=OFF
            -DARROW_WITH_BROTLI=OFF
            -DCMAKE_BUILD_TYPE=Release
            -DARROW_DEPENDENCY_SOURCE=BUNDLED #Build all arrow dependencies from source instead of calling find_package first
            -DBOOST_SOURCE=AUTO #try to find BOOST in the system default locations and build from source if not found
            )

    externalproject_add(arrow_ep
            GIT_REPOSITORY
            ${ARROW_SOURCE_URL}
            GIT_TAG
            ${ARROW_VERSION}
            GIT_SHALLOW
            TRUE
            SOURCE_SUBDIR
            cpp
            ${EP_LOG_OPTIONS}
            CMAKE_ARGS
            ${ARROW_CMAKE_ARGS}
            BUILD_COMMAND
            ""
            INSTALL_COMMAND
            ${MAKE} ${MAKE_BUILD_ARGS} install
            BUILD_BYPRODUCTS
            "${ARROW_STATIC_LIB}"
            )

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    add_library(arrow STATIC IMPORTED)
    set_target_properties(arrow
            PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}")
    add_dependencies(arrow arrow_ep)

    set(JEMALLOC_PREFIX "${INDEX_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep-build/jemalloc_ep-prefix/src/jemalloc_ep")

    add_custom_command(TARGET arrow_ep POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory ${ARROW_LIB_DIR}
            COMMAND ${CMAKE_COMMAND} -E copy ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a ${ARROW_LIB_DIR}
            DEPENDS ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a)

endmacro()

if (KNOWHERE_WITH_ARROW AND NOT TARGET arrow_ep)

    resolve_dependency(Arrow)

    link_directories(SYSTEM ${ARROW_LIB_DIR})
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
endif ()

# ----------------------------------------------------------------------
# OpenBLAS
set(OPENBLAS_PREFIX "${INDEX_BINARY_DIR}/openblas_ep-prefix/src/openblas_ep")
macro(build_openblas)
    message(STATUS "Building OpenBLAS-${OPENBLAS_VERSION} from source")
    set(OpenBLAS_INCLUDE_DIR "${OPENBLAS_PREFIX}/include")
    set(OpenBLAS_LIB_DIR "${OPENBLAS_PREFIX}/lib")
    set(OPENBLAS_SHARED_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(OPENBLAS_STATIC_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}openblas${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(OPENBLAS_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_BUILD_TYPE=Release
            -DBUILD_SHARED_LIBS=ON
            -DBUILD_STATIC_LIBS=ON
            -DTARGET=CORE2
            -DDYNAMIC_ARCH=1
            -DDYNAMIC_OLDER=1
            -DUSE_THREAD=0
            -DUSE_OPENMP=0
            -DFC=gfortran
            -DCC=gcc
            -DINTERFACE64=0
            -DNUM_THREADS=128
            -DNO_LAPACKE=1
            "-DVERSION=${OPENBLAS_VERSION}"
            "-DCMAKE_INSTALL_PREFIX=${OPENBLAS_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib)

    externalproject_add(openblas_ep
            URL
            ${OPENBLAS_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CMAKE_ARGS
            ${OPENBLAS_CMAKE_ARGS}
            BUILD_COMMAND
            ${MAKE}
            ${MAKE_BUILD_ARGS}
            BUILD_IN_SOURCE
            1
            INSTALL_COMMAND
            ${MAKE}
            PREFIX=${OPENBLAS_PREFIX}
            install
            BUILD_BYPRODUCTS
            ${OPENBLAS_SHARED_LIB}
            ${OPENBLAS_STATIC_LIB})

    file(MAKE_DIRECTORY "${OpenBLAS_INCLUDE_DIR}")
    add_library(openblas SHARED IMPORTED)
    set_target_properties(
            openblas
            PROPERTIES
            IMPORTED_LOCATION "${OPENBLAS_SHARED_LIB}"
            LIBRARY_OUTPUT_NAME "openblas"
            INTERFACE_INCLUDE_DIRECTORIES "${OpenBLAS_INCLUDE_DIR}")
    add_dependencies(openblas openblas_ep)
    get_target_property(OpenBLAS_INCLUDE_DIR openblas INTERFACE_INCLUDE_DIRECTORIES)
    set(OpenBLAS_LIBRARIES "${OPENBLAS_SHARED_LIB}")
endmacro()

if (KNOWHERE_WITH_OPENBLAS)
    resolve_dependency(OpenBLAS)
    include_directories(SYSTEM "${OpenBLAS_INCLUDE_DIR}")
    link_directories(SYSTEM "${OpenBLAS_LIB_DIR}")
endif()

# ----------------------------------------------------------------------
# Google gtest

macro(build_gtest)
    message(STATUS "Building gtest-${GTEST_VERSION} from source")
    set(GTEST_VENDORED TRUE)
    set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")

    if (APPLE)
        set(GTEST_CMAKE_CXX_FLAGS
                ${GTEST_CMAKE_CXX_FLAGS}
                -DGTEST_USE_OWN_TR1_TUPLE=1
                -Wno-unused-value
                -Wno-ignored-attributes)
    endif ()

    set(GTEST_PREFIX "${INDEX_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GTEST_STATIC_LIB
            "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_MAIN_STATIC_LIB
            "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(GTEST_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}"
            "-DCMAKE_INSTALL_LIBDIR=lib"
            -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS}
            -DCMAKE_BUILD_TYPE=Release)

    set(GMOCK_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GMOCK_STATIC_LIB
            "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )

        ExternalProject_Add(googletest_ep
                URL
                ${GTEST_SOURCE_URL}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_BYPRODUCTS
                ${GTEST_STATIC_LIB}
                ${GTEST_MAIN_STATIC_LIB}
                ${GMOCK_STATIC_LIB}
                CMAKE_ARGS
                ${GTEST_CMAKE_ARGS}
                ${EP_LOG_OPTIONS})

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${GTEST_INCLUDE_DIR}")

    add_library(gtest STATIC IMPORTED)
    set_target_properties(gtest
            PROPERTIES IMPORTED_LOCATION "${GTEST_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

    add_library(gtest_main STATIC IMPORTED)
    set_target_properties(gtest_main
            PROPERTIES IMPORTED_LOCATION "${GTEST_MAIN_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

    add_library(gmock STATIC IMPORTED)
    set_target_properties(gmock
            PROPERTIES IMPORTED_LOCATION "${GMOCK_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GTEST_INCLUDE_DIR}")

    add_dependencies(gtest googletest_ep)
    add_dependencies(gtest_main googletest_ep)
    add_dependencies(gmock googletest_ep)

endmacro()

if (KNOWHERE_BUILD_TESTS AND NOT TARGET googletest_ep)
    resolve_dependency(GTest)

    if (NOT GTEST_VENDORED)
    endif ()

    # TODO: Don't use global includes but rather target_include_directories
    get_target_property(GTEST_INCLUDE_DIR gtest INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM "${GTEST_PREFIX}/lib")
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
endif ()

# ----------------------------------------------------------------------
# MKL

macro(build_mkl)

    if (FAISS_WITH_MKL)
        if (EXISTS "/proc/cpuinfo")
            FILE(READ /proc/cpuinfo PROC_CPUINFO)

            SET(VENDOR_ID_RX "vendor_id[ \t]*:[ \t]*([a-zA-Z]+)\n")
            STRING(REGEX MATCH "${VENDOR_ID_RX}" VENDOR_ID "${PROC_CPUINFO}")
            STRING(REGEX REPLACE "${VENDOR_ID_RX}" "\\1" VENDOR_ID "${VENDOR_ID}")

            if (NOT ${VENDOR_ID} STREQUAL "GenuineIntel")
                set(FAISS_WITH_MKL OFF)
            endif ()
        endif ()

        find_path(MKL_LIB_PATH
                NAMES "libmkl_intel_ilp64.a" "libmkl_gnu_thread.a" "libmkl_core.a"
                PATH_SUFFIXES "intel/compilers_and_libraries_${MKL_VERSION}/linux/mkl/lib/intel64/")
        if (${MKL_LIB_PATH} STREQUAL "MKL_LIB_PATH-NOTFOUND")
            message(FATAL_ERROR "Could not find MKL libraries")
        endif ()
        message(STATUS "MKL lib path = ${MKL_LIB_PATH}")

        set(MKL_LIBS
                ${MKL_LIB_PATH}/libmkl_intel_ilp64.a
                ${MKL_LIB_PATH}/libmkl_gnu_thread.a
                ${MKL_LIB_PATH}/libmkl_core.a
                )
    endif ()
endmacro()

# ----------------------------------------------------------------------
# FAISS

macro(build_faiss)
    message(STATUS "Building FAISS-${FAISS_VERSION} from source")

    set(FAISS_PREFIX "${INDEX_BINARY_DIR}/faiss_ep-prefix/src/faiss_ep")
    set(FAISS_INCLUDE_DIR "${FAISS_PREFIX}/include")
    set(FAISS_STATIC_LIB
            "${FAISS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}faiss${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(FAISS_CONFIGURE_ARGS
            "--prefix=${FAISS_PREFIX}"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS} -msse4.2 -O3"
            --without-python)

    if (FAISS_WITH_MKL)
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "CPPFLAGS=-DFINTEGER=long -DMKL_ILP64 -m64 -I${MKL_LIB_PATH}/../../include"
                "LDFLAGS=-L${MKL_LIB_PATH}"
                )
    else ()
        message(STATUS "Build Faiss with OpenBlas/LAPACK")
        if(OpenBLAS_FOUND)
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "LDFLAGS=-L${OpenBLAS_LIB_DIR}")
        else()
            set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "LDFLAGS=-L${OPENBLAS_PREFIX}/lib")
        endif()
    endif ()

    if (KNOWHERE_GPU_VERSION)
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "--with-cuda=${CUDA_TOOLKIT_ROOT_DIR}"
                "--with-cuda-arch=-gencode=arch=compute_35,code=sm_35 -gencode=arch=compute_50,code=sm_50 -gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_86,code=sm_86"
                )
    else ()
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "CPPFLAGS=-DUSE_CPU"
                --without-cuda)
    endif ()

    message(STATUS "Building FAISS with configure args -${FAISS_CONFIGURE_ARGS}")

    if (DEFINED ENV{FAISS_SOURCE_URL})
        set(FAISS_SOURCE_URL "$ENV{FAISS_SOURCE_URL}")
        externalproject_add(faiss_ep
                URL
                ${FAISS_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${FAISS_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE} ${MAKE_BUILD_ARGS} all
                BUILD_IN_SOURCE
                1
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                ${FAISS_STATIC_LIB})
    else ()
        externalproject_add(faiss_ep
                DOWNLOAD_COMMAND
                ""
                SOURCE_DIR
                ${FAISS_SOURCE_DIR}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${FAISS_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE} ${MAKE_BUILD_ARGS} all
                BUILD_IN_SOURCE
                1
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                ${FAISS_STATIC_LIB})
    endif ()

    if(NOT OpenBLAS_FOUND)
        message("add faiss dependencies: openblas_ep")
        ExternalProject_Add_StepDependencies(faiss_ep configure openblas_ep)
    endif()

    file(MAKE_DIRECTORY "${FAISS_INCLUDE_DIR}")
    add_library(faiss STATIC IMPORTED)

    set_target_properties(
            faiss
            PROPERTIES
            IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
    )
    if (FAISS_WITH_MKL)
        set_target_properties(
                faiss
                PROPERTIES
                INTERFACE_LINK_LIBRARIES "${MKL_LIBS}")
    else ()
        set_target_properties(
                faiss
                PROPERTIES
                INTERFACE_LINK_LIBRARIES "${OpenBLAS_LIBRARIES}")
    endif ()

    add_dependencies(faiss faiss_ep)

endmacro()

if (KNOWHERE_WITH_FAISS AND NOT TARGET faiss_ep)

    if (FAISS_WITH_MKL)
        resolve_dependency(MKL)
    else ()
        message("faiss with no mkl")
    endif ()

    resolve_dependency(FAISS)
    get_target_property(FAISS_INCLUDE_DIR faiss INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${FAISS_INCLUDE_DIR}")
    link_directories(SYSTEM ${FAISS_PREFIX}/lib/)
endif ()
