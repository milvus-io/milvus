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
message(STATUS "Using ${KNOWHERE_DEPENDENCY_SOURCE} approach to find dependencies")
set(OpenBLAS_SOURCE "AUTO")

# ----------------------------------------------------------------------
# Identify OS
if (UNIX)
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
endif (UNIX)


# ----------------------------------------------------------------------
# thirdparty directory
set(THIRDPARTY_DIR "${INDEX_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
# ExternalProject options

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(FAISS_FLAGS "-DELPP_THREAD_SAFE -fopenmp -Werror=return-type")
set(EP_CXX_FLAGS "${FAISS_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS   "${FAISS_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

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

set(MAKE_BUILD_ARGS "-j6")


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

if (DEFINED ENV{KNOWHERE_OPENBLAS_URL})
    set(OPENBLAS_SOURCE_URL "$ENV{KNOWHERE_OPENBLAS_URL}")
else ()
    set(OPENBLAS_SOURCE_URL
            "https://github.com.cnpmjs.org/xianyi/OpenBLAS/archive/v${OPENBLAS_VERSION}.tar.gz")
endif ()

# ----------------------------------------------------------------------
# Openblas
macro(build_openblas)
    set(OPENBLAS_PREFIX "${INDEX_BINARY_DIR}/openblas_ep-prefix/src/openblas_ep")
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
    if (OpenBLAS_SOURCE STREQUAL "AUTO")
       unset(OpenBLAS_DIR CACHE)
       set(OpenBLAS_INCLUDE_SEARCH_PATHS
            /opt/OpenBLAS/include
            ${OPENBLAS_PREFIX}
          )
        set(OpenBLAS_LIB_SEARCH_PATHS
            /opt/OpenBLAS/lib
            ${OPENBLAS_PREFIX}
	  )
        find_path(OpenBLAS_INCLUDE_DIR NAMES openblas_config.h lapacke.h PATHS ${OpenBLAS_INCLUDE_SEARCH_PATHS})
        find_library(OpenBLAS_LIB NAMES openblas PATHS ${OpenBLAS_LIB_SEARCH_PATHS})
        find_library(Lapacke_LIB NAMES lapacke PATHS ${OpenBLAS_LIB_SEARCH_PATHS})
        set(OpenBLAS_FOUND ON)
        #    Check include files
        if(NOT OpenBLAS_INCLUDE_DIR)
            set(OpenBLAS_FOUND OFF)
        endif()
        #    Check libraries
        if(NOT OpenBLAS_LIB)
            set(OpenBLAS_FOUND OFF)
        endif()
	if (OpenBLAS_FOUND)
        set(OpenBLAS_LIBRARIES ${OpenBLAS_LIB})  #location of .so
	    STRING(REGEX REPLACE "/libopenblas.so" "" OpenBLAS_LIB_DIR ${OpenBLAS_LIBRARIES})
        if (Lapacke_LIB)
            set(OpenBLAS_LIBRARIES ${OpenBLAS_LIBRARIES} ${Lapacke_LIB})
        endif()
        add_library(openblas SHARED IMPORTED)
        set_target_properties(
            openblas
            PROPERTIES
	        IMPORTED_GLOBAL TRUE
	        IMPORTED_LOCATION "${OpenBLAS_LIBRARIES}"
            INTERFACE_INCLUDE_DIRECTORIES "${OpenBLAS_INCLUDE_DIR}")
        else()
	        build_openblas()
        endif()
    elseif (OpenBLAS_SOURCE STREQUAL "BUNDLED")
        build_openblas()
    elseif (OpenBLAS_SOURCE STREQUAL "SYSTEM")
        find_package(OpenBLAS REQUIRED)
    endif ()    
    include_directories(SYSTEM "${OpenBLAS_INCLUDE_DIR}")
    link_directories(SYSTEM "${OpenBLAS_LIB_DIR}")
endif()
