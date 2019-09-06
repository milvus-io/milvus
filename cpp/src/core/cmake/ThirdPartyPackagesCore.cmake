# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(KNOWHERE_THIRDPARTY_DEPENDENCIES

        ARROW
#        BOOST
        FAISS
        GTest
        LAPACK
        OpenBLAS
        )

message(STATUS "Using ${KNOWHERE_DEPENDENCY_SOURCE} approach to find dependencies")

# For each dependency, set dependency source to global default, if unset
foreach(DEPENDENCY ${KNOWHERE_THIRDPARTY_DEPENDENCIES})
    if("${${DEPENDENCY}_SOURCE}" STREQUAL "")
        set(${DEPENDENCY}_SOURCE ${KNOWHERE_DEPENDENCY_SOURCE})
    endif()
endforeach()

macro(build_dependency DEPENDENCY_NAME)
    if("${DEPENDENCY_NAME}" STREQUAL "ARROW")
        build_arrow()
    elseif("${DEPENDENCY_NAME}" STREQUAL "LAPACK")
        build_lapack()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "GTest")
        build_gtest()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "OpenBLAS")
        build_openblas()
    elseif("${DEPENDENCY_NAME}" STREQUAL "FAISS")
        build_faiss()
    else()
        message(FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
    endif ()
endmacro()

macro(resolve_dependency DEPENDENCY_NAME)
    if (${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
        #message(STATUS "Finding ${DEPENDENCY_NAME} package")
#        find_package(${DEPENDENCY_NAME} QUIET)
#        if (NOT ${DEPENDENCY_NAME}_FOUND)
            #message(STATUS "${DEPENDENCY_NAME} package not found")
        build_dependency(${DEPENDENCY_NAME})
#        endif ()
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
        set (CMAKE_OS_NAME "osx" CACHE STRING "Operating system name" FORCE)
    else (APPLE)
        ## Check for Debian GNU/Linux ________________
        find_file (DEBIAN_FOUND debian_version debconf.conf
                PATHS /etc
                )
        if (DEBIAN_FOUND)
            set (CMAKE_OS_NAME "debian" CACHE STRING "Operating system name" FORCE)
        endif (DEBIAN_FOUND)
        ##  Check for Fedora _________________________
        find_file (FEDORA_FOUND fedora-release
                PATHS /etc
                )
        if (FEDORA_FOUND)
            set (CMAKE_OS_NAME "fedora" CACHE STRING "Operating system name" FORCE)
        endif (FEDORA_FOUND)
        ##  Check for RedHat _________________________
        find_file (REDHAT_FOUND redhat-release inittab.RH
                PATHS /etc
                )
        if (REDHAT_FOUND)
            set (CMAKE_OS_NAME "redhat" CACHE STRING "Operating system name" FORCE)
        endif (REDHAT_FOUND)
        ## Extra check for Ubuntu ____________________
        if (DEBIAN_FOUND)
            ## At its core Ubuntu is a Debian system, with
            ## a slightly altered configuration; hence from
            ## a first superficial inspection a system will
            ## be considered as Debian, which signifies an
            ## extra check is required.
            find_file (UBUNTU_EXTRA legal issue
                    PATHS /etc
                    )
            if (UBUNTU_EXTRA)
                ## Scan contents of file
                file (STRINGS ${UBUNTU_EXTRA} UBUNTU_FOUND
                        REGEX Ubuntu
                        )
                ## Check result of string search
                if (UBUNTU_FOUND)
                    set (CMAKE_OS_NAME "ubuntu" CACHE STRING "Operating system name" FORCE)
                    set (DEBIAN_FOUND FALSE)
                endif (UBUNTU_FOUND)
            endif (UBUNTU_EXTRA)
        endif (DEBIAN_FOUND)
    endif (APPLE)
endif (UNIX)


# ----------------------------------------------------------------------
# thirdparty directory
set(THIRDPARTY_DIR "${CORE_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
# JFrog
if(NOT DEFINED USE_JFROG_CACHE)
    set(USE_JFROG_CACHE "OFF")
endif()
if(USE_JFROG_CACHE STREQUAL "ON")
    set(JFROG_ARTFACTORY_CACHE_URL "http://192.168.1.201:80/artifactory/generic-local/milvus/thirdparty/cache/${CMAKE_OS_NAME}/${KNOWHERE_BUILD_ARCH}/${BUILD_TYPE}")
    set(JFROG_USER_NAME "test")
    set(JFROG_PASSWORD "Fantast1c")
    set(THIRDPARTY_PACKAGE_CACHE "${THIRDPARTY_DIR}/cache")
endif()

macro(resolve_dependency DEPENDENCY_NAME)
    if (${DEPENDENCY_NAME}_SOURCE STREQUAL "AUTO")
        #disable find_package for now
        build_dependency(${DEPENDENCY_NAME})
    elseif (${DEPENDENCY_NAME}_SOURCE STREQUAL "BUNDLED")
        build_dependency(${DEPENDENCY_NAME})
    elseif (${DEPENDENCY_NAME}_SOURCE STREQUAL "SYSTEM")
        find_package(${DEPENDENCY_NAME} REQUIRED)
    endif ()
endmacro()

# ----------------------------------------------------------------------
# ExternalProject options

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if(NOT MSVC)
    # Set -fPIC on all external projects
    set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
    set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")
endif()

# CC/CXX environment variables are captured on the first invocation of the
# builder (e.g make or ninja) instead of when CMake is invoked into to build
# directory. This leads to issues if the variables are exported in a subshell
# and the invocation of make/ninja is in distinct subshell without the same
# environment (CC/CXX).
set(EP_COMMON_TOOLCHAIN -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})

if(CMAKE_AR)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_AR=${CMAKE_AR})
endif()

if(CMAKE_RANLIB)
    set(EP_COMMON_TOOLCHAIN ${EP_COMMON_TOOLCHAIN} -DCMAKE_RANLIB=${CMAKE_RANLIB})
endif()

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

if(NOT KNOWHERE_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else()
    set(EP_LOG_OPTIONS)
endif()

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
    if(NOT MSVC)
        find_program(MAKE make)
    endif()
endif()

set(MAKE_BUILD_ARGS "-j8")

## Using make -j in sub-make is fragile
## see discussion https://github.com/apache/KNOWHERE/pull/2779
#if(${CMAKE_GENERATOR} MATCHES "Makefiles")
#    set(MAKE_BUILD_ARGS "")
#else()
#    # limit the maximum number of jobs for ninja
#    set(MAKE_BUILD_ARGS "-j4")
#endif()

# ----------------------------------------------------------------------
# Find pthreads

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# ----------------------------------------------------------------------
# Versions and URLs for toolchain builds, which also can be used to configure
# offline builds

# Read toolchain versions from cpp/thirdparty/versions.txt
file(STRINGS "${THIRDPARTY_DIR}/versions.txt" TOOLCHAIN_VERSIONS_TXT)
foreach(_VERSION_ENTRY ${TOOLCHAIN_VERSIONS_TXT})
    # Exclude comments
    if(NOT _VERSION_ENTRY MATCHES "^[^#][A-Za-z0-9-_]+_VERSION=")
        continue()
    endif()

    string(REGEX MATCH "^[^=]*" _LIB_NAME ${_VERSION_ENTRY})
    string(REPLACE "${_LIB_NAME}=" "" _LIB_VERSION ${_VERSION_ENTRY})

    # Skip blank or malformed lines
    if(${_LIB_VERSION} STREQUAL "")
        continue()
    endif()

    # For debugging
    #message(STATUS "${_LIB_NAME}: ${_LIB_VERSION}")

    set(${_LIB_NAME} "${_LIB_VERSION}")
endforeach()

#if(DEFINED ENV{KNOWHERE_BOOST_URL})
#    set(BOOST_SOURCE_URL "$ENV{KNOWHERE_BOOST_URL}")
#else()
#    string(REPLACE "." "_" BOOST_VERSION_UNDERSCORES ${BOOST_VERSION})
#    set(BOOST_SOURCE_URL
#            "https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORES}.tar.gz"
#    )
#endif()

if(DEFINED ENV{KNOWHERE_FAISS_URL})
    set(FAISS_SOURCE_URL "$ENV{KNOWHERE_FAISS_URL}")
else()
    set(FAISS_SOURCE_URL "http://192.168.1.105:6060/jinhai/faiss/-/archive/${FAISS_VERSION}/faiss-${FAISS_VERSION}.tar.gz")
#    set(FAISS_SOURCE_URL "https://github.com/facebookresearch/faiss/archive/${FAISS_VERSION}.tar.gz")
#    set(FAISS_SOURCE_URL "${CMAKE_SOURCE_DIR}/thirdparty/faiss-1.5.3")
    message(STATUS "FAISS URL = ${FAISS_SOURCE_URL}")
endif()
# set(FAISS_MD5 "a589663865a8558205533c8ac414278c")
# set(FAISS_MD5 "57da9c4f599cc8fa4260488b1c96e1cc") # commit-id 6dbdf75987c34a2c853bd172ea0d384feea8358c
set(FAISS_MD5 "21deb1c708490ca40ecb899122c01403") # commit-id 643e48f479637fd947e7b93fa4ca72b38ecc9a39

if(DEFINED ENV{KNOWHERE_ARROW_URL})
    set(ARROW_SOURCE_URL "$ENV{KNOWHERE_ARROW_URL}")
else()
    set(ARROW_SOURCE_URL
            "https://github.com/apache/arrow.git"
            )
endif()

if (DEFINED ENV{KNOWHERE_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{KNOWHERE_GTEST_URL}")
else ()
    set(GTEST_SOURCE_URL
            "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif()
set(GTEST_MD5 "2e6fbeb6a91310a16efe181886c59596")

if(DEFINED ENV{KNOWHERE_LAPACK_URL})
    set(LAPACK_SOURCE_URL "$ENV{KNOWHERE_LAPACK_URL}")
else()
    set(LAPACK_SOURCE_URL "https://github.com/Reference-LAPACK/lapack/archive/${LAPACK_VERSION}.tar.gz")
endif()
set(LAPACK_MD5 "96591affdbf58c450d45c1daa540dbd2")

if (DEFINED ENV{KNOWHERE_OPENBLAS_URL})
    set(OPENBLAS_SOURCE_URL "$ENV{KNOWHERE_OPENBLAS_URL}")
else ()
    set(OPENBLAS_SOURCE_URL
            "https://github.com/xianyi/OpenBLAS/archive/${OPENBLAS_VERSION}.tar.gz")
endif()
set(OPENBLAS_MD5 "8a110a25b819a4b94e8a9580702b6495")

# ----------------------------------------------------------------------
# ARROW
set(ARROW_PREFIX "${CORE_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")

macro(build_arrow)
    message(STATUS "Building Apache ARROW-${ARROW_VERSION} from source")
#    set(ARROW_PREFIX "${CORE_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")
    set(ARROW_STATIC_LIB_NAME arrow)
#    set(PARQUET_STATIC_LIB_NAME parquet)
    #    set(ARROW_CUDA_STATIC_LIB_NAME arrow_cuda)
    set(ARROW_STATIC_LIB
            "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
#    set(PARQUET_STATIC_LIB
#            "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${PARQUET_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
#            )
    #    set(ARROW_CUDA_STATIC_LIB
    #            "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_CUDA_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    #            )
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            #            "-DARROW_THRIFT_URL=${THRIFT_SOURCE_URL}"
            #"env ARROW_THRIFT_URL=${THRIFT_SOURCE_URL}"
            -DARROW_BUILD_STATIC=ON
            -DARROW_BUILD_SHARED=OFF
            -DARROW_PARQUET=OFF
            -DARROW_USE_GLOG=OFF
            -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
            "-DCMAKE_LIBRARY_PATH=${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs"
            -DCMAKE_BUILD_TYPE=Release
            -DARROW_DEPENDENCY_SOURCE=BUNDLED) #Build all arrow dependencies from source instead of calling find_package first

    #    set($ENV{ARROW_THRIFT_URL} ${THRIFT_SOURCE_URL})

    if(USE_JFROG_CACHE STREQUAL "ON")
        execute_process(COMMAND sh -c "git ls-remote --heads --tags ${ARROW_SOURCE_URL} ${ARROW_VERSION} | cut -f 1" OUTPUT_VARIABLE ARROW_LAST_COMMIT_ID)
        if(${ARROW_LAST_COMMIT_ID} MATCHES "^[^#][a-z0-9]+")
            string(MD5 ARROW_COMBINE_MD5 "${ARROW_LAST_COMMIT_ID}")
            set(ARROW_CACHE_PACKAGE_NAME "arrow_${ARROW_COMBINE_MD5}.tar.gz")
            set(ARROW_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${ARROW_CACHE_PACKAGE_NAME}")
            set(ARROW_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${ARROW_CACHE_PACKAGE_NAME}")

            execute_process(COMMAND wget -q --method HEAD ${ARROW_CACHE_URL} RESULT_VARIABLE return_code)
            message(STATUS "Check the remote file ${ARROW_CACHE_URL}. return code = ${return_code}")
            if (NOT return_code EQUAL 0)
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
                        ${MAKE}
                        ${MAKE_BUILD_ARGS}
                        INSTALL_COMMAND
                        ${MAKE} install
                        BUILD_BYPRODUCTS
                        "${ARROW_STATIC_LIB}"
                        )

                ExternalProject_Create_Cache(arrow_ep ${ARROW_CACHE_PACKAGE_PATH} "${CORE_BINARY_DIR}/arrow_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${ARROW_CACHE_URL})
            else()
                file(DOWNLOAD ${ARROW_CACHE_URL} ${ARROW_CACHE_PACKAGE_PATH} STATUS status)
                list(GET status 0 status_code)
                message(STATUS "DOWNLOADING FROM ${ARROW_CACHE_URL} TO ${ARROW_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
                if (status_code EQUAL 0)
                    ExternalProject_Use_Cache(arrow_ep ${ARROW_CACHE_PACKAGE_PATH} ${CORE_BINARY_DIR})
                endif()
            endif()
        else()
            message(FATAL_ERROR "The last commit ID of \"${ARROW_SOURCE_URL}\" repository don't match!")
        endif()
    else()
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
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                "${ARROW_STATIC_LIB}"
                )
    endif()

    file(MAKE_DIRECTORY "${ARROW_PREFIX}/include")
    add_library(arrow STATIC IMPORTED)
    set_target_properties(arrow
            PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}")
    #            INTERFACE_LINK_LIBRARIES thrift)
    add_dependencies(arrow arrow_ep)

    set(JEMALLOC_PREFIX "${CORE_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep-build/jemalloc_ep-prefix/src/jemalloc_ep")

    add_custom_command(TARGET arrow_ep POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory ${ARROW_PREFIX}/lib/
            COMMAND ${CMAKE_COMMAND} -E copy ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a ${ARROW_PREFIX}/lib/
            DEPENDS ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a)

endmacro()

if(KNOWHERE_WITH_ARROW AND NOT TARGET arrow_ep)

    resolve_dependency(ARROW)

    link_directories(SYSTEM ${ARROW_PREFIX}/lib/)
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

#set(Boost_USE_MULTITHREADED ON)
#if(MSVC AND KNOWHERE_USE_STATIC_CRT)
#    set(Boost_USE_STATIC_RUNTIME ON)
#endif()
#set(Boost_ADDITIONAL_VERSIONS
#        "1.70.0"
#        "1.70"
#        "1.69.0"
#        "1.69"
#        "1.68.0"
#        "1.68"
#        "1.67.0"
#        "1.67"
#        "1.66.0"
#        "1.66"
#        "1.65.0"
#        "1.65"
#        "1.64.0"
#        "1.64"
#        "1.63.0"
#        "1.63"
#        "1.62.0"
#        "1.61"
#        "1.61.0"
#        "1.62"
#        "1.60.0"
#        "1.60")
#
## TODO
#if(KNOWHERE_BOOST_VENDORED)
##    system thread serialization wserialization regex
#    set(BOOST_PREFIX "${CORE_BINARY_DIR}/boost_ep-prefix/src/boost_ep")
#    set(BOOST_LIB_DIR "${BOOST_PREFIX}/stage/lib")
#    set(BOOST_BUILD_LINK "static")
#    set(BOOST_STATIC_SYSTEM_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX}"
#    )
#    set(BOOST_STATIC_FILESYSTEM_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX}"
#    )
#    set(BOOST_STATIC_SERIALIZATION_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_serialization${CMAKE_STATIC_LIBRARY_SUFFIX}"
#    )
#    set(BOOST_STATIC_WSERIALIZATION_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_wserialization${CMAKE_STATIC_LIBRARY_SUFFIX}"
#            )
#    set(BOOST_STATIC_REGEX_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_regex${CMAKE_STATIC_LIBRARY_SUFFIX}"
#            )
#    set(BOOST_STATIC_THREAD_LIBRARY
#            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_thread${CMAKE_STATIC_LIBRARY_SUFFIX}"
#            )
#    set(BOOST_SYSTEM_LIBRARY boost_system_static)
#    set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_static)
#    set(BOOST_SERIALIZATION_LIBRARY boost_serialization_static)
#    set(BOOST_WSERIALIZATION_LIBRARY boost_wserialization_static)
#    set(BOOST_REGEX_LIBRARY boost_regex_static)
#    set(BOOST_THREAD_LIBRARY boost_thread_static)
#
#    if(KNOWHERE_BOOST_HEADER_ONLY)
#        set(BOOST_BUILD_PRODUCTS)
#        set(BOOST_CONFIGURE_COMMAND "")
#        set(BOOST_BUILD_COMMAND "")
#    else()
#        set(BOOST_BUILD_PRODUCTS ${BOOST_STATIC_SYSTEM_LIBRARY}
#                ${BOOST_STATIC_FILESYSTEM_LIBRARY} ${BOOST_STATIC_SERIALIZATION_LIBRARY}
#                ${BOOST_STATIC_WSERIALIZATION_LIBRARY} ${BOOST_STATIC_REGEX_LIBRARY}
#                ${BOOST_STATIC_THREAD_LIBRARY})
#        set(BOOST_CONFIGURE_COMMAND "./bootstrap.sh" "--prefix=${BOOST_PREFIX}"
#                "--with-libraries=filesystem,serialization,wserialization,system,thread,regex")
#        if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
#            set(BOOST_BUILD_VARIANT "debug")
#        else()
#            set(BOOST_BUILD_VARIANT "release")
#        endif()
#        set(BOOST_BUILD_COMMAND
#                "./b2"
#                "link=${BOOST_BUILD_LINK}"
#                "variant=${BOOST_BUILD_VARIANT}"
#                "cxxflags=-fPIC")
#
#        add_thirdparty_lib(boost_system STATIC_LIB "${BOOST_STATIC_SYSTEM_LIBRARY}")
#
#        add_thirdparty_lib(boost_filesystem STATIC_LIB "${BOOST_STATIC_FILESYSTEM_LIBRARY}")
#
#        add_thirdparty_lib(boost_serialization STATIC_LIB "${BOOST_STATIC_SERIALIZATION_LIBRARY}")
#
#        add_thirdparty_lib(boost_wserialization STATIC_LIB "${BOOST_STATIC_WSERIALIZATION_LIBRARY}")
#
#        add_thirdparty_lib(boost_regex STATIC_LIB "${BOOST_STATIC_REGEX_LIBRARY}")
#
#        add_thirdparty_lib(boost_thread STATIC_LIB "${BOOST_STATIC_THREAD_LIBRARY}")
#
#        set(KNOWHERE_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY} ${BOOST_SERIALIZATION_LIBRARY}
#                ${BOOST_WSERIALIZATION_LIBRARY} ${BOOST_REGEX_LIBRARY} ${BOOST_THREAD_LIBRARY})
#    endif()
#    externalproject_add(boost_ep
#            URL
#            ${BOOST_SOURCE_URL}
#            BUILD_BYPRODUCTS
#            ${BOOST_BUILD_PRODUCTS}
#            BUILD_IN_SOURCE
#            1
#            CONFIGURE_COMMAND
#            ${BOOST_CONFIGURE_COMMAND}
#            BUILD_COMMAND
#            ${BOOST_BUILD_COMMAND}
#            INSTALL_COMMAND
#            ""
#            ${EP_LOG_OPTIONS})
#    set(Boost_INCLUDE_DIR "${BOOST_PREFIX}")
#    set(Boost_INCLUDE_DIRS "${BOOST_INCLUDE_DIR}")
#    add_dependencies(boost_system_static boost_ep)
#    add_dependencies(boost_filesystem_static boost_ep)
#    add_dependencies(boost_serialization_static boost_ep)
#    add_dependencies(boost_wserialization_static boost_ep)
#    add_dependencies(boost_regex_static boost_ep)
#    add_dependencies(boost_thread_static boost_ep)
#
##else()
##    if(MSVC)
##        # disable autolinking in boost
##        add_definitions(-DBOOST_ALL_NO_LIB)
##    endif()
#
##    if(DEFINED ENV{BOOST_ROOT} OR DEFINED BOOST_ROOT)
##        # In older versions of CMake (such as 3.2), the system paths for Boost will
##        # In older versions of CMake (such as 3.2), the system paths for Boost will
##        # be looked in first even if we set $BOOST_ROOT or pass -DBOOST_ROOT
##        set(Boost_NO_SYSTEM_PATHS ON)
##    endif()
#
##    if(KNOWHERE_BOOST_USE_SHARED)
##        # Find shared Boost libraries.
##        set(Boost_USE_STATIC_LIBS OFF)
##        set(BUILD_SHARED_LIBS_KEEP ${BUILD_SHARED_LIBS})
##        set(BUILD_SHARED_LIBS ON)
##
##        if(MSVC)
##            # force all boost libraries to dynamic link
##            add_definitions(-DBOOST_ALL_DYN_LINK)
##        endif()
##
##        if(KNOWHERE_BOOST_HEADER_ONLY)
##            find_package(Boost REQUIRED)
##        else()
##            find_package(Boost COMPONENTS serialization system filesystem REQUIRED)
##            set(BOOST_SYSTEM_LIBRARY Boost::system)
##            set(BOOST_FILESYSTEM_LIBRARY Boost::filesystem)
##            set(BOOST_SERIALIZATION_LIBRARY Boost::serialization)
##            set(KNOWHERE_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY})
##        endif()
##        set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_KEEP})
##        unset(BUILD_SHARED_LIBS_KEEP)
##    else()
##        # Find static boost headers and libs
##        # TODO Differentiate here between release and debug builds
##        set(Boost_USE_STATIC_LIBS ON)
##        if(KNOWHERE_BOOST_HEADER_ONLY)
##            find_package(Boost REQUIRED)
##        else()
##            find_package(Boost COMPONENTS serialization system filesystem REQUIRED)
##            set(BOOST_SYSTEM_LIBRARY Boost::system)
##            set(BOOST_FILESYSTEM_LIBRARY Boost::filesystem)
##            set(BOOST_SERIALIZATION_LIBRARY Boost::serialization)
##            set(KNOWHERE_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY})
##        endif()
##    endif()
#endif()
#
##message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIR})
##message(STATUS "Boost libraries: " ${Boost_LIBRARIES})
#
#include_directories(SYSTEM ${Boost_INCLUDE_DIR})
#link_directories(SYSTEM ${BOOST_LIB_DIR})

# ----------------------------------------------------------------------
# OpenBLAS

macro(build_openblas)
    message(STATUS "Building OpenBLAS-${OPENBLAS_VERSION} from source")
    set(OPENBLAS_PREFIX "${CORE_BINARY_DIR}/openblas_ep-prefix/src/openblas_ep")
    set(OPENBLAS_INCLUDE_DIR "${OPENBLAS_PREFIX}/include")
    set(OPENBLAS_STATIC_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}openblas${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(OPENBLAS_REAL_STATIC_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}openblas_haswellp-r0.3.6${CMAKE_STATIC_LIBRARY_SUFFIX}")

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(OPENBLAS_CACHE_PACKAGE_NAME "openblas_${OPENBLAS_MD5}.tar.gz")
        set(OPENBLAS_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${OPENBLAS_CACHE_PACKAGE_NAME}")
        set(OPENBLAS_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${OPENBLAS_CACHE_PACKAGE_NAME}")

        execute_process(COMMAND wget -q --method HEAD ${OPENBLAS_CACHE_URL} RESULT_VARIABLE return_code)
        message(STATUS "Check the remote file ${OPENBLAS_CACHE_URL}. return code = ${return_code}")
        if (NOT return_code EQUAL 0)
            externalproject_add(openblas_ep
                    URL
                    ${OPENBLAS_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    CONFIGURE_COMMAND
                    ""
                    BUILD_IN_SOURCE
                    1
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    INSTALL_COMMAND
                    ${MAKE}
                    PREFIX=${OPENBLAS_PREFIX}
                    install
                    BUILD_BYPRODUCTS
                    ${OPENBLAS_STATIC_LIB})

            ExternalProject_Create_Cache(openblas_ep ${OPENBLAS_CACHE_PACKAGE_PATH} "${CORE_BINARY_DIR}/openblas_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${OPENBLAS_CACHE_URL})
        else()
            file(DOWNLOAD ${OPENBLAS_CACHE_URL} ${OPENBLAS_CACHE_PACKAGE_PATH} STATUS status)
            list(GET status 0 status_code)
            message(STATUS "DOWNLOADING FROM ${OPENBLAS_CACHE_URL} TO ${OPENBLAS_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
            if (status_code EQUAL 0)
                ExternalProject_Use_Cache(openblas_ep ${OPENBLAS_CACHE_PACKAGE_PATH} ${CORE_BINARY_DIR})
            endif()
        endif()
    else()
        externalproject_add(openblas_ep
                URL
                ${OPENBLAS_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                ""
                BUILD_IN_SOURCE
                1
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                INSTALL_COMMAND
                ${MAKE}
                PREFIX=${OPENBLAS_PREFIX}
                install
                BUILD_BYPRODUCTS
                ${OPENBLAS_STATIC_LIB})
    endif()

    file(MAKE_DIRECTORY "${OPENBLAS_INCLUDE_DIR}")
    add_library(openblas STATIC IMPORTED)
    set_target_properties(
            openblas
            PROPERTIES IMPORTED_LOCATION "${OPENBLAS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${OPENBLAS_INCLUDE_DIR}")

    add_dependencies(openblas openblas_ep)
endmacro()

#if(KNOWHERE_WITH_OPENBLAS)
#    resolve_dependency(OpenBLAS)
#
#    get_target_property(OPENBLAS_INCLUDE_DIR openblas INTERFACE_INCLUDE_DIRECTORIES)
#    include_directories(SYSTEM "${OPENBLAS_INCLUDE_DIR}")
#    link_directories(SYSTEM ${OPENBLAS_PREFIX}/lib)
#endif()

# ----------------------------------------------------------------------
# LAPACK

macro(build_lapack)
    message(STATUS "Building LAPACK-${LAPACK_VERSION} from source")
    set(LAPACK_PREFIX "${CORE_BINARY_DIR}/lapack_ep-prefix/src/lapack_ep")
    set(LAPACK_INCLUDE_DIR "${LAPACK_PREFIX}/include")
    set(LAPACK_STATIC_LIB
            "${LAPACK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lapack${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(BLAS_STATIC_LIB
            "${LAPACK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}blas${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(LAPACK_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${LAPACK_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib)

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(LAPACK_CACHE_PACKAGE_NAME "lapack_${LAPACK_MD5}.tar.gz")
        set(LAPACK_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${LAPACK_CACHE_PACKAGE_NAME}")
        set(LAPACK_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${LAPACK_CACHE_PACKAGE_NAME}")

        execute_process(COMMAND wget -q --method HEAD ${LAPACK_CACHE_URL} RESULT_VARIABLE return_code)
        message(STATUS "Check the remote file ${LAPACK_CACHE_URL}. return code = ${return_code}")
        if (NOT return_code EQUAL 0)
            externalproject_add(lapack_ep
                    URL
                    ${LAPACK_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    CMAKE_ARGS
                    ${LAPACK_CMAKE_ARGS}
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    BUILD_BYPRODUCTS
                    ${LAPACK_STATIC_LIB})

            ExternalProject_Create_Cache(lapack_ep ${LAPACK_CACHE_PACKAGE_PATH} "${CORE_BINARY_DIR}/lapack_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${LAPACK_CACHE_URL})
        else()
            file(DOWNLOAD ${LAPACK_CACHE_URL} ${LAPACK_CACHE_PACKAGE_PATH} STATUS status)
            list(GET status 0 status_code)
            message(STATUS "DOWNLOADING FROM ${LAPACK_CACHE_URL} TO ${LAPACK_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
            if (status_code EQUAL 0)
                ExternalProject_Use_Cache(lapack_ep ${LAPACK_CACHE_PACKAGE_PATH} ${CORE_BINARY_DIR})
            endif()
        endif()
    else()
        externalproject_add(lapack_ep
                URL
                ${LAPACK_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CMAKE_ARGS
                ${LAPACK_CMAKE_ARGS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_BYPRODUCTS
                ${LAPACK_STATIC_LIB})
    endif()

    file(MAKE_DIRECTORY "${LAPACK_INCLUDE_DIR}")
    add_library(lapack STATIC IMPORTED)
    set_target_properties(
            lapack
            PROPERTIES IMPORTED_LOCATION "${LAPACK_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${LAPACK_INCLUDE_DIR}")

    add_dependencies(lapack lapack_ep)
endmacro()

#if(KNOWHERE_WITH_LAPACK)
#    resolve_dependency(LAPACK)
#
#    get_target_property(LAPACK_INCLUDE_DIR lapack INTERFACE_INCLUDE_DIRECTORIES)
#    include_directories(SYSTEM "${LAPACK_INCLUDE_DIR}")
#    link_directories(SYSTEM ${LAPACK_PREFIX}/lib)
#endif()

# ----------------------------------------------------------------------
# Google gtest

macro(build_gtest)
    message(STATUS "Building gtest-${GTEST_VERSION} from source")
    set(GTEST_VENDORED TRUE)
    set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")

    if(APPLE)
        set(GTEST_CMAKE_CXX_FLAGS
                ${GTEST_CMAKE_CXX_FLAGS}
                -DGTEST_USE_OWN_TR1_TUPLE=1
                -Wno-unused-value
                -Wno-ignored-attributes)
    endif()

    set(GTEST_PREFIX "${CORE_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
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


    if(USE_JFROG_CACHE STREQUAL "ON")
        set(GTEST_CACHE_PACKAGE_NAME "googletest_${GTEST_MD5}.tar.gz")
        set(GTEST_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${GTEST_CACHE_PACKAGE_NAME}")
        set(GTEST_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${GTEST_CACHE_PACKAGE_NAME}")

        execute_process(COMMAND wget -q --method HEAD ${GTEST_CACHE_URL} RESULT_VARIABLE return_code)
        message(STATUS "Check the remote file ${GTEST_CACHE_URL}. return code = ${return_code}")
        if (NOT return_code EQUAL 0)
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

            ExternalProject_Create_Cache(googletest_ep ${GTEST_CACHE_PACKAGE_PATH} "${CORE_BINARY_DIR}/googletest_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${GTEST_CACHE_URL})
        else()
            file(DOWNLOAD ${GTEST_CACHE_URL} ${GTEST_CACHE_PACKAGE_PATH} STATUS status)
            list(GET status 0 status_code)
            message(STATUS "DOWNLOADING FROM ${GTEST_CACHE_URL} TO ${GTEST_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
            if (status_code EQUAL 0)
                ExternalProject_Use_Cache(googletest_ep ${GTEST_CACHE_PACKAGE_PATH} ${CORE_BINARY_DIR})
            endif()
        endif()
    else()
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
    endif()

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
    #message(STATUS "Resolving gtest dependency")
    resolve_dependency(GTest)

    if(NOT GTEST_VENDORED)
    endif()

    # TODO: Don't use global includes but rather target_include_directories
    get_target_property(GTEST_INCLUDE_DIR gtest INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM "${GTEST_PREFIX}/lib")
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# FAISS

macro(build_faiss)
    message(STATUS "Building FAISS-${FAISS_VERSION} from source")
    set(FAISS_PREFIX "${CORE_BINARY_DIR}/faiss_ep-prefix/src/faiss_ep")
    set(FAISS_INCLUDE_DIR "${FAISS_PREFIX}/include")
    set(FAISS_STATIC_LIB
            "${FAISS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}faiss${CMAKE_STATIC_LIBRARY_SUFFIX}")

    #    add_custom_target(faiss_dependencies)
    #    add_dependencies(faiss_dependencies openblas_ep)
    #    add_dependencies(faiss_dependencies openblas)
    #    get_target_property(FAISS_OPENBLAS_LIB_DIR openblas IMPORTED_LOCATION)
    #    get_filename_component(FAISS_OPENBLAS_LIB "${FAISS_OPENBLAS_LIB_DIR}" DIRECTORY)

    set(FAISS_CONFIGURE_ARGS
            "--prefix=${FAISS_PREFIX}"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS}"
            "LDFLAGS=-L${OPENBLAS_PREFIX}/lib -L${LAPACK_PREFIX}/lib -lopenblas -llapack"
            --without-python)

    #    if(OPENBLAS_STATIC_LIB)
    #        set(OPENBLAS_LIBRARY ${OPENBLAS_STATIC_LIB})
    #    else()
    #        set(OPENBLAS_LIBRARY ${OPENBLAS_SHARED_LIB})
    #    endif()
    #    set(FAISS_DEPENDENCIES ${FAISS_DEPENDENCIES} ${OPENBLAS_LIBRARY})

    if(${KNOWHERE_WITH_FAISS_GPU_VERSION} STREQUAL "ON")
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "--with-cuda=${CUDA_TOOLKIT_ROOT_DIR}"
                "--with-cuda-arch=-gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_75,code=sm_75"
                )
    else()
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS} --without-cuda)
    endif()

    if(USE_JFROG_CACHE STREQUAL "ON")
#        Check_Last_Modify("${CMAKE_SOURCE_DIR}/thirdparty/faiss_cache_check_lists.txt" "${CMAKE_SOURCE_DIR}" FAISS_LAST_MODIFIED_COMMIT_ID)
        string(MD5 FAISS_COMBINE_MD5 "${FAISS_MD5}${LAPACK_MD5}${OPENBLAS_MD5}")
        # string(MD5 FAISS_COMBINE_MD5 "${FAISS_LAST_MODIFIED_COMMIT_ID}${LAPACK_MD5}${OPENBLAS_MD5}")
        set(FAISS_CACHE_PACKAGE_NAME "faiss_${FAISS_COMBINE_MD5}.tar.gz")
        set(FAISS_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${FAISS_CACHE_PACKAGE_NAME}")
        set(FAISS_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${FAISS_CACHE_PACKAGE_NAME}")

        execute_process(COMMAND wget -q --method HEAD ${FAISS_CACHE_URL} RESULT_VARIABLE return_code)
        message(STATUS "Check the remote file ${FAISS_CACHE_URL}. return code = ${return_code}")
        if (NOT return_code EQUAL 0)
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

            ExternalProject_Add_StepDependencies(faiss_ep build openblas_ep lapack_ep)

            ExternalProject_Create_Cache(faiss_ep ${FAISS_CACHE_PACKAGE_PATH} "${CORE_BINARY_DIR}/faiss_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${FAISS_CACHE_URL})
        else()
            file(DOWNLOAD ${FAISS_CACHE_URL} ${FAISS_CACHE_PACKAGE_PATH} STATUS status)
            list(GET status 0 status_code)
            message(STATUS "DOWNLOADING FROM ${FAISS_CACHE_URL} TO ${FAISS_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
            if (status_code EQUAL 0)
                ExternalProject_Use_Cache(faiss_ep ${FAISS_CACHE_PACKAGE_PATH} ${CORE_BINARY_DIR})
            endif()
        endif()
    else()
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

        ExternalProject_Add_StepDependencies(faiss_ep build openblas_ep lapack_ep)
    endif()

    file(MAKE_DIRECTORY "${FAISS_INCLUDE_DIR}")
    add_library(faiss STATIC IMPORTED)
    set_target_properties(
            faiss
            PROPERTIES IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "openblas;lapack" )

    add_dependencies(faiss faiss_ep)
    #add_dependencies(faiss openblas_ep)
    #add_dependencies(faiss lapack_ep)
    #target_link_libraries(faiss ${OPENBLAS_PREFIX}/lib)
    #target_link_libraries(faiss ${LAPACK_PREFIX}/lib)

endmacro()

if(KNOWHERE_WITH_FAISS AND NOT TARGET faiss_ep)

    resolve_dependency(OpenBLAS)
    get_target_property(OPENBLAS_INCLUDE_DIR openblas INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${OPENBLAS_INCLUDE_DIR}")
    link_directories(SYSTEM ${OPENBLAS_PREFIX}/lib)

    resolve_dependency(LAPACK)
    get_target_property(LAPACK_INCLUDE_DIR lapack INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${LAPACK_INCLUDE_DIR}")
    link_directories(SYSTEM "${LAPACK_PREFIX}/lib")

    resolve_dependency(FAISS)
    get_target_property(FAISS_INCLUDE_DIR faiss INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${FAISS_INCLUDE_DIR}")
#    include_directories(SYSTEM "${CORE_BINARY_DIR}/faiss_ep-prefix/src/")
#    link_directories(SYSTEM ${FAISS_PREFIX}/)
    link_directories(SYSTEM ${FAISS_PREFIX}/lib/)
#    link_directories(SYSTEM ${FAISS_PREFIX}/gpu/)
endif()
