# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(MILVUS_THIRDPARTY_DEPENDENCIES

        ARROW
        BOOST
        BZip2
        Easylogging++
        FAISS
        GTest
        Knowhere
        JSONCONS
        LAPACK
        Lz4
        MySQLPP
        OpenBLAS
        Prometheus
        RocksDB
        Snappy
        SQLite
        SQLite_ORM
        Thrift
        yaml-cpp
        ZLIB
        ZSTD
        AWS
        libunwind
        gperftools)

message(STATUS "Using ${MILVUS_DEPENDENCY_SOURCE} approach to find dependencies")

# For each dependency, set dependency source to global default, if unset
foreach(DEPENDENCY ${MILVUS_THIRDPARTY_DEPENDENCIES})
    if("${${DEPENDENCY}_SOURCE}" STREQUAL "")
        set(${DEPENDENCY}_SOURCE ${MILVUS_DEPENDENCY_SOURCE})
    endif()
endforeach()

macro(build_dependency DEPENDENCY_NAME)
    if("${DEPENDENCY_NAME}" STREQUAL "ARROW")
        build_arrow()
    elseif("${DEPENDENCY_NAME}" STREQUAL "BZip2")
        build_bzip2()
    elseif("${DEPENDENCY_NAME}" STREQUAL "Easylogging++")
        build_easyloggingpp()
    elseif("${DEPENDENCY_NAME}" STREQUAL "FAISS")
        build_faiss()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "GTest")
        build_gtest()
    elseif("${DEPENDENCY_NAME}" STREQUAL "LAPACK")
        build_lapack()
    elseif("${DEPENDENCY_NAME}" STREQUAL "Knowhere")
        build_knowhere()
    elseif("${DEPENDENCY_NAME}" STREQUAL "Lz4")
        build_lz4()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "MySQLPP")
        build_mysqlpp()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "JSONCONS")
        build_jsoncons()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "OpenBLAS")
        build_openblas()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "Prometheus")
        build_prometheus()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "RocksDB")
        build_rocksdb()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "Snappy")
        build_snappy()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "SQLite")
        build_sqlite()
    elseif ("${DEPENDENCY_NAME}" STREQUAL "SQLite_ORM")
        build_sqlite_orm()
    elseif("${DEPENDENCY_NAME}" STREQUAL "Thrift")
        build_thrift()
    elseif("${DEPENDENCY_NAME}" STREQUAL "yaml-cpp")
        build_yamlcpp()
    elseif("${DEPENDENCY_NAME}" STREQUAL "ZLIB")
        build_zlib()
    elseif("${DEPENDENCY_NAME}" STREQUAL "ZSTD")
        build_zstd()
    elseif("${DEPENDENCY_NAME}" STREQUAL "AWS")
        build_aws()
    elseif("${DEPENDENCY_NAME}" STREQUAL "libunwind")
        build_libunwind()
    elseif("${DEPENDENCY_NAME}" STREQUAL "gperftools")
        build_gperftools()
    else()
        message(FATAL_ERROR "Unknown thirdparty dependency to build: ${DEPENDENCY_NAME}")
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
set(THIRDPARTY_DIR "${MILVUS_SOURCE_DIR}/thirdparty")

# ----------------------------------------------------------------------
# JFrog
if(NOT DEFINED USE_JFROG_CACHE)
    set(USE_JFROG_CACHE "ON")
endif()
if(USE_JFROG_CACHE STREQUAL "ON")    
    set(JFROG_ARTFACTORY_CACHE_URL "http://192.168.1.201:80/artifactory/generic-local/milvus/thirdparty/cache/${CMAKE_OS_NAME}/${MILVUS_BUILD_ARCH}")
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

if(NOT MILVUS_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else()
    set(EP_LOG_OPTIONS)
endif()

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
    find_program(MAKE make)
endif()

if (NOT DEFINED MAKE_BUILD_ARGS)
    set(MAKE_BUILD_ARGS "-j8")
endif()
message(STATUS "Third Party MAKE_BUILD_ARGS = ${MAKE_BUILD_ARGS}")

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

if(DEFINED ENV{MILVUS_ARROW_URL})
    set(ARROW_SOURCE_URL "$ENV{MILVUS_ARROW_URL}")
else()
    set(ARROW_SOURCE_URL
            "https://github.com/youny626/arrow.git"
            )
endif()

if(DEFINED ENV{MILVUS_BOOST_URL})
    set(BOOST_SOURCE_URL "$ENV{MILVUS_BOOST_URL}")
else()
    string(REPLACE "." "_" BOOST_VERSION_UNDERSCORES ${BOOST_VERSION})
    set(BOOST_SOURCE_URL
            "https://dl.bintray.com/boostorg/release/${BOOST_VERSION}/source/boost_${BOOST_VERSION_UNDERSCORES}.tar.gz"
    )
endif()
set(BOOST_MD5 "fea771fe8176828fabf9c09242ee8c26")

if(DEFINED ENV{MILVUS_BZIP2_URL})
    set(BZIP2_SOURCE_URL "$ENV{MILVUS_BZIP2_URL}")
else()
    set(BZIP2_SOURCE_URL "https://sourceware.org/pub/bzip2/bzip2-${BZIP2_VERSION}.tar.gz")
endif()
set(BZIP2_MD5 "00b516f4704d4a7cb50a1d97e6e8e15b")

if(DEFINED ENV{MILVUS_EASYLOGGINGPP_URL})
    set(EASYLOGGINGPP_SOURCE_URL "$ENV{MILVUS_EASYLOGGINGPP_URL}")
else()
    set(EASYLOGGINGPP_SOURCE_URL "https://github.com/zuhd-org/easyloggingpp/archive/${EASYLOGGINGPP_VERSION}.tar.gz")
endif()
set(EASYLOGGINGPP_MD5 "b78cd319db4be9b639927657b8aa7732")

if(DEFINED ENV{MILVUS_FAISS_URL})
    set(FAISS_SOURCE_URL "$ENV{MILVUS_FAISS_URL}")
else()
    set(FAISS_SOURCE_URL "https://github.com/facebookresearch/faiss/archive/${FAISS_VERSION}.tar.gz")
endif()
set(FAISS_MD5 "0bc12737b23def156f6a1eb782050135")

if(DEFINED ENV{MILVUS_KNOWHERE_URL})
    set(KNOWHERE_SOURCE_URL "$ENV{MILVUS_KNOWHERE_URL}")
else()
    set(KNOWHERE_SOURCE_URL "${CMAKE_SOURCE_DIR}/thirdparty/knowhere")
endif()

if (DEFINED ENV{MILVUS_GTEST_URL})
    set(GTEST_SOURCE_URL "$ENV{MILVUS_GTEST_URL}")
else ()
    set(GTEST_SOURCE_URL
            "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
endif()

if (DEFINED ENV{MILVUS_JSONCONS_URL})
    set(JSONCONS_SOURCE_URL "$ENV{MILVUS_JSONCONS_URL}")
else ()
    set(JSONCONS_SOURCE_URL
            "https://github.com/danielaparker/jsoncons/archive/v${JSONCONS_VERSION}.tar.gz")
endif()

if(DEFINED ENV{MILVUS_LAPACK_URL})
    set(LAPACK_SOURCE_URL "$ENV{MILVUS_LAPACK_URL}")
else()
    set(LAPACK_SOURCE_URL "https://github.com/Reference-LAPACK/lapack/archive/${LAPACK_VERSION}.tar.gz")
endif()
set(LAPACK_MD5 "96591affdbf58c450d45c1daa540dbd2")

if(DEFINED ENV{MILVUS_LZ4_URL})
    set(LZ4_SOURCE_URL "$ENV{MILVUS_LZ4_URL}")
else()
    set(LZ4_SOURCE_URL "https://github.com/lz4/lz4/archive/${LZ4_VERSION}.tar.gz")
endif()
set(LZ4_MD5 "a80f28f2a2e5fe59ebfe8407f793da22")

if(DEFINED ENV{MILVUS_MYSQLPP_URL})
    set(MYSQLPP_SOURCE_URL "$ENV{MILVUS_MYSQLPP_URL}")
else()
    set(MYSQLPP_SOURCE_URL "https://tangentsoft.com/mysqlpp/releases/mysql++-${MYSQLPP_VERSION}.tar.gz")
endif()
set(MYSQLPP_MD5 "cda38b5ecc0117de91f7c42292dd1e79")

if (DEFINED ENV{MILVUS_OPENBLAS_URL})
    set(OPENBLAS_SOURCE_URL "$ENV{MILVUS_OPENBLAS_URL}")
else ()
    set(OPENBLAS_SOURCE_URL
            "https://github.com/xianyi/OpenBLAS/archive/${OPENBLAS_VERSION}.tar.gz")
endif()
set(OPENBLAS_MD5 "8a110a25b819a4b94e8a9580702b6495")

if (DEFINED ENV{MILVUS_PROMETHEUS_URL})
    set(PROMETHEUS_SOURCE_URL "$ENV{PROMETHEUS_OPENBLAS_URL}")
else ()
    set(PROMETHEUS_SOURCE_URL
            https://github.com/jupp0r/prometheus-cpp.git)
endif()


if (DEFINED ENV{MILVUS_ROCKSDB_URL})
    set(ROCKSDB_SOURCE_URL "$ENV{MILVUS_ROCKSDB_URL}")
else ()
    set(ROCKSDB_SOURCE_URL
            "https://github.com/facebook/rocksdb/archive/${ROCKSDB_VERSION}.tar.gz")
endif()

if(DEFINED ENV{MILVUS_SNAPPY_URL})
    set(SNAPPY_SOURCE_URL "$ENV{MILVUS_SNAPPY_URL}")
else()
    set(SNAPPY_SOURCE_URL
            "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz")
endif()
set(SNAPPY_MD5 "ee9086291c9ae8deb4dac5e0b85bf54a")

if(DEFINED ENV{MILVUS_SQLITE_URL})
    set(SQLITE_SOURCE_URL "$ENV{MILVUS_SQLITE_URL}")
else()
    set(SQLITE_SOURCE_URL
            "https://www.sqlite.org/2019/sqlite-autoconf-${SQLITE_VERSION}.tar.gz")
endif()
set(SQLITE_MD5 "3c68eb400f8354605736cd55400e1572")

if(DEFINED ENV{MILVUS_SQLITE_ORM_URL})
    set(SQLITE_ORM_SOURCE_URL "$ENV{MILVUS_SQLITE_ORM_URL}")
else()
    set(SQLITE_ORM_SOURCE_URL
            "https://github.com/fnc12/sqlite_orm/archive/${SQLITE_ORM_VERSION}.zip")
endif()
set(SQLITE_ORM_MD5 "ba9a405a8a1421c093aa8ce988ff8598")

if(DEFINED ENV{MILVUS_THRIFT_URL})
    set(THRIFT_SOURCE_URL "$ENV{MILVUS_THRIFT_URL}")
else()
    set(THRIFT_SOURCE_URL
            "https://github.com/apache/thrift/archive/${THRIFT_VERSION}.tar.gz")
endif()
set(THRIFT_MD5 "ff9af01fec424b5a279fa8a3c9e95c0c")

if(DEFINED ENV{MILVUS_YAMLCPP_URL})
    set(YAMLCPP_SOURCE_URL "$ENV{MILVUS_YAMLCPP_URL}")
else()
    set(YAMLCPP_SOURCE_URL "https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-${YAMLCPP_VERSION}.tar.gz")
endif()
set(YAMLCPP_MD5 "5b943e9af0060d0811148b037449ef82")

if(DEFINED ENV{MILVUS_ZLIB_URL})
    set(ZLIB_SOURCE_URL "$ENV{MILVUS_ZLIB_URL}")
else()
    set(ZLIB_SOURCE_URL "https://github.com/madler/zlib/archive/${ZLIB_VERSION}.tar.gz")
endif()
set(ZLIB_MD5 "0095d2d2d1f3442ce1318336637b695f")

if(DEFINED ENV{MILVUS_ZSTD_URL})
    set(ZSTD_SOURCE_URL "$ENV{MILVUS_ZSTD_URL}")
else()
    set(ZSTD_SOURCE_URL "https://github.com/facebook/zstd/archive/${ZSTD_VERSION}.tar.gz")
endif()
set(ZSTD_MD5 "340c837db48354f8d5eafe74c6077120")

if(DEFINED ENV{MILVUS_AWS_URL})
    set(AWS_SOURCE_URL "$ENV{MILVUS_AWS_URL}")
else()
    set(AWS_SOURCE_URL "https://github.com/aws/aws-sdk-cpp/archive/${AWS_VERSION}.tar.gz")
endif()
set(AWS_MD5 "9217f5bc8bf23dea04f4466521c85fd9")

if(DEFINED ENV{MILVUS_LIBUNWIND_URL})
    set(LIBUNWIND_SOURCE_URL "$ENV{MILVUS_LIBUNWIND_URL}")
else()
    set(LIBUNWIND_SOURCE_URL
            "https://github.com/libunwind/libunwind/releases/download/v${LIBUNWIND_VERSION}/libunwind-${LIBUNWIND_VERSION}.tar.gz")
endif()

if(DEFINED ENV{MILVUS_GPERFTOOLS_URL})
    set(GPERFTOOLS_SOURCE_URL "$ENV{MILVUS_GPERFTOOLS_URL}")
else()
    set(GPERFTOOLS_SOURCE_URL
            "https://github.com/gperftools/gperftools/releases/download/gperftools-${GPERFTOOLS_VERSION}/gperftools-${GPERFTOOLS_VERSION}.tar.gz")
endif()

# ----------------------------------------------------------------------
# ARROW

macro(build_arrow)
    message(STATUS "Building Apache ARROW-${ARROW_VERSION} from source")
    set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")
    set(ARROW_STATIC_LIB_NAME arrow)

    set(ARROW_STATIC_LIB
            "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
    set(ARROW_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DARROW_BUILD_STATIC=ON
            -DARROW_BUILD_SHARED=OFF
            -DARROW_PARQUET=ON
            -DARROW_USE_GLOG=OFF
            -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
            "-DCMAKE_LIBRARY_PATH=${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs"
            -DCMAKE_BUILD_TYPE=Release)

    if(USE_JFROG_CACHE STREQUAL "ON")
        execute_process(COMMAND git ls-remote --heads ${ARROW_SOURCE_URL} ${ARROW_VERSION} OUTPUT_VARIABLE ARROW_LAST_COMMIT_ID)
        if(${ARROW_LAST_COMMIT_ID} MATCHES "^[0-9a-z]+$")
            string(MD5 ARROW_COMBINE_MD5 "${ARROW_LAST_COMMIT_ID}")
            set(ARROW_CACHE_PACKAGE_NAME "arrow_${ARROW_COMBINE_MD5}.tar.gz")
            set(ARROW_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${ARROW_CACHE_PACKAGE_NAME}")
            set(ARROW_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${ARROW_CACHE_PACKAGE_NAME}")

            file(DOWNLOAD ${ARROW_CACHE_URL} ${ARROW_CACHE_PACKAGE_PATH} STATUS status)
            list(GET status 0 status_code)
            message(STATUS "DOWNLOADING FROM ${ARROW_CACHE_URL} TO ${ARROW_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
            if (NOT status_code EQUAL 0)
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

                ExternalProject_Create_Cache(arrow_ep ${ARROW_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${ARROW_CACHE_URL})
            else()
                ExternalProject_Use_Cache(arrow_ep ${ARROW_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
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

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    add_library(arrow STATIC IMPORTED)
    set_target_properties(arrow
            PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}")

    add_dependencies(arrow arrow_ep)

    set(JEMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep-build/jemalloc_ep-prefix/src/jemalloc_ep")

    add_custom_command(TARGET arrow_ep POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory ${ARROW_PREFIX}/lib/
            COMMAND ${CMAKE_COMMAND} -E copy ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a ${ARROW_PREFIX}/lib/
            DEPENDS ${JEMALLOC_PREFIX}/lib/libjemalloc_pic.a)

endmacro()

if(MILVUS_WITH_ARROW)

    resolve_dependency(ARROW)

    link_directories(SYSTEM ${ARROW_PREFIX}/lib/)
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# Add Boost dependencies (code adapted from Apache Kudu (incubating))

set(Boost_USE_MULTITHREADED ON)
set(Boost_ADDITIONAL_VERSIONS
        "1.70.0"
        "1.70"
        "1.69.0"
        "1.69"
        "1.68.0"
        "1.68"
        "1.67.0"
        "1.67"
        "1.66.0"
        "1.66"
        "1.65.0"
        "1.65"
        "1.64.0"
        "1.64"
        "1.63.0"
        "1.63"
        "1.62.0"
        "1.61"
        "1.61.0"
        "1.62"
        "1.60.0"
        "1.60")

if(MILVUS_BOOST_VENDORED)
    set(BOOST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/boost_ep-prefix/src/boost_ep")
    set(BOOST_LIB_DIR "${BOOST_PREFIX}/stage/lib")
    set(BOOST_BUILD_LINK "static")
    set(BOOST_STATIC_SYSTEM_LIBRARY
            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_system${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(BOOST_STATIC_FILESYSTEM_LIBRARY
            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_filesystem${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(BOOST_STATIC_SERIALIZATION_LIBRARY
            "${BOOST_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}boost_serialization${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    set(BOOST_SYSTEM_LIBRARY boost_system_static)
    set(BOOST_FILESYSTEM_LIBRARY boost_filesystem_static)
    set(BOOST_SERIALIZATION_LIBRARY boost_serialization_static)

    if(MILVUS_BOOST_HEADER_ONLY)
        set(BOOST_BUILD_PRODUCTS)
        set(BOOST_CONFIGURE_COMMAND "")
        set(BOOST_BUILD_COMMAND "")
    else()
        set(BOOST_BUILD_PRODUCTS ${BOOST_STATIC_SYSTEM_LIBRARY}
                ${BOOST_STATIC_FILESYSTEM_LIBRARY} ${BOOST_STATIC_SERIALIZATION_LIBRARY})
        set(BOOST_CONFIGURE_COMMAND "./bootstrap.sh" "--prefix=${BOOST_PREFIX}"
                "--with-libraries=filesystem,serialization,system")
        if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
            set(BOOST_BUILD_VARIANT "debug")
        else()
            set(BOOST_BUILD_VARIANT "release")
        endif()
        set(BOOST_BUILD_COMMAND
                "./b2"
                "link=${BOOST_BUILD_LINK}"
                "variant=${BOOST_BUILD_VARIANT}"
                "cxxflags=-fPIC")

        add_thirdparty_lib(boost_system STATIC_LIB "${BOOST_STATIC_SYSTEM_LIBRARY}")

        add_thirdparty_lib(boost_filesystem STATIC_LIB "${BOOST_STATIC_FILESYSTEM_LIBRARY}")

        add_thirdparty_lib(boost_serialization STATIC_LIB "${BOOST_STATIC_SERIALIZATION_LIBRARY}")

        set(MILVUS_BOOST_LIBS ${BOOST_SYSTEM_LIBRARY} ${BOOST_FILESYSTEM_LIBRARY} ${BOOST_STATIC_SERIALIZATION_LIBRARY})
    endif()
    externalproject_add(boost_ep
            URL
            ${BOOST_SOURCE_URL}
            BUILD_BYPRODUCTS
            ${BOOST_BUILD_PRODUCTS}
            BUILD_IN_SOURCE
            1
            CONFIGURE_COMMAND
            ${BOOST_CONFIGURE_COMMAND}
            BUILD_COMMAND
            ${BOOST_BUILD_COMMAND}
            INSTALL_COMMAND
            ""
            ${EP_LOG_OPTIONS})


    set(Boost_INCLUDE_DIR "${BOOST_PREFIX}")
    set(Boost_INCLUDE_DIRS "${Boost_INCLUDE_DIR}")
    add_dependencies(boost_system_static boost_ep)
    add_dependencies(boost_filesystem_static boost_ep)
    add_dependencies(boost_serialization_static boost_ep)

endif()

include_directories(SYSTEM ${Boost_INCLUDE_DIR})
link_directories(SYSTEM ${BOOST_LIB_DIR})

# ----------------------------------------------------------------------
# bzip2

macro(build_bzip2)
    message(STATUS "Building BZip2-${BZIP2_VERSION} from source")
    set(BZIP2_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/bzip2_ep-prefix/src/bzip2_ep")
    set(BZIP2_INCLUDE_DIR "${BZIP2_PREFIX}/include")
    set(BZIP2_STATIC_LIB
            "${BZIP2_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}bz2${CMAKE_STATIC_LIBRARY_SUFFIX}")

    if(USE_JFROG_CACHE STREQUAL "ON") 
        set(BZIP2_CACHE_PACKAGE_NAME "bzip2_${BZIP2_MD5}.tar.gz")
        set(BZIP2_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${BZIP2_CACHE_PACKAGE_NAME}")
        set(BZIP2_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${BZIP2_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${BZIP2_CACHE_URL} ${BZIP2_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${BZIP2_CACHE_URL} TO ${BZIP2_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(bzip2_ep
                                ${EP_LOG_OPTIONS}
                                CONFIGURE_COMMAND
                                ""
                                BUILD_IN_SOURCE
                                1
                                BUILD_COMMAND
                                ${MAKE}
                                ${MAKE_BUILD_ARGS}
                                CFLAGS=${EP_C_FLAGS}
                                INSTALL_COMMAND
                                ${MAKE}
                                install
                                PREFIX=${BZIP2_PREFIX}
                                CFLAGS=${EP_C_FLAGS}
                                INSTALL_DIR
                                ${BZIP2_PREFIX}
                                URL
                                ${BZIP2_SOURCE_URL}
                                BUILD_BYPRODUCTS
                                "${BZIP2_STATIC_LIB}")

            ExternalProject_Create_Cache(bzip2_ep ${BZIP2_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/bzip2_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${BZIP2_CACHE_URL})
        else()
            ExternalProject_Use_Cache(bzip2_ep ${BZIP2_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(bzip2_ep
                                ${EP_LOG_OPTIONS}
                                CONFIGURE_COMMAND
                                ""
                                BUILD_IN_SOURCE
                                1
                                BUILD_COMMAND
                                ${MAKE}
                                ${MAKE_BUILD_ARGS}
                                CFLAGS=${EP_C_FLAGS}
                                INSTALL_COMMAND
                                ${MAKE}
                                install
                                PREFIX=${BZIP2_PREFIX}
                                CFLAGS=${EP_C_FLAGS}
                                INSTALL_DIR
                                ${BZIP2_PREFIX}
                                URL
                                ${BZIP2_SOURCE_URL}
                                BUILD_BYPRODUCTS
                                "${BZIP2_STATIC_LIB}")
    endif()

    file(MAKE_DIRECTORY "${BZIP2_INCLUDE_DIR}")
    add_library(bzip2 STATIC IMPORTED)
    set_target_properties(
            bzip2
            PROPERTIES IMPORTED_LOCATION "${BZIP2_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${BZIP2_INCLUDE_DIR}")

    add_dependencies(bzip2 bzip2_ep)
endmacro()

if(MILVUS_WITH_BZ2)
    resolve_dependency(BZip2)

    if(NOT TARGET bzip2)
        add_library(bzip2 UNKNOWN IMPORTED)
        set_target_properties(bzip2
                PROPERTIES IMPORTED_LOCATION "${BZIP2_LIBRARIES}"
                INTERFACE_INCLUDE_DIRECTORIES "${BZIP2_INCLUDE_DIR}")
    endif()
    link_directories(SYSTEM ${BZIP2_PREFIX}/lib/)
    include_directories(SYSTEM "${BZIP2_INCLUDE_DIR}")
endif()

# ----------------------------------------------------------------------
# Knowhere

macro(build_knowhere)
    message(STATUS "Building knowhere from source")
    set(KNOWHERE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/knowhere_ep-prefix/src/knowhere_ep")
    set(KNOWHERE_INCLUDE_DIR "${KNOWHERE_PREFIX}/include")
    set(KNOWHERE_STATIC_LIB
            "${KNOWHERE_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}knowhere${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(KNOWHERE_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${KNOWHERE_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib
            "-DCMAKE_CUDA_COMPILER=${CMAKE_CUDA_COMPILER}"
            "-DCUDA_TOOLKIT_ROOT_DIR=${CUDA_TOOLKIT_ROOT_DIR}"
            -DCMAKE_BUILD_TYPE=Release)

    externalproject_add(knowhere_ep
            URL
            ${KNOWHERE_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CMAKE_ARGS
            ${KNOWHERE_CMAKE_ARGS}
            BUILD_COMMAND
            ${MAKE}
            ${MAKE_BUILD_ARGS}
            BUILD_BYPRODUCTS
            ${KNOWHERE_STATIC_LIB})

    file(MAKE_DIRECTORY "${KNOWHERE_INCLUDE_DIR}")
    add_library(knowhere STATIC IMPORTED)
    set_target_properties(
            knowhere
            PROPERTIES IMPORTED_LOCATION "${KNOWHERE_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${KNOWHERE_INCLUDE_DIR}")

    add_dependencies(knowhere knowhere_ep)
endmacro()

if(MILVUS_WITH_KNOWHERE)
    resolve_dependency(Knowhere)

    get_target_property(KNOWHERE_INCLUDE_DIR knowhere INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM "${KNOWHERE_PREFIX}/lib")
    include_directories(SYSTEM "${KNOWHERE_INCLUDE_DIR}")
    include_directories(SYSTEM "${KNOWHERE_INCLUDE_DIR}/SPTAG/AnnService")
endif()

# ----------------------------------------------------------------------
# Easylogging++

macro(build_easyloggingpp)
    message(STATUS "Building Easylogging++-${EASYLOGGINGPP_VERSION} from source")
    set(EASYLOGGINGPP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/easyloggingpp_ep-prefix/src/easyloggingpp_ep")
    set(EASYLOGGINGPP_INCLUDE_DIR "${EASYLOGGINGPP_PREFIX}/include")
    set(EASYLOGGINGPP_STATIC_LIB
            "${EASYLOGGINGPP_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}easyloggingpp${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(EASYLOGGINGPP_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${EASYLOGGINGPP_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib
            -Dtest=OFF
            -Dbuild_static_lib=ON)

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(EASYLOGGINGPP_CACHE_PACKAGE_NAME "easyloggingpp_${EASYLOGGINGPP_MD5}.tar.gz")
        set(EASYLOGGINGPP_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${EASYLOGGINGPP_CACHE_PACKAGE_NAME}")
        set(EASYLOGGINGPP_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${EASYLOGGINGPP_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${EASYLOGGINGPP_CACHE_URL} ${EASYLOGGINGPP_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${EASYLOGGINGPP_CACHE_URL} TO ${EASYLOGGINGPP_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(easyloggingpp_ep
                URL
                ${EASYLOGGINGPP_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CMAKE_ARGS
                ${EASYLOGGINGPP_CMAKE_ARGS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_BYPRODUCTS
                ${EASYLOGGINGPP_STATIC_LIB})

            ExternalProject_Create_Cache(easyloggingpp_ep ${EASYLOGGINGPP_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/easyloggingpp_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${EASYLOGGINGPP_CACHE_URL})
        else()
            ExternalProject_Use_Cache(easyloggingpp_ep ${EASYLOGGINGPP_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(easyloggingpp_ep
                URL
                ${EASYLOGGINGPP_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CMAKE_ARGS
                ${EASYLOGGINGPP_CMAKE_ARGS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_BYPRODUCTS
                ${EASYLOGGINGPP_STATIC_LIB})
    endif()

    file(MAKE_DIRECTORY "${EASYLOGGINGPP_INCLUDE_DIR}")
    add_library(easyloggingpp STATIC IMPORTED)
    set_target_properties(
            easyloggingpp
            PROPERTIES IMPORTED_LOCATION "${EASYLOGGINGPP_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${EASYLOGGINGPP_INCLUDE_DIR}")

    add_dependencies(easyloggingpp easyloggingpp_ep)
endmacro()

if(MILVUS_WITH_EASYLOGGINGPP)
    resolve_dependency(Easylogging++)

    get_target_property(EASYLOGGINGPP_INCLUDE_DIR easyloggingpp INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM "${EASYLOGGINGPP_PREFIX}/lib")
    include_directories(SYSTEM "${EASYLOGGINGPP_INCLUDE_DIR}")
endif()

# ----------------------------------------------------------------------
# OpenBLAS

macro(build_openblas)
    message(STATUS "Building OpenBLAS-${OPENBLAS_VERSION} from source")
    set(OPENBLAS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/openblas_ep-prefix/src/openblas_ep")
    set(OPENBLAS_INCLUDE_DIR "${OPENBLAS_PREFIX}/include")
    set(OPENBLAS_STATIC_LIB
            "${OPENBLAS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}openblas${CMAKE_STATIC_LIBRARY_SUFFIX}")

        if(USE_JFROG_CACHE STREQUAL "ON")
        set(OPENBLAS_CACHE_PACKAGE_NAME "openblas_${OPENBLAS_MD5}.tar.gz")
        set(OPENBLAS_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${OPENBLAS_CACHE_PACKAGE_NAME}")
        set(OPENBLAS_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${OPENBLAS_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${OPENBLAS_CACHE_URL} ${OPENBLAS_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${OPENBLAS_CACHE_URL} TO ${OPENBLAS_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
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

            ExternalProject_Create_Cache(openblas_ep ${OPENBLAS_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/openblas_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${OPENBLAS_CACHE_URL})

            file(MAKE_DIRECTORY "${OPENBLAS_INCLUDE_DIR}")
        else()
            ExternalProject_Use_Cache(openblas_ep ${OPENBLAS_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
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

        file(MAKE_DIRECTORY "${OPENBLAS_INCLUDE_DIR}")
    endif()

    add_library(openblas STATIC IMPORTED)
    set_target_properties(
            openblas
            PROPERTIES IMPORTED_LOCATION "${OPENBLAS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${OPENBLAS_INCLUDE_DIR}")

    add_dependencies(openblas openblas_ep)
endmacro()

# ----------------------------------------------------------------------
# LAPACK

macro(build_lapack)
    message(STATUS "Building LAPACK-${LAPACK_VERSION} from source")
    set(LAPACK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lapack_ep-prefix/src/lapack_ep")
    set(LAPACK_INCLUDE_DIR "${LAPACK_PREFIX}/include")
    set(LAPACK_STATIC_LIB
            "${LAPACK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lapack${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(LAPACK_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${LAPACK_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib)

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(LAPACK_CACHE_PACKAGE_NAME "lapack_${LAPACK_MD5}.tar.gz")
        set(LAPACK_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${LAPACK_CACHE_PACKAGE_NAME}")
        set(LAPACK_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${LAPACK_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${LAPACK_CACHE_URL} ${LAPACK_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${LAPACK_CACHE_URL} TO ${LAPACK_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
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

            ExternalProject_Create_Cache(lapack_ep ${LAPACK_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/lapack_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${LAPACK_CACHE_URL})
        else()
            ExternalProject_Use_Cache(lapack_ep ${LAPACK_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
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

# ----------------------------------------------------------------------
# FAISS

if(NOT DEFINED BUILD_FAISS_WITH_MKL)
    set(BUILD_FAISS_WITH_MKL OFF)
endif()

if(EXISTS "/proc/cpuinfo")
    FILE(READ /proc/cpuinfo PROC_CPUINFO)

    SET(VENDOR_ID_RX "vendor_id[ \t]*:[ \t]*([a-zA-Z]+)\n")
    STRING(REGEX MATCH "${VENDOR_ID_RX}" VENDOR_ID "${PROC_CPUINFO}")
    STRING(REGEX REPLACE "${VENDOR_ID_RX}" "\\1" VENDOR_ID "${VENDOR_ID}")

    if(NOT ${VENDOR_ID} STREQUAL "GenuineIntel")
        set(BUILD_FAISS_WITH_MKL OFF)
    endif()
endif()

macro(build_faiss)
    message(STATUS "Building FAISS-${FAISS_VERSION} from source")
    set(FAISS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/faiss_ep-prefix/src/faiss_ep")
    set(FAISS_INCLUDE_DIR "${FAISS_PREFIX}/include")
    set(FAISS_STATIC_LIB
            "${FAISS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}faiss${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(FAISS_CONFIGURE_ARGS
            "--prefix=${FAISS_PREFIX}"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS}"
            --without-python)

    set(FAISS_CFLAGS ${EP_C_FLAGS})
    set(FAISS_CXXFLAGS ${EP_CXX_FLAGS})

    if(${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
        message(STATUS "Build Faiss with MKL")
        if(NOT DEFINED MKL_LIB_PATH)
            set(MKL_LIB_PATH "/opt/intel/compilers_and_libraries_${MKL_VERSION}/linux/mkl/lib/intel64")
            message(STATUS "MKL_LIB_PATH = ${MKL_LIB_PATH}")
        endif()

        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "CPPFLAGS=-DFINTEGER=long -DMKL_ILP64 -m64 -I${MKL_LIB_PATH}/../../include"
                "LDFLAGS=-L${MKL_LIB_PATH}"
                "LIBS=-Wl,--start-group ${MKL_LIB_PATH}/libmkl_intel_ilp64.a ${MKL_LIB_PATH}/libmkl_gnu_thread.a ${MKL_LIB_PATH}/libmkl_core.a -Wl,--end-group -lgomp -lpthread -lm -ldl")

    else()
        message(STATUS "Build Faiss with OpenBlas/LAPACK")
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "LDFLAGS=-L${OPENBLAS_PREFIX}/lib -L${LAPACK_PREFIX}/lib")
    endif()

    if(${MILVUS_WITH_FAISS_GPU_VERSION} STREQUAL "ON")
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS}
                "--with-cuda=${CUDA_TOOLKIT_ROOT_DIR}"
                "--with-cuda-arch=-gencode=arch=compute_60,code=sm_60 -gencode=arch=compute_61,code=sm_61 -gencode=arch=compute_75,code=sm_75"
                )
    else()
        set(FAISS_CONFIGURE_ARGS ${FAISS_CONFIGURE_ARGS} --without-cuda)
    endif()

    if(USE_JFROG_CACHE STREQUAL "ON")
        string(MD5 FAISS_COMBINE_MD5 "${FAISS_MD5}${LAPACK_MD5}${OPENBLAS_MD5}")
        set(FAISS_CACHE_PACKAGE_NAME "faiss_${FAISS_COMBINE_MD5}.tar.gz")
        set(FAISS_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${FAISS_CACHE_PACKAGE_NAME}")
        set(FAISS_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${FAISS_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${FAISS_CACHE_URL} ${FAISS_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${FAISS_CACHE_URL} TO ${FAISS_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(faiss_ep
                    URL
                    ${FAISS_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    CONFIGURE_COMMAND
                    "./configure"
                    ${FAISS_CONFIGURE_ARGS}
                    BUILD_COMMAND
                    ${MAKE} ${MAKE_BUILD_ARGS} VERBOSE=1
                    BUILD_IN_SOURCE
                    1
                    INSTALL_COMMAND
                    ${MAKE} install
                    BUILD_BYPRODUCTS
                    ${FAISS_STATIC_LIB})

            if(${BUILD_FAISS_WITH_MKL} STREQUAL "OFF")
                ExternalProject_Add_StepDependencies(faiss_ep build openblas_ep lapack_ep)
            endif()

            ExternalProject_Create_Cache(faiss_ep ${FAISS_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/faiss_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${FAISS_CACHE_URL})
        else()
            ExternalProject_Use_Cache(faiss_ep ${FAISS_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
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
                ${MAKE} ${MAKE_BUILD_ARGS} VERBOSE=1
                BUILD_IN_SOURCE
                1
                INSTALL_COMMAND
                ${MAKE} install
                BUILD_BYPRODUCTS
                ${FAISS_STATIC_LIB})

        if(${BUILD_FAISS_WITH_MKL} STREQUAL "OFF")
            ExternalProject_Add_StepDependencies(faiss_ep build openblas_ep lapack_ep)
        endif()
    endif()

    file(MAKE_DIRECTORY "${FAISS_INCLUDE_DIR}")
    add_library(faiss SHARED IMPORTED)

    if(${BUILD_FAISS_WITH_MKL} STREQUAL "ON")
        set(MKL_LIBS ${MKL_LIB_PATH}/libmkl_intel_ilp64.a
                     ${MKL_LIB_PATH}/libmkl_gnu_thread.a
                     ${MKL_LIB_PATH}/libmkl_core.a)

        set_target_properties(
                faiss
                PROPERTIES IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
                INTERFACE_LINK_LIBRARIES "${MKL_LIBS}" )
    else()
        set_target_properties(
                faiss
                PROPERTIES IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
                INTERFACE_LINK_LIBRARIES "openblas;lapack" )
    endif()
            
    add_dependencies(faiss faiss_ep)

    if(${BUILD_FAISS_WITH_MKL} STREQUAL "OFF")
        add_dependencies(faiss openblas_ep)
        add_dependencies(faiss lapack_ep)
    endif()

endmacro()

if(MILVUS_WITH_FAISS)

    if(${BUILD_FAISS_WITH_MKL} STREQUAL "OFF")
        resolve_dependency(OpenBLAS)
        get_target_property(OPENBLAS_INCLUDE_DIR openblas INTERFACE_INCLUDE_DIRECTORIES)
        include_directories(SYSTEM "${OPENBLAS_INCLUDE_DIR}")
        link_directories(SYSTEM ${OPENBLAS_PREFIX}/lib)

        resolve_dependency(LAPACK)
        get_target_property(LAPACK_INCLUDE_DIR lapack INTERFACE_INCLUDE_DIRECTORIES)
        include_directories(SYSTEM "${LAPACK_INCLUDE_DIR}")
        link_directories(SYSTEM "${LAPACK_PREFIX}/lib")
    endif()

    resolve_dependency(FAISS)
    get_target_property(FAISS_INCLUDE_DIR faiss INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${FAISS_INCLUDE_DIR}")
    link_directories(SYSTEM ${FAISS_PREFIX}/lib/)
endif()

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

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
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

if (MILVUS_BUILD_TESTS)
    resolve_dependency(GTest)

    if(NOT GTEST_VENDORED)
    endif()
    
    get_target_property(GTEST_INCLUDE_DIR gtest INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM "${GTEST_PREFIX}/lib")
    include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# JSONCONS

macro(build_jsoncons)
    message(STATUS "Building JSONCONS-${JSONCONS_VERSION} from source")

    set(JSONCONS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jsoncons_ep-prefix")
    set(JSONCONS_TAR_NAME "${JSONCONS_PREFIX}/jsoncons-${JSONCONS_VERSION}.tar.gz")
    set(JSONCONS_INCLUDE_DIR "${JSONCONS_PREFIX}/jsoncons-${JSONCONS_VERSION}/include")
    if (NOT EXISTS ${JSONCONS_INCLUDE_DIR})
        file(MAKE_DIRECTORY ${JSONCONS_PREFIX})
        file(DOWNLOAD ${JSONCONS_SOURCE_URL}
                ${JSONCONS_TAR_NAME})
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar -xf ${JSONCONS_TAR_NAME}
                WORKING_DIRECTORY ${JSONCONS_PREFIX})

    endif ()
endmacro()

if(MILVUS_WITH_JSONCONS)
    resolve_dependency(JSONCONS)
    include_directories(SYSTEM "${JSONCONS_INCLUDE_DIR}")
endif()

# ----------------------------------------------------------------------
# lz4

macro(build_lz4)
    message(STATUS "Building lz4-${LZ4_VERSION} from source")
    set(LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
    set(LZ4_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/")

    set(LZ4_STATIC_LIB "${LZ4_BUILD_DIR}/lib/liblz4.a")
    set(LZ4_BUILD_COMMAND BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS} CFLAGS=${EP_C_FLAGS})

    # We need to copy the header in lib to directory outside of the build
    if(USE_JFROG_CACHE STREQUAL "ON")
        set(LZ4_CACHE_PACKAGE_NAME "lz4_${LZ4_MD5}.tar.gz")
        set(LZ4_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${LZ4_CACHE_PACKAGE_NAME}")
        set(LZ4_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${LZ4_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${LZ4_CACHE_URL} ${LZ4_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${LZ4_CACHE_URL} TO ${LZ4_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(lz4_ep
                    URL
                    ${LZ4_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    UPDATE_COMMAND
                    ${CMAKE_COMMAND}
                    -E
                    copy_directory
                    "${LZ4_BUILD_DIR}/lib"
                    "${LZ4_PREFIX}/include"
                    ${LZ4_PATCH_COMMAND}
                    CONFIGURE_COMMAND
                    ""
                    INSTALL_COMMAND
                    ""
                    BINARY_DIR
                    ${LZ4_BUILD_DIR}
                    BUILD_BYPRODUCTS
                    ${LZ4_STATIC_LIB}
                    ${LZ4_BUILD_COMMAND})

            ExternalProject_Create_Cache(lz4_ep ${LZ4_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${LZ4_CACHE_URL})
        else()
            ExternalProject_Use_Cache(lz4_ep ${LZ4_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(lz4_ep
                URL
                ${LZ4_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                UPDATE_COMMAND
                ${CMAKE_COMMAND}
                -E
                copy_directory
                "${LZ4_BUILD_DIR}/lib"
                "${LZ4_PREFIX}/include"
                ${LZ4_PATCH_COMMAND}
                CONFIGURE_COMMAND
                ""
                INSTALL_COMMAND
                ""
                BINARY_DIR
                ${LZ4_BUILD_DIR}
                BUILD_BYPRODUCTS
                ${LZ4_STATIC_LIB}
                ${LZ4_BUILD_COMMAND})
    endif()

    file(MAKE_DIRECTORY "${LZ4_PREFIX}/include")
    add_library(lz4 STATIC IMPORTED)
    set_target_properties(lz4
            PROPERTIES IMPORTED_LOCATION "${LZ4_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${LZ4_PREFIX}/include")
    add_dependencies(lz4 lz4_ep)
endmacro()

if(MILVUS_WITH_LZ4)
    resolve_dependency(Lz4)

    get_target_property(LZ4_INCLUDE_DIR lz4 INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM ${LZ4_BUILD_DIR}/lib/)
    include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# MySQL++

macro(build_mysqlpp)
    message(STATUS "Building MySQL++-${MYSQLPP_VERSION} from source")
    set(MYSQLPP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep")
    set(MYSQLPP_INCLUDE_DIR "${MYSQLPP_PREFIX}/include")
    set(MYSQLPP_SHARED_LIB
            "${MYSQLPP_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}")

    set(MYSQLPP_CONFIGURE_ARGS
            "--prefix=${MYSQLPP_PREFIX}"
            "--enable-thread-check"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS}"
            "LDFLAGS=-pthread")

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(MYSQLPP_CACHE_PACKAGE_NAME "mysqlpp_${MYSQLPP_MD5}.tar.gz")
        set(MYSQLPP_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${MYSQLPP_CACHE_PACKAGE_NAME}")
        set(MYSQLPP_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${MYSQLPP_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${MYSQLPP_CACHE_URL} ${MYSQLPP_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${MYSQLPP_CACHE_URL} TO ${MYSQLPP_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(mysqlpp_ep
                    URL
                    ${MYSQLPP_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    CONFIGURE_COMMAND
                    "./configure"
                    ${MYSQLPP_CONFIGURE_ARGS}
                    BUILD_COMMAND
                    ${MAKE} ${MAKE_BUILD_ARGS}
                    BUILD_IN_SOURCE
                    1
                    BUILD_BYPRODUCTS
                    ${MYSQLPP_SHARED_LIB})

            ExternalProject_Create_Cache(mysqlpp_ep ${MYSQLPP_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/mysqlpp_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${MYSQLPP_CACHE_URL})
        else()
            ExternalProject_Use_Cache(mysqlpp_ep ${MYSQLPP_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(mysqlpp_ep
                URL
                ${MYSQLPP_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${MYSQLPP_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE} ${MAKE_BUILD_ARGS}
                BUILD_IN_SOURCE
                1
                BUILD_BYPRODUCTS
                ${MYSQLPP_SHARED_LIB})
    endif()

    file(MAKE_DIRECTORY "${MYSQLPP_INCLUDE_DIR}")
    add_library(mysqlpp SHARED IMPORTED)
    set_target_properties(
            mysqlpp
            PROPERTIES
            IMPORTED_LOCATION "${MYSQLPP_SHARED_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${MYSQLPP_INCLUDE_DIR}")

    add_dependencies(mysqlpp mysqlpp_ep)

endmacro()

if(MILVUS_WITH_MYSQLPP)

    resolve_dependency(MySQLPP)
    get_target_property(MYSQLPP_INCLUDE_DIR mysqlpp INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM "${MYSQLPP_INCLUDE_DIR}")
    link_directories(SYSTEM ${MYSQLPP_PREFIX}/lib)
endif()

# ----------------------------------------------------------------------
# Prometheus

macro(build_prometheus)
    message(STATUS "Building Prometheus-${PROMETHEUS_VERSION} from source")
    set(PROMETHEUS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/prometheus_ep-prefix/src/prometheus_ep")
    set(PROMETHEUS_STATIC_LIB_NAME prometheus-cpp)
    set(PROMETHEUS_CORE_STATIC_LIB
            "${PROMETHEUS_PREFIX}/core/${CMAKE_STATIC_LIBRARY_PREFIX}${PROMETHEUS_STATIC_LIB_NAME}-core${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
    set(PROMETHEUS_PUSH_STATIC_LIB
            "${PROMETHEUS_PREFIX}/push/${CMAKE_STATIC_LIBRARY_PREFIX}${PROMETHEUS_STATIC_LIB_NAME}-push${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )
    set(PROMETHEUS_PULL_STATIC_LIB
            "${PROMETHEUS_PREFIX}/pull/${CMAKE_STATIC_LIBRARY_PREFIX}${PROMETHEUS_STATIC_LIB_NAME}-pull${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )

    set(PROMETHEUS_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_INSTALL_LIBDIR=lib
            -DBUILD_SHARED_LIBS=OFF
            "-DCMAKE_INSTALL_PREFIX=${PROMETHEUS_PREFIX}"
            -DCMAKE_BUILD_TYPE=Release)

    externalproject_add(prometheus_ep
            GIT_REPOSITORY
            ${PROMETHEUS_SOURCE_URL}
            GIT_TAG
            ${PROMETHEUS_VERSION}
            GIT_SHALLOW
            TRUE
            ${EP_LOG_OPTIONS}
            CMAKE_ARGS
            ${PROMETHEUS_CMAKE_ARGS}
            BUILD_COMMAND
            ${MAKE}
            ${MAKE_BUILD_ARGS}
            BUILD_IN_SOURCE
            1
            INSTALL_COMMAND
            ${MAKE}
            "DESTDIR=${PROMETHEUS_PREFIX}"
            install
            BUILD_BYPRODUCTS
            "${PROMETHEUS_CORE_STATIC_LIB}"
            "${PROMETHEUS_PUSH_STATIC_LIB}"
            "${PROMETHEUS_PULL_STATIC_LIB}")

    file(MAKE_DIRECTORY "${PROMETHEUS_PREFIX}/push/include")
    add_library(prometheus-cpp-push STATIC IMPORTED)
    set_target_properties(prometheus-cpp-push
            PROPERTIES IMPORTED_LOCATION "${PROMETHEUS_PUSH_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${PROMETHEUS_PREFIX}/push/include")
    add_dependencies(prometheus-cpp-push prometheus_ep)

    file(MAKE_DIRECTORY "${PROMETHEUS_PREFIX}/pull/include")
    add_library(prometheus-cpp-pull STATIC IMPORTED)
    set_target_properties(prometheus-cpp-pull
            PROPERTIES IMPORTED_LOCATION "${PROMETHEUS_PULL_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${PROMETHEUS_PREFIX}/pull/include")
    add_dependencies(prometheus-cpp-pull prometheus_ep)

    file(MAKE_DIRECTORY "${PROMETHEUS_PREFIX}/core/include")
    add_library(prometheus-cpp-core STATIC IMPORTED)
    set_target_properties(prometheus-cpp-core
            PROPERTIES IMPORTED_LOCATION "${PROMETHEUS_CORE_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${PROMETHEUS_PREFIX}/core/include")
    add_dependencies(prometheus-cpp-core prometheus_ep)
endmacro()

if(MILVUS_WITH_PROMETHEUS)

    resolve_dependency(Prometheus)

    link_directories(SYSTEM ${PROMETHEUS_PREFIX}/push/)
    include_directories(SYSTEM ${PROMETHEUS_PREFIX}/push/include)

    link_directories(SYSTEM ${PROMETHEUS_PREFIX}/pull/)
    include_directories(SYSTEM ${PROMETHEUS_PREFIX}/pull/include)

    link_directories(SYSTEM ${PROMETHEUS_PREFIX}/core/)
    include_directories(SYSTEM ${PROMETHEUS_PREFIX}/core/include)

endif()

# ----------------------------------------------------------------------
# RocksDB

macro(build_rocksdb)
    message(STATUS "Building RocksDB-${ROCKSDB_VERSION} from source")
    set(ROCKSDB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/rocksdb_ep-prefix/src/rocksdb_ep")
    set(ROCKSDB_INCLUDE_DIRS "${ROCKSDB_PREFIX}/include")
    set(ROCKSDB_STATIC_LIB_NAME rocksdb)
    set(ROCKSDB_STATIC_LIB
            "${ROCKSDB_PREFIX}/lib/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ROCKSDB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
            )

    externalproject_add(rocksdb_ep
            URL
            ${ROCKSDB_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CONFIGURE_COMMAND
            ""
            BUILD_COMMAND
            ${MAKE}
            ${MAKE_BUILD_ARGS}
            static_lib
            "prefix=${ROCKSDB_PREFIX}"
            BUILD_IN_SOURCE
            1
            INSTALL_COMMAND
            ${MAKE}
            install-static
            "INSTALL_PATH=${ROCKSDB_PREFIX}/lib"
            BUILD_BYPRODUCTS
            "${ROCKSDB_STATIC_LIB}")

    file(MAKE_DIRECTORY "${ROCKSDB_PREFIX}/include")

    add_library(rocksdb STATIC IMPORTED)
    set_target_properties(rocksdb
            PROPERTIES IMPORTED_LOCATION "${ROCKSDB_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIRS}")
    add_dependencies(rocksdb rocksdb_ep)
endmacro()

if(MILVUS_WITH_ROCKSDB)

    resolve_dependency(RocksDB)

    link_directories(SYSTEM ${ROCKSDB_PREFIX}/lib/lib/)
    include_directories(SYSTEM ${ROCKSDB_INCLUDE_DIRS})
endif()

# ----------------------------------------------------------------------
# Snappy

macro(build_snappy)
    message(STATUS "Building snappy-${SNAPPY_VERSION} from source")
    set(SNAPPY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep-prefix/src/snappy_ep")
    set(SNAPPY_INCLUDE_DIRS "${SNAPPY_PREFIX}/include")
    set(SNAPPY_STATIC_LIB_NAME snappy)
    set(SNAPPY_STATIC_LIB
            "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )

    set(SNAPPY_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            -DCMAKE_INSTALL_LIBDIR=lib
            -DSNAPPY_BUILD_TESTS=OFF
            "-DCMAKE_INSTALL_PREFIX=${SNAPPY_PREFIX}")

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(SNAPPY_CACHE_PACKAGE_NAME "snappy_${SNAPPY_MD5}.tar.gz")
        set(SNAPPY_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${SNAPPY_CACHE_PACKAGE_NAME}")
        set(SNAPPY_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${SNAPPY_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${SNAPPY_CACHE_URL} ${SNAPPY_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${SNAPPY_CACHE_URL} TO ${SNAPPY_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(snappy_ep
                    ${EP_LOG_OPTIONS}
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    BUILD_IN_SOURCE
                    1
                    INSTALL_DIR
                    ${SNAPPY_PREFIX}
                    URL
                    ${SNAPPY_SOURCE_URL}
                    CMAKE_ARGS
                    ${SNAPPY_CMAKE_ARGS}
                    BUILD_BYPRODUCTS
                    "${SNAPPY_STATIC_LIB}")

            ExternalProject_Create_Cache(snappy_ep ${SNAPPY_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${SNAPPY_CACHE_URL})
        else()
            ExternalProject_Use_Cache(snappy_ep ${SNAPPY_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(snappy_ep
                ${EP_LOG_OPTIONS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_IN_SOURCE
                1
                INSTALL_DIR
                ${SNAPPY_PREFIX}
                URL
                ${SNAPPY_SOURCE_URL}
                CMAKE_ARGS
                ${SNAPPY_CMAKE_ARGS}
                BUILD_BYPRODUCTS
                "${SNAPPY_STATIC_LIB}")
    endif()

    file(MAKE_DIRECTORY "${SNAPPY_INCLUDE_DIR}")
    add_library(snappy STATIC IMPORTED)
    set_target_properties(snappy
                        PROPERTIES IMPORTED_LOCATION "${SNAPPY_STATIC_LIB}"
                        INTERFACE_INCLUDE_DIRECTORIES
                        "${SNAPPY_INCLUDE_DIR}")
    add_dependencies(snappy snappy_ep)
endmacro()

if(MILVUS_WITH_SNAPPY)

    resolve_dependency(Snappy)
    
    get_target_property(SNAPPY_INCLUDE_DIRS snappy INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM ${SNAPPY_PREFIX}/lib/)
    include_directories(SYSTEM ${SNAPPY_INCLUDE_DIRS})
endif()

# ----------------------------------------------------------------------
# SQLite

macro(build_sqlite)
    message(STATUS "Building SQLITE-${SQLITE_VERSION} from source")
    set(SQLITE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/sqlite_ep-prefix/src/sqlite_ep")
    set(SQLITE_INCLUDE_DIR "${SQLITE_PREFIX}/include")
    set(SQLITE_STATIC_LIB
            "${SQLITE_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}sqlite3${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(SQLITE_CONFIGURE_ARGS
            "--prefix=${SQLITE_PREFIX}"
            "CC=${CMAKE_C_COMPILER}"
            "CXX=${CMAKE_CXX_COMPILER}"
            "CFLAGS=${EP_C_FLAGS}"
            "CXXFLAGS=${EP_CXX_FLAGS}")

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(SQLITE_CACHE_PACKAGE_NAME "sqlite_${SQLITE_MD5}.tar.gz")
        set(SQLITE_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${SQLITE_CACHE_PACKAGE_NAME}")
        set(SQLITE_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${SQLITE_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${SQLITE_CACHE_URL} ${SQLITE_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${SQLITE_CACHE_URL} TO ${SQLITE_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(sqlite_ep
                    URL
                    ${SQLITE_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    CONFIGURE_COMMAND
                    "./configure"
                    ${SQLITE_CONFIGURE_ARGS}
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    BUILD_IN_SOURCE
                    1
                    BUILD_BYPRODUCTS
                    "${SQLITE_STATIC_LIB}")

            ExternalProject_Create_Cache(sqlite_ep ${SQLITE_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/sqlite_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${SQLITE_CACHE_URL})
        else()
            ExternalProject_Use_Cache(sqlite_ep ${SQLITE_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(sqlite_ep
                URL
                ${SQLITE_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                CONFIGURE_COMMAND
                "./configure"
                ${SQLITE_CONFIGURE_ARGS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_IN_SOURCE
                1
                BUILD_BYPRODUCTS
                "${SQLITE_STATIC_LIB}")
    endif()

    file(MAKE_DIRECTORY "${SQLITE_INCLUDE_DIR}")
    add_library(sqlite STATIC IMPORTED)
    set_target_properties(
            sqlite
            PROPERTIES IMPORTED_LOCATION "${SQLITE_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${SQLITE_INCLUDE_DIR}")

    add_dependencies(sqlite sqlite_ep)
endmacro()

if(MILVUS_WITH_SQLITE)
    resolve_dependency(SQLite)
    include_directories(SYSTEM "${SQLITE_INCLUDE_DIR}")
    link_directories(SYSTEM ${SQLITE_PREFIX}/lib/)
endif()

# ----------------------------------------------------------------------
# SQLite_ORM

macro(build_sqlite_orm)
    message(STATUS "Building SQLITE_ORM-${SQLITE_ORM_VERSION} from source")

    set(SQLITE_ORM_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/sqlite_orm_ep-prefix")
    set(SQLITE_ORM_TAR_NAME "${SQLITE_ORM_PREFIX}/sqlite_orm-${SQLITE_ORM_VERSION}.tar.gz")
    set(SQLITE_ORM_INCLUDE_DIR "${SQLITE_ORM_PREFIX}/sqlite_orm-${SQLITE_ORM_VERSION}/include/sqlite_orm")
    if (NOT EXISTS ${SQLITE_ORM_INCLUDE_DIR})
        file(MAKE_DIRECTORY ${SQLITE_ORM_PREFIX})
        file(DOWNLOAD ${SQLITE_ORM_SOURCE_URL}
                ${SQLITE_ORM_TAR_NAME})
        execute_process(COMMAND ${CMAKE_COMMAND} -E tar -xf ${SQLITE_ORM_TAR_NAME}
                        WORKING_DIRECTORY ${SQLITE_ORM_PREFIX})

    endif ()

endmacro()

if(MILVUS_WITH_SQLITE_ORM)
    resolve_dependency(SQLite_ORM)
    include_directories(SYSTEM "${SQLITE_ORM_INCLUDE_DIR}")
endif()

# ----------------------------------------------------------------------
# Thrift

macro(build_thrift)
    message(STATUS "Building Apache Thrift-${THRIFT_VERSION} from source")
    set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-prefix/src/thrift_ep")
    set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
    set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
    set(THRIFT_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
            "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
	    -DBOOST_ROOT=${BOOST_PREFIX}
            -DWITH_CPP=ON
            -DWITH_STATIC_LIB=ON
	    -DBUILD_SHARED_LIBS=OFF
	    -DBUILD_TESTING=OFF
	    -DBUILD_EXAMPLES=OFF
            -DBUILD_TUTORIALS=OFF
            -DWITH_QT4=OFF
            -DWITH_QT5=OFF
            -DWITH_C_GLIB=OFF
            -DWITH_JAVA=OFF
            -DWITH_PYTHON=OFF
            -DWITH_HASKELL=OFF
            -DWITH_LIBEVENT=OFF
            -DCMAKE_BUILD_TYPE=Release)

    # Thrift also uses boost. Forward important boost settings if there were ones passed.
    if(DEFINED BOOST_ROOT)
        set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBOOST_ROOT=${BOOST_ROOT}")
    endif()
    if(DEFINED Boost_NAMESPACE)
        set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBoost_NAMESPACE=${Boost_NAMESPACE}")
    endif()

    set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")

    set(THRIFT_STATIC_LIB
            "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

    if(ZLIB_SHARED_LIB)
        set(THRIFT_CMAKE_ARGS "-DZLIB_LIBRARY=${ZLIB_SHARED_LIB}" ${THRIFT_CMAKE_ARGS})
    else()
        set(THRIFT_CMAKE_ARGS "-DZLIB_LIBRARY=${ZLIB_STATIC_LIB}" ${THRIFT_CMAKE_ARGS})
    endif()
    set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} ${ZLIB_LIBRARY})

    if(USE_JFROG_CACHE STREQUAL "ON")
        string(MD5 THRIFT_COMBINE_MD5 "${THRIFT_MD5}${ZLIB_MD5}")
        set(THRIFT_CACHE_PACKAGE_NAME "thrift_${THRIFT_COMBINE_MD5}.tar.gz")
        set(THRIFT_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${THRIFT_CACHE_PACKAGE_NAME}")
        set(THRIFT_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${THRIFT_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${THRIFT_CACHE_URL} ${THRIFT_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${THRIFT_CACHE_URL} TO ${THRIFT_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(thrift_ep
                    URL
                    ${THRIFT_SOURCE_URL}
                    BUILD_BYPRODUCTS
                    "${THRIFT_STATIC_LIB}"
                    "${THRIFT_COMPILER}"
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    CMAKE_ARGS
                    ${THRIFT_CMAKE_ARGS}
                    INSTALL_COMMAND
                    ${MAKE} install
                    DEPENDS
                    ${THRIFT_DEPENDENCIES}
                    ${EP_LOG_OPTIONS})

            ExternalProject_Create_Cache(thrift_ep ${THRIFT_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${THRIFT_CACHE_URL})
        else()
            ExternalProject_Use_Cache(thrift_ep ${THRIFT_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(thrift_ep
                URL
                ${THRIFT_SOURCE_URL}
                BUILD_BYPRODUCTS
                "${THRIFT_STATIC_LIB}"
                "${THRIFT_COMPILER}"
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                CMAKE_ARGS
                ${THRIFT_CMAKE_ARGS}
                INSTALL_COMMAND
                ${MAKE} install
                DEPENDS
                ${THRIFT_DEPENDENCIES}
                ${EP_LOG_OPTIONS})
    endif()

    # The include directory must exist before it is referenced by a target.
    file(MAKE_DIRECTORY "${THRIFT_INCLUDE_DIR}")
    add_library(thrift STATIC IMPORTED)
    set_target_properties(thrift
            PROPERTIES IMPORTED_LOCATION "${THRIFT_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${THRIFT_INCLUDE_DIR}")
    add_dependencies(thrift thrift_ep)
endmacro()

if(MILVUS_WITH_THRIFT)
    resolve_dependency(Thrift)

    link_directories(SYSTEM ${THRIFT_PREFIX}/lib/)
    link_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-prefix/src/thrift_ep-build/lib)
    include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
    include_directories(SYSTEM ${THRIFT_PREFIX}/lib/cpp/src)
    include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/thrift_ep-prefix/src/thrift_ep-build)
endif()

# ----------------------------------------------------------------------
# yaml-cpp

macro(build_yamlcpp)
    message(STATUS "Building yaml-cpp-${YAMLCPP_VERSION} from source")
    set(YAMLCPP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/yaml-cpp_ep-prefix/src/yaml-cpp_ep")
    set(YAMLCPP_STATIC_LIB "${YAMLCPP_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}yaml-cpp${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(YAMLCPP_INCLUDE_DIR "${YAMLCPP_PREFIX}/include")
    set(YAMLCPP_CMAKE_ARGS
            ${EP_COMMON_CMAKE_ARGS}
            "-DCMAKE_INSTALL_PREFIX=${YAMLCPP_PREFIX}"
            -DCMAKE_INSTALL_LIBDIR=lib
            -DYAML_CPP_BUILD_TESTS=OFF
            -DYAML_CPP_BUILD_TOOLS=OFF)

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(YAMLCPP_CACHE_PACKAGE_NAME "yaml-cpp_${YAMLCPP_MD5}.tar.gz")
        set(YAMLCPP_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${YAMLCPP_CACHE_PACKAGE_NAME}")
        set(YAMLCPP_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${YAMLCPP_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${YAMLCPP_CACHE_URL} ${YAMLCPP_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${YAMLCPP_CACHE_URL} TO ${YAMLCPP_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(yaml-cpp_ep
                    URL
                    ${YAMLCPP_SOURCE_URL}
                    ${EP_LOG_OPTIONS}
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    BUILD_BYPRODUCTS
                    "${YAMLCPP_STATIC_LIB}"
                    CMAKE_ARGS
                    ${YAMLCPP_CMAKE_ARGS})

            ExternalProject_Create_Cache(yaml-cpp_ep ${YAMLCPP_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/yaml-cpp_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${YAMLCPP_CACHE_URL})
        else()
            ExternalProject_Use_Cache(yaml-cpp_ep ${YAMLCPP_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(yaml-cpp_ep
                URL
                ${YAMLCPP_SOURCE_URL}
                ${EP_LOG_OPTIONS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                BUILD_BYPRODUCTS
                "${YAMLCPP_STATIC_LIB}"
                CMAKE_ARGS
                ${YAMLCPP_CMAKE_ARGS})
    endif()

    file(MAKE_DIRECTORY "${YAMLCPP_INCLUDE_DIR}")
    add_library(yaml-cpp STATIC IMPORTED)
    set_target_properties(yaml-cpp
            PROPERTIES IMPORTED_LOCATION "${YAMLCPP_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${YAMLCPP_INCLUDE_DIR}")

    add_dependencies(yaml-cpp yaml-cpp_ep)
endmacro()

if(MILVUS_WITH_YAMLCPP)
    resolve_dependency(yaml-cpp)
    
    get_target_property(YAMLCPP_INCLUDE_DIR yaml-cpp INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM ${YAMLCPP_PREFIX}/lib/)
    include_directories(SYSTEM ${YAMLCPP_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# zlib

macro(build_zlib)
    message(STATUS "Building ZLIB-${ZLIB_VERSION} from source")
    set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep-prefix/src/zlib_ep")
    set(ZLIB_STATIC_LIB_NAME libz.a)
    set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
    set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
    set(ZLIB_CMAKE_ARGS ${EP_COMMON_CMAKE_ARGS} "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}"
            -DBUILD_SHARED_LIBS=OFF)

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(ZLIB_CACHE_PACKAGE_NAME "zlib_${ZLIB_MD5}.tar.gz")
        set(ZLIB_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${ZLIB_CACHE_PACKAGE_NAME}")
        set(ZLIB_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${ZLIB_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${ZLIB_CACHE_URL} ${ZLIB_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${ZLIB_CACHE_URL} TO ${ZLIB_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
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

            ExternalProject_Create_Cache(zlib_ep ${ZLIB_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${ZLIB_CACHE_URL})
        else()
            ExternalProject_Use_Cache(zlib_ep ${ZLIB_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
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
    endif()

    file(MAKE_DIRECTORY "${ZLIB_INCLUDE_DIR}")
    add_library(zlib STATIC IMPORTED)
    set_target_properties(zlib
            PROPERTIES IMPORTED_LOCATION "${ZLIB_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ZLIB_INCLUDE_DIR}")

    add_dependencies(zlib zlib_ep)
endmacro()

if(MILVUS_WITH_ZLIB)
    resolve_dependency(ZLIB)
    
    get_target_property(ZLIB_INCLUDE_DIR zlib INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# zstd

macro(build_zstd)
    message(STATUS "Building zstd-${ZSTD_VERSION} from source")
    set(ZSTD_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix/src/zstd_ep")

    set(ZSTD_CMAKE_ARGS
            ${EP_COMMON_TOOLCHAIN}
            "-DCMAKE_INSTALL_PREFIX=${ZSTD_PREFIX}"
            -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_INSTALL_LIBDIR=lib #${CMAKE_INSTALL_LIBDIR}
            -DZSTD_BUILD_PROGRAMS=off
            -DZSTD_BUILD_SHARED=off
            -DZSTD_BUILD_STATIC=on
            -DZSTD_MULTITHREAD_SUPPORT=off)


    set(ZSTD_STATIC_LIB "${ZSTD_PREFIX}/lib/libzstd.a")
    set(ZSTD_INCLUDE_DIR "${ZSTD_PREFIX}/include")
    set(ZSTD_CMAKE_ARGS
            ${ZSTD_CMAKE_ARGS}
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            -DCMAKE_C_FLAGS=${EP_C_FLAGS}
            -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS})

    if(CMAKE_VERSION VERSION_LESS 3.7)
        message(FATAL_ERROR "Building zstd using ExternalProject requires at least CMake 3.7")
    endif()

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(ZSTD_CACHE_PACKAGE_NAME "zstd_${ZSTD_MD5}.tar.gz")
        set(ZSTD_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${ZSTD_CACHE_PACKAGE_NAME}")
        set(ZSTD_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${ZSTD_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${ZSTD_CACHE_URL} ${ZSTD_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${ZSTD_CACHE_URL} TO ${ZSTD_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(zstd_ep
                    ${EP_LOG_OPTIONS}
                    CMAKE_ARGS
                    ${ZSTD_CMAKE_ARGS}
                    SOURCE_SUBDIR
                    "build/cmake"
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    INSTALL_DIR
                    ${ZSTD_PREFIX}
                    URL
                    ${ZSTD_SOURCE_URL}
                    BUILD_BYPRODUCTS
                    "${ZSTD_STATIC_LIB}")

            ExternalProject_Create_Cache(zstd_ep ${ZSTD_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${ZSTD_CACHE_URL})
        else()
            ExternalProject_Use_Cache(zstd_ep ${ZSTD_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(zstd_ep
                ${EP_LOG_OPTIONS}
                CMAKE_ARGS
                ${ZSTD_CMAKE_ARGS}
                SOURCE_SUBDIR
                "build/cmake"
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                INSTALL_DIR
                ${ZSTD_PREFIX}
                URL
                ${ZSTD_SOURCE_URL}
                BUILD_BYPRODUCTS
                "${ZSTD_STATIC_LIB}")
    endif()

    file(MAKE_DIRECTORY "${ZSTD_INCLUDE_DIR}")
    add_library(zstd STATIC IMPORTED)
    set_target_properties(zstd
            PROPERTIES IMPORTED_LOCATION "${ZSTD_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")

    add_dependencies(zstd zstd_ep)
endmacro()

if(MILVUS_WITH_ZSTD)
    resolve_dependency(ZSTD)
    
    get_target_property(ZSTD_INCLUDE_DIR zstd INTERFACE_INCLUDE_DIRECTORIES)
    link_directories(SYSTEM ${ZSTD_PREFIX}/lib)
    include_directories(SYSTEM ${ZSTD_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# aws
macro(build_aws)
    message(STATUS "Building aws-${AWS_VERSION} from source")
    set(AWS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/aws_ep-prefix/src/aws_ep")

    set(AWS_CMAKE_ARGS
            ${EP_COMMON_TOOLCHAIN}
            "-DCMAKE_INSTALL_PREFIX=${AWS_PREFIX}"
            -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_INSTALL_LIBDIR=lib
            -DBUILD_ONLY=s3
            -DBUILD_SHARED_LIBS=off
            -DENABLE_TESTING=off
            -DENABLE_UNITY_BUILD=on
            -DNO_ENCRYPTION=off)

    set(AWS_CPP_SDK_CORE_STATIC_LIB
            "${AWS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-core${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(AWS_CPP_SDK_S3_STATIC_LIB
            "${AWS_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-s3${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(AWS_INCLUDE_DIR "${AWS_PREFIX}/include")
    set(AWS_CMAKE_ARGS
            ${AWS_CMAKE_ARGS}
            -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
            -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
            -DCMAKE_C_FLAGS=${EP_C_FLAGS}
            -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS})

    if(USE_JFROG_CACHE STREQUAL "ON")
        set(AWS_CACHE_PACKAGE_NAME "aws_${AWS_MD5}.tar.gz")
        set(AWS_CACHE_URL "${JFROG_ARTFACTORY_CACHE_URL}/${AWS_CACHE_PACKAGE_NAME}")
        set(AWS_CACHE_PACKAGE_PATH "${THIRDPARTY_PACKAGE_CACHE}/${AWS_CACHE_PACKAGE_NAME}")

        file(DOWNLOAD ${AWS_CACHE_URL} ${AWS_CACHE_PACKAGE_PATH} STATUS status)
        list(GET status 0 status_code)
        message(STATUS "DOWNLOADING FROM ${AWS_CACHE_URL} TO ${AWS_CACHE_PACKAGE_PATH}. STATUS = ${status_code}")
        if (NOT status_code EQUAL 0)
            externalproject_add(aws_ep
                    ${EP_LOG_OPTIONS}
                    CMAKE_ARGS
                    ${AWS_CMAKE_ARGS}
                    BUILD_COMMAND
                    ${MAKE}
                    ${MAKE_BUILD_ARGS}
                    INSTALL_DIR
                    ${AWS_PREFIX}
                    URL
                    ${AWS_SOURCE_URL}
                    BUILD_BYPRODUCTS
                    "${AWS_CPP_SDK_S3_STATIC_LIB}"
                    "${AWS_CPP_SDK_CORE_STATIC_LIB}")

            ExternalProject_Create_Cache(aws_ep ${AWS_CACHE_PACKAGE_PATH} "${CMAKE_CURRENT_BINARY_DIR}/aws_ep-prefix" ${JFROG_USER_NAME} ${JFROG_PASSWORD} ${AWS_CACHE_URL})
        else()
            ExternalProject_Use_Cache(aws_ep ${AWS_CACHE_PACKAGE_PATH} ${CMAKE_CURRENT_BINARY_DIR})
        endif()
    else()
        externalproject_add(aws_ep
                ${EP_LOG_OPTIONS}
                CMAKE_ARGS
                ${AWS_CMAKE_ARGS}
                BUILD_COMMAND
                ${MAKE}
                ${MAKE_BUILD_ARGS}
                INSTALL_DIR
                ${AWS_PREFIX}
                URL
                ${AWS_SOURCE_URL}
                BUILD_BYPRODUCTS
                "${AWS_CPP_SDK_S3_STATIC_LIB}"
                "${AWS_CPP_SDK_CORE_STATIC_LIB}")
    endif()

    file(MAKE_DIRECTORY "${AWS_INCLUDE_DIR}")
    add_library(aws-cpp-sdk-s3 STATIC IMPORTED)
    set_target_properties(aws-cpp-sdk-s3
            PROPERTIES
            IMPORTED_LOCATION "${AWS_CPP_SDK_S3_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${AWS_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "${AWS_PREFIX}/lib/libaws-c-event-stream.a;${AWS_PREFIX}/lib/libaws-checksums.a;${AWS_PREFIX}/lib/libaws-c-common.a")

    add_library(aws-cpp-sdk-core STATIC IMPORTED)
    set_target_properties(aws-cpp-sdk-core
            PROPERTIES IMPORTED_LOCATION "${AWS_CPP_SDK_CORE_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${AWS_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES "${AWS_PREFIX}/lib/libaws-c-event-stream.a;${AWS_PREFIX}/lib/libaws-checksums.a;${AWS_PREFIX}/lib/libaws-c-common.a")

    add_dependencies(aws-cpp-sdk-s3 aws_ep)
    add_dependencies(aws-cpp-sdk-core aws_ep)

endmacro()

if(MILVUS_WITH_AWS)
    resolve_dependency(AWS)
    
    link_directories(SYSTEM ${AWS_PREFIX}/lib)

    get_target_property(AWS_CPP_SDK_S3_INCLUDE_DIR aws-cpp-sdk-s3 INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${AWS_CPP_SDK_S3_INCLUDE_DIR})

    get_target_property(AWS_CPP_SDK_CORE_INCLUDE_DIR aws-cpp-sdk-core INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${AWS_CPP_SDK_CORE_INCLUDE_DIR})

endif()

# ----------------------------------------------------------------------
# libunwind

macro(build_libunwind)
    message(STATUS "Building libunwind-${LIBUNWIND_VERSION} from source")
    set(LIBUNWIND_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/libunwind_ep-prefix/src/libunwind_ep/install")
    set(LIBUNWIND_INCLUDE_DIR "${LIBUNWIND_PREFIX}/include")
    set(LIBUNWIND_SHARED_LIB "${LIBUNWIND_PREFIX}/lib/libunwind${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(LIBUNWIND_CONFIGURE_ARGS "--prefix=${LIBUNWIND_PREFIX}")

    externalproject_add(libunwind_ep
            URL
            ${LIBUNWIND_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CONFIGURE_COMMAND
            "./configure"
            ${LIBUNWIND_CONFIGURE_ARGS}
            BUILD_COMMAND
            ${MAKE} ${MAKE_BUILD_ARGS}
            BUILD_IN_SOURCE
            1
            INSTALL_COMMAND
            ${MAKE} install
            BUILD_BYPRODUCTS
            ${LIBUNWIND_SHARED_LIB})

    file(MAKE_DIRECTORY "${LIBUNWIND_INCLUDE_DIR}")

    add_library(libunwind SHARED IMPORTED)
    set_target_properties(libunwind
            PROPERTIES IMPORTED_LOCATION "${LIBUNWIND_SHARED_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${LIBUNWIND_INCLUDE_DIR}")

    add_dependencies(libunwind libunwind_ep)
endmacro()

if(MILVUS_WITH_LIBUNWIND)
    resolve_dependency(libunwind)

    # TODO: Don't use global includes but rather target_include_directories
    get_target_property(LIBUNWIND_INCLUDE_DIR libunwind INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${LIBUNWIND_INCLUDE_DIR})
endif()

# ----------------------------------------------------------------------
# gperftools

macro(build_gperftools)
    message(STATUS "Building gperftools-${GPERFTOOLS_VERSION} from source")
    set(GPERFTOOLS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gperftools_ep-prefix/src/gperftools_ep")
    set(GPERFTOOLS_INCLUDE_DIR "${GPERFTOOLS_PREFIX}/include")
    set(GPERFTOOLS_STATIC_LIB "${GPERFTOOLS_PREFIX}/lib/libprofiler${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GPERFTOOLS_CONFIGURE_ARGS "--prefix=${GPERFTOOLS_PREFIX}")

    externalproject_add(gperftools_ep
            URL
            ${GPERFTOOLS_SOURCE_URL}
            ${EP_LOG_OPTIONS}
            CONFIGURE_COMMAND
            "./configure"
            ${GPERFTOOLS_CONFIGURE_ARGS}
            BUILD_COMMAND
            ${MAKE} ${MAKE_BUILD_ARGS}
            BUILD_IN_SOURCE
            1
            INSTALL_COMMAND
            ${MAKE} install
            BUILD_BYPRODUCTS
            ${GPERFTOOLS_STATIC_LIB})

    ExternalProject_Add_StepDependencies(gperftools_ep build libunwind_ep)

    file(MAKE_DIRECTORY "${GPERFTOOLS_INCLUDE_DIR}")

    add_library(gperftools STATIC IMPORTED)
    set_target_properties(gperftools
            PROPERTIES IMPORTED_LOCATION "${GPERFTOOLS_STATIC_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "${GPERFTOOLS_INCLUDE_DIR}"
            INTERFACE_LINK_LIBRARIES libunwind)

    add_dependencies(gperftools gperftools_ep)
    add_dependencies(gperftools libunwind_ep)
endmacro()

if(MILVUS_WITH_GPERFTOOLS)
    resolve_dependency(gperftools)

    # TODO: Don't use global includes but rather target_include_directories
    get_target_property(GPERFTOOLS_INCLUDE_DIR gperftools INTERFACE_INCLUDE_DIRECTORIES)
    include_directories(SYSTEM ${GPERFTOOLS_INCLUDE_DIR})
    link_directories(SYSTEM ${GPERFTOOLS_PREFIX}/lib)
endif()
