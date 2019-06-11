#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.14)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

MACRO (GET_CURRENT_TIME CURRENT_TIME)
    execute_process(COMMAND "date" +"%Y-%m-%d %H:%M.%S" OUTPUT_VARIABLE ${CURRENT_TIME})
ENDMACRO (GET_CURRENT_TIME)

GET_CURRENT_TIME(BUILD_TIME)
string(REGEX REPLACE "\n" "" BUILD_TIME ${BUILD_TIME})
message(STATUS "Build time = ${BUILD_TIME}")

MACRO (GET_GIT_BRANCH_NAME GIT_BRANCH_NAME)
    execute_process(COMMAND "git" symbolic-ref --short HEAD OUTPUT_VARIABLE ${GIT_BRANCH_NAME})
ENDMACRO (GET_GIT_BRANCH_NAME)

GET_GIT_BRANCH_NAME(GIT_BRANCH_NAME)
string(REGEX REPLACE "\n" "" GIT_BRANCH_NAME ${GIT_BRANCH_NAME})

set(MEGASEARCH_VERSION "${GIT_BRANCH_NAME}")
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]" MEGASEARCH_VERSION "${MEGASEARCH_VERSION}")
message(STATUS "Build version = ${MEGASEARCH_VERSION}")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "release")
else()
    set(BUILD_TYPE "debug")
endif()
message(STATUS "Build type = ${BUILD_TYPE}")

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.h.macro ${CMAKE_CURRENT_SOURCE_DIR}/version.h)

project(megasearch VERSION "${MEGASEARCH_VERSION}")
project(vecwise_engine LANGUAGES CUDA CXX)

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
    if(NOT MSVC)
        find_program(MAKE make)
    endif()
endif()

set(GCC_VERSION 5.3.0)
set(GCC_DIR "${CMAKE_CURRENT_BINARY_DIR}/gcc")
set(GCC_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/gcc/gcc_build")
set(GCC_TAR_NAME "${GCC_BUILD_DIR}/gcc-${GCC_VERSION}.tar.gz")
if (NOT EXISTS ${GCC_TAR_NAME})
    file(MAKE_DIRECTORY ${GCC_BUILD_DIR})
    file(DOWNLOAD http://ftp.tsukuba.wide.ad.jp/software/gcc/releases/gcc-${GCC_VERSION}/${GCC_TAR_NAME}
            ${GCC_TAR_NAME})
    message(STATUS "Building GCC-${GCC_VERSION} from source")
    execute_process(
            COMMAND
            "${CMAKE_COMMAND} -E tar -xf ${GCC_TAR_NAME}"
            COMMAND
            "./configure --prefix=${GCC_DIR} --with-local-prefix=${GCC_DIR}
            --enable-languages=cxx,fortran
            --enable-checking=release
            --disable-bootstrap"
            COMMAND
            "${MAKE} -j4"
            COMMAND
            "${MAKE} install"
            RESULT_VARIABLE	_status
            #OUTPUT_QUIET
            #ERROR_QUIET
            WORKING_DIRECTORY ${GCC_BUILD_DIR})
endif ()

set(MEGASEARCH_VERSION_MAJOR "${megasearch_VERSION_MAJOR}")
set(MEGASEARCH_VERSION_MINOR "${megasearch_VERSION_MINOR}")
set(MEGASEARCH_VERSION_PATCH "${megasearch_VERSION_PATCH}")

if(MEGASEARCH_VERSION_MAJOR STREQUAL ""
        OR MEGASEARCH_VERSION_MINOR STREQUAL ""
        OR MEGASEARCH_VERSION_PATCH STREQUAL "")
    message(FATAL_ERROR "Failed to determine MegaSearch version from '${MEGASEARCH_VERSION}'")
endif()

message(STATUS "MegaSearch version: "
        "${MEGASEARCH_VERSION_MAJOR}.${MEGASEARCH_VERSION_MINOR}.${MEGASEARCH_VERSION_PATCH} "
        "(full: '${MEGASEARCH_VERSION}')")

set(MEGASEARCH_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(MEGASEARCH_BINARY_DIR ${PROJECT_BINARY_DIR})

find_package(CUDA)
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES -arch sm_60 --expt-extended-lambda")
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")
message("CUDA_TOOLKIT_ROOT_DIR=${CUDA_TOOLKIT_ROOT_DIR}")
message("CUDA_NVCC_FLAGS=${CUDA_NVCC_FLAGS}")

if (GPU_VERSION STREQUAL "ON")
    add_definitions("-DGPU_VERSION")
endif ()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED on)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message("building vecwise_engine on x86 architecture")
    set(VECWISE_BUILD_ARCH x86_64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message("building vecwise_engine on ppc architecture")
    set(VECWISE_BUILD_ARCH ppc64le)
else()
    message("unknown processor type")
    message("CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(VECWISE_BUILD_ARCH unknown)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -DELPP_THREAD_SAFE -fopenmp")
     if (GPU_VERSION STREQUAL "ON")
         set(ENABLE_LICENSE "ON")
         add_definitions("-DENABLE_LICENSE")
     endif ()
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -DELPP_THREAD_SAFE -fopenmp")
endif()



set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

#if (BUILD_UNIT_TEST)
#    option(MEGASEARCH_BUILD_TESTS "Build the megasearch test suite" ON)
#endif(BUILD_UNIT_TEST)

include(ExternalProject)
include(DefineOptions)
include(BuildUtils)
include(ThirdPartyPackages)

include_directories(${MEGASEARCH_SOURCE_DIR})
link_directories(${MEGASEARCH_BINARY_DIR})

## Following should be check

set(VECWISE_ENGINE_INCLUDE ${PROJECT_SOURCE_DIR}/include)
set(VECWISE_ENGINE_SRC ${PROJECT_SOURCE_DIR}/src)
#set(VECWISE_THIRD_PARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
#set(VECWISE_THIRD_PARTY_BUILD ${CMAKE_CURRENT_SOURCE_DIR}/third_party/build)

add_compile_definitions(PROFILER=${PROFILER})

include_directories(${VECWISE_ENGINE_INCLUDE})
include_directories(${VECWISE_ENGINE_SRC})
#include_directories(${VECWISE_THIRD_PARTY_BUILD}/include)

link_directories(${CMAKE_CURRRENT_BINARY_DIR})
#link_directories(${VECWISE_THIRD_PARTY_BUILD}/lib)
#link_directories(${VECWISE_THIRD_PARTY_BUILD}/lib64)
#execute_process(COMMAND bash build.sh
#                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

add_subdirectory(src)

if (BUILD_UNIT_TEST)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/unittest)
endif(BUILD_UNIT_TEST)

add_custom_target(Clean-All COMMAND ${CMAKE_BUILD_TOOL} clean)

#install
install(FILES
        scripts/start_server.sh
        scripts/stop_server.sh
        DESTINATION
        scripts)
install(FILES
        conf/server_config.yaml
        conf/vecwise_engine_log.conf
        DESTINATION
        conf)

config_summary()
