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
if(NOT GIT_BRANCH_NAME STREQUAL "")
    string(REGEX REPLACE "\n" "" GIT_BRANCH_NAME ${GIT_BRANCH_NAME})
endif()

set(MILVUS_VERSION "${GIT_BRANCH_NAME}")
string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]" MILVUS_VERSION "${MILVUS_VERSION}")

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "release")
else()
    set(BUILD_TYPE "debug")
endif()
message(STATUS "Build type = ${BUILD_TYPE}")

project(milvus VERSION "${MILVUS_VERSION}")
project(milvus_engine LANGUAGES CUDA CXX)

# Ensure that a default make is set
if("${MAKE}" STREQUAL "")
    if(NOT MSVC)
        find_program(MAKE make)
    endif()
endif()

set(MILVUS_VERSION_MAJOR "${milvus_VERSION_MAJOR}")
set(MILVUS_VERSION_MINOR "${milvus_VERSION_MINOR}")
set(MILVUS_VERSION_PATCH "${milvus_VERSION_PATCH}")

if(MILVUS_VERSION_MAJOR STREQUAL ""
        OR MILVUS_VERSION_MINOR STREQUAL ""
        OR MILVUS_VERSION_PATCH STREQUAL "")
    message(WARNING "Failed to determine Milvus version from git branch name")
    set(MILVUS_VERSION "0.3.0")
endif()

message(STATUS "Build version = ${MILVUS_VERSION}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/version.h.macro ${CMAKE_CURRENT_SOURCE_DIR}/version.h)

message(STATUS "Milvus version: "
        "${MILVUS_VERSION_MAJOR}.${MILVUS_VERSION_MINOR}.${MILVUS_VERSION_PATCH} "
        "(full: '${MILVUS_VERSION}')")

set(MILVUS_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(MILVUS_BINARY_DIR ${PROJECT_BINARY_DIR})

find_package(CUDA)
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES -arch sm_60 --expt-extended-lambda")
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")
message("CUDA_TOOLKIT_ROOT_DIR=${CUDA_TOOLKIT_ROOT_DIR}")
message("CUDA_NVCC_FLAGS=${CUDA_NVCC_FLAGS}")

set(GPU_VERSION "ON")
add_definitions("-DGPU_VERSION")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED on)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message("building milvus_engine on x86 architecture")
    set(MILVUS_BUILD_ARCH x86_64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message("building milvus_engine on ppc architecture")
    set(MILVUS_BUILD_ARCH ppc64le)
else()
    message("unknown processor type")
    message("CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(MILVUS_BUILD_ARCH unknown)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -DELPP_THREAD_SAFE -fopenmp")
     if (CMAKE_LICENSE_CHECK STREQUAL "ON")
         set(ENABLE_LICENSE "ON")
         add_definitions("-DENABLE_LICENSE")
     endif ()
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -DELPP_THREAD_SAFE -fopenmp")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ExternalProject)
include(DefineOptions)
include(BuildUtils)
include(ThirdPartyPackages)

include_directories(${MILVUS_SOURCE_DIR})
link_directories(${MILVUS_BINARY_DIR})

## Following should be check

set(MILVUS_ENGINE_INCLUDE ${PROJECT_SOURCE_DIR}/include)
set(MILVUS_ENGINE_SRC ${PROJECT_SOURCE_DIR}/src)
#set(MILVUS_THIRD_PARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
#set(MILVUS_THIRD_PARTY_BUILD ${CMAKE_CURRENT_SOURCE_DIR}/third_party/build)

add_compile_definitions(PROFILER=${PROFILER})

include_directories(${MILVUS_ENGINE_INCLUDE})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(/usr/local/cuda/include)
#include_directories(${MILVUS_THIRD_PARTY_BUILD}/include)

link_directories(${CMAKE_CURRRENT_BINARY_DIR})
#link_directories(${MILVUS_THIRD_PARTY_BUILD}/lib)
#link_directories(${MILVUS_THIRD_PARTY_BUILD}/lib64)
#execute_process(COMMAND bash build.sh
#                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

add_subdirectory(src)

if (BUILD_COVERAGE STREQUAL "ON")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()


if ("${BUILD_UNIT_TEST}" STREQUAL "ON")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/unittest)
endif(BUILD_UNIT_TEST)

add_custom_target(Clean-All COMMAND ${CMAKE_BUILD_TOOL} clean)


if("${MILVUS_DB_PATH}" STREQUAL "")
    set(MILVUS_DB_PATH "/tmp/milvus")
endif()
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.template ${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.yaml)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf/log_config.template ${CMAKE_CURRENT_SOURCE_DIR}/conf/log_config.conf)

#install
install(FILES
        scripts/start_server.sh
        scripts/stop_server.sh
        DESTINATION
        scripts)
install(FILES
        conf/server_config.yaml
        conf/log_config.conf
        DESTINATION
        conf)



config_summary()
