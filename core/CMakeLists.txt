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


cmake_minimum_required(VERSION 3.12)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# get build time
MACRO(GET_CURRENT_TIME CURRENT_TIME)
    execute_process(COMMAND "date" +"%Y-%m-%d %H:%M.%S" OUTPUT_VARIABLE ${CURRENT_TIME})
ENDMACRO(GET_CURRENT_TIME)

GET_CURRENT_TIME(BUILD_TIME)
string(REGEX REPLACE "\n" "" BUILD_TIME ${BUILD_TIME})
message(STATUS "Build time = ${BUILD_TIME}")

if (NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build.")
endif ()

# get Milvus version via branch name
set(GIT_BRANCH_NAME_REGEX "[0-9]+\\.[0-9]+\\.[0-9]")

MACRO(GET_GIT_BRANCH_NAME GIT_BRANCH_NAME)
    execute_process(COMMAND sh "-c" "git log --decorate | head -n 1 | sed 's/.*(\\(.*\\))/\\1/' | sed 's/.*, //' | sed 's=[a-zA-Z]*\/==g'"
            OUTPUT_VARIABLE ${GIT_BRANCH_NAME})
    if (NOT GIT_BRANCH_NAME MATCHES "${GIT_BRANCH_NAME_REGEX}")
        execute_process(COMMAND "git" rev-parse --abbrev-ref HEAD OUTPUT_VARIABLE ${GIT_BRANCH_NAME})
    endif ()
    if (NOT GIT_BRANCH_NAME MATCHES "${GIT_BRANCH_NAME_REGEX}")
        execute_process(COMMAND "git" symbolic-ref --short -q HEAD HEAD OUTPUT_VARIABLE ${GIT_BRANCH_NAME})
    endif ()
ENDMACRO(GET_GIT_BRANCH_NAME)

GET_GIT_BRANCH_NAME(GIT_BRANCH_NAME)
message(STATUS "GIT_BRANCH_NAME = ${GIT_BRANCH_NAME}")
if (NOT GIT_BRANCH_NAME STREQUAL "")
    string(REGEX REPLACE "\n" "" GIT_BRANCH_NAME ${GIT_BRANCH_NAME})
endif ()

set(MILVUS_VERSION "${GIT_BRANCH_NAME}")
string(REGEX MATCH "${GIT_BRANCH_NAME_REGEX}" MILVUS_VERSION "${MILVUS_VERSION}")

# get last commit id
MACRO(GET_LAST_COMMIT_ID LAST_COMMIT_ID)
    execute_process(COMMAND sh "-c" "git log --decorate | head -n 1 | awk '{print $2}'"
            OUTPUT_VARIABLE ${LAST_COMMIT_ID})
ENDMACRO(GET_LAST_COMMIT_ID)

GET_LAST_COMMIT_ID(LAST_COMMIT_ID)
message(STATUS "LAST_COMMIT_ID = ${LAST_COMMIT_ID}")
if (NOT LAST_COMMIT_ID STREQUAL "")
    string(REGEX REPLACE "\n" "" LAST_COMMIT_ID ${LAST_COMMIT_ID})
    set(LAST_COMMIT_ID "${LAST_COMMIT_ID}")
else ()
    set(LAST_COMMIT_ID "Unknown")
endif ()

# set build type
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(BUILD_TYPE "Release")
else ()
    set(BUILD_TYPE "Debug")
endif ()
message(STATUS "Build type = ${BUILD_TYPE}")

project(milvus VERSION "${MILVUS_VERSION}")
project(milvus_engine LANGUAGES CXX)

unset(CMAKE_EXPORT_COMPILE_COMMANDS CACHE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(MILVUS_VERSION_MAJOR "${milvus_VERSION_MAJOR}")
set(MILVUS_VERSION_MINOR "${milvus_VERSION_MINOR}")
set(MILVUS_VERSION_PATCH "${milvus_VERSION_PATCH}")

if (MILVUS_VERSION_MAJOR STREQUAL ""
        OR MILVUS_VERSION_MINOR STREQUAL ""
        OR MILVUS_VERSION_PATCH STREQUAL "")
    message(WARNING "Failed to determine Milvus version from git branch name")
    set(MILVUS_VERSION "1.1.1")
endif ()

message(STATUS "Build version = ${MILVUS_VERSION}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/version.h.in ${CMAKE_CURRENT_SOURCE_DIR}/src/version.h @ONLY)

message(STATUS "Milvus version: "
        "${MILVUS_VERSION_MAJOR}.${MILVUS_VERSION_MINOR}.${MILVUS_VERSION_PATCH} "
        "(full: '${MILVUS_VERSION}')")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED on)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message(STATUS "Building milvus_engine on x86 architecture")
    set(MILVUS_BUILD_ARCH x86_64)
elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message(STATUS "Building milvus_engine on ppc architecture")
    set(MILVUS_BUILD_ARCH ppc64le)
else ()
    message(WARNING "Unknown processor type")
    message(WARNING "CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(MILVUS_BUILD_ARCH unknown)
endif ()

# Ensure that a default make is set
if ("${MAKE}" STREQUAL "")
    if (NOT MSVC)
        find_program(MAKE make)
    endif ()
endif ()

find_path(MYSQL_INCLUDE_DIR
        NAMES "mysql.h"
        PATH_SUFFIXES "mysql")
if (${MYSQL_INCLUDE_DIR} STREQUAL "MYSQL_INCLUDE_DIR-NOTFOUND")
    message(FATAL_ERROR "Could not found MySQL include directory")
else ()
    include_directories(${MYSQL_INCLUDE_DIR})
endif ()

set(MILVUS_SOURCE_DIR ${PROJECT_SOURCE_DIR})
set(MILVUS_BINARY_DIR ${PROJECT_BINARY_DIR})
set(MILVUS_ENGINE_SRC ${PROJECT_SOURCE_DIR}/src)
set(MILVUS_THIRDPARTY_SRC ${PROJECT_SOURCE_DIR}/thirdparty)

include(ExternalProject)
include(DefineOptions)
include(BuildUtils)
include(ThirdPartyPackages)

if (MILVUS_USE_CCACHE)
    find_program(CCACHE_FOUND ccache)
    if (CCACHE_FOUND)
        message(STATUS "Using ccache: ${CCACHE_FOUND}")
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
        # let ccache preserve C++ comments, because some of them may be
        # meaningful to the compiler
        set(ENV{CCACHE_COMMENTS} "1")
    endif (CCACHE_FOUND)
endif ()
if (MILVUS_FPGA_VERSION)
    message(STATUS "Building Milvus FPGA version")
    add_compile_definitions("MILVUS_FPGA_VERSION")
else ()
    message(STATUS "Building Milvus CPU version")
endif ()
if (MILVUS_APU_VERSION)
    message(STATUS "Building Milvus APU version")
    add_compile_definitions("MILVUS_APU_VERSION")
else ()
    message(STATUS "Building Milvus CPU version")
endif ()
if (MILVUS_GPU_VERSION)
    message(STATUS "Building Milvus GPU version")
    add_compile_definitions("MILVUS_GPU_VERSION")
    enable_language(CUDA)
    find_package(CUDA 11 REQUIRED)
    set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES --expt-extended-lambda")
else ()
    message(STATUS "Building Milvus CPU version")
endif ()

if (MILVUS_WITH_PROMETHEUS)
    add_compile_definitions("MILVUS_WITH_PROMETHEUS")
endif ()

message("ENABLE_CPU_PROFILING = ${ENABLE_CPU_PROFILING}")
if (ENABLE_CPU_PROFILING STREQUAL "ON")
    ADD_DEFINITIONS(-DENABLE_CPU_PROFILING)
endif()

if (MILVUS_WITH_FIU)
    add_compile_definitions("FIU_ENABLE")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -DELPP_THREAD_SAFE -fopenmp")
    if (MILVUS_GPU_VERSION)
        set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O3")
    endif ()
else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -DELPP_THREAD_SAFE -fopenmp")
    if (MILVUS_GPU_VERSION)
        set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")
    endif ()
endif ()

config_summary()
add_subdirectory(src)

if (BUILD_UNIT_TEST STREQUAL "ON")
    add_compile_definitions("FIU_ENABLE")
    if (BUILD_COVERAGE STREQUAL "ON")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    endif ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DELPP_DISABLE_LOGS")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/unittest)
endif ()

add_custom_target(Clean-All COMMAND ${CMAKE_BUILD_TOOL} clean)

if ("${MILVUS_DB_PATH}" STREQUAL "")
    set(MILVUS_DB_PATH "${CMAKE_INSTALL_PREFIX}")
endif ()
if (MILVUS_FPGA_VERSION)
    set(FPGA_ENABLE "true")
else ()
    set(FPGA_ENABLE "false")
endif ()
if (MILVUS_APU_VERSION)
    set(APU_ENABLE "true")
else ()
    set(APU_ENABLE "false")
endif ()
if (MILVUS_GPU_VERSION)
    set(GPU_ENABLE "true")
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.template 
        ${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.yaml
        @ONLY)
else ()
    set(GPU_ENABLE "false")
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.template 
        ${CMAKE_CURRENT_SOURCE_DIR}/conf/server_config.yaml
        @ONLY)
endif ()

install(DIRECTORY scripts/
        DESTINATION scripts
        FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ
        FILES_MATCHING PATTERN "*.sh")
install(DIRECTORY scripts/migration
        DESTINATION scripts
        FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_READ
        WORLD_EXECUTE WORLD_READ)
install(FILES
        conf/server_config.yaml
        DESTINATION
        conf)

find_package(Python COMPONENTS Interpreter Development)
find_package(ClangTools)
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

#
# "make lint" target
#
if (NOT MILVUS_VERBOSE_LINT)
    set(MILVUS_LINT_QUIET "--quiet")
endif ()

if (NOT LINT_EXCLUSIONS_FILE)
    # source files matching a glob from a line in this file
    # will be excluded from linting (cpplint, clang-tidy, clang-format)
    set(LINT_EXCLUSIONS_FILE ${BUILD_SUPPORT_DIR}/lint_exclusions.txt)
endif ()

find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")

#
# "make lint" targets
#
add_custom_target(lint
        ${PYTHON_EXECUTABLE}
        ${BUILD_SUPPORT_DIR}/run_cpplint.py
        --cpplint_binary
        ${CPPLINT_BIN}
        --exclude_globs
        ${LINT_EXCLUSIONS_FILE}
        --source_dir
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${MILVUS_LINT_QUIET})

#
# "make clang-format" and "make check-clang-format" targets
#
if (${CLANG_FORMAT_FOUND})
    # runs clang format and updates files in place.
    add_custom_target(clang-format
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_format.py
            --clang_format_binary
            ${CLANG_FORMAT_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            --fix
            ${MILVUS_LINT_QUIET})

    # runs clang format and exits with a non-zero exit code if any files need to be reformatted
    add_custom_target(check-clang-format
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_format.py
            --clang_format_binary
            ${CLANG_FORMAT_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MILVUS_LINT_QUIET})
endif ()

#
# "make clang-tidy" and "make check-clang-tidy" targets
#
if (${CLANG_TIDY_FOUND})
    # runs clang-tidy and attempts to fix any warning automatically
    add_custom_target(clang-tidy
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
            --clang_tidy_binary
            ${CLANG_TIDY_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --compile_commands
            ${CMAKE_BINARY_DIR}/compile_commands.json
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            --fix
            ${MILVUS_LINT_QUIET})

    # runs clang-tidy and exits with a non-zero exit code if any errors are found.
    add_custom_target(check-clang-tidy
            ${PYTHON_EXECUTABLE}
            ${BUILD_SUPPORT_DIR}/run_clang_tidy.py
            --clang_tidy_binary
            ${CLANG_TIDY_BIN}
            --exclude_globs
            ${LINT_EXCLUSIONS_FILE}
            --compile_commands
            ${CMAKE_BINARY_DIR}/compile_commands.json
            --source_dir
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${MILVUS_LINT_QUIET})
endif ()
