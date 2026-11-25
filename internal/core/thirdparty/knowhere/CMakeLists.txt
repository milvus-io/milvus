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

# Update KNOWHERE_VERSION for the first occurrence
milvus_add_pkg_config("knowhere")
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES "")
set( KNOWHERE_VERSION 0184cb42 )
set( GIT_REPOSITORY  "https://github.com/zilliztech/knowhere.git")

message(STATUS "Knowhere repo: ${GIT_REPOSITORY}")
message(STATUS "Knowhere version: ${KNOWHERE_VERSION}")

message(STATUS "Building knowhere-${KNOWHERE_VERSION} from source")
message(STATUS ${CMAKE_BUILD_TYPE})

if ( BUILD_DISK_ANN STREQUAL "ON" )
    set(WITH_DISKANN ON CACHE BOOL "" FORCE )
else ()
    set(WITH_DISKANN OFF CACHE BOOL "" FORCE )
endif ()

if ( INDEX_ENGINE STREQUAL "cardinal" )
    set(WITH_CARDINAL ON CACHE BOOL "" FORCE )
else ()
    set(WITH_CARDINAL OFF CACHE BOOL "" FORCE )
endif()

if ( WITH_GPU AND NOT WITH_CARDINAL )
    set(raft_DIR "${FETCHCONTENT_BASE_DIR}/raft-build" CACHE PATH "Path to raft build directory" FORCE)
    set(rmm_DIR "${FETCHCONTENT_BASE_DIR}/rmm-build" CACHE PATH "Path to rmm build directory" FORCE)
endif ()

# Resolve auto for the target without replacing the user's cached selection.
if ( USE_TSAN AND WITH_SVS STREQUAL "ON" )
    message(FATAL_ERROR "WITH_SVS=ON is incompatible with USE_TSAN: the SVS runtime links GNU libgomp, but TSan requires LLVM libomp")
endif()
if ( WITH_SVS STREQUAL "ON" OR
     ( WITH_SVS STREQUAL "AUTO" AND NOT USE_TSAN AND CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
       CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$" ) )
    set(WITH_SVS ON)
else ()
    set(WITH_SVS OFF)
endif()
message(STATUS "Knowhere SVS support: ${WITH_SVS}")

# Conan 2.x: CMAKE_PREFIX_PATH is set by conan_toolchain.cmake
FetchContent_Declare(
        knowhere
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${KNOWHERE_VERSION}
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/knowhere-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

FetchContent_GetProperties( knowhere )
if ( NOT knowhere_POPULATED )
    FetchContent_Populate( knowhere )

    # Adding the following target:
    # knowhere
    add_subdirectory( ${knowhere_SOURCE_DIR}
                      ${knowhere_BINARY_DIR} )
endif()

# Install the runtime alongside libknowhere.so. Both fetched and preinstalled
# SVS packages place their config under lib/cmake/svs_runtime.
if ( WITH_SVS )
    get_directory_property(_SVS_CONFIG_DIR DIRECTORY "${knowhere_SOURCE_DIR}" DEFINITION svs_runtime_DIR)
    file(GLOB _SVS_RUNTIME_LIBS "${_SVS_CONFIG_DIR}/../../libsvs_runtime.so*")
    if ( NOT _SVS_RUNTIME_LIBS )
        message(FATAL_ERROR "SVS runtime libraries not found for package config at ${_SVS_CONFIG_DIR}")
    endif()
    # Use the notices from the same package as the binary. Early v0.5.1
    # bundles shipped without these files, including existing offline installs.
    get_filename_component(_SVS_PREFIX "${_SVS_CONFIG_DIR}/../../.." ABSOLUTE)
    if ( EXISTS "${_SVS_PREFIX}/LICENSE" AND EXISTS "${_SVS_PREFIX}/THIRD-PARTY-PROGRAMS" )
        file(READ "${_SVS_PREFIX}/LICENSE" _SVS_LICENSE)
        file(READ "${_SVS_PREFIX}/THIRD-PARTY-PROGRAMS" _SVS_THIRD_PARTY)
        set(_SVS_NOTICES "${CMAKE_CURRENT_BINARY_DIR}/svs-notices.txt")
        file(WRITE "${_SVS_NOTICES}" "Intel® Scalable Vector Search: Copyright (C) Intel Corporation\n\n${_SVS_LICENSE}\n${_SVS_THIRD_PARTY}")
    else()
        get_directory_property(_SVS_VERSION DIRECTORY "${knowhere_SOURCE_DIR}" DEFINITION svs_runtime_VERSION)
        if ( NOT _SVS_VERSION VERSION_EQUAL "0.5.1" )
            message(FATAL_ERROR "Missing LICENSE and/or THIRD-PARTY-PROGRAMS for SVS ${_SVS_VERSION} at ${_SVS_PREFIX}")
        endif()
        set(_SVS_NOTICES "${CMAKE_CURRENT_SOURCE_DIR}/svs-0.5.1-notices.txt")
    endif()
    install(FILES ${_SVS_RUNTIME_LIBS} DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    install(FILES "${_SVS_NOTICES}" DESTINATION "${CMAKE_INSTALL_LIBDIR}" RENAME svs-notices.txt)
endif()

if ( WITH_GPU AND NOT WITH_CARDINAL )
    set( _RMM_LIB_DIR "${FETCHCONTENT_BASE_DIR}/rmm-build" )
    if ( EXISTS "${_RMM_LIB_DIR}" )
        install(DIRECTORY ${_RMM_LIB_DIR}/
                DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING PATTERN "librmm.so*")
    else()
        message(WARNING "cuVS enabled but ${_RMM_LIB_DIR} not found \u2014 librmm.so will not be installed")
    endif()

    set( _RAPIDS_LOGGER_LIB_DIR "${FETCHCONTENT_BASE_DIR}/rapids_logger-build" )
    if ( EXISTS "${_RAPIDS_LOGGER_LIB_DIR}" )
        install(DIRECTORY ${_RAPIDS_LOGGER_LIB_DIR}/
                DESTINATION ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                FILES_MATCHING PATTERN "librapids_logger.so*")
    else()
        message(WARNING "cuVS enabled but ${_RAPIDS_LOGGER_LIB_DIR} not found \u2014 librapids_logger.so will not be installed")
    endif()
endif()

# get prometheus COMPILE_OPTIONS
get_property( var DIRECTORY "${knowhere_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "knowhere src compile options: ${var}" )

set( KNOWHERE_INCLUDE_DIR ${knowhere_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory" )
