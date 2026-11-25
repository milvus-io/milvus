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

if ( WITH_SVS )
    # SVS bundles its own fmt headers. Prefer the headers of the fmt library linked by Milvus.
    target_include_directories(faiss_svs SYSTEM BEFORE INTERFACE
            "$<TARGET_PROPERTY:fmt::fmt,INTERFACE_INCLUDE_DIRECTORIES>")

    # Both fetched and preinstalled packages provide their config under lib/cmake/svs_runtime.
    get_directory_property(_SVS_CONFIG_DIR DIRECTORY "${knowhere_SOURCE_DIR}" DEFINITION svs_runtime_DIR)
    file(GLOB _SVS_RUNTIME_LIBS "${_SVS_CONFIG_DIR}/../../libsvs_runtime.so*")
    if ( NOT _SVS_RUNTIME_LIBS )
        message(FATAL_ERROR "SVS runtime libraries not found for package config at ${_SVS_CONFIG_DIR}")
    endif()
    install(FILES ${_SVS_RUNTIME_LIBS} DESTINATION "${CMAKE_INSTALL_LIBDIR}")
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
