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

# ----------------------------------------------------------------------
message(STATUS "Building (vendored) opendal from source")

set(GIT_REPOSITORY  "https://github.com/apache/opendal.git")
set(GIT_TAG "v0.43.0-rc.2")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo +1.83 build --verbose)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo +1.83 build --release --verbose)
    set(TARGET_DIR "release")
endif ()

set(SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/src")

FetchContent_Declare(
        opendal
        GIT_REPOSITORY  ${GIT_REPOSITORY}
        GIT_TAG         ${GIT_TAG}
        GIT_SHALLOW     TRUE
        SOURCE_DIR      ${SOURCE_DIR}
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH})

FetchContent_GetProperties(opendal)
if ( NOT opendal_POPULATED )
    FetchContent_Populate(opendal)
endif()

set(OPENDAL_LIB_DIR "${SOURCE_DIR}/target/${TARGET_DIR}" CACHE INTERNAL "opendal lib dir")
set(OPENDAL_INCLUDE_DIR "${SOURCE_DIR}/bindings/c/include" CACHE INTERNAL "opendal include dir")
set(OPENDAL_LIB "libopendal_c${CMAKE_STATIC_LIBRARY_SUFFIX}" CACHE INTERNAL "opendal lib")

add_custom_target(build_opendal
    COMMAND ${CARGO_CMD}
    WORKING_DIRECTORY ${SOURCE_DIR}/bindings/c
)

add_library(opendal STATIC IMPORTED)
add_dependencies(opendal build_opendal)
set_target_properties(opendal
    PROPERTIES
    IMPORTED_GLOBAL TRUE
    IMPORTED_LOCATION "${OPENDAL_LIB_DIR}/${OPENDAL_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${OPENDAL_INCLUDE_DIR}")

get_target_property(OPENDAL_IMPORTED_LOCATION opendal IMPORTED_LOCATION)
get_target_property(OPENDAL_INTERFACE_INCLUDE_DIRECTORIES opendal INTERFACE_INCLUDE_DIRECTORIES)
message("OPENDAL_IMPORTED_LOCATION: ${OPENDAL_IMPORTED_LOCATION}")
message("OPENDAL_INTERFACE_INCLUDE_DIRECTORIES: ${OPENDAL_INTERFACE_INCLUDE_DIRECTORIES}")
