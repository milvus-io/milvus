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

set(OPENDAL_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib")
set(OPENDAL_INCLUDE_DIR "${CMAKE_INSTALL_PREFIX}/include")
set(OPENDAL_NAME "libopendal_c${CMAKE_STATIC_LIBRARY_SUFFIX}")

add_library(opendal STATIC IMPORTED)
set_target_properties(opendal
    PROPERTIES
    IMPORTED_GLOBAL TRUE
    IMPORTED_LOCATION "${OPENDAL_LIB_DIR}/${OPENDAL_NAME}"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/include")

get_target_property(OPENDAL_IMPORTED_LOCATION opendal IMPORTED_LOCATION)
get_target_property(OPENDAL_INTERFACE_INCLUDE_DIRECTORIES opendal INTERFACE_INCLUDE_DIRECTORIES)
message("OPENDAL_IMPORTED_LOCATION: ${OPENDAL_IMPORTED_LOCATION}")
message("OPENDAL_INTERFACE_INCLUDE_DIRECTORIES: ${OPENDAL_INTERFACE_INCLUDE_DIRECTORIES}")
