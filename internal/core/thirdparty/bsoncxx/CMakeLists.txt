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

message(STATUS "Building mongo-cxx-driver (bsoncxx only) from source")

# Disable tests and examples for mongo-cxx-driver
set(ENABLE_TESTS OFF CACHE INTERNAL "")
set(BUILD_TESTING OFF CACHE INTERNAL "")
set(ENABLE_EXAMPLES OFF CACHE INTERNAL "")
set(BUILD_MONGOCXX OFF CACHE INTERNAL "")
set(BUILD_SHARED_LIBS ON CACHE INTERNAL "")
set(BUILD_VERSION "0.0.0" CACHE INTERNAL "")

# Disable optional features in mongo-c-driver to avoid linking issues on macOS
set(ENABLE_SASL OFF CACHE INTERNAL "")
set(ENABLE_SNAPPY OFF CACHE INTERNAL "")
set(ENABLE_ZSTD OFF CACHE INTERNAL "")
set(ENABLE_SSL OFF CACHE INTERNAL "")

# Use FetchContent to download and build mongo-cxx-driver at configure time
FetchContent_Declare(
    mongo-cxx-driver
    GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
    GIT_TAG releases/v3.11
    GIT_SHALLOW TRUE
)

FetchContent_MakeAvailable(mongo-cxx-driver)

# The targets are now available: bsoncxx_shared
# Create aliases for easier use
if(TARGET bsoncxx_shared)
    add_library(bsoncxx ALIAS bsoncxx_shared)
    message(STATUS "bsoncxx target created as alias to bsoncxx_shared")
endif()

# Get source directories from FetchContent
FetchContent_GetProperties(mongo-cxx-driver SOURCE_DIR MONGO_CXX_SOURCE_DIR)

# Set the include directories for use by other CMakeLists that use include_directories()
# We need:
# - v_noabi directory for the main headers
# - base include for v1 headers
# - build/lib for generated config headers
set(BSONCXX_INCLUDE_DIR
    "${MONGO_CXX_SOURCE_DIR}/src/bsoncxx/include/bsoncxx/v_noabi"
    "${MONGO_CXX_SOURCE_DIR}/src/bsoncxx/include"
    "${CMAKE_BINARY_DIR}/3rdparty_download/mongo-cxx-driver-build/src/bsoncxx/lib/bsoncxx/v_noabi"
    "${CMAKE_BINARY_DIR}/3rdparty_download/mongo-c-driver-src/src/libbson/src/bson"
    "${CMAKE_BINARY_DIR}/3rdparty_download/mongo-c-driver-build/src/libbson/src/bson"
    CACHE INTERNAL "Path to bsoncxx include directories")

message("BSONCXX_INCLUDE_DIR: ${BSONCXX_INCLUDE_DIR}")
