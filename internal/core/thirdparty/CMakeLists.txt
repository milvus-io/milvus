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
# Using default c and cxx compiler in our build tree
# Thirdpart cxx and c flags
append_flags(CMAKE_CXX_FLAGS FLAGS "-O3 -fPIC -Wno-error -fopenmp")

if (NOT KNOWHERE_VERBOSE_THIRDPARTY_BUILD)
    set(EP_LOG_OPTIONS LOG_CONFIGURE 1 LOG_BUILD 1 LOG_INSTALL 1 LOG_DOWNLOAD 1)
else ()
    set(EP_LOG_OPTIONS)
endif ()

set(MAKE_BUILD_ARGS "-j6")

include(FetchContent)
set(FETCHCONTENT_BASE_DIR  ${MILVUS_BINARY_DIR}/3rdparty_download)
set(FETCHCONTENT_QUIET     OFF)

if(CUSTOM_THIRDPARTY_DOWNLOAD_PATH)
    set(THIRDPARTY_DOWNLOAD_PATH ${CUSTOM_THIRDPARTY_DOWNLOAD_PATH})
else()
    set(THIRDPARTY_DOWNLOAD_PATH ${CMAKE_BINARY_DIR}/3rdparty_download/download)
endif()
message(STATUS "Thirdparty downloaded file path: ${THIRDPARTY_DOWNLOAD_PATH}")
# ----------------------------------------------------------------------
# Find pthreads

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_subdirectory(knowhere)

add_subdirectory(boost_ext)
add_subdirectory(rocksdb)
add_subdirectory(rdkafka)
add_subdirectory(simdjson)

if (LINUX)
    add_subdirectory(jemalloc)
endif()

