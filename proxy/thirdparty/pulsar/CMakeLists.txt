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

if (DEFINED ENV{MILVUS_PULSAR_URL})
    set(PULSAR_URL "$ENV{MILVUS_PULSAR_URL}")
else ()
    set(PULSAR_URL
            "https://github.com/apache/pulsar/archive/${PULSAR_CLIENT_CPP_VERSION}.zip")
endif ()


message(STATUS "Building pulsar-client-cpp-${PULSAR_CLIENT_CPP_VERSION} from source")
FetchContent_Declare(
        pulsar
        URL ${PULSAR_URL}
        #        URL_MD5 "f9137c5bc18b7d74027936f0f1bfa5c8"
        DOWNLOAD_DIR ${MILVUS_BINARY_DIR}/3rdparty_download/download
        SOURCE_DIR  ${CMAKE_CURRENT_BINARY_DIR}/pulsar-src
        BINARY_DIR  ${CMAKE_CURRENT_BINARY_DIR}/pulsar-build
)

include(FetchContent)
FetchContent_GetProperties( pulsar )

if (NOT pulsar_POPULATED)
    FetchContent_Populate(pulsar)

    file(REMOVE ${pulsar_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt)
    message("${pulsar_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt")
    EXECUTE_PROCESS(COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists ${pulsar_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt)

    SET(BUILD_TESTS CACHE BOOL OFF FORCE)
    # pulsar-cpp-client CMakeLists.txt need variables below, set them for build without install libprotobuf_dev
    SET(Protobuf_INCLUDE_DIRS CACHE PATH ${CMAKE_BINARY_DIR}/thirdparty/grpc/grpc-src/third_party/protobuf/src FORCE)
    SET(Protobuf_LITE_LIBRARIES CACHE PATH ${CMAKE_BINARY_DIR}/thirdparty/grpc/grpc-build/third_party/protobuf FORCE)

    # Adding the following targets:
    # pulsar-client-cpp
    add_subdirectory(${pulsar_SOURCE_DIR}/pulsar-client-cpp
            ${pulsar_BINARY_DIR}
            EXCLUDE_FROM_ALL)

    target_include_directories(pulsarStatic PUBLIC ${pulsar_SOURCE_DIR}/pulsar-client-cpp/include)
    target_include_directories(pulsarStatic PUBLIC ${pulsar_SOURCE_DIR}/pulsar-client-cpp)
    target_include_directories(pulsarStatic PUBLIC ${CMAKE_BINARY_DIR}/thirdparty/grpc/grpc-src/third_party/protobuf/src)
endif ()

get_property(var DIRECTORY "${pulsar_SOURCE_DIR}/pulsar-client-cpp" PROPERTY COMPILE_OPTIONS)
message(STATUS "pulsar-client-cpp compile options: ${var}")
