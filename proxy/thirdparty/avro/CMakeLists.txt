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

if (DEFINED ENV{MILVUS_AVRO_URL})
    set(AVRO_URL "$ENV{MILVUS_AVRO_URL}")
else ()
    set(AVRO_URL
            "https://github.com/apache/avro/archive/${AVRO_VERSION}.zip")
endif ()


message(STATUS "Building avro-${AVRO_VERSION} from source")


FetchContent_Declare(
        avro
        URL ${AVRO_URL}
        #        URL_MD5 "f9137c5bc18b7d74027936f0f1bfa5c8"
        DOWNLOAD_DIR ${MILVUS_BINARY_DIR}/3rdparty_download/download
        SOURCE_DIR  ${CMAKE_CURRENT_BINARY_DIR}/avro-src
        BINARY_DIR  ${CMAKE_CURRENT_BINARY_DIR}/avro-build
)

include(FetchContent)
FetchContent_GetProperties( avro )
if (NOT avro_POPULATED)
    FetchContent_Populate(avro)

#    file(REMOVE ${avro_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt)
#    message("${pulsar_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt")
#    EXECUTE_PROCESS(COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists ${pulsar_SOURCE_DIR}/pulsar-client-cpp/CMakeLists.txt)

    # Adding the following targets:
    # pulsar-client-cpp
    add_subdirectory(${avro_SOURCE_DIR}/lang/c++
            ${avro_BINARY_DIR}
            EXCLUDE_FROM_ALL)

    file (INSTALL DIRECTORY ${avro_SOURCE_DIR}/lang/c++/api/ DESTINATION ${avro_BINARY_DIR}/include/avro
            FILES_MATCHING PATTERN *.hh)

#    target_include_directories(avrocpp PUBLIC ${pulsar_SOURCE_DIR}/pulsar-client-cpp/include)
endif ()

get_property(var DIRECTORY "${avro_SOURCE_DIR}/lang/c++" PROPERTY COMPILE_OPTIONS)
message(STATUS "avro compile options: ${var}")
