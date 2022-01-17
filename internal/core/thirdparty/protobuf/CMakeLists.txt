# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set( PROTOBUF_SOURCE_URL
     "https://github.com/protocolbuffers/protobuf/releases/download/v3.9.0/protobuf-cpp-3.9.0.zip" )

message( STATUS "Building protobuf-cpp-3.9.0 from source" )

FetchContent_Declare( protobuf
    URL             ${PROTOBUF_SOURCE_URL}
    URL_MD5         "9562b27cc6ac5ebd087f201f1310c885"
    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/protobuf-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/protobuf-build
)

set( protobuf_BUILD_TESTS CACHE BOOL OFF FORCE )
set( protobuf_WITH_ZLIB CACHE BOOL OFF FORCE )
if ( NOT protobuf_POPULATED )
    FetchContent_Populate( protobuf )

    # Adding the following targets:
    # protobuf::libprotobuf - static target
    # protobuf::protoc - executable target
    add_subdirectory( ${protobuf_SOURCE_DIR}/cmake
                      ${protobuf_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()

get_target_property( PROTOBUF_INCLUDE_DIR protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS "protobuf interface include dir" ${PROTOBUF_INCLUDE_DIR} )

get_target_property( PROTOC_INCLUDE_DIR protobuf::protoc INCLUDE_DIRECTORIES )
message( STATUS "protoc interface include dir" ${PROTOC_INCLUDE_DIR} )
