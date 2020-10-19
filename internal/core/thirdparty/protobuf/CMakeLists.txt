# Copyright 2017 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if ( DEFINED ENV{MILVUS_GTEST_URL} )
    set( GTEST_SOURCE_URL "$ENV{MILVUS_GTEST_URL}" )
else()
    set( GTEST_SOURCE_URL
	 "https://github.com/protocolbuffers/protobuf/releases/download/v3.9.0/protobuf-cpp-3.9.0.zip")
endif()

message( STATUS "Building protobuf-cpp-3.9.0 from source" )

FetchContent_Declare(
    protobuf
    URL        ${GTEST_SOURCE_URL} 
    URL_MD5    "9562b27cc6ac5ebd087f201f1310c885"
    DOWNLOAD_DIR    ${MILVUS_BINARY_DIR}/3rdparty_download/download
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/protobuf-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/protobuf-build

)


if ( NOT protobuf_POPULATED )
    FetchContent_Populate( protobuf )

    add_subdirectory(${protobuf_SOURCE_DIR}/cmake 
	    ${protobuf_BINARY_DIR} 
	    EXCLUDE_FROM_ALL)

endif()


set(_PROTOBUF_LIBRARY_NAME "libprotobuf")

#if(TARGET libprotoc)
set(_gRPC_PROTOBUF_PROTOC_LIBRARIES libprotoc)
#endif()

set(gRPC_PROTOBUF_PROVIDER "module" CACHE STRING "Provider of protobuf library")
set_property(CACHE gRPC_PROTOBUF_PROVIDER PROPERTY STRINGS "module" "package")


#if(TARGET protoc)
set(_gRPC_PROTOBUF_PROTOC protoc)
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
#endif()

# Building the protobuf tests require gmock what is not part of a standard protobuf checkout.
# Disable them unless they are explicitly requested from the cmake command line (when we assume
# gmock is downloaded to the right location inside protobuf).
if(NOT protobuf_BUILD_TESTS)
set(protobuf_BUILD_TESTS OFF CACHE BOOL "Build protobuf tests")
endif()

# Disable building protobuf with zlib. Building protobuf with zlib breaks
# the build if zlib is not installed on the system.
if(NOT protobuf_WITH_ZLIB)
set(protobuf_WITH_ZLIB OFF CACHE BOOL "Build protobuf with zlib.")
endif()

if(NOT PROTOBUF_ROOT_DIR)
  #set(PROTOBUF_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf)
  set(PROTOBUF_ROOT_DIR ${protobuf_SOURCE_DIR})
endif()


set( PROTOC_EXCUTABLE           $<TARGET_FILE:protoc> )
set( GRPC_CPP_PLUGIN_EXCUTABLE  $<TARGET_FILE:grpc_cpp_plugin> )
#set( PROTO_INCLUDE_PATH "${MILVUS_SOURCE_DIR}/../proto/" )
set( PROTO_PATH          "${MILVUS_SOURCE_DIR}/../proto/" )
set( PROTO_OUTPUT_PATH          "${MILVUS_SOURCE_DIR}/src/pb/")

add_custom_target(generate_suvlim_pb_grpc ALL DEPENDS protoc)
message (STATUS "CURRENT SOURCE DIR" "${CMAKE_CURRENT_SOURCE_DIR}")

add_custom_command(TARGET generate_suvlim_pb_grpc
	POST_BUILD
	COMMAND echo "${PROTOC_EXCUTABLE}"

	COMMAND ${PROTOC_EXCUTABLE} -I "${PROTO_PATH}" --cpp_out "${PROTO_OUTPUT_PATH}"
		"message.proto" "master.proto"
		DEPENDS "${PROTO_PATH}/*.proto"
	)

set_property( GLOBAL PROPERTY PROTOC_EXCUTABLE ${PROTOC_EXCUTABLE})
