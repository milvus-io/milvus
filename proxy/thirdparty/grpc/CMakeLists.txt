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

if ( DEFINED ENV{MILVUS_GRPC_URL} )
    set( GRPC_SOURCE_URL "$ENV{MILVUS_GRPC_URL}" )
else()
    set( GRPC_SOURCE_URL
         "https://github.com/milvus-io/grpc-milvus/archive/${GRPC_VERSION}.zip"
         )
endif()

message( STATUS "Building GRPC-${GRPC_VERSION} from source" )
FetchContent_Declare(
    grpc
    URL         ${GRPC_SOURCE_URL}
    URL_MD5     "f11a28bebdc14e7790dab64743a96836"
    DOWNLOAD_DIR ${MILVUS_BINARY_DIR}/3rdparty_download/download
    SOURCE_DIR  ${CMAKE_CURRENT_BINARY_DIR}/grpc-src
    BINARY_DIR  ${CMAKE_CURRENT_BINARY_DIR}/grpc-build )

# using openSSL rather than boringssl
set(gRPC_SSL_PROVIDER "package" CACHE STRING "OPENSSL" FORCE)
FetchContent_GetProperties( grpc )

if ( NOT grpc_POPULATED )
    FetchContent_Populate( grpc )

    # Adding the following targets:
    # libprotobuf
    # grpc++_reflection
    # protoc (excutable)
    # grpc++
    add_subdirectory( ${grpc_SOURCE_DIR}
                      ${grpc_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()

# get grpc COMPILE_OPTIONS )
get_property( var DIRECTORY "${grpc_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "grpc src compile options: ${var}" )


# **************************************************************************
# TODO grpc file dependee
# generate cc files
# **************************************************************************
set( PROTOC_EXCUTABLE           $<TARGET_FILE:protoc> )
set( GRPC_CPP_PLUGIN_EXCUTABLE  $<TARGET_FILE:grpc_cpp_plugin> )
set( PROTO_PATH          "${MILVUS_SOURCE_DIR}/src/grpc" )
set( PROTO_GEN_SCRIPTS_DIR "${PROJECT_SOURCE_DIR}/../scripts")

add_custom_target(generate_suvlim_pb_grpc ALL DEPENDS protoc grpc_cpp_plugin)

add_custom_command(TARGET generate_suvlim_pb_grpc
	POST_BUILD
	COMMAND echo "${PROTOC_EXCUTABLE}"
	COMMAND bash "${PROTO_GEN_SCRIPTS_DIR}/generate_go.sh" -p  "${PROTOC_EXCUTABLE}"
	COMMAND bash "${PROTO_GEN_SCRIPTS_DIR}/generate_cpp.sh" -p "${PROTOC_EXCUTABLE}" -g "${GRPC_CPP_PLUGIN_EXCUTABLE}"
	)

set_property( GLOBAL PROPERTY PROTOC_EXCUTABLE ${PROTOC_EXCUTABLE})
set_property( GLOBAL PROPERTY GRPC_CPP_PLUGIN_EXCUTABLE ${GRPC_CPP_PLUGIN_EXCUTABLE})
