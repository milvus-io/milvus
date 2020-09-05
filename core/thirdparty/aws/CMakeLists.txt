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
if (DEFINED ENV{MILVUS_AWS_URL})
    set(AWS_SOURCE_URL "$ENV{MILVUS_AWS_URL}")
else ()
    set(AWS_SOURCE_URL "https://github.com/aws/aws-sdk-cpp/archive/${AWS_VERSION}.tar.gz")
endif ()

message(STATUS "Building aws-${AWS_VERSION} from source")
FetchContent_Declare(
        aws
        URL             ${AWS_SOURCE_URL}
        URL_MD5         "d7a3423ab2b7986290cdf63324080591"
        SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/aws-src
        BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/aws-build
        DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH} )

set( ENABLE_TESTING         OFF CACHE BOOL "" FORCE )
set( BUILD_SHARED_LIBS      OFF CACHE BOOL "" FORCE )
set( FORCE_SHARED_CRT       OFF CACHE BOOL "" FORCE )
set( ENABLE_UNITY_BUILD     OFF CACHE BOOL ""  FORCE )
set( BUILD_DEPS             ON  CACHE BOOL "" FORCE )
set( BUILD_ONLY            "s3" CACHE STRING "" FORCE )
set( AWS_DEPS_INSTALL_DIR   ${CMAKE_CURRENT_BINARY_DIR}/aws-deps-install )

FetchContent_GetProperties( aws )
if ( NOT aws_POPULATED )
    FetchContent_Populate( aws )


    # Adding the following targets:
    # AWS::aws-cpp-sdk-s3
    # AWS::aws-cpp-sdk-core
    add_subdirectory( ${aws_SOURCE_DIR}
                      ${aws_BINARY_DIR}
                      EXCLUDE_FROM_ALL )
endif()

get_target_property( AWS_INCLUDE_DIR  AWS::aws-cpp-sdk-core INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${AWS_INCLUDE_DIR} )

get_property( var DIRECTORY "${aws_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "aws compile options: ${var}" )
