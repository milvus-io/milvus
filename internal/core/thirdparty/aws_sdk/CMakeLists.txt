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

set ( AWS_SDK_VERSION "1.8.186" )
set ( AWS_SDK_SOURCE_URL 
      "https://github.com/aws/aws-sdk-cpp/archive/refs/tags/${AWS_SDK_VERSION}.tar.gz" )
set ( AWS_SDK_MD5 "ef4351b0969474cb85f39bc9f1975eb5" )

macro ( build_aws_sdk_s3 )
    message( STATUS "Building AWS-SDK-${AWS_SDK_VERSION} from source" )

    set ( AWS_SDK_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX} )

    set( AWS_SDK_BUILD_COMMAND make )
    set( AWS_SDK_INSTALL_COMMAND make install )

    set (AWS_SDK_S3_CMAKE_ARGS 
        "-DCMAKE_BUILD_TYPE=Release"
        "-DBUILD_ONLY=s3"
		"-DENABLE_TESTING=OFF"
        "-DAUTORUN_UNIT_TESTS=OFF"
        "-DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}" )

    ExternalProject_Add(aws_sdk_s3_ep
            URL                    ${AWS_SDK_SOURCE_URL}
            URL_MD5                ${AWS_SDK_MD5}
            BINARY_DIR             aws-s3-bin
            PREFIX                 ${CMAKE_BINARY_DIR}/3rdparty_download/aws-sdk-subbuild
            BUILD_COMMAND          ${AWS_SDK_BUILD_COMMAND}
            INSTALL_COMMAND        ${AWS_SDK_INSTALL_COMMAND}
            CMAKE_ARGS             ${AWS_SDK_S3_CMAKE_ARGS}
    )

    add_library(aws-cpp-sdk-s3 SHARED IMPORTED)
    set_target_properties(aws-cpp-sdk-s3
            PROPERTIES
            IMPORTED_GLOBAL TRUE
            IMPORTED_LOCATION ${AWS_SDK_INSTALL_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}aws-cpp-sdk-s3${CMAKE_SHARED_LIBRARY_SUFFIX}
            INTERFACE_INCLUDE_DIRECTORIES ${AWS_SDK_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR})
    add_dependencies(aws-cpp-sdk-s3 aws_sdk_s3_ep)

    get_target_property(S3_IMPORTED_LOCATION aws-cpp-sdk-s3 IMPORTED_LOCATION)
    get_target_property(S3_INTERFACE_INCLUDE_DIRECTORIES aws-cpp-sdk-s3 INTERFACE_INCLUDE_DIRECTORIES)
    message("AWS_SDK_INSTALL_PREFIX: ${AWS_SDK_INSTALL_PREFIX}")
    message("CMAKE_INSTALL_LIBDIR: ${CMAKE_INSTALL_LIBDIR}")
    message("S3_IMPORTED_LOCATION: ${S3_IMPORTED_LOCATION}")
    message("S3_INTERFACE_INCLUDE_DIRECTORIES: ${S3_INTERFACE_INCLUDE_DIRECTORIES}")
endmacro()


build_aws_sdk_s3()
