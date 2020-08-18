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

if ( DEFINED ENV{MILVUS_CRC32C_URL} )
    set( CRC32C_SOURCE_URL "$ENV{MILVUS_CRC32C_URL}" )
endif()

message( STATUS "Building crc32c-master from source" )
FetchContent_Declare(
        crc32c
#        URL         ${CRC32C_SOURCE_URL}
#        URL_MD5     "b45bf1089a382e81f6b661062c10d0c2"
        SOURCE_DIR  ${CMAKE_CURRENT_BINARY_DIR}/crc32c-src
        BINARY_DIR  ${CMAKE_CURRENT_BINARY_DIR}/crc32c-build
)

FetchContent_GetProperties( crc32c )
if ( NOT crc32c_POPULATED )

    FetchContent_Populate( crc32c )
    set (CRC32C_BUILD_TESTS CACHE BOOL OFF FORCE)
    set (CRC32C_BUILD_BENCHMARKS CACHE BOOL OFF FORCE)
    add_subdirectory( ${crc32c_SOURCE_DIR}
            ${crc32c_BINARY_DIR}
            EXCLUDE_FROM_ALL )

endif()