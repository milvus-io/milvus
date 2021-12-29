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

if ( DEFINED ENV{MILVUS_YAMLCPP_URL} )
    set( YAMLCPP_SOURCE_URL "$ENV{MILVUS_YAMLCPP_URL}" )
else()
    set( YAMLCPP_SOURCE_URL
        "https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-${YAMLCPP_VERSION}.tar.gz" )
endif()

message( STATUS "Building yaml-cpp-${YAMLCPP_VERSION} from source" )
FetchContent_Declare(
    yaml-cpp
    URL         ${YAMLCPP_SOURCE_URL}
    URL_MD5     "b45bf1089a382e81f6b661062c10d0c2"
    DOWNLOAD_DIR    ${THIRDPARTY_DOWNLOAD_PATH}
    SOURCE_DIR      ${CMAKE_CURRENT_BINARY_DIR}/yaml-src
    BINARY_DIR      ${CMAKE_CURRENT_BINARY_DIR}/yaml-build
    )

set( YAML_CPP_BUILD_TESTS CACHE BOOL OFF FORCE )
set( YAML_CPP_BUILD_TOOLS CACHE BOOL OFF FORCE )
FetchContent_GetProperties( yaml-cpp )
if ( NOT yaml-cpp_POPULATED )

    FetchContent_Populate( yaml-cpp )

    # Adding the following targets:
    # yaml-cpp::yaml-cpp, yaml-cpp
    add_subdirectory( ${yaml-cpp_SOURCE_DIR}
                      ${yaml-cpp_BINARY_DIR}
                      EXCLUDE_FROM_ALL )

endif()

get_target_property( YAML_CPP_INCLUDE_DIR yaml-cpp INTERFACE_INCLUDE_DIRECTORIES )
message( STATUS ${YAML_CPP_INCLUDE_DIR} )

get_property( var DIRECTORY "${yaml-cpp_SOURCE_DIR}" PROPERTY COMPILE_OPTIONS )
message( STATUS "yaml compile options: ${var}" )