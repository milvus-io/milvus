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

cmake_minimum_required(VERSION 3.16)
project(MilvusCMEKCipherPlugin LANGUAGES CXX)

include(CTest)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

if(NOT MILVUS_SOURCE_DIR)
    message(FATAL_ERROR "MILVUS_SOURCE_DIR is required")
endif()

add_library(CipherPlugin SHARED cipher_plugin.cpp)
target_compile_features(CipherPlugin PRIVATE cxx_std_17)
target_compile_options(CipherPlugin PRIVATE -fPIC)
target_include_directories(CipherPlugin PRIVATE
    "${MILVUS_SOURCE_DIR}/internal/core/src"
)
target_link_libraries(CipherPlugin PRIVATE OpenSSL::Crypto)
set_target_properties(CipherPlugin PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "CipherPlugin"
)

add_executable(CipherPluginTest cipher_plugin_test.cpp)
target_compile_features(CipherPluginTest PRIVATE cxx_std_17)
target_include_directories(CipherPluginTest PRIVATE
    "${MILVUS_SOURCE_DIR}/internal/core/src"
)
target_link_libraries(CipherPluginTest PRIVATE CipherPlugin)
add_test(NAME CipherPluginContract COMMAND CipherPluginTest)
