#-------------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#-------------------------------------------------------------------------------


aux_source_directory(interface interface_files)

aux_source_directory(util util_files)

include_directories(/usr/include)
include_directories(include)
include_directories(/usr/local/include)


aux_source_directory(grpc grpc_client_files)

include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus)
include_directories(${CMAKE_SOURCE_DIR}/src/grpc/gen-status)

set(grpc_service_files
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-milvus/milvus.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/src/grpc/gen-status/status.pb.cc
        )

add_library(milvus_sdk STATIC
        ${interface_files}
        ${grpc_client_files}
        ${util_files}
        ${grpc_service_files}
        )

target_link_libraries(milvus_sdk
        ${third_party_libs}
        )

install(TARGETS milvus_sdk DESTINATION lib)

add_subdirectory(examples)
