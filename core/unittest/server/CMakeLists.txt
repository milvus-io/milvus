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

set(test_files
        ${CMAKE_CURRENT_SOURCE_DIR}/test_cache.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_config.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_rpc.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_util.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp)

include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

set(util_files
        ${MILVUS_ENGINE_SRC}/utils/StringHelpFunctions.cpp
        ${MILVUS_ENGINE_SRC}/utils/LogUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/SignalUtil.cpp)

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

set(server_test_files
        ${common_files}
        ${server_files}
        ${grpc_server_files}
        ${grpc_service_files}
        ${server_delivery_files}
        ${util_files}
        ${entry_file}
        ${test_files}
        )

add_executable(test_server ${server_test_files})

set(grpc_lib
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc)

target_link_libraries(test_server
        knowhere
        stdc++
        ${grpc_lib}
        ${unittest_libs}
        )

install(TARGETS test_server DESTINATION unittest)
