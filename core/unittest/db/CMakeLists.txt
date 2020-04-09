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

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

set(test_files
        ${CMAKE_CURRENT_SOURCE_DIR}/test_db.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_db_mysql.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_wal.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_engine.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_mem.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_meta.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_meta_mysql.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_misc.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_search.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_delete.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_search_by_id.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp)

add_executable(test_db
        ${common_files}
        ${test_files}
        ${grpc_service_files}
        )

set(grpc_lib
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc
        )

target_link_libraries(test_db
        knowhere
        metrics
        ${unittest_libs}
        ${grpc_lib})

install(TARGETS test_db DESTINATION unittest)



