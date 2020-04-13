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

set(test_files
        test_metricbase.cpp
        test_metrics.cpp
        utils.cpp
        )

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

if (MILVUS_WITH_PROMETHEUS)
    set(test_files ${test_files}
            test_prometheus.cpp)
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-access-control")
add_executable(test_metrics
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

target_link_libraries(test_metrics
        knowhere
        metrics
        ${unittest_libs}
        ${grpc_lib})

install(TARGETS test_metrics DESTINATION unittest)