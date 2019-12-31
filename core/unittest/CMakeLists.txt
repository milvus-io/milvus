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

foreach (dir ${INDEX_INCLUDE_DIRS})
    include_directories(${dir})
endforeach ()

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/metrics metrics_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/action scheduler_action_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/event scheduler_event_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/job scheduler_job_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/optimizer scheduler_optimizer_files)
set(scheduler_files
        ${scheduler_main_files}
        ${scheduler_action_files}
        ${scheduler_event_files}
        ${scheduler_job_files}
        ${scheduler_resource_files}
        ${scheduler_task_files}
        ${scheduler_optimizer_files}
        )

aux_source_directory(${MILVUS_THIRDPARTY_SRC}/easyloggingpp thirdparty_easyloggingpp_files)
aux_source_directory(${MILVUS_THIRDPARTY_SRC}/nlohmann thirdparty_nlohmann_files)
set(thirdparty_files
        ${thirdparty_easyloggingpp_files}
        ${thirdparty_nlohmann_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_impl_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl/interceptor grpc_interceptor_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/context server_context_files)
set(grpc_server_files
        ${grpc_impl_files}
        ${grpc_interceptor_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery server_delivery_impl_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery/request server_delivery_request_files)
set(server_delivery_files
        ${server_delivery_impl_files}
        ${server_delivery_request_files})

aux_source_directory(${MILVUS_ENGINE_SRC}/utils utils_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/tracing tracing_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/storage storage_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/file storage_file_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 storage_s3_files)
set(storage_files
        ${storage_main_files}
        ${storage_file_files}
        ${storage_s3_files}
        )

set(entry_file
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

set(helper_files
        ${MILVUS_ENGINE_SRC}/server/Config.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        ${MILVUS_ENGINE_SRC}/utils/StringHelpFunctions.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_THIRDPARTY_SRC}/easyloggingpp/easylogging++.cc
        )

set(common_files
        ${cache_files}
        ${config_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${metrics_files}
        ${thirdparty_files}
        ${scheduler_files}
        ${wrapper_files}
        ${storage_files}
        ${helper_files}
        ${server_context_files}
        ${tracing_files}
        )

set(unittest_libs
        sqlite
        libboost_system.a
        libboost_filesystem.a
        mysqlpp
        yaml-cpp
        gtest
        gmock
        gtest_main
        gmock_main
        pthread
        metrics
        gfortran
        opentracing
        opentracing_mocktracer
        fiu
        aws-cpp-sdk-s3
        aws-cpp-sdk-core
        curl
        crypto
        )
if (MILVUS_WITH_PROMETHEUS)
    set(unittest_libs ${unittest_libs}
            prometheus-cpp-push
            prometheus-cpp-pull
            prometheus-cpp-core
            )
endif ()
set(unittest_libs ${unittest_libs}
    dl
    z
    )

if (MILVUS_GPU_VERSION)
    include_directories("${CUDA_INCLUDE_DIRS}")
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(unittest_libs ${unittest_libs}
            ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
            cudart
            cublas
            )
    aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/gpu wrapper_gpu_files)
    set(common_files ${common_files}
            ${wrapper_gpu_files}
            )
endif ()

add_subdirectory(db)
add_subdirectory(wrapper)
add_subdirectory(metrics)
add_subdirectory(scheduler)
add_subdirectory(server)
add_subdirectory(storage)
