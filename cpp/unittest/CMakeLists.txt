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

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})

aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler scheduler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/context scheduler_context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/scheduler/task scheduler_task_files)
set(db_scheduler_files
        ${scheduler_files}
        ${scheduler_context_files}
        ${scheduler_task_files}
        )

set(grpc_service_files
        grpc/gen-milvus/milvus.grpc.pb.cc
        grpc/gen-milvus/milvus.pb.cc
        grpc/gen-status/status.grpc.pb.cc
        grpc/gen-status/status.pb.cc
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/metrics metrics_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/action scheduler_action_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/event scheduler_event_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/job scheduler_job_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler scheduler_root_files)
set(scheduler_files
        ${scheduler_action_files}
        ${scheduler_event_files}
        ${scheduler_job_files}
        ${scheduler_resource_files}
        ${scheduler_task_files}
        ${scheduler_root_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_server_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/utils utils_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/knowhere knowhere_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/db db_srcs)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)

set(unittest_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

set(helper_files
        ${MILVUS_ENGINE_SRC}/server/ServerConfig.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        )

set(unittest_libs
        yaml-cpp
        gtest
        gmock
        gtest_main
        gmock_main
        pthread
        metrics
        gfortran
        prometheus-cpp-pull
        prometheus-cpp-push
        prometheus-cpp-core
        dl
        z
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        cudart
        cublas
        )

foreach(dir ${CORE_INCLUDE_DIRS})
    include_directories(${dir})
endforeach()

add_subdirectory(db)
add_subdirectory(knowhere)
add_subdirectory(metrics)
add_subdirectory(scheduler)
add_subdirectory(server)
#add_subdirectory(storage)