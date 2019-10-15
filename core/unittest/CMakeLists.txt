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
if (CUSTOMIZATION)
    add_definitions(-DCUSTOMIZATION)
endif (CUSTOMIZATION)

include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")

foreach(dir ${CORE_INCLUDE_DIRS})
    include_directories(${dir})
endforeach()

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

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

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/utils utils_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_files)

set(entry_file
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

set(helper_files
        ${MILVUS_ENGINE_SRC}/server/Config.cpp
        ${MILVUS_ENGINE_SRC}/utils/CommonUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/TimeRecorder.cpp
        ${MILVUS_ENGINE_SRC}/utils/Status.cpp
        ${MILVUS_ENGINE_SRC}/utils/ValidationUtil.cpp
        ${MILVUS_ENGINE_SRC}/utils/easylogging++.cc
        )

set(common_files
        ${cache_files}
        ${config_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${metrics_files}
        ${scheduler_files}
        ${wrapper_files}
        ${helper_files}
        )

set(unittest_libs
        sqlite
        boost_system_static
        boost_filesystem_static
        lz4
        mysqlpp
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

add_subdirectory(db)
add_subdirectory(wrapper)
add_subdirectory(metrics)
add_subdirectory(scheduler)
add_subdirectory(server)