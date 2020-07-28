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

foreach (dir ${INDEX_INCLUDE_DIRS})
    include_directories(${dir})
endforeach ()

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/search search_files)

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
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/selector scheduler_selector_files)
set(scheduler_files
        ${scheduler_main_files}
        ${scheduler_action_files}
        ${scheduler_event_files}
        ${scheduler_job_files}
        ${scheduler_resource_files}
        ${scheduler_task_files}
        ${scheduler_selector_files}
        )


aux_source_directory(${MILVUS_ENGINE_SRC}/server server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/init server_init_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_impl_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl/interceptor grpc_interceptor_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/context server_context_files)
set(grpc_server_files
        ${grpc_impl_files}
        ${grpc_interceptor_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl/handler web_handler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl/component web_conponent_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl/controller web_controller_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl/dto web_dto_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl/utils web_utils_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/web_impl web_impl_files)
set(web_server_files
        ${web_handler_files}
        ${web_conponent_files}
        ${web_controller_files}
        ${web_dto_files}
        ${web_utils_files}
        ${web_impl_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery server_delivery_impl_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery/request server_delivery_request_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery/strategy server_delivery_strategy_files)
set(server_delivery_files
        ${server_delivery_impl_files}
        ${server_delivery_request_files}
        ${server_delivery_strategy_files}
        )

set(entry_file
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp)

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
        gfortran
        opentracing
        opentracing_mocktracer
        fiu
        dl
        z
        )

if (MILVUS_WITH_AWS)
    set(s3_client_lib
            aws-cpp-sdk-s3
            aws-cpp-sdk-core
            )
    list(APPEND unittest_libs
            ${s3_client_lib}
            curl
            crypto
            )

endif ()

add_subdirectory(db)
#add_subdirectory(metrics)
#add_subdirectory(scheduler)
#add_subdirectory(thirdparty)
#add_subdirectory(storage)
