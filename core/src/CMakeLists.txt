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

include_directories(${MILVUS_SOURCE_DIR})
include_directories(${MILVUS_ENGINE_SRC})
include_directories(${MILVUS_THIRDPARTY_SRC})

include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

set(FOUND_OPENBLAS "unknown")

add_subdirectory(index)
set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
foreach (dir ${INDEX_INCLUDE_DIRS})
    include_directories(${dir})
endforeach ()

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
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/selector scheduler_selector_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/resource scheduler_resource_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/scheduler/task scheduler_task_files)
set(scheduler_files
        ${scheduler_main_files}
        ${scheduler_action_files}
        ${scheduler_event_files}
        ${scheduler_job_files}
        ${scheduler_selector_files}
        ${scheduler_resource_files}
        ${scheduler_task_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_service_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/init server_init_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery/request delivery_request_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery/strategy delivery_strategy_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/delivery delivery_files)
set(server_files
        ${server_init_files}
        ${server_service_files}
        ${server_init_files}
        ${delivery_request_files}
        ${delivery_strategy_files}
        ${delivery_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_impl_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl/interceptor grpc_interceptor_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/context server_context_files)
set(grpc_server_files
        ${grpc_request_files}
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

set(s3_client_lib
        aws-cpp-sdk-s3
        aws-cpp-sdk-core
        )

add_subdirectory(utils)
add_subdirectory(metrics)
add_subdirectory(config)
add_subdirectory(tracing)
add_subdirectory(query)
add_subdirectory(db) # target milvus_engine
add_subdirectory(log)

# **************************** Get&Print Include Directories ****************************
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES )

foreach (dir ${dirs})
    message(STATUS "Current Include DIRS: "${dir})
endforeach ()


set(server_libs
        milvus_engine
        config
        metrics
        tracing
        log
        oatpp
        query
        utils
        )

add_executable(milvus_server
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${context_files}
        ${scheduler_files}
        ${server_files}
        ${grpc_server_files}
        ${grpc_service_files}
        ${web_server_files}
        ${server_context_files}
        )

target_link_libraries(milvus_server
        ${server_libs}
        )

install(TARGETS milvus_server DESTINATION bin)

install(FILES
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
        ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}.0
        ${CMAKE_BINARY_DIR}/fiu_ep-prefix/src/fiu_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}fiu${CMAKE_SHARED_LIBRARY_SUFFIX}.1.00
        DESTINATION lib)

if (FOUND_OPENBLAS STREQUAL "false")
    install(FILES
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}.0
        ${CMAKE_BINARY_DIR}/src/index/openblas_ep-prefix/src/openblas_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}openblas${CMAKE_SHARED_LIBRARY_SUFFIX}.0.3
        DESTINATION lib)
endif()
