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

set( GRPC_SERVICE_FILES ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
                        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
                        )


aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler            SCHEDULER_MAIN_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/action     SCHEDULER_ACTION_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/event      SCHEDULER_EVENT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/job        SCHEDULER_JOB_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/selector   SCHEDULER_SELECTOR_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/resource   SCHEDULER_RESOURCE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/scheduler/task       SCHEDULER_TASK_FILES )

set( SCHEDULER_FILES  ${SCHEDULER_MAIN_FILES}
                      ${SCHEDULER_ACTION_FILES}
                      ${SCHEDULER_EVENT_FILES}
                      ${SCHEDULER_JOB_FILES}
                      ${SCHEDULER_SELECTOR_FILES}
                      ${SCHEDULER_RESOURCE_FILES}
                      ${SCHEDULER_TASK_FILES}
                      )


aux_source_directory( ${MILVUS_ENGINE_SRC}/server                   SERVER_SERVICE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/init              SERVER_INIT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery/request  DELIVERY_REQUEST_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery/strategy DELIVERY_STRATEGY_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/delivery          DELIVERY_FILES )

set( SERVER_FILES     ${SERVER_INIT_FILES}
                      ${SERVER_SERVICE_FILES}
                      ${SERVER_INIT_FILES}
                      ${DELIVERY_REQUEST_FILES}
                      ${DELIVERY_STRATEGY_FILES}
                      ${DELIVERY_FILES}
                      )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server/grpc_impl             GRPC_IMPL_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/grpc_impl/interceptor GRPC_INTERCEPTOR_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/context               SERVER_CONTEXT_FILES )

set( GRPC_SERVER_FILES ${GRPC_REQUEST_FILES}
                       ${GRPC_IMPL_FILES}
                       ${GRPC_INTERCEPTOR_FILES}
                       )

aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/handler      WEB_HANDLER_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/component    WEB_CONPONENT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/controller   WEB_CONTROLLER_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/dto          WEB_DTO_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl/utils        WEB_UTILS_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/server/web_impl              WEB_IMPL_FILES )

set( WEB_SERVER_FILES ${WEB_HANDLER_FILES}
                      ${WEB_CONPONENT_FILES}
                      ${WEB_CONTROLLER_FILES}
                      ${WEB_DTO_FILES}
                      ${WEB_UTILS_FILES}
                      ${WEB_IMPL_FILES}
                      )

set( ENTRY_FILE ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp )

set( UNITTEST_LIBS  sqlite
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

if ( MILVUS_WITH_AWS )
    set( S3_CLIENT_LIB  aws-cpp-sdk-s3
                        aws-cpp-sdk-core
                        )

    list( APPEND UNITTEST_LIBS  ${S3_CLIENT_LIB}
                                curl
                                crypto
                                )
endif ()

add_subdirectory(db)
#add_subdirectory(metrics)
#add_subdirectory(scheduler)
#add_subdirectory(thirdparty)
#add_subdirectory(storage)
