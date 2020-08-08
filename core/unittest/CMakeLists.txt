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
                    libstdc++fs.a
                    pthread
                    gfortran
                    opentracing
                    opentracing_mocktracer
                    fiu
                    dl
                    z
                    )

if ( MILVUS_GPU_VERSION )
    list( APPEND UNITTEST_LIBS   ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
            cudart
            cublas
            )
endif()

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
