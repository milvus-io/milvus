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

include_directories(${CUDA_TOOLKIT_ROOT_DIR}/include)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-status)
include_directories(${MILVUS_ENGINE_SRC}/grpc/gen-milvus)

#this statement must put here, since the CORE_INCLUDE_DIRS is defined in code/CMakeList.txt
add_subdirectory(core)
set(CORE_INCLUDE_DIRS ${CORE_INCLUDE_DIRS} PARENT_SCOPE)
foreach (dir ${CORE_INCLUDE_DIRS})
    include_directories(${dir})
endforeach ()

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
set(scheduler_files
        ${scheduler_main_files}
        ${scheduler_action_files}
        ${scheduler_event_files}
        ${scheduler_job_files}
        ${scheduler_resource_files}
        ${scheduler_task_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/server server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/server/grpc_impl grpc_server_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/utils utils_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper wrapper_files)

set(engine_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${metrics_files}
        ${utils_files}
        ${wrapper_files}
        )

set(client_grpc_lib
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc
        )

set(prometheus_lib
        prometheus-cpp-push
        prometheus-cpp-pull
        prometheus-cpp-core
        )

set(boost_lib
        boost_system_static
        boost_filesystem_static
        boost_serialization_static
        )

set(cuda_lib
        ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
        cudart
        cublas
        )

set(third_party_libs
        sqlite
        ${client_grpc_lib}
        yaml-cpp
        ${prometheus_lib}
        ${boost_lib}
        bzip2
        lz4
        snappy
        zlib
        zstd
        ${cuda_lib}
        mysqlpp
        )

if (MILVUS_ENABLE_PROFILING STREQUAL "ON")
    set(third_party_libs ${third_party_libs}
        gperftools
        libunwind
        )
endif ()

link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
set(engine_libs
        pthread
        libgomp.a
        libgfortran.a
        )

if (NOT ${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
    set(engine_libs
            ${engine_libs}
            libquadmath.a
            )
endif ()

cuda_add_library(milvus_engine STATIC ${engine_files})
target_link_libraries(milvus_engine
        knowhere
        ${engine_libs}
        ${third_party_libs}
        )

add_library(metrics STATIC ${metrics_files})

set(metrics_lib
        yaml-cpp
        ${prometheus_lib}
        )

target_link_libraries(metrics ${metrics_lib})

set(server_libs
        milvus_engine
        pthread
        dl
        metrics
        )

add_executable(milvus_server
        ${config_files}
        ${metrics_files}
        ${scheduler_files}
        ${server_files}
        ${grpc_server_files}
        ${grpc_service_files}
        ${utils_files}
        )

target_link_libraries(milvus_server
        ${server_libs}
        )

install(TARGETS milvus_server DESTINATION bin)

install(FILES
        ${CMAKE_SOURCE_DIR}/src/core/thirdparty/tbb/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_SOURCE_DIR}/src/core/thirdparty/tbb/${CMAKE_SHARED_LIBRARY_PREFIX}tbb${CMAKE_SHARED_LIBRARY_SUFFIX}.2
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3
        ${CMAKE_BINARY_DIR}/mysqlpp_ep-prefix/src/mysqlpp_ep/lib/${CMAKE_SHARED_LIBRARY_PREFIX}mysqlpp${CMAKE_SHARED_LIBRARY_SUFFIX}.3.2.4
        DESTINATION lib)

add_subdirectory(sdk)
