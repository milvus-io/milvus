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
if (FAISS_WITH_MKL)
    add_compile_definitions("WITH_MKL")
endif ()

set(INDEX_INCLUDE_DIRS ${INDEX_INCLUDE_DIRS} PARENT_SCOPE)
foreach (dir ${INDEX_INCLUDE_DIRS})
    include_directories(${dir})
endforeach ()

aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config config_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/config/handler config_handler_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/metrics metrics_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/metrics/prometheus metrics_prometheus_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/merge db_merge_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/wal db_wal_files)

set(grpc_service_files
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-milvus/milvus.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.grpc.pb.cc
        ${MILVUS_ENGINE_SRC}/grpc/gen-status/status.pb.cc
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/query query_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/context context_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/search search_files)

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

aux_source_directory(${MILVUS_THIRDPARTY_SRC}/easyloggingpp thirdparty_easyloggingpp_files)
aux_source_directory(${MILVUS_THIRDPARTY_SRC}/nlohmann thirdparty_nlohmann_files)
aux_source_directory(${MILVUS_THIRDPARTY_SRC}/dablooms thirdparty_dablooms_files)
set(thirdparty_files
        ${thirdparty_easyloggingpp_files}
        ${thirdparty_nlohmann_files}
        ${thirdparty_dablooms_files}
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

aux_source_directory(${MILVUS_ENGINE_SRC}/storage storage_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/disk storage_disk_files)
if(MILVUS_WITH_AWS)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 storage_s3_files)
endif()
if(MILVUS_WITH_OSS)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/oss storage_oss_files)
endif()

set(storage_files
        ${storage_main_files}
        ${storage_disk_files}
        ${storage_s3_files}
        ${storage_oss_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/utils utils_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/index/archive wrapper_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/tracing tracing_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/codecs codecs_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/codecs/default codecs_default_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/segment segment_files)

set(engine_files
        ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
        ${cache_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_merge_files}
        ${db_wal_files}
        ${metrics_files}
        ${storage_files}
        ${thirdparty_files}
        ${utils_files}
        ${wrapper_files}
        ${codecs_files}
        ${codecs_default_files}
        ${segment_files}
        )

if (MILVUS_WITH_PROMETHEUS)
    set(engine_files ${engine_files}
            ${metrics_prometheus_files})
endif ()

set(grpc_lib
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
        curl
        )

set(boost_lib
        libboost_system.a
        libboost_filesystem.a
        libboost_serialization.a
        )

set(s3_client_lib
        aws-cpp-sdk-s3
        aws-cpp-sdk-core
        )

set(oss_sdk_lib
        alibabacloud-oss-cpp-sdk
)

set(third_party_libs
        sqlite
        ${grpc_lib}
        yaml-cpp
        mysqlpp
        zlib
        fiu
        ${boost_lib}
        )

if (MILVUS_GPU_VERSION)
    include_directories(${CUDA_INCLUDE_DIRS})
    link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")
    set(cuda_lib
            nvidia-ml
            cudart
            cublas
            )
    set(third_party_libs ${third_party_libs}
            ${cuda_lib}
            )
    aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/gpu wrapper_gpu_files)
    set(engine_files ${engine_files}
            ${wrapper_gpu_files}
            )
endif ()

# cannot be enabled together with ENABLE_CPU_PROFILING
if (ENABLE_MEM_PROFILING)
    set(third_party_libs ${third_party_libs}
            tcmalloc
            )
endif ()

if (ENABLE_CPU_PROFILING)
    set(third_party_libs ${third_party_libs}
            gperftools
            libunwind
            )
endif ()

if (MILVUS_WITH_PROMETHEUS)
    set(third_party_libs ${third_party_libs}
            ${prometheus_lib}
            )
endif ()

if (MILVUS_WITH_AWS)
    add_compile_definitions(MILVUS_WITH_AWS)
    set(third_party_libs ${third_party_libs}
            ${s3_client_lib}
            curl
            crypto
            )
endif ()

if (MILVUS_WITH_OSS)
    add_compile_definitions(MILVUS_WITH_OSS)
    set(third_party_libs ${third_party_libs}
            ${oss_sdk_lib}
            curl
            crypto
            )
endif ()

set(engine_libs
        pthread
        libgomp.a
        libgfortran.a
        dl
        )

if (NOT ${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
    set(engine_libs
            ${engine_libs}
            libquadmath.a
            )
endif ()

add_library(milvus_engine STATIC ${engine_files})
add_dependencies(milvus_engine fiu)
target_link_libraries(milvus_engine
        knowhere
        ${third_party_libs}
        ${engine_libs}
        )

if (MILVUS_WITH_PROMETHEUS)
    add_library(metrics STATIC ${metrics_files} ${metrics_prometheus_files})
else ()
    add_library(metrics STATIC ${metrics_files})
endif ()
add_dependencies(metrics fiu)

set(metrics_lib
        yaml-cpp
        )

if (MILVUS_WITH_PROMETHEUS)
    set(metrics_lib ${metrics_lib}
            ${prometheus_lib}
            )
endif ()

target_link_libraries(metrics ${metrics_lib})

add_library(tracing STATIC ${tracing_files} ${thirdparty_files})
add_dependencies(tracing fiu)
set(tracing_lib
        opentracing
        opentracing_mocktracer
        ${grpc_lib}
        pthread
        z
        )
target_link_libraries(tracing ${tracing_lib})

set(server_libs
        milvus_engine
        metrics
        tracing
        oatpp
        )

add_executable(milvus_server
        ${config_files}
        ${config_handler_files}
        ${metrics_files}
        ${query_files}
        ${search_files}
        ${context_files}
        ${scheduler_files}
        ${server_files}
        ${grpc_server_files}
        ${grpc_service_files}
        ${web_server_files}
        ${server_context_files}
        ${utils_files}
        ${tracing_files}
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
