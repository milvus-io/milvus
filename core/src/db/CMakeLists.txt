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

aux_source_directory(${MILVUS_ENGINE_SRC}/cache cache_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db db_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/engine db_engine_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/insert db_insert_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/merge db_merge_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/wal db_wal_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/snapshot db_snapshot_files)

aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta db_meta_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/db/meta/backend db_meta_backend_files)
set(db_meta_files
        ${db_meta_main_files}
        ${db_meta_backend_files}
        )

aux_source_directory(${MILVUS_ENGINE_SRC}/storage storage_main_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/storage/disk storage_disk_files)
#aux_source_directory(${MILVUS_ENGINE_SRC}/storage/s3 storage_s3_files)
set(storage_files
        ${storage_main_files}
        ${storage_disk_files}
#        ${storage_s3_files}
        )
aux_source_directory(${MILVUS_ENGINE_SRC}/index/archive wrapper_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/codecs codecs_files)
aux_source_directory(${MILVUS_ENGINE_SRC}/segment segment_files)


aux_source_directory(${MILVUS_THIRDPARTY_SRC}/easyloggingpp thirdparty_easyloggingpp_files)
aux_source_directory(${MILVUS_THIRDPARTY_SRC}/nlohmann thirdparty_nlohmann_files)
aux_source_directory(${MILVUS_THIRDPARTY_SRC}/dablooms thirdparty_dablooms_files)

set(thirdparty_files
        ${thirdparty_easyloggingpp_files}
        ${thirdparty_nlohmann_files}
        ${thirdparty_dablooms_files}
        )

set(engine_files
        ${CMAKE_CURRENT_SOURCE_DIR}/../main.cpp
        ${cache_files}
        ${db_main_files}
        ${db_engine_files}
        ${db_insert_files}
        ${db_meta_files}
        ${db_merge_files}
        ${db_wal_files}
        ${db_snapshot_files}
        ${storage_files}
        ${thirdparty_files}
        ${wrapper_files}
        ${codecs_files}
        ${segment_files}
        )

set(engine_libs
        pthread
        libgomp.a
        libgfortran.a
        )

set(grpc_lib
        grpcpp_channelz
        grpc++
        grpc
        grpc_protobuf
        grpc_protoc
        )

set(boost_lib
        libboost_system.a
        libboost_filesystem.a
        libboost_serialization.a
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
    set(cuda_lib
            ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
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

if (MILVS_WITH_AWS)
    set(third_party_libs ${third_party_libs}
            ${s3_client_lib}
            curl
            crypto
            )
endif ()

if (NOT ${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
    set(engine_libs
            ${engine_libs}
            libquadmath.a
            )
endif ()

add_library(milvus_engine STATIC ${engine_files})
target_link_libraries(milvus_engine
    PUBLIC
        knowhere
        ${third_party_libs}
        ${engine_libs}
        )
