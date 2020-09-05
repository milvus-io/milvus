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

# **************************** Engine Source Files ****************************
aux_source_directory( ${MILVUS_ENGINE_SRC}/db               DB_MAIN_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/engine        DB_ENGINE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/insert        DB_INSERT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/merge         DB_MERGE_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/wal           DB_WAL_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/snapshot      DB_SNAPSHOT_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/transcript    DB_TRANSCRIPT_FILES )

aux_source_directory( ${MILVUS_ENGINE_SRC}/db/meta          DB_META_MAIN_FILES )
aux_source_directory( ${MILVUS_ENGINE_SRC}/db/meta/backend  DB_META_BACKEND_FILES )
set( DB_META_FILES  ${DB_META_MAIN_FILES}
                    ${DB_META_BACKEND_FILES}
                    )

aux_source_directory( ${MILVUS_ENGINE_SRC}/index/archive    WRAPPER_FILES )


aux_source_directory( ${MILVUS_THIRDPARTY_SRC}/easyloggingpp THIRDPARTY_EASYLOGGINGPP_FILES )
aux_source_directory( ${MILVUS_THIRDPARTY_SRC}/nlohmann      THIRDPARTY_NLOHMANN_FILES )

set( THIRDPARTY_FILES   ${THIRDPARTY_EASYLOGGINGPP_FILES}
                        ${THIRDPARTY_NLOHMANN_FILES}
                        )

set( ENGINE_FILES   ${DB_MAIN_FILES}
                    ${DB_ENGINE_FILES}
                    ${DB_INSERT_FILES}
                    ${DB_META_FILES}
                    ${DB_MERGE_FILES}
                    ${DB_WAL_FILES}
                    ${DB_SNAPSHOT_FILES}
                    ${DB_TRANSCRIPT_FILES}
                    ${THIRDPARTY_FILES}
                    ${WRAPPER_FILES}
                    )


# **************************** Add Target milvus engine ****************************
add_library(    milvus_engine STATIC)
target_sources( milvus_engine PRIVATE ${ENGINE_FILES})

target_include_directories( milvus_engine
    PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/engine"
            "${CMAKE_CURRENT_SOURCE_DIR}/insert"
            "${CMAKE_CURRENT_SOURCE_DIR}/merge"
            "${CMAKE_CURRENT_SOURCE_DIR}/meta"
            "${CMAKE_CURRENT_SOURCE_DIR}/meta/backend"
            "${CMAKE_CURRENT_SOURCE_DIR}/snapshot"
            "${CMAKE_CURRENT_SOURCE_DIR}/wal"
            )

# **************************** Libraries to be linked with target Engine ****************************
set( ENGINE_LIBS    pthread
                    libstdc++fs.a
                    libgomp.a
                    libgfortran.a
                    )

set( GRPC_LIB   libprotobuf
                grpc++_reflection
                grpc++
                )

set( BOOST_LIB  libboost_system.a
                libboost_filesystem.a
                libboost_serialization.a
                )

set( THIRD_PARTY_LIBS   sqlite
                        yaml-cpp # log
                        mysqlpp
                        fiu
                        ${BOOST_LIB}
                        ${GRPC_LIB}
                        )

set( S3_CLIENT_LIB  AWS::aws-cpp-sdk-s3
                    AWS::aws-cpp-sdk-core )

if ( MILVUS_GPU_VERSION )
    set( CUDA_LIB   ${CUDA_TOOLKIT_ROOT_DIR}/lib64/stubs/libnvidia-ml.so
                    cudart
                    cublas
                    )

    list( APPEND THIRD_PARTY_LIBS ${CUDA_LIB} )
    aux_source_directory( ${MILVUS_ENGINE_SRC}/wrapper/gpu WRAPPER_GPU_FILES )
    target_sources( milvus_engine PUBLIC ${WRAPPER_GPU_FILES} )
endif ()

if ( ENABLE_CPU_PROFILING )
    list( APPEND THIRD_PARTY_LIBS
                 PUBLIC  gperftools )
endif ()

if ( MILVS_WITH_AWS )
    list(APPEND THIRD_PARTY_LIBS
                PUBLIC  ${S3_CLIENT_LIB}
                        curl
                        crypto
                        )
endif ()

if ( NOT ${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64" )
    list( APPEND THIRD_PARTY_LIBS libquadmath.a )
endif ()


# **************************** Link Libraries with milvus engine ****************************
target_link_libraries( milvus_engine
               PUBLIC  knowhere
                       server
                       segment
                       cache
                       codecs
                       storage
                       tracing
                       ${THIRD_PARTY_LIBS}
                       ${ENGINE_LIBS}
                       )
