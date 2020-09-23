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

include_directories(${MILVUS_ENGINE_SRC}/index)
include_directories(${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index)
include_directories(${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index)
include_directories(${MILVUS_ENGINE_SRC}/index/thirdparty/NGT/lib)
include_directories(${MILVUS_ENGINE_SRC}/index/unittest)
include_directories(${MILVUS_ENGINE_SRC}/metrics)

set( TEST_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_snapshot.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_segment.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_db.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_meta.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_event.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_transcript.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_wal.cpp
                )

set( index_files ${MILVUS_ENGINE_SRC}/index/unittest/utils.cpp
                 ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/IndexType.cpp
                 ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/IndexIVF.cpp
                 ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
                 ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/VecIndexFactory.cpp
                 )

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-access-control")

add_executable( test_db
                ${SCHEDULER_FILES}
                ${TEST_FILES}
                ${index_files}
                # ${grpc_server_files}
                # ${grpc_service_files}
                # ${web_server_files}
                # ${server_delivery_files}
                # ${server_files}
                # ${server_init_files}
                )

add_executable( test_segment
                ${SCHEDULER_FILES}
                ${index_files}
                ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_segment.cpp
                )

target_link_libraries( test_db  ${UNITTEST_LIBS}
                                server
                                 milvus_engine
                                 knowhere
                                 metrics
                                 config
                                 stdc++
                                 utils
                                 tracing
                                 query
                                 log
                                )

target_link_libraries( test_segment
        ${UNITTEST_LIBS}
        server
        milvus_engine
        knowhere
        metrics
        config
        stdc++
        utils
        tracing
        query
        log
        )

install( TARGETS test_db DESTINATION unittest )
