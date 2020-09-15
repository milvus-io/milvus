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

set( TEST_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_snapshot.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_segment.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_db.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_meta.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_event.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_transcript.cpp
                ${CMAKE_CURRENT_SOURCE_DIR}/test_wal.cpp
                )

add_executable( test_db
                ${SCHEDULER_FILES}
                ${TEST_FILES}
                # ${grpc_server_files}
                # ${grpc_service_files}
                # ${web_server_files}
                # ${server_delivery_files}
                # ${server_files}
                # ${server_init_files}
                )

target_link_libraries( test_db  ${UNITTEST_LIBS}
                                server
                                 milvus_engine
                                 metrics
                                 config
                                 stdc++
                                 utils
                                 tracing
                                 query
                                 log
                                )

install( TARGETS test_db DESTINATION unittest )
