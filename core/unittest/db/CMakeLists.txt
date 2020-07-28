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

set(test_files
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_snapshot.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_segment.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_db.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_meta.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_job.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_task.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_ss_event.cpp
        )

add_executable(test_db
        ${scheduler_files}
        ${server_context_files}
        ${test_files}
        # ${grpc_server_files}
        # ${grpc_service_files}
        # ${web_server_files}
        # ${server_delivery_files}
        # ${server_files}
        # ${server_init_files}
        )

target_link_libraries(test_db
        metrics
        config
        stdc++
        ${unittest_libs}
        utils
        tracing
        query
        log
        milvus_engine
        )

install(TARGETS test_db DESTINATION unittest)
