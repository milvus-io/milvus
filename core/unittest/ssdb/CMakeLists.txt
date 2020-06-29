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
        ${CMAKE_CURRENT_SOURCE_DIR}/test_snapshot.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp)

set(util_files
        ${MILVUS_ENGINE_SRC}/db/Utils.cpp)

add_executable(test_ssdb
        ${common_files}
        ${log_files}
        ${cache_files}
        ${config_files}
        ${config_handler_files}
        ${db_snapshot_files}
        ${metrics_files}
        ${metrics_prometheus_files}
#        ${thirdparty_files}
        ${util_files}
        ${test_files}
        )

target_link_libraries(test_ssdb
        metrics
        stdc++
        ${unittest_libs}
        )

install(TARGETS test_db DESTINATION unittest)
