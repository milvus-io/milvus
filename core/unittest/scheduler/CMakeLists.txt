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
        ${CMAKE_CURRENT_SOURCE_DIR}/test_algorithm.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_event.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_node.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_resource.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_resource_factory.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_resource_mgr.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_scheduler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_task.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_job.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_selector.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_tasktable.cpp)

add_executable(test_scheduler
        ${common_files}
        ${entry_file}
        ${test_files}
        )

target_link_libraries(test_scheduler
        knowhere
        metrics
        ${unittest_libs})

install(TARGETS test_scheduler DESTINATION unittest)

