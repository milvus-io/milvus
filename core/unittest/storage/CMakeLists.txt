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
        # ${CMAKE_CURRENT_SOURCE_DIR}/test_s3_client.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/test_disk.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
        )

include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

add_executable(test_storage
        ${test_files}
        )

target_link_libraries(test_storage
        ${UNITTEST_LIBS}
        stdc++
        server
        milvus_engine
        metrics
        config
        utils
        tracing
        query
        log
        )

install(TARGETS test_storage DESTINATION unittest)
