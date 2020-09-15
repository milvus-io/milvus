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

set(test_files
        ${CMAKE_CURRENT_SOURCE_DIR}/test_segment_rw.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/utils.cpp
        )

set(index_util_files
        ${MILVUS_ENGINE_SRC}/index/unittest/utils.cpp
        )

set(index_files
        ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/IndexType.cpp
        ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/IndexIVF.cpp
        ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/IndexIVFSQ.cpp
        ${MILVUS_ENGINE_SRC}/index/knowhere/knowhere/index/vector_index/VecIndexFactory.cpp
        )

set(metrics_files
        ${MILVUS_ENGINE_SRC}/metrics/Metrics.cpp
        )

aux_source_directory( ${MILVUS_ENGINE_SRC}/utils common_util_files )

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-access-control")  # access to private member function

add_executable(test_segment
        ${test_files}
        ${index_util_files}
        ${index_files}
        ${metrics_files}
        ${common_util_files}
        )

target_link_libraries(test_segment
        ${UNITTEST_LIBS}
        stdc++
        server
        milvus_engine
        metrics
        knowhere
        config
        utils
        tracing
        query
        log
        )

install(TARGETS test_segment DESTINATION unittest)
