# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

include_directories(${CMAKE_HOME_DIRECTORY}/src)
include_directories(${CMAKE_HOME_DIRECTORY}/src/index/knowhere)
include_directories(${CMAKE_HOME_DIRECTORY}/src/thirdparty)

add_definitions(-DMILVUS_TEST_SEGCORE_YAML_PATH="${CMAKE_SOURCE_DIR}/unittest/test_utils/test_segcore.yaml")

set(MILVUS_TEST_FILES
        init_gtest.cpp
        test_binary.cpp
        test_bitmap.cpp
        test_common.cpp
        test_concurrent_vector.cpp
        test_c_api.cpp
        test_expr.cpp
        test_get_entity_by_ids.cpp
        test_indexing.cpp
        test_index_wrapper.cpp
        test_init.cpp
        test_interface.cpp
        test_load.cpp
        test_naive.cpp
        test_plan_proto.cpp
        test_query.cpp
        test_reduce.cpp
        test_sealed.cpp
        test_segcore.cpp
        test_span.cpp
        test_timestamp_index.cpp
        test_reduce_c.cpp
        test_arrow.cpp)

add_executable(all_tests
        ${MILVUS_TEST_FILES}
        test_arrow.cpp)

# check if memory leak exists in index builder
set(INDEX_BUILDER_TEST_FILES
        test_index_wrapper.cpp
        )

add_executable(index_builder_test
        ${INDEX_BUILDER_TEST_FILES}
        )

target_link_libraries(index_builder_test
        gtest
        gtest_main
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        )

target_link_libraries(all_tests
        gtest
        milvus_segcore
        milvus_indexbuilder
        log
        pthread
        arrow
        )

install(TARGETS all_tests DESTINATION unittest)
install(TARGETS index_builder_test DESTINATION unittest)
