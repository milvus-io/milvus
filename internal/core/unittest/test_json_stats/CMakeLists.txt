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

set(JSON_STATS_TEST_FILES
    ../init_gtest.cpp
    test_bson_builder.cpp
    test_bson_view.cpp
    test_parquet_writer.cpp
    test_utils.cpp
    test_json_key_stats.cpp
    test_traverse_json_for_build_stats.cpp
)

add_executable(json_stats_test
    ${JSON_STATS_TEST_FILES}
)

target_link_libraries(json_stats_test
    gtest
    gmock
    milvus_core
    milvus-storage
)

install(TARGETS json_stats_test DESTINATION unittest)
