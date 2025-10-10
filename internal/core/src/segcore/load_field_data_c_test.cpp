// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <cstdlib>

#include "segcore/load_field_data_c.h"
#include "common/LoadInfo.h"

TEST(CApiTest, LoadInfoTest) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());
    AppendLoadFieldInfo(c_load_info, 100, 100);
    EnableMmap(c_load_info, 100, true);

    EXPECT_TRUE(load_info->field_infos.at(100).enable_mmap);
}

TEST(CApiTest, LoadFieldDataContextTest) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());

    // Test setting context metadata
    auto status = AppendLoadFieldDataContext(c_load_info, 1001, 2002, 3003);
    EXPECT_EQ(status.error_code, 0);

    // Verify the context was set correctly
    EXPECT_EQ(load_info->collection_id, 1001);
    EXPECT_EQ(load_info->partition_id, 2002);
    EXPECT_EQ(load_info->segment_id, 3003);
}

TEST(CApiTest, LoadFieldDataContextEdgeCases) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());

    // Test with zero values
    auto status = AppendLoadFieldDataContext(c_load_info, 0, 0, 0);
    EXPECT_EQ(status.error_code, 0);
    EXPECT_EQ(load_info->collection_id, 0);
    EXPECT_EQ(load_info->partition_id, 0);
    EXPECT_EQ(load_info->segment_id, 0);

    // Test with negative values (deprecated values)
    status = AppendLoadFieldDataContext(c_load_info, -1, -1, -1);
    EXPECT_EQ(status.error_code, 0);
    EXPECT_EQ(load_info->collection_id, -1);
    EXPECT_EQ(load_info->partition_id, -1);
    EXPECT_EQ(load_info->segment_id, -1);

    // Test with large values
    status = AppendLoadFieldDataContext(c_load_info,
                                        9223372036854775807LL,
                                        9223372036854775806LL,
                                        9223372036854775805LL);
    EXPECT_EQ(status.error_code, 0);
    EXPECT_EQ(load_info->collection_id, 9223372036854775807LL);
    EXPECT_EQ(load_info->partition_id, 9223372036854775806LL);
    EXPECT_EQ(load_info->segment_id, 9223372036854775805LL);
}

TEST(CApiTest, LoadFieldDataContextMultipleCalls) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());

    // First call
    auto status = AppendLoadFieldDataContext(c_load_info, 1001, 2002, 3003);
    EXPECT_EQ(status.error_code, 0);
    EXPECT_EQ(load_info->collection_id, 1001);
    EXPECT_EQ(load_info->partition_id, 2002);
    EXPECT_EQ(load_info->segment_id, 3003);

    // Second call should overwrite the values
    status = AppendLoadFieldDataContext(c_load_info, 4004, 5005, 6006);
    EXPECT_EQ(status.error_code, 0);
    EXPECT_EQ(load_info->collection_id, 4004);
    EXPECT_EQ(load_info->partition_id, 5005);
    EXPECT_EQ(load_info->segment_id, 6006);
}

TEST(CApiTest, LoadFieldDataInfoDefaultValues) {
    // Test that LoadFieldDataInfo has correct default values for the new fields
    auto load_info = std::make_shared<LoadFieldDataInfo>();

    // New metadata fields should be initialized to -1 (deprecated value)
    EXPECT_EQ(load_info->collection_id, -1);
    EXPECT_EQ(load_info->partition_id, -1);
    EXPECT_EQ(load_info->segment_id, -1);
}

TEST(CApiTest, LoadFieldDataContextIntegrationWithExistingFields) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());

    // Set up some existing field data first
    AppendLoadFieldInfo(c_load_info, 100, 1000);
    EnableMmap(c_load_info, 100, true);

    // Verify existing functionality still works
    EXPECT_TRUE(load_info->field_infos.at(100).enable_mmap);
    EXPECT_EQ(load_info->field_infos.at(100).field_id, 100);

    // Now set context metadata
    auto status = AppendLoadFieldDataContext(c_load_info, 1001, 2002, 3003);
    EXPECT_EQ(status.error_code, 0);

    // Verify both old and new functionality work together
    EXPECT_EQ(load_info->collection_id, 1001);
    EXPECT_EQ(load_info->partition_id, 2002);
    EXPECT_EQ(load_info->segment_id, 3003);
    EXPECT_TRUE(load_info->field_infos.at(100).enable_mmap);
}

TEST(CApiTest, LoadFieldDataContextSuccessStatusFormat) {
    auto load_info = std::make_shared<LoadFieldDataInfo>();
    auto c_load_info = reinterpret_cast<CLoadFieldDataInfo*>(load_info.get());

    auto status = AppendLoadFieldDataContext(c_load_info, 1001, 2002, 3003);

    // Verify success status format matches milvus::SuccessCStatus()
    EXPECT_EQ(status.error_code, milvus::Success);
}
