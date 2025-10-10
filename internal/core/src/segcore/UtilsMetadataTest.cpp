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
#include <memory>
#include <vector>
#include <string>

#include "segcore/Utils.h"
#include "storage/Types.h"

using namespace milvus;
using namespace milvus::segcore;

// Mock tests for Utils.cpp metadata parameter changes
// Note: These are interface tests since the actual functions require remote file system access

TEST(UtilsMetadata, LoadArrowReaderFromRemoteInterface) {
    // Test that LoadArrowReaderFromRemote accepts new metadata parameters without crashing
    std::vector<std::string> empty_files;  // Empty to avoid actual remote calls
    auto channel = std::make_shared<ArrowReaderChannel>(1000);

    // Test metadata structures
    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 1001;
    field_meta.partition_id = 2002;
    field_meta.segment_id = 3003;
    field_meta.field_id = 100;

    storage::IndexMeta index_meta;
    index_meta.segment_id = 3003;
    index_meta.field_id = 100;
    index_meta.field_type = DataType::INT32;
    index_meta.build_id = 5000;

    // Test with metadata parameters (should not crash, though it won't load anything)
    ASSERT_NO_THROW({
        LoadArrowReaderFromRemote(empty_files,
                                  channel,
                                  milvus::proto::common::LoadPriority::HIGH,
                                  field_meta,
                                  index_meta);
    });
}

TEST(UtilsMetadata, LoadFieldDatasFromRemoteInterface) {
    // Test that LoadFieldDatasFromRemote accepts new metadata parameters
    std::vector<std::string> empty_files;  // Empty to avoid actual remote calls
    auto channel = std::make_shared<FieldDataChannel>(1000);

    // Test metadata structures with different values
    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 7777;
    field_meta.partition_id = 8888;
    field_meta.segment_id = 9999;
    field_meta.field_id = 200;

    storage::IndexMeta index_meta;
    index_meta.segment_id = 9999;
    index_meta.field_id = 200;
    index_meta.field_type = DataType::VECTOR_FLOAT;
    index_meta.build_id = 6000;

    // Test with metadata parameters
    ASSERT_NO_THROW({
        LoadFieldDatasFromRemote(empty_files,
                                 channel,
                                 milvus::proto::common::LoadPriority::LOW,
                                 field_meta,
                                 index_meta);
    });
}

TEST(UtilsMetadata, MetadataStructureValues) {
    // Test that metadata structures hold correct values
    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 1234;
    field_meta.partition_id = 5678;
    field_meta.segment_id = 9012;
    field_meta.field_id = 3456;

    ASSERT_EQ(field_meta.collection_id, 1234);
    ASSERT_EQ(field_meta.partition_id, 5678);
    ASSERT_EQ(field_meta.segment_id, 9012);
    ASSERT_EQ(field_meta.field_id, 3456);

    storage::IndexMeta index_meta;
    index_meta.segment_id = 9012;
    index_meta.field_id = 3456;
    index_meta.field_type = DataType::STRING;
    index_meta.build_id = 7890;

    ASSERT_EQ(index_meta.segment_id, 9012);
    ASSERT_EQ(index_meta.field_id, 3456);
    ASSERT_EQ(index_meta.field_type, DataType::STRING);
    ASSERT_EQ(index_meta.build_id, 7890);
}

TEST(UtilsMetadata, MetadataEdgeCases) {
    // Test metadata with edge case values
    storage::FieldDataMeta field_meta;

    // Test with zero values
    field_meta.collection_id = 0;
    field_meta.partition_id = 0;
    field_meta.segment_id = 0;
    field_meta.field_id = 0;

    ASSERT_EQ(field_meta.collection_id, 0);
    ASSERT_EQ(field_meta.partition_id, 0);
    ASSERT_EQ(field_meta.segment_id, 0);
    ASSERT_EQ(field_meta.field_id, 0);

    // Test with negative values (deprecated)
    field_meta.collection_id = -1;
    field_meta.partition_id = -1;
    field_meta.segment_id = -1;
    field_meta.field_id = -1;

    ASSERT_EQ(field_meta.collection_id, -1);
    ASSERT_EQ(field_meta.partition_id, -1);
    ASSERT_EQ(field_meta.segment_id, -1);
    ASSERT_EQ(field_meta.field_id, -1);

    // Test with large values
    const int64_t large_value = 9223372036854775807LL;  // max int64
    field_meta.collection_id = large_value;
    field_meta.partition_id = large_value - 1;
    field_meta.segment_id = large_value - 2;
    field_meta.field_id = large_value - 3;

    ASSERT_EQ(field_meta.collection_id, large_value);
    ASSERT_EQ(field_meta.partition_id, large_value - 1);
    ASSERT_EQ(field_meta.segment_id, large_value - 2);
    ASSERT_EQ(field_meta.field_id, large_value - 3);
}

TEST(UtilsMetadata, LoadPriorityHandling) {
    // Test that different load priorities work with metadata
    std::vector<std::string> empty_files;
    auto channel = std::make_shared<FieldDataChannel>(1000);

    storage::FieldDataMeta field_meta;
    field_meta.collection_id = 1111;
    field_meta.partition_id = 2222;
    field_meta.segment_id = 3333;
    field_meta.field_id = 444;

    storage::IndexMeta index_meta;
    index_meta.segment_id = 3333;
    index_meta.field_id = 444;
    index_meta.field_type = DataType::DOUBLE;

    // Test HIGH priority
    ASSERT_NO_THROW({
        LoadFieldDatasFromRemote(empty_files,
                                 channel,
                                 milvus::proto::common::LoadPriority::HIGH,
                                 field_meta,
                                 index_meta);
    });

    // Test LOW priority
    ASSERT_NO_THROW({
        LoadFieldDatasFromRemote(empty_files,
                                 channel,
                                 milvus::proto::common::LoadPriority::LOW,
                                 field_meta,
                                 index_meta);
    });
}