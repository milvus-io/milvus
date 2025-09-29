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

#include "segcore/storagev1translator/ChunkTranslator.h"
#include "storage/Types.h"
#include "common/Types.h"
#include "common/Schema.h"
#include "common/FieldMeta.h"

using namespace milvus;
using namespace milvus::segcore::storagev1translator;

// Mock test for ChunkTranslator metadata functionality
// Note: This tests the interface and metadata handling without requiring actual file operations

class ChunkTranslatorMetadataTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Set up basic test data
        segment_id_ = 1001;
        field_id_ = 100;

        // Create a simple field meta
        field_meta_ =
            FieldMeta("test_field", FieldId(field_id_), DataType::INT32, false);

        // Create test field data info
        field_data_info_.field_id = field_id_;
        field_data_info_.enable_mmap = false;
        field_data_info_.in_load_list = true;

        // Create empty file infos for testing (to avoid actual file operations)
        file_infos_.clear();

        // Set up test metadata
        field_data_meta_.collection_id = 2001;
        field_data_meta_.partition_id = 3001;
        field_data_meta_.segment_id = segment_id_;
        field_data_meta_.field_id = field_id_;

        index_meta_.segment_id = segment_id_;
        index_meta_.field_id = field_id_;
        index_meta_.field_type = DataType::INT32;
        index_meta_.build_id = 4001;
    }

    SegmentId segment_id_;
    FieldId field_id_;
    FieldMeta field_meta_;
    FieldDataInfo field_data_info_;
    std::vector<FileInfo> file_infos_;
    storage::FieldDataMeta field_data_meta_;
    storage::IndexMeta index_meta_;
};

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithMetadata) {
    // Test that ChunkTranslator can be constructed with metadata parameters
    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            field_meta_,
            field_data_info_,
            std::move(file_infos_),
            false,  // use_mmap
            milvus::proto::common::LoadPriority::HIGH,
            field_data_meta_,
            index_meta_);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithDifferentMetadata) {
    // Test construction with different metadata values
    storage::FieldDataMeta different_field_meta;
    different_field_meta.collection_id = 9999;
    different_field_meta.partition_id = 8888;
    different_field_meta.segment_id = 7777;
    different_field_meta.field_id = 666;

    storage::IndexMeta different_index_meta;
    different_index_meta.segment_id = 7777;
    different_index_meta.field_id = 666;
    different_index_meta.field_type = DataType::FLOAT;
    different_index_meta.build_id = 5555;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            field_meta_,
            field_data_info_,
            std::move(file_infos_),
            true,  // use_mmap
            milvus::proto::common::LoadPriority::LOW,
            different_field_meta,
            different_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithZeroMetadata) {
    // Test construction with zero metadata values
    storage::FieldDataMeta zero_field_meta;
    zero_field_meta.collection_id = 0;
    zero_field_meta.partition_id = 0;
    zero_field_meta.segment_id = 0;
    zero_field_meta.field_id = 0;

    storage::IndexMeta zero_index_meta;
    zero_index_meta.segment_id = 0;
    zero_index_meta.field_id = 0;
    zero_index_meta.field_type = DataType::NONE;
    zero_index_meta.build_id = 0;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            field_meta_,
            field_data_info_,
            std::move(file_infos_),
            false,  // use_mmap
            milvus::proto::common::LoadPriority::MEDIUM,
            zero_field_meta,
            zero_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithDeprecatedMetadata) {
    // Test construction with deprecated metadata values (-1)
    storage::FieldDataMeta deprecated_field_meta;
    deprecated_field_meta.collection_id = -1;
    deprecated_field_meta.partition_id = -1;
    deprecated_field_meta.segment_id = -1;
    deprecated_field_meta.field_id = -1;

    storage::IndexMeta deprecated_index_meta;
    deprecated_index_meta.segment_id = -1;
    deprecated_index_meta.field_id = -1;
    deprecated_index_meta.field_type = DataType::NONE;
    deprecated_index_meta.build_id = -1;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            field_meta_,
            field_data_info_,
            std::move(file_infos_),
            false,  // use_mmap
            milvus::proto::common::LoadPriority::HIGH,
            deprecated_field_meta,
            deprecated_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithLargeMetadata) {
    // Test construction with large metadata values
    const int64_t large_value = 9223372036854775807LL;  // max int64

    storage::FieldDataMeta large_field_meta;
    large_field_meta.collection_id = large_value;
    large_field_meta.partition_id = large_value - 1;
    large_field_meta.segment_id = large_value - 2;
    large_field_meta.field_id = large_value - 3;

    storage::IndexMeta large_index_meta;
    large_index_meta.segment_id = large_value - 2;
    large_index_meta.field_id = large_value - 3;
    large_index_meta.field_type = DataType::INT64;
    large_index_meta.build_id = large_value - 4;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            field_meta_,
            field_data_info_,
            std::move(file_infos_),
            true,  // use_mmap
            milvus::proto::common::LoadPriority::LOW,
            large_field_meta,
            large_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithMixedDataTypes) {
    // Test construction with different field data types
    auto float_field_meta =
        FieldMeta("float_field", FieldId(200), DataType::FLOAT, false);
    field_data_info_.field_id = 200;

    storage::FieldDataMeta float_field_data_meta;
    float_field_data_meta.collection_id = 1111;
    float_field_data_meta.partition_id = 2222;
    float_field_data_meta.segment_id = 3333;
    float_field_data_meta.field_id = 200;

    storage::IndexMeta float_index_meta;
    float_index_meta.segment_id = 3333;
    float_index_meta.field_id = 200;
    float_index_meta.field_type = DataType::FLOAT;
    float_index_meta.build_id = 4444;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            float_field_meta,
            field_data_info_,
            std::move(file_infos_),
            false,  // use_mmap
            milvus::proto::common::LoadPriority::HIGH,
            float_field_data_meta,
            float_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, ConstructorWithVectorField) {
    // Test construction with vector field type
    auto vector_field_meta = FieldMeta(
        "vector_field", FieldId(300), DataType::VECTOR_FLOAT, 128, false);
    field_data_info_.field_id = 300;

    storage::FieldDataMeta vector_field_data_meta;
    vector_field_data_meta.collection_id = 5555;
    vector_field_data_meta.partition_id = 6666;
    vector_field_data_meta.segment_id = 7777;
    vector_field_data_meta.field_id = 300;

    storage::IndexMeta vector_index_meta;
    vector_index_meta.segment_id = 7777;
    vector_index_meta.field_id = 300;
    vector_index_meta.field_type = DataType::VECTOR_FLOAT;
    vector_index_meta.build_id = 8888;

    ASSERT_NO_THROW({
        auto translator = std::make_unique<ChunkTranslator>(
            segment_id_,
            vector_field_meta,
            field_data_info_,
            std::move(file_infos_),
            true,  // use_mmap
            milvus::proto::common::LoadPriority::MEDIUM,
            vector_field_data_meta,
            vector_index_meta);
    });
}

TEST_F(ChunkTranslatorMetadataTest, MetadataConsistency) {
    // Test that metadata values are consistent across constructor calls
    storage::FieldDataMeta consistent_field_meta;
    consistent_field_meta.collection_id = 12345;
    consistent_field_meta.partition_id = 67890;
    consistent_field_meta.segment_id = segment_id_;
    consistent_field_meta.field_id = field_id_;

    storage::IndexMeta consistent_index_meta;
    consistent_index_meta.segment_id = segment_id_;
    consistent_index_meta.field_id = field_id_;
    consistent_index_meta.field_type = DataType::INT32;
    consistent_index_meta.build_id = 11111;

    // Create multiple translators with the same metadata
    for (int i = 0; i < 3; ++i) {
        std::vector<FileInfo>
            empty_files;  // Fresh empty vector for each iteration

        ASSERT_NO_THROW({
            auto translator = std::make_unique<ChunkTranslator>(
                segment_id_,
                field_meta_,
                field_data_info_,
                std::move(empty_files),
                false,  // use_mmap
                milvus::proto::common::LoadPriority::HIGH,
                consistent_field_meta,
                consistent_index_meta);
        });
    }
}

TEST_F(ChunkTranslatorMetadataTest, DifferentLoadPriorities) {
    // Test that different load priorities work with metadata
    std::vector<milvus::proto::common::LoadPriority> priorities = {
        milvus::proto::common::LoadPriority::HIGH,
        milvus::proto::common::LoadPriority::MEDIUM,
        milvus::proto::common::LoadPriority::LOW};

    for (auto priority : priorities) {
        std::vector<FileInfo>
            empty_files;  // Fresh empty vector for each iteration

        ASSERT_NO_THROW({
            auto translator =
                std::make_unique<ChunkTranslator>(segment_id_,
                                                  field_meta_,
                                                  field_data_info_,
                                                  std::move(empty_files),
                                                  false,  // use_mmap
                                                  priority,
                                                  field_data_meta_,
                                                  index_meta_);
        }) << "Failed with priority: "
           << static_cast<int>(priority);
    }
}