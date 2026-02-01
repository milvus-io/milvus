// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <stddef.h>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "cachinglayer/Utils.h"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "mmap/Types.h"
#include "pb/schema.pb.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"

using namespace milvus;
using namespace milvus::segcore::storagev1translator;

class DefaultValueChunkTranslatorTest : public ::testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        // Create a unique temp directory for mmap tests
        temp_dir_ = std::filesystem::temp_directory_path() /
                    ("milvus_param_test_" + std::to_string(segment_id_) + "_" +
                     std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(temp_dir_);
    }

    void
    TearDown() override {
        // Clean up temp directory
        if (std::filesystem::exists(temp_dir_)) {
            std::filesystem::remove_all(temp_dir_);
        }
    }

    // Helper to get mmap_dir_path based on use_mmap parameter
    std::string
    getMmapDirPath() const {
        return GetParam() ? temp_dir_.string() : "";
    }

    std::filesystem::path temp_dir_;
    int64_t segment_id_ = 12345;
};

// Test basic int64 field with default value
TEST_P(DefaultValueChunkTranslatorTest, TestInt64WithDefaultValue) {
    bool use_mmap = GetParam();
    int64_t row_count = 1000;
    int64_t default_value = 42;

    // Create field meta with default value
    DefaultValueType value_field;
    value_field.set_long_data(default_value);
    FieldMeta field_meta(FieldName("test_int64"),
                         FieldId(101),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(101, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    // Test num_cells
    EXPECT_GT(translator->num_cells(), 0);

    // Test cell_id_of - should be identical mapping
    for (size_t i = 0; i < translator->num_cells(); ++i) {
        EXPECT_EQ(translator->cell_id_of(i), i);
    }

    // Test estimated_byte_size_of_cell
    auto [usage, peak_usage] = translator->estimated_byte_size_of_cell(0);
    if (use_mmap) {
        EXPECT_GT(usage.file_bytes, 0);
    } else {
        EXPECT_GT(usage.memory_bytes, 0);
    }

    // Test get_cells
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    EXPECT_EQ(cid, 0);
    ASSERT_NE(chunk, nullptr);

    // Verify the chunk contains default values
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    for (size_t i = 0; i < span.row_count(); ++i) {
        auto value = *reinterpret_cast<int64_t*>((char*)span.data() +
                                                 i * span.element_sizeof());
        EXPECT_EQ(value, default_value);
    }
}

// Test int64 field without default value (nulls)
TEST_P(DefaultValueChunkTranslatorTest, TestInt64WithoutDefaultValue) {
    bool use_mmap = GetParam();
    int64_t row_count = 500;

    FieldMeta field_meta(FieldName("test_int64_null"),
                         FieldId(102),
                         DataType::INT64,
                         true,
                         std::nullopt);

    FieldDataInfo field_data_info(102, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    // All values should be marked as invalid (null)
    for (size_t i = 0; i < span.row_count(); ++i) {
        EXPECT_FALSE(fixed_chunk->isValid(i));
    }
}

// Test various fixed-width data types with default values
TEST_P(DefaultValueChunkTranslatorTest, TestVariousFixedWidthTypes) {
    bool use_mmap = GetParam();
    int64_t row_count = 100;

    // Test BOOL
    {
        DefaultValueType value_field;
        value_field.set_bool_data(true);
        FieldMeta field_meta(FieldName("test_bool"),
                             FieldId(201),
                             DataType::BOOL,
                             false,
                             value_field);
        FieldDataInfo field_data_info(201, row_count, getMmapDirPath());

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap, true);

        EXPECT_GT(translator->num_cells(), 0);
        EXPECT_GT(translator->value_size(), 0);
    }

    // Test INT32
    {
        DefaultValueType value_field;
        value_field.set_int_data(100);
        FieldMeta field_meta(FieldName("test_int32"),
                             FieldId(202),
                             DataType::INT32,
                             false,
                             value_field);
        FieldDataInfo field_data_info(202, row_count, getMmapDirPath());

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap, true);

        EXPECT_EQ(translator->value_size(), sizeof(int32_t));
    }

    // Test FLOAT
    {
        DefaultValueType value_field;
        value_field.set_float_data(3.14f);
        FieldMeta field_meta(FieldName("test_float"),
                             FieldId(203),
                             DataType::FLOAT,
                             false,
                             value_field);
        FieldDataInfo field_data_info(203, row_count, getMmapDirPath());

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap, true);

        EXPECT_EQ(translator->value_size(), sizeof(float));
    }

    // Test DOUBLE
    {
        DefaultValueType value_field;
        value_field.set_double_data(2.718281828);
        FieldMeta field_meta(FieldName("test_double"),
                             FieldId(204),
                             DataType::DOUBLE,
                             false,
                             value_field);
        FieldDataInfo field_data_info(204, row_count, getMmapDirPath());

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap, true);

        EXPECT_EQ(translator->value_size(), sizeof(double));
    }
}

// Test VARCHAR/STRING with default value
TEST_P(DefaultValueChunkTranslatorTest, TestStringWithDefaultValue) {
    bool use_mmap = GetParam();
    int64_t row_count = 200;
    std::string default_string = "default_value";

    DefaultValueType value_field;
    value_field.set_string_data(default_string);
    FieldMeta field_meta(FieldName("test_string"),
                         FieldId(301),
                         DataType::STRING,
                         false,
                         value_field);

    FieldDataInfo field_data_info(301, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_GT(translator->value_size(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    auto string_chunk = static_cast<StringChunk*>(chunk.get());
    auto [views, valid] = string_chunk->StringViews(std::nullopt);

    EXPECT_GT(views.size(), 0);
    for (const auto& view : views) {
        EXPECT_EQ(view, default_string);
    }
}

// Test VARCHAR without default value
TEST_P(DefaultValueChunkTranslatorTest, TestStringWithoutDefaultValue) {
    bool use_mmap = GetParam();
    int64_t row_count = 150;

    FieldMeta field_meta(FieldName("test_string_null"),
                         FieldId(302),
                         DataType::VARCHAR,
                         true,
                         std::nullopt);

    FieldDataInfo field_data_info(302, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    auto string_chunk = static_cast<StringChunk*>(chunk.get());
    auto [views, valid] = string_chunk->StringViews(std::nullopt);

    EXPECT_GT(views.size(), 0);
    // All should be marked as invalid (null)
    for (const auto& v : valid) {
        EXPECT_FALSE(v);
    }
}

// Test large row count that requires multiple cells
TEST_P(DefaultValueChunkTranslatorTest, TestMultipleCells) {
    bool use_mmap = GetParam();
    // Use a large row count to ensure multiple cells are created
    int64_t row_count = 10000000;  // 10 million rows

    DefaultValueType value_field;
    value_field.set_long_data(999);
    FieldMeta field_meta(FieldName("test_large"),
                         FieldId(401),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(401, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    // With 10M rows and int64 (8 bytes), we expect multiple cells
    // since target cell size is 64KB
    EXPECT_GT(translator->num_cells(), 1);

    // Test multiple cells
    std::vector<cachinglayer::cid_t> cids;
    for (size_t i = 0; i < std::min<size_t>(3, translator->num_cells()); ++i) {
        cids.push_back(i);
    }

    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), cids.size());

    int64_t total_rows = 0;
    for (const auto& [cid, chunk] : cells) {
        auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
        auto span = fixed_chunk->Span();
        total_rows += span.row_count();

        // Verify all values are the default
        for (size_t i = 0; i < span.row_count(); ++i) {
            auto value =
                *(int64_t*)((char*)span.data() + i * span.element_sizeof());
            EXPECT_EQ(value, 999);
        }
    }

    EXPECT_GT(total_rows, 0);
}

// Test small row count (single cell)
TEST_P(DefaultValueChunkTranslatorTest, TestSmallRowCount) {
    bool use_mmap = GetParam();
    int64_t row_count = 10;

    DefaultValueType value_field;
    value_field.set_int_data(55);
    FieldMeta field_meta(FieldName("test_small"),
                         FieldId(501),
                         DataType::INT32,
                         false,
                         value_field);

    FieldDataInfo field_data_info(501, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    // Small row count should result in a single cell
    EXPECT_EQ(translator->num_cells(), 1);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_EQ(span.row_count(), row_count);
}

// Test cells_storage_bytes
TEST_P(DefaultValueChunkTranslatorTest, TestCellsStorageBytes) {
    bool use_mmap = GetParam();
    int64_t row_count = 500;

    proto::schema::ValueField value_field;
    value_field.set_long_data(777);
    FieldMeta field_meta(FieldName("test_storage_bytes"),
                         FieldId(701),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(701, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    std::vector<cachinglayer::cid_t> cids = {0};
    // cells_storage_bytes always returns 0 for default-value chunks
    EXPECT_EQ(translator->cells_storage_bytes(cids), 0);
}

// Test get_cells with multiple cell IDs
TEST_P(DefaultValueChunkTranslatorTest, TestGetMultipleCells) {
    bool use_mmap = GetParam();
    int64_t row_count =
        DefaultValueChunkTranslator::kTargetCellBytes / sizeof(float) * 3;

    DefaultValueType value_field;
    value_field.set_float_data(1.5f);
    FieldMeta field_meta(FieldName("test_multi_cells"),
                         FieldId(801),
                         DataType::FLOAT,
                         false,
                         value_field);

    FieldDataInfo field_data_info(801, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    size_t num_cells = translator->num_cells();
    if (num_cells > 1) {
        std::vector<cachinglayer::cid_t> cids;
        for (size_t i = 0; i < num_cells; ++i) {
            cids.push_back(i);
        }

        auto cells = translator->get_cells(nullptr, cids);
        EXPECT_EQ(cells.size(), cids.size());

        for (size_t i = 0; i < cells.size(); ++i) {
            EXPECT_EQ(cells[i].first, cids[i]);
            ASSERT_NE(cells[i].second, nullptr);
            auto fixed_chunk =
                static_cast<FixedWidthChunk*>(cells[i].second.get());
            auto span = fixed_chunk->Span();
            EXPECT_GT(span.row_count(), 0);
            for (size_t j = 0; j < span.row_count(); ++j) {
                auto value = *reinterpret_cast<float*>(
                    (char*)span.data() + j * span.element_sizeof());
                EXPECT_EQ(value, 1.5f);
            }
            if (i > 0) {
                EXPECT_EQ(
                    cells[i].second->RawData() == cells[0].second->RawData(),
                    true);
            }
        }
    }
}

// Test JSON type
TEST_P(DefaultValueChunkTranslatorTest, TestJsonType) {
    bool use_mmap = GetParam();
    int64_t row_count = 100;

    FieldMeta field_meta(FieldName("test_json"),
                         FieldId(901),
                         DataType::JSON,
                         false,
                         std::nullopt);

    FieldDataInfo field_data_info(901, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_EQ(translator->value_size(), sizeof(Json));
}

// Test ARRAY type
TEST_P(DefaultValueChunkTranslatorTest, TestArrayType) {
    bool use_mmap = GetParam();
    int64_t row_count = 100;

    FieldMeta field_meta(FieldName("test_array"),
                         FieldId(902),
                         DataType::ARRAY,
                         DataType::INT32,
                         false,
                         std::nullopt);

    FieldDataInfo field_data_info(902, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_EQ(translator->value_size(), sizeof(Array));
}

// Test TIMESTAMPTZ type
TEST_P(DefaultValueChunkTranslatorTest, TestTimestamptzType) {
    bool use_mmap = GetParam();
    int64_t row_count = 200;

    DefaultValueType value_field;
    value_field.set_timestamptz_data(1234567890);  // Timestamp value
    FieldMeta field_meta(FieldName("test_timestamptz"),
                         FieldId(903),
                         DataType::TIMESTAMPTZ,
                         false,
                         value_field);

    FieldDataInfo field_data_info(903, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_EQ(translator->value_size(), sizeof(int64_t));

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    EXPECT_EQ(cells.size(), 1);

    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    for (size_t i = 0; i < span.row_count(); ++i) {
        auto value = *reinterpret_cast<int64_t*>((char*)span.data() +
                                                 i * span.element_sizeof());
        EXPECT_EQ(value, 1234567890);
    }
}

// Test with zero rows (edge case)
TEST_P(DefaultValueChunkTranslatorTest, TestZeroRows) {
    bool use_mmap = GetParam();
    int64_t row_count = 0;

    DefaultValueType value_field;
    value_field.set_long_data(0);
    FieldMeta field_meta(FieldName("test_zero"),
                         FieldId(1001),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(1001, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    // Zero rows should result in zero cells
    EXPECT_EQ(translator->num_cells(), 0);
}

// Test estimated_byte_size_of_cell for different cell indices
TEST_P(DefaultValueChunkTranslatorTest, TestEstimatedByteSizeMultipleCells) {
    bool use_mmap = GetParam();
    int64_t row_count = 5000000;  // Ensure multiple cells

    DefaultValueType value_field;
    value_field.set_long_data(42);
    FieldMeta field_meta(FieldName("test_byte_size"),
                         FieldId(1101),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(1101, row_count, getMmapDirPath());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap, true);

    size_t num_cells = translator->num_cells();
    if (num_cells > 1) {
        for (size_t i = 0; i < num_cells; ++i) {
            auto [usage, peak_usage] =
                translator->estimated_byte_size_of_cell(i);
            if (use_mmap) {
                EXPECT_GT(usage.file_bytes, 0);
            } else {
                EXPECT_GT(usage.memory_bytes, 0);
            }
        }
    }
}

// Parameterized test with both mmap modes
INSTANTIATE_TEST_SUITE_P(MmapModes,
                         DefaultValueChunkTranslatorTest,
                         testing::Bool());

// Non-parameterized test class for mmap file verification tests
class DefaultValueChunkTranslatorMmapTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create a unique temp directory for each test
        temp_dir_ =
            std::filesystem::temp_directory_path() /
            ("milvus_test_" + std::to_string(::testing::UnitTest::GetInstance()
                                                 ->current_test_info()
                                                 ->line()));
        std::filesystem::create_directories(temp_dir_);
    }

    void
    TearDown() override {
        // Clean up temp directory
        if (std::filesystem::exists(temp_dir_)) {
            std::filesystem::remove_all(temp_dir_);
        }
    }

    std::filesystem::path temp_dir_;
    int64_t segment_id_ = 99999;
};

// Test that mmap creates file on disk
TEST_F(DefaultValueChunkTranslatorMmapTest, TestMmapCreatesFile) {
    int64_t row_count = 1000;
    int64_t field_id = 101;
    int64_t default_value = 42;

    proto::schema::ValueField value_field;
    value_field.set_long_data(default_value);
    FieldMeta field_meta(FieldName("test_mmap_file"),
                         FieldId(field_id),
                         DataType::INT64,
                         false,
                         value_field);

    // Pass mmap_dir_path to FieldDataInfo
    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, true /* use_mmap */, true);

    // Trigger cell creation
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), 1);

    // Verify file was created
    auto expected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    EXPECT_TRUE(std::filesystem::exists(expected_file))
        << "Expected mmap file to be created at: " << expected_file;

    // Verify the chunk data is correct
    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    for (size_t i = 0; i < span.row_count(); ++i) {
        auto value =
            *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(value, default_value);
    }
}

// Test that non-mmap mode does not create file
TEST_F(DefaultValueChunkTranslatorMmapTest, TestNoMmapNoFile) {
    int64_t row_count = 1000;
    int64_t field_id = 102;
    int64_t default_value = 99;

    proto::schema::ValueField value_field;
    value_field.set_long_data(default_value);
    FieldMeta field_meta(FieldName("test_no_mmap_file"),
                         FieldId(field_id),
                         DataType::INT64,
                         false,
                         value_field);

    // Pass mmap_dir_path even though mmap is disabled
    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, false /* use_mmap */, true);

    // Trigger cell creation
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), 1);

    // Verify no file was created (memory-only mode)
    auto unexpected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    EXPECT_FALSE(std::filesystem::exists(unexpected_file))
        << "Expected no mmap file when use_mmap=false, but found: "
        << unexpected_file;

    // Verify the chunk data is still correct
    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    for (size_t i = 0; i < span.row_count(); ++i) {
        auto value =
            *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(value, default_value);
    }
}

// Test mmap with string type
TEST_F(DefaultValueChunkTranslatorMmapTest, TestMmapWithString) {
    int64_t row_count = 500;
    int64_t field_id = 103;
    std::string default_string = "mmap_default_value";

    proto::schema::ValueField value_field;
    value_field.set_string_data(default_string);
    FieldMeta field_meta(FieldName("test_mmap_string"),
                         FieldId(field_id),
                         DataType::VARCHAR,
                         false,
                         value_field);

    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, true /* use_mmap */, true);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), 1);

    // Verify file was created
    auto expected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    EXPECT_TRUE(std::filesystem::exists(expected_file))
        << "Expected mmap file for string type at: " << expected_file;

    // Verify string data
    auto& [cid, chunk] = cells[0];
    auto string_chunk = static_cast<StringChunk*>(chunk.get());
    auto [views, valid] = string_chunk->StringViews(std::nullopt);

    EXPECT_GT(views.size(), 0);
    for (size_t i = 0; i < views.size(); ++i) {
        EXPECT_EQ(views[i], default_string);
    }
}

// Test mmap with nullable field (nulls)
TEST_F(DefaultValueChunkTranslatorMmapTest, TestMmapWithNullableField) {
    int64_t row_count = 300;
    int64_t field_id = 104;

    FieldMeta field_meta(FieldName("test_mmap_nullable"),
                         FieldId(field_id),
                         DataType::INT64,
                         true,  // nullable
                         std::nullopt);

    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, true /* use_mmap */, true);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), 1);

    // Verify file was created
    auto expected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    EXPECT_TRUE(std::filesystem::exists(expected_file))
        << "Expected mmap file for nullable field at: " << expected_file;

    // Verify all values are null
    auto& [cid, chunk] = cells[0];
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_GT(span.row_count(), 0);

    for (size_t i = 0; i < span.row_count(); ++i) {
        EXPECT_FALSE(fixed_chunk->isValid(i));
    }
}

// Test mmap with multiple cells
TEST_F(DefaultValueChunkTranslatorMmapTest, TestMmapMultipleCells) {
    int64_t row_count = 10000000;  // Large enough for multiple cells
    int64_t field_id = 105;
    int64_t default_value = 12345;

    proto::schema::ValueField value_field;
    value_field.set_long_data(default_value);
    FieldMeta field_meta(FieldName("test_mmap_multi_cells"),
                         FieldId(field_id),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, true /* use_mmap */, true);

    // Ensure we have multiple cells
    size_t num_cells = translator->num_cells();
    ASSERT_GT(num_cells, 1) << "Expected multiple cells for large row count";

    // Request multiple cells
    std::vector<cachinglayer::cid_t> cids;
    for (size_t i = 0; i < std::min<size_t>(3, num_cells); ++i) {
        cids.push_back(i);
    }
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), cids.size());

    // Verify file was created (all cells share the same buffer/file)
    auto expected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    EXPECT_TRUE(std::filesystem::exists(expected_file))
        << "Expected mmap file at: " << expected_file;

    // Verify data in all cells
    for (const auto& [cid, chunk] : cells) {
        auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
        auto span = fixed_chunk->Span();
        EXPECT_GT(span.row_count(), 0);

        for (size_t i = 0; i < span.row_count(); ++i) {
            auto value =
                *(int64_t*)((char*)span.data() + i * span.element_sizeof());
            EXPECT_EQ(value, default_value);
        }
    }
}

// Test mmap file is properly sized
TEST_F(DefaultValueChunkTranslatorMmapTest, TestMmapFileSize) {
    int64_t row_count = 1000;
    int64_t field_id = 106;
    int64_t default_value = 77;

    proto::schema::ValueField value_field;
    value_field.set_long_data(default_value);
    FieldMeta field_meta(FieldName("test_mmap_file_size"),
                         FieldId(field_id),
                         DataType::INT64,
                         false,
                         value_field);

    FieldDataInfo field_data_info(field_id, row_count, temp_dir_.string());

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, true /* use_mmap */, true);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(nullptr, cids);
    ASSERT_EQ(cells.size(), 1);

    auto expected_file =
        temp_dir_ / fmt::format("seg_{}_f_{}_def", segment_id_, field_id);
    ASSERT_TRUE(std::filesystem::exists(expected_file));

    // File size should be at least row_count * sizeof(int64_t)
    auto file_size = std::filesystem::file_size(expected_file);
    EXPECT_GE(file_size, row_count * sizeof(int64_t))
        << "Mmap file size should be at least " << row_count * sizeof(int64_t)
        << " bytes";
}
