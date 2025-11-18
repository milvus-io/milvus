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

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "common/Chunk.h"
#include "common/FieldMeta.h"
#include "common/Types.h"
#include "mmap/Types.h"
#include "segcore/storagev1translator/DefaultValueChunkTranslator.h"
#include "pb/schema.pb.h"

using namespace milvus;
using namespace milvus::segcore::storagev1translator;

class DefaultValueChunkTranslatorTest : public ::testing::TestWithParam<bool> {
 protected:
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

    FieldDataInfo field_data_info(101, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    // Test num_cells
    EXPECT_GT(translator->num_cells(), 0);

    // Test cell_id_of - should be identical mapping
    for (size_t i = 0; i < translator->num_cells(); ++i) {
        EXPECT_EQ(translator->cell_id_of(i), i);
    }

    // Test estimated_byte_size_of_cell
    auto [usage, peak_usage] = translator->estimated_byte_size_of_cell(0);
    EXPECT_GT(usage.memory_bytes, 0);

    // Test get_cells
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(102, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    EXPECT_GT(translator->num_cells(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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
        FieldDataInfo field_data_info(201, row_count);

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap);

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
        FieldDataInfo field_data_info(202, row_count);

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap);

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
        FieldDataInfo field_data_info(203, row_count);

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap);

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
        FieldDataInfo field_data_info(204, row_count);

        auto translator = std::make_unique<DefaultValueChunkTranslator>(
            segment_id_, field_meta, field_data_info, use_mmap);

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

    FieldDataInfo field_data_info(301, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_GT(translator->value_size(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(302, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    EXPECT_GT(translator->num_cells(), 0);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(401, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    // With 10M rows and int64 (8 bytes), we expect multiple cells
    // since target cell size is 64KB
    EXPECT_GT(translator->num_cells(), 1);

    // Test multiple cells
    std::vector<cachinglayer::cid_t> cids;
    for (size_t i = 0; i < std::min<size_t>(3, translator->num_cells()); ++i) {
        cids.push_back(i);
    }

    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(501, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    // Small row count should result in a single cell
    EXPECT_EQ(translator->num_cells(), 1);

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(701, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    std::vector<cachinglayer::cid_t> cids = {0};
    // cells_storage_bytes always returns 0 for default-value chunks
    EXPECT_EQ(translator->cells_storage_bytes(cids), 0);
}

// Test get_cells with multiple cell IDs
TEST_P(DefaultValueChunkTranslatorTest, TestGetMultipleCells) {
    bool use_mmap = GetParam();
    int64_t row_count = 5000000;  // Ensure multiple cells

    DefaultValueType value_field;
    value_field.set_float_data(1.5f);
    FieldMeta field_meta(FieldName("test_multi_cells"),
                         FieldId(801),
                         DataType::FLOAT,
                         false,
                         value_field);

    FieldDataInfo field_data_info(801, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    size_t num_cells = translator->num_cells();
    if (num_cells > 1) {
        std::vector<cachinglayer::cid_t> cids;
        for (size_t i = 0; i < std::min<size_t>(5, num_cells); ++i) {
            cids.push_back(i);
        }

        auto cells = translator->get_cells(cids);
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
                EXPECT_EQ(cells[i].second.get() == cells[0].second.get(), true);
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

    FieldDataInfo field_data_info(901, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

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

    FieldDataInfo field_data_info(902, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

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

    FieldDataInfo field_data_info(903, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    EXPECT_GT(translator->num_cells(), 0);
    EXPECT_EQ(translator->value_size(), sizeof(int64_t));

    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = translator->get_cells(cids);
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

    FieldDataInfo field_data_info(1001, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

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

    FieldDataInfo field_data_info(1101, row_count);

    auto translator = std::make_unique<DefaultValueChunkTranslator>(
        segment_id_, field_meta, field_data_info, use_mmap);

    size_t num_cells = translator->num_cells();
    if (num_cells > 1) {
        for (size_t i = 0; i < num_cells; ++i) {
            auto [usage, peak_usage] =
                translator->estimated_byte_size_of_cell(i);
            EXPECT_GT(usage.memory_bytes, 0);
            EXPECT_GE(peak_usage.memory_bytes, usage.memory_bytes);
        }
    }
}

// Parameterized test with both mmap modes
INSTANTIATE_TEST_SUITE_P(MmapModes,
                         DefaultValueChunkTranslatorTest,
                         testing::Bool());
