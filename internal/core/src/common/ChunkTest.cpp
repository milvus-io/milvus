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

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <simdjson.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "boost/filesystem/operations.hpp"
#include "common/Array.h"
#include "common/Chunk.h"
#include "common/ChunkDataView.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/object.h"
#include "knowhere/sparse_utils.h"
#include "pb/schema.pb.h"
#include "storage/Event.h"
#include "storage/PayloadReader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

using namespace milvus;

TEST(chunk, test_int64_field) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT64,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::INT64,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<int64_t>();
    EXPECT_EQ(data_view->RowCount(), data.size());
    auto view_data = data_view->Data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(view_data[i], data[i]);
    }
}

TEST(chunk, test_timestmamptz_field) {
    FixedVector<int64_t> data = {
        1, 2, 3, 4, 5};  // Timestamptz is stored as int64
    auto field_data =
        milvus::storage::CreateFieldData(DataType::TIMESTAMPTZ, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::TIMESTAMPTZ,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<int64_t>();
    EXPECT_EQ(data_view->RowCount(), data.size());
    auto view_data = data_view->Data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(view_data[i], data[i]);
    }
}

TEST(chunk, test_variable_field) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<std::string_view>();
    auto view_data = data_view->Data();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(view_data[i], data[i]);
    }
}

TEST(chunk, test_variable_field_nullable) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    FixedVector<bool> validity = {true, false, true, false, true};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE, true);
    uint8_t* valid_data = new uint8_t[1]{0x15};  // 10101 in binary
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);
    delete[] valid_data;

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         true,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<std::string_view>();
    auto view_data = data_view->Data();
    auto view_valid = data_view->ValidData();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(view_valid[i], validity[i]);
        if (validity[i]) {
            EXPECT_EQ(view_data[i], data[i]);
        }
    }
}

TEST(chunk, test_json_field) {
    auto row_num = 100;
    FixedVector<Json> data;
    data.reserve(row_num);
    std::string json_str = "{\"key\": \"value\"}";
    for (auto i = 0; i < row_num; i++) {
        auto json = Json(json_str.data(), json_str.size());
        data.push_back(std::move(json));
    }
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::JSON,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();

    auto get_record_batch_reader =
        [&]() -> std::pair<std::shared_ptr<::arrow::RecordBatchReader>,
                           std::unique_ptr<parquet::arrow::FileReader>> {
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());
        return {rb_reader, std::move(arrow_reader)};
    };

    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        // nullable=false
        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());
        {
            // Test full data view
            auto json_view = json_chunk->GetDataView<Json>();
            EXPECT_EQ(row_num, json_view->RowCount());
            auto view_data = json_view->Data();
            for (int64_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(view_data[i].data(), data[i].data());
            }
        }
        {
            // Test partial data view with offset and length
            int64_t start = 10;
            int64_t len = 20;
            auto json_view = json_chunk->GetDataView<Json>(start, len);
            EXPECT_EQ(len, json_view->RowCount());
            auto view_data = json_view->Data();
            for (int64_t i = 0; i < len; ++i) {
                EXPECT_EQ(view_data[i].data(), data[start + i].data());
            }
        }
    }
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        // nullable=true
        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             true,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());
        {
            // Test full data view with validity
            auto json_view = json_chunk->GetDataView<Json>();
            EXPECT_EQ(row_num, json_view->RowCount());
            auto view_data = json_view->Data();
            auto valid_data = json_view->ValidData();
            for (int64_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(view_data[i].data(), data[i].data());
                EXPECT_TRUE(valid_data[i]);  // all valid in test data
            }
        }
        {
            // Test partial data view with offset and length
            int64_t start = 10;
            int64_t len = 20;
            auto json_view = json_chunk->GetDataView<Json>(start, len);
            EXPECT_EQ(len, json_view->RowCount());
            auto view_data = json_view->Data();
            auto valid_data = json_view->ValidData();
            for (int64_t i = 0; i < len; ++i) {
                EXPECT_EQ(view_data[i].data(), data[start + i].data());
                EXPECT_TRUE(valid_data[i]);  // all valid in test data
            }
        }
        {
            // Test error cases: invalid offset
            int64_t start = -1;
            int64_t len = 5;
            EXPECT_THROW(json_chunk->GetDataView<Json>(start, len),
                         milvus::SegcoreError);
        }
        {
            // Test error cases: length exceeds bounds
            int64_t start = 0;
            int64_t len = row_num + 1;
            EXPECT_THROW(json_chunk->GetDataView<Json>(start, len),
                         milvus::SegcoreError);
        }
        {
            // Test error cases: offset + length exceeds bounds
            int64_t start = 95;
            int64_t len = 11;
            EXPECT_THROW(json_chunk->GetDataView<Json>(start, len),
                         milvus::SegcoreError);
        }
    }
}

// Test JsonChunk::GetAnyDataView returns ContiguousDataView<Json>
TEST(chunk, test_json_chunk_data_view) {
    auto row_num = 100;
    // Keep original strings alive so Json views remain valid for comparison
    std::vector<std::string> json_strings;
    json_strings.reserve(row_num);
    FixedVector<Json> data;
    data.reserve(row_num);
    for (auto i = 0; i < row_num; i++) {
        std::string json_str = "{\"key\": \"value" + std::to_string(i) +
                               "\", \"num\": " + std::to_string(i) + "}";
        json_strings.emplace_back(std::move(json_str));
        auto json =
            Json(json_strings.back().data(), json_strings.back().size());
        data.push_back(std::move(json));
    }
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::JSON,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();

    auto get_record_batch_reader =
        [&]() -> std::pair<std::shared_ptr<::arrow::RecordBatchReader>,
                           std::unique_ptr<parquet::arrow::FileReader>> {
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());
        return {rb_reader, std::move(arrow_reader)};
    };

    // Test GetAnyDataView() - full chunk
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        FieldMeta field_meta(FieldName("json_field"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());

        // GetAnyDataView should return ContiguousDataView<Json>
        auto any_view = json_chunk->GetAnyDataView();
        auto json_view = any_view.as<Json>();
        ASSERT_NE(json_view, nullptr);
        EXPECT_EQ(json_view->RowCount(), row_num);

        // Verify data content
        auto json_data = json_view->Data();
        for (int64_t i = 0; i < row_num; ++i) {
            EXPECT_EQ(json_data[i].data(), data[i].data());
        }
    }

    // Test GetAnyDataView(offset, length) - sub-range
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        FieldMeta field_meta(FieldName("json_field"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());

        int64_t start = 10;
        int64_t len = 20;
        auto any_view = json_chunk->GetAnyDataView(start, len);
        auto json_view = any_view.as<Json>();
        ASSERT_NE(json_view, nullptr);
        EXPECT_EQ(json_view->RowCount(), len);

        // Verify data content
        auto json_data = json_view->Data();
        for (int64_t i = 0; i < len; ++i) {
            EXPECT_EQ(json_data[i].data(), data[start + i].data());
        }
    }

    // Test GetAnyDataView(offsets) - specific offsets
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        FieldMeta field_meta(FieldName("json_field"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());

        FixedVector<int32_t> offsets = {0, 5, 10, 50, 99};
        auto any_view = json_chunk->GetAnyDataView(offsets);
        auto json_view = any_view.as<Json>();
        ASSERT_NE(json_view, nullptr);
        EXPECT_EQ(json_view->RowCount(), static_cast<int64_t>(offsets.size()));

        // Verify data content
        auto json_data = json_view->Data();
        for (size_t i = 0; i < offsets.size(); ++i) {
            EXPECT_EQ(json_data[i].data(), data[offsets[i]].data());
        }
    }

    // Test with nullable JSON
    {
        auto [rb_reader, arrow_reader] = get_record_batch_reader();
        FieldMeta field_meta(FieldName("json_field"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             true,  // nullable
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);
        auto json_chunk = static_cast<JSONChunk*>(chunk.get());

        auto any_view = json_chunk->GetAnyDataView();
        auto json_view = any_view.as<Json>();
        ASSERT_NE(json_view, nullptr);
        EXPECT_EQ(json_view->RowCount(), row_num);

        // ValidData should be non-null for nullable field
        // (all values are valid in this test data)
        auto valid_data = json_view->ValidData();
        ASSERT_NE(valid_data, nullptr);
        for (int64_t i = 0; i < row_num; ++i) {
            EXPECT_TRUE(valid_data[i]);
        }
    }
}

TEST(chunk, test_null_int64) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE, true);

    // Set up validity bitmap: 10011 (1st, 4th, and 5th are valid)
    uint8_t* input_valid = new uint8_t[1]{0x13};  // 10011 in binary
    field_data->FillFieldData(data.data(), input_valid, data.size(), 0);
    delete[] input_valid;

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::INT64,
                         true,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<int64_t>();
    EXPECT_EQ(data_view->RowCount(), data.size());
    auto view_data = data_view->Data();
    auto valid_data = data_view->ValidData();

    // Check validity based on our bitmap pattern (10011)
    EXPECT_TRUE(valid_data[0]);
    EXPECT_TRUE(valid_data[1]);
    EXPECT_FALSE(valid_data[2]);
    EXPECT_FALSE(valid_data[3]);
    EXPECT_TRUE(valid_data[4]);

    // Verify data for valid entries
    for (size_t i = 0; i < data.size(); ++i) {
        if (valid_data[i]) {
            EXPECT_EQ(view_data[i], data[i]);
        }
    }
}

TEST(chunk, test_array) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test_array1");
    field_string_data.mutable_string_data()->add_data("test_array2");
    field_string_data.mutable_string_data()->add_data("test_array3");
    field_string_data.mutable_string_data()->add_data("test_array4");
    field_string_data.mutable_string_data()->add_data("test_array5");
    auto string_array = Array(field_string_data);
    FixedVector<Array> data = {string_array};
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::ARRAY,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::ARRAY,
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<ArrayView>();
    EXPECT_EQ(data_view->RowCount(), 1);
    auto view_data = data_view->Data();
    auto& arr = view_data[0];
    for (size_t i = 0; i < arr.length(); ++i) {
        auto str = arr.get_data<std::string>(i);
        EXPECT_EQ(str, field_string_data.string_data().data(i));
    }
}

TEST(chunk, test_null_array) {
    // Create a test with some arrays being null
    auto array_count = 5;
    FixedVector<Array> data;
    data.reserve(array_count);

    // Create a string array to use for non-null values
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("test1");
    field_string_data.mutable_string_data()->add_data("test2");
    field_string_data.mutable_string_data()->add_data("test3");
    auto string_array = Array(field_string_data);

    for (int i = 0; i < array_count; i++) {
        data.emplace_back(string_array);
    }

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::ARRAY, DataType::NONE, true);

    // Set up validity bitmap: 10101 (1st, 3rd, and 5th are valid)
    uint8_t* valid_data = new uint8_t[1]{0x15};  // 10101 in binary
    field_data->FillFieldData(data.data(), valid_data, data.size(), 0);
    delete[] valid_data;

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::ARRAY,
                         DataType::STRING,
                         true,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto data_view = chunk->GetDataView<ArrayView>();

    EXPECT_EQ(data_view->RowCount(), array_count);
    auto view_data = data_view->Data();
    auto valid = data_view->ValidData();

    // Check validity based on our bitmap pattern (10101)
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_FALSE(valid[3]);
    EXPECT_TRUE(valid[4]);

    // Verify data for valid arrays
    for (int64_t i = 0; i < array_count; i++) {
        if (valid[i]) {
            auto& arr = view_data[i];
            EXPECT_EQ(arr.length(),
                      field_string_data.string_data().data_size());
            for (size_t j = 0; j < arr.length(); j++) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
}

TEST(chunk, test_array_views) {
    milvus::proto::schema::ScalarField field_string_data;
    field_string_data.mutable_string_data()->add_data("a");
    field_string_data.mutable_string_data()->add_data("b");
    field_string_data.mutable_string_data()->add_data("c");
    field_string_data.mutable_string_data()->add_data("d");
    field_string_data.mutable_string_data()->add_data("e");
    auto string_array = Array(field_string_data);

    auto array_count = 10;
    FixedVector<Array> data;
    data.reserve(array_count);
    for (int i = 0; i < array_count; i++) {
        data.emplace_back(string_array);
    }

    auto field_data = milvus::storage::CreateFieldData(storage::DataType::ARRAY,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("field1"),
                         milvus::FieldId(1),
                         DataType::ARRAY,
                         DataType::STRING,
                         true,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    {
        // Test full data view
        auto data_view = chunk->GetDataView<ArrayView>();
        EXPECT_EQ(data_view->RowCount(), array_count);
        auto view_data = data_view->Data();
        for (int64_t i = 0; i < array_count; i++) {
            auto& arr = view_data[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        // Test partial data view with offset and length
        int64_t start = 2;
        int64_t len = 5;
        auto data_view = chunk->GetDataView<ArrayView>(start, len);
        EXPECT_EQ(data_view->RowCount(), len);
        auto view_data = data_view->Data();
        for (int64_t i = 0; i < len; i++) {
            auto& arr = view_data[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        // Test error cases: invalid offset
        int64_t start = -1;
        int64_t len = 5;
        EXPECT_THROW(chunk->GetDataView<ArrayView>(start, len),
                     milvus::SegcoreError);
    }
    {
        // Test error cases: length exceeds bounds
        int64_t start = 0;
        int64_t len = array_count + 1;
        EXPECT_THROW(chunk->GetDataView<ArrayView>(start, len),
                     milvus::SegcoreError);
    }
    {
        // Test error cases: offset + length exceeds bounds
        int64_t start = 5;
        int64_t len = 7;
        EXPECT_THROW(chunk->GetDataView<ArrayView>(start, len),
                     milvus::SegcoreError);
    }
}

TEST(chunk, test_sparse_float) {
    auto n_rows = 100;
    auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
        n_rows, kTestSparseDim, kTestSparseVectorDensity);
    auto field_data =
        milvus::storage::CreateFieldData(DataType::VECTOR_SPARSE_U32_F32,
                                         DataType::NONE,
                                         false,
                                         kTestSparseDim,
                                         n_rows);
    field_data->FillFieldData(vecs.get(), n_rows);

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::VECTOR_SPARSE_U32_F32,
                         kTestSparseDim,
                         "IP",
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto vec_chunk = static_cast<SparseFloatVectorChunk*>(chunk.get());
    auto vec = vec_chunk->Vec();
    for (size_t i = 0; i < n_rows; ++i) {
        auto v1 = vec[i];
        auto v2 = vecs[i];
        EXPECT_EQ(v1.size(), v2.size());
        for (size_t j = 0; j < v1.size(); ++j) {
            EXPECT_EQ(v1[j].val, v2[j].val);
        }
    }
}

TEST(chunk, test_lower_bound_string) {
    // Test data: sorted strings
    FixedVector<std::string> data = {"apple",
                                     "banana",
                                     "cherry",
                                     "date",
                                     "elderberry",
                                     "fig",
                                     "grape",
                                     "honeydew",
                                     "kiwi",
                                     "lemon"};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto string_chunk = static_cast<StringChunk*>(chunk.get());

    // Test cases for lower_bound_string
    // Case 1: Target exists in the middle
    EXPECT_EQ(string_chunk->lower_bound_string("cherry"), 2);

    // Case 2: Target exists at the beginning
    EXPECT_EQ(string_chunk->lower_bound_string("apple"), 0);

    // Case 3: Target exists at the end
    EXPECT_EQ(string_chunk->lower_bound_string("lemon"), 9);

    // Case 4: Target doesn't exist, should return insertion point
    EXPECT_EQ(string_chunk->lower_bound_string("blueberry"),
              2);  // between banana and cherry
    EXPECT_EQ(string_chunk->lower_bound_string("mango"), 10);   // after lemon
    EXPECT_EQ(string_chunk->lower_bound_string("apricot"), 1);  // after apple

    // Case 5: Target is less than all elements
    EXPECT_EQ(string_chunk->lower_bound_string("aardvark"), 0);

    // Case 6: Target is greater than all elements
    EXPECT_EQ(string_chunk->lower_bound_string("zebra"), 10);

    // Case 7: Empty string edge case
    EXPECT_EQ(string_chunk->lower_bound_string(""), 0);

    // Case 8: Duplicate elements (if they existed, lower_bound would return first occurrence)
    // Since our data has no duplicates, test with existing elements
    EXPECT_EQ(string_chunk->lower_bound_string("banana"), 1);
}

TEST(chunk, test_upper_bound_string) {
    // Test data: sorted strings
    FixedVector<std::string> data = {"apple",
                                     "banana",
                                     "cherry",
                                     "date",
                                     "elderberry",
                                     "fig",
                                     "grape",
                                     "honeydew",
                                     "kiwi",
                                     "lemon"};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto string_chunk = static_cast<StringChunk*>(chunk.get());

    // Test cases for upper_bound_string
    // Case 1: Target exists in the middle
    EXPECT_EQ(string_chunk->upper_bound_string("cherry"),
              3);  // points to next element

    // Case 2: Target exists at the beginning
    EXPECT_EQ(string_chunk->upper_bound_string("apple"),
              1);  // points to next element

    // Case 3: Target exists at the end
    EXPECT_EQ(string_chunk->upper_bound_string("lemon"), 10);  // points to end

    // Case 4: Target doesn't exist, should return insertion point
    EXPECT_EQ(string_chunk->upper_bound_string("blueberry"),
              2);  // between banana and cherry
    EXPECT_EQ(string_chunk->upper_bound_string("mango"), 10);   // after lemon
    EXPECT_EQ(string_chunk->upper_bound_string("apricot"), 1);  // after apple

    // Case 5: Target is less than all elements
    EXPECT_EQ(string_chunk->upper_bound_string("aardvark"), 0);

    // Case 6: Target is greater than all elements
    EXPECT_EQ(string_chunk->upper_bound_string("zebra"), 10);

    // Case 7: Empty string edge case
    EXPECT_EQ(string_chunk->upper_bound_string(""), 0);

    // Case 8: Test with existing elements (upper_bound points to next element)
    EXPECT_EQ(string_chunk->upper_bound_string("banana"), 2);
    EXPECT_EQ(string_chunk->upper_bound_string("date"), 4);
}

TEST(chunk, test_binary_search_methods_edge_cases) {
    // Test with single element
    FixedVector<std::string> single_data = {"middle"};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(single_data.data(), single_data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto string_chunk = static_cast<StringChunk*>(chunk.get());

    // Test with single element
    EXPECT_EQ(string_chunk->lower_bound_string("middle"), 0);
    EXPECT_EQ(string_chunk->upper_bound_string("middle"), 1);
    EXPECT_EQ(string_chunk->lower_bound_string("before"), 0);
    EXPECT_EQ(string_chunk->upper_bound_string("before"), 0);
    EXPECT_EQ(string_chunk->lower_bound_string("next"), 1);
    EXPECT_EQ(string_chunk->upper_bound_string("next"), 1);
}

TEST(chunk, test_binary_search_methods_duplicates) {
    // Test with duplicate elements (if the data had duplicates)
    FixedVector<std::string> data = {
        "apple", "apple", "banana", "banana", "banana", "cherry"};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto string_chunk = static_cast<StringChunk*>(chunk.get());

    // Test with duplicates
    // lower_bound should return the first occurrence
    EXPECT_EQ(string_chunk->lower_bound_string("apple"), 0);
    EXPECT_EQ(string_chunk->lower_bound_string("banana"), 2);
    EXPECT_EQ(string_chunk->lower_bound_string("cherry"), 5);

    // upper_bound should return the position after the last occurrence
    EXPECT_EQ(string_chunk->upper_bound_string("apple"), 2);
    EXPECT_EQ(string_chunk->upper_bound_string("banana"), 5);
    EXPECT_EQ(string_chunk->upper_bound_string("cherry"), 6);
}

TEST(chunk, test_binary_search_methods_comparison) {
    // Test to verify the relationship between lower_bound and upper_bound
    FixedVector<std::string> data = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    FieldMeta field_meta(FieldName("a"),
                         milvus::FieldId(1),
                         DataType::STRING,
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto string_chunk = static_cast<StringChunk*>(chunk.get());

    // Test that upper_bound >= lower_bound for any target
    for (const auto& target :
         {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "z", ""}) {
        auto lb = string_chunk->lower_bound_string(target);
        auto ub = string_chunk->upper_bound_string(target);
        EXPECT_GE(ub, lb) << "For target: " << target;

        // For existing elements, upper_bound should be lower_bound + 1
        if (std::find(data.begin(), data.end(), target) != data.end()) {
            EXPECT_EQ(ub, lb + 1) << "For existing target: " << target;
        }
    }
}

TEST(chunk, test_create_group_chunk_basic) {
    // Prepare data for multiple fields
    size_t row_count = 5;

    // Field 1: INT64
    FixedVector<int64_t> int_data = {1, 2, 3, 4, 5};
    auto int_field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE);
    int_field_data->FillFieldData(int_data.data(), int_data.size());

    // Field 2: VARCHAR
    FixedVector<std::string> str_data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto str_field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE);
    str_field_data->FillFieldData(str_data.data(), str_data.size());

    // Field 3: DOUBLE
    FixedVector<double> double_data = {1.1, 2.2, 3.3, 4.4, 5.5};
    auto double_field_data = milvus::storage::CreateFieldData(
        storage::DataType::DOUBLE, DataType::NONE);
    double_field_data->FillFieldData(double_data.data(), double_data.size());

    // Create arrow arrays for each field
    std::vector<arrow::ArrayVector> array_vecs;

    // Process INT64 field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(int_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Process VARCHAR field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(str_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Process DOUBLE field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(double_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Create field metadata
    std::vector<FieldId> field_ids = {FieldId(1), FieldId(2), FieldId(3)};
    std::vector<FieldMeta> field_metas = {FieldMeta(FieldName("int_field"),
                                                    FieldId(1),
                                                    DataType::INT64,
                                                    false,
                                                    std::nullopt),
                                          FieldMeta(FieldName("str_field"),
                                                    FieldId(2),
                                                    DataType::STRING,
                                                    false,
                                                    std::nullopt),
                                          FieldMeta(FieldName("double_field"),
                                                    FieldId(3),
                                                    DataType::DOUBLE,
                                                    false,
                                                    std::nullopt)};

    // Create group chunk without mmap
    auto chunks = create_group_chunk(field_ids, field_metas, array_vecs);

    // Verify all chunks were created
    EXPECT_EQ(chunks.size(), 3);
    EXPECT_NE(chunks.find(FieldId(1)), chunks.end());
    EXPECT_NE(chunks.find(FieldId(2)), chunks.end());
    EXPECT_NE(chunks.find(FieldId(3)), chunks.end());

    // Verify INT64 chunk using DataView interface
    auto int_chunk = chunks[FieldId(1)].get();
    auto int_view = int_chunk->GetDataView<int64_t>();
    EXPECT_EQ(int_view->RowCount(), row_count);
    auto int_view_data = int_view->Data();
    for (size_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(int_view_data[i], int_data[i]);
    }

    // Verify STRING chunk using DataView interface
    auto str_chunk = chunks[FieldId(2)].get();
    auto str_view = str_chunk->GetDataView<std::string_view>();
    EXPECT_EQ(str_view->RowCount(), row_count);
    auto str_view_data = str_view->Data();
    for (size_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(str_view_data[i], str_data[i]);
    }

    // Verify DOUBLE chunk using DataView interface
    auto double_chunk = chunks[FieldId(3)].get();
    auto double_view = double_chunk->GetDataView<double>();
    EXPECT_EQ(double_view->RowCount(), row_count);
    auto double_view_data = double_view->Data();
    for (size_t i = 0; i < row_count; ++i) {
        EXPECT_DOUBLE_EQ(double_view_data[i], double_data[i]);
    }
}

TEST(chunk, test_create_group_chunk_with_mmap) {
    // Prepare data for multiple fields
    size_t row_count = 5;

    // Field 1: INT32
    FixedVector<int32_t> int32_data = {10, 20, 30, 40, 50};
    auto int32_field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT32, DataType::NONE);
    int32_field_data->FillFieldData(int32_data.data(), int32_data.size());

    // Field 2: FLOAT
    FixedVector<float> float_data = {1.5f, 2.5f, 3.5f, 4.5f, 5.5f};
    auto float_field_data = milvus::storage::CreateFieldData(
        storage::DataType::FLOAT, DataType::NONE);
    float_field_data->FillFieldData(float_data.data(), float_data.size());

    // Create arrow arrays for each field
    std::vector<arrow::ArrayVector> array_vecs;

    // Process INT32 field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(int32_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Process FLOAT field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(float_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Create field metadata
    std::vector<FieldId> field_ids = {FieldId(10), FieldId(11)};
    std::vector<FieldMeta> field_metas = {FieldMeta(FieldName("int32_field"),
                                                    FieldId(10),
                                                    DataType::INT32,
                                                    false,
                                                    std::nullopt),
                                          FieldMeta(FieldName("float_field"),
                                                    FieldId(11),
                                                    DataType::FLOAT,
                                                    false,
                                                    std::nullopt)};

    // Create group chunk with mmap
    std::string mmap_file = TestLocalPath + "test_group_chunk_mmap.bin";
    if (boost::filesystem::exists(mmap_file)) {
        boost::filesystem::remove(mmap_file);
    }
    auto chunks =
        create_group_chunk(field_ids, field_metas, array_vecs, true, mmap_file);

    // Verify all chunks were created
    EXPECT_EQ(chunks.size(), 2);
    EXPECT_NE(chunks.find(FieldId(10)), chunks.end());
    EXPECT_NE(chunks.find(FieldId(11)), chunks.end());

    // Verify INT32 chunk using DataView interface
    auto int32_chunk = chunks[FieldId(10)].get();
    auto int32_view = int32_chunk->GetDataView<int32_t>();
    EXPECT_EQ(int32_view->RowCount(), row_count);
    auto int32_view_data = int32_view->Data();
    for (size_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(int32_view_data[i], int32_data[i]);
    }

    // Verify FLOAT chunk using DataView interface
    auto float_chunk = chunks[FieldId(11)].get();
    auto float_view = float_chunk->GetDataView<float>();
    EXPECT_EQ(float_view->RowCount(), row_count);
    auto float_view_data = float_view->Data();
    for (size_t i = 0; i < row_count; ++i) {
        EXPECT_FLOAT_EQ(float_view_data[i], float_data[i]);
    }

    // Verify file exists
    EXPECT_TRUE(boost::filesystem::exists(mmap_file));

    // Clean up mmap file
    chunks.clear();

    // Verify file is removed by ChunkMmapGuard
    EXPECT_FALSE(boost::filesystem::exists(mmap_file));
}

TEST(chunk, test_create_group_chunk_nullable_fields) {
    // Test group chunk with nullable fields
    size_t row_count = 5;

    // Field 1: Nullable INT64
    FixedVector<int64_t> int_data = {1, 2, 3, 4, 5};
    auto int_field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE, true);
    uint8_t* int_valid_data = new uint8_t[1]{0x15};  // 10101 in binary
    int_field_data->FillFieldData(
        int_data.data(), int_valid_data, int_data.size(), 0);
    delete[] int_valid_data;

    // Field 2: Nullable VARCHAR
    FixedVector<std::string> str_data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto str_field_data = milvus::storage::CreateFieldData(
        storage::DataType::VARCHAR, DataType::NONE, true);
    uint8_t* str_valid_data = new uint8_t[1]{0x1A};  // 11010 in binary
    str_field_data->FillFieldData(
        str_data.data(), str_valid_data, str_data.size(), 0);
    delete[] str_valid_data;

    // Create arrow arrays
    std::vector<arrow::ArrayVector> array_vecs;

    // Process INT64 field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(int_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    // Process VARCHAR field
    {
        storage::InsertEventData event_data;
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(str_field_data);
        event_data.payload_reader = payload_reader;
        auto ser_data = event_data.Serialize();
        auto buffer = std::make_shared<arrow::io::BufferReader>(
            ser_data.data() + 2 * sizeof(milvus::Timestamp),
            ser_data.size() - 2 * sizeof(milvus::Timestamp));

        parquet::arrow::FileReaderBuilder reader_builder;
        auto s = reader_builder.Open(buffer);
        EXPECT_TRUE(s.ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        s = reader_builder.Build(&arrow_reader);
        EXPECT_TRUE(s.ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        s = arrow_reader->GetRecordBatchReader(&rb_reader);
        EXPECT_TRUE(s.ok());

        array_vecs.push_back(read_single_column_batches(rb_reader));
    }

    std::vector<FieldId> field_ids = {FieldId(30), FieldId(31)};
    std::vector<FieldMeta> field_metas = {FieldMeta(FieldName("nullable_int"),
                                                    FieldId(30),
                                                    DataType::INT64,
                                                    true,
                                                    std::nullopt),
                                          FieldMeta(FieldName("nullable_str"),
                                                    FieldId(31),
                                                    DataType::STRING,
                                                    true,
                                                    std::nullopt)};

    auto chunks = create_group_chunk(field_ids, field_metas, array_vecs);

    EXPECT_EQ(chunks.size(), 2);

    // Verify nullable INT64 chunk (validity: 10101) using DataView interface
    auto int_chunk = chunks[FieldId(30)].get();
    auto int_view = int_chunk->GetDataView<int64_t>();
    auto int_valid = int_view->ValidData();
    EXPECT_TRUE(int_valid[0]);
    EXPECT_FALSE(int_valid[1]);
    EXPECT_TRUE(int_valid[2]);
    EXPECT_FALSE(int_valid[3]);
    EXPECT_TRUE(int_valid[4]);

    // Verify nullable VARCHAR chunk (validity: 11010) using DataView interface
    auto str_chunk = chunks[FieldId(31)].get();
    auto str_view = str_chunk->GetDataView<std::string_view>();
    auto str_valid = str_view->ValidData();
    auto str_view_data = str_view->Data();
    EXPECT_FALSE(str_valid[0]);
    EXPECT_TRUE(str_valid[1]);
    EXPECT_FALSE(str_valid[2]);
    EXPECT_TRUE(str_valid[3]);
    EXPECT_TRUE(str_valid[4]);

    // Verify data for valid entries
    for (size_t i = 0; i < row_count; ++i) {
        if (str_valid[i]) {
            EXPECT_EQ(str_view_data[i], str_data[i]);
        }
    }
}