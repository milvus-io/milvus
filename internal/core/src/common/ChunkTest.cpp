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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <unistd.h>
#include <memory>
#include <optional>
#include <string>

#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/FieldMeta.h"
#include "common/File.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "storage/Event.h"
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
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_EQ(span.row_count(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
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
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_EQ(span.row_count(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
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
    auto string_chunk = static_cast<StringChunk*>(chunk.get());
    auto views = string_chunk->StringViews(std::nullopt);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.first[i], data[i]);
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
    auto string_chunk = static_cast<StringChunk*>(chunk.get());
    auto views = string_chunk->StringViews(std::nullopt);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.second[i], validity[i]);
        if (validity[i]) {
            EXPECT_EQ(views.first[i], data[i]);
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
            auto [views, valid] = json_chunk->StringViews(std::nullopt);
            EXPECT_EQ(row_num, views.size());
            for (size_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                //nullable is false, no judging valid
            }
        }
        {
            auto start = 10;
            auto len = 20;
            auto [views, valid] =
                json_chunk->StringViews(std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
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
            auto [views, valid] = json_chunk->StringViews(std::nullopt);
            EXPECT_EQ(row_num, views.size());
            for (size_t i = 0; i < row_num; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                EXPECT_TRUE(valid[i]);  //no input valid map, all padded as true
            }
        }
        {
            auto start = 10;
            auto len = 20;
            auto [views, valid] =
                json_chunk->StringViews(std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                EXPECT_TRUE(valid[i]);  //no input valid map, all padded as true
            }
        }
        {
            auto start = -1;
            auto len = 5;
            EXPECT_THROW(json_chunk->StringViews(std::make_pair(start, len)),
                         milvus::SegcoreError);
        }
        {
            auto start = 0;
            auto len = row_num + 1;
            EXPECT_THROW(json_chunk->StringViews(std::make_pair(start, len)),
                         milvus::SegcoreError);
        }
        {
            auto start = 95;
            auto len = 11;
            EXPECT_THROW(json_chunk->StringViews(std::make_pair(start, len)),
                         milvus::SegcoreError);
        }
    }
}

TEST(chunk, test_null_int64) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::INT64, DataType::NONE, true);

    // Set up validity bitmap: 10011 (1st, 4th, and 5th are valid)
    uint8_t* valid_data = new uint8_t[1]{0x13};  // 10011 in binary
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
                         DataType::INT64,
                         true,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, array_vec);
    auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
    auto span = fixed_chunk->Span();
    EXPECT_EQ(span.row_count(), data.size());

    // Check validity based on our bitmap pattern (10011)
    EXPECT_TRUE(fixed_chunk->isValid(0));
    EXPECT_TRUE(fixed_chunk->isValid(1));
    EXPECT_FALSE(fixed_chunk->isValid(2));
    EXPECT_FALSE(fixed_chunk->isValid(3));
    EXPECT_TRUE(fixed_chunk->isValid(4));

    // Verify data for valid entries
    for (size_t i = 0; i < data.size(); ++i) {
        if (fixed_chunk->isValid(i)) {
            auto n =
                *(int64_t*)((char*)span.data() + i * span.element_sizeof());
            EXPECT_EQ(n, data[i]);
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
    auto array_chunk = static_cast<ArrayChunk*>(chunk.get());
    auto [views, valid] = array_chunk->Views(std::nullopt);
    EXPECT_EQ(views.size(), 1);
    auto& arr = views[0];
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
    auto array_chunk = static_cast<ArrayChunk*>(chunk.get());
    auto [views, valid] = array_chunk->Views(std::nullopt);

    EXPECT_EQ(views.size(), array_count);
    EXPECT_EQ(valid.size(), array_count);

    // Check validity based on our bitmap pattern (10101)
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_FALSE(valid[3]);
    EXPECT_TRUE(valid[4]);

    // Verify data for valid arrays
    for (size_t i = 0; i < array_count; i++) {
        if (valid[i]) {
            auto& arr = views[i];
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
    auto array_chunk = static_cast<ArrayChunk*>(chunk.get());
    {
        auto [views, valid] = array_chunk->Views(std::nullopt);
        EXPECT_EQ(views.size(), array_count);
        for (auto i = 0; i < array_count; i++) {
            auto& arr = views[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        auto start = 2;
        auto len = 5;
        auto [views, valid] = array_chunk->Views(std::make_pair(start, len));
        EXPECT_EQ(views.size(), len);
        for (auto i = 0; i < len; i++) {
            auto& arr = views[i];
            for (size_t j = 0; j < arr.length(); ++j) {
                auto str = arr.get_data<std::string>(j);
                EXPECT_EQ(str, field_string_data.string_data().data(j));
            }
        }
    }
    {
        auto start = -1;
        auto len = 5;
        EXPECT_THROW(array_chunk->Views(std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 0;
        auto len = array_count + 1;
        EXPECT_THROW(array_chunk->Views(std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 5;
        auto len = 7;
        EXPECT_THROW(array_chunk->Views(std::make_pair(start, len)),
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