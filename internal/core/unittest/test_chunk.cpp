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
#include "common/Types.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"

using namespace milvus;

TEST(chunk, test_int64_field) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64);
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto span = std::dynamic_pointer_cast<FixedWidthChunk>(chunk)->Span();
    EXPECT_EQ(span.row_count(), data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
    }
}

TEST(chunk, test_variable_field) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VARCHAR);
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto views = std::dynamic_pointer_cast<StringChunk>(chunk)->StringViews(
        std::nullopt);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.first[i], data[i]);
    }
}

TEST(chunk, test_variable_field_nullable) {
    FixedVector<std::string> data = {
        "test1", "test2", "test3", "test4", "test5"};
    FixedVector<bool> validity = {true, false, true, false, true};

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::VARCHAR, true);
    uint8_t* valid_data = new uint8_t[1]{0x15};  // 10101 in binary
    field_data->FillFieldData(data.data(), valid_data, data.size());
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto views = std::dynamic_pointer_cast<StringChunk>(chunk)->StringViews(
        std::nullopt);
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
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::JSON);
    field_data->FillFieldData(data.data(), data.size());

    storage::InsertEventData event_data;
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    event_data.payload_reader = payload_reader;
    auto ser_data = event_data.Serialize();

    auto get_record_batch_reader =
        [&]() -> std::shared_ptr<::arrow::RecordBatchReader> {
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
        return rb_reader;
    };

    {
        auto rb_reader = get_record_batch_reader();
        // nullable=false
        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, 1, array_vec);
        {
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::nullopt);
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
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
            }
        }
    }
    {
        auto rb_reader = get_record_batch_reader();
        // nullable=true
        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::JSON,
                             true,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, 1, array_vec);
        {
            auto [views, valid] =
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::nullopt);
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
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len));
            EXPECT_EQ(len, views.size());
            for (size_t i = 0; i < len; ++i) {
                EXPECT_EQ(views[i], data[i].data());
                EXPECT_TRUE(valid[i]);  //no input valid map, all padded as true
            }
        }
        {
            auto start = -1;
            auto len = 5;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
        {
            auto start = 0;
            auto len = row_num + 1;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
        {
            auto start = 95;
            auto len = 11;
            EXPECT_THROW(
                std::dynamic_pointer_cast<JSONChunk>(chunk)->StringViews(
                    std::make_pair(start, len)),
                milvus::SegcoreError);
        }
    }
}

TEST(chunk, test_null_int64) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64, true);

    // Set up validity bitmap: 10011 (1st, 4th, and 5th are valid)
    uint8_t* valid_data = new uint8_t[1]{0x13};  // 10011 in binary
    field_data->FillFieldData(data.data(), valid_data, data.size());
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto fixed_chunk = std::dynamic_pointer_cast<FixedWidthChunk>(chunk);
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
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY);
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto [views, valid] =
        std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(std::nullopt);
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

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY, true);

    // Set up validity bitmap: 10101 (1st, 3rd, and 5th are valid)
    uint8_t* valid_data = new uint8_t[1]{0x15};  // 10101 in binary
    field_data->FillFieldData(data.data(), valid_data, data.size());
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
    auto chunk = create_chunk(field_meta, 1, array_vec);
    auto [views, valid] =
        std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(std::nullopt);

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

    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::ARRAY);
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
    auto chunk = create_chunk(field_meta, 1, array_vec);

    {
        auto [views, valid] =
            std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(std::nullopt);
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
        auto [views, valid] =
            std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                std::make_pair(start, len));
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
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 0;
        auto len = array_count + 1;
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
    {
        auto start = 5;
        auto len = 7;
        EXPECT_THROW(std::dynamic_pointer_cast<ArrayChunk>(chunk)->Views(
                         std::make_pair(start, len)),
                     milvus::SegcoreError);
    }
}

TEST(chunk, test_sparse_float) {
    auto n_rows = 100;
    auto vecs = milvus::segcore::GenerateRandomSparseFloatVector(
        n_rows, kTestSparseDim, kTestSparseVectorDensity);
    auto field_data = milvus::storage::CreateFieldData(
        storage::DataType::VECTOR_SPARSE_FLOAT, false, kTestSparseDim, n_rows);
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
                         DataType::VECTOR_SPARSE_FLOAT,
                         kTestSparseDim,
                         "IP",
                         false,
                         std::nullopt);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, kTestSparseDim, array_vec);
    auto vec = std::dynamic_pointer_cast<SparseFloatVectorChunk>(chunk)->Vec();
    for (size_t i = 0; i < n_rows; ++i) {
        auto v1 = vec[i];
        auto v2 = vecs[i];
        EXPECT_EQ(v1.size(), v2.size());
        for (size_t j = 0; j < v1.size(); ++j) {
            EXPECT_EQ(v1[j].val, v2[j].val);
        }
    }
}

class TempDir {
 public:
    TempDir() {
        auto path = boost::filesystem::unique_path("%%%%_%%%%");
        auto abs_path = boost::filesystem::temp_directory_path() / path;
        boost::filesystem::create_directory(abs_path);
        dir_ = abs_path;
    }

    ~TempDir() {
        boost::filesystem::remove_all(dir_);
    }

    std::string
    dir() {
        return dir_.string();
    }

 private:
    boost::filesystem::path dir_;
};

TEST(chunk, multiple_chunk_mmap) {
    TempDir temp;
    std::string temp_dir = temp.dir();
    auto file = File::Open(temp_dir + "/multi_chunk_mmap", O_CREAT | O_RDWR);

    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64);
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
    int file_offset = 0;
    auto page_size = sysconf(_SC_PAGESIZE);
    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    auto chunk = create_chunk(field_meta, 1, file, file_offset, array_vec);
    EXPECT_TRUE(chunk->Size() % page_size == 0);
    file_offset += chunk->Size();

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader2;
    s = arrow_reader->GetRecordBatchReader(&rb_reader2);
    EXPECT_TRUE(s.ok());
    arrow::ArrayVector array_vec2 = read_single_column_batches(rb_reader2);
    auto chunk2 = create_chunk(field_meta, 1, file, file_offset, array_vec2);
    EXPECT_TRUE(chunk->Size() % page_size == 0);
}