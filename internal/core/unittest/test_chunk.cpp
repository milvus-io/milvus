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

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::INT64, false);
    auto chunk = create_chunk(field_meta, 1, rb_reader);
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

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::STRING, false);
    auto chunk = create_chunk(field_meta, 1, rb_reader);
    auto views = std::dynamic_pointer_cast<StringChunk>(chunk)->StringViews();
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(views.first[i], data[i]);
    }
}

TEST(chunk, test_null_field) {
    FixedVector<int64_t> data = {1, 2, 3, 4, 5};
    auto field_data =
        milvus::storage::CreateFieldData(storage::DataType::INT64, true);
    uint8_t* valid_data_ = new uint8_t[1]{0x13};
    field_data->FillFieldData(data.data(), valid_data_, data.size());
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

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::INT64, true);
    auto chunk = create_chunk(field_meta, 1, rb_reader);
    auto span = std::dynamic_pointer_cast<FixedWidthChunk>(chunk)->Span();
    EXPECT_EQ(span.row_count(), data.size());
    data = {1, 2, 0, 0, 5};
    FixedVector<bool> valid_data = {true, true, false, false, true};
    for (size_t i = 0; i < data.size(); ++i) {
        auto n = *(int64_t*)((char*)span.data() + i * span.element_sizeof());
        EXPECT_EQ(n, data[i]);
        auto v = *(bool*)((char*)span.valid_data() + i);
        EXPECT_EQ(v, valid_data[i]);
    }
    delete[] valid_data_;
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
                         false);
    auto chunk = create_chunk(field_meta, 1, rb_reader);
    auto span = std::dynamic_pointer_cast<ArrayChunk>(chunk)->Span();
    EXPECT_EQ(span.row_count(), 1);
    auto arr = *(ArrayView*)span.data();
    for (size_t i = 0; i < arr.length(); ++i) {
        auto str = arr.get_data<std::string>(i);
        EXPECT_EQ(str, field_string_data.string_data().data(i));
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
                         false);
    auto chunk = create_chunk(field_meta, kTestSparseDim, rb_reader);
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

    FieldMeta field_meta(
        FieldName("a"), milvus::FieldId(1), DataType::INT64, false);
    int file_offset = 0;
    auto page_size = sysconf(_SC_PAGESIZE);
    auto chunk = create_chunk(field_meta, 1, file, file_offset, rb_reader);
    EXPECT_TRUE(chunk->Size() % page_size == 0);
    file_offset += chunk->Size();

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader2;
    s = arrow_reader->GetRecordBatchReader(&rb_reader2);
    EXPECT_TRUE(s.ok());
    auto chunk2 = create_chunk(field_meta, 1, file, file_offset, rb_reader2);
    EXPECT_TRUE(chunk->Size() % page_size == 0);
}