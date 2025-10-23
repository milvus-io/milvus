// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include "index/skipindex_stats/SkipIndexStats.h"
#include <arrow/api.h>
#include <arrow/type_fwd.h>
#include <parquet/statistics.h>
#include <memory>
#include <vector>
#include "storage/Util.h"
#include "common/ChunkWriter.h"

using namespace milvus;
using namespace milvus::index;

class SkipIndexStatsBuilderTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        builder_ = std::make_unique<SkipIndexStatsBuilder>();
    }

    std::unique_ptr<SkipIndexStatsBuilder> builder_;
};

TEST_F(SkipIndexStatsBuilderTest, BuildFromArrowBatch_AllTypes) {
    // BOOL
    {
        auto schema = arrow::schema({arrow::field("col", arrow::boolean())});
        arrow::BooleanBuilder builder;
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());
        ASSERT_TRUE(builder.Append(false).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 4, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::BOOL);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::BOOLEAN);

        std::vector<Metrics> query_true = {true, false};
        std::vector<Metrics> query_false = {false, true};
        EXPECT_TRUE(metrics->CanSkipIn(query_true));
        EXPECT_FALSE(metrics->CanSkipIn(query_false));
    }

    // INT8
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int8())});
        arrow::Int8Builder builder;
        std::vector<int8_t> values = {-10, 0, 5, 10, 20};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT8);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(5)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(100)));
    }

    // INT16
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int16())});
        arrow::Int16Builder builder;
        for (int16_t i = 0; i < 100; ++i) {
            ASSERT_TRUE(builder.Append(i * 10).ok());
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 100, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT16);
        ASSERT_NE(metrics, nullptr);
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::GreaterThan, int16_t(1000)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int16_t(50)));
    }

    // INT32
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        arrow::Int32Builder builder;
        for (int32_t i = 0; i < 1000; ++i) {
            ASSERT_TRUE(builder.Append(i).ok());
        }

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 1000, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        std::vector<Metrics> in_values = {int32_t(2000), int32_t(3000)};
        EXPECT_TRUE(metrics->CanSkipIn(in_values));
        std::vector<Metrics> in_values2 = {int32_t(10), int32_t(20)};
        EXPECT_FALSE(metrics->CanSkipIn(in_values2));
    }

    // INT64
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int64())});
        arrow::Int64Builder builder;
        std::vector<int64_t> values = {INT64_MIN / 2, 0, INT64_MAX / 2};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT64);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int64_t(114514)));
    }

    // FLOAT
    {
        auto schema = arrow::schema({arrow::field("col", arrow::float32())});
        arrow::FloatBuilder builder;
        std::vector<float> values = {-1.5f, 0.0f, 1.5f, 3.14f};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::FLOAT);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::LessThan, -2.0f));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::LessThan, 0.0f));
    }

    // DOUBLE
    {
        auto schema = arrow::schema({arrow::field("col", arrow::float64())});
        arrow::DoubleBuilder builder;
        std::vector<double> values = {
            -3.141592653589793, 0.0, 2.718281828459045};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::DOUBLE);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0));
    }

    // STRING
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;
        std::vector<std::string> values = {"apple", "banana", "cherry", "date"};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);
        EXPECT_FALSE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("banana")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("zebra")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::LessThan, std::string("aaa")));
    }

    // STRING with N-gram
    {
        auto schema = arrow::schema({arrow::field("col", arrow::utf8())});
        arrow::StringBuilder builder;
        std::vector<std::string> values = {"milvus_vector", "database_system"};
        ASSERT_TRUE(builder.AppendValues(values).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, values.size(), {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::STRING);
        ASSERT_NE(metrics, nullptr);

        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::InnerMatch, std::string("abc")));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::InnerMatch,
                                                std::string("vector")));
    }

    // Contains null values
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        arrow::Int32Builder builder;
        ASSERT_TRUE(builder.Append(10).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(20).ok());
        ASSERT_TRUE(builder.AppendNull().ok());
        ASSERT_TRUE(builder.Append(30).ok());

        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        auto batch = arrow::RecordBatch::Make(schema, 5, {array});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(10)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(5)));
    }

    // Multiple Batches
    {
        auto schema = arrow::schema({arrow::field("col", arrow::int32())});
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        arrow::Int32Builder builder1;
        for (int i = 0; i < 100; ++i) {
            ASSERT_TRUE(builder1.Append(i).ok());
        }
        std::shared_ptr<arrow::Array> array1;
        ASSERT_TRUE(builder1.Finish(&array1).ok());
        batches.push_back(arrow::RecordBatch::Make(schema, 100, {array1}));

        arrow::Int32Builder builder2;
        for (int i = 100; i < 200; ++i) {
            ASSERT_TRUE(builder2.Append(i).ok());
        }
        std::shared_ptr<arrow::Array> array2;
        ASSERT_TRUE(builder2.Finish(&array2).ok());
        batches.push_back(arrow::RecordBatch::Make(schema, 100, {array2}));

        auto metrics = builder_->Build(batches, 0, arrow::Type::INT32);
        ASSERT_NE(metrics, nullptr);

        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(50)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(150)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(300)));
    }

    // Empty Batches
    {
        std::vector<std::shared_ptr<arrow::RecordBatch>> empty_batches;
        auto metrics = builder_->Build(empty_batches, 0, arrow::Type::INT32);

        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::NONE);
    }
}

TEST_F(SkipIndexStatsBuilderTest, BuildFromChunk_AllTypes) {
    // BOOL
    {
        FixedVector<bool> data = {true, true, true, true};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::BOOL, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::BOOL,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::BOOL, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::BOOLEAN);

        std::vector<Metrics> query_true = {true, false};
        std::vector<Metrics> query_false = {false, true};
        EXPECT_FALSE(metrics->CanSkipIn(query_true));
        EXPECT_TRUE(metrics->CanSkipIn(query_false));
    }

    // INT8
    {
        FixedVector<int8_t> data = {-50, -25, 0, 25, 50};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT8, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT8,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT8, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::LessThan, int8_t(-51)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int8_t(0)));
    }

    // INT16
    {
        FixedVector<int16_t> data;
        for (int16_t i = 0; i < 100; ++i) {
            data.push_back(i * 10);
        }
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT16, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT16,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT16, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::GreaterThan, int16_t(1000)));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int16_t(50)));
    }

    // INT32
    {
        FixedVector<int32_t> data;
        for (int32_t i = 0; i < 1000; ++i) {
            data.push_back(i);
        }
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT32, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT32,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT32, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(500)));
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int32_t(2000)));
    }

    // INT64
    {
        FixedVector<int64_t> data = {-1145141919810, 0, 1145141919810};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::INT64, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::INT64,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::INT64, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::INT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::Equal, int64_t(114514)));
    }

    // FLOAT
    {
        FixedVector<float> data = {-3.14f, -1.0f, 0.0f, 1.0f, 2.718f};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::FLOAT, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::FLOAT,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::FLOAT, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_TRUE(metrics->CanSkipUnaryRange(OpType::GreaterThan, 3.0f));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0f));
    }

    // DOUBLE
    {
        FixedVector<double> data = {-3.141592653589793, 0.0, 2.718281828459045};
        auto field_data = milvus::storage::CreateFieldData(
            storage::DataType::DOUBLE, DataType::NONE);
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::DOUBLE,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::DOUBLE, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::FLOAT);
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::Equal, 0.0));
    }

    // VARCHAR
    {
        FixedVector<std::string> data = {
            "apple", "banana", "cherry", "date", "elderberry"};
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
        ASSERT_TRUE(reader_builder.Open(buffer).ok());
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ASSERT_TRUE(reader_builder.Build(&arrow_reader).ok());

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        ASSERT_TRUE(arrow_reader->GetRecordBatchReader(&rb_reader).ok());

        FieldMeta field_meta(FieldName("a"),
                             milvus::FieldId(1),
                             DataType::STRING,
                             false,
                             std::nullopt);
        arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
        auto chunk = create_chunk(field_meta, array_vec);

        auto metrics = builder_->Build(DataType::VARCHAR, chunk.get());
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetMetricsType(), FieldChunkMetricsType::STRING);
        EXPECT_FALSE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("banana")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::Equal, std::string("xyz")));
        EXPECT_TRUE(
            metrics->CanSkipUnaryRange(OpType::LessThan, std::string("aaa")));
        EXPECT_FALSE(metrics->CanSkipUnaryRange(OpType::LessThan,
                                                std::string("cherry")));
    }
}