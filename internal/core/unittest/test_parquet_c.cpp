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
#include <fstream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "storage/parquet_c.h"
#include "storage/ColumnType.h"
#include "storage/PayloadStream.h"

static void
WriteToFile(CBuffer cb) {
    auto data_file = std::ofstream("/tmp/wrapper_test_data.dat", std::ios::binary);
    data_file.write(cb.data, cb.length);
    data_file.close();
}

static std::shared_ptr<arrow::Table>
ReadFromFile() {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    auto rst = arrow::io::ReadableFile::Open("/tmp/wrapper_test_data.dat");
    if (!rst.ok())
        return nullptr;
    infile = *rst;

    std::shared_ptr<arrow::Table> table;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
    if (!st.ok())
        return nullptr;
    st = reader->ReadTable(&table);
    if (!st.ok())
        return nullptr;
    return table;
}

TEST(wrapper, inoutstream) {
    arrow::Int64Builder i64builder;
    arrow::Status st;
    st = i64builder.AppendValues({1, 2, 3, 4, 5});
    ASSERT_TRUE(st.ok());
    std::shared_ptr<arrow::Array> i64array;
    st = i64builder.Finish(&i64array);
    ASSERT_TRUE(st.ok());

    auto schema = arrow::schema({arrow::field("val", arrow::int64())});
    ASSERT_NE(schema, nullptr);
    auto table = arrow::Table::Make(schema, {i64array});
    ASSERT_NE(table, nullptr);

    auto os = std::make_shared<wrapper::PayloadOutputStream>();
    st = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), os, 1024);
    ASSERT_TRUE(st.ok());

    const uint8_t* buf = os->Buffer().data();
    int64_t buf_size = os->Buffer().size();
    auto is = std::make_shared<wrapper::PayloadInputStream>(buf, buf_size);

    std::shared_ptr<arrow::Table> intable;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    st = parquet::arrow::OpenFile(is, arrow::default_memory_pool(), &reader);
    ASSERT_TRUE(st.ok());
    st = reader->ReadTable(&intable);
    ASSERT_TRUE(st.ok());

    auto chunks = intable->column(0)->chunks();
    ASSERT_EQ(chunks.size(), 1);

    auto inarray = std::dynamic_pointer_cast<arrow::Int64Array>(chunks[0]);
    ASSERT_NE(inarray, nullptr);
    ASSERT_EQ(inarray->Value(0), 1);
    ASSERT_EQ(inarray->Value(1), 2);
    ASSERT_EQ(inarray->Value(2), 3);
    ASSERT_EQ(inarray->Value(3), 4);
    ASSERT_EQ(inarray->Value(4), 5);
}

TEST(wrapper, boolean) {
    auto payload = NewPayloadWriter(ColumnType::BOOL);
    bool data[] = {true, false, true, false};

    auto st = AddBooleanToPayload(payload, data, 4);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_GT(cb.length, 0);
    ASSERT_NE(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 4);

    auto reader = NewPayloadReader(ColumnType::BOOL, (uint8_t*)cb.data, cb.length);
    bool* values;
    int length;
    st = GetBoolFromPayload(reader, &values, &length);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(length, 4);
    length = GetPayloadLengthFromReader(reader);
    ASSERT_EQ(length, 4);
    for (int i = 0; i < length; i++) {
        ASSERT_EQ(data[i], values[i]);
    }

    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

#define NUMERIC_TEST(TEST_NAME, COLUMN_TYPE, DATA_TYPE, ADD_FUNC, GET_FUNC, ARRAY_TYPE) \
    TEST(wrapper, TEST_NAME) {                                                          \
        auto payload = NewPayloadWriter(COLUMN_TYPE);                                   \
        DATA_TYPE data[] = {-1, 1, -100, 100};                                          \
                                                                                        \
        auto st = ADD_FUNC(payload, data, 4);                                           \
        ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                   \
        st = FinishPayloadWriter(payload);                                              \
        ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                   \
        auto cb = GetPayloadBufferFromWriter(payload);                                  \
        ASSERT_GT(cb.length, 0);                                                        \
        ASSERT_NE(cb.data, nullptr);                                                    \
        auto nums = GetPayloadLengthFromWriter(payload);                                \
        ASSERT_EQ(nums, 4);                                                             \
                                                                                        \
        auto reader = NewPayloadReader(COLUMN_TYPE, (uint8_t*)cb.data, cb.length);      \
        DATA_TYPE* values;                                                              \
        int length;                                                                     \
        st = GET_FUNC(reader, &values, &length);                                        \
        ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                   \
        ASSERT_NE(values, nullptr);                                                     \
        ASSERT_EQ(length, 4);                                                           \
        length = GetPayloadLengthFromReader(reader);                                    \
        ASSERT_EQ(length, 4);                                                           \
                                                                                        \
        for (int i = 0; i < length; i++) {                                              \
            ASSERT_EQ(data[i], values[i]);                                              \
        }                                                                               \
                                                                                        \
        ReleasePayloadWriter(payload);                                                  \
        ReleasePayloadReader(reader);                                                   \
    }

NUMERIC_TEST(int8, ColumnType::INT8, int8_t, AddInt8ToPayload, GetInt8FromPayload, arrow::Int8Array)
NUMERIC_TEST(int16, ColumnType::INT16, int16_t, AddInt16ToPayload, GetInt16FromPayload, arrow::Int16Array)
NUMERIC_TEST(int32, ColumnType::INT32, int32_t, AddInt32ToPayload, GetInt32FromPayload, arrow::Int32Array)
NUMERIC_TEST(int64, ColumnType::INT64, int64_t, AddInt64ToPayload, GetInt64FromPayload, arrow::Int64Array)
NUMERIC_TEST(float32, ColumnType::FLOAT, float, AddFloatToPayload, GetFloatFromPayload, arrow::FloatArray)
NUMERIC_TEST(float64, ColumnType::DOUBLE, double, AddDoubleToPayload, GetDoubleFromPayload, arrow::DoubleArray)

TEST(wrapper, stringarray) {
    auto payload = NewPayloadWriter(ColumnType::STRING);
    auto st = AddOneStringToPayload(payload, (char*)"1234", 4);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    st = AddOneStringToPayload(payload, (char*)"12345", 5);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    char v[3] = {0};
    v[1] = 'a';
    st = AddOneStringToPayload(payload, v, 3);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);

    st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_GT(cb.length, 0);
    ASSERT_NE(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 3);

    auto reader = NewPayloadReader(ColumnType::STRING, (uint8_t*)cb.data, cb.length);
    int length = GetPayloadLengthFromReader(reader);
    ASSERT_EQ(length, 3);
    char *v0, *v1, *v2;
    int s0, s1, s2;
    st = GetOneStringFromPayload(reader, 0, &v0, &s0);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_EQ(s0, 4);
    ASSERT_EQ(v0[0], '1');
    ASSERT_EQ(v0[1], '2');
    ASSERT_EQ(v0[2], '3');
    ASSERT_EQ(v0[3], '4');

    st = GetOneStringFromPayload(reader, 1, &v1, &s1);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_EQ(s1, 5);
    ASSERT_EQ(v1[0], '1');
    ASSERT_EQ(v1[1], '2');
    ASSERT_EQ(v1[2], '3');
    ASSERT_EQ(v1[3], '4');
    ASSERT_EQ(v1[4], '5');

    st = GetOneStringFromPayload(reader, 2, &v2, &s2);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_EQ(s2, 3);
    ASSERT_EQ(v2[0], 0);
    ASSERT_EQ(v2[1], 'a');
    ASSERT_EQ(v2[2], 0);

    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

TEST(wrapper, binary_vector) {
    auto payload = NewPayloadWriter(ColumnType::VECTOR_BINARY);
    uint8_t data[] = {0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8};

    auto st = AddBinaryVectorToPayload(payload, data, 16, 4);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_GT(cb.length, 0);
    ASSERT_NE(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 4);

    auto reader = NewPayloadReader(ColumnType::VECTOR_BINARY, (uint8_t*)cb.data, cb.length);
    uint8_t* values;
    int length;
    int dim;

    st = GetBinaryVectorFromPayload(reader, &values, &dim, &length);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(dim, 16);
    ASSERT_EQ(length, 4);
    length = GetPayloadLengthFromReader(reader);
    ASSERT_EQ(length, 4);
    for (int i = 0; i < 8; i++) {
        ASSERT_EQ(values[i], data[i]);
    }

    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

TEST(wrapper, binary_vector_empty) {
    auto payload = NewPayloadWriter(ColumnType::VECTOR_BINARY);
    auto st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_EQ(cb.length, 0);
    ASSERT_EQ(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 0);
    auto reader = NewPayloadReader(ColumnType::VECTOR_BINARY, (uint8_t*)cb.data, cb.length);
    ASSERT_EQ(reader, nullptr);
    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

TEST(wrapper, float_vector) {
    auto payload = NewPayloadWriter(ColumnType::VECTOR_FLOAT);
    float data[] = {1, 2, 3, 4, 5, 6, 7, 8};

    auto st = AddFloatVectorToPayload(payload, data, 2, 4);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_GT(cb.length, 0);
    ASSERT_NE(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 4);

    auto reader = NewPayloadReader(ColumnType::VECTOR_FLOAT, (uint8_t*)cb.data, cb.length);
    float* values;
    int length;
    int dim;

    st = GetFloatVectorFromPayload(reader, &values, &dim, &length);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    ASSERT_NE(values, nullptr);
    ASSERT_EQ(dim, 2);
    ASSERT_EQ(length, 4);
    length = GetPayloadLengthFromReader(reader);
    ASSERT_EQ(length, 4);
    for (int i = 0; i < 8; i++) {
        ASSERT_EQ(values[i], data[i]);
    }

    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

TEST(wrapper, float_vector_empty) {
    auto payload = NewPayloadWriter(ColumnType::VECTOR_FLOAT);
    auto st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_EQ(cb.length, 0);
    ASSERT_EQ(cb.data, nullptr);
    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 0);
    auto reader = NewPayloadReader(ColumnType::VECTOR_FLOAT, (uint8_t*)cb.data, cb.length);
    ASSERT_EQ(reader, nullptr);
    ReleasePayloadWriter(payload);
    ReleasePayloadReader(reader);
}

TEST(wrapper, int8_2) {
    auto payload = NewPayloadWriter(ColumnType::INT8);
    int8_t data[] = {-1, 1, -100, 100};

    auto st = AddInt8ToPayload(payload, data, 4);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    st = FinishPayloadWriter(payload);
    ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
    auto cb = GetPayloadBufferFromWriter(payload);
    ASSERT_GT(cb.length, 0);
    ASSERT_NE(cb.data, nullptr);

    WriteToFile(cb);

    auto nums = GetPayloadLengthFromWriter(payload);
    ASSERT_EQ(nums, 4);
    ReleasePayloadWriter(payload);

    auto table = ReadFromFile();
    ASSERT_NE(table, nullptr);

    auto chunks = table->column(0)->chunks();
    ASSERT_EQ(chunks.size(), 1);

    auto bool_array = std::dynamic_pointer_cast<arrow::Int8Array>(chunks[0]);
    ASSERT_NE(bool_array, nullptr);

    ASSERT_EQ(bool_array->Value(0), -1);
    ASSERT_EQ(bool_array->Value(1), 1);
    ASSERT_EQ(bool_array->Value(2), -100);
    ASSERT_EQ(bool_array->Value(3), 100);
}
