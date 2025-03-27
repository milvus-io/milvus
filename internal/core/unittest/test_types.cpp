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
#include <arrow/api.h>
#include "common/Types.h"

using namespace milvus;

TEST(GetArrowDataTypeTest, BOOL) {
    auto result = GetArrowDataType(DataType::BOOL);
    ASSERT_TRUE(result->Equals(arrow::boolean()));
}

TEST(GetArrowDataTypeTest, INT8) {
    auto result = GetArrowDataType(DataType::INT8);
    ASSERT_TRUE(result->Equals(arrow::int8()));
}

TEST(GetArrowDataTypeTest, INT16) {
    auto result = GetArrowDataType(DataType::INT16);
    ASSERT_TRUE(result->Equals(arrow::int16()));
}

TEST(GetArrowDataTypeTest, INT32) {
    auto result = GetArrowDataType(DataType::INT32);
    ASSERT_TRUE(result->Equals(arrow::int32()));
}

TEST(GetArrowDataTypeTest, INT64) {
    auto result = GetArrowDataType(DataType::INT64);
    ASSERT_TRUE(result->Equals(arrow::int64()));
}

TEST(GetArrowDataTypeTest, FLOAT) {
    auto result = GetArrowDataType(DataType::FLOAT);
    ASSERT_TRUE(result->Equals(arrow::float32()));
}

TEST(GetArrowDataTypeTest, DOUBLE) {
    auto result = GetArrowDataType(DataType::DOUBLE);
    ASSERT_TRUE(result->Equals(arrow::float64()));
}

TEST(GetArrowDataTypeTest, STRING_TYPES) {
    auto result1 = GetArrowDataType(DataType::STRING);
    auto result2 = GetArrowDataType(DataType::VARCHAR);
    auto result3 = GetArrowDataType(DataType::TEXT);

    ASSERT_TRUE(result1->Equals(arrow::utf8()));
    ASSERT_TRUE(result2->Equals(arrow::utf8()));
    ASSERT_TRUE(result3->Equals(arrow::utf8()));
}

TEST(GetArrowDataTypeTest, ARRAY_JSON) {
    auto result1 = GetArrowDataType(DataType::ARRAY);
    auto result2 = GetArrowDataType(DataType::JSON);

    ASSERT_TRUE(result1->Equals(arrow::binary()));
    ASSERT_TRUE(result2->Equals(arrow::binary()));
}

TEST(GetArrowDataTypeTest, VECTOR_FLOAT) {
    int dim = 3;
    auto result = GetArrowDataType(DataType::VECTOR_FLOAT, dim);
    ASSERT_TRUE(result->Equals(arrow::fixed_size_binary(dim * 4)));
}

TEST(GetArrowDataTypeTest, VECTOR_BINARY) {
    int dim = 5;
    auto result = GetArrowDataType(DataType::VECTOR_BINARY, dim);
    ASSERT_TRUE(result->Equals(arrow::fixed_size_binary((dim + 7) / 8)));
}

TEST(GetArrowDataTypeTest, VECTOR_FLOAT16) {
    int dim = 2;
    auto result = GetArrowDataType(DataType::VECTOR_FLOAT16, dim);
    ASSERT_TRUE(result->Equals(arrow::fixed_size_binary(dim * 2)));
}

TEST(GetArrowDataTypeTest, VECTOR_BFLOAT16) {
    int dim = 2;
    auto result = GetArrowDataType(DataType::VECTOR_BFLOAT16, dim);
    ASSERT_TRUE(result->Equals(arrow::fixed_size_binary(dim * 2)));
}

TEST(GetArrowDataTypeTest, VECTOR_SPARSE_FLOAT) {
    auto result = GetArrowDataType(DataType::VECTOR_SPARSE_FLOAT);
    ASSERT_TRUE(result->Equals(arrow::binary()));
}

TEST(GetArrowDataTypeTest, VECTOR_INT8) {
    int dim = 4;
    auto result = GetArrowDataType(DataType::VECTOR_INT8, dim);
    ASSERT_TRUE(result->Equals(arrow::fixed_size_binary(dim)));
}

TEST(GetArrowDataTypeTest, InvalidDataType) {
    EXPECT_THROW(
        GetArrowDataType(static_cast<DataType>(999)),
        std::runtime_error);  // Assuming PanicInfo throws a std::runtime_error
}