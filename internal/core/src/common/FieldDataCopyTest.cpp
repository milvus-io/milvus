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

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/FieldData.h"

namespace milvus {
namespace {

TEST(FieldDataOwnershipTest, VectorArrayTakesOwnershipOfDecodedBuffer) {
    for (const auto type : {DataType::VECTOR_FLOAT,
                            DataType::VECTOR_FLOAT16,
                            DataType::VECTOR_BFLOAT16,
                            DataType::VECTOR_INT8,
                            DataType::VECTOR_BINARY}) {
        constexpr int dim = 16;
        const auto size = 2 * vector_bytes_per_element(type, dim);
        auto buffer = std::make_unique<char[]>(size);
        std::memset(buffer.get(), 42, size);
        const auto* address = buffer.get();
        VectorArray array(std::move(buffer), 2, dim, type);
        EXPECT_EQ(array.data(), address);
        EXPECT_EQ(array.physical_length(), 2);
        EXPECT_EQ(array.byte_size(), size);
        EXPECT_EQ(std::string_view(array.data(), size), std::string(size, 42));
        const VectorArray copied(address, 2, dim, type);
        EXPECT_EQ(array.output_data().SerializeAsString(),
                  copied.output_data().SerializeAsString());
    }
}

TEST(FieldDataOwnershipTest, ScalarArraysSurviveSlicedArrowBatches) {
    for (bool nullable : {false, true}) {
        for (int capacity : {0, 32}) {
            FieldData<Array> field(DataType::ARRAY, nullable, capacity);
            std::vector<std::optional<std::string>> expected;
            for (int batch = 0; batch < 3; ++batch) {
                arrow::BinaryBuilder builder;
                ASSERT_TRUE(builder.Append("discarded prefix").ok());
                for (int row = 0; row < 5; ++row) {
                    if (nullable && row == 1) {
                        ASSERT_TRUE(builder.AppendNull().ok());
                        expected.emplace_back(std::nullopt);
                    } else {
                        ScalarFieldProto value;
                        const std::string text(100 + batch * 5 + row, 'x');
                        value.mutable_string_data()->add_data(text);
                        ASSERT_TRUE(
                            builder.Append(value.SerializeAsString()).ok());
                        expected.emplace_back(text);
                    }
                }
                std::shared_ptr<arrow::Array> array;
                ASSERT_TRUE(builder.Finish(&array).ok());
                field.FillFieldData(array->Slice(1, 5));
            }
            EXPECT_EQ(field.length(), expected.size());
            EXPECT_EQ(field.get_null_count(), nullable ? 3 : 0);
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(field.is_valid(i), expected[i].has_value());
                if (expected[i]) {
                    const auto& row =
                        *static_cast<const Array*>(field.RawValue(i));
                    ASSERT_EQ(row.length(), 1);
                    EXPECT_EQ(row.get_data_unchecked<std::string>(0),
                              *expected[i]);
                }
            }
        }
    }
}

TEST(FieldDataOwnershipTest, VectorArraysKeepCompactedNullableLayout) {
    for (const auto type : {DataType::VECTOR_FLOAT,
                            DataType::VECTOR_FLOAT16,
                            DataType::VECTOR_BFLOAT16,
                            DataType::VECTOR_INT8,
                            DataType::VECTOR_BINARY}) {
        for (bool nullable : {false, true}) {
            for (int capacity : {0, 32}) {
                constexpr int dim = 16;
                const auto width = vector_bytes_per_element(type, dim);
                FieldData<VectorArray> field(dim, type, nullable, capacity);
                std::vector<std::optional<std::string>> expected;
                // Append an all-null batch followed by mixed and all-valid
                // batches; slices and appends both start at non-byte boundaries.
                for (int batch = 0; batch < 3; ++batch) {
                    auto values =
                        std::make_shared<arrow::FixedSizeBinaryBuilder>(
                            arrow::fixed_size_binary(width));
                    arrow::ListBuilder builder(arrow::default_memory_pool(),
                                               values);
                    ASSERT_TRUE(builder.Append().ok());
                    for (int row = 0; row < 5; ++row) {
                        if (nullable &&
                            (batch == 0 || (batch == 1 && row == 1))) {
                            ASSERT_TRUE(builder.AppendNull().ok());
                            expected.emplace_back(std::nullopt);
                            continue;
                        }
                        ASSERT_TRUE(builder.Append().ok());
                        std::string bytes;
                        for (int vec = 0; vec < row % 3; ++vec) {
                            const std::string vector(
                                width, static_cast<char>(batch * 10 + row));
                            ASSERT_TRUE(values->Append(vector).ok());
                            bytes += vector;
                        }
                        expected.emplace_back(bytes);
                    }
                    std::shared_ptr<arrow::Array> array;
                    ASSERT_TRUE(builder.Finish(&array).ok());
                    field.FillFieldData(array->Slice(1, 5));
                }
                EXPECT_EQ(field.length(), expected.size());
                EXPECT_EQ(field.get_null_count(), nullable ? 6 : 0);
                EXPECT_EQ(field.get_valid_rows(),
                          expected.size() - (nullable ? 6 : 0));
                const auto* rows =
                    static_cast<const VectorArray*>(field.Data());
                size_t valid_row = 0;
                for (size_t i = 0; i < expected.size(); ++i) {
                    EXPECT_EQ(field.is_valid(i), expected[i].has_value());
                    if (!expected[i]) {
                        continue;
                    }
                    const auto& row = rows[nullable ? valid_row++ : i];
                    EXPECT_EQ(row.physical_length(),
                              expected[i]->size() / width);
                    EXPECT_EQ(row.byte_size(), expected[i]->size());
                    if (!expected[i]->empty()) {
                        EXPECT_EQ(std::string_view(row.data(), row.byte_size()),
                                  *expected[i]);
                    }
                }
            }
        }
    }
}

}  // namespace
}  // namespace milvus
