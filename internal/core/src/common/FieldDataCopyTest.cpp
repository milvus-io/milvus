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
#include "common/ColumnarArrayChunkBuilder.h"

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

TEST(FieldDataOwnershipTest, NativeListArrayPreservesElementValidity) {
    proto::schema::TypeSchema type;
    type.set_nullable(true);
    type.mutable_array_element()->set_leaf_type(proto::schema::Int64);
    type.mutable_array_element()->set_nullable(true);
    arrow::ListBuilder builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::Int64Builder>());
    auto* values = static_cast<arrow::Int64Builder*>(builder.value_builder());
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->Append(7).ok());
    ASSERT_TRUE(values->AppendNull().ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    FieldData<ArrayValue> data(type, true);
    data.FillFieldData(array);
    ASSERT_EQ(data.Length(), 3);
    const auto* rows = static_cast<const ArrayValue*>(data.Data());
    auto first = rows[0].output_data();
    ASSERT_EQ(first.long_data().data_size(), 2);
    ASSERT_EQ(first.valid_data_size(), 2);
    EXPECT_EQ(first.long_data().data(0), 7);
    EXPECT_EQ(first.long_data().data(1), 0);
    EXPECT_FALSE(first.valid_data(1));
    EXPECT_FALSE(data.is_valid(1));
    EXPECT_EQ(rows[2].size(), 0);

    proto::schema::TypeSchema wrong_type = type;
    wrong_type.mutable_array_element()->set_leaf_type(proto::schema::Float);
    FieldData<ArrayValue> wrong(wrong_type, true);
    EXPECT_ANY_THROW(wrong.FillFieldData(array));
}

TEST(FieldDataOwnershipTest, NativeListArrayMatchesProtoProjectionAfterSlice) {
    proto::schema::TypeSchema type;
    type.set_nullable(true);
    type.mutable_array_element()->set_leaf_type(proto::schema::Int64);
    type.mutable_array_element()->set_nullable(true);
    arrow::ListBuilder builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::Int64Builder>());
    auto* values = static_cast<arrow::Int64Builder*>(builder.value_builder());
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->Append(99).ok());
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->Append(7).ok());
    ASSERT_TRUE(values->AppendNull().ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append().ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    auto sliced = std::static_pointer_cast<arrow::ListArray>(array->Slice(1, 3));
    ASSERT_NE(sliced->offset(), 0);
    const auto expected = ArrowListToScalarFieldProto(*sliced, type);

    FieldData<ArrayValue> data(type, true);
    data.FillFieldData(sliced);
    ASSERT_EQ(data.Length(), expected.size());
    const auto* rows = static_cast<const ArrayValue*>(data.Data());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(rows[i].output_data().SerializeAsString(),
                  expected[i].SerializeAsString())
            << i;
        EXPECT_EQ(data.is_valid(i), !sliced->IsNull(i));
    }
    ASSERT_EQ(expected[0].valid_data_size(), 2);
    EXPECT_EQ(expected[0].long_data().data(1), 0);
    EXPECT_FALSE(expected[0].valid_data(1));
    EXPECT_EQ(expected[1].data_case(), ScalarFieldProto::DATA_NOT_SET);
    EXPECT_EQ(expected[2].long_data().data_size(), 0);
}

TEST(FieldDataOwnershipTest, NativeNestedListMatchesProtoProjection) {
    for (bool row_nullable : {false, true}) {
        for (bool inner_nullable : {false, true}) {
            for (bool leaf_nullable : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << "row=" << row_nullable
                             << " inner=" << inner_nullable
                             << " leaf=" << leaf_nullable);
                proto::schema::TypeSchema type;
                type.set_nullable(row_nullable);
                auto* child_type = type.mutable_array_element();
                child_type->set_nullable(inner_nullable);
                auto* leaf_type = child_type->mutable_array_element();
                leaf_type->set_leaf_type(proto::schema::Int64);
                leaf_type->set_nullable(leaf_nullable);

                auto inner_builder = std::make_shared<arrow::ListBuilder>(
                    arrow::default_memory_pool(),
                    std::make_shared<arrow::Int64Builder>());
                arrow::ListBuilder outer_builder(arrow::default_memory_pool(),
                                                 inner_builder);
                auto* inner = static_cast<arrow::ListBuilder*>(
                    outer_builder.value_builder());
                auto* values = static_cast<arrow::Int64Builder*>(
                    inner->value_builder());
                ASSERT_TRUE(outer_builder.Append().ok());
                ASSERT_TRUE(inner->Append().ok());
                ASSERT_TRUE(values->Append(99).ok());
                ASSERT_TRUE(outer_builder.Append().ok());
                ASSERT_TRUE(inner->Append().ok());
                ASSERT_TRUE(values->Append(7).ok());
                ASSERT_TRUE((leaf_nullable ? values->AppendNull()
                                           : values->Append(8))
                                .ok());
                ASSERT_TRUE((inner_nullable ? inner->AppendNull()
                                            : inner->Append())
                                .ok());
                ASSERT_TRUE(inner->Append().ok());
                ASSERT_TRUE((row_nullable ? outer_builder.AppendNull()
                                          : outer_builder.Append())
                                .ok());
                ASSERT_TRUE(outer_builder.Append().ok());
                std::shared_ptr<arrow::Array> array;
                ASSERT_TRUE(outer_builder.Finish(&array).ok());
                auto sliced = std::static_pointer_cast<arrow::ListArray>(
                    array->Slice(1, 3));
                ASSERT_NE(sliced->offset(), 0);
                const auto expected =
                    ArrowListToScalarFieldProto(*sliced, type);

                FieldData<ArrayValue> data(type, row_nullable);
                data.FillFieldData(sliced);
                ASSERT_EQ(data.Length(), expected.size());
                const auto* rows = static_cast<const ArrayValue*>(data.Data());
                for (size_t i = 0; i < expected.size(); ++i) {
                    EXPECT_EQ(rows[i].output_data().SerializeAsString(),
                              expected[i].SerializeAsString())
                        << i;
                    EXPECT_EQ(data.is_valid(i), !sliced->IsNull(i));
                }
                const auto& first = rows[0].output_data();
                ASSERT_EQ(first.array_data().data_size(), 3);
                EXPECT_EQ(first.valid_data_size(), inner_nullable ? 3 : 0);
                const auto& first_inner = first.array_data().data(0);
                ASSERT_EQ(first_inner.long_data().data_size(), 2);
                EXPECT_EQ(first_inner.long_data().data(1),
                          leaf_nullable ? 0 : 8);
                EXPECT_EQ(first_inner.valid_data_size(),
                          leaf_nullable ? 2 : 0);
                if (leaf_nullable) {
                    EXPECT_FALSE(first_inner.valid_data(1));
                }
                if (inner_nullable) {
                    EXPECT_FALSE(first.valid_data(1));
                    EXPECT_EQ(first.array_data().data(1).data_case(),
                              ScalarFieldProto::kLongData);
                    EXPECT_EQ(first.array_data().data(1).long_data().data_size(),
                              0);
                }
                EXPECT_EQ(rows[1].is_null(), row_nullable);
                EXPECT_EQ(rows[2].size(), 0);
            }
        }
    }
}

TEST(FieldDataOwnershipTest, BinaryVectorListKeepsCompactPayload) {
    arrow::ListBuilder builder(
        arrow::default_memory_pool(),
        std::make_shared<arrow::BinaryBuilder>());
    auto* values = static_cast<arrow::BinaryBuilder*>(builder.value_builder());
    const float vector[] = {1.0F, 2.0F};
    ASSERT_TRUE(builder.Append().ok());
    ASSERT_TRUE(values->AppendNull().ok());
    ASSERT_TRUE(values->Append(
        reinterpret_cast<const uint8_t*>(vector), sizeof(vector)).ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    FieldData<VectorArray> data(2, DataType::VECTOR_FLOAT);
    data.FillFieldData(array);
    ASSERT_EQ(data.Length(), 1);
    const auto* rows = static_cast<const VectorArray*>(data.Data());
    EXPECT_EQ(rows[0].length(), 2);
    EXPECT_EQ(rows[0].physical_length(), 1);
    auto proto = rows[0].output_data();
    ASSERT_EQ(proto.valid_data_size(), 2);
    EXPECT_FALSE(proto.valid_data(0));
    EXPECT_TRUE(proto.valid_data(1));
    EXPECT_EQ(proto.float_vector().data_size(), 2);
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
