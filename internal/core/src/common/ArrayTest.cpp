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

#include <stdint.h>
#include <string>
#include <string_view>
#include <vector>

#include "common/Array.h"
#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

TEST(Array, TestConstructArray) {
    using namespace milvus;

    int N = 10;
    // 1. test int
    milvus::proto::schema::ScalarField field_int_data;
    milvus::proto::plan::Array field_int_array;
    field_int_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_int_data.mutable_int_data()->add_data(i);
        field_int_array.mutable_array()->Add()->set_int64_val(i);
    }
    auto int_array = Array(field_int_data);
    ASSERT_EQ(N, int_array.length());
    ASSERT_EQ(N * sizeof(int), int_array.byte_size());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(int_array.get_data_unchecked<int>(i), i);
    }
    ASSERT_TRUE(int_array.is_same_array(field_int_array));
    auto int_array_tmp = Array(const_cast<char*>(int_array.data()),
                               int_array.length(),
                               int_array.byte_size(),
                               int_array.get_element_type(),
                               int_array.get_offsets_data());
    auto int_8_array = Array(const_cast<char*>(int_array.data()),
                             int_array.length(),
                             int_array.byte_size(),
                             DataType::INT8,
                             int_array.get_offsets_data());
    ASSERT_EQ(int_array.length(), int_8_array.length());
    auto int_16_array = Array(const_cast<char*>(int_array.data()),
                              int_array.length(),
                              int_array.byte_size(),
                              DataType::INT16,
                              int_array.get_offsets_data());
    ASSERT_EQ(int_array.length(), int_16_array.length());
    ASSERT_TRUE(int_array_tmp == int_array);
    auto int_array_view = ArrayView(const_cast<char*>(int_array.data()),
                                    int_array.length(),
                                    int_array.byte_size(),
                                    int_array.get_element_type(),
                                    int_array.get_offsets_data());
    ASSERT_EQ(int_array.length(), int_array_view.length());
    ASSERT_EQ(int_array.byte_size(), int_array_view.byte_size());
    ASSERT_EQ(int_array.get_element_type(), int_array_view.get_element_type());

    // 2. test long
    milvus::proto::schema::ScalarField field_long_data;
    milvus::proto::plan::Array field_long_array;
    field_long_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_long_data.mutable_long_data()->add_data(i);
        field_long_array.mutable_array()->Add()->set_int64_val(i);
    }
    auto long_array = Array(field_long_data);
    ASSERT_EQ(N, long_array.length());
    ASSERT_EQ(N * sizeof(int64_t), long_array.byte_size());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(long_array.get_data_unchecked<int64_t>(i), i);
    }
    ASSERT_TRUE(long_array.is_same_array(field_int_array));
    auto long_array_tmp = Array(const_cast<char*>(long_array.data()),
                                long_array.length(),
                                long_array.byte_size(),
                                long_array.get_element_type(),
                                long_array.get_offsets_data());
    ASSERT_TRUE(long_array_tmp == long_array);
    auto long_array_view = ArrayView(const_cast<char*>(long_array.data()),
                                     long_array.length(),
                                     long_array.byte_size(),
                                     long_array.get_element_type(),
                                     long_array.get_offsets_data());
    ASSERT_EQ(long_array.length(), long_array_view.length());
    ASSERT_EQ(long_array.byte_size(), long_array_view.byte_size());
    ASSERT_EQ(long_array.get_element_type(),
              long_array_view.get_element_type());

    // 3. test string
    milvus::proto::schema::ScalarField field_string_data;
    milvus::proto::plan::Array field_string_array;
    field_string_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_string_data.mutable_string_data()->add_data(std::to_string(i));
        proto::plan::GenericValue string_val;
        string_val.set_string_val(std::to_string(i));
        field_string_array.mutable_array()->Add()->CopyFrom(string_val);
    }
    auto string_array = Array(field_string_data);
    ASSERT_EQ(N, string_array.length());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(string_array.get_data_unchecked<std::string_view>(i),
                  std::to_string(i));
    }
    ASSERT_TRUE(string_array.is_same_array(field_string_array));
    auto string_array_tmp = Array(const_cast<char*>(string_array.data()),
                                  string_array.length(),
                                  string_array.byte_size(),
                                  string_array.get_element_type(),
                                  string_array.get_offsets_data());
    ASSERT_TRUE(string_array_tmp == string_array);
    auto string_array_view = ArrayView(const_cast<char*>(string_array.data()),
                                       string_array.length(),
                                       string_array.byte_size(),
                                       string_array.get_element_type(),
                                       string_array.get_offsets_data());
    ASSERT_EQ(string_array.length(), string_array_view.length());
    ASSERT_EQ(string_array.byte_size(), string_array_view.byte_size());
    ASSERT_EQ(string_array.get_element_type(),
              string_array_view.get_element_type());

    // 4. test bool
    milvus::proto::schema::ScalarField field_bool_data;
    milvus::proto::plan::Array field_bool_array;
    field_bool_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_bool_data.mutable_bool_data()->add_data(bool(i));
        field_bool_array.mutable_array()->Add()->set_bool_val(bool(i));
    }
    auto bool_array = Array(field_bool_data);
    ASSERT_EQ(N, bool_array.length());
    ASSERT_EQ(N * sizeof(bool), bool_array.byte_size());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(bool_array.get_data_unchecked<bool>(i), bool(i));
    }
    ASSERT_TRUE(bool_array.is_same_array(field_bool_array));
    auto bool_array_tmp = Array(const_cast<char*>(bool_array.data()),
                                bool_array.length(),
                                bool_array.byte_size(),
                                bool_array.get_element_type(),
                                bool_array.get_offsets_data());
    ASSERT_TRUE(bool_array_tmp == bool_array);
    auto bool_array_view = ArrayView(const_cast<char*>(bool_array.data()),
                                     bool_array.length(),
                                     bool_array.byte_size(),
                                     bool_array.get_element_type(),
                                     bool_array.get_offsets_data());
    ASSERT_EQ(bool_array.length(), bool_array_view.length());
    ASSERT_EQ(bool_array.byte_size(), bool_array_view.byte_size());
    ASSERT_EQ(bool_array.get_element_type(),
              bool_array_view.get_element_type());

    //5. test float
    milvus::proto::schema::ScalarField field_float_data;
    milvus::proto::plan::Array field_float_array;
    field_float_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_float_data.mutable_float_data()->add_data(float(i) * 0.1);
        field_float_array.mutable_array()->Add()->set_float_val(float(i * 0.1));
    }
    auto float_array = Array(field_float_data);
    ASSERT_EQ(N, float_array.length());
    ASSERT_EQ(N * sizeof(float), float_array.byte_size());
    for (int i = 0; i < N; ++i) {
        ASSERT_DOUBLE_EQ(float_array.get_data_unchecked<float>(i), float(i * 0.1));
    }
    ASSERT_TRUE(float_array.is_same_array(field_float_array));
    auto float_array_tmp = Array(const_cast<char*>(float_array.data()),
                                 float_array.length(),
                                 float_array.byte_size(),
                                 float_array.get_element_type(),
                                 float_array.get_offsets_data());
    ASSERT_TRUE(float_array_tmp == float_array);
    auto float_array_view = ArrayView(const_cast<char*>(float_array.data()),
                                      float_array.length(),
                                      float_array.byte_size(),
                                      float_array.get_element_type(),
                                      float_array.get_offsets_data());
    ASSERT_EQ(float_array.length(), float_array_view.length());
    ASSERT_EQ(float_array.byte_size(), float_array_view.byte_size());
    ASSERT_EQ(float_array.get_element_type(),
              float_array_view.get_element_type());

    //6. test double
    milvus::proto::schema::ScalarField field_double_data;
    milvus::proto::plan::Array field_double_array;
    field_double_array.set_same_type(true);
    for (int i = 0; i < N; i++) {
        field_double_data.mutable_double_data()->add_data(double(i) * 0.1);
        field_double_array.mutable_array()->Add()->set_float_val(
            double(i * 0.1));
    }
    auto double_array = Array(field_double_data);
    ASSERT_EQ(N, double_array.length());
    ASSERT_EQ(N * sizeof(double), double_array.byte_size());
    for (int i = 0; i < N; ++i) {
        ASSERT_DOUBLE_EQ(double_array.get_data_unchecked<double>(i), double(i * 0.1));
    }
    ASSERT_TRUE(double_array.is_same_array(field_double_array));
    auto double_array_tmp = Array(const_cast<char*>(double_array.data()),
                                  double_array.length(),
                                  double_array.byte_size(),
                                  double_array.get_element_type(),
                                  double_array.get_offsets_data());
    ASSERT_TRUE(double_array_tmp == double_array);
    auto double_array_view = ArrayView(const_cast<char*>(double_array.data()),
                                       double_array.length(),
                                       double_array.byte_size(),
                                       double_array.get_element_type(),
                                       double_array.get_offsets_data());
    ASSERT_EQ(double_array.length(), double_array_view.length());
    ASSERT_EQ(double_array.byte_size(), double_array_view.byte_size());
    ASSERT_EQ(double_array.get_element_type(),
              double_array_view.get_element_type());

    milvus::proto::schema::ScalarField field_empty_data;
    milvus::proto::plan::Array field_empty_array;
    auto empty_array = Array(field_empty_data);
    ASSERT_EQ(0, empty_array.length());
    ASSERT_EQ(0, empty_array.byte_size());
    ASSERT_TRUE(empty_array.is_same_array(field_empty_array));

    ArrayView null_view;
    EXPECT_NO_THROW({
        auto null_view_copy = null_view;
        EXPECT_EQ(0, null_view_copy.length());
    });

    ScalarFieldProto typed_empty_data;
    typed_empty_data.mutable_int_data();
    auto typed_empty_array = Array(typed_empty_data);
    EXPECT_NO_THROW({
        auto typed_empty_view = ArrayView(typed_empty_array);
        auto typed_empty_view_copy = typed_empty_view;
        EXPECT_EQ(0, typed_empty_view_copy.length());
        EXPECT_EQ(DataType::INT32, typed_empty_view_copy.get_element_type());
    });
}

TEST(Array, TestLiteralElementTypeMismatch) {
    using namespace milvus;

    auto expect_mismatch = [](const Array& array,
                              const proto::plan::Array& literal) {
        EXPECT_FALSE(array.is_same_array(literal));

        auto array_view = ArrayView(const_cast<char*>(array.data()),
                                    array.length(),
                                    array.byte_size(),
                                    array.get_element_type(),
                                    array.get_offsets_data());
        EXPECT_FALSE(array_view.is_same_array(literal));
    };

    proto::schema::ScalarField int64_data;
    int64_data.mutable_long_data()->add_data(0);
    auto int64_array = Array(int64_data);
    proto::plan::Array float_literal;
    float_literal.set_same_type(true);
    float_literal.mutable_array()->Add()->set_float_val(1.5);
    expect_mismatch(int64_array, float_literal);

    proto::schema::ScalarField bool_data;
    bool_data.mutable_bool_data()->add_data(false);
    auto bool_array = Array(bool_data);
    proto::plan::Array int_literal;
    int_literal.set_same_type(true);
    int_literal.mutable_array()->Add()->set_int64_val(0);
    expect_mismatch(bool_array, int_literal);

    proto::schema::ScalarField string_data;
    string_data.mutable_string_data()->add_data("");
    auto string_array = Array(string_data);
    proto::plan::Array bool_literal;
    bool_literal.set_same_type(true);
    bool_literal.mutable_array()->Add()->set_bool_val(false);
    expect_mismatch(string_array, bool_literal);
}

namespace {

milvus::ScalarFieldProto
BuildElementNullableIntArray(const std::vector<int32_t>& values,
                             const std::vector<bool>& valid_data) {
    milvus::ScalarFieldProto proto;
    proto.mutable_int_data()->mutable_data()->Add(values.begin(), values.end());
    for (auto valid : valid_data) {
        proto.add_valid_data(valid);
    }
    return proto;
}

milvus::ScalarFieldProto
BuildElementNullableStringArray(const std::vector<std::string>& values,
                                const std::vector<bool>& valid_data) {
    milvus::ScalarFieldProto proto;
    for (const auto& value : values) {
        proto.mutable_string_data()->add_data(value);
    }
    for (auto valid : valid_data) {
        proto.add_valid_data(valid);
    }
    return proto;
}

}  // namespace

TEST(Array, ElementNullableRoundTripPreservesDensePayloadAndValidity) {
    using namespace milvus;

    auto input = BuildElementNullableIntArray(
        {10, 20, 30, 40}, {true, false, true, true});
    Array array(input, true);

    ASSERT_TRUE(array.is_element_nullable());
    ASSERT_TRUE(array.has_invalid_element());
    ASSERT_EQ(array.length(), 4);
    EXPECT_TRUE(array.is_element_valid(0));
    EXPECT_FALSE(array.is_element_valid(1));
    EXPECT_TRUE(array.is_element_valid(2));
    EXPECT_TRUE(array.is_element_valid(3));
    EXPECT_EQ(array.get_data_unchecked<int32_t>(1), 20);

    auto output = array.output_data();
    ASSERT_EQ(output.int_data().data_size(), 4);
    ASSERT_EQ(output.valid_data_size(), 4);
    EXPECT_EQ(output.int_data().data(1), 20);
    EXPECT_FALSE(output.valid_data(1));

    Array restored(output, true);
    EXPECT_EQ(restored, array);

    ArrayView view(array);
    auto view_output = view.output_data();
    EXPECT_EQ(view_output.SerializeAsString(), output.SerializeAsString());
}

TEST(Array, ElementNullableCopyAndEqualityIgnoreInvalidPayload) {
    using namespace milvus;

    Array left(BuildElementNullableIntArray(
                   {10, 20, 30}, {true, false, true}),
               true);
    Array same_logical_value(BuildElementNullableIntArray(
                                 {10, 99, 30}, {true, false, true}),
                             true);
    Array different_validity(BuildElementNullableIntArray(
                                 {10, 20, 30}, {true, true, true}),
                             true);

    EXPECT_EQ(left, same_logical_value);
    EXPECT_NE(left, different_validity);

    Array copied(left);
    EXPECT_EQ(copied, left);
    Array assigned;
    assigned = left;
    EXPECT_EQ(assigned, left);
}

TEST(Array, ElementNullableStringRoundTripPreservesOffsets) {
    using namespace milvus;

    Array array(BuildElementNullableStringArray(
                    {"alpha", "placeholder", "gamma"},
                    {true, false, true}),
                true);
    ArrayView view(array);

    EXPECT_EQ(view.get_data_unchecked<std::string_view>(0), "alpha");
    EXPECT_EQ(view.get_data_unchecked<std::string_view>(1), "placeholder");
    EXPECT_EQ(view.get_data_unchecked<std::string_view>(2), "gamma");
    EXPECT_FALSE(view.is_element_valid(1));

    auto output = view.output_data();
    ASSERT_EQ(output.string_data().data_size(), 3);
    EXPECT_EQ(output.string_data().data(1), "placeholder");
    EXPECT_FALSE(output.valid_data(1));
}

TEST(Array, ElementNullableValidationRejectsAmbiguousInput) {
    using namespace milvus;

    auto input = BuildElementNullableIntArray({1, 2}, {true, false});
    EXPECT_ANY_THROW((void)Array(input));

    input.add_valid_data(true);
    EXPECT_ANY_THROW(Array(input, true));
}

TEST(Array, ElementNullablePlanLiteralRequiresAllElementsValid) {
    using namespace milvus;

    proto::plan::Array literal;
    literal.set_same_type(true);
    literal.mutable_array()->Add()->set_int64_val(1);
    literal.mutable_array()->Add()->set_int64_val(2);

    Array with_null(
        BuildElementNullableIntArray({1, 2}, {true, false}), true);
    Array all_valid(
        BuildElementNullableIntArray({1, 2}, {true, true}), true);

    EXPECT_FALSE(with_null.is_same_array(literal));
    EXPECT_TRUE(all_valid.is_same_array(literal));
    EXPECT_FALSE(ArrayView(with_null).is_same_array(literal));
    EXPECT_TRUE(ArrayView(all_valid).is_same_array(literal));
}
