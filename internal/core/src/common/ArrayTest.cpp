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
#include <algorithm>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Array.h"
#include "common/Span.h"
#include "common/Types.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

namespace {

milvus::ScalarFieldProto
OutputArrayForTest(const milvus::Array& array) {
    milvus::ScalarFieldProto output;
    array.output_data(output);
    return output;
}

void
ExpectArraysEqualForTest(const milvus::Array& left,
                         const milvus::Array& right) {
    EXPECT_EQ(left.get_element_type(), right.get_element_type());
    EXPECT_EQ(left.length(), right.length());
    EXPECT_EQ(OutputArrayForTest(left).SerializeAsString(),
              OutputArrayForTest(right).SerializeAsString());
}

std::vector<uint64_t>
ElementValidityForTest(const milvus::ScalarFieldProto& input) {
    std::vector<uint64_t> validity((input.valid_data_size() + 63) / 64, 0);
    for (int i = 0; i < input.valid_data_size(); ++i) {
        if (input.valid_data(i)) {
            validity[i / 64] |= uint64_t{1} << (i % 64);
        }
    }
    return validity;
}

milvus::ArrayView
MakeArrayViewForTest(const milvus::Array& array) {
    return milvus::ArrayView(const_cast<char*>(array.data()),
                             array.length(),
                             array.byte_size(),
                             array.get_element_type(),
                             array.get_offsets_data(),
                             array.get_element_valid_data(),
                             array.is_element_nullable(),
                             array.has_invalid_element());
}

milvus::ArrayView
MakeElementNullableArrayViewForTest(const milvus::Array& array,
                                    const std::vector<uint64_t>& validity) {
    return milvus::ArrayView(const_cast<char*>(array.data()),
                             array.length(),
                             array.byte_size(),
                             array.get_element_type(),
                             array.get_offsets_data(),
                             validity.data(),
                             true,
                             array.has_invalid_element());
}

}  // namespace

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
    ExpectArraysEqualForTest(int_array_tmp, int_array);
    auto int_array_view = ArrayView(const_cast<char*>(int_array.data()),
                                    int_array.length(),
                                    int_array.byte_size(),
                                    int_array.get_element_type(),
                                    int_array.get_offsets_data());
    ASSERT_EQ(int_array.length(), int_array_view.length());

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
    ExpectArraysEqualForTest(long_array_tmp, long_array);
    auto long_array_view = ArrayView(const_cast<char*>(long_array.data()),
                                     long_array.length(),
                                     long_array.byte_size(),
                                     long_array.get_element_type(),
                                     long_array.get_offsets_data());
    ASSERT_EQ(long_array.length(), long_array_view.length());

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
    ExpectArraysEqualForTest(string_array_tmp, string_array);
    auto string_array_view = ArrayView(const_cast<char*>(string_array.data()),
                                       string_array.length(),
                                       string_array.byte_size(),
                                       string_array.get_element_type(),
                                       string_array.get_offsets_data());
    ASSERT_EQ(string_array.length(), string_array_view.length());

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
    ExpectArraysEqualForTest(bool_array_tmp, bool_array);
    auto bool_array_view = ArrayView(const_cast<char*>(bool_array.data()),
                                     bool_array.length(),
                                     bool_array.byte_size(),
                                     bool_array.get_element_type(),
                                     bool_array.get_offsets_data());
    ASSERT_EQ(bool_array.length(), bool_array_view.length());

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
        ASSERT_DOUBLE_EQ(float_array.get_data_unchecked<float>(i),
                         float(i * 0.1));
    }
    ASSERT_TRUE(float_array.is_same_array(field_float_array));
    auto float_array_tmp = Array(const_cast<char*>(float_array.data()),
                                 float_array.length(),
                                 float_array.byte_size(),
                                 float_array.get_element_type(),
                                 float_array.get_offsets_data());
    ExpectArraysEqualForTest(float_array_tmp, float_array);
    auto float_array_view = ArrayView(const_cast<char*>(float_array.data()),
                                      float_array.length(),
                                      float_array.byte_size(),
                                      float_array.get_element_type(),
                                      float_array.get_offsets_data());
    ASSERT_EQ(float_array.length(), float_array_view.length());

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
        ASSERT_DOUBLE_EQ(double_array.get_data_unchecked<double>(i),
                         double(i * 0.1));
    }
    ASSERT_TRUE(double_array.is_same_array(field_double_array));
    auto double_array_tmp = Array(const_cast<char*>(double_array.data()),
                                  double_array.length(),
                                  double_array.byte_size(),
                                  double_array.get_element_type(),
                                  double_array.get_offsets_data());
    ExpectArraysEqualForTest(double_array_tmp, double_array);
    auto double_array_view = ArrayView(const_cast<char*>(double_array.data()),
                                       double_array.length(),
                                       double_array.byte_size(),
                                       double_array.get_element_type(),
                                       double_array.get_offsets_data());
    ASSERT_EQ(double_array.length(), double_array_view.length());

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
        auto typed_empty_view = MakeArrayViewForTest(typed_empty_array);
        auto typed_empty_view_copy = typed_empty_view;
        EXPECT_EQ(0, typed_empty_view_copy.length());
        EXPECT_EQ(ScalarFieldProto::kIntData,
                  typed_empty_view_copy.output_data().data_case());
    });
}

TEST(Array, TestLiteralElementTypeMismatch) {
    using namespace milvus;

    auto expect_mismatch = [](const Array& array,
                              const proto::plan::Array& literal) {
        EXPECT_FALSE(array.is_same_array(literal));

        auto array_view = MakeArrayViewForTest(array);
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

    auto input = BuildElementNullableIntArray({10, 20, 30, 40},
                                              {true, false, true, true});
    Array array(input, true);

    ASSERT_EQ(array.length(), 4);
    EXPECT_FALSE(array.is_element_valid(1));
    EXPECT_EQ(array.get_data_unchecked<int32_t>(1), 20);

    auto output = OutputArrayForTest(array);
    ASSERT_EQ(output.int_data().data_size(), 4);
    ASSERT_EQ(output.valid_data_size(), 4);
    EXPECT_EQ(output.int_data().data(1), 20);
    EXPECT_FALSE(output.valid_data(1));

    Array restored(output, true);
    ExpectArraysEqualForTest(restored, array);

    auto view = MakeArrayViewForTest(array);
    EXPECT_EQ(view.data(), array.data());
    auto view_output = view.output_data();
    EXPECT_EQ(view_output.SerializeAsString(), output.SerializeAsString());

    Array copied_from_view;
    view.output_data(copied_from_view);
    ExpectArraysEqualForTest(copied_from_view, array);
}

TEST(Array, ElementNullableRawConstructorPreservesDensePayloadAndValidity) {
    using namespace milvus;

    std::vector<int32_t> values = {10, 20, 30};
    const uint64_t validity[] = {0b101};

    Array array(reinterpret_cast<char*>(values.data()),
                static_cast<int>(values.size()),
                values.size() * sizeof(int32_t),
                DataType::INT32,
                nullptr,
                validity,
                true);

    ASSERT_EQ(array.length(), 3);
    EXPECT_FALSE(array.is_element_valid(1));
    EXPECT_EQ(array.get_data_unchecked<int32_t>(1), 20);
    auto output = OutputArrayForTest(array);
    ASSERT_EQ(output.int_data().data_size(), 3);
    EXPECT_EQ(output.int_data().data(0), 10);
    EXPECT_EQ(output.int_data().data(1), 20);
    EXPECT_EQ(output.int_data().data(2), 30);
    ASSERT_EQ(output.valid_data_size(), 3);
    EXPECT_TRUE(output.valid_data(0));
    EXPECT_FALSE(output.valid_data(1));
    EXPECT_TRUE(output.valid_data(2));
}

TEST(Array, ElementValidityPreservesWordBoundariesAndOwnership) {
    using namespace milvus;

    for (int length : {0, 1, 63, 64, 65, 129}) {
        for (int pattern : {0, 1, 2}) {
            SCOPED_TRACE(length);
            SCOPED_TRACE(pattern);
            std::vector<int32_t> values(length);
            std::vector<bool> valid_data(length);
            proto::plan::Array literal;
            literal.set_same_type(true);
            for (int i = 0; i < length; ++i) {
                values[i] = i + 10;
                valid_data[i] = pattern == 0 || (pattern == 2 && i % 2 == 0);
                literal.add_array()->set_int64_val(values[i]);
            }
            auto input = BuildElementNullableIntArray(values, valid_data);
            Array copied;
            {
                Array source(input, true);
                copied = source;
            }
            Array moved(std::move(copied));
            auto view = MakeArrayViewForTest(moved);
            EXPECT_EQ(view.data(), moved.data());
            EXPECT_EQ(view.output_data().SerializeAsString(),
                      input.SerializeAsString());
            const bool all_valid = std::all_of(
                valid_data.begin(), valid_data.end(), [](bool valid) {
                    return valid;
                });
            EXPECT_EQ(moved.is_same_array(literal), all_valid);
            EXPECT_EQ(view.is_same_array(literal), all_valid);

            auto validity = ElementValidityForTest(input);
            auto raw_view =
                MakeElementNullableArrayViewForTest(moved, validity);
            EXPECT_EQ(raw_view.is_same_array(literal), all_valid);
            ArrayView copied_view(raw_view);
            Array restored;
            copied_view.output_data(restored);
            ExpectArraysEqualForTest(restored, moved);
            EXPECT_TRUE(restored.is_element_nullable());
            EXPECT_EQ(restored.has_invalid_element(), !all_valid);
        }
    }
}

TEST(Array, ElementValidityRawCopyOwnsWordsAndIgnoresPadding) {
    using namespace milvus;

    constexpr int length = 129;
    std::vector<int32_t> values(length, 10);
    std::vector<uint64_t> validity((length + 63) / 64, 0);
    for (int i = 0; i < length; ++i) {
        if (i % 3 != 0) {
            validity[i / 64] |= uint64_t{1} << (i % 64);
        }
    }
    Array array(reinterpret_cast<char*>(values.data()),
                length,
                values.size() * sizeof(int32_t),
                DataType::INT32,
                nullptr,
                validity.data(),
                true);
    std::fill(validity.begin(), validity.end(), 0);
    auto view = MakeArrayViewForTest(array);
    for (int i = 0; i < length; ++i) {
        EXPECT_EQ(array.is_element_valid(i), i % 3 != 0);
        EXPECT_EQ(view.is_element_valid(i), i % 3 != 0);
    }

    // Only bit zero in the last word belongs to the array.
    std::fill(validity.begin(), validity.end(), ~uint64_t{0});
    validity.back() = 1;
    Array all_valid(reinterpret_cast<char*>(values.data()),
                    length,
                    values.size() * sizeof(int32_t),
                    DataType::INT32,
                    nullptr,
                    validity.data(),
                    true);
    EXPECT_FALSE(all_valid.has_invalid_element());

    validity.back() = ~uint64_t{1};
    Array tail_null(reinterpret_cast<char*>(values.data()),
                    length,
                    values.size() * sizeof(int32_t),
                    DataType::INT32,
                    nullptr,
                    validity.data(),
                    true);
    EXPECT_TRUE(tail_null.has_invalid_element());
    EXPECT_FALSE(tail_null.is_element_valid(length - 1));
}

TEST(Array, LegacySpanPreservesRowStrideAndElementValidity) {
    using namespace milvus;

    std::vector<Array> rows;
    rows.emplace_back();
    rows.emplace_back(BuildElementNullableIntArray({10, 20}, {}), false);
    rows.emplace_back(
        BuildElementNullableIntArray({30, 40, 50}, {true, false, true}), true);
    rows.emplace_back(
        BuildElementNullableStringArray({"first", "placeholder", "last"},
                                        {true, false, true}),
        true);
    rows.emplace_back(BuildElementNullableIntArray({}, {}), true);
    const Span<ArrayView> views(
        SpanBase(rows.data(), rows.size(), sizeof(Array)));
    for (size_t i = 0; i < rows.size(); ++i) {
        SCOPED_TRACE(i);
        const auto& view = views.data()[i];
        EXPECT_EQ(view.length(), rows[i].length());
        EXPECT_EQ(view.data(), rows[i].data());
        EXPECT_EQ(view.output_data().SerializeAsString(),
                  rows[i].output_data().SerializeAsString());
    }
}

TEST(Array, ElementNullableTypedEmptyArrayPreservesElementType) {
    using namespace milvus;

    ScalarFieldProto input;
    input.mutable_int_data();

    Array array(input, true);
    EXPECT_EQ(array.length(), 0);
    EXPECT_EQ(array.byte_size(), 0);

    auto output = OutputArrayForTest(array);
    EXPECT_EQ(output.data_case(), ScalarFieldProto::kIntData);
    EXPECT_EQ(output.int_data().data_size(), 0);
    EXPECT_EQ(output.valid_data_size(), 0);
}

TEST(Array, ElementNullableAllNullArrayPreservesDensePlaceholders) {
    using namespace milvus;

    auto input = BuildElementNullableIntArray({10, 20}, {false, false});
    Array array(input, true);

    ASSERT_EQ(array.length(), 2);
    auto output = OutputArrayForTest(array);
    ASSERT_EQ(output.int_data().data_size(), 2);
    EXPECT_EQ(output.int_data().data(0), 10);
    EXPECT_EQ(output.int_data().data(1), 20);
    ASSERT_EQ(output.valid_data_size(), 2);
    EXPECT_FALSE(output.valid_data(0));
    EXPECT_FALSE(output.valid_data(1));
}

TEST(Array, ElementNullableCopyPreservesPayloadAndValidity) {
    using namespace milvus;

    Array left(BuildElementNullableIntArray({10, 20, 30}, {true, false, true}),
               true);
    Array copied(left);
    ExpectArraysEqualForTest(copied, left);
    Array assigned;
    assigned = left;
    ExpectArraysEqualForTest(assigned, left);
}

TEST(Array, ElementNullableStringRoundTripPreservesOffsets) {
    using namespace milvus;

    auto input = BuildElementNullableStringArray(
        {"alpha", "placeholder", "gamma"}, {true, false, true});
    Array array(input, true);
    auto view = MakeArrayViewForTest(array);
    EXPECT_EQ(view.data(), array.data());

    EXPECT_TRUE(view.is_element_valid(0));
    EXPECT_FALSE(view.is_element_valid(1));
    EXPECT_TRUE(view.is_element_valid(2));
    EXPECT_EQ(view.get_data_unchecked<std::string_view>(0), "alpha");
    EXPECT_EQ(view.get_data_unchecked<std::string_view>(1), "placeholder");
    EXPECT_EQ(view.get_data_unchecked<std::string_view>(2), "gamma");
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

    int32_t value = 1;
    auto* payload = reinterpret_cast<char*>(&value);
    const uint64_t validity = 1;
    EXPECT_ANY_THROW(Array(
        payload, 1, sizeof(value), DataType::INT32, nullptr, nullptr, true));
    EXPECT_ANY_THROW(ArrayView(payload,
                               1,
                               sizeof(value),
                               DataType::INT32,
                               nullptr,
                               nullptr,
                               true,
                               false));
    EXPECT_ANY_THROW(Array(
        payload, 1, sizeof(value), DataType::INT32, nullptr, &validity, false));
    EXPECT_ANY_THROW(ArrayView(payload,
                               1,
                               sizeof(value),
                               DataType::INT32,
                               nullptr,
                               &validity,
                               false,
                               false));
}

TEST(Array, ElementNullablePlanLiteralRequiresAllElementsValid) {
    using namespace milvus;

    proto::plan::Array literal;
    literal.set_same_type(true);
    literal.mutable_array()->Add()->set_int64_val(1);
    literal.mutable_array()->Add()->set_int64_val(2);

    auto with_null_input = BuildElementNullableIntArray({1, 2}, {true, false});
    auto all_valid_input = BuildElementNullableIntArray({1, 2}, {true, true});
    Array with_null(with_null_input, true);
    Array all_valid(all_valid_input, true);

    EXPECT_FALSE(with_null.is_same_array(literal));
    EXPECT_TRUE(all_valid.is_same_array(literal));
    auto with_null_validity = ElementValidityForTest(with_null_input);
    auto all_valid_validity = ElementValidityForTest(all_valid_input);
    EXPECT_FALSE(
        MakeElementNullableArrayViewForTest(with_null, with_null_validity)
            .is_same_array(literal));
    EXPECT_TRUE(
        MakeElementNullableArrayViewForTest(all_valid, all_valid_validity)
            .is_same_array(literal));
}

TEST(Array, CopyAndMoveExceptionContracts) {
    using milvus::Array;
    EXPECT_FALSE(std::is_nothrow_copy_constructible_v<Array>);
    EXPECT_TRUE(std::is_nothrow_move_constructible_v<Array>);
    EXPECT_TRUE(std::is_nothrow_move_assignable_v<Array>);

    milvus::proto::schema::ScalarField field;
    field.mutable_string_data()->add_data("first");
    field.mutable_string_data()->add_data("second");
    Array source(field);
    Array copy(source);
    Array assigned;
    assigned = source;
    EXPECT_NE(copy.data(), source.data());
    EXPECT_NE(copy.get_offsets_data(), source.get_offsets_data());
    EXPECT_NE(assigned.data(), source.data());
    EXPECT_EQ(copy.get_data_unchecked<std::string_view>(1), "second");
    EXPECT_EQ(assigned.get_data_unchecked<std::string_view>(0), "first");
}
