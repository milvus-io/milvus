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

#include <gtest/gtest.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"

using namespace milvus;

TEST(VectorArray, TestSchema) {
    namespace milvus_pb = milvus::proto;
    milvus_pb::schema::CollectionSchema proto;
    proto.set_name("col");
    proto.set_description("asdfhsalkgfhsadg");
    std::string varchar_dafualt_vlaue = "20";

    {
        auto field = proto.add_fields();
        field->set_name("key");
        field->set_nullable(false);
        field->set_fieldid(100);
        field->set_is_primary_key(true);
        field->set_description("asdgfsagf");
        field->set_data_type(milvus_pb::schema::DataType::Int64);
    }

    {
        auto struct_field = proto.add_struct_array_fields();
        struct_field->set_name("struct");
        struct_field->set_fieldid(101);

        auto field = struct_field->add_fields();
        field->set_name("struct_key");
        field->set_nullable(false);
        field->set_fieldid(102);
        field->set_data_type(milvus_pb::schema::DataType::Array);
        field->set_element_type(milvus_pb::schema::DataType::Int64);

        auto field2 = struct_field->add_fields();
        field2->set_name("struct_float_vec");
        field2->set_fieldid(103);
        field2->set_data_type(milvus_pb::schema::DataType::ArrayOfVector);
        field2->set_element_type(milvus_pb::schema::DataType::FloatVector);
        auto param = field2->add_type_params();
        param->set_key("dim");
        param->set_value("16");
        auto iparam = field2->add_index_params();
        iparam->set_key("metric_type");
        iparam->set_value("L2");
    }

    auto schema = Schema::ParseFrom(proto);
    auto field = schema->operator[](FieldId(102));
    ASSERT_EQ(field.get_data_type(), DataType::ARRAY);
    ASSERT_EQ(field.get_element_type(), DataType::INT64);

    auto field2 = schema->operator[](FieldId(103));
    ASSERT_EQ(field2.get_data_type(), DataType::VECTOR_ARRAY);
    ASSERT_EQ(field2.get_element_type(), DataType::VECTOR_FLOAT);
    ASSERT_EQ(field2.get_dim(), 16);
}

std::vector<float>
generate_float_vector(int64_t seed, int64_t N, int64_t dim) {
    std::vector<float> final(dim * N);
    for (int n = 0; n < N; ++n) {
        // generate random float vector
        std::vector<float> data(dim);
        std::default_random_engine er2(seed + n);
        std::normal_distribution<> distr2(0, 1);
        for (auto& x : data) {
            x = distr2(er2);
        }

        std::copy(data.begin(), data.end(), final.begin() + dim * n);
    }
    return final;
};

namespace {

struct ElementNullableByteVectorParam {
    DataType data_type;
    int64_t dim;
    std::string name;
};

void
SetByteVectorPayload(VectorFieldProto* field,
                     DataType data_type,
                     const std::string& payload) {
    switch (data_type) {
        case DataType::VECTOR_BINARY:
            field->set_binary_vector(payload);
            return;
        case DataType::VECTOR_FLOAT16:
            field->set_float16_vector(payload);
            return;
        case DataType::VECTOR_BFLOAT16:
            field->set_bfloat16_vector(payload);
            return;
        case DataType::VECTOR_INT8:
            field->set_int8_vector(payload);
            return;
        default:
            FAIL() << "unsupported byte vector type "
                   << static_cast<int>(data_type);
    }
}

std::string
GetByteVectorPayload(const VectorFieldProto& field, DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_BINARY:
            return field.binary_vector();
        case DataType::VECTOR_FLOAT16:
            return field.float16_vector();
        case DataType::VECTOR_BFLOAT16:
            return field.bfloat16_vector();
        case DataType::VECTOR_INT8:
            return field.int8_vector();
        default:
            ADD_FAILURE() << "unsupported byte vector type "
                          << static_cast<int>(data_type);
            return {};
    }
}

class ElementNullableByteVectorArrayTest
    : public ::testing::TestWithParam<ElementNullableByteVectorParam> {};

}  // namespace

TEST(VectorArray, TestConstructVectorArray) {
    using namespace milvus;

    int N = 10;
    // 1. test float vector
    int64_t dim = 128;
    milvus::proto::schema::VectorField field_float_vector_array;
    field_float_vector_array.set_dim(dim);

    auto data = generate_float_vector(100, N, dim);
    field_float_vector_array.mutable_float_vector()->mutable_data()->Add(
        data.begin(), data.end());

    auto float_vector_array = milvus::VectorArray(field_float_vector_array);
    ASSERT_EQ(float_vector_array.length(), N);
    ASSERT_EQ(float_vector_array.dim(), dim);
    ASSERT_EQ(float_vector_array.get_element_type(), DataType::VECTOR_FLOAT);
    ASSERT_EQ(float_vector_array.byte_size(), N * dim * sizeof(float));

    ASSERT_TRUE(float_vector_array.is_same_array(field_float_vector_array));

    auto float_vector_array_tmp = milvus::VectorArray(float_vector_array);

    ASSERT_TRUE(float_vector_array_tmp.is_same_array(field_float_vector_array));

    auto float_vector_array_view =
        milvus::VectorArrayView(const_cast<char*>(float_vector_array.data()),
                                float_vector_array.length(),
                                float_vector_array.dim(),
                                float_vector_array.byte_size(),
                                float_vector_array.get_element_type());

    ASSERT_TRUE(
        float_vector_array_view.is_same_array(field_float_vector_array));

    // todo: add other vector types
}

TEST(VectorArray, TestConstructorWithData) {
    using namespace milvus;

    int N = 10;  // number of vectors
    int64_t dim = 128;

    // Generate test data
    auto data = generate_float_vector(42, N, dim);

    // Test 1: Direct construction from raw float data
    {
        milvus::VectorArray va(data.data(), N, dim, DataType::VECTOR_FLOAT);

        ASSERT_EQ(va.length(), N);
        ASSERT_EQ(va.dim(), dim);
        ASSERT_EQ(va.get_element_type(), DataType::VECTOR_FLOAT);
        ASSERT_EQ(va.byte_size(), N * dim * sizeof(float));

        // Verify data integrity
        for (int i = 0; i < N; ++i) {
            auto vec_data = va.get_data<float>(i);
            for (int j = 0; j < dim; ++j) {
                ASSERT_FLOAT_EQ(vec_data[j], data[i * dim + j]);
            }
        }
    }

    // Test 2: Compare with protobuf-based constructor
    {
        // Create via protobuf
        milvus::proto::schema::VectorField field_proto;
        field_proto.set_dim(dim);
        field_proto.mutable_float_vector()->mutable_data()->Add(data.begin(),
                                                                data.end());
        milvus::VectorArray va_proto(field_proto);

        // Create via data constructor
        milvus::VectorArray va_direct(
            data.data(), N, dim, DataType::VECTOR_FLOAT);

        // Both should be equal
        ASSERT_EQ(va_proto.length(), va_direct.length());
        ASSERT_EQ(va_proto.dim(), va_direct.dim());
        ASSERT_EQ(va_proto.byte_size(), va_direct.byte_size());
        ASSERT_EQ(va_proto.get_element_type(), va_direct.get_element_type());

        // Compare data
        for (int i = 0; i < N; ++i) {
            auto proto_vec = va_proto.get_data<float>(i);
            auto direct_vec = va_direct.get_data<float>(i);
            for (int j = 0; j < dim; ++j) {
                ASSERT_FLOAT_EQ(proto_vec[j], direct_vec[j]);
            }
        }
    }

    // Test 3: Test with edge cases
    {
        // Single vector
        milvus::VectorArray va_single(
            data.data(), 1, dim, DataType::VECTOR_FLOAT);
        ASSERT_EQ(va_single.length(), 1);
        ASSERT_EQ(va_single.byte_size(), dim * sizeof(float));

        // Small dimension
        int64_t small_dim = 4;
        auto small_data = generate_float_vector(123, 5, small_dim);
        milvus::VectorArray va_small(
            small_data.data(), 5, small_dim, DataType::VECTOR_FLOAT);
        ASSERT_EQ(va_small.length(), 5);
        ASSERT_EQ(va_small.dim(), small_dim);
    }
}

TEST(VectorArray, ElementNullableCompactProtoExpandsToDenseRuntime) {
    constexpr int64_t dim = 2;

    VectorFieldProto input;
    input.set_dim(dim);
    input.mutable_float_vector()->add_data(1.0F);
    input.mutable_float_vector()->add_data(2.0F);
    input.mutable_float_vector()->add_data(5.0F);
    input.mutable_float_vector()->add_data(6.0F);
    input.add_valid_data(true);
    input.add_valid_data(false);
    input.add_valid_data(true);

    milvus::VectorArray array(input, true);
    ASSERT_TRUE(array.is_element_nullable());
    ASSERT_TRUE(array.has_invalid_element());
    ASSERT_EQ(array.length(), 3);
    EXPECT_TRUE(array.is_element_valid(0));
    EXPECT_FALSE(array.is_element_valid(1));
    EXPECT_TRUE(array.is_element_valid(2));

    auto first = array.get_data<float>(0);
    EXPECT_FLOAT_EQ(first[0], 1.0F);
    EXPECT_FLOAT_EQ(first[1], 2.0F);
    auto null_slot = array.get_data<float>(1);
    EXPECT_FLOAT_EQ(null_slot[0], 0.0F);
    EXPECT_FLOAT_EQ(null_slot[1], 0.0F);
    auto third = array.get_data<float>(2);
    EXPECT_FLOAT_EQ(third[0], 5.0F);
    EXPECT_FLOAT_EQ(third[1], 6.0F);

    auto output = array.output_data();
    ASSERT_EQ(output.valid_data_size(), 3);
    EXPECT_TRUE(output.valid_data(0));
    EXPECT_FALSE(output.valid_data(1));
    EXPECT_TRUE(output.valid_data(2));
    ASSERT_EQ(output.float_vector().data_size(), 4);
    EXPECT_FLOAT_EQ(output.float_vector().data(0), 1.0F);
    EXPECT_FLOAT_EQ(output.float_vector().data(1), 2.0F);
    EXPECT_FLOAT_EQ(output.float_vector().data(2), 5.0F);
    EXPECT_FLOAT_EQ(output.float_vector().data(3), 6.0F);

    milvus::VectorArray restored(output, true);
    EXPECT_EQ(restored, array);
    VectorArrayView view(array);
    EXPECT_EQ(view.output_data().SerializeAsString(),
              output.SerializeAsString());
}

TEST(VectorArray, ElementNullableByteVectorRoundTrip) {
    constexpr int64_t dim = 16;

    VectorFieldProto input;
    input.set_dim(dim);
    input.mutable_binary_vector()->assign("\x01\x02\x03\x04", 4);
    input.add_valid_data(true);
    input.add_valid_data(false);
    input.add_valid_data(true);

    milvus::VectorArray array(input, true);
    ASSERT_EQ(array.length(), 3);
    EXPECT_EQ(array.byte_size(), 6);
    auto dense_data = reinterpret_cast<const uint8_t*>(array.data());
    EXPECT_EQ(dense_data[0], 0x01);
    EXPECT_EQ(dense_data[1], 0x02);
    EXPECT_EQ(dense_data[2], 0x00);
    EXPECT_EQ(dense_data[3], 0x00);
    EXPECT_EQ(dense_data[4], 0x03);
    EXPECT_EQ(dense_data[5], 0x04);

    auto output = array.output_data();
    EXPECT_EQ(output.binary_vector(), std::string("\x01\x02\x03\x04", 4));
    EXPECT_EQ(output.valid_data_size(), 3);
}

TEST_P(ElementNullableByteVectorArrayTest, CompactProtoExpandsAndRoundTrips) {
    const auto& param = GetParam();
    const auto bytes_per_vector =
        vector_bytes_per_element(param.data_type, param.dim);
    std::string compact_payload(bytes_per_vector * 2, '\0');
    for (size_t i = 0; i < compact_payload.size(); ++i) {
        compact_payload[i] = static_cast<char>(i + 1);
    }

    VectorFieldProto input;
    input.set_dim(param.dim);
    SetByteVectorPayload(&input, param.data_type, compact_payload);
    input.add_valid_data(true);
    input.add_valid_data(false);
    input.add_valid_data(true);

    milvus::VectorArray array(input, true);
    ASSERT_EQ(array.get_element_type(), param.data_type);
    ASSERT_EQ(array.length(), 3);
    ASSERT_EQ(array.byte_size(), bytes_per_vector * 3);
    ASSERT_TRUE(array.has_invalid_element());

    const auto* dense = array.data();
    EXPECT_EQ(std::string(dense, bytes_per_vector),
              compact_payload.substr(0, bytes_per_vector));
    EXPECT_EQ(std::string(dense + bytes_per_vector, bytes_per_vector),
              std::string(bytes_per_vector, '\0'));
    EXPECT_EQ(std::string(dense + bytes_per_vector * 2, bytes_per_vector),
              compact_payload.substr(bytes_per_vector, bytes_per_vector));

    auto output = array.output_data();
    EXPECT_EQ(GetByteVectorPayload(output, param.data_type), compact_payload);
    ASSERT_EQ(output.valid_data_size(), 3);
    EXPECT_TRUE(output.valid_data(0));
    EXPECT_FALSE(output.valid_data(1));
    EXPECT_TRUE(output.valid_data(2));

    milvus::VectorArray restored(output, true);
    EXPECT_EQ(std::string(restored.data(), restored.byte_size()),
              std::string(array.data(), array.byte_size()));
    EXPECT_EQ(VectorArrayView(array).output_data().SerializeAsString(),
              output.SerializeAsString());
}

TEST_P(ElementNullableByteVectorArrayTest,
       AllInvalidElementsPreserveTypedEmptyPayload) {
    const auto& param = GetParam();
    VectorFieldProto input;
    input.set_dim(param.dim);
    SetByteVectorPayload(&input, param.data_type, {});
    input.add_valid_data(false);
    input.add_valid_data(false);

    milvus::VectorArray array(input, true);
    ASSERT_EQ(array.length(), 2);
    ASSERT_TRUE(array.has_invalid_element());
    EXPECT_EQ(array.byte_size(),
              vector_bytes_per_element(param.data_type, param.dim) * 2);

    auto output = array.output_data();
    EXPECT_NE(output.data_case(), VectorFieldProto::DATA_NOT_SET);
    EXPECT_TRUE(GetByteVectorPayload(output, param.data_type).empty());
    ASSERT_EQ(output.valid_data_size(), 2);
    EXPECT_FALSE(output.valid_data(0));
    EXPECT_FALSE(output.valid_data(1));
}

INSTANTIATE_TEST_SUITE_P(
    ByteVectorTypes,
    ElementNullableByteVectorArrayTest,
    ::testing::Values(
        ElementNullableByteVectorParam{
            DataType::VECTOR_BINARY, 16, "BinaryVector"},
        ElementNullableByteVectorParam{
            DataType::VECTOR_FLOAT16, 2, "Float16Vector"},
        ElementNullableByteVectorParam{
            DataType::VECTOR_BFLOAT16, 2, "BFloat16Vector"},
        ElementNullableByteVectorParam{DataType::VECTOR_INT8, 4, "Int8Vector"}),
    [](const ::testing::TestParamInfo<ElementNullableByteVectorParam>& info) {
        return info.param.name;
    });

TEST(VectorArray, ElementNullableValidationRejectsInvalidCompactPayload) {
    constexpr int64_t dim = 2;

    VectorFieldProto mismatched;
    mismatched.set_dim(dim);
    mismatched.mutable_float_vector()->add_data(1.0F);
    mismatched.mutable_float_vector()->add_data(2.0F);
    mismatched.mutable_float_vector()->add_data(3.0F);
    mismatched.mutable_float_vector()->add_data(4.0F);
    mismatched.add_valid_data(true);
    mismatched.add_valid_data(false);
    EXPECT_ANY_THROW(milvus::VectorArray(mismatched, true));

    VectorFieldProto unexpected_validity;
    unexpected_validity.set_dim(dim);
    unexpected_validity.mutable_float_vector()->add_data(1.0F);
    unexpected_validity.mutable_float_vector()->add_data(2.0F);
    unexpected_validity.add_valid_data(true);
    EXPECT_ANY_THROW((void)milvus::VectorArray(unexpected_validity));
}
