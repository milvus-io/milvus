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
#include <random>
#include <vector>
#include <string>

#include "common/VectorArray.h"
#include "pb/schema.pb.h"
#include "common/Schema.h"

using namespace milvus;

TEST(VectorArray, TestSchema) {
    namespace pb = milvus::proto;
    pb::schema::CollectionSchema proto;
    proto.set_name("col");
    proto.set_description("asdfhsalkgfhsadg");
    auto dim = 16;
    bool bool_default_value = true;
    int32_t int_default_value = 20;
    int64_t long_default_value = 20;
    float float_default_value = 20;
    double double_default_value = 20;
    std::string varchar_dafualt_vlaue = "20";

    {
        auto field = proto.add_fields();
        field->set_name("key");
        field->set_nullable(false);
        field->set_fieldid(100);
        field->set_is_primary_key(true);
        field->set_description("asdgfsagf");
        field->set_data_type(pb::schema::DataType::Int64);
    }

    {
        auto struct_field = proto.add_struct_array_fields();
        struct_field->set_name("struct");
        struct_field->set_fieldid(101);

        auto field = struct_field->add_fields();
        field->set_name("struct_key");
        field->set_nullable(false);
        field->set_fieldid(102);
        field->set_data_type(pb::schema::DataType::Array);
        field->set_element_type(pb::schema::DataType::Int64);

        auto field2 = struct_field->add_fields();
        field2->set_name("struct_float_vec");
        field2->set_fieldid(103);
        field2->set_data_type(pb::schema::DataType::ArrayOfVector);
        field2->set_element_type(pb::schema::DataType::FloatVector);
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
