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

#include "common/Array.h"

TEST(Array, TestConstructArray) {
    using namespace milvus;

    int N = 10;
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
        ASSERT_EQ(int_array.get_data<int>(i), i);
    }
    ASSERT_TRUE(int_array.is_same_array(field_int_array));

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
        ASSERT_EQ(long_array.get_data<int64_t>(i), i);
    }
    ASSERT_TRUE(long_array.is_same_array(field_int_array));

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
    //    ASSERT_EQ(N, string_array.size());
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(string_array.get_data<std::string_view>(i),
                  std::to_string(i));
    }
    ASSERT_TRUE(string_array.is_same_array(field_string_array));

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
        ASSERT_EQ(bool_array.get_data<bool>(i), bool(i));
    }
    ASSERT_TRUE(bool_array.is_same_array(field_bool_array));

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
        ASSERT_DOUBLE_EQ(float_array.get_data<float>(i), float(i * 0.1));
    }
    ASSERT_TRUE(float_array.is_same_array(field_float_array));

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
        ASSERT_DOUBLE_EQ(double_array.get_data<double>(i), double(i * 0.1));
    }
    ASSERT_TRUE(double_array.is_same_array(field_double_array));

    milvus::proto::schema::ScalarField field_empty_data;
    milvus::proto::plan::Array field_empty_array;
    auto empty_array = Array(field_empty_data);
    ASSERT_EQ(0, empty_array.length());
    ASSERT_EQ(0, empty_array.byte_size());
    ASSERT_TRUE(empty_array.is_same_array(field_empty_array));
}
