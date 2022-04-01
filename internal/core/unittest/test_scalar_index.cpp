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
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/ConfAdapterMgr.h>

#include "index/IndexFactory.h"
#include "common/CDataType.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using milvus::scalar::ScalarIndexPtr;

template <typename T>
class TypedScalarIndexTest : public ::testing::Test {
 protected:
    // void
    // SetUp() override {
    // }

    // void
    // TearDown() override {
    // }
};

TYPED_TEST_CASE_P(TypedScalarIndexTest);

TYPED_TEST_P(TypedScalarIndexTest, Dummy) {
    using T = TypeParam;
    std::cout << typeid(T()).name() << std::endl;
    std::cout << milvus::GetDType<T>() << std::endl;
}

TYPED_TEST_P(TypedScalarIndexTest, Constructor) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex(dtype, index_type);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, In) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<T>(index_type);
        auto arr = GenArr<T>(nb);
        index->Build(nb, arr.data());
        assert_in<T>(index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, NotIn) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<T>(index_type);
        auto arr = GenArr<T>(nb);
        index->Build(nb, arr.data());
        assert_not_in<T>(index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Range) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<T>(index_type);
        auto arr = GenArr<T>(nb);
        index->Build(nb, arr.data());
        assert_range<T>(index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Codec) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        auto index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<T>(index_type);
        auto arr = GenArr<T>(nb);
        index->Build(nb, arr.data());

        auto binary_set = index->Serialize(nullptr);
        auto copy_index = milvus::scalar::IndexFactory::GetInstance().CreateIndex<T>(index_type);
        copy_index->Load(binary_set);

        assert_in<T>(copy_index, arr);
        assert_not_in<T>(copy_index, arr);
        assert_range<T>(copy_index, arr);
    }
}

// TODO: it's easy to overflow for int8_t. Design more reasonable ut.
using ScalarT = ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double>;

REGISTER_TYPED_TEST_CASE_P(TypedScalarIndexTest, Dummy, Constructor, In, NotIn, Range, Codec);

INSTANTIATE_TYPED_TEST_CASE_P(ArithmeticCheck, TypedScalarIndexTest, ScalarT);
