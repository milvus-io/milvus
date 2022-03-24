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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <map>
#include <tuple>
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/ConfAdapterMgr.h>
#include <knowhere/archive/KnowhereConfig.h>
#include "pb/index_cgo_msg.pb.h"

#define private public

#include "index/IndexFactory.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "index/ScalarIndexSort.h"
#include "common/CDataType.h"
#include "test_utils/indexbuilder_test_utils.h"

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using milvus::scalar::ScalarIndexPtr;

namespace {
template <typename T>
inline std::vector<std::string>
GetIndexTypes() {
    return std::vector<std::string>{"inverted_index"};
}

template <>
inline std::vector<std::string>
GetIndexTypes<std::string>() {
    return std::vector<std::string>{"marisa-trie"};
}

template <typename T>
inline void
assert_in(const ScalarIndexPtr<T>& index, const std::vector<T>& arr) {
    // hard to compare floating point value.
    if (std::is_floating_point_v<T>) {
        return;
    }

    auto bitset1 = index->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = index->In(1, test.get());
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->none());
}
template <typename T>
inline void
assert_not_in(const ScalarIndexPtr<T>& index, const std::vector<T>& arr) {
    auto bitset1 = index->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->none());
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = index->NotIn(1, test.get());
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());
}

template <typename T>
inline void
assert_range(const ScalarIndexPtr<T>& index, const std::vector<T>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset1 = index->Range(test_min - 1, OperatorType::GT);
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());

    auto bitset2 = index->Range(test_min, OperatorType::GE);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset3 = index->Range(test_max + 1, OperatorType::LT);
    ASSERT_EQ(arr.size(), bitset3->size());
    ASSERT_TRUE(bitset3->any());

    auto bitset4 = index->Range(test_max, OperatorType::LE);
    ASSERT_EQ(arr.size(), bitset4->size());
    ASSERT_TRUE(bitset4->any());

    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5->size());
    ASSERT_TRUE(bitset5->any());
}

template <>
inline void
assert_in(const ScalarIndexPtr<std::string>& index, const std::vector<std::string>& arr) {
    auto bitset1 = index->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());
}

template <>
inline void
assert_not_in(const ScalarIndexPtr<std::string>& index, const std::vector<std::string>& arr) {
    auto bitset1 = index->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->none());
}

template <>
inline void
assert_range(const ScalarIndexPtr<std::string>& index, const std::vector<std::string>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset2 = index->Range(test_min, OperatorType::GE);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset4 = index->Range(test_max, OperatorType::LE);
    ASSERT_EQ(arr.size(), bitset4->size());
    ASSERT_TRUE(bitset4->any());

    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5->size());
    ASSERT_TRUE(bitset5->any());
}
}  // namespace

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
using ArithmeticT = ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double, std::string>;

REGISTER_TYPED_TEST_CASE_P(TypedScalarIndexTest, Dummy, Constructor, In, NotIn, Range, Codec);

INSTANTIATE_TYPED_TEST_CASE_P(ArithmeticCheck, TypedScalarIndexTest, ArithmeticT);

// TODO: bool.
