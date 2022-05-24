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

#pragma once

#include <gtest/gtest.h>
#include <vector>
#include <memory>

using milvus::scalar::ScalarIndexPtr;

namespace {
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

    auto bitset1 = index->Range(test_min - 1, milvus::OpType::GreaterThan);
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());

    auto bitset2 = index->Range(test_min, milvus::OpType::GreaterEqual);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset3 = index->Range(test_max + 1, milvus::OpType::LessThan);
    ASSERT_EQ(arr.size(), bitset3->size());
    ASSERT_TRUE(bitset3->any());

    auto bitset4 = index->Range(test_max, milvus::OpType::LessEqual);
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

    auto bitset2 = index->Range(test_min, milvus::OpType::GreaterEqual);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset4 = index->Range(test_max, milvus::OpType::LessEqual);
    ASSERT_EQ(arr.size(), bitset4->size());
    ASSERT_TRUE(bitset4->any());

    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5->size());
    ASSERT_TRUE(bitset5->any());
}
}  // namespace
