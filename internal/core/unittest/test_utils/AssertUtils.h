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
#include <cmath>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/QueryResult.h"
#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IScalarValueReader.h"
namespace {

bool
compare_float(float x, float y, float epsilon = 0.000001f) {
    if (fabs(x - y) < epsilon)
        return true;
    return false;
}

bool
compare_double(double x, double y, double epsilon = 0.000001f) {
    if (fabs(x - y) < epsilon)
        return true;
    return false;
}

inline void
assert_order(const milvus::SearchResult& result,
             const knowhere::MetricType& metric_type) {
    bool dsc = milvus::PositivelyRelated(metric_type);
    auto& ids = result.seg_offsets_;
    auto& dist = result.distances_;
    auto nq = result.total_nq_;
    auto topk = result.unity_topK_;
    if (dsc) {
        for (int i = 0; i < nq; i++) {
            for (int j = 1; j < topk; j++) {
                auto idx = i * topk + j;
                if (ids[idx] != -1) {
                    ASSERT_GE(dist[idx - 1], dist[idx]);
                }
            }
        }
    } else {
        for (int i = 0; i < nq; i++) {
            for (int j = 1; j < topk; j++) {
                auto idx = i * topk + j;
                if (ids[idx] != -1) {
                    ASSERT_LE(dist[idx - 1], dist[idx]);
                }
            }
        }
    }
}

template <typename T>
using AssertQueryValue =
    std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

template <typename T>
inline auto
assert_query_values(const std::vector<T>& arr) {
    auto values = std::make_unique<AssertQueryValue<T>[]>(arr.size());
    for (size_t i = 0; i < arr.size(); ++i) {
        values[i] = arr[i];
    }
    return values;
}

template <typename T>
inline void
assert_in(const milvus::index::IIndexReaderBase* base,
          const std::vector<T>& arr) {
    if constexpr (std::is_floating_point_v<T>) {
        return;
    }
    const auto* index = dynamic_cast<
        const milvus::index::IScalarPredicateReader<AssertQueryValue<T>>*>(
        base);
    ASSERT_NE(index, nullptr);
    auto values = assert_query_values(arr);
    auto bitset1 = index->In(arr.size(), values.get());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(bitset1.any());
    if constexpr (!std::is_same_v<T, std::string> &&
                  !std::is_same_v<T, std::string_view>) {
        const T absent = arr.back() + 1;
        auto bitset2 = index->In(1, &absent);
        ASSERT_EQ(arr.size(), bitset2.size());
        ASSERT_TRUE(bitset2.none());
    }
}

template <typename T>
inline void
assert_not_in(const milvus::index::IIndexReaderBase* base,
              const std::vector<T>& arr) {
    const auto* index = dynamic_cast<
        const milvus::index::IScalarPredicateReader<AssertQueryValue<T>>*>(
        base);
    ASSERT_NE(index, nullptr);
    auto values = assert_query_values(arr);
    auto bitset1 = index->NotIn(arr.size(), values.get());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(bitset1.none());
    if constexpr (!std::is_same_v<T, std::string> &&
                  !std::is_same_v<T, std::string_view>) {
        const T absent = arr.back() + 1;
        auto bitset2 = index->NotIn(1, &absent);
        ASSERT_EQ(arr.size(), bitset2.size());
        ASSERT_TRUE(bitset2.any());
    }
}

template <typename T>
inline void
assert_range(const milvus::index::IIndexReaderBase* base,
             const std::vector<T>& arr) {
    using Op = milvus::index::CompareOp;
    const auto* index = dynamic_cast<
        const milvus::index::IScalarPredicateReader<AssertQueryValue<T>>*>(
        base);
    ASSERT_NE(index, nullptr);
    const AssertQueryValue<T> test_min = arr.front();
    const AssertQueryValue<T> test_max = arr.back();
    if constexpr (!std::is_same_v<T, std::string> &&
                  !std::is_same_v<T, std::string_view>) {
        auto bitset1 = index->Range(test_min - 1, Op::GreaterThan);
        ASSERT_EQ(arr.size(), bitset1.size());
        ASSERT_TRUE(bitset1.any());
        auto bitset3 = index->Range(test_max + 1, Op::LessThan);
        ASSERT_EQ(arr.size(), bitset3.size());
        ASSERT_TRUE(bitset3.any());
    }
    auto bitset2 = index->Range(test_min, Op::GreaterEqual);
    ASSERT_EQ(arr.size(), bitset2.size());
    ASSERT_TRUE(bitset2.any());
    auto bitset4 = index->Range(test_max, Op::LessEqual);
    ASSERT_EQ(arr.size(), bitset4.size());
    ASSERT_TRUE(bitset4.any());
    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5.size());
    ASSERT_TRUE(bitset5.any());
}

template <typename T>
inline void
assert_reverse(const milvus::index::IIndexReaderBase* base,
               const std::vector<T>& arr) {
    const auto* index = dynamic_cast<
        const milvus::index::IScalarValueReader<AssertQueryValue<T>>*>(base);
    ASSERT_NE(index, nullptr);
    for (size_t offset = 0; offset < arr.size(); ++offset) {
        auto raw = index->Lookup(offset);
        ASSERT_TRUE(raw.has_value());
        if constexpr (std::is_same_v<T, float>) {
            ASSERT_TRUE(compare_float(raw.value(), arr[offset]));
        } else if constexpr (std::is_same_v<T, double>) {
            ASSERT_TRUE(compare_double(raw.value(), arr[offset]));
        } else {
            ASSERT_EQ(raw.value(), arr[offset]);
        }
    }
}

}  // namespace
