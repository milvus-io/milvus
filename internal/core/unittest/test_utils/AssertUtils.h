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

#include "common/QueryResult.h"
#include "common/Types.h"
#include "index/ScalarIndex.h"

using milvus::index::ScalarIndex;
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

bool
Any(const milvus::TargetBitmap& bitmap) {
    return bitmap.any();
}

bool
BitSetNone(const milvus::TargetBitmap& bitmap) {
    return bitmap.none();
}

uint64_t
Count(const milvus::TargetBitmap& bitmap) {
    return bitmap.count();
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
inline void
assert_in(ScalarIndex<T>* index, const std::vector<T>& arr) {
    // hard to compare floating point value.
    if (std::is_floating_point_v<T>) {
        return;
    }

    auto bitset1 = index->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(Any(bitset1));
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = index->In(1, test.get());
    ASSERT_EQ(arr.size(), bitset2.size());
    ASSERT_TRUE(BitSetNone(bitset2));
}

template <typename T>
inline void
assert_not_in(ScalarIndex<T>* index, const std::vector<T>& arr) {
    auto bitset1 = index->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(BitSetNone(bitset1));
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = index->NotIn(1, test.get());
    ASSERT_EQ(arr.size(), bitset2.size());
    ASSERT_TRUE(Any(bitset2));
}

template <typename T>
inline void
assert_range(ScalarIndex<T>* index, const std::vector<T>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset1 = index->Range(test_min - 1, milvus::OpType::GreaterThan);
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(Any(bitset1));

    auto bitset2 = index->Range(test_min, milvus::OpType::GreaterEqual);
    ASSERT_EQ(arr.size(), bitset2.size());
    ASSERT_TRUE(Any(bitset2));

    auto bitset3 = index->Range(test_max + 1, milvus::OpType::LessThan);
    ASSERT_EQ(arr.size(), bitset3.size());
    ASSERT_TRUE(Any(bitset3));

    auto bitset4 = index->Range(test_max, milvus::OpType::LessEqual);
    ASSERT_EQ(arr.size(), bitset4.size());
    ASSERT_TRUE(Any(bitset4));

    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5.size());
    ASSERT_TRUE(Any(bitset5));
}

template <typename T>
inline void
assert_reverse(ScalarIndex<T>* index, const std::vector<T>& arr) {
    for (size_t offset = 0; offset < arr.size(); ++offset) {
        auto raw = index->Reverse_Lookup(offset);
        ASSERT_TRUE(raw.has_value());
        ASSERT_EQ(raw.value(), arr[offset]);
    }
}

template <>
inline void
assert_reverse(ScalarIndex<float>* index, const std::vector<float>& arr) {
    for (size_t offset = 0; offset < arr.size(); ++offset) {
        auto raw = index->Reverse_Lookup(offset);
        ASSERT_TRUE(raw.has_value());
        ASSERT_TRUE(compare_float(raw.value(), arr[offset]));
    }
}

template <>
inline void
assert_reverse(ScalarIndex<double>* index, const std::vector<double>& arr) {
    for (size_t offset = 0; offset < arr.size(); ++offset) {
        auto raw = index->Reverse_Lookup(offset);
        ASSERT_TRUE(raw.has_value());
        ASSERT_TRUE(compare_double(raw.value(), arr[offset]));
    }
}

template <>
inline void
assert_reverse(ScalarIndex<std::string>* index,
               const std::vector<std::string>& arr) {
    for (size_t offset = 0; offset < arr.size(); ++offset) {
        auto raw = index->Reverse_Lookup(offset);
        ASSERT_TRUE(raw.has_value());
        ASSERT_TRUE(arr[offset].compare(raw.value()) == 0);
    }
}

template <>
inline void
assert_in(ScalarIndex<std::string>* index,
          const std::vector<std::string>& arr) {
    auto bitset1 = index->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(Any(bitset1));
}

template <>
inline void
assert_not_in(ScalarIndex<std::string>* index,
              const std::vector<std::string>& arr) {
    auto bitset1 = index->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1.size());
    ASSERT_TRUE(BitSetNone(bitset1));
}

template <>
inline void
assert_range(ScalarIndex<std::string>* index,
             const std::vector<std::string>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset2 = index->Range(test_min, milvus::OpType::GreaterEqual);
    ASSERT_EQ(arr.size(), bitset2.size());
    ASSERT_TRUE(Any(bitset2));

    auto bitset4 = index->Range(test_max, milvus::OpType::LessEqual);
    ASSERT_EQ(arr.size(), bitset4.size());
    ASSERT_TRUE(Any(bitset4));

    auto bitset5 = index->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5.size());
    ASSERT_TRUE(Any(bitset5));
}
}  // namespace
