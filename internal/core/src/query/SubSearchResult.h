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

#include <limits>
#include <utility>
#include <vector>
#include "common/Types.h"

namespace milvus::query {

class SubSearchResult {
 public:
    SubSearchResult(int64_t num_queries, int64_t topk, const knowhere::MetricType& metric_type, int64_t round_decimal)
        : num_queries_(num_queries),
          topk_(topk),
          round_decimal_(round_decimal),
          metric_type_(metric_type),
          seg_offsets_(num_queries * topk, -1),
          distances_(num_queries * topk, init_value(metric_type)) {
    }

    SubSearchResult(SubSearchResult&& other)
        : num_queries_(other.num_queries_),
          topk_(other.topk_),
          round_decimal_(other.round_decimal_),
          metric_type_(other.metric_type_),
          seg_offsets_(std::move(other.seg_offsets_)),
          distances_(std::move(other.distances_)) {
    }

 public:
    static float
    init_value(const knowhere::MetricType& metric_type) {
        return (is_descending(metric_type) ? -1 : 1) * std::numeric_limits<float>::max();
    }

    static bool
    is_descending(const knowhere::MetricType& metric_type) {
        // TODO(dog): more types
        if (metric_type == knowhere::metric::IP) {
            return true;
        } else {
            return false;
        }
    }

 public:
    int64_t
    get_num_queries() const {
        return num_queries_;
    }

    int64_t
    get_topk() const {
        return topk_;
    }

    const int64_t*
    get_ids() const {
        return seg_offsets_.data();
    }

    int64_t*
    get_seg_offsets() {
        return seg_offsets_.data();
    }

    const float*
    get_distances() const {
        return distances_.data();
    }

    float*
    get_distances() {
        return distances_.data();
    }

    auto&
    mutable_seg_offsets() {
        return seg_offsets_;
    }

    auto&
    mutable_distances() {
        return distances_;
    }

    void
    round_values();

    void
    merge(const SubSearchResult& sub_result);

 private:
    template <bool is_desc>
    void
    merge_impl(const SubSearchResult& sub_result);

 private:
    int64_t num_queries_;
    int64_t topk_;
    int64_t round_decimal_;
    knowhere::MetricType metric_type_;
    std::vector<int64_t> seg_offsets_;
    std::vector<float> distances_;
};

}  // namespace milvus::query
