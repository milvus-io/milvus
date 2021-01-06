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
#include "common/Types.h"
#include <limits>
#include <vector>
namespace milvus::query {

class SubQueryResult {
 public:
    SubQueryResult(int64_t num_queries, int64_t topk, MetricType metric_type)
        : metric_type_(metric_type),
          num_queries_(num_queries),
          topk_(topk),
          labels_(num_queries * topk, -1),
          values_(num_queries * topk, init_value(metric_type)) {
    }

 public:
    static constexpr float
    init_value(MetricType metric_type) {
        return (is_descending(metric_type) ? -1 : 1) * std::numeric_limits<float>::max();
    }

    static constexpr bool
    is_descending(MetricType metric_type) {
        // TODO
        if (metric_type == MetricType::METRIC_INNER_PRODUCT) {
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
    get_labels() const {
        return labels_.data();
    }
    int64_t*
    get_labels() {
        return labels_.data();
    }
    const float*
    get_values() const {
        return values_.data();
    }
    float*
    get_values() {
        return values_.data();
    }
    auto&
    mutable_labels() {
        return labels_;
    }
    auto&
    mutable_values() {
        return values_;
    }

    static SubQueryResult
    merge(const SubQueryResult& left, const SubQueryResult& right);

    void
    merge(const SubQueryResult& sub_result);

 private:
    template <bool is_desc>
    void
    merge_impl(const SubQueryResult& sub_result);

 private:
    int64_t num_queries_;
    int64_t topk_;
    MetricType metric_type_;
    std::vector<int64_t> labels_;
    std::vector<float> values_;
};

}  // namespace milvus::query
