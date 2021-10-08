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

#include "exceptions/EasyAssert.h"
#include "query/SubSearchResult.h"
#include "segcore/Reduce.h"
#include <cmath>

namespace milvus::query {

template <bool is_desc>
void
SubSearchResult::merge_impl(const SubSearchResult& right) {
    AssertInfo(num_queries_ == right.num_queries_, "[SubSearchResult]Nq check failed");
    AssertInfo(topk_ == right.topk_, "[SubSearchResult]Topk check failed");
    AssertInfo(metric_type_ == right.metric_type_, "[SubSearchResult]Metric type check failed");
    AssertInfo(is_desc == is_descending(metric_type_), "[SubSearchResult]Metric type isn't desc");

    for (int64_t qn = 0; qn < num_queries_; ++qn) {
        auto offset = qn * topk_;

        int64_t* __restrict__ left_labels = this->get_labels() + offset;
        float* __restrict__ left_values = this->get_values() + offset;

        auto right_labels = right.get_labels() + offset;
        auto right_values = right.get_values() + offset;

        std::vector<float> buf_values(topk_);
        std::vector<int64_t> buf_labels(topk_);

        auto lit = 0;  // left iter
        auto rit = 0;  // right iter

        for (auto buf_iter = 0; buf_iter < topk_; ++buf_iter) {
            auto left_v = left_values[lit];
            auto right_v = right_values[rit];
            // optimize out at compiling
            if (is_desc ? (left_v >= right_v) : (left_v <= right_v)) {
                buf_values[buf_iter] = left_values[lit];
                buf_labels[buf_iter] = left_labels[lit];
                ++lit;
            } else {
                buf_values[buf_iter] = right_values[rit];
                buf_labels[buf_iter] = right_labels[rit];
                ++rit;
            }
        }
        std::copy_n(buf_values.data(), topk_, left_values);
        std::copy_n(buf_labels.data(), topk_, left_labels);
    }
}

void
SubSearchResult::merge(const SubSearchResult& sub_result) {
    AssertInfo(metric_type_ == sub_result.metric_type_, "[SubSearchResult]Metric type check failed when merge");
    if (is_descending(metric_type_)) {
        this->merge_impl<true>(sub_result);
    } else {
        this->merge_impl<false>(sub_result);
    }
}

SubSearchResult
SubSearchResult::merge(const SubSearchResult& left, const SubSearchResult& right) {
    auto left_copy = left;
    left_copy.merge(right);
    return left_copy;
}

void
SubSearchResult::round_values() {
    if (round_decimal_ == -1)
        return;
    const float multiplier = pow(10.0, round_decimal_);
    for (auto it = this->values_.begin(); it != this->values_.end(); it++) {
        *it = round(*it * multiplier) / multiplier;
    }
}

}  // namespace milvus::query
