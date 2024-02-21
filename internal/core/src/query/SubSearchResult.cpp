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

#include <cmath>

#include "common/EasyAssert.h"
#include "query/SubSearchResult.h"

namespace milvus::query {

template <bool is_desc>
void
SubSearchResult::merge_impl(const SubSearchResult& right) {
    AssertInfo(num_queries_ == right.num_queries_,
               "[SubSearchResult]Nq check failed");
    AssertInfo(topk_ == right.topk_, "[SubSearchResult]Topk check failed");
    AssertInfo(metric_type_ == right.metric_type_,
               "[SubSearchResult]Metric type check failed");
    AssertInfo(is_desc == PositivelyRelated(metric_type_),
               "[SubSearchResult]Metric type isn't desc");

    for (int64_t qn = 0; qn < num_queries_; ++qn) {
        auto offset = qn * topk_;

        int64_t* __restrict__ left_ids = this->get_seg_offsets() + offset;
        float* __restrict__ left_distances = this->get_distances() + offset;

        auto right_ids = right.get_ids() + offset;
        auto right_distances = right.get_distances() + offset;

        std::vector<float> buf_distances(topk_);
        std::vector<int64_t> buf_ids(topk_);

        auto lit = 0;  // left iter
        auto rit = 0;  // right iter

        for (auto buf_iter = 0; buf_iter < topk_; ++buf_iter) {
            auto left_id = left_ids[lit];
            auto left_v = left_distances[lit];
            auto right_id = right_ids[rit];
            auto right_v = right_distances[rit];
            // optimize out at compiling
            if (left_id == INVALID_SEG_OFFSET) {
                buf_distances[buf_iter] = right_distances[rit];
                buf_ids[buf_iter] = right_ids[rit];
                ++rit;
            } else if (right_id == INVALID_SEG_OFFSET) {
                buf_distances[buf_iter] = left_distances[lit];
                buf_ids[buf_iter] = left_ids[lit];
                ++lit;
            } else {
                if (is_desc ? (left_v >= right_v) : (left_v <= right_v)) {
                    buf_distances[buf_iter] = left_distances[lit];
                    buf_ids[buf_iter] = left_ids[lit];
                    ++lit;
                } else {
                    buf_distances[buf_iter] = right_distances[rit];
                    buf_ids[buf_iter] = right_ids[rit];
                    ++rit;
                }
            }
        }
        std::copy_n(buf_distances.data(), topk_, left_distances);
        std::copy_n(buf_ids.data(), topk_, left_ids);
    }
}

void
SubSearchResult::merge(const SubSearchResult& other) {
    AssertInfo(metric_type_ == other.metric_type_,
               "[SubSearchResult]Metric type check failed when merge");
    if (!other.chunk_iterators_.empty()) {
        std::move(std::begin(other.chunk_iterators_),
                  std::end(other.chunk_iterators_),
                  std::back_inserter(this->chunk_iterators_));
    } else {
        if (PositivelyRelated(metric_type_)) {
            this->merge_impl<true>(other);
        } else {
            this->merge_impl<false>(other);
        }
    }
}

void
SubSearchResult::round_values() {
    if (round_decimal_ == -1)
        return;
    const float multiplier = pow(10.0, round_decimal_);
    for (float& distance : this->distances_) {
        distance = std::round(distance * multiplier) / multiplier;
    }
}

}  // namespace milvus::query
