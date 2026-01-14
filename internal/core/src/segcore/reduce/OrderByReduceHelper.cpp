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

#include "OrderByReduceHelper.h"
#include "segcore/SegmentInterface.h"
#include "segcore/ReduceStructure.h"
#include "segcore/ReduceUtils.h"
#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::segcore {

int64_t
OrderByReduceHelper::ReduceSearchResultForOneNQ(int64_t qi,
                                                int64_t topk,
                                                int64_t& offset) {
    if (field_reader_.Empty()) {
        // Fallback to base class implementation if no order_by fields
        return ReduceHelper::ReduceSearchResultForOneNQ(qi, topk, offset);
    }

    // Create comparator with order_by_fields and metric_type
    SearchResultPairComparator comparator(
        order_by_fields_, plan_->plan_node_->search_info_.metric_type_);

    std::priority_queue<SearchResultPair*,
                        std::vector<SearchResultPair*>,
                        SearchResultPairComparator>
        heap(comparator);
    pk_set_.clear();
    pairs_.clear();
    pairs_.reserve(num_segments_);

    for (int i = 0; i < num_segments_; i++) {
        auto search_result = search_results_[i];
        auto offset_beg = search_result->topk_per_nq_prefix_sum_[qi];
        auto offset_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
        if (offset_beg == offset_end) {
            continue;
        }
        auto primary_key = search_result->primary_keys_[offset_beg];
        auto distance = search_result->distances_[offset_beg];

        pairs_.emplace_back(
            primary_key, distance, search_result, i, offset_beg, offset_end);
        auto* pair = &pairs_.back();

        // Read order_by field values
        ReadOrderByValues(pair);

        heap.push(pair);
    }

    // nq has no results for all segments
    if (heap.size() == 0) {
        return 0;
    }

    int64_t dup_cnt = 0;
    auto start = offset;
    while (offset - start < topk && !heap.empty()) {
        auto pilot = heap.top();
        heap.pop();

        auto index = pilot->segment_index_;
        auto pk = pilot->primary_key_;
        // no valid search result for this nq, break to next
        if (pk == INVALID_PK) {
            break;
        }
        // remove duplicates
        if (pk_set_.count(pk) == 0) {
            pilot->search_result_->result_offsets_.push_back(offset++);
            final_search_records_[index][qi].push_back(pilot->offset_);
            pk_set_.insert(pk);
        } else {
            // skip entity with same primary key
            dup_cnt++;
        }
        pilot->advance();
        if (pilot->primary_key_ != INVALID_PK) {
            // Read order_by values for the next result
            ReadOrderByValues(pilot);
            heap.push(pilot);
        }
    }
    return dup_cnt;
}

void
OrderByReduceHelper::ReadOrderByValues(SearchResultPair* pair) {
    if (field_reader_.Empty() || pair->offset_ >= pair->offset_rb_) {
        pair->order_by_values_ = std::nullopt;
        return;
    }

    auto search_result = pair->search_result_;
    auto segment_interface =
        static_cast<const SegmentInterface*>(search_result->segment_);
    auto segment =
        dynamic_cast<const SegmentInternalInterface*>(segment_interface);
    AssertInfo(segment != nullptr,
               "Failed to cast SegmentInterface to SegmentInternalInterface");
    auto seg_offset = search_result->seg_offsets_[pair->offset_];

    pair->order_by_values_ = field_reader_.Read(*segment, seg_offset);
}

void
OrderByReduceHelper::FillOtherData(
    int result_count,
    int64_t nq_begin,
    int64_t nq_end,
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_res_data) {
    if (field_reader_.Empty()) {
        // No order_by fields, call base class implementation
        ReduceHelper::FillOtherData(
            result_count, nq_begin, nq_end, search_res_data);
        return;
    }

    // Collect order_by values for all results
    std::vector<std::vector<OrderByValueType>> order_by_vals_list;
    order_by_vals_list.resize(result_count);

    for (auto qi = nq_begin; qi < nq_end; qi++) {
        for (auto search_result : search_results_) {
            AssertInfo(search_result != nullptr,
                       "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto topk_start = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];

            auto segment_interface =
                static_cast<const SegmentInterface*>(search_result->segment_);
            auto segment = dynamic_cast<const SegmentInternalInterface*>(
                segment_interface);
            AssertInfo(
                segment != nullptr,
                "Failed to cast SegmentInterface to SegmentInternalInterface");

            for (auto ki = topk_start; ki < topk_end; ki++) {
                auto loc = search_result->result_offsets_[ki];
                auto seg_offset = search_result->seg_offsets_[ki];

                order_by_vals_list[loc] =
                    field_reader_.Read(*segment, seg_offset);
            }
        }
    }

    // Assemble order_by values into SearchResultData
    AssembleOrderByValues(search_res_data, order_by_vals_list, plan_);
}

}  // namespace milvus::segcore
