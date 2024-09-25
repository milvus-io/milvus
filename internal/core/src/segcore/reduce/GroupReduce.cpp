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

#include "GroupReduce.h"
#include "log/Log.h"
#include "segcore/SegmentInterface.h"
#include "segcore/ReduceUtils.h"

namespace milvus::segcore {

void
GroupReduceHelper::FillOtherData(
    int result_count,
    int64_t nq_begin,
    int64_t nq_end,
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_res_data) {
    std::vector<GroupByValueType> group_by_values;
    group_by_values.resize(result_count);
    for (auto qi = nq_begin; qi < nq_end; qi++) {
        for (auto search_result : search_results_) {
            AssertInfo(search_result != nullptr,
                       "null search result when reorganize");
            if (search_result->result_offsets_.size() == 0) {
                continue;
            }

            auto topk_start = search_result->topk_per_nq_prefix_sum_[qi];
            auto topk_end = search_result->topk_per_nq_prefix_sum_[qi + 1];
            for (auto ki = topk_start; ki < topk_end; ki++) {
                auto loc = search_result->result_offsets_[ki];
                group_by_values[loc] =
                    search_result->group_by_values_.value()[ki];
            }
        }
    }
    AssembleGroupByValues(search_res_data, group_by_values, plan_);
}

void
GroupReduceHelper::RefreshSingleSearchResult(SearchResult* search_result,
                                             int seg_res_idx,
                                             std::vector<int64_t>& real_topks) {
    AssertInfo(search_result->group_by_values_.has_value(),
               "no group by values for search result, group reducer should not "
               "be called, wrong code");
    AssertInfo(search_result->primary_keys_.size() ==
                   search_result->group_by_values_.value().size(),
               "Wrong size for group_by_values size before refresh:{}, "
               "not equal to "
               "primary_keys_.size:{}",
               search_result->group_by_values_.value().size(),
               search_result->primary_keys_.size());

    uint32_t size = 0;
    for (int j = 0; j < total_nq_; j++) {
        size += final_search_records_[seg_res_idx][j].size();
    }
    std::vector<milvus::PkType> primary_keys(size);
    std::vector<float> distances(size);
    std::vector<int64_t> seg_offsets(size);
    std::vector<GroupByValueType> group_by_values(size);

    uint32_t index = 0;
    for (int j = 0; j < total_nq_; j++) {
        for (auto offset : final_search_records_[seg_res_idx][j]) {
            primary_keys[index] = search_result->primary_keys_[offset];
            distances[index] = search_result->distances_[offset];
            seg_offsets[index] = search_result->seg_offsets_[offset];
            group_by_values[index] =
                search_result->group_by_values_.value()[offset];
            index++;
            real_topks[j]++;
        }
    }
    search_result->primary_keys_.swap(primary_keys);
    search_result->distances_.swap(distances);
    search_result->seg_offsets_.swap(seg_offsets);
    search_result->group_by_values_.value().swap(group_by_values);
    AssertInfo(search_result->primary_keys_.size() ==
                   search_result->group_by_values_.value().size(),
               "Wrong size for group_by_values size after refresh:{}, "
               "not equal to "
               "primary_keys_.size:{}",
               search_result->group_by_values_.value().size(),
               search_result->primary_keys_.size());
}

void
GroupReduceHelper::FilterInvalidSearchResult(SearchResult* search_result) {
    //do nothing, for group-by reduce, as we calculate prefix_sum for nq when doing group by and no padding invalid results
    //so there's no need to filter search_result
}

int64_t
GroupReduceHelper::ReduceSearchResultForOneNQ(int64_t qi,
                                              int64_t topk,
                                              int64_t& offset) {
    std::priority_queue<SearchResultPair*,
                        std::vector<SearchResultPair*>,
                        SearchResultPairComparator>
        heap;
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
        AssertInfo(search_result->group_by_values_.has_value(),
                   "Wrong state, search_result has no group_by_vales for "
                   "group_by_reduce, must be sth wrong!");
        AssertInfo(search_result->group_by_values_.value().size() ==
                       search_result->primary_keys_.size(),
                   "Wrong state, search_result's group_by_values's length is "
                   "not equal to pks' size!");
        auto group_by_val = search_result->group_by_values_.value()[offset_beg];
        pairs_.emplace_back(primary_key,
                            distance,
                            search_result,
                            i,
                            offset_beg,
                            offset_end,
                            std::move(group_by_val));
        heap.push(&pairs_.back());
    }

    // nq has no results for all segments
    if (heap.size() == 0) {
        return 0;
    }

    int64_t group_size = search_results_[0]->group_size_.value();
    int64_t group_by_total_size = group_size * topk;
    int64_t filtered_count = 0;
    auto start = offset;
    std::unordered_map<GroupByValueType, int64_t> group_by_map;

    auto should_filtered = [&](const PkType& pk,
                               const GroupByValueType& group_by_val) {
        if (pk_set_.count(pk) != 0)
            return true;
        if (group_by_map.size() >= topk &&
            group_by_map.count(group_by_val) == 0)
            return true;
        if (group_by_map[group_by_val] >= group_size)
            return true;
        return false;
    };

    while (offset - start < group_by_total_size && !heap.empty()) {
        //fetch value
        auto pilot = heap.top();
        heap.pop();
        auto index = pilot->segment_index_;
        auto pk = pilot->primary_key_;
        AssertInfo(pk != INVALID_PK,
                   "Wrong, search results should have been filtered and "
                   "invalid_pk should not be existed");
        auto group_by_val = pilot->group_by_value_.value();

        //judge filter
        if (!should_filtered(pk, group_by_val)) {
            pilot->search_result_->result_offsets_.push_back(offset++);
            final_search_records_[index][qi].push_back(pilot->offset_);
            pk_set_.insert(pk);
            group_by_map[group_by_val] += 1;
        } else {
            filtered_count++;
        }

        //move pilot forward
        pilot->advance();
        if (pilot->primary_key_ != INVALID_PK) {
            heap.push(pilot);
        }
    }
    return filtered_count;
}

}  // namespace milvus::segcore
