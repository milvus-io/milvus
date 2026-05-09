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

#include <cstdint>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/QueryResult.h"
#include "log/Log.h"
#include "segcore/ReduceUtils.h"

namespace milvus::segcore {
namespace {

// Returns true if this hit is kept (map + pk_set updated). False if filtered.
// Uses try_emplace + rollback; mirrors CompositeGroupByMap::Push.
// Contract: group_size >= 1 (enforced by PlanProto normalization).
bool
TryAcceptCompositeGroup(
    const SearchResultPair& result,
    const CompositeGroupKey& composite_key,
    std::unordered_set<PkType>& pk_set,
    std::unordered_set<ElementSearchResultKey, ElementSearchResultKeyHash>&
        element_result_set,
    std::unordered_map<CompositeGroupKey, int64_t, CompositeGroupKeyHash>&
        composite_group_by_map,
    int64_t topk,
    int64_t group_size) {
    AssertInfo(group_size >= 1,
               "group_size must be >= 1 (PlanProto normalizes), got {}",
               group_size);

    auto search_result = result.search_result_;
    ElementSearchResultKey element_key{result.primary_key_, -1};
    if (search_result->element_level_) {
        AssertInfo(
            result.offset_ >= 0 && static_cast<size_t>(result.offset_) <
                                       search_result->element_indices_.size(),
            "invalid element-level search result offset {}, "
            "element_indices size {}",
            result.offset_,
            search_result->element_indices_.size());
        element_key.element_index =
            search_result->element_indices_[result.offset_];
        if (element_result_set.count(element_key) != 0) {
            return false;
        }
    } else {
        if (pk_set.count(result.primary_key_) != 0) {
            return false;
        }
    }

    auto [it, inserted] = composite_group_by_map.try_emplace(composite_key, 0);
    if (inserted &&
        static_cast<int64_t>(composite_group_by_map.size()) > topk) {
        composite_group_by_map.erase(it);
        return false;
    }
    // Contract group_size >= 1 + freshly inserted count=0 ⇒ this branch
    // can only fire on existing keys (inserted=false), no rollback needed.
    if (it->second >= group_size) {
        return false;
    }
    it->second += 1;
    if (search_result->element_level_) {
        element_result_set.insert(std::move(element_key));
    } else {
        pk_set.insert(result.primary_key_);
    }
    return true;
}

}  // namespace

void
GroupReduceHelper::FillOtherData(
    int result_count,
    int64_t nq_begin,
    int64_t nq_end,
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_res_data) {
    std::vector<CompositeGroupKey> composite_group_by_values;
    composite_group_by_values.resize(result_count);
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
                composite_group_by_values[loc] = std::move(
                    search_result->composite_group_by_values_.value()[ki]);
            }
        }
    }
    AssembleCompositeGroupByValues(
        search_res_data, composite_group_by_values, plan_);
}

void
GroupReduceHelper::RefreshSingleSearchResult(SearchResult* search_result,
                                             int seg_res_idx,
                                             std::vector<int64_t>& real_topks) {
    AssertInfo(
        search_result->composite_group_by_values_.has_value(),
        "no composite_group_by_values for search result, group reducer should "
        "not be called, wrong code");
    AssertInfo(
        search_result->primary_keys_.size() ==
            search_result->composite_group_by_values_.value().size(),
        "Wrong size for composite_group_by_values size before refresh:{}, "
        "not equal to primary_keys_.size:{}",
        search_result->composite_group_by_values_.value().size(),
        search_result->primary_keys_.size());

    uint32_t size = 0;
    for (int j = 0; j < total_nq_; j++) {
        size += final_search_records_[seg_res_idx][j].size();
    }
    std::vector<milvus::PkType> primary_keys(size);
    std::vector<float> distances(size);
    std::vector<int64_t> seg_offsets(size);
    std::vector<CompositeGroupKey> composite_group_by_values(size);
    std::vector<int32_t> element_indices;

    if (search_result->element_level_) {
        element_indices.resize(size);
    }

    uint32_t index = 0;
    for (int j = 0; j < total_nq_; j++) {
        for (auto offset : final_search_records_[seg_res_idx][j]) {
            primary_keys[index] =
                std::move(search_result->primary_keys_[offset]);
            distances[index] = search_result->distances_[offset];
            seg_offsets[index] = search_result->seg_offsets_[offset];
            composite_group_by_values[index] = std::move(
                search_result->composite_group_by_values_.value()[offset]);
            if (search_result->element_level_) {
                element_indices[index] =
                    search_result->element_indices_[offset];
            }
            index++;
            real_topks[j]++;
        }
    }
    search_result->primary_keys_.swap(primary_keys);
    search_result->distances_.swap(distances);
    search_result->seg_offsets_.swap(seg_offsets);
    search_result->composite_group_by_values_.value().swap(
        composite_group_by_values);
    if (search_result->element_level_) {
        search_result->element_indices_.swap(element_indices);
    }

    AssertInfo(
        search_result->primary_keys_.size() ==
            search_result->composite_group_by_values_.value().size(),
        "Wrong size for composite_group_by_values size after refresh:{}, "
        "not equal to primary_keys_.size:{}",
        search_result->composite_group_by_values_.value().size(),
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
    element_result_set_.clear();
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

        AssertInfo(
            search_result->composite_group_by_values_.has_value(),
            "Wrong state, search_result has no composite_group_by_values "
            "for group_by_reduce, must be sth wrong!");
        AssertInfo(search_result->composite_group_by_values_.value().size() ==
                       search_result->primary_keys_.size(),
                   "Wrong state, search_result's composite_group_by_values's "
                   "length is not equal to pks' size!");

        pairs_.emplace_back(
            primary_key, distance, search_result, i, offset_beg, offset_end);
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

    // Use CompositeGroupKey for grouping
    std::unordered_map<CompositeGroupKey, int64_t, CompositeGroupKeyHash>
        composite_group_by_map;
    if (topk > 0) {
        composite_group_by_map.reserve(static_cast<size_t>(topk));
    }

    while (offset - start < group_by_total_size && !heap.empty()) {
        auto pilot = heap.top();
        heap.pop();
        auto index = pilot->segment_index_;
        auto pk = pilot->primary_key_;
        AssertInfo(pk != INVALID_PK,
                   "Wrong, search results should have been filtered and "
                   "invalid_pk should not be existed");

        // Get the composite key for this result
        const auto& composite_key =
            pilot->search_result_->composite_group_by_values_
                .value()[pilot->offset_];

        if (TryAcceptCompositeGroup(*pilot,
                                    composite_key,
                                    pk_set_,
                                    element_result_set_,
                                    composite_group_by_map,
                                    topk,
                                    group_size)) {
            pilot->search_result_->result_offsets_.push_back(offset++);
            final_search_records_[index][qi].push_back(pilot->offset_);
        } else {
            filtered_count++;
        }

        pilot->advance();
        if (pilot->primary_key_ != INVALID_PK) {
            heap.push(pilot);
        }
    }
    AssertInfo(static_cast<int64_t>(composite_group_by_map.size()) <= topk,
               "composite_group_by_map size {} exceeds topk {}, qi={}, "
               "offset={}, filtered_count={}",
               composite_group_by_map.size(),
               topk,
               qi,
               offset,
               filtered_count);
    LOG_TRACE(
        "GroupReduceHelper::ReduceSearchResultForOneNQ qi={}, topk={}, "
        "offset={}, filtered_count={}, composite_group_by_map_size={}",
        qi,
        topk,
        offset,
        filtered_count,
        composite_group_by_map.size());
    return filtered_count;
}

}  // namespace milvus::segcore
