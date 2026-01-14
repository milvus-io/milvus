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

#include "GroupOrderByReduceHelper.h"
#include "segcore/SegmentInterface.h"
#include "segcore/ReduceStructure.h"
#include "segcore/ReduceUtils.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include <algorithm>
#include <map>
#include <functional>

namespace milvus::segcore {

int64_t
GroupOrderByReduceHelper::ReduceSearchResultForOneNQ(int64_t qi,
                                                     int64_t topk,
                                                     int64_t& offset) {
    if (field_reader_.Empty()) {
        // Fallback to base class implementation if no order_by fields
        return GroupReduceHelper::ReduceSearchResultForOneNQ(qi, topk, offset);
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
        AssertInfo(search_result->group_by_values_.has_value(),
                   "Wrong state, search_result has no group_by_values for "
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
    struct SelectedItem {
        int segment_index;
        int64_t offset;
        GroupByValueType group_by_val;
        SearchResult* search_result;
    };
    std::vector<SelectedItem> selected_items;
    selected_items.reserve(group_by_total_size);

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

    while (static_cast<int64_t>(selected_items.size()) < group_by_total_size &&
           !heap.empty()) {
        // fetch value
        auto pilot = heap.top();
        heap.pop();
        auto index = pilot->segment_index_;
        auto pk = pilot->primary_key_;
        AssertInfo(pk != INVALID_PK,
                   "Wrong, search results should have been filtered and "
                   "invalid_pk should not be existed");
        auto group_by_val = pilot->group_by_value_.value();

        // judge filter
        if (!should_filtered(pk, group_by_val)) {
            selected_items.push_back(SelectedItem{
                index, pilot->offset_, group_by_val, pilot->search_result_});
            pk_set_.insert(pk);
            group_by_map[group_by_val] += 1;
        } else {
            filtered_count++;
        }

        // move pilot forward
        pilot->advance();
        if (pilot->primary_key_ != INVALID_PK) {
            heap.push(pilot);
        }
    }
    offset = start + static_cast<int64_t>(selected_items.size());

    if (selected_items.empty()) {
        return filtered_count;
    }

    std::unordered_map<GroupByValueType, std::vector<size_t>> group_item_map;
    std::vector<GroupByValueType> group_list;
    group_list.reserve(group_by_map.size());
    for (size_t i = 0; i < selected_items.size(); ++i) {
        const auto& item = selected_items[i];
        if (group_item_map.find(item.group_by_val) == group_item_map.end()) {
            group_list.push_back(item.group_by_val);
        }
        group_item_map[item.group_by_val].push_back(i);
    }

    struct GroupInfo {
        GroupByValueType group_val;
        std::vector<OrderByValueType> order_by_vals;
    };
    std::vector<GroupInfo> groups;
    groups.reserve(group_list.size());

    for (const auto& group_val : group_list) {
        const auto& indices = group_item_map[group_val];
        const auto& first_item = selected_items[indices[0]];
        SearchResultPair temp_pair(
            first_item.search_result->primary_keys_[first_item.offset],
            first_item.search_result->distances_[first_item.offset],
            first_item.search_result,
            first_item.segment_index,
            first_item.offset,
            first_item.offset + 1,
            first_item.group_by_val);
        ReadOrderByValues(&temp_pair);
        std::vector<OrderByValueType> order_by_vals;
        if (temp_pair.order_by_values_.has_value()) {
            order_by_vals = temp_pair.order_by_values_.value();
        }
        if (order_by_vals.size() < order_by_fields_.size()) {
            order_by_vals.resize(order_by_fields_.size(), std::nullopt);
        }
        groups.push_back(GroupInfo{group_val, std::move(order_by_vals)});
    }

    auto group_comparator = [&](const GroupInfo& lhs,
                                const GroupInfo& rhs) -> bool {
        for (size_t field_idx = 0; field_idx < order_by_fields_.size();
             ++field_idx) {
            const auto& field = order_by_fields_[field_idx];
            const auto& lhs_val = lhs.order_by_vals[field_idx];
            const auto& rhs_val = rhs.order_by_vals[field_idx];

            if (!lhs_val.has_value() && !rhs_val.has_value()) {
                continue;
            }
            if (!lhs_val.has_value()) {
                return field.ascending_;
            }
            if (!rhs_val.has_value()) {
                return !field.ascending_;
            }
            int cmp = CompareOrderByValue(lhs_val, rhs_val);
            if (cmp < 0) {
                return field.ascending_;
            }
            if (cmp > 0) {
                return !field.ascending_;
            }
        }
        return false;
    };
    std::stable_sort(groups.begin(), groups.end(), group_comparator);

    // Cache the order_by values of the first item in each group to avoid re-reading in FillOtherData
    // This optimization eliminates duplicate segment I/O and scanning
    for (const auto& group : groups) {
        const auto& indices = group_item_map[group.group_val];
        if (!indices.empty()) {
            const auto& first_item = selected_items[indices[0]];
            auto* search_result = first_item.search_result;

            // Initialize the cache map if not present
            if (!search_result->group_first_item_order_by_values_.has_value()) {
                search_result->group_first_item_order_by_values_ =
                    std::unordered_map<GroupByValueType,
                                       std::vector<OrderByValueType>>();
            }

            // Store the first item's order_by values for this group
            search_result->group_first_item_order_by_values_
                .value()[group.group_val] = group.order_by_vals;
        }
    }

    int64_t loc = start;
    for (const auto& group : groups) {
        const auto& indices = group_item_map[group.group_val];
        for (auto idx : indices) {
            const auto& item = selected_items[idx];
            item.search_result->result_offsets_.push_back(loc++);
            final_search_records_[item.segment_index][qi].push_back(
                item.offset);
        }
    }
    return filtered_count;
}

void
GroupOrderByReduceHelper::ReadOrderByValues(SearchResultPair* pair) {
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

std::vector<milvus::OrderByValueType>
GroupOrderByReduceHelper::GetFirstItemOrderByValues(
    const GroupByValueType& group_by_val,
    SearchResult* search_result,
    int64_t qi,
    int64_t query_start,
    int64_t query_end) {
    // Find the first item in this group within the current query's results
    // Since results are already grouped by group_by_value in Pipeline,
    // we need to find the first occurrence of this group_by_value
    // within the current query's range
    // Note: qi, query_start, query_end are passed in from caller to avoid O(nq) search

    // Find the first item with this group_by_value in the current query's results
    int64_t first_item_offset = query_start;
    for (int64_t i = query_start;
         i < query_end && i < search_result->group_by_values_.value().size();
         ++i) {
        if (search_result->group_by_values_.value()[i] == group_by_val) {
            first_item_offset = i;
            break;
        }
    }

    // Create a temporary SearchResultPair to read order_by values
    SearchResultPair temp_pair(search_result->primary_keys_[first_item_offset],
                               search_result->distances_[first_item_offset],
                               search_result,
                               0,  // segment_index not used here
                               first_item_offset,
                               first_item_offset + 1,
                               group_by_val);

    ReadOrderByValues(&temp_pair);
    if (temp_pair.order_by_values_.has_value()) {
        return temp_pair.order_by_values_.value();
    }
    return {};
}

void
GroupOrderByReduceHelper::FillOtherData(
    int result_count,
    int64_t nq_begin,
    int64_t nq_end,
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_res_data) {
    // First call base class to fill group_by values
    GroupReduceHelper::FillOtherData(
        result_count, nq_begin, nq_end, search_res_data);

    if (field_reader_.Empty()) {
        return;
    }

    // Collect order_by values for all results (using first item's order_by values for each group)
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

            // Track which groups we've already processed (to get first item's order_by values)
            std::unordered_map<GroupByValueType, std::vector<OrderByValueType>>
                group_order_by_map;

            for (auto ki = topk_start; ki < topk_end; ki++) {
                auto loc = search_result->result_offsets_[ki];
                auto group_by_val = search_result->group_by_values_.value()[ki];

                // Get the first item's order_by values for this group
                // Use the cached values from ReduceSearchResultForOneNQ to avoid re-reading
                std::vector<OrderByValueType> order_by_vals;
                if (group_order_by_map.find(group_by_val) !=
                    group_order_by_map.end()) {
                    order_by_vals = group_order_by_map[group_by_val];
                } else {
                    // Try to use cached values first (optimization: avoid duplicate segment I/O)
                    if (search_result->group_first_item_order_by_values_
                            .has_value()) {
                        auto& cache =
                            search_result->group_first_item_order_by_values_
                                .value();
                        auto cache_it = cache.find(group_by_val);
                        if (cache_it != cache.end()) {
                            order_by_vals = cache_it->second;
                        } else {
                            // Fallback: group not in cache (shouldn't happen but defensive)
                            order_by_vals =
                                GetFirstItemOrderByValues(group_by_val,
                                                          search_result,
                                                          qi,
                                                          topk_start,
                                                          topk_end);
                        }
                    } else {
                        // Fallback: cache not present (shouldn't happen but defensive)
                        order_by_vals = GetFirstItemOrderByValues(group_by_val,
                                                                  search_result,
                                                                  qi,
                                                                  topk_start,
                                                                  topk_end);
                    }
                    group_order_by_map[group_by_val] = order_by_vals;
                }
                order_by_vals_list[loc] = std::move(order_by_vals);
            }
        }
    }

    // Assemble order_by values into SearchResultData
    AssembleOrderByValues(search_res_data, order_by_vals_list, plan_);
}

}  // namespace milvus::segcore
