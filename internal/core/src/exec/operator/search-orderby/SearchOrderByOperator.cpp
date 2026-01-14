// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "SearchOrderByOperator.h"
#include "common/EasyAssert.h"
#include "exec/operator/search-groupby/SearchGroupByOperator.h"
#include "query/Utils.h"
#include "segcore/ReduceUtils.h"
#include <algorithm>
#include <unordered_map>

namespace milvus {
namespace exec {

// Use the shared CompareOrderByValue from segcore::ReduceUtils
using segcore::CompareOrderByValue;

void
SearchOrderBy(milvus::OpContext* op_ctx,
              const std::vector<plan::OrderByField>& order_by_fields,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::optional<std::vector<GroupByValueType>>& group_by_values,
              std::vector<size_t>& topk_per_nq_prefix_sum,
              const knowhere::MetricType& metric_type) {
    if (order_by_fields.empty()) {
        return;
    }

    const bool has_group_by = group_by_values.has_value();
    const size_t total_size = seg_offsets.size();
    if (total_size == 0) {
        return;
    }

    // Create data getters for each field
    // Use a function pointer to handle different types
    using GetterFunc = std::function<OrderByValueType(int64_t)>;
    std::vector<GetterFunc> getters;
    getters.reserve(order_by_fields.size());

    for (const auto& field : order_by_fields) {
        auto data_type = segment.GetFieldDataType(field.field_id_);
        GetterFunc getter;

        // Create appropriate DataGetter based on field type and wrap in function
        switch (data_type) {
            case DataType::BOOL: {
                auto dg = GetDataGetter<bool>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::INT8: {
                auto dg = GetDataGetter<int8_t>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::INT16: {
                auto dg = GetDataGetter<int16_t>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::INT32: {
                auto dg = GetDataGetter<int32_t>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::INT64: {
                auto dg = GetDataGetter<int64_t>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::FLOAT: {
                auto dg = GetDataGetter<float>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::DOUBLE: {
                auto dg = GetDataGetter<double>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::VARCHAR:
            case DataType::STRING: {
                auto dg = GetDataGetter<std::string>(
                    op_ctx, segment, field.field_id_, field.json_path_);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            case DataType::JSON: {
                if (!field.json_path_.has_value()) {
                    ThrowInfo(UnexpectedError,
                              "Order by JSON requires json_path");
                }
                auto dg =
                    GetDataGetter<std::string, milvus::Json>(op_ctx,
                                                             segment,
                                                             field.field_id_,
                                                             field.json_path_,
                                                             std::nullopt,
                                                             false);
                getter = [dg](int64_t idx) -> OrderByValueType {
                    return MakeOrderByValue(dg->Get(idx));
                };
                break;
            }
            default:
                ThrowInfo(UnexpectedError,
                          "Order by field type {} not supported",
                          data_type);
        }
        getters.push_back(getter);
    }

    // Precompute all order_by values to avoid repeated segment reads during sorting
    // This reduces DataGetter calls from O(n*log(n)*fields) to O(n*fields)
    std::vector<std::vector<OrderByValueType>> precomputed_values(total_size);
    for (size_t i = 0; i < total_size; ++i) {
        precomputed_values[i].reserve(order_by_fields.size());
        for (size_t field_idx = 0; field_idx < order_by_fields.size();
             ++field_idx) {
            precomputed_values[i].push_back(getters[field_idx](seg_offsets[i]));
        }
    }

    // Create index array for sorting
    std::vector<size_t> indices(total_size);
    for (size_t i = 0; i < total_size; ++i) {
        indices[i] = i;
    }

    // Custom comparator for multi-field sorting (uses precomputed values)
    auto comparator = [&](size_t lhs_idx, size_t rhs_idx) -> bool {
        for (size_t field_idx = 0; field_idx < order_by_fields.size();
             ++field_idx) {
            const auto& field = order_by_fields[field_idx];
            const auto& lhs_val = precomputed_values[lhs_idx][field_idx];
            const auto& rhs_val = precomputed_values[rhs_idx][field_idx];

            // Handle null values: nulls are considered less than non-nulls
            if (!lhs_val.has_value() && !rhs_val.has_value()) {
                continue;  // Both null, compare next field
            }
            if (!lhs_val.has_value()) {
                return field.ascending_;  // null < non-null
            }
            if (!rhs_val.has_value()) {
                return !field.ascending_;  // non-null > null
            }

            // Compare values using OrderByValueType comparison
            int cmp = CompareOrderByValue(lhs_val, rhs_val);
            if (cmp < 0) {
                return field.ascending_;
            }
            if (cmp > 0) {
                return !field.ascending_;
            }
            // Equal, continue to next field
        }
        // All order_by fields equal, tie-break by distance using metric-aware comparison.
        // Use symmetric checks so NaN distances still fall through to the index tie-break.
        if (milvus::query::dis_closer(
                distances[lhs_idx], distances[rhs_idx], metric_type)) {
            return true;
        }
        if (milvus::query::dis_closer(
                distances[rhs_idx], distances[lhs_idx], metric_type)) {
            return false;
        }
        // Distances also equal, use original index as final tie-breaker for determinism
        // This preserves the original input order (effectively stable sort)
        return lhs_idx < rhs_idx;
    };

    // Sort by query groups if has group_by
    if (has_group_by) {
        // Group + OrderBy Behavior:
        // When both group_by and order_by are specified, groups are sorted by the order_by
        // field value of the FIRST (best-scoring) item within each group. This means:
        //   - Items within each group are first selected by their similarity score (distance)
        //   - The first item (highest score) in each group represents the group for ordering
        //   - Groups are then sorted based on this representative item's order_by field value
        //
        // Example: With group_by=category, order_by=price ASC
        //   - Group "Electronics" first item has price $999
        //   - Group "Books" first item has price $15
        //   - Result: Books group comes first because $15 < $999
        size_t start_idx = 0;
        for (size_t i = 0; i < topk_per_nq_prefix_sum.size() - 1; ++i) {
            size_t end_idx = topk_per_nq_prefix_sum[i + 1];
            if (start_idx >= end_idx) {
                start_idx = end_idx;
                continue;
            }

            // Group by group_by_values within this query's results
            std::unordered_map<GroupByValueType, std::vector<size_t>> group_map;
            for (size_t j = start_idx; j < end_idx; ++j) {
                group_map[group_by_values.value()[j]].push_back(j);
            }

            // Create a vector of groups with their first item's order_by values
            struct GroupInfo {
                GroupByValueType group_val;
                std::vector<size_t> indices;
                std::vector<OrderByValueType> first_item_order_by_values;
            };
            std::vector<GroupInfo> groups;
            groups.reserve(group_map.size());

            for (auto& [group_val, group_indices] : group_map) {
                GroupInfo group_info;
                group_info.group_val = group_val;
                group_info.indices = std::move(group_indices);

                // Get order_by field values from the first item in this group (use precomputed)
                size_t first_idx = group_info.indices[0];
                group_info.first_item_order_by_values =
                    precomputed_values[first_idx];
                groups.push_back(std::move(group_info));
            }

            // Sort groups by their first item's order_by field values
            auto group_comparator = [&](const GroupInfo& lhs,
                                        const GroupInfo& rhs) -> bool {
                for (size_t field_idx = 0; field_idx < order_by_fields.size();
                     ++field_idx) {
                    const auto& field = order_by_fields[field_idx];
                    const auto& lhs_val =
                        lhs.first_item_order_by_values[field_idx];
                    const auto& rhs_val =
                        rhs.first_item_order_by_values[field_idx];

                    // Handle null values: nulls are considered less than non-nulls
                    if (!lhs_val.has_value() && !rhs_val.has_value()) {
                        continue;  // Both null, compare next field
                    }
                    if (!lhs_val.has_value()) {
                        return field.ascending_;  // null < non-null
                    }
                    if (!rhs_val.has_value()) {
                        return !field.ascending_;  // non-null > null
                    }

                    // Compare values using OrderByValueType comparison
                    int cmp = CompareOrderByValue(lhs_val, rhs_val);
                    if (cmp < 0) {
                        return field.ascending_;
                    }
                    if (cmp > 0) {
                        return !field.ascending_;
                    }
                    // Equal, continue to next field
                }
                // All order_by fields equal, tie-break by first item's distance
                // using metric-aware comparison.
                // Use symmetric checks so NaN distances still fall through to the index tie-break.
                if (milvus::query::dis_closer(distances[lhs.indices[0]],
                                              distances[rhs.indices[0]],
                                              metric_type)) {
                    return true;
                }
                if (milvus::query::dis_closer(distances[rhs.indices[0]],
                                              distances[lhs.indices[0]],
                                              metric_type)) {
                    return false;
                }
                // Distances also equal, use first item's original index for determinism
                return lhs.indices[0] < rhs.indices[0];
            };

            std::sort(groups.begin(), groups.end(), group_comparator);

            // Flatten sorted groups back to indices (preserve order within each group)
            std::vector<size_t> sorted_indices;
            for (const auto& group : groups) {
                sorted_indices.insert(sorted_indices.end(),
                                      group.indices.begin(),
                                      group.indices.end());
            }

            // Update indices for this query
            for (size_t j = 0; j < sorted_indices.size(); ++j) {
                indices[start_idx + j] = sorted_indices[j];
            }

            start_idx = end_idx;
        }
    } else {
        // Sort all results (per query if topk_per_nq_prefix_sum is available)
        if (topk_per_nq_prefix_sum.size() > 1) {
            size_t start_idx = 0;
            for (size_t i = 0; i < topk_per_nq_prefix_sum.size() - 1; ++i) {
                size_t end_idx = topk_per_nq_prefix_sum[i + 1];
                if (start_idx < end_idx) {
                    std::sort(indices.begin() + start_idx,
                              indices.begin() + end_idx,
                              comparator);
                }
                start_idx = end_idx;
            }
        } else {
            // Single query or no prefix sum
            std::sort(indices.begin(), indices.end(), comparator);
        }
    }

    // Reorder arrays based on sorted indices
    std::vector<int64_t> sorted_offsets(total_size);
    std::vector<float> sorted_distances(total_size);
    for (size_t i = 0; i < total_size; ++i) {
        sorted_offsets[i] = seg_offsets[indices[i]];
        sorted_distances[i] = distances[indices[i]];
    }
    seg_offsets = std::move(sorted_offsets);
    distances = std::move(sorted_distances);

    // Reorder group_by_values if exists
    if (has_group_by) {
        std::vector<GroupByValueType> sorted_group_values(total_size);
        for (size_t i = 0; i < total_size; ++i) {
            sorted_group_values[i] = group_by_values.value()[indices[i]];
        }
        group_by_values = std::move(sorted_group_values);
    }
}

}  // namespace exec
}  // namespace milvus
