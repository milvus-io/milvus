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
#include "SearchGroupByOperator.h"

#include <algorithm>
#include <cstdint>
#include <tuple>
#include <utility>
#include <variant>

#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "fmt/core.h"
#include "query/Utils.h"

namespace milvus {

namespace exec {

// Helper to create a single-field getter that returns GroupByValueType
template <typename T, typename InnerRawType = T>
static std::function<GroupByValueType(int64_t)>
CreateFieldGetter(milvus::OpContext* op_ctx,
                  const segcore::SegmentInternalInterface& segment,
                  FieldId field_id,
                  std::optional<std::string> json_path = std::nullopt,
                  std::optional<DataType> json_type = std::nullopt,
                  bool strict_cast = false) {
    auto getter = GetDataGetter<T, InnerRawType>(
        op_ctx, segment, field_id, json_path, json_type, strict_cast);
    return
        [getter](int64_t idx) -> GroupByValueType { return getter->Get(idx); };
}

MultiFieldDataGetter::MultiFieldDataGetter(
    milvus::OpContext* op_ctx,
    const segcore::SegmentInternalInterface& segment,
    const std::vector<FieldId>& field_ids,
    const std::optional<std::string>& json_path,
    const std::optional<DataType>& json_type,
    bool strict_cast)
    : field_count_(field_ids.size()) {
    getters_.reserve(field_ids.size());

    for (const auto& field_id : field_ids) {
        auto data_type = segment.GetFieldDataType(field_id);
        std::function<GroupByValueType(int64_t)> getter;

        switch (data_type) {
            case DataType::INT8:
                getter = CreateFieldGetter<int8_t>(op_ctx, segment, field_id);
                break;
            case DataType::INT16:
                getter = CreateFieldGetter<int16_t>(op_ctx, segment, field_id);
                break;
            case DataType::INT32:
                getter = CreateFieldGetter<int32_t>(op_ctx, segment, field_id);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                getter = CreateFieldGetter<int64_t>(op_ctx, segment, field_id);
                break;
            case DataType::BOOL:
                getter = CreateFieldGetter<bool>(op_ctx, segment, field_id);
                break;
            case DataType::VARCHAR:
                getter =
                    CreateFieldGetter<std::string>(op_ctx, segment, field_id);
                break;
            case DataType::JSON:
                if (json_type.has_value()) {
                    switch (json_type.value()) {
                        case DataType::BOOL:
                            getter = CreateFieldGetter<bool, milvus::Json>(
                                op_ctx,
                                segment,
                                field_id,
                                json_path,
                                json_type,
                                strict_cast);
                            break;
                        case DataType::INT8:
                            getter = CreateFieldGetter<int8_t, milvus::Json>(
                                op_ctx,
                                segment,
                                field_id,
                                json_path,
                                json_type,
                                strict_cast);
                            break;
                        case DataType::INT16:
                            getter = CreateFieldGetter<int16_t, milvus::Json>(
                                op_ctx,
                                segment,
                                field_id,
                                json_path,
                                json_type,
                                strict_cast);
                            break;
                        case DataType::INT32:
                            getter = CreateFieldGetter<int32_t, milvus::Json>(
                                op_ctx,
                                segment,
                                field_id,
                                json_path,
                                json_type,
                                strict_cast);
                            break;
                        case DataType::INT64:
                            getter = CreateFieldGetter<int64_t, milvus::Json>(
                                op_ctx,
                                segment,
                                field_id,
                                json_path,
                                json_type,
                                strict_cast);
                            break;
                        case DataType::VARCHAR:
                            getter =
                                CreateFieldGetter<std::string, milvus::Json>(
                                    op_ctx,
                                    segment,
                                    field_id,
                                    json_path,
                                    json_type,
                                    strict_cast);
                            break;
                        default:
                            ThrowInfo(Unsupported,
                                      fmt::format("unsupported json_type {} "
                                                  "for JSON group by",
                                                  json_type.value()));
                    }
                } else {
                    getter = CreateFieldGetter<std::string, milvus::Json>(
                        op_ctx,
                        segment,
                        field_id,
                        json_path,
                        json_type,
                        strict_cast);
                }
                break;
            default:
                ThrowInfo(Unsupported,
                          fmt::format(
                              "unsupported data type {} for group by operator",
                              data_type));
        }
        getters_.push_back(std::move(getter));
    }
}

CompositeGroupKey
MultiFieldDataGetter::Get(int64_t idx) const {
    CompositeGroupKey key(field_count_);
    GetInto(idx, key);
    return key;
}

void
MultiFieldDataGetter::GetInto(int64_t idx, CompositeGroupKey& out) const {
    out.Clear();
    out.Reserve(field_count_);
    for (const auto& getter : getters_) {
        out.Add(getter(idx));
    }
}

// Internal helper: iterate a single iterator and collect grouped results.
// All tunables (topk / group_size / strict_group_size / metric_type) are
// read from `search_info` — don't duplicate them as separate parameters.
static void
GroupIteratorResult(const std::shared_ptr<VectorIterator>& iterator,
                    const std::shared_ptr<MultiFieldDataGetter>& data_getter,
                    std::vector<CompositeGroupKey>& composite_group_by_values,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances,
                    const SearchInfo& search_info) {
    // 1. Create group map for composite keys
    CompositeGroupByMap groupMap(search_info.topk_,
                                 search_info.group_size_,
                                 search_info.strict_group_size_);

    auto is_element_id = search_info.element_level();

    //2. do iteration until fill the whole map or run out of all data
    //note it may enumerate all data inside a segment and can block following
    //query and search possibly
    std::vector<std::tuple<int64_t, float, CompositeGroupKey>> res;
    CompositeGroupKey scratch_key;
    while (iterator->HasNext() && !groupMap.IsGroupResEnough()) {
        auto offset_dis_pair = iterator->Next();
        AssertInfo(offset_dis_pair.has_value(),
                   "Wrong state! iterator cannot return valid result whereas "
                   "it still tells hasNext");
        auto raw_offset = offset_dis_pair.value().first;
        auto dis = offset_dis_pair.value().second;

        // For element-level search, the offset is the element_id, we need to convert it to the row_id.
        int64_t row_offset = raw_offset;
        if (is_element_id) {
            AssertInfo(search_info.array_offsets_ != nullptr,
                       "Array offsets not available for element-level search");
            row_offset =
                search_info.array_offsets_
                    ->ElementIDToRowID(static_cast<int32_t>(raw_offset))
                    .first;
        }

        data_getter->GetInto(row_offset, scratch_key);
        if (groupMap.Push(scratch_key)) {
            // Safe to move: next iteration's GetInto() will Clear+Reserve+Add
            // on the moved-from small_vector, which is guaranteed empty-inline.
            res.emplace_back(row_offset, dis, std::move(scratch_key));
        }
    }

    // 3. Sort based on distances and metrics
    auto customComparator = [&](const auto& lhs, const auto& rhs) {
        return milvus::query::dis_closer(
            std::get<1>(lhs), std::get<1>(rhs), search_info.metric_type_);
    };
    std::sort(res.begin(), res.end(), customComparator);

    // 4. Save results
    for (auto iter = res.begin(); iter != res.end(); ++iter) {
        offsets.emplace_back(std::get<0>(*iter));
        distances.emplace_back(std::get<1>(*iter));
        composite_group_by_values.emplace_back(std::move(std::get<2>(*iter)));
    }
}

void
SearchGroupBy(milvus::OpContext* op_ctx,
              const std::vector<std::shared_ptr<VectorIterator>>& iterators,
              const SearchInfo& search_info,
              std::vector<CompositeGroupKey>& composite_group_by_values,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::vector<size_t>& topk_per_nq_prefix_sum) {
    // Get field IDs for group by
    AssertInfo(!search_info.group_by_field_ids_.empty(),
               "group_by_field_ids must be set for group by search");
    const auto& field_ids = search_info.group_by_field_ids_;

    int max_total_size =
        search_info.topk_ * search_info.group_size_ * iterators.size();
    seg_offsets.reserve(max_total_size);
    distances.reserve(max_total_size);
    composite_group_by_values.reserve(max_total_size);
    topk_per_nq_prefix_sum.reserve(iterators.size() + 1);

    // Create data getter for all fields
    auto data_getter =
        std::make_shared<MultiFieldDataGetter>(op_ctx,
                                               segment,
                                               field_ids,
                                               search_info.json_path_,
                                               search_info.json_type_,
                                               search_info.strict_cast_);

    topk_per_nq_prefix_sum.push_back(0);
    for (const auto& iterator : iterators) {
        GroupIteratorResult(iterator,
                            data_getter,
                            composite_group_by_values,
                            seg_offsets,
                            distances,
                            search_info);
        topk_per_nq_prefix_sum.push_back(seg_offsets.size());
    }
}

}  // namespace exec
}  // namespace milvus
