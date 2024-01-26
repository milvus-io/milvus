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
#include "GroupByOperator.h"
#include "common/Consts.h"
#include "segcore/SegmentSealedImpl.h"
#include "Utils.h"

namespace milvus {
namespace query {

void
GroupBy(const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>&
            iterators,
        const SearchInfo& search_info,
        std::vector<GroupByValueType>& group_by_values,
        const segcore::SegmentInternalInterface& segment,
        std::vector<int64_t>& seg_offsets,
        std::vector<float>& distances) {
    //0. check segment type, for period-1, only support group by for sealed segments
    if (!dynamic_cast<const segcore::SegmentSealedImpl*>(&segment)) {
        LOG_ERROR(
            "Not support group_by operation for non-sealed segment, "
            "segment_id:{}",
            segment.get_segment_id());
        return;
    }

    //1. get search meta
    FieldId group_by_field_id = search_info.group_by_field_id_.value();
    auto data_type = segment.GetFieldDataType(group_by_field_id);

    switch (data_type) {
        case DataType::INT8: {
            DataGetter<int8_t> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<int8_t>(iterators,
                                         group_by_field_id,
                                         search_info.topk_,
                                         dataGetter,
                                         group_by_values,
                                         seg_offsets,
                                         distances,
                                         search_info.metric_type_);
            break;
        }
        case DataType::INT16: {
            DataGetter<int16_t> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<int16_t>(iterators,
                                          group_by_field_id,
                                          search_info.topk_,
                                          dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_);
            break;
        }
        case DataType::INT32: {
            DataGetter<int32_t> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<int32_t>(iterators,
                                          group_by_field_id,
                                          search_info.topk_,
                                          dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_);
            break;
        }
        case DataType::INT64: {
            DataGetter<int64_t> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<int64_t>(iterators,
                                          group_by_field_id,
                                          search_info.topk_,
                                          dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_);
            break;
        }
        case DataType::BOOL: {
            DataGetter<bool> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<bool>(iterators,
                                       group_by_field_id,
                                       search_info.topk_,
                                       dataGetter,
                                       group_by_values,
                                       seg_offsets,
                                       distances,
                                       search_info.metric_type_);
            break;
        }
        case DataType::VARCHAR: {
            DataGetter<std::string> dataGetter(segment, group_by_field_id);
            GroupIteratorsByType<std::string>(iterators,
                                              group_by_field_id,
                                              search_info.topk_,
                                              dataGetter,
                                              group_by_values,
                                              seg_offsets,
                                              distances,
                                              search_info.metric_type_);
            break;
        }
        default: {
            PanicInfo(
                Unsupported,
                fmt::format("unsupported data type {} for group by operator",
                            data_type));
        }
    }
}

template <typename T>
void
GroupIteratorsByType(
    const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>&
        iterators,
    FieldId field_id,
    int64_t topK,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& seg_offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type) {
    for (auto& iterator : iterators) {
        GroupIteratorResult<T>(iterator,
                               field_id,
                               topK,
                               data_getter,
                               group_by_values,
                               seg_offsets,
                               distances,
                               metrics_type);
    }
}

template <typename T>
void
GroupIteratorResult(
    const std::shared_ptr<knowhere::IndexNode::iterator>& iterator,
    FieldId field_id,
    int64_t topK,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type) {
    //1.
    std::unordered_map<T, std::pair<int64_t, float>> groupMap;

    //2. do iteration until fill the whole map or run out of all data
    //note it may enumerate all data inside a segment and can block following
    //query and search possibly
    auto dis_closer = [&](float l, float r) {
        if (PositivelyRelated(metrics_type))
            return l > r;
        return l < r;
    };
    while (iterator->HasNext() && groupMap.size() < topK) {
        auto [offset, dis] = iterator->Next();
        T row_data = data_getter.Get(offset);
        auto it = groupMap.find(row_data);
        if (it == groupMap.end()) {
            groupMap.emplace(row_data, std::make_pair(offset, dis));
        } else if (dis_closer(dis, it->second.second)) {
            it->second = {offset, dis};
        }
    }

    //3. sorted based on distances and metrics
    std::vector<std::pair<T, std::pair<int64_t, float>>> sortedGroupVals(
        groupMap.begin(), groupMap.end());
    auto customComparator = [&](const auto& lhs, const auto& rhs) {
        return dis_closer(lhs.second.second, rhs.second.second);
    };
    std::sort(sortedGroupVals.begin(), sortedGroupVals.end(), customComparator);

    //4. save groupBy results
    group_by_values.reserve(sortedGroupVals.size());
    offsets.reserve(sortedGroupVals.size());
    distances.reserve(sortedGroupVals.size());
    for (auto iter = sortedGroupVals.cbegin(); iter != sortedGroupVals.cend();
         iter++) {
        group_by_values.emplace_back(iter->first);
        offsets.push_back(iter->second.first);
        distances.push_back(iter->second.second);
    }

    //5. padding topK results, extra memory consumed will be removed when reducing
    for (std::size_t idx = groupMap.size(); idx < topK; idx++) {
        offsets.push_back(INVALID_SEG_OFFSET);
        distances.push_back(0.0);
        group_by_values.emplace_back(std::monostate{});
    }
}

}  // namespace query
}  // namespace milvus
