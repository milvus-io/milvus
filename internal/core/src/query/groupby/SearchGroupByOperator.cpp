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
#include "common/Consts.h"
#include "segcore/SegmentSealedImpl.h"
#include "query/Utils.h"

namespace milvus {
namespace query {

void
SearchGroupBy(const std::vector<std::shared_ptr<VectorIterator>>& iterators,
              const SearchInfo& search_info,
              std::vector<GroupByValueType>& group_by_values,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::vector<size_t>& topk_per_nq_prefix_sum) {
    //1. get search meta
    FieldId group_by_field_id = search_info.group_by_field_id_.value();
    auto data_type = segment.GetFieldDataType(group_by_field_id);
    int max_total_size =
        search_info.topk_ * search_info.group_size_ * iterators.size();
    seg_offsets.reserve(max_total_size);
    distances.reserve(max_total_size);
    group_by_values.reserve(max_total_size);
    topk_per_nq_prefix_sum.reserve(iterators.size() + 1);
    switch (data_type) {
        case DataType::INT8: {
            auto dataGetter = GetDataGetter<int8_t>(segment, group_by_field_id);
            GroupIteratorsByType<int8_t>(iterators,
                                         search_info.topk_,
                                         search_info.group_size_,
                                         search_info.group_strict_size_,
                                         *dataGetter,
                                         group_by_values,
                                         seg_offsets,
                                         distances,
                                         search_info.metric_type_,
                                         topk_per_nq_prefix_sum);
            break;
        }
        case DataType::INT16: {
            auto dataGetter =
                GetDataGetter<int16_t>(segment, group_by_field_id);
            GroupIteratorsByType<int16_t>(iterators,
                                          search_info.topk_,
                                          search_info.group_size_,
                                          search_info.group_strict_size_,
                                          *dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_,
                                          topk_per_nq_prefix_sum);
            break;
        }
        case DataType::INT32: {
            auto dataGetter =
                GetDataGetter<int32_t>(segment, group_by_field_id);
            GroupIteratorsByType<int32_t>(iterators,
                                          search_info.topk_,
                                          search_info.group_size_,
                                          search_info.group_strict_size_,
                                          *dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_,
                                          topk_per_nq_prefix_sum);
            break;
        }
        case DataType::INT64: {
            auto dataGetter =
                GetDataGetter<int64_t>(segment, group_by_field_id);
            GroupIteratorsByType<int64_t>(iterators,
                                          search_info.topk_,
                                          search_info.group_size_,
                                          search_info.group_strict_size_,
                                          *dataGetter,
                                          group_by_values,
                                          seg_offsets,
                                          distances,
                                          search_info.metric_type_,
                                          topk_per_nq_prefix_sum);
            break;
        }
        case DataType::BOOL: {
            auto dataGetter = GetDataGetter<bool>(segment, group_by_field_id);
            GroupIteratorsByType<bool>(iterators,
                                       search_info.topk_,
                                       search_info.group_size_,
                                       search_info.group_strict_size_,
                                       *dataGetter,
                                       group_by_values,
                                       seg_offsets,
                                       distances,
                                       search_info.metric_type_,
                                       topk_per_nq_prefix_sum);
            break;
        }
        case DataType::VARCHAR: {
            auto dataGetter =
                GetDataGetter<std::string>(segment, group_by_field_id);
            GroupIteratorsByType<std::string>(iterators,
                                              search_info.topk_,
                                              search_info.group_size_,
                                              search_info.group_strict_size_,
                                              *dataGetter,
                                              group_by_values,
                                              seg_offsets,
                                              distances,
                                              search_info.metric_type_,
                                              topk_per_nq_prefix_sum);
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
    const std::vector<std::shared_ptr<VectorIterator>>& iterators,
    int64_t topK,
    int64_t group_size,
    bool group_strict_size,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& seg_offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type,
    std::vector<size_t>& topk_per_nq_prefix_sum) {
    topk_per_nq_prefix_sum.push_back(0);
    for (auto& iterator : iterators) {
        GroupIteratorResult<T>(iterator,
                               topK,
                               group_size,
                               group_strict_size,
                               data_getter,
                               group_by_values,
                               seg_offsets,
                               distances,
                               metrics_type);
        topk_per_nq_prefix_sum.push_back(seg_offsets.size());
    }
}

template <typename T>
void
GroupIteratorResult(const std::shared_ptr<VectorIterator>& iterator,
                    int64_t topK,
                    int64_t group_size,
                    bool group_strict_size,
                    const DataGetter<T>& data_getter,
                    std::vector<GroupByValueType>& group_by_values,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances,
                    const knowhere::MetricType& metrics_type) {
    //1.
    GroupByMap<T> groupMap(topK, group_size, group_strict_size);

    //2. do iteration until fill the whole map or run out of all data
    //note it may enumerate all data inside a segment and can block following
    //query and search possibly
    std::vector<std::tuple<int64_t, float, T>> res;
    while (iterator->HasNext() && !groupMap.IsGroupResEnough()) {
        auto offset_dis_pair = iterator->Next();
        AssertInfo(
            offset_dis_pair.has_value(),
            "Wrong state! iterator cannot return valid result whereas it still"
            "tells hasNext, terminate groupBy operation");
        auto offset = offset_dis_pair.value().first;
        auto dis = offset_dis_pair.value().second;
        T row_data = data_getter.Get(offset);
        if (groupMap.Push(row_data)) {
            res.emplace_back(offset, dis, row_data);
        }
    }

    //3. sorted based on distances and metrics
    auto customComparator = [&](const auto& lhs, const auto& rhs) {
        return dis_closer(std::get<1>(lhs), std::get<1>(rhs), metrics_type);
    };
    std::sort(res.begin(), res.end(), customComparator);

    //4. save groupBy results
    for (auto iter = res.cbegin(); iter != res.cend(); iter++) {
        offsets.emplace_back(std::get<0>(*iter));
        distances.emplace_back(std::get<1>(*iter));
        group_by_values.emplace_back(std::move(std::get<2>(*iter)));
    }
}

}  // namespace query
}  // namespace milvus
