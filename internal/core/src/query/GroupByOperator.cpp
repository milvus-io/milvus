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

namespace milvus{
namespace query{

knowhere::DataSetPtr
GroupBy(
        const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>& iterators,
        const SearchInfo& search_info,
        std::vector<GroupByValueType>& group_by_values,
        const segcore::SegmentInternalInterface& segment,
        std::vector<int64_t>& seg_offsets,
        std::vector<float>& distances) {
    //0. check segment type, for period-1, only support group by for sealed segments
    if(!dynamic_cast<const segcore::SegmentSealedImpl*>(&segment)){
        LOG_SEGCORE_ERROR_ << "Not support group_by operation "
                              "for non-sealed segment, segment_id:" << segment.get_segment_id();
        return nullptr;
    }

    //1. get search meta
    FieldId group_by_field_id = search_info.group_by_field_id_.value();
    auto data_type = segment.GetFieldDataType(group_by_field_id);

    switch(data_type){
        case DataType::INT8:{
            auto field_data = segment.chunk_data<int8_t>(group_by_field_id, 0);
            GroupIteratorsByType<int8_t>(iterators, group_by_field_id, search_info.topk_,
                                         field_data, group_by_values, seg_offsets, distances);
            break;
        }
        case DataType::INT16:{
            auto field_data = segment.chunk_data<int16_t>(group_by_field_id, 0);
            GroupIteratorsByType<int16_t>(iterators, group_by_field_id, search_info.topk_,
                                          field_data, group_by_values, seg_offsets, distances);
            break;
        }
        case DataType::INT32:{
            auto field_data = segment.chunk_data<int32_t>(group_by_field_id, 0);
            GroupIteratorsByType<int32_t>(iterators, group_by_field_id, search_info.topk_,
                                          field_data, group_by_values, seg_offsets, distances);
            break;
        }
        case DataType::INT64:{
            auto field_data = segment.chunk_data<int64_t>(group_by_field_id, 0);
            GroupIteratorsByType<int64_t>(iterators, group_by_field_id, search_info.topk_,
                                          field_data, group_by_values, seg_offsets, distances);
            break;
        }
        case DataType::BOOL: {
            auto field_data = segment.chunk_data<bool>(group_by_field_id, 0);
            GroupIteratorsByType<bool>(iterators, group_by_field_id, search_info.topk_,
                                          field_data, group_by_values, seg_offsets, distances);
            break;
        }
        case DataType::VARCHAR:{
            auto field_data = segment.chunk_data<std::string_view>(group_by_field_id, 0);
            GroupIteratorsByType<std::string_view>(iterators, group_by_field_id, search_info.topk_,
                                       field_data, group_by_values, seg_offsets, distances);
            break;
        }
        default:{
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported data type {} for group by operator", data_type));
        }
    }
    return nullptr;
}

template <typename T>
void
GroupIteratorsByType(const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>> &iterators,
                     FieldId field_id,
                     int64_t topK,
                     Span<T> field_data,
                     std::vector<GroupByValueType> &group_by_values,
                     std::vector<int64_t>& seg_offsets,
                     std::vector<float>& distances){
    for(auto& iterator: iterators){
        GroupIteratorResult<T>(iterator, field_id, topK, field_data, group_by_values, seg_offsets, distances);
    }
}

template<typename T>
void
GroupIteratorResult(const std::shared_ptr<knowhere::IndexNode::iterator>& iterator,
                    FieldId field_id,
                    int64_t topK,
                    Span<T> field_data,
                    std::vector<GroupByValueType>& group_by_values,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances){
    //1.
    std::unordered_map<T, std::pair<int64_t, float>> groupMap;
    std::vector<int64_t> tmpOffsets;
    std::vector<float> tmpDistances;

    //2. do iteration for at most 10 rounds
    int round = 0;
    while(round < 10) {
        int64_t count = 0;
        while(iterator->HasNext()){
            auto nextPair = iterator->Next();
            int64_t offset = nextPair.first;
            float dis = nextPair.second;
            tmpOffsets.emplace_back(offset);
            tmpDistances.emplace_back(dis);
            count++;
            if(count >= topK){
                break;
            }
        }
        round++;
        GroupOneRound<T>(tmpOffsets, tmpDistances, field_data, groupMap);
        tmpOffsets.clear();
        tmpDistances.clear();
        if(!iterator->HasNext() || groupMap.size()==topK) break;
    }

    std::vector<std::pair<T, std::pair<int64_t, float>>> sortedGroupVals(groupMap.begin(), groupMap.end());
    auto customComparator = [](const auto& lhs, const auto& rhs){
        return lhs.second.second > rhs.second.second;
    };
    std::sort(sortedGroupVals.begin(), sortedGroupVals.end(), customComparator);

    //3. save groupBy results
    for(auto iter = sortedGroupVals.cbegin(); iter != sortedGroupVals.cend(); iter++){
        group_by_values.emplace_back(iter->first);
        offsets.push_back(iter->second.first);
        distances.push_back(iter->second.second);
    }

    //4. padding topK results, extra memory consumed will be removed when reducing
    for(std::size_t idx = groupMap.size(); idx < topK; idx++){
        offsets.push_back(INVALID_SEG_OFFSET);
        distances.push_back(0.0);
        group_by_values.emplace_back(std::monostate{});
    }

}

template <typename T>
void
GroupOneRound(const std::vector<int64_t>& seg_offsets,
              const std::vector<float>& distances,
              Span<T> field_data,
              std::unordered_map<T, std::pair<int64_t, float>>& groupMap) {

    for(std::size_t idx = 0; idx < seg_offsets.size(); idx++){
        auto seg_offset = seg_offsets.at(idx);
        const T& row_data = field_data.operator[](seg_offset);
        auto it = groupMap.find(row_data);
        if(it == groupMap.end()){
            groupMap.insert(std::make_pair(row_data, std::make_pair(seg_offset, distances[idx])));
        }
    }
}


}
}
