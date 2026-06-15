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

#pragma once

#include <limits>
#include <string>

#include <utility>
#include <vector>

#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/Utils.h"

namespace milvus::query {
inline void
FillEmptySearchResult(SearchResult& result, int64_t num_queries, int64_t topk) {
    auto total_num = num_queries * topk;
    result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
    result.distances_.resize(total_num, 0.0f);
    result.total_nq_ = num_queries;
    result.unity_topK_ = topk;
}

// Map logical element IDs returned by knowhere to (doc_id, elem_idx) pairs
// via ArrayOffsets.
inline std::pair<std::vector<int64_t>, std::vector<int32_t>>
ApplyElementIDMapping(const std::vector<int64_t>& element_ids,
                      const milvus::IArrayOffsets& array_offsets) {
    std::vector<int64_t> doc_offsets;
    std::vector<int32_t> element_indices;
    doc_offsets.reserve(element_ids.size());
    element_indices.reserve(element_ids.size());
    for (size_t i = 0; i < element_ids.size(); i++) {
        if (element_ids[i] == INVALID_SEG_OFFSET) {
            doc_offsets.push_back(INVALID_SEG_OFFSET);
            element_indices.push_back(-1);
        } else {
            auto [doc_id, elem_index] =
                array_offsets.ElementIDToRowID(element_ids[i]);
            doc_offsets.push_back(doc_id);
            element_indices.push_back(elem_index);
        }
    }
    return std::make_pair(std::move(doc_offsets), std::move(element_indices));
}

inline void
FinalizeVectorSearchOffsets(SearchResult& result,
                            const milvus::IArrayOffsets* array_offsets) {
    if (array_offsets != nullptr) {
        auto [doc_offsets, elem_indices] =
            ApplyElementIDMapping(result.seg_offsets_, *array_offsets);
        result.seg_offsets_ = std::move(doc_offsets);
        result.element_indices_ = std::move(elem_indices);
        result.element_level_ = true;
    }
}

template <typename T, typename U>
inline bool
Match(const T& x, const U& y, OpType op) {
    ThrowInfo(NotImplemented, "not supported");
}

template <>
inline bool
Match<std::string>(const std::string& str, const std::string& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

template <>
inline bool
Match<std::string_view>(const std::string_view& str,
                        const std::string& val,
                        OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

// Overloads for string_view combinations used when CompareExpr operands
// hold string_view in the data_access_type variant (chunk access), or a
// mix of string (index access) and string_view (chunk access).
inline bool
Match(const std::string_view& str, const std::string_view& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

inline bool
Match(const std::string& str, const std::string_view& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::InnerMatch:
            return InnerMatch(str, val);
        default:
            ThrowInfo(OpTypeInvalid, "not supported");
    }
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
gt_ub(int64_t t) {
    return t > std::numeric_limits<T>::max();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
lt_lb(int64_t t) {
    return t < std::numeric_limits<T>::min();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
out_of_range(int64_t t) {
    return gt_ub<T>(t) || lt_lb<T>(t);
}

inline bool
dis_closer(float dis1, float dis2, const MetricType& metric_type) {
    if (PositivelyRelated(metric_type))
        return dis1 > dis2;
    return dis1 < dis2;
}

}  // namespace milvus::query
