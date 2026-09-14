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
#include "common/OffsetMapping.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "knowhere/array_store.h"

namespace milvus::query {
inline bool
CanUseStrictGroupControls(const SearchInfo& info, int64_t nq) {
    return info.strict_group_size_ && info.group_size_ > 1 && info.topk_ > 0 &&
           nq == 1 && info.array_offsets_ == nullptr &&
           info.group_by_field_ids_.size() == 1;
}

inline void
ApplyStrictGroupSkipRefine(const SearchInfo& info,
                           int64_t nq,
                           knowhere::Json& params) {
    if (CanUseStrictGroupControls(info, nq)) {
        params["skip_refine"] = info.strict_group_skip_refine_;
    }
}

// Convert a group quota into ordinary vector Search without mutating phase one.
inline SearchInfo
StrictGroupSearchInfo(const SearchInfo& original, int64_t remaining_topk) {
    auto info = original;
    // Providers are registered only for nq=1. Set the backend parameter before
    // converting per-group completion into an ordinary (non-grouped) Search.
    ApplyStrictGroupSkipRefine(original, 1, info.search_params_);
    info.topk_ = remaining_topk;
    info.group_by_field_ids_.clear();
    info.group_size_ = 1;
    info.strict_group_size_ = false;
    info.iterative_filter_execution = false;
    info.iterator_v2_info_.reset();
    // Group-by consumes unrounded iterator distances; preserve that here.
    info.round_decimal_ = -1;
    info.search_params_[knowhere::meta::TOPK] = remaining_topk;
    // Iterator accepts ef < k, but HNSW Search does not. Knowhere accepts
    // integer strings as well as JSON integers. Raise only a valid explicit ef
    // in this copy; leave malformed values and defaults to backend validation.
    auto ef = info.search_params_.find("ef");
    if (ef != info.search_params_.end()) {
        if (ef->is_number_integer() && *ef > 0 &&
            *ef <= std::numeric_limits<int>::max() && *ef < remaining_topk) {
            *ef = remaining_topk;
        } else if (ef->is_string()) {
            const auto& value = ef->get_ref<const std::string&>();
            try {
                size_t end = 0;
                const auto parsed = std::stoll(value, &end);
                if (end == value.size() && parsed > 0 &&
                    parsed <= std::numeric_limits<int>::max() &&
                    parsed < remaining_topk) {
                    *ef = std::to_string(remaining_topk);
                }
            } catch (const std::invalid_argument&) {
                // Preserve the invalid input for Knowhere's validation.
            } catch (const std::out_of_range&) {
                // Preserve overflow instead of turning it into a valid ef.
            }
        }
    }
    return info;
}

inline bool
CanUseStrictGroupSearch(const SearchInfo& search_info, int64_t num_queries) {
    return search_info.strict_group_strategy_ ==
               StrictGroupStrategy::PerGroup &&
           CanUseStrictGroupControls(search_info, num_queries);
}

inline void
FillEmptySearchResult(SearchResult& result, int64_t num_queries, int64_t topk) {
    auto total_num = num_queries * topk;
    result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
    result.distances_.resize(total_num, 0.0f);
    result.total_nq_ = num_queries;
    result.unity_topK_ = topk;
}

inline BitsetView
AttachOffsetMappingIds(const BitsetView& bitset,
                       const OffsetMappingIdView& ids) {
    auto mapped = bitset;
    if (!ids.empty()) {
        // BF scans local physical ids. The mapping view is already clipped to
        // one contiguous p2l window, so the backend can use ids directly.
        knowhere::IdArray out_ids(ids.data, static_cast<size_t>(ids.count));
        mapped.set_id_offset(0);
        mapped.set_out_ids(out_ids, out_ids.size());
        mapped.set_vector_count(static_cast<size_t>(ids.count));
    }
    return mapped;
}

inline const void*
AdvanceVectorDataPointer(const void* data,
                         DataType data_type,
                         int64_t dim,
                         int64_t rows) {
    if (rows == 0) {
        return data;
    }
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        return static_cast<const knowhere::sparse::SparseRow<SparseValueType>*>(
                   data) +
               rows;
    }
    return static_cast<const uint8_t*>(data) +
           rows * static_cast<int64_t>(GetDataTypeSize(data_type, dim));
}

// Map VECTOR_ARRAY element IDs returned by Knowhere to (doc_id, elem_idx)
// pairs via ArrayOffsets. This is element-space only; row-level nullable
// mapping is handled before or inside Knowhere search.
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

// Convert VECTOR_ARRAY element IDs to (row_id, elem_idx). Row-level vector
// search already receives logical IDs from Knowhere: indexed paths use IdMap,
// raw BF paths pass physical->logical IDs through BitsetView.
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
