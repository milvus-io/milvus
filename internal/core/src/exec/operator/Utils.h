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

#pragma once

#include <cstddef>
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "knowhere/index/index_node.h"
#include "log/Log.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/ConcurrentVector.h"
#include "common/Span.h"
#include "query/Utils.h"
#include "common/EasyAssert.h"

namespace milvus {
namespace exec {

static bool
UseVectorIterator(const SearchInfo& search_info) {
    return search_info.group_by_field_id_.has_value() ||
           search_info.iterative_filter_execution;
}

static bool
PrepareVectorIteratorsFromIndex(const SearchInfo& search_info,
                                int nq,
                                const DatasetPtr dataset,
                                SearchResult& search_result,
                                const BitsetView& bitset,
                                const index::VectorIndex& index) {
    // when we use group by, we will use vector iterator to continously get results and group on them
    // when we use iterative filtered search, we will use vector iterator to continously get results and check scalar attr on them
    // until we get valid topk results
    if (UseVectorIterator(search_info)) {
        try {
            auto search_conf = index.PrepareSearchParams(search_info);
            knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
                iterators_val =
                    index.VectorIterators(dataset, search_conf, bitset);
            if (iterators_val.has_value()) {
                search_result.AssembleChunkVectorIterators(
                    nq, 1, {0}, iterators_val.value());
            } else {
                std::string operator_type = "";
                if (search_info.group_by_field_id_.has_value()) {
                    operator_type = "group_by";
                } else {
                    operator_type = "iterative filter";
                }
                LOG_ERROR(
                    "Returned knowhere iterator has non-ready iterators "
                    "inside, terminate {} operation:{}",
                    operator_type,
                    knowhere::Status2String(iterators_val.error()));
                ThrowInfo(
                    ErrorCode::Unsupported,
                    fmt::format(
                        "Returned knowhere iterator has non-ready iterators "
                        "inside, terminate {} operation",
                        operator_type));
            }
            search_result.total_nq_ = dataset->GetRows();
            search_result.unity_topK_ = search_info.topk_;
        } catch (const std::runtime_error& e) {
            std::string operator_type = "";
            if (search_info.group_by_field_id_.has_value()) {
                operator_type = "group_by";
            } else {
                operator_type = "iterative filter";
            }
            LOG_ERROR(
                "Caught error:{} when trying to initialize ann iterators for "
                "{}: "
                "operation will be terminated",
                e.what(),
                operator_type);
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("Failed to {}, current index:" +
                                      index.GetIndexType() + " doesn't support",
                                  operator_type));
        }
        return true;
    }
    return false;
}

inline void
sort_search_result(milvus::SearchResult& result, bool large_is_better) {
    auto nq = result.total_nq_;
    auto topk = result.unity_topK_;
    auto size = nq * topk;

    std::vector<float> new_distances = std::vector<float>();
    std::vector<int64_t> new_seg_offsets = std::vector<int64_t>();
    new_distances.reserve(size);
    new_seg_offsets.reserve(size);

    std::vector<size_t> idx(topk);

    for (size_t start = 0; start < size; start += topk) {
        for (size_t i = 0; i < idx.size(); ++i) idx[i] = start + i;

        if (large_is_better) {
            std::sort(idx.begin(), idx.end(), [&](size_t i, size_t j) {
                return result.distances_[i] > result.distances_[j] || (result.seg_offsets_[j] >=0 &&result.seg_offsets_[j] < 0);
            });
        } else {
            std::sort(idx.begin(), idx.end(), [&](size_t i, size_t j) {
                return result.distances_[i] < result.distances_[j] || (result.seg_offsets_[j] >=0 &&result.seg_offsets_[j] < 0);
            });
        }
        for (auto i : idx) {
            new_distances.push_back(result.distances_[i]);
            new_seg_offsets.push_back(result.seg_offsets_[i]);
        }
    }

    result.distances_ = new_distances;
    result.seg_offsets_ = new_seg_offsets;
}

}  // namespace exec
}  // namespace milvus