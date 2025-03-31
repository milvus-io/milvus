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

#include "common/QueryInfo.h"
#include "knowhere/index/index_node.h"
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
                PanicInfo(
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
            PanicInfo(ErrorCode::Unsupported,
                      fmt::format("Failed to {}, current index:" +
                                      index.GetIndexType() + " doesn't support",
                                  operator_type));
        }
        return true;
    }
    return false;
}
}  // namespace exec
}  // namespace milvus