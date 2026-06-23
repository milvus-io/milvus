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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/OpContext.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "knowhere/dataset.h"
#include "query/PlanImpl.h"

namespace milvus::segcore {

class ReduceHelper {
 public:
    explicit ReduceHelper(
        std::vector<SearchResult*>& search_results,
        milvus::query::Plan* plan,
        const milvus::query::PlaceholderGroup* placeholder_group,
        int64_t* slice_nqs,
        int64_t* slice_topKs,
        int64_t slice_num,
        tracer::TraceContext* trace_ctx,
        milvus::OpContext* op_ctx = nullptr)
        : search_results_(search_results),
          plan_(plan),
          placeholder_group_(placeholder_group),
          slice_nqs_(slice_nqs, slice_nqs + slice_num),
          slice_topKs_(slice_topKs, slice_topKs + slice_num),
          trace_ctx_(trace_ctx),
          op_ctx_(op_ctx) {
        Initialize();
    }

    // PreReduce prepares per-segment SearchResults for the Go-based reduce
    // pipeline: filter invalid rows, optionally apply Global Refine
    // (truncate + refine), and fill primary keys.
    virtual void
    PreReduce();

    int64_t
    GetAllSearchCount() const {
        int64_t all_search_count = 0;
        for (auto search_result : search_results_) {
            all_search_count += search_result->total_data_cnt_;
        }
        return all_search_count;
    }

 protected:
    virtual void
    FilterInvalidSearchResult(SearchResult* search_result);

    void
    FilterInvalidSearchResults();

    void
    FillPrimaryKey();

    void
    TruncateToRefineTopk();

    void
    TruncateSearchResultForOneNQ(int64_t qi, int64_t topk);

    void
    ResetMergeState();

    void
    RefineDistances();

    bool
    CanUseGlobalRefine() const;

    virtual bool
    IsSearchResultRefineEnabled(SearchResult* search_result) const;

    void
    RefineOneSegment(SearchResult* search_result,
                     FieldId field_id,
                     bool is_cosine,
                     bool is_negated,
                     int64_t dim,
                     int64_t element_size,
                     const char* dense_blob);

    void
    ApplyRefinedOrderForOneNQ(SearchResult* search_result,
                              size_t nq_begin,
                              std::vector<size_t>& indices,
                              const std::vector<float>& new_distances);

 private:
    void
    Initialize();

 protected:
    std::vector<SearchResult*>& search_results_;
    milvus::query::Plan* plan_;
    const milvus::query::PlaceholderGroup* placeholder_group_;
    int64_t num_slices_;
    std::vector<int64_t> slice_nqs_prefix_sum_;
    int64_t num_segments_;
    std::vector<int64_t> slice_topKs_;
    // dim0: num_segments_; dim1: total_nq_; dim2: offset
    std::vector<std::vector<std::vector<int64_t>>> final_search_records_;
    std::vector<int64_t> slice_nqs_;
    int64_t total_nq_;
    tracer::TraceContext* trace_ctx_;
    milvus::OpContext* op_ctx_{nullptr};
};

}  // namespace milvus::segcore
