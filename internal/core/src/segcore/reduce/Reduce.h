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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>
#include <queue>
#include <unordered_set>

#include "common/type_c.h"
#include "common/QueryResult.h"
#include "query/PlanImpl.h"
#include "segcore/ReduceStructure.h"
#include "common/Tracer.h"
#include "segcore/segment_c.h"

namespace milvus::segcore {

// SearchResultDataBlobs contains the marshal blobs of many `milvus::proto::schema::SearchResultData`
struct SearchResultDataBlobs {
    std::vector<std::vector<char>> blobs;
};

class ReduceHelper {
 public:
    explicit ReduceHelper(std::vector<SearchResult*>& search_results,
                          milvus::query::Plan* plan,
                          int64_t* slice_nqs,
                          int64_t* slice_topKs,
                          int64_t slice_num,
                          tracer::TraceContext* trace_ctx)
        : search_results_(search_results),
          plan_(plan),
          slice_nqs_(slice_nqs, slice_nqs + slice_num),
          slice_topKs_(slice_topKs, slice_topKs + slice_num),
          trace_ctx_(trace_ctx) {
        Initialize();
    }

    void
    Reduce();

    void
    Marshal();

    void*
    GetSearchResultDataBlobs() {
        return search_result_data_blobs_.release();
    }

 protected:
    virtual void
    FilterInvalidSearchResult(SearchResult* search_result);

    void
    RefreshSearchResults();

    virtual void
    RefreshSingleSearchResult(SearchResult* search_result,
                              int seg_res_idx,
                              std::vector<int64_t>& real_topks);

    void
    FillPrimaryKey();

    void
    FillSysData();

    void
    ReduceResultData();

    virtual int64_t
    ReduceSearchResultForOneNQ(int64_t qi,
                               int64_t topk,
                               int64_t& result_offset);

    virtual void
    FillOtherData(int result_count,
                  int64_t nq_begin,
                  int64_t nq_end,
                  std::unique_ptr<milvus::proto::schema::SearchResultData>&
                      search_res_data);

 private:
    void
    Initialize();

    void
    FillEntryData();

    std::vector<char>
    GetSearchResultDataSlice(int slice_index_);

 protected:
    std::vector<SearchResult*>& search_results_;
    milvus::query::Plan* plan_;
    int64_t num_slices_;
    std::vector<int64_t> slice_nqs_prefix_sum_;
    int64_t num_segments_;
    std::vector<int64_t> slice_topKs_;
    // Used for merge results,
    // define these here to avoid allocating them for each query
    std::vector<SearchResultPair> pairs_;
    std::unordered_set<milvus::PkType> pk_set_;
    // dim0: num_segments_; dim1: total_nq_; dim2: offset
    std::vector<std::vector<std::vector<int64_t>>> final_search_records_;
    std::vector<int64_t> slice_nqs_;
    int64_t total_nq_;
    // output
    std::unique_ptr<SearchResultDataBlobs> search_result_data_blobs_;
    tracer::TraceContext* trace_ctx_;
};

}  // namespace milvus::segcore
