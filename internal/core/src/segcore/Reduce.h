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

#include "utils/Status.h"
#include "common/type_c.h"
#include "common/QueryResult.h"
#include "query/PlanImpl.h"

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
                          int64_t slice_num)
        : search_results_(search_results),
          plan_(plan),
          slice_nqs_(slice_nqs, slice_nqs + slice_num),
          slice_topKs_(slice_topKs, slice_topKs + slice_num) {
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

 private:
    void
    Initialize();

    void
    FilterInvalidSearchResult(SearchResult* search_result);

    void
    ReduceResultData(int slice_index);

    std::vector<char>
    GetSearchResultDataSlice(int slice_index_, int64_t result_count);

 private:
    std::vector<int64_t> slice_topKs_;
    std::vector<int64_t> slice_nqs_;
    int64_t unify_topK_;
    int64_t total_nq_;
    int64_t num_segments_;
    int64_t num_slices_;

    milvus::query::Plan* plan_;
    std::vector<SearchResult*>& search_results_;

    //
    std::vector<int32_t> nq_slice_offsets_;
    std::vector<std::vector<int64_t>> final_search_records_;
    std::vector<std::vector<int64_t>> final_real_topKs_;

    // output
    std::unique_ptr<SearchResultDataBlobs> search_result_data_blobs_;
};

}  // namespace milvus::segcore
