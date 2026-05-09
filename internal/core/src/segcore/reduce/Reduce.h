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
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "common/OpContext.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "knowhere/dataset.h"
#include "pb/schema.pb.h"
#include "query/PlanImpl.h"
#include "segcore/ReduceStructure.h"

namespace milvus::segcore {

// SearchResultDataBlobs contains the marshal blobs of many `milvus::proto::schema::SearchResultData`
struct SearchResultDataBlobs {
    std::vector<std::vector<char>> blobs;  // the marshal blobs of each slice
    std::vector<StorageCost> costs;        // the cost of each slice
};

struct ElementSearchResultKey {
    milvus::PkType pk;
    int32_t element_index;

    bool
    operator==(const ElementSearchResultKey& other) const {
        return pk == other.pk && element_index == other.element_index;
    }
};

struct ElementSearchResultKeyHash {
    size_t
    operator()(const ElementSearchResultKey& key) const {
        auto seed = std::hash<milvus::PkType>{}(key.pk);
        seed ^= std::hash<int32_t>{}(key.element_index) + 0x9e3779b9 +
                (seed << 6) + (seed >> 2);
        return seed;
    }
};

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

    virtual void
    SortEqualScoresByPks();

    virtual void
    SortEqualScoresOneNQ(size_t nq_begin,
                         size_t nq_end,
                         SearchResult* search_result);

 private:
    void
    Initialize();

    void
    FillEntryData();

    std::pair<std::vector<char>, StorageCost>
    GetSearchResultDataSlice(const int slice_index,
                             const StorageCost& total_cost);

    bool
    TryAcceptSearchResult(const SearchResultPair& result);

    void
    GetTotalStorageCost();

 protected:
    std::vector<SearchResult*>& search_results_;
    milvus::query::Plan* plan_;
    const milvus::query::PlaceholderGroup* placeholder_group_;
    int64_t num_slices_;
    std::vector<int64_t> slice_nqs_prefix_sum_;
    int64_t num_segments_;
    std::vector<int64_t> slice_topKs_;
    // Used for merge results,
    // define these here to avoid allocating them for each query
    std::vector<SearchResultPair> pairs_;
    std::unordered_set<milvus::PkType> pk_set_;
    std::unordered_set<ElementSearchResultKey, ElementSearchResultKeyHash>
        element_result_set_;
    // dim0: num_segments_; dim1: total_nq_; dim2: offset
    std::vector<std::vector<std::vector<int64_t>>> final_search_records_;
    std::vector<int64_t> slice_nqs_;
    int64_t total_nq_;
    // output
    std::unique_ptr<SearchResultDataBlobs> search_result_data_blobs_;
    tracer::TraceContext* trace_ctx_;
    StorageCost total_search_storage_cost_;
    milvus::OpContext* op_ctx_{nullptr};
};

}  // namespace milvus::segcore
