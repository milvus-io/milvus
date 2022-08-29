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

#include <vector>
#include "Reduce.h"
#include "common/CGoHelper.h"
#include "common/QueryResult.h"
#include "exceptions/EasyAssert.h"
#include "query/Plan.h"
#include "segcore/ReduceStructure.h"
#include "segcore/reduce_c.h"
#include "segcore/Utils.h"

using SearchResult = milvus::SearchResult;

CStatus
ReduceSearchResultsAndFillData(CSearchResultDataBlobs* cSearchResultDataBlobs,
                               CSearchPlan c_plan,
                               CSearchResult* c_search_results,
                               int64_t num_segments,
                               int64_t* slice_nqs,
                               int64_t* slice_topKs,
                               int64_t num_slices) {
    try {
        // get SearchResult and SearchPlan
        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        AssertInfo(num_segments > 0, "num_segments must be greater than 0");
        std::vector<SearchResult*> search_results(num_segments);
        for (int i = 0; i < num_segments; ++i) {
            search_results[i] = static_cast<SearchResult*>(c_search_results[i]);
        }

        auto reduce_helper = milvus::segcore::ReduceHelper(search_results, plan, slice_nqs, slice_topKs, num_slices);
        reduce_helper.Reduce();
        reduce_helper.Marshal();

        // set final result ptr
        *cSearchResultDataBlobs = reduce_helper.GetSearchResultDataBlobs();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
GetSearchResultDataBlob(CProto* searchResultDataBlob,
                        CSearchResultDataBlobs cSearchResultDataBlobs,
                        int32_t blob_index) {
    try {
        auto search_result_data_blobs =
            reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(cSearchResultDataBlobs);
        AssertInfo(blob_index < search_result_data_blobs->blobs.size(), "blob_index out of range");
        searchResultDataBlob->proto_blob = search_result_data_blobs->blobs[blob_index].data();
        searchResultDataBlob->proto_size = search_result_data_blobs->blobs[blob_index].size();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        searchResultDataBlob->proto_blob = nullptr;
        searchResultDataBlob->proto_size = 0;
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

void
DeleteSearchResultDataBlobs(CSearchResultDataBlobs cSearchResultDataBlobs) {
    if (cSearchResultDataBlobs == nullptr) {
        return;
    }
    auto search_result_data_blobs = reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(cSearchResultDataBlobs);
    delete search_result_data_blobs;
}
