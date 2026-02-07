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

#include <exception>
#include <memory>
#include <optional>
#include <vector>

#include "common/EasyAssert.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Tracer.h"
#include "monitor/scope_metric.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "segcore/ReduceStructure.h"
#include "segcore/reduce/GroupReduce.h"
#include "segcore/reduce/Reduce.h"
#include "segcore/reduce_c.h"

using SearchResult = milvus::SearchResult;

CStatus
ReduceSearchResultsAndFillData(CTraceContext c_trace,
                               CSearchResultDataBlobs* cSearchResultDataBlobs,
                               CSearchPlan c_plan,
                               CSearchResult* c_search_results,
                               int64_t num_segments,
                               int64_t* slice_nqs,
                               int64_t* slice_topKs,
                               int64_t num_slices) {
    SCOPE_CGO_CALL_METRIC();

    try {
        // get SearchResult and SearchPlan
        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        AssertInfo(num_segments > 0, "num_segments must be greater than 0");
        auto trace_ctx = milvus::tracer::TraceContext{
            c_trace.traceID, c_trace.spanID, c_trace.traceFlags};
        milvus::tracer::AutoSpan span(
            "ReduceSearchResultsAndFillData", &trace_ctx, true);
        std::vector<SearchResult*> search_results(num_segments);
        for (int i = 0; i < num_segments; ++i) {
            search_results[i] = static_cast<SearchResult*>(c_search_results[i]);
        }

        std::shared_ptr<milvus::segcore::ReduceHelper> reduce_helper;
        if (plan->plan_node_->search_info_.group_by_field_id_.has_value()) {
            reduce_helper =
                std::make_shared<milvus::segcore::GroupReduceHelper>(
                    search_results,
                    plan,
                    slice_nqs,
                    slice_topKs,
                    num_slices,
                    &trace_ctx);
        } else {
            reduce_helper =
                std::make_shared<milvus::segcore::ReduceHelper>(search_results,
                                                                plan,
                                                                slice_nqs,
                                                                slice_topKs,
                                                                num_slices,
                                                                &trace_ctx);
        }
        reduce_helper->Reduce();
        reduce_helper->Marshal();

        // set final result ptr
        *cSearchResultDataBlobs = reduce_helper->GetSearchResultDataBlobs();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
GetSearchResultDataBlob(CProto* searchResultDataBlob,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes,
                        CSearchResultDataBlobs cSearchResultDataBlobs,
                        int32_t blob_index) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto search_result_data_blobs =
            reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(
                cSearchResultDataBlobs);
        AssertInfo(blob_index < search_result_data_blobs->blobs.size(),
                   "blob_index out of range");
        searchResultDataBlob->proto_blob =
            search_result_data_blobs->blobs[blob_index].data();
        searchResultDataBlob->proto_size =
            search_result_data_blobs->blobs[blob_index].size();
        *scanned_remote_bytes =
            search_result_data_blobs->costs[blob_index].scanned_remote_bytes;
        *scanned_total_bytes =
            search_result_data_blobs->costs[blob_index].scanned_total_bytes;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        searchResultDataBlob->proto_blob = nullptr;
        searchResultDataBlob->proto_size = 0;
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteSearchResultDataBlobs(CSearchResultDataBlobs cSearchResultDataBlobs) {
    SCOPE_CGO_CALL_METRIC();

    if (cSearchResultDataBlobs == nullptr) {
        return;
    }
    auto search_result_data_blobs =
        reinterpret_cast<milvus::segcore::SearchResultDataBlobs*>(
            cSearchResultDataBlobs);
    delete search_result_data_blobs;
}
