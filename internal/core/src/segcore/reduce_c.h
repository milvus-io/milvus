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

#ifdef __cplusplus
extern "C" {
#endif

#include "common/type_c.h"
#include "segcore/plan_c.h"
#include "segcore/segment_c.h"

typedef void* CSearchResultDataBlobs;
typedef void* CSearchStreamReducer;

CStatus
NewStreamReducer(CSearchPlan c_plan,
                 int64_t* slice_nqs,
                 int64_t* slice_topKs,
                 int64_t num_slices,
                 CSearchStreamReducer* stream_reducer);

CStatus
StreamReduce(CSearchStreamReducer c_stream_reducer,
             CSearchResult* c_search_results,
             int64_t num_segments);

CStatus
GetStreamReduceResult(CSearchStreamReducer c_stream_reducer,
                      CSearchResultDataBlobs* c_search_result_data_blobs);

CStatus
ReduceSearchResultsAndFillData(CTraceContext c_trace,
                               CSearchResultDataBlobs* cSearchResultDataBlobs,
                               CSearchPlan c_plan,
                               CSearchResult* search_results,
                               int64_t num_segments,
                               int64_t* slice_nqs,
                               int64_t* slice_topKs,
                               int64_t num_slices);

CStatus
GetSearchResultDataBlob(CProto* searchResultDataBlob,
                        CSearchResultDataBlobs cSearchResultDataBlobs,
                        int32_t blob_index);

void
DeleteSearchResultDataBlobs(CSearchResultDataBlobs cSearchResultDataBlobs);

void
DeleteStreamSearchReducer(CSearchStreamReducer c_stream_reducer);

#ifdef __cplusplus
}
#endif
