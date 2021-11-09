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

#include <stdbool.h>
#include <stdint.h>

#include "common/type_c.h"
#include "segcore/plan_c.h"
#include "segcore/segment_c.h"

typedef void* CMarshaledHits;

void
DeleteMarshaledHits(CMarshaledHits c_marshaled_hits);

int
MergeInto(int64_t num_queries, int64_t topk, float* distances, int64_t* uids, float* new_distances, int64_t* new_uids);

CStatus
ReduceSearchResultsAndFillData(CSearchPlan c_plan, CSearchResult* search_results, int64_t num_segments);

CStatus
ReorganizeSearchResults(CMarshaledHits* c_marshaled_hits, CSearchResult* c_search_results, int64_t num_segments);

int64_t
GetHitsBlobSize(CMarshaledHits c_marshaled_hits);

void
GetHitsBlob(CMarshaledHits c_marshaled_hits, const void* hits);

int64_t
GetNumQueriesPerGroup(CMarshaledHits c_marshaled_hits, int64_t group_index);

void
GetHitSizePerQueries(CMarshaledHits c_marshaled_hits, int64_t group_index, int64_t* hit_size_peer_query);

#ifdef __cplusplus
}
#endif
