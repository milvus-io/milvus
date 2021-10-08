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
#include "segcore/collection_c.h"
#include "common/type_c.h"

typedef void* CSearchPlan;
typedef void* CPlaceholderGroup;
typedef void* CRetrievePlan;

CStatus
CreateSearchPlan(CCollection col, const char* dsl, CSearchPlan* res_plan);

// Note: serialized_expr_plan is of binary format
CStatus
CreateSearchPlanByExpr(CCollection col, const char* serialized_expr_plan, int64_t size, CSearchPlan* res_plan);

CStatus
ParsePlaceholderGroup(CSearchPlan plan,
                      void* placeholder_group_blob,
                      int64_t blob_size,
                      CPlaceholderGroup* res_placeholder_group);

int64_t
GetNumOfQueries(CPlaceholderGroup placeholder_group);

int64_t
GetTopK(CSearchPlan plan);

const char*
GetMetricType(CSearchPlan plan);

void
DeleteSearchPlan(CSearchPlan plan);

void
DeletePlaceholderGroup(CPlaceholderGroup placeholder_group);

CStatus
CreateRetrievePlan(CCollection c_col, CProto retrieve_request, CRetrievePlan* output);

CStatus
CreateRetrievePlanByExpr(CCollection c_col, const char* serialized_expr_plan, int64_t size, CRetrievePlan* res_plan);

void
DeleteRetrievePlan(CRetrievePlan plan);

#ifdef __cplusplus
}
#endif
