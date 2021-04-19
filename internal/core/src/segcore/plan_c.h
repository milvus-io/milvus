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

typedef void* CPlan;
typedef void* CPlaceholderGroup;

CPlan
CreatePlan(CCollection col, const char* dsl);

CPlaceholderGroup
ParsePlaceholderGroup(CPlan plan, void* placeholder_group_blob, int64_t blob_size);

int64_t
GetNumOfQueries(CPlaceholderGroup placeholder_group);

int64_t
GetTopK(CPlan plan);

void
DeletePlan(CPlan plan);

void
DeletePlaceholderGroup(CPlaceholderGroup placeholder_group);

#ifdef __cplusplus
}
#endif
