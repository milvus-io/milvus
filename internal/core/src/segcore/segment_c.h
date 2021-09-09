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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>

#include "common/type_c.h"
#include "segcore/plan_c.h"
#include "segcore/load_index_c.h"

typedef void* CSegmentInterface;
typedef void* CSearchResult;
typedef void* CRetrieveResult;

//////////////////////////////    common interfaces    //////////////////////////////
CSegmentInterface
NewSegment(CCollection collection, uint64_t segment_id, SegmentType seg_type);

void
DeleteSegment(CSegmentInterface c_segment);

void
DeleteSearchResult(CSearchResult search_result);

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       uint64_t timestamp,
       CSearchResult* result);

CProtoResult
Retrieve(CSegmentInterface c_segment, CRetrievePlan c_plan, uint64_t timestamp);

int64_t
GetMemoryUsageInBytes(CSegmentInterface c_segment);

int64_t
GetRowCount(CSegmentInterface c_segment);

int64_t
GetDeletedCount(CSegmentInterface c_segment);

//////////////////////////////    interfaces for growing segment    //////////////////////////////
CStatus
Insert(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       void* raw_data,
       int sizeof_per_row,
       int64_t count);

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset);

CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps);

int64_t
PreDelete(CSegmentInterface c_segment, int64_t size);

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info);

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment, CLoadIndexInfo c_load_index_info);

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id);

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id);

#ifdef __cplusplus
}
#endif
