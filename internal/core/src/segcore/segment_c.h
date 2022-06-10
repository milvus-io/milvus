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
typedef CProto CRetrieveResult;

//////////////////////////////    common interfaces    //////////////////////////////
CStatus
NewSegment(CCollection collection, SegmentType seg_type, int64_t segment_id, CSegmentInterface* c_segment);

CStatus
DeleteSegment(CSegmentInterface c_segment);

CStatus
DeleteSearchResult(CSearchResult search_result);

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       uint64_t timestamp,
       CSearchResult* result,
       int64_t segment_id);

CStatus
DeleteRetrieveResult(CRetrieveResult* retrieve_result);

CStatus
Retrieve(CSegmentInterface c_segment, CRetrievePlan c_plan, uint64_t timestamp, CRetrieveResult* result);

CStatus
GetMemoryUsageInBytes(CSegmentInterface c_segment, int64_t* mem_size);

CStatus
GetRowCount(CSegmentInterface c_segment, int64_t* row_count);

CStatus
GetDeletedCount(CSegmentInterface c_segment, int64_t* deleted_count);

//////////////////////////////    interfaces for growing segment    //////////////////////////////
CStatus
Insert(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       const uint8_t* data_info,
       const uint64_t data_info_len);

CStatus
PreInsert(CSegmentInterface c_segment, int64_t size, int64_t* offset);

//////////////////////////////    interfaces for sealed segment    //////////////////////////////
CStatus
LoadFieldData(CSegmentInterface c_segment, CLoadFieldDataInfo load_field_data_info);

CStatus
LoadDeletedRecord(CSegmentInterface c_segment, CLoadDeletedRecordInfo deleted_record_info);

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment, CLoadIndexInfo c_load_index_info);

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id);

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id);

//////////////////////////////    interfaces for SegmentInterface    //////////////////////////////
CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps);

CStatus
PreDelete(CSegmentInterface c_segment, int64_t size, int64_t* offset);
#ifdef __cplusplus
}
#endif
