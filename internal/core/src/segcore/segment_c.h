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
#include "futures/future_c.h"
#include "segcore/plan_c.h"
#include "segcore/load_index_c.h"
#include "segcore/load_field_data_c.h"

typedef void* CSearchResult;
typedef CProto CRetrieveResult;

//////////////////////////////    common interfaces    //////////////////////////////
CStatus
NewSegment(CCollection collection,
           SegmentType seg_type,
           int64_t segment_id,
           CSegmentInterface* newSegment,
           bool is_sorted_by_pk);

void
DeleteSegment(CSegmentInterface c_segment);

void
ClearSegmentData(CSegmentInterface c_segment);

void
DeleteSearchResult(CSearchResult search_result);

CFuture*  // Future<CSearchResultBody>
AsyncSearch(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CSearchPlan c_plan,
            CPlaceholderGroup c_placeholder_group,
            uint64_t timestamp);

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result);

CFuture*  // Future<CRetrieveResult>
AsyncRetrieve(CTraceContext c_trace,
              CSegmentInterface c_segment,
              CRetrievePlan c_plan,
              uint64_t timestamp,
              int64_t limit_size,
              bool ignore_non_pk);

CFuture*  // Future<CRetrieveResult>
AsyncRetrieveByOffsets(CTraceContext c_trace,
                       CSegmentInterface c_segment,
                       CRetrievePlan c_plan,
                       int64_t* offsets,
                       int64_t len);

int64_t
GetMemoryUsageInBytes(CSegmentInterface c_segment);

int64_t
GetRowCount(CSegmentInterface c_segment);

int64_t
GetDeletedCount(CSegmentInterface c_segment);

int64_t
GetRealCount(CSegmentInterface c_segment);

bool
HasRawData(CSegmentInterface c_segment, int64_t field_id);

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
LoadFieldData(CSegmentInterface c_segment,
              CLoadFieldDataInfo load_field_data_info);

CStatus
LoadFieldRawData(CSegmentInterface c_segment,
                 int64_t field_id,
                 const void* data,
                 int64_t row_count);

CStatus
LoadDeletedRecord(CSegmentInterface c_segment,
                  CLoadDeletedRecordInfo deleted_record_info);

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment,
                         CLoadIndexInfo c_load_index_info);

CStatus
UpdateFieldRawDataSize(CSegmentInterface c_segment,
                       int64_t field_id,
                       int64_t num_rows,
                       int64_t field_data_size);

CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id);

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id);

CStatus
AddFieldDataInfoForSealed(CSegmentInterface c_segment,
                          CLoadFieldDataInfo c_load_field_data_info);

CStatus
WarmupChunkCache(CSegmentInterface c_segment,
                 int64_t field_id,
                 bool mmap_enabled);

//////////////////////////////    interfaces for SegmentInterface    //////////////////////////////
CStatus
ExistPk(CSegmentInterface c_segment,
        const uint8_t* raw_ids,
        const uint64_t size,
        bool* results);

CStatus
Delete(CSegmentInterface c_segment,
       int64_t reserved_offset,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps);

void
RemoveFieldFile(CSegmentInterface c_segment, int64_t field_id);

#ifdef __cplusplus
}
#endif
