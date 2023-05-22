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
typedef void* CDataArray;

//////////////////////////////    common interfaces    //////////////////////////////
CSegmentInterface
NewSegment(CCollection collection, SegmentType seg_type, int64_t segment_id);

void
DeleteSegment(CSegmentInterface c_segment);

void
DeleteSearchResult(CSearchResult search_result);

CStatus
Search(CSegmentInterface c_segment,
       CSearchPlan c_plan,
       CPlaceholderGroup c_placeholder_group,
       CTraceContext c_trace,
       uint64_t timestamp,
       CSearchResult* result);

CStatus
Retrieve(CSegmentInterface c_segment,
         CRetrievePlan c_plan,
         CTraceContext c_trace,
         uint64_t timestamp,
         CRetrieveResult* result);

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
LoadDeletedRecord(CSegmentInterface c_segment,
                  CLoadDeletedRecordInfo deleted_record_info);

CStatus
UpdateSealedSegmentIndex(CSegmentInterface c_segment,
                         CLoadIndexInfo c_load_index_info);

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

int64_t
PreDelete(CSegmentInterface c_segment, int64_t size);

//////////////////////////////    interfaces for RetrieveResult    //////////////////////////////
void
DeleteRetrieveResult(CRetrieveResult retrieve_result);

bool
RetrieveResultIsCount(CRetrieveResult retrieve_result);

int64_t
GetRetrieveResultRowCount(CRetrieveResult result);

int64_t
GetRetrieveResultFieldSize(CRetrieveResult retrieve_result);

CStatus
GetRetrieveResultOffsets(CRetrieveResult result, int64_t* dest, int64_t size);

bool
RetrieveResultHasIds(CRetrieveResult retrieve_result);

CDataType
GetRetrieveResultPkType(CRetrieveResult result);

CStatus
GetRetrieveResultFieldMeta(CRetrieveResult retrieve_result,
                           int64_t index,
                           CFieldMeta* meta);

CStatus
GetRetrieveResultPkDataForInt(CRetrieveResult retrieve_result,
                              int64_t* data,
                              int64_t size);

CStatus
GetRetrieveResultPkDataForString(CRetrieveResult retrieve_result,
                                 char** data,
                                 int64_t size);

CStatus
GetRetrieveResultFieldDataForBool(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  bool* data,
                                  int64_t size);
CStatus
GetRetrieveResultFieldDataForInt(CRetrieveResult retrieve_result,
                                 int64_t index,
                                 int32_t* data,
                                 int64_t size);
CStatus
GetRetrieveResultFieldDataForLong(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  int64_t* data,
                                  int64_t size);

CStatus
GetRetrieveResultFieldDataForFloat(CRetrieveResult retrieve_result,
                                   int64_t index,
                                   float* data,
                                   int64_t size);

CStatus
GetRetrieveResultFieldDataForDouble(CRetrieveResult retrieve_result,
                                    int64_t index,
                                    double* data,
                                    int64_t size);

CStatus
GetRetrieveResultFieldDataForVarChar(CRetrieveResult retrieve_result,
                                     int64_t index,
                                     char** data,
                                     int64_t size);
CStatus
GetRetrieveResultFieldDataForFloatVector(CRetrieveResult retrieve_result,
                                         int64_t index,
                                         float* data,
                                         int64_t dim,
                                         int64_t size);

CStatus
GetRetrieveResultFieldDataForBinaryVector(CRetrieveResult retrieve_result,
                                          int64_t index,
                                          char* data,
                                          int64_t dim,
                                          int64_t size);

CStatus
GetRetrieveResultFieldDataForJson(CRetrieveResult retrieve_result,
                                  int64_t index,
                                  char** data,
                                  int64_t size);

#ifdef __cplusplus
}
#endif
