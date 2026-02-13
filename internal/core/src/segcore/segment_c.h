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

#include <stdint.h>
#include <stdbool.h>

#include "common/common_type_c.h"
#include "common/type_c.h"
#include "futures/future_c_types.h"
#include "segcore/collection_c.h"
#include "segcore/load_field_data_c.h"
#include "segcore/load_index_c.h"
#include "segcore/plan_c.h"

typedef void* CSearchResult;
typedef CProto CRetrieveResult;

//////////////////////////////    common interfaces    //////////////////////////////
CStatus
NewSegment(CCollection collection,
           SegmentType seg_type,
           int64_t segment_id,
           CSegmentInterface* newSegment,
           bool is_sorted_by_pk);

/**
 * @brief Create a new segment with pre-loaded segment information
 * This function creates a segment and initializes it with serialized load info,
 * which can include precomputed metadata, statistics, or configuration data
 *
 * @param collection: The collection that this segment belongs to
 * @param seg_type: Type of the segment (growing, sealed, etc.)
 * @param segment_id: Unique identifier for this segment
 * @param newSegment: Output parameter for the created segment interface
 * @param is_sorted_by_pk: Whether the segment data is sorted by primary key
 * @param load_info_blob: Serialized load information blob
 * @param load_info_length: Length of the load_info_blob in bytes
 * @return CStatus indicating success or failure
 */
CStatus
NewSegmentWithLoadInfo(CCollection collection,
                       SegmentType seg_type,
                       int64_t segment_id,
                       CSegmentInterface* newSegment,
                       bool is_sorted_by_pk,
                       const uint8_t* load_info_blob,
                       const int64_t load_info_length);
/**
 * @brief Dispatch a segment manage load task.
 * This function make segment itself load index & field data according to load info previously set.
 *
 * @param c_trace: tracing context param
 * @param c_segment: segment handle indicate which segment to load
 * @return CStatus indicating success or failure
 */
/**
 * @brief Opaque handle to a cancellation source for load operations
 */
typedef void* CLoadCancellationSource;

/**
 * @brief Create a new cancellation source for load operations
 * @return Handle to the cancellation source
 */
CLoadCancellationSource
NewLoadCancellationSource();

/**
 * @brief Request cancellation through the source
 * @param source: The cancellation source handle
 */
void
CancelLoadCancellationSource(CLoadCancellationSource source);

/**
 * @brief Release the cancellation source
 * @param source: The cancellation source handle to release
 */
void
ReleaseLoadCancellationSource(CLoadCancellationSource source);

/**
 * @brief Load segment with cancellation support
 * @param c_trace: tracing context param
 * @param c_segment: segment handle indicate which segment to load
 * @param source: cancellation source for cancelling the load operation (can be NULL)
 * @return CStatus indicating success or failure
 */
CStatus
SegmentLoad(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CLoadCancellationSource source);

/**
 * @brief Reopen an existing segment with updated load information
 *
 * This function reopens a segment with new load configuration, typically used
 * when the segment needs to be reconfigured due to schema changes or updated
 * load parameters. The segment will be reinitialized with the provided load info
 * while preserving its identity (segment_id).
 *
 * @param c_trace Tracing context for distributed tracing and debugging
 * @param c_segment The segment handle to be reopened
 * @param load_info_blob Serialized SegmentLoadInfo protobuf message containing
 *                       the new load configuration (field data info, index info, etc.)
 * @param load_info_length Length of the load_info_blob in bytes
 * @return CStatus indicating success or failure with error details
 */
CStatus
ReopenSegment(CTraceContext c_trace,
              CSegmentInterface c_segment,
              const uint8_t* load_info_blob,
              const int64_t load_info_length);

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
            uint64_t timestamp,
            int32_t consistency_level,
            uint64_t collection_ttl,
            uint64_t entity_ttl_physical_time_us);

void
DeleteRetrieveResult(CRetrieveResult* retrieve_result);

CFuture*  // Future<CRetrieveResult>
AsyncRetrieve(CTraceContext c_trace,
              CSegmentInterface c_segment,
              CRetrievePlan c_plan,
              uint64_t timestamp,
              int64_t limit_size,
              bool ignore_non_pk,
              int32_t consistency_level,
              uint64_t collection_ttl,
              uint64_t entity_ttl_physical_time_us);

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

bool
HasFieldData(CSegmentInterface c_segment, int64_t field_id);

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
LoadJsonKeyIndex(CTraceContext c_trace,
                 CSegmentInterface c_segment,
                 const uint8_t* serialied_load_json_key_index_info,
                 const uint64_t len,
                 CLoadCancellationSource source);

CStatus
UpdateFieldRawDataSize(CSegmentInterface c_segment,
                       int64_t field_id,
                       int64_t num_rows,
                       int64_t field_data_size);

// This function is currently used only in test.
// Current implement supports only dropping of non-system fields.
CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id);

CStatus
DropSealedSegmentIndex(CSegmentInterface c_segment, int64_t field_id);

CStatus
DropSealedSegmentJSONIndex(CSegmentInterface c_segment,
                           int64_t field_id,
                           const char* nested_path);

//////////////////////////////    interfaces for SegmentInterface    //////////////////////////////
CStatus
ExistPk(CSegmentInterface c_segment,
        const uint8_t* raw_ids,
        const uint64_t size,
        bool* results);

CStatus
Delete(CSegmentInterface c_segment,
       int64_t size,
       const uint8_t* ids,
       const uint64_t ids_size,
       const uint64_t* timestamps);

void
RemoveFieldFile(CSegmentInterface c_segment, int64_t field_id);

CStatus
ExprResCacheEraseSegment(int64_t segment_id);

#ifdef __cplusplus
}
#endif
