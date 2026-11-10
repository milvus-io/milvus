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

struct ArrowSchema;
struct ArrowArray;

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

// Borrows CCollection for this call. The explicit empty scope loads no user
// fields except required physical dependencies. Existing constructors retain
// their legacy all-fields interpretation.
CStatus
NewSealedSegmentWithCollection(CCollection collection,
                               int64_t segment_id,
                               CSegmentInterface* output,
                               bool sorted,
                               const uint8_t* load_blob,
                               int64_t load_length,
                               const int64_t* load_fields,
                               int64_t field_count);

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
 * @brief Async load segment using Future mechanism with built-in cancellation
 * @param c_trace: tracing context param
 * @param c_segment: segment handle indicate which segment to load
 * @return CFuture* that resolves when load completes (result pointer is nullptr)
 */
CFuture*
AsyncSegmentLoad(CTraceContext c_trace, CSegmentInterface c_segment);

/**
 * @brief Async reopen segment using Future mechanism with built-in cancellation.
 *
 * Reopen applies a new SegmentLoadInfo and the latest collection schema.
 * schema_blob/schema_length are required so sealed segments compute LoadDiff
 * against the new schema and handle schema changes through the same load-diff
 * framework as manifest/binlog/index updates.
 *
 * @param c_trace tracing context param
 * @param c_segment segment handle to reopen
 * @param load_info_blob serialized SegmentLoadInfo protobuf message
 * @param load_info_length length of load_info_blob in bytes
 * @param schema_blob serialized CollectionSchema protobuf message; must not be null
 * @param schema_length length of schema_blob in bytes; must be positive
 * @param schema_version schema version assigned to the parsed schema
 * @return CFuture* that resolves when reopen completes (result pointer is nullptr)
 */
CFuture*
AsyncReopenSegment(CTraceContext c_trace,
                   CSegmentInterface c_segment,
                   const uint8_t* load_info_blob,
                   const int64_t load_info_length,
                   const void* schema_blob,
                   const int64_t schema_length,
                   const uint64_t schema_version);

// Borrows collection Schema until this Future actually completes (including
// cancellation). The caller must pin both collection and segment for that
// duration. Input blobs/field lists are copied before this function returns.
CFuture*
AsyncReopenSealedSegmentWithCollection(CTraceContext trace,
                                       CSegmentInterface segment,
                                       CCollection collection,
                                       const uint8_t* load_blob,
                                       int64_t load_length,
                                       const int64_t* load_fields,
                                       int64_t load_field_count);

// Return a read-only sealed facade. Retain c_segment until deleting the facade.
CStatus
AdoptSealedSegmentReadState(CSegmentInterface c_segment,
                            CSegmentInterface read_segment);

CStatus
PinSealedSegmentReadState(CSegmentInterface c_segment,
                          CSegmentInterface* read_segment);

void
DeleteSegment(CSegmentInterface c_segment);

void
ClearSegmentData(CSegmentInterface c_segment);

void
DeleteSearchResult(CSearchResult search_result);

/**
 * @brief Get the valid_count from a search result (used for two-stage search)
 * @param search_result: The search result to extract valid_count from
 * @return valid_count (-1 if not set, i.e., normal search mode)
 */
int64_t
GetSearchResultValidCount(CSearchResult search_result);

/**
 * @brief Execute search on a segment
 *
 * @param c_trace: Tracing context for distributed tracing
 * @param c_segment: Segment to search
 * @param c_plan: Search plan containing filter predicates and vector query
 * @param c_placeholder_group: Placeholder group with query vectors (can be NULL if filter_only=true)
 * @param timestamp: MVCC timestamp for consistency
 * @param consistency_level: Consistency level for the query
 * @param collection_ttl: Collection TTL for query context
 * @param filter_only: If true, only execute filter and return valid_count in result (Stage 1 of two-stage search)
 * @param enable_expr_cache: If true, enable expression filter cache for two-stage search
 * @return CFuture* Future that resolves to SearchResult (with valid_count set if filter_only=true)
 */
CFuture*  // Future<CSearchResultBody>
AsyncSearch(CTraceContext c_trace,
            CSegmentInterface c_segment,
            CSearchPlan c_plan,
            CPlaceholderGroup c_placeholder_group,
            uint64_t timestamp,
            int32_t consistency_level,
            uint64_t collection_ttl,
            uint64_t entity_ttl_physical_time_us,
            bool filter_only,
            bool enable_expr_cache);

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

// In QueryScan candidate mode this carries $seg_offset in Arrow and only
// scalar statistics in header. Identity reads additionally carry $id and
// $timestamp. No per-row data is serialized into either header.
//
// For ordinary Query, CRetrieveArrowResult splits one retrieve result in two:
//
//   header       the protobuf RetrieveResults with the *user* output columns
//                removed from fields_data. ids, offset, element_indices, the
//                scalar counters and the system-field columns (RowID,
//                Timestamp) are left exactly as Retrieve() produced them.
//   schema/array the user output columns as an Arrow RecordBatch, exported
//                through the Arrow C Data Interface.
//
// The Arrow fields carry NO per-field metadata. Columns are identified
// positionally instead, from two schema-level keys:
//
//   milvus.field_order        the field ids of the original fields_data, in
//                             their original order, comma separated. Go
//                             rebuilds fields_data to this order, which is
//                             what the protobuf path emits. It cannot be
//                             derived from the plan: FillTargetEntry follows
//                             plan->field_ids_ while FillOrderByResult follows
//                             pipeline_field_ids_ and appends system fields
//                             last.
//   milvus.valid_data_fields  the ids whose source DataArray carried a
//                             valid_data bitmap (see segment_c.cpp).
//
// The Arrow columns are field_order minus the ids retained in the header, in
// field_order sequence, so their ids need not be repeated per field. Dropping
// the per-field `milvus.field_id` / `milvus.data_type` that MilvusField would
// attach avoids decoding and re-cloning them on the Go side
// (cdata.decodeCMetadata, then Arrow's StructOf). In a one-off profile of the
// narrow 4-column shape that was roughly a third of the per-call allocations;
// it is not a committed benchmark, so treat it as an order of magnitude rather
// than a figure. MilvusField itself is unchanged, since the search export
// relies on it.
//
// Leaked by AsyncRetrieveAsArrow; free with DeleteRetrieveArrowResult.
typedef struct CRetrieveArrowResult {
    CRetrieveResult* header;
    struct ArrowSchema* schema;
    struct ArrowArray* array;
} CRetrieveArrowResult;

// AsyncRetrieveAsArrow mirrors AsyncRetrieve, but splits the result as above
// instead of serializing all of it to protobuf.
//
// QueryScan candidate mode exports offsets regardless of ignore_non_pk.
// For ordinary Query, preconditions are enforced by the Go caller
// (shouldUseArrowTransport in querynodev2/segments/retrieve.go):
//   - no aggregates and no group-by fields: aggregation columns carry
//     field_id 0 and are identified positionally, so they cannot be matched
//     by id;
//   - not ignore_non_pk: that withholds every user column, leaving nothing
//     for the Arrow batch.
// count(*) needs no exclusion: it sets no output fields, so the batch is zero
// columns wide and the header carries the answer unchanged.
CFuture*  // Future<CRetrieveArrowResult>
AsyncRetrieveAsArrow(CTraceContext c_trace,
                     CSegmentInterface c_segment,
                     CRetrievePlan c_plan,
                     uint64_t timestamp,
                     int64_t limit_size,
                     int32_t consistency_level,
                     uint64_t collection_ttl,
                     uint64_t entity_ttl_physical_time_us);

void
DeleteRetrieveArrowResult(CRetrieveArrowResult* result);

CFuture*  // Future<CRetrieveResult>
AsyncRetrieveByOffsets(CTraceContext c_trace,
                       CSegmentInterface c_segment,
                       CRetrievePlan c_plan,
                       int64_t* offsets,
                       int64_t len);

// Read PK and timestamp for selected offsets, before reduction.
// Returns Future<CRetrieveArrowResult>: $seg_offset, $id, $timestamp
// Arrow columns, with only scalar statistics in the protobuf header.
CFuture*
AsyncRetrieveIdentitiesByOffsets(CTraceContext c_trace,
                                 CSegmentInterface c_segment,
                                 CRetrievePlan c_plan,
                                 int64_t* offsets,
                                 int64_t len,
                                 bool take_for_output_allowed);

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

// This function is currently used only in test.
// Current implement supports only dropping of non-system fields.
CStatus
DropFieldData(CSegmentInterface c_segment, int64_t field_id);

//////////////////////////////    interfaces for SegmentInterface    //////////////////////////////
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

CStatus
SegmentSetCommitTimestamp(CSegmentInterface c_segment, uint64_t commit_ts);

#ifdef __cplusplus
}
#endif
