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

//////////////////////////////    interfaces for growing segment flush    //////////////////////////////

/**
 * @brief Configuration for flushing growing segment data to storage.
 */
typedef struct CFlushConfig {
    const char* segment_path;  // base path for segment manifest and data
    const void* schema_blob;  // serialized CollectionSchema for this flush task
    int64_t schema_length;    // length of schema_blob in bytes
    int64_t read_version;     // version to read (-1 = latest)
    uint32_t retry_limit;     // retry limit for commit
    const char* writer_format;          // writer.format
    const char* schema_based_pattern;   // writer.split.schema_based.patterns
    const char* schema_based_formats;   // writer.split.schema_based.formats
    int64_t* allowed_field_ids;         // projected field IDs to flush
    size_t num_allowed_fields;          // number of projected fields
    int64_t* column_group_ids;          // column group IDs to summarize
    int64_t* column_group_field_ids;    // flattened field IDs per column group
    size_t* column_group_field_counts;  // field count per column group
    size_t num_column_groups;           // number of column groups
    // TEXT column configurations
    int64_t* text_field_ids;      // array of TEXT field IDs
    const char** text_lob_paths;  // array of LOB paths for each TEXT field
    int64_t text_inline_threshold;
    int64_t text_max_lob_file_bytes;
    int64_t text_flush_threshold_bytes;
    size_t num_text_columns;       // number of TEXT columns
    int64_t* bm25_field_ids;       // array of BM25 sparse output field IDs
    int64_t* bm25_stats_log_ids;   // array of BM25 stats log IDs
    size_t num_bm25_fields;        // number of BM25 output fields
    bool write_merged_bm25_stats;  // whether to write compound BM25 stats
} CFlushConfig;

/**
 * @brief Result of flushing growing segment data.
 */
typedef struct CFlushResult {
    char* manifest_path;  // path to the committed manifest (caller must free)
    int64_t committed_version;   // committed version number
    int64_t num_rows;            // number of rows flushed
    uint64_t timestamp_from;     // minimum row timestamp flushed
    uint64_t timestamp_to;       // maximum row timestamp flushed
    int64_t* field_ids;          // field ids for per-field flush summaries
    int64_t* field_null_counts;  // null count per field
    size_t num_field_stats;      // number of field summary entries
    int64_t*
        flushed_field_ids;  // exact set of columns actually written; skipped
                            // non-materialized function outputs are absent
    size_t num_flushed_fields;  // number of entries in flushed_field_ids
    int64_t* column_group_ids;  // column group ids for binlog summaries
    int64_t*
        column_group_memory_sizes;  // uncompressed Arrow data size per column group
    size_t num_column_groups;       // number of column group summary entries
    int64_t* bm25_field_ids;        // field ids for serialized BM25 stats
    uint8_t** bm25_stats;           // serialized BM25 stats per field
    size_t* bm25_stats_sizes;       // serialized BM25 stats sizes
    size_t num_bm25_stats;          // number of BM25 stats entries
} CFlushResult;

/**
 * @brief Flush data from a growing segment directly to storage.
 *
 * This function extracts data from the growing segment's ConcurrentVector
 * and writes it to storage via milvus-storage. It handles:
 * - Extracting raw field data from InsertRecord
 * - Converting to Arrow RecordBatch
 * - Writing to storage with TEXT column LOB handling
 * - Creating manifest with committed version
 *
 * @param c_segment The growing segment to flush
 * @param start_offset Start row offset (inclusive)
 * @param end_offset End row offset (exclusive)
 * @param config Flush configuration
 * @param result Output flush result (caller must free manifest_path)
 * @return CStatus indicating success or failure
 */
/**
 * @brief Get the field ids with materialized columns in a growing segment.
 * The flush layout must be derived from this set. Caller frees *field_ids
 * with free().
 */
CStatus
GetGrowingSegmentMaterializedFieldIDs(CSegmentInterface c_segment,
                                      int64_t** field_ids,
                                      int64_t* count);

CStatus
FlushGrowingSegmentData(CSegmentInterface c_segment,
                        int64_t start_offset,
                        int64_t end_offset,
                        const CFlushConfig* config,
                        CFlushResult* result);

/**
 * @brief Free the manifest_path in CFlushResult.
 */
void
FreeFlushResult(CFlushResult* result);

CStatus
SegmentSetCommitTimestamp(CSegmentInterface c_segment, uint64_t commit_ts);

#ifdef __cplusplus
}
#endif
