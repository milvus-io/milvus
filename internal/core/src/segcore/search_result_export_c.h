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

#include "common/type_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"

// Forward declarations for Arrow C Data Interface
struct ArrowSchema;
struct ArrowArray;

// Export a per-segment SearchResult as one full Arrow RecordBatch for the
// segment. The caller receives the per-NQ row counts in out_chunk_sizes and
// must free them with free(). Caller owns out_schema/out_array and must release
// them through the Arrow C Data Interface when imported/consumed.
// cancellation_source may be null; otherwise it must point to a
// folly::CancellationSource created by NewLoadCancellationSource().
CStatus
ExportSearchResultAsArrowRecordBatch(CSearchResult c_search_result,
                                     CSearchPlan c_plan,
                                     const int64_t* extra_field_ids,
                                     int64_t num_extra_fields,
                                     struct ArrowSchema* out_schema,
                                     struct ArrowArray* out_array,
                                     int64_t** out_chunk_sizes,
                                     int64_t* out_num_chunks,
                                     void* cancellation_source);

// Fill output fields for multiple segments in a single call, producing
// results in the specified output order.
//
// This replaces per-segment field materialization plus Go-side scatter.
// The caller provides the reduce result as parallel arrays:
//   result_seg_indices[i] = which search_results[] this row came from
//   result_seg_offsets[i] = seg_offset within that segment
// The output proto FieldsData is assembled in the order of these arrays.
//
// Internally: groups by segment, calls FillTargetEntry per segment,
// then uses MergeDataArray to produce the correctly-ordered output.
// Writes a serialized SearchResultData proto into out_result. Caller owns
// out_result->proto_blob and must free it after use when proto_size > 0.
CStatus
FillOutputFieldsOrdered(CSearchResult* search_results,
                        int64_t num_search_results,
                        CSearchPlan c_plan,
                        const int32_t* result_seg_indices,
                        const int64_t* result_seg_offsets,
                        int64_t total_rows,
                        CProto* out_result,
                        void* cancellation_source);

// Run the pre-export phase of reduce across all per-segment SearchResults:
// filter invalid rows, optionally apply Global Refine (truncate + refine),
// and fill primary keys. Mutates the passed SearchResults in place; the
// Go-side pipeline then exports the prepared results via
// ExportSearchResultAsArrowRecordBatch.
//
// Internally constructs a ReduceHelper and calls helper.PreReduce(). When
// global refine is enabled (plan's search_info has non-zero ratios) and at
// least one segment's index reports IsIndexRefineEnabled, this runs the
// truncate + refine pipeline using the query vectors in c_placeholder_group.
// Otherwise behaves as filter + fill_pk only.
// Writes the sum of total_data_cnt_ values into all_search_count when non-null.
CStatus
PrepareSearchResultsForExport(CTraceContext c_trace,
                              CSearchPlan c_plan,
                              CPlaceholderGroup c_placeholder_group,
                              CSearchResult* c_search_results,
                              int64_t num_segments,
                              int64_t* slice_nqs,
                              int64_t num_slices,
                              int64_t* slice_topKs,
                              int64_t* all_search_count,
                              void* cancellation_source);

// Read post-search metadata from a SearchResult in a single CGO call.
// All four outputs are populated unconditionally:
//   - has_group_by: true when the plan enabled group-by and the
//     SearchGroupByNode populated composite_group_by_values_ in parallel with
//     seg_offsets_/distances_
//   - group_size: the configured per-group cap (0 when group-by is disabled)
//   - scanned_remote_bytes / scanned_total_bytes: storage cost accumulated by
//     the segment search itself, by ExportSearchResultAsArrowRecordBatch when
//     reading extra fields, and by FillOutputFieldsOrdered during late
//     materialization. Caller should invoke this after all those phases.
void
GetSearchResultMetadata(CSearchResult c_search_result,
                        bool* has_group_by,
                        int64_t* group_size,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes);

// Export bounded Milvus-owned C++ storage observations accumulated while
// materializing this SearchResult. durations must have capacity for at least
// duration_capacity uint64_t values; out_count never exceeds that capacity.
void
GetSearchResultStorageProfile(CSearchResult c_search_result,
                              uint64_t* durations,
                              int64_t duration_capacity,
                              int64_t* out_count,
                              uint64_t* completed_bytes,
                              uint64_t* dropped_observations);

#ifdef __cplusplus
}
#endif
