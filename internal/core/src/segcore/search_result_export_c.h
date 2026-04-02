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

// Forward declaration for Arrow C Stream Interface
struct ArrowArrayStream;

// Export a per-segment SearchResult as a stream of Arrow RecordBatches,
// one RecordBatch per NQ query, via the Arrow C Stream Interface.
//
// This stream export pre-splits the results by NQ, so the Go side can consume
// each NQ's batch directly without needing to parse topk_per_nq metadata and
// slice arrays.
//
// Each RecordBatch has columns: $id (int64/string), $score (float32),
// $seg_offset (int64), plus optional $group_by_<fieldID> columns when group-by
// is enabled, optional $element_indices for element-level search, and any extra
// fields requested via extra_field_ids.
// The group-by and extra-field Arrow fields carry Milvus field_id/data_type
// metadata and preserve field nullability, so Go import can reconstruct the
// DataFrame field metadata and null bitmap.
// The number of RecordBatches equals the number of NQ queries. Empty NQs
// produce a 0-row RecordBatch.
//
// extra_field_ids: optional array of field IDs to export (e.g., fields needed by L0 rerank).
//                  Pass NULL with num_extra_fields=0 if no extra fields are needed.
// out_stream must point to a caller-allocated ArrowArrayStream struct.
//
// Precondition: caller must have run PrepareSearchResultsForExport first, so
// topk_per_nq_prefix_sum_ is populated and primary_keys_ is filled. This
// function only builds the Arrow stream from the already-prepared result.
CStatus
ExportSearchResultAsArrowStream(CSearchResult c_search_result,
                                CSearchPlan c_plan,
                                const int64_t* extra_field_ids,
                                int64_t num_extra_fields,
                                struct ArrowArrayStream* out_stream);

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
CStatus
FillOutputFieldsOrdered(CSearchResult* search_results,
                        int64_t num_search_results,
                        CSearchPlan c_plan,
                        const int32_t* result_seg_indices,
                        const int64_t* result_seg_offsets,
                        int64_t total_rows,
                        CProto* out_result);

// Run the pre-export phase of reduce across all per-segment SearchResults:
// filter invalid rows, optionally apply Global Refine (truncate + refine),
// and fill primary keys. Mutates the passed SearchResults in place; the
// Go-side pipeline then exports the prepared results via
// ExportSearchResultAsArrowStream.
//
// Internally constructs a ReduceHelper and calls helper.PreReduce(). When
// global refine is enabled (plan's search_info has non-zero ratios) and at
// least one segment's index reports IsIndexRefineEnabled, this runs the
// truncate + refine pipeline using the query vectors in c_placeholder_group.
// Otherwise behaves as filter + fill_pk only.
CStatus
PrepareSearchResultsForExport(CTraceContext c_trace,
                              CSearchPlan c_plan,
                              CPlaceholderGroup c_placeholder_group,
                              CSearchResult* c_search_results,
                              int64_t num_segments,
                              int64_t* slice_nqs,
                              int64_t num_slices,
                              int64_t* slice_topKs,
                              int64_t* all_search_count);

// Read post-search metadata from a SearchResult in a single CGO call.
// All four outputs are populated unconditionally:
//   - has_group_by: true when the plan enabled group-by and the
//     SearchGroupByNode populated composite_group_by_values_ in parallel with
//     seg_offsets_/distances_
//   - group_size: the configured per-group cap (0 when group-by is disabled)
//   - scanned_remote_bytes / scanned_total_bytes: storage cost accumulated by
//     the segment search itself, by ExportSearchResultAsArrowStream when
//     reading extra fields, and by FillOutputFieldsOrdered during late
//     materialization. Caller should invoke this after all those phases.
void
GetSearchResultMetadata(CSearchResult c_search_result,
                        bool* has_group_by,
                        int64_t* group_size,
                        int64_t* scanned_remote_bytes,
                        int64_t* scanned_total_bytes);

#ifdef __cplusplus
}
#endif
