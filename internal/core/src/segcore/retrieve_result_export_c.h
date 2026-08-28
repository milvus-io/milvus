// Copyright (C) 2019-2026 Zilliz. All rights reserved.
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

struct ArrowSchema;
struct ArrowArray;

// Read output fields from multiple segments and export one Arrow
// RecordBatch in the caller-provided row order.
//
// segments[seg_indices[i]] provides the data for output row i,
// at the segment-internal offset seg_offsets[i].
//
// Internally: groups rows by segment, calls bulk_subscript per
// segment, merges via MergeDataArray into the requested order,
// converts to Arrow via BuildExplicitFieldsBatch, and exports
// via arrow::ExportRecordBatch.
//
// Caller owns out_schema/out_array and must release them through
// the Arrow C Data Interface.
CStatus
FillRetrieveFieldsOrdered(CSegmentInterface* segments,
                          int64_t num_segments,
                          CRetrievePlan c_plan,
                          const int32_t* seg_indices,
                          const int64_t* seg_offsets,
                          int64_t total_rows,
                          struct ArrowSchema* out_schema,
                          struct ArrowArray* out_array,
                          void* cancellation_source);

#ifdef __cplusplus
}
#endif
