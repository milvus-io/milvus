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
#include <stdint.h>
#include "common/type_c.h"
#ifdef __cplusplus
extern "C" {
#endif
struct ArrowSchema;
struct ArrowArray;
// No pointers: safe to pass from Go for the duration of the synchronous call.
typedef struct {
    int64_t field_id;
    int64_t limit;
    int64_t batch_size;
    uint32_t pattern_kind, case_mode, boundary;
    uint32_t has_max_count;
    uint64_t max_regex_bytes, max_dfa_cache_bytes;
    uint64_t max_result_bytes;
    // Per-field matching-line cap, distinct from the Query document limit.
    uint64_t max_count;
    uint64_t before_context, after_context;
} CGrepScanOptions;
// Borrowed only for ScanGrep. Go callers pin the pattern and length buffers
// while this request (which contains pointers to them) is passed through cgo.
typedef struct {
    CGrepScanOptions options;
    const uint8_t* patterns;
    uint64_t pattern_bytes;
    const uint64_t* lengths;
    uint32_t pattern_count;
} CGrepScanRequest;
// Input is a RecordBatch with $seg_offset:Int64; other columns are retained
// for this synchronous call and ignored unless consumed by the operator.
// Import consumes the C descriptors; caller releases any unconsumed descriptors
// on failure. No input buffers are retained after return.
// Patterns are concatenated bytes with one length per pattern. Inputs and
// segment are borrowed through return. Outputs own their Arrow buffers.
// Record columns: positions:Int64 and details:Binary (milvus.data_type=JSON).
// Each Binary row is one document's UTF-8 JSON array of matching/context lines:
// [{"content":"...","line_number":1,"line_start_offset":0,
//   "starts":[...],"ends":[...]}]. Line numbers are one-based; offsets are
// zero-based UTF-8 bytes within the original field; spans remain line-relative.
// Nonmatching context lines have empty starts/ends arrays.
CStatus
ScanGrep(void* segment,
         struct ArrowSchema* input_schema,
         struct ArrowArray* input_array,
         const CGrepScanRequest* request,
         struct ArrowSchema* out_schema,
         struct ArrowArray* out_array,
         void* cancellation_source);
#ifdef __cplusplus
}
#endif
