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
    uint64_t max_regex_bytes, max_dfa_cache_bytes;
    uint64_t max_result_bytes;
} CGrepScanOptions;
// Patterns are concatenated bytes with one length per pattern. Inputs and
// segment are borrowed through return. Outputs own their Arrow buffers.
// Record columns: positions:Int64 and details:Binary (milvus.data_type=JSON).
// Each Binary row is one document's UTF-8 JSON array of matching-line objects:
// [{"content":"...","starts":[...],"ends":[...]}].
CStatus
ScanGrepByOffsets(void* segment,
                  const int64_t* offsets,
                  int64_t size,
                  const CGrepScanOptions* options,
                  const uint8_t* patterns,
                  uint64_t pattern_bytes,
                  const uint64_t* lengths,
                  uint32_t pattern_count,
                  struct ArrowSchema* out_schema,
                  struct ArrowArray* out_array,
                  void* cancellation_source);
#ifdef __cplusplus
}
#endif
