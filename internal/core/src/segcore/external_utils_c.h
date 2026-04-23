// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include "common/type_c.h"
#include "milvus-storage/ffi_c.h"

typedef struct {
    const char* field_name;  // external column name
    int64_t
        avg_mem_bytes;  // per-row average memory size (Arrow buffer, decompressed)
} CFieldMemSize;

typedef struct {
    CFieldMemSize* sizes;
    int count;
} CFieldMemSizeList;

// Sample rows from an external segment via Take API and return per-field
// average memory size. The returned avg_mem_bytes is the Arrow buffer size
// (decompressed), equivalent to Binlog.MemorySize for internal segments.
//
// manifest_path: the segment's manifest path (JSON with base_path and ver)
// sample_rows: number of rows to sample (capped to actual row count)
// collection_id: collection ID for extfs property resolution
// properties: storage properties (constructed by Go caller, includes extfs overrides)
// out: per-field size output, caller must free with FreeCFieldMemSizeList
CStatus
SampleExternalSegmentFieldSizes(const char* manifest_path,
                                int sample_rows,
                                int64_t collection_id,
                                const LoonProperties* properties,
                                CFieldMemSizeList* out);

void
FreeCFieldMemSizeList(CFieldMemSizeList* list);

#ifdef __cplusplus
}
#endif
