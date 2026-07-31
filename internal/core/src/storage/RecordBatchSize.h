// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "arrow/record_batch.h"
#include "arrow/util/byte_size.h"

namespace milvus::storage {

// Returns the bytes referenced by this record-batch slice. Unlike
// TotalBufferSize, this accounts for array offsets and does not charge every
// slice for its entire shared backing buffer.
//
// ReferencedBufferSize cannot walk every layout. On the pinned arrow 17 its
// byte-range visitor has no overload for STRING_VIEW / BINARY_VIEW /
// LIST_VIEW and falls through to a TypeError catch-all, which
// ReferencedBufferSize(RecordBatch) propagates for the whole batch -- and
// external (vortex schemaless) readers do produce those layouts, on the
// pre-normalization batch this is called with. Fall back to the plain buffer
// sum, which works for any type. It over-charges slices that share a backing
// buffer, but for a memory cap over-charging is the safe direction: the row
// count would charge a 64MB batch as 8192 bytes and silently disable byte
// backpressure altogether, leaving only the batch-count limit.
inline int64_t
EstimateRecordBatchBytes(const arrow::RecordBatch& batch) {
    auto size_result = arrow::util::ReferencedBufferSize(batch);
    if (size_result.ok() && size_result.ValueOrDie() > 0) {
        return size_result.ValueOrDie();
    }
    auto total_bytes = arrow::util::TotalBufferSize(batch);
    return total_bytes > 0 ? total_bytes : batch.num_rows();
}

}  // namespace milvus::storage
