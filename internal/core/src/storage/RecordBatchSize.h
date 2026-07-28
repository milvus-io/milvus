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
// slice for its entire shared backing buffer. Falls back to the row count if
// Arrow cannot compute the referenced ranges.
inline int64_t
EstimateRecordBatchBytes(const arrow::RecordBatch& batch) {
    auto size_result = arrow::util::ReferencedBufferSize(batch);
    if (!size_result.ok()) {
        return batch.num_rows();
    }
    auto referenced_bytes = size_result.ValueOrDie();
    return referenced_bytes > 0 ? referenced_bytes : batch.num_rows();
}

}  // namespace milvus::storage
