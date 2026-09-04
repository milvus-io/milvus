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

#include <cstdint>

namespace milvus {

namespace index {
class FieldChunkMetrics;
}

// Narrow, source-agnostic seam for chunk-level skip pruning. Anything that can
// hand out per-chunk FieldChunkMetrics — a Parquet column generation, a Vortex
// column generation, or Milvus-native statistics — implements this. The
// returned pointer is owned by the provider and remains valid for the
// provider's lifetime. A nullptr (or a NoneFieldChunkMetrics) means the chunk
// has no usable statistics and the skip filter must fail open.
class FieldChunkMetricsProvider {
 public:
    virtual ~FieldChunkMetricsProvider() = default;

    virtual const index::FieldChunkMetrics*
    GetSkipMetrics(int64_t chunk_id) const = 0;
};

}  // namespace milvus
