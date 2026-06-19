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
namespace exec {

// Runtime-dispatched SIMD filter for TermExpr IN evaluation.
// Checks each data[i] against sorted vals[], OR's matching bits into bitmap.
// bitmap must be byte-aligned (no bit offset). vals must be sorted and
// deduplicated. At startup, the best available instruction set is selected
// (AVX-512 > AVX2 > SSE2 on x86; NEON on ARM).
template <typename T>
void
simdFilterChunk(
    const T* data, int size, uint8_t* bitmap, const T* vals, int num_vals);

// Returns the SIMD lane count for type T on the best available instruction set.
// E.g. AVX2 + int32 → 8, NEON + int32 → 4.
template <typename T>
int
simdLaneCount();

}  // namespace exec
}  // namespace milvus
