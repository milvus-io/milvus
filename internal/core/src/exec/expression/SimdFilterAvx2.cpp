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

// Compiled with: -mavx2 -mfma (set by CMakeLists.txt)

#if defined(__x86_64__)

#include "SimdFilterImpl.h"

namespace milvus {
namespace exec {
namespace avx2 {

template <typename T>
void
filterChunk(
    const T* data, int size, uint8_t* bitmap, const T* vals, int num_vals) {
    detail::filterChunkImpl<T, xsimd::avx2>(data, size, bitmap, vals, num_vals);
}

template <typename T>
int
laneCount() {
    return xsimd::batch<T, xsimd::avx2>::size;
}

#define INSTANTIATE(T)                                                    \
    template void filterChunk<T>(const T*, int, uint8_t*, const T*, int); \
    template int laneCount<T>();

INSTANTIATE(int8_t)
INSTANTIATE(uint8_t)
INSTANTIATE(int16_t)
INSTANTIATE(int32_t)
INSTANTIATE(int64_t)
INSTANTIATE(float)
INSTANTIATE(double)
#undef INSTANTIATE

}  // namespace avx2
}  // namespace exec
}  // namespace milvus

#endif  // __x86_64__
