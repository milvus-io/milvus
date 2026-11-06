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

#include <cstddef>

namespace milvus {

// Find the first value >= target in the sorted range [cursor, size).
// A forward cursor bounds work by the gap between consecutive query values.
template <typename Value, typename ValueAt>
size_t
GallopingLowerBound(size_t size,
                    size_t cursor,
                    const Value& target,
                    ValueAt value_at) {
    if (cursor == size || !(value_at(cursor) < target)) {
        return cursor;
    }
    const size_t remaining = size - cursor;
    size_t lo = 1;
    size_t hi = 1;
    while (hi < remaining && value_at(cursor + hi) < target) {
        lo = hi + 1;
        hi = hi > remaining / 2 ? remaining : hi * 2;
    }
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (value_at(cursor + mid) < target) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return cursor + lo;
}

}  // namespace milvus
