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
#include <cstdint>
#include <limits>

#include "common/EasyAssert.h"

namespace milvus {

inline size_t
CheckedInputSize(int64_t value, const char* label) {
    if (value < 0 ||
        static_cast<uint64_t>(value) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(ConfigInvalid,
                  "{} {} is outside the supported domain",
                  label,
                  value);
    }
    return static_cast<size_t>(value);
}

inline size_t
CheckedInputProduct(size_t lhs, size_t rhs, const char* label) {
    if (lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs) {
        ThrowInfo(ConfigInvalid, "{} size overflows", label);
    }
    return lhs * rhs;
}

inline size_t
CheckedKnowhereBytes(size_t count, size_t element_size, const char* label) {
    if (count != 0 &&
        element_size > std::numeric_limits<size_t>::max() / count) {
        ThrowInfo(KnowhereError, "knowhere {} byte size overflows", label);
    }
    return count * element_size;
}

}  // namespace milvus
