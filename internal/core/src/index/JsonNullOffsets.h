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

#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>

#include "common/Types.h"

namespace milvus::index {

// V3 JSON-path INVERTED capability; independent of has_null and engine version.
inline constexpr char kJsonNullOffsetsComplete[] = "json_null_offsets_complete";

template <typename T>
bool
IsJsonComparableScalar(const T& value) {
    if constexpr (std::is_floating_point_v<T>) {
        // Match the historical finite-range query, including cast results.
        return std::isfinite(value);
    }
    return true;
}

// Recover validity for unmarked packed artifacts and legacy readers that
// still query it directly. New writers collect offsets during JSON extraction.
template <typename T, typename Engine>
TargetBitmap
QueryJsonComparableValues(Engine& engine, int64_t count) {
    TargetBitmap result(count);
    if constexpr (std::is_same_v<T, bool>) {
        bool values[] = {false, true};
        engine.terms_query(values, 2, &result);
    } else if constexpr (std::is_arithmetic_v<T>) {
        engine.range_query(std::numeric_limits<T>::lowest(),
                           std::numeric_limits<T>::max(),
                           true,
                           true,
                           &result);
    } else {
        static_assert(std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>);
        engine.prefix_query("", &result);
    }
    return result;
}

}  // namespace milvus::index
