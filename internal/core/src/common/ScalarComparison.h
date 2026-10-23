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

#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <type_traits>

namespace milvus {

// One sortable numeric key contract for scans, indexes and JSON statistics.
// Keep FLOAT keys 32-bit; widening is explicit when comparing two float types.
// All NaNs share the largest key, and signed zeros share the positive-zero key.
template <typename T>
using FloatSortableKey =
    std::conditional_t<std::is_same_v<T, float>, uint32_t, uint64_t>;

template <typename T>
inline FloatSortableKey<T>
FloatToSortableKey(T value) {
    static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
    using Key = FloatSortableKey<T>;
    if (std::isnan(value)) {
        return std::numeric_limits<Key>::max();
    }
    if (value == T(0)) {
        value = T(0);
    }
    const auto bits = std::bit_cast<Key>(value);
    constexpr Key sign = Key{1} << (sizeof(Key) * 8 - 1);
    return bits & sign ? ~bits : bits ^ sign;
}

template <typename T>
inline bool
ScalarIsNaN(const T& value) {
    if constexpr (std::is_floating_point_v<T>) {
        return std::isnan(value);
    }
    return false;
}

template <typename T, typename U>
inline bool
ScalarEqual(const T& left, const U& right) {
    if constexpr (std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        using Common = std::common_type_t<T, U>;
        return FloatToSortableKey(static_cast<Common>(left)) ==
               FloatToSortableKey(static_cast<Common>(right));
    } else if constexpr (std::is_floating_point_v<T> ||
                         std::is_floating_point_v<U>) {
        if (ScalarIsNaN(left)) {
            return ScalarIsNaN(right);
        }
    }
    return left == right;
}

template <typename T, typename U>
inline bool
ScalarLess(const T& left, const U& right) {
    if constexpr (std::is_floating_point_v<T> && std::is_floating_point_v<U>) {
        using Common = std::common_type_t<T, U>;
        return FloatToSortableKey(static_cast<Common>(left)) <
               FloatToSortableKey(static_cast<Common>(right));
    } else if constexpr (std::is_floating_point_v<T> ||
                         std::is_floating_point_v<U>) {
        if (ScalarIsNaN(left)) {
            return false;
        }
        if (ScalarIsNaN(right)) {
            return true;
        }
    }
    return left < right;
}

template <typename T, typename U>
inline bool
ScalarGreater(const T& left, const U& right) {
    return ScalarLess(right, left);
}
template <typename T, typename U>
inline bool
ScalarLessEqual(const T& left, const U& right) {
    return ScalarLess(left, right) || ScalarEqual(left, right);
}
template <typename T, typename U>
inline bool
ScalarGreaterEqual(const T& left, const U& right) {
    return ScalarLess(right, left) || ScalarEqual(left, right);
}

template <typename T>
struct ScalarLessThan {
    bool
    operator()(const T& left, const T& right) const {
        return ScalarLess(left, right);
    }
};
template <typename T>
struct ScalarEqualTo {
    bool
    operator()(const T& left, const T& right) const {
        return ScalarEqual(left, right);
    }
};
template <typename T>
struct ScalarHash {
    size_t
    operator()(const T& value) const {
        if constexpr (std::is_floating_point_v<T>) {
            return std::hash<FloatSortableKey<T>>{}(FloatToSortableKey(value));
        } else {
            return std::hash<T>{}(value);
        }
    }
};

}  // namespace milvus
