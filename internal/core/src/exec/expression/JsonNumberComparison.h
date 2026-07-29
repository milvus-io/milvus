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

#include <cmath>
#include <cstdint>
#include <optional>

#include <simdjson.h>

#include "pb/plan.pb.h"

namespace milvus::exec {

inline bool
IsInt64SafeForJsonDoubleIndex(int64_t value) {
    constexpr int64_t kFirstNonInjectiveInteger = int64_t{1} << 53;
    return value > -kFirstNonInjectiveInteger &&
           value < kFirstNonInjectiveInteger;
}

inline bool
JsonNumericBoundRequiresPreciseInt64Comparison(
    const proto::plan::GenericValue& bound) {
    if (bound.has_int64_val()) {
        return !IsInt64SafeForJsonDoubleIndex(bound.int64_val());
    }
    if (!bound.has_float_val() || !std::isfinite(bound.float_val())) {
        return false;
    }

    constexpr double kFirstNonInjectiveInteger = 0x1p53;
    constexpr double kInt64Magnitude = 0x1p63;
    const auto value = bound.float_val();
    return (value >= kFirstNonInjectiveInteger && value <= kInt64Magnitude) ||
           (value <= -kFirstNonInjectiveInteger && value >= -kInt64Magnitude);
}

inline int
CompareInt64ToDouble(int64_t lhs, double rhs) {
    constexpr double kInt64Lower = -0x1p63;
    constexpr double kInt64Upper = 0x1p63;
    if (rhs < kInt64Lower) {
        return 1;
    }
    if (rhs >= kInt64Upper) {
        return -1;
    }

    const auto rhs_integer = static_cast<int64_t>(rhs);
    if (lhs < rhs_integer) {
        return -1;
    }
    if (lhs > rhs_integer) {
        return 1;
    }

    const auto rhs_integer_as_double = static_cast<double>(rhs_integer);
    if (rhs > rhs_integer_as_double) {
        return -1;
    }
    if (rhs < rhs_integer_as_double) {
        return 1;
    }
    return 0;
}

inline int
CompareUint64ToDouble(uint64_t lhs, double rhs) {
    constexpr double kUint64Upper = 0x1p64;
    if (rhs < 0) {
        return 1;
    }
    if (rhs >= kUint64Upper) {
        return -1;
    }

    const auto rhs_integer = static_cast<uint64_t>(rhs);
    if (lhs < rhs_integer) {
        return -1;
    }
    if (lhs > rhs_integer) {
        return 1;
    }

    const auto rhs_integer_as_double = static_cast<double>(rhs_integer);
    if (rhs > rhs_integer_as_double) {
        return -1;
    }
    if (rhs < rhs_integer_as_double) {
        return 1;
    }
    return 0;
}

inline std::optional<int>
CompareJsonNumberToBound(int64_t number,
                         const proto::plan::GenericValue& bound) {
    if (bound.has_int64_val()) {
        const auto rhs = bound.int64_val();
        return number < rhs ? -1 : number > rhs ? 1 : 0;
    }
    if (bound.has_float_val()) {
        const auto rhs = bound.float_val();
        return std::isnan(rhs)
                   ? std::nullopt
                   : std::optional<int>(CompareInt64ToDouble(number, rhs));
    }
    return std::nullopt;
}

inline std::optional<int>
CompareJsonNumberToBound(double number,
                         const proto::plan::GenericValue& bound) {
    if (std::isnan(number)) {
        return std::nullopt;
    }
    if (bound.has_int64_val()) {
        return -CompareInt64ToDouble(bound.int64_val(), number);
    }
    if (bound.has_float_val()) {
        const auto rhs = bound.float_val();
        if (std::isnan(rhs)) {
            return std::nullopt;
        }
        return number < rhs ? -1 : number > rhs ? 1 : 0;
    }
    return std::nullopt;
}

inline std::optional<int>
CompareJsonNumberToBound(const simdjson::ondemand::number& number,
                         const proto::plan::GenericValue& bound) {
    if (number.is_int64()) {
        return CompareJsonNumberToBound(number.get_int64(), bound);
    }
    if (number.is_uint64()) {
        const auto lhs = number.get_uint64();
        if (bound.has_int64_val()) {
            const auto rhs = bound.int64_val();
            if (rhs < 0) {
                return 1;
            }
            const auto unsigned_rhs = static_cast<uint64_t>(rhs);
            return lhs < unsigned_rhs ? -1 : lhs > unsigned_rhs ? 1 : 0;
        }
        if (bound.has_float_val()) {
            const auto rhs = bound.float_val();
            if (std::isnan(rhs)) {
                return std::nullopt;
            }
            return CompareUint64ToDouble(lhs, rhs);
        }
        return std::nullopt;
    }
    return CompareJsonNumberToBound(number.get_double(), bound);
}

// JSON stats and typed JSON indexes store uint64 values as double. Preserve
// that established behavior while comparing int64 JSON values exactly.
inline std::optional<int>
CompareJsonNumberToBoundWithUint64DoubleFallback(
    const simdjson::ondemand::number& number,
    const proto::plan::GenericValue& bound) {
    if (number.is_uint64()) {
        return CompareJsonNumberToBound(
            static_cast<double>(number.get_uint64()), bound);
    }
    return CompareJsonNumberToBound(number, bound);
}

inline bool
JsonNumberMatchesOp(int comparison, proto::plan::OpType op) {
    switch (op) {
        case proto::plan::OpType::Equal:
            return comparison == 0;
        case proto::plan::OpType::NotEqual:
            return comparison != 0;
        case proto::plan::OpType::GreaterThan:
            return comparison > 0;
        case proto::plan::OpType::GreaterEqual:
            return comparison >= 0;
        case proto::plan::OpType::LessThan:
            return comparison < 0;
        case proto::plan::OpType::LessEqual:
            return comparison <= 0;
        default:
            return false;
    }
}

}  // namespace milvus::exec
