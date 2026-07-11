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
#include <cstdint>
#include <optional>

#include <simdjson.h>

#include "pb/plan.pb.h"

namespace milvus::exec {

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
CompareJsonNumberToBound(const simdjson::ondemand::number& number,
                         const proto::plan::GenericValue& bound) {
    if (bound.has_int64_val()) {
        const auto rhs = bound.int64_val();
        if (number.is_int64()) {
            const auto lhs = number.get_int64();
            return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
        }
        if (number.is_uint64()) {
            const auto lhs = number.get_uint64();
            if (rhs < 0) {
                return 1;
            }
            const auto unsigned_rhs = static_cast<uint64_t>(rhs);
            return lhs < unsigned_rhs ? -1 : lhs > unsigned_rhs ? 1 : 0;
        }
        const auto lhs = number.get_double();
        if (std::isnan(lhs)) {
            return std::nullopt;
        }
        return -CompareInt64ToDouble(rhs, lhs);
    }

    if (bound.has_float_val()) {
        const auto rhs = bound.float_val();
        if (std::isnan(rhs)) {
            return std::nullopt;
        }
        if (number.is_int64()) {
            return CompareInt64ToDouble(number.get_int64(), rhs);
        }
        if (number.is_uint64()) {
            return CompareUint64ToDouble(number.get_uint64(), rhs);
        }
        const auto lhs = number.get_double();
        if (std::isnan(lhs)) {
            return std::nullopt;
        }
        return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
    }

    return std::nullopt;
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
