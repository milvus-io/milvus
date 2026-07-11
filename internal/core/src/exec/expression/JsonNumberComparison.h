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
#include <type_traits>

#include <simdjson.h>

#include "common/bson_view.h"
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

inline bool
Int64RoundTripsThroughDouble(int64_t value) {
    return CompareInt64ToDouble(value, static_cast<double>(value)) == 0;
}

inline std::optional<int64_t>
DoubleToInt64Exact(double value) {
    constexpr double kInt64Lower = -0x1p63;
    constexpr double kInt64Upper = 0x1p63;
    if (!std::isfinite(value) || value < kInt64Lower || value >= kInt64Upper) {
        return std::nullopt;
    }
    const auto integer = static_cast<int64_t>(value);
    return CompareInt64ToDouble(integer, value) == 0
               ? std::optional<int64_t>(integer)
               : std::nullopt;
}

template <typename Target, typename Source>
std::optional<Target>
ConvertJsonNumberExact(Source value) {
    static_assert(
        (std::is_same_v<Target, int64_t> || std::is_same_v<Target, double>)&&(
            std::is_same_v<Source, int64_t> || std::is_same_v<Source, double>));
    if constexpr (std::is_same_v<Target, Source>) {
        return value;
    } else if constexpr (std::is_same_v<Target, int64_t>) {
        return DoubleToInt64Exact(value);
    } else {
        return Int64RoundTripsThroughDouble(value)
                   ? std::optional<double>(static_cast<double>(value))
                   : std::nullopt;
    }
}

template <typename Target>
std::optional<Target>
GetBsonNumberExact(const milvus::bson::value_view& value) {
    static_assert(std::is_same_v<Target, int64_t> ||
                  std::is_same_v<Target, double>);
    if (auto integer32 =
            milvus::BsonView::GetValueFromBsonView<int32_t>(value)) {
        return ConvertJsonNumberExact<Target>(static_cast<int64_t>(*integer32));
    }
    if (auto integer = milvus::BsonView::GetValueFromBsonView<int64_t>(value)) {
        return ConvertJsonNumberExact<Target>(*integer);
    }
    if (auto floating = milvus::BsonView::GetValueFromBsonView<double>(value)) {
        return ConvertJsonNumberExact<Target>(*floating);
    }
    return std::nullopt;
}

template <typename Target>
std::optional<Target>
ParseBsonNumberExact(milvus::BsonView& bson, size_t offset, bool& is_number) {
    static_assert(std::is_same_v<Target, int64_t> ||
                  std::is_same_v<Target, double>);
    if (auto integer = bson.ParseAsValueAtOffset<int64_t>(offset)) {
        is_number = true;
        return ConvertJsonNumberExact<Target>(*integer);
    }
    if (auto floating = bson.ParseAsValueAtOffset<double>(offset)) {
        is_number = true;
        return ConvertJsonNumberExact<Target>(*floating);
    }
    is_number = false;
    return std::nullopt;
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
CompareBsonNumberToBound(const milvus::bson::value_view& number,
                         const proto::plan::GenericValue& bound) {
    if (auto integer32 =
            milvus::BsonView::GetValueFromBsonView<int32_t>(number)) {
        return CompareJsonNumberToBound(static_cast<int64_t>(*integer32),
                                        bound);
    }
    if (auto integer =
            milvus::BsonView::GetValueFromBsonView<int64_t>(number)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating =
            milvus::BsonView::GetValueFromBsonView<double>(number)) {
        return CompareJsonNumberToBound(*floating, bound);
    }
    return std::nullopt;
}

inline std::optional<int>
CompareBsonNumberToBound(milvus::BsonView& bson,
                         size_t offset,
                         const proto::plan::GenericValue& bound) {
    if (auto integer = bson.ParseAsValueAtOffset<int64_t>(offset)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating = bson.ParseAsValueAtOffset<double>(offset)) {
        return CompareJsonNumberToBound(*floating, bound);
    }
    return std::nullopt;
}

inline std::optional<int>
CompareBsonArrayNumberToBound(const milvus::bson::array_view& array,
                              size_t index,
                              const proto::plan::GenericValue& bound) {
    if (auto integer32 = milvus::BsonView::GetNthElementInArray<int32_t>(
            array.data(), index)) {
        return CompareJsonNumberToBound(static_cast<int64_t>(*integer32),
                                        bound);
    }
    if (auto integer = milvus::BsonView::GetNthElementInArray<int64_t>(
            array.data(), index)) {
        return CompareJsonNumberToBound(*integer, bound);
    }
    if (auto floating = milvus::BsonView::GetNthElementInArray<double>(
            array.data(), index)) {
        return CompareJsonNumberToBound(*floating, bound);
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
            return CompareUint64ToDouble(number.get_uint64(), rhs);
        }
        return std::nullopt;
    }
    return CompareJsonNumberToBound(number.get_double(), bound);
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
