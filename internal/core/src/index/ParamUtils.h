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
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/Utils.h"
#include "nlohmann/json.hpp"

namespace milvus::index {

// Internal build/load configuration uses the numeric DataType enum.
// External spellings are normalized before family configuration is parsed.
// Missing/null/default policy and supported-type checks belong to the caller.
inline DataType
ParseDataTypeValue(const nlohmann::json& value, std::string_view key) {
    int64_t encoded;
    if (value.is_number_unsigned()) {
        const auto unsigned_encoded = value.get<uint64_t>();
        if (unsigned_encoded >
            static_cast<uint64_t>(std::numeric_limits<int32_t>::max())) {
            ThrowInfo(DataTypeInvalid,
                      "data type parameter {} is out of int32 range",
                      key);
        }
        encoded = static_cast<int64_t>(unsigned_encoded);
    } else if (value.is_number_integer()) {
        encoded = value.get<int64_t>();
        if (encoded < std::numeric_limits<int32_t>::min() ||
            encoded > std::numeric_limits<int32_t>::max()) {
            ThrowInfo(DataTypeInvalid,
                      "data type parameter {} is out of int32 range",
                      key);
        }
    } else {
        ThrowInfo(DataTypeInvalid,
                  "data type parameter {} must use the integer enum encoding",
                  key);
    }
    return static_cast<DataType>(static_cast<int32_t>(encoded));
}

inline std::optional<DataType>
ReadDataTypeParam(const Config& params, std::string_view key) {
    if (!params.is_object() || !params.contains(key)) {
        return std::nullopt;
    }
    return ParseDataTypeValue(params.at(key), key);
}

// These string parameters default only when absent, not when explicitly null.
inline std::string
ReadStringParam(const nlohmann::json& params,
                std::string_view key,
                std::string fallback,
                std::string_view context) {
    if (!params.is_object() || !params.contains(key)) {
        return fallback;
    }
    try {
        return params.at(key).get<std::string>();
    } catch (const nlohmann::json::exception& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid {} parameter {}: {}",
                  context,
                  key,
                  error.what());
    }
}

inline const std::initializer_list<std::string_view> kNestedParamKeys = {
    "nested", "is_nested", "is_nested_index"};

// The caller controls decoding and whether null counts as missing. Visit keys
// in order, keeping the first supplied value and its diagnostic key. Defaulting
// and required-value checks stay with the caller. Callables are not type-erased.
template <typename T, typename Read, typename OnConflict>
std::optional<T>
ReadAliasedParam(std::initializer_list<std::string_view> keys,
                 Read&& read,
                 OnConflict&& on_conflict) {
    std::optional<T> result;
    std::string_view first_key;
    for (const auto key : keys) {
        const auto value = read(key);
        if (!value.has_value()) {
            continue;
        }
        if (result.has_value() && *result != *value) {
            on_conflict(first_key, key);
        }
        if (!result.has_value()) {
            result = *value;
            first_key = key;
        }
    }
    return result;
}

template <typename Read>
std::optional<bool>
ReadNestedParam(Read&& read, std::string_view context) {
    return ReadAliasedParam<bool>(
        kNestedParamKeys, read, [context](auto first, auto second) {
            ThrowInfo(DataTypeInvalid,
                      "{} nested parameters {} and {} disagree",
                      context,
                      first,
                      second);
        });
}

inline std::optional<bool>
ReadNestedConfigParam(const Config& params, std::string_view context) {
    return ReadNestedParam(
        [&](std::string_view key) {
            return GetValueFromConfig<bool>(params, std::string(key));
        },
        context);
}

// Production load boundaries restore this canonical runtime parameter from
// schema metadata before invoking a loader. Missing old persisted aliases are
// accepted there; absence here means the normalized internal contract was not
// satisfied.
inline bool
ReadRequiredNestedParam(const Config& params, std::string_view context) {
    const auto nested = ReadNestedConfigParam(params, context);
    if (!nested.has_value()) {
        ThrowInfo(DataTypeInvalid,
                  "{} requires an explicit normalized nested parameter",
                  context);
    }
    return *nested;
}

}  // namespace milvus::index
