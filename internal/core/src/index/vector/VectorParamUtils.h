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

#include <cstdint>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/ParamUtils.h"
#include "nlohmann/json.hpp"

namespace milvus::index::vector_params {

inline int32_t
ParsePositiveInt32(const nlohmann::json& value, std::string_view key) {
    const auto parsed = index::TryParseInt64Value(value);
    if (!parsed.has_value() || *parsed <= 0 ||
        *parsed > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            ConfigInvalid, "vector parameter {} must be a positive int32", key);
    }
    return static_cast<int32_t>(*parsed);
}

inline std::optional<DataType>
ReadAliasedDataType(const Config& params,
                    std::initializer_list<std::string_view> keys,
                    std::string_view label) {
    return index::ReadAliasedParam<DataType>(
        keys,
        [&](std::string_view key) {
            return index::ReadNullableDataTypeParam(params, key);
        },
        [label](auto first, auto second) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first,
                      second);
        });
}

inline std::string
ReadRequiredString(const Config& params,
                   std::string_view key,
                   std::string_view diagnostic_prefix) {
    if (!params.contains(key) || !params.at(key).is_string()) {
        ThrowInfo(UnexpectedError,
                  "{} {} must be a string",
                  diagnostic_prefix,
                  key);
    }
    auto value = params.at(key).get<std::string>();
    if (value.empty()) {
        ThrowInfo(UnexpectedError,
                  "{} {} must not be empty",
                  diagnostic_prefix,
                  key);
    }
    return value;
}

}  // namespace milvus::index::vector_params
