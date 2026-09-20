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

#include <charconv>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "common/Types.h"
#include "index/ParamUtils.h"

namespace milvus::index::json_flat_params {

inline int64_t
ParseInteger(const Config& params,
             std::string_view key,
             int64_t fallback,
             bool required,
             std::string_view stage) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(DataTypeInvalid,
                      "JSON flat {} requires parameter {}",
                      stage,
                      key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        const auto value = encoded.get<uint64_t>();
        if (value >
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat parameter {} is out of range", key);
        }
        return static_cast<int64_t>(value);
    }
    if (encoded.is_number_integer()) {
        return encoded.get<int64_t>();
    }
    if (encoded.is_string()) {
        const auto text = encoded.get<std::string>();
        int64_t value = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), value);
        if (error == std::errc{} && end == text.data() + text.size()) {
            return value;
        }
    }
    ThrowInfo(
        DataTypeInvalid, "JSON flat parameter {} must be an integer", key);
}

inline DataType
RequireDataTypeParam(const Config& params,
                     std::string_view key,
                     std::string_view stage) {
    const auto type = ReadDataTypeParam(params, key);
    if (!type.has_value()) {
        ThrowInfo(
            DataTypeInvalid, "JSON flat {} requires parameter {}", stage, key);
    }
    return *type;
}

inline std::string
ParseString(const Config& params,
            std::string_view key,
            std::string fallback = {}) {
    return ReadStringParam(params, key, std::move(fallback), "JSON flat");
}

inline void
ValidateRowDomain(const Config& params, std::string_view stage) {
    if (ReadRequiredNestedParam(
            params, std::string("JSON flat ") + std::string(stage))) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat indexes support only the row coordinate domain");
    }
}

}  // namespace milvus::index::json_flat_params
