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

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <map>
#include <string>
#include <utility>
#include <string_view>

#include "common/EasyAssert.h"
#include "index/ParamUtils.h"
#include "common/Types.h"
#include "nlohmann/json.hpp"
#include "common/Consts.h"
#include "index/Meta.h"

namespace milvus::index::ngram_params {

inline std::string
Upper(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](char c) {
        return static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    });
    return value;
}

inline uint64_t
ParseUnsigned(const Config& params,
              std::string_view key,
              uint64_t fallback,
              bool required) {
    if (!params.is_object() || !params.contains(key)) {
        if (required) {
            ThrowInfo(DataTypeInvalid, "NGRAM requires parameter {}", key);
        }
        return fallback;
    }
    const auto& encoded = params.at(key);
    if (encoded.is_number_unsigned()) {
        try {
            return encoded.get<uint64_t>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_number_integer()) {
        try {
            const auto value = encoded.get<int64_t>();
            if (value < 0) {
                ThrowInfo(DataTypeInvalid,
                          "NGRAM parameter {} must be non-negative",
                          key);
            }
            return static_cast<uint64_t>(value);
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    if (encoded.is_string()) {
        try {
            const auto text = encoded.get<std::string>();
            uint64_t value = 0;
            const auto* begin = text.data();
            const auto* end = begin + text.size();
            const auto parsed = std::from_chars(begin, end, value);
            if (parsed.ec == std::errc{} && parsed.ptr == end) {
                return value;
            }
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "invalid NGRAM parameter {}: {}",
                      key,
                      error.what());
        }
    }
    ThrowInfo(DataTypeInvalid, "NGRAM parameter {} must be an integer", key);
}

inline std::string
ParseString(const Config& params,
            std::string_view key,
            std::string fallback = {}) {
    return ReadStringParam(params, key, std::move(fallback), "NGRAM");
}

inline std::string
ParseJsonPath(const Config& params) {
    const bool has_json_path = params.is_object() && params.contains(JSON_PATH);
    const bool has_nested_path =
        params.is_object() && params.contains("nested_path");
    const auto json_path = ParseString(params, JSON_PATH);
    const auto nested_path = ParseString(params, "nested_path");
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid, "NGRAM json_path and nested_path disagree");
    }
    return has_json_path ? json_path : nested_path;
}

}  // namespace milvus::index::ngram_params
