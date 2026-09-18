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

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "nlohmann/json.hpp"

namespace milvus::index::vector_params {

inline int64_t
ParseInt64(const nlohmann::json& value,
           std::string_view key,
           std::string_view diagnostic_prefix) {
    if (value.is_number_unsigned()) {
        const auto parsed = value.get<uint64_t>();
        if (parsed <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(parsed);
        }
    } else if (value.is_number_integer()) {
        return value.get<int64_t>();
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t parsed = 0;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (error == std::errc() && end == text.data() + text.size()) {
            return parsed;
        }
    }
    ThrowInfo(UnexpectedError,
              "{} {} is not an int64",
              diagnostic_prefix,
              key);
}

inline int32_t
ParsePositiveInt32(const nlohmann::json& value, std::string_view key) {
    int64_t parsed = 0;
    bool valid = false;
    if (value.is_number_unsigned()) {
        const auto number = value.get<uint64_t>();
        if (number <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            parsed = static_cast<int64_t>(number);
            valid = true;
        }
    } else if (value.is_number_integer()) {
        parsed = value.get<int64_t>();
        valid = true;
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), parsed);
        valid = error == std::errc() && end == text.data() + text.size();
    }
    if (!valid || parsed <= 0 || parsed > std::numeric_limits<int32_t>::max()) {
        ThrowInfo(
            ConfigInvalid, "vector parameter {} must be a positive int32", key);
    }
    return static_cast<int32_t>(parsed);
}

}  // namespace milvus::index::vector_params
