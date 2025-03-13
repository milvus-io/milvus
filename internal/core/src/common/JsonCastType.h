// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include "common/EasyAssert.h"
namespace milvus {

enum class JsonCastType { BOOL, DOUBLE, VARCHAR };

inline auto
format_as(JsonCastType f) {
    return fmt::underlying(f);
}

inline const std::unordered_map<std::string, JsonCastType> JsonCastTypeMap = {
    {"BOOL", JsonCastType::BOOL},
    {"DOUBLE", JsonCastType::DOUBLE},
    {"VARCHAR", JsonCastType::VARCHAR}};

inline JsonCastType
ConvertToJsonCastType(const std::string& str) {
    auto it = JsonCastTypeMap.find(str);
    if (it != JsonCastTypeMap.end()) {
        return it->second;
    }
    PanicInfo(Unsupported, "Invalid json cast type: " + str);
}

}  // namespace milvus