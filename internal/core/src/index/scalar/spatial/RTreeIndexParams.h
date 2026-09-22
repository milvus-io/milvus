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

#include <optional>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "nlohmann/json.hpp"
#include "index/ParamUtils.h"

namespace milvus::index::spatial_params {

inline std::optional<DataType>
ReadDataType(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return milvus::index::ParseDataTypeValue(params.at(key), key);
}

inline void
ValidateGeometryParams(const Config& params) {
    for (const auto key :
         {std::string_view("field_type"), std::string_view("value_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::GEOMETRY) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be GEOMETRY, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }
    for (const auto key : {std::string_view("array_element_type"),
                           std::string_view("element_type")}) {
        const auto type = ReadDataType(params, key);
        if (type.has_value() && *type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "R-Tree parameter {} must be NONE, got {}",
                      key,
                      static_cast<int>(*type));
        }
    }

    const auto nested = ReadNestedConfigParam(params, "R-Tree");
    if (nested.value_or(false)) {
        ThrowInfo(DataTypeInvalid, "R-Tree does not support nested input");
    }
}

}  // namespace milvus::index::spatial_params
