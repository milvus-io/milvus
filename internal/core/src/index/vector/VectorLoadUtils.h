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

#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/ParamUtils.h"
#include "index/vector/VectorParamUtils.h"
#include "nlohmann/json.hpp"

namespace milvus::index::vector_load_params {

inline std::optional<int64_t>
ReadInt64(const Config& params, std::string_view key) {
    if (!params.contains(key) || params.at(key).is_null()) {
        return std::nullopt;
    }
    return vector_params::ParseInt64(
        params.at(key), key, "normalized vector parameter");
}

template <typename T, typename Read>
std::optional<T>
ReadVectorAliases(const Config& params,
                  std::initializer_list<std::string_view> keys,
                  std::string_view label,
                  Read&& read) {
    return ReadAliasedParam<T>(
        keys,
        [&](std::string_view key) { return read(params, key); },
        [label](auto first, auto second) {
            ThrowInfo(UnexpectedError,
                      "normalized vector {} parameters {} and {} disagree",
                      label,
                      first,
                      second);
        });
}

inline std::optional<int64_t>
ReadAliasedInt64(const Config& params,
                 std::initializer_list<std::string_view> keys,
                 std::string_view label) {
    return ReadVectorAliases<int64_t>(params, keys, label, ReadInt64);
}

inline std::optional<DataType>
ReadDataType(const Config& params, std::string_view key) {
    if (params.contains(key) && params.at(key).is_null()) {
        return std::nullopt;
    }
    return ReadDataTypeParam(params, key);
}

inline std::optional<DataType>
ReadAliasedDataType(const Config& params,
                    std::initializer_list<std::string_view> keys,
                    std::string_view label) {
    return ReadVectorAliases<DataType>(params, keys, label, ReadDataType);
}

inline std::optional<bool>
ReadBool(const Config& params, std::string_view key) {
    return GetValueFromConfig<bool>(params, std::string(key));
}

inline std::optional<bool>
ReadAliasedBool(const Config& params,
                std::initializer_list<std::string_view> keys,
                std::string_view label) {
    return ReadVectorAliases<bool>(params, keys, label, ReadBool);
}

inline std::string
ReadRequiredString(const Config& params, std::string_view key) {
    if (!params.contains(key) || !params.at(key).is_string()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must be a string",
                  key);
    }
    auto value = params.at(key).get<std::string>();
    if (value.empty()) {
        ThrowInfo(UnexpectedError,
                  "normalized vector parameter {} must not be empty",
                  key);
    }
    return value;
}

inline bool
IsPhysicalVectorType(DataType type) {
    switch (type) {
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_SPARSE_U32_F32:
        case DataType::VECTOR_INT8:
            return true;
        default:
            return false;
    }
}

}  // namespace milvus::index::vector_load_params
