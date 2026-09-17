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
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/Types.h"
#include "index/ParamUtils.h"

namespace milvus::index {

inline bool
ScalarValueTypesMatch(DataType actual, DataType expected) {
    return actual == expected ||
           (IsStringDataType(actual) && IsStringDataType(expected)) ||
           ((actual == DataType::INT64 || actual == DataType::TIMESTAMPTZ) &&
            (expected == DataType::INT64 || expected == DataType::TIMESTAMPTZ));
}

inline DataType
ReadCompatibleScalarArrayElementType(const Config& params,
                                     std::string_view family) {
    const auto array_element_type =
        ReadDataTypeParam(params, "array_element_type")
            .value_or(DataType::NONE);
    const auto legacy_element_type =
        ReadDataTypeParam(params, "element_type").value_or(DataType::NONE);
    if (array_element_type != DataType::NONE &&
        legacy_element_type != DataType::NONE &&
        !ScalarValueTypesMatch(array_element_type, legacy_element_type)) {
        ThrowInfo(DataTypeInvalid,
                  "{} array_element_type {} conflicts with element_type {}",
                  family,
                  static_cast<int>(array_element_type),
                  static_cast<int>(legacy_element_type));
    }
    return array_element_type != DataType::NONE ? array_element_type
                                                : legacy_element_type;
}

inline void
AssignString(std::string& target, std::string_view source) {
    if (source.empty()) {
        target.clear();
    } else {
        target.assign(source.data(), source.size());
    }
}

// Non-primitive representations use the caller's family-specific fallback.
template <typename T>
constexpr DataType
CppDataType(DataType fallback = DataType::VARCHAR) {
    if constexpr (std::is_same_v<T, bool>) {
        return DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return DataType::INT32;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return DataType::INT64;
    } else if constexpr (std::is_same_v<T, float>) {
        return DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return DataType::DOUBLE;
    } else {
        return fallback;
    }
}

inline TargetBitmap
MakeValidity(const std::vector<uint8_t>& validity) {
    TargetBitmap result(validity.size(), false);
    for (size_t i = 0; i < validity.size(); ++i) {
        if (validity[i] != 0) {
            result.set(i);
        }
    }
    return result;
}

}  // namespace milvus::index
