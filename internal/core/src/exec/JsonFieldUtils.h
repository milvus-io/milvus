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

#include <memory>
#include <string>
#include <vector>

#include "common/Json.h"
#include "common/Vector.h"

namespace milvus {
namespace exec {

// Extract JSON sub-field values from a JSON ColumnVector into a new VARCHAR
// ColumnVector. Each row's JSON blob is parsed and the value at the given
// pointer path is extracted as a string.
inline ColumnVectorPtr
ExtractJsonSubField(const ColumnVectorPtr& json_col,
                    const std::vector<std::string>& nested_path) {
    auto num_rows = json_col->size();
    auto result =
        std::make_shared<ColumnVector>(DataType::VARCHAR, num_rows);
    auto pointer = Json::pointer(nested_path);
    auto* json_array = json_col->RawAsValues<Json>();

    for (size_t i = 0; i < num_rows; i++) {
        if (!json_col->ValidAt(i)) {
            result->SetValueAt<std::string>(i, "");
            continue;
        }
        auto& json_val = json_array[i];
        auto extracted = json_val.at_string_any(pointer);
        if (extracted.error()) {
            result->SetValueAt<std::string>(i, "");
        } else {
            result->SetValueAt<std::string>(i, extracted.value());
        }
    }
    return result;
}

// Extract JSON sub-field values as DOUBLE for numeric aggregation.
inline VectorPtr
ExtractJsonSubFieldAsDouble(const VectorPtr& col,
                            const std::vector<std::string>& nested_path) {
    auto json_col = std::dynamic_pointer_cast<ColumnVector>(col);
    if (!json_col) {
        return col;
    }
    auto num_rows = json_col->size();
    auto result =
        std::make_shared<ColumnVector>(DataType::DOUBLE, num_rows);
    auto pointer = Json::pointer(nested_path);
    auto* json_array = json_col->RawAsValues<Json>();

    for (size_t i = 0; i < num_rows; i++) {
        if (!json_col->ValidAt(i)) {
            result->SetValueAt<double>(i, 0.0);
            continue;
        }
        auto& json_val = json_array[i];
        auto dbl_result = json_val.at<double>(pointer);
        if (dbl_result.error()) {
            // Try parsing as string then converting to double
            auto str_result = json_val.at<std::string_view>(pointer);
            if (!str_result.error()) {
                char* end = nullptr;
                double val = std::strtod(
                    std::string(str_result.value()).c_str(), &end);
                result->SetValueAt<double>(i, val);
            } else {
                result->SetValueAt<double>(i, 0.0);
            }
        } else {
            result->SetValueAt<double>(i, dbl_result.value());
        }
    }
    return result;
}

}  // namespace exec
}  // namespace milvus
