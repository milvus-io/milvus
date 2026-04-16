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
#include <memory>

#include "arrow/type.h"
#include "common/Types.h"

namespace milvus::segcore {

// Expand Arrow bitpacked validity bitmap to Milvus byte-per-bool format.
// If bitmap is nullptr, returns an empty vector (all valid).
inline FixedVector<bool>
ExpandArrowValidity(const uint8_t* bitmap,
                    int64_t offset,
                    int64_t length) {
    if (bitmap == nullptr) {
        return {};
    }
    FixedVector<bool> result(length);
    for (int64_t i = 0; i < length; ++i) {
        int64_t bit_idx = offset + i;
        result[i] =
            (bitmap[bit_idx >> 3] & (1 << (bit_idx & 7))) != 0;
    }
    return result;
}

// Map Arrow data types to Milvus DataType enum.
inline DataType
ArrowTypeToMilvusType(
    const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return DataType::BOOL;
        case arrow::Type::INT8:
            return DataType::INT8;
        case arrow::Type::INT16:
            return DataType::INT16;
        case arrow::Type::INT32:
            return DataType::INT32;
        case arrow::Type::INT64:
            return DataType::INT64;
        case arrow::Type::FLOAT:
            return DataType::FLOAT;
        case arrow::Type::DOUBLE:
            return DataType::DOUBLE;
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return DataType::VARCHAR;
        default:
            return DataType::NONE;
    }
}

}  // namespace milvus::segcore
