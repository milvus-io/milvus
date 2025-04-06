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

#include <memory>
#include <string>
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"

#include "common/FieldMeta.h"
#include "common/Span.h"
#include "common/Types.h"

namespace milvus::query {

template <typename T>
inline index::ScalarIndexPtr<T>
generate_scalar_index(Span<T> data) {
    auto indexing = std::make_unique<index::ScalarIndexSort<T>>();
    indexing->Build(data.row_count(), data.data(), data.valid_data());
    return indexing;
}

template <>
inline index::ScalarIndexPtr<std::string>
generate_scalar_index(Span<std::string> data) {
    auto indexing = index::CreateStringIndexSort();
    indexing->Build(data.row_count(), data.data(), data.valid_data());
    return indexing;
}

inline index::IndexBasePtr
generate_scalar_index(SpanBase data, DataType data_type) {
    Assert(!IsVectorDataType(data_type));
    switch (data_type) {
        case DataType::BOOL:
            return generate_scalar_index(Span<bool>(data));
        case DataType::INT8:
            return generate_scalar_index(Span<int8_t>(data));
        case DataType::INT16:
            return generate_scalar_index(Span<int16_t>(data));
        case DataType::INT32:
            return generate_scalar_index(Span<int32_t>(data));
        case DataType::TIMESTAMP:
        case DataType::INT64:
            return generate_scalar_index(Span<int64_t>(data));
        case DataType::FLOAT:
            return generate_scalar_index(Span<float>(data));
        case DataType::DOUBLE:
            return generate_scalar_index(Span<double>(data));
        case DataType::VARCHAR:
            return generate_scalar_index(Span<std::string>(data));
        default:
            PanicInfo(DataTypeInvalid, "unsupported type {}", data_type);
    }
}

}  // namespace milvus::query
