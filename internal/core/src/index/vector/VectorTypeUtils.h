// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <type_traits>
#include <utility>

#include "common/Types.h"

namespace milvus::index {

template <typename Visitor, typename Unsupported>
inline decltype(auto)
DispatchPhysicalVectorDataType(DataType data_type,
                               Visitor&& visitor,
                               Unsupported&& unsupported) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return std::forward<Visitor>(visitor).template operator()<float>();
        case DataType::VECTOR_BINARY:
            return std::forward<Visitor>(visitor).template operator()<bin1>();
        case DataType::VECTOR_FLOAT16:
            return std::forward<Visitor>(visitor)
                .template operator()<float16>();
        case DataType::VECTOR_BFLOAT16:
            return std::forward<Visitor>(visitor)
                .template operator()<bfloat16>();
        case DataType::VECTOR_INT8:
            return std::forward<Visitor>(visitor).template operator()<int8>();
        case DataType::VECTOR_SPARSE_U32_F32:
            return std::forward<Visitor>(visitor)
                .template operator()<sparse_u32_f32>();
        default:
            return std::forward<Unsupported>(unsupported)();
    }
}

template <typename T>
constexpr DataType
PhysicalVectorDataType() {
    if constexpr (std::is_same_v<T, float>) {
        return DataType::VECTOR_FLOAT;
    } else if constexpr (std::is_same_v<T, bin1>) {
        return DataType::VECTOR_BINARY;
    } else if constexpr (std::is_same_v<T, float16>) {
        return DataType::VECTOR_FLOAT16;
    } else if constexpr (std::is_same_v<T, bfloat16>) {
        return DataType::VECTOR_BFLOAT16;
    } else if constexpr (std::is_same_v<T, int8>) {
        return DataType::VECTOR_INT8;
    } else if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        return DataType::VECTOR_SPARSE_U32_F32;
    } else {
        static_assert(!sizeof(T), "unsupported physical vector type");
    }
}

}  // namespace milvus::index
