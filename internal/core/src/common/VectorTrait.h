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
#include "Types.h"

namespace milvus {

class VectorTrait {};

class FloatVector : public VectorTrait {
 public:
    using embedded_type = float;
    static constexpr auto metric_type = DataType::VECTOR_FLOAT;
};

class BinaryVector : public VectorTrait {
 public:
    using embedded_type = uint8_t;
    static constexpr auto metric_type = DataType::VECTOR_BINARY;
};

template <typename VectorType>
inline constexpr int64_t
element_sizeof(int64_t dim) {
    static_assert(std::is_base_of_v<VectorType, VectorTrait>);
    if constexpr (std::is_same_v<VectorType, FloatVector>) {
        return dim * sizeof(float);
    } else {
        return dim / 8;
    }
}

template <typename T>
constexpr bool IsVector = std::is_base_of_v<VectorTrait, T>;

template <typename T>
constexpr bool IsScalar = std::is_fundamental_v<T>;

template <typename T, typename Enabled = void>
struct EmbeddedTypeImpl;

template <typename T>
struct EmbeddedTypeImpl<T, std::enable_if_t<IsScalar<T>>> {
    using type = T;
};

template <typename T>
struct EmbeddedTypeImpl<T, std::enable_if_t<IsVector<T>>> {
    using type = std::conditional_t<std::is_same_v<T, FloatVector>, float, uint8_t>;
};

template <typename T>
using EmbeddedType = typename EmbeddedTypeImpl<T>::type;

}  // namespace milvus
