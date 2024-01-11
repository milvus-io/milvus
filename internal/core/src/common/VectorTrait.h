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
#include "Types.h"
#include <string>
#include <type_traits>
#include "Array.h"

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

class Float16Vector : public VectorTrait {
 public:
    using embedded_type = float16;
    static constexpr auto metric_type = DataType::VECTOR_FLOAT16;
};

class BFloat16Vector : public VectorTrait {
 public:
    using embedded_type = bfloat16;
    static constexpr auto metric_type = DataType::VECTOR_BFLOAT16;
};

template <typename VectorType>
inline constexpr int64_t
element_sizeof(int64_t dim) {
    static_assert(std::is_base_of_v<VectorType, VectorTrait>);
    if constexpr (std::is_same_v<VectorType, FloatVector>) {
        return dim * sizeof(float);
    } else if constexpr (std::is_same_v<VectorType, Float16Vector>) {
        return dim * sizeof(float16);
    } else if constexpr (std::is_same_v<VectorType, BFloat16Vector>) {
        return dim * sizeof(bfloat16);
    } else {
        return dim / 8;
    }
}

template <typename T>
constexpr bool IsVector = std::is_base_of_v<VectorTrait, T>;

template <typename T>
constexpr bool IsScalar =
    std::is_fundamental_v<T> || std::is_same_v<T, std::string> ||
    std::is_same_v<T, Json> || std::is_same_v<T, std::string_view> ||
    std::is_same_v<T, Array> || std::is_same_v<T, ArrayView> ||
    std::is_same_v<T, proto::plan::Array>;

template <typename T, typename Enabled = void>
struct EmbeddedTypeImpl;

template <typename T>
struct EmbeddedTypeImpl<T, std::enable_if_t<IsScalar<T>>> {
    using type = T;
};

template <typename T>
struct EmbeddedTypeImpl<T, std::enable_if_t<IsVector<T>>> {
    using type = std::conditional_t<
        std::is_same_v<T, FloatVector>,
        float,
        std::conditional_t<std::is_same_v<T, Float16Vector>,
                           float16,
                           std::conditional_t<std::is_same_v<T, BFloat16Vector>,
                                              bfloat16,
                                              uint8_t>>>;
};

template <typename T>
using EmbeddedType = typename EmbeddedTypeImpl<T>::type;

struct FundamentalTag {};
struct StringTag {};

template <class T>
struct TagDispatchTrait {
    using Tag = FundamentalTag;
};

template <>
struct TagDispatchTrait<std::string> {
    using Tag = StringTag;
};

}  // namespace milvus
