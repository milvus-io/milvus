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

#include <string>
#include <type_traits>

#include "Array.h"
#include "Types.h"
#include "common/type_c.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"

namespace milvus {

#define GET_ELEM_TYPE_FOR_VECTOR_TRAIT                             \
    using elem_type = std::conditional_t<                          \
        std::is_same_v<TraitType, milvus::BinaryVector>,           \
        BinaryVector::embedded_type,                               \
        std::conditional_t<                                        \
            std::is_same_v<TraitType, milvus::Float16Vector>,      \
            Float16Vector::embedded_type,                          \
            std::conditional_t<                                    \
                std::is_same_v<TraitType, milvus::BFloat16Vector>, \
                BFloat16Vector::embedded_type,                     \
                FloatVector::embedded_type>>>;

#define GET_SCHEMA_DATA_TYPE_FOR_VECTOR_TRAIT               \
    auto schema_data_type =                                 \
        std::is_same_v<TraitType, milvus::FloatVector>      \
            ? FloatVector::schema_data_type                 \
        : std::is_same_v<TraitType, milvus::Float16Vector>  \
            ? Float16Vector::schema_data_type               \
        : std::is_same_v<TraitType, milvus::BFloat16Vector> \
            ? BFloat16Vector::schema_data_type              \
            : BinaryVector::schema_data_type;

class VectorTrait {};

class FloatVector : public VectorTrait {
 public:
    using embedded_type = float;
    static constexpr int32_t dim_factor = 1;
    static constexpr auto data_type = DataType::VECTOR_FLOAT;
    static constexpr auto c_data_type = CDataType::FloatVector;
    static constexpr auto schema_data_type =
        proto::schema::DataType::FloatVector;
    static constexpr auto vector_type = proto::plan::VectorType::FloatVector;
    static constexpr auto placeholder_type =
        proto::common::PlaceholderType::FloatVector;
};

class BinaryVector : public VectorTrait {
 public:
    using embedded_type = uint8_t;
    static constexpr int32_t dim_factor = 8;
    static constexpr auto data_type = DataType::VECTOR_BINARY;
    static constexpr auto c_data_type = CDataType::BinaryVector;
    static constexpr auto schema_data_type =
        proto::schema::DataType::BinaryVector;
    static constexpr auto vector_type = proto::plan::VectorType::BinaryVector;
    static constexpr auto placeholder_type =
        proto::common::PlaceholderType::BinaryVector;
};

class Float16Vector : public VectorTrait {
 public:
    using embedded_type = float16;
    static constexpr int32_t dim_factor = 1;
    static constexpr auto data_type = DataType::VECTOR_FLOAT16;
    static constexpr auto c_data_type = CDataType::Float16Vector;
    static constexpr auto schema_data_type =
        proto::schema::DataType::Float16Vector;
    static constexpr auto vector_type = proto::plan::VectorType::Float16Vector;
    static constexpr auto placeholder_type =
        proto::common::PlaceholderType::Float16Vector;
};

class BFloat16Vector : public VectorTrait {
 public:
    using embedded_type = bfloat16;
    static constexpr int32_t dim_factor = 1;
    static constexpr auto data_type = DataType::VECTOR_BFLOAT16;
    static constexpr auto c_data_type = CDataType::BFloat16Vector;
    static constexpr auto schema_data_type =
        proto::schema::DataType::BFloat16Vector;
    static constexpr auto vector_type = proto::plan::VectorType::BFloat16Vector;
    static constexpr auto placeholder_type =
        proto::common::PlaceholderType::BFloat16Vector;
};

class SparseFloatVector : public VectorTrait {
 public:
    using embedded_type = float;
    static constexpr int32_t dim_factor = 1;
    static constexpr auto data_type = DataType::VECTOR_SPARSE_FLOAT;
    static constexpr auto c_data_type = CDataType::SparseFloatVector;
    static constexpr auto schema_data_type =
        proto::schema::DataType::SparseFloatVector;
    static constexpr auto vector_type =
        proto::plan::VectorType::SparseFloatVector;
    static constexpr auto placeholder_type =
        proto::common::PlaceholderType::SparseFloatVector;
};

template <typename T>
constexpr bool IsVector = std::is_base_of_v<VectorTrait, T>;

template <typename T>
constexpr bool IsScalar =
    std::is_fundamental_v<T> || std::is_same_v<T, std::string> ||
    std::is_same_v<T, Json> || std::is_same_v<T, std::string_view> ||
    std::is_same_v<T, Array> || std::is_same_v<T, ArrayView> ||
    std::is_same_v<T, proto::plan::Array>;

template <typename T>
constexpr bool IsSparse = std::is_same_v<T, SparseFloatVector> ||
                          std::is_same_v<T, knowhere::sparse::SparseRow<float>>;

template <typename T>
constexpr bool IsVariableType =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view> ||
    std::is_same_v<T, Array> || std::is_same_v<T, ArrayView> ||
    std::is_same_v<T, proto::plan::Array> || std::is_same_v<T, Json> ||
    IsSparse<T>;

template <typename T>
constexpr bool IsVariableTypeSupportInChunk =
    std::is_same_v<T, std::string> || std::is_same_v<T, Array> ||
    std::is_same_v<T, Json> ||
    std::is_same_v<T, knowhere::sparse::SparseRow<float>>;

template <typename T>
using ChunkViewType = std::conditional_t<
    std::is_same_v<T, std::string>,
    std::string_view,
    std::conditional_t<std::is_same_v<T, Array>, ArrayView, T>>;

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
