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
#include "VectorArray.h"

namespace milvus {

template <typename T>
constexpr bool IsVector = std::is_base_of_v<VectorTrait, T>;

template <typename T>
constexpr bool IsScalar =
    std::is_fundamental_v<T> || std::is_same_v<T, std::string> ||
    std::is_same_v<T, Json> || std::is_same_v<T, Geometry> ||
    std::is_same_v<T, std::string_view> || std::is_same_v<T, Array> ||
    std::is_same_v<T, ArrayView> || std::is_same_v<T, proto::plan::Array>;

template <typename T>
constexpr bool IsSparse =
    std::is_same_v<T, SparseFloatVector> ||
    std::is_same_v<T, knowhere::sparse::SparseRow<SparseValueType>>;

template <typename T>
constexpr bool IsVariableType =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view> ||
    std::is_same_v<T, Array> || std::is_same_v<T, ArrayView> ||
    std::is_same_v<T, proto::plan::Array> || std::is_same_v<T, Json> ||
    IsSparse<T> || std::is_same_v<T, VectorArray> ||
    std::is_same_v<T, VectorArrayView>;

// todo(SpadeA): support vector array
template <typename T>
constexpr bool IsVariableTypeSupportInChunk =
    std::is_same_v<T, std::string> || std::is_same_v<T, Array> ||
    std::is_same_v<T, Json> ||
    std::is_same_v<T, knowhere::sparse::SparseRow<SparseValueType>>;

template <typename T>
using ChunkViewType = std::conditional_t<
    std::is_same_v<T, std::string>,
    std::string_view,
    std::conditional_t<std::is_same_v<T, Array>,
                       ArrayView,
                       std::conditional_t<std::is_same_v<T, VectorArray>,
                                          VectorArrayView,
                                          T>>>;

}  // namespace milvus
