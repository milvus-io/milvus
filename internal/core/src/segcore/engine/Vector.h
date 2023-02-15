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
#include <vector>

#include <velox/vector/BaseVector.h>
#include <velox/vector/FlatVector.h>

#include "exceptions/EasyAssert.h"
#include "common/Types.h"

namespace milvus::engine {

using VectorPtr = facebook::velox::VectorPtr;

template <facebook::velox::TypeKind kind>
facebook::velox::VectorPtr
CreateFlatVector(facebook::velox::memory::MemoryPool* pool,
                 const facebook::velox::TypePtr& type,
                 facebook::velox::BufferPtr nulls,
                 size_t length,
                 facebook::velox::BufferPtr values,
                 int64_t nullCount) {
    using T = typename facebook::velox::TypeTraits<kind>::NativeType;
    return std::make_shared<facebook::velox::FlatVector<T>>(
        pool,
        type,
        nulls,
        length,
        values,
        std::vector<facebook::velox::BufferPtr>());
}

template <typename T>
facebook::velox::VectorPtr
InitVectorPtr(int64_t size, facebook::velox::memory::MemoryPool* pool) {
    auto type = facebook::velox::CppToType<T>::create();
    facebook::velox::VectorPtr vec =
        facebook::velox::BaseVector::create(type, size, pool);
    using EvalType = typename facebook::velox::CppToType<T>::NativeType;
    auto test = vec->template asFlatVector<EvalType>();
    if (test == NULL) {
        std::cout << "init failed" << std::endl;
    }
    return vec;
}

template <typename T, typename P>
void
SetFlatVector(facebook::velox::VectorPtr& vec, const std::vector<P>& data) {
    auto flatVector =
        std::dynamic_pointer_cast<facebook::velox::FlatVector<T>>(vec);
    using EvalType = typename facebook::velox::CppToType<T>::NativeType;
    for (facebook::velox::vector_size_t i = 0; i < data.size(); i++) {
        flatVector->set(i, EvalType(data[i]));
    }
}

template <typename T>
facebook::velox::TypePtr
ToVeloxType() {
    if constexpr (std::is_same_v<T, bool>) {
        return facebook::velox::BOOLEAN();
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return facebook::velox::TINYINT();
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return facebook::velox::SMALLINT();
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return facebook::velox::INTEGER();
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return facebook::velox::BIGINT();
    } else if constexpr (std::is_same_v<T, double>) {
        return facebook::velox::DOUBLE();
    } else if constexpr (std::is_same_v<T, float>) {
        return facebook::velox::REAL();
    } else if constexpr (std::is_same_v<T, std::string>) {
        return facebook::velox::VARCHAR();
    } else {
        PanicInfo("unsupported data type");
    }
}

facebook::velox::TypePtr
ToVeloxType(const milvus::DataType type);

}  // namespace milvus::engine
