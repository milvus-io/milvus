// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include "common.h"

namespace milvus {
namespace simd {

BitsetBlockType
GetBitsetBlockRef(const bool* src);

template <typename T>
bool
FindTermRef(const T* src, size_t size, T val) {
    for (size_t i = 0; i < size; ++i) {
        if (src[i] == val) {
            return true;
        }
    }
    return false;
}

}  // namespace simd
}  // namespace milvus
