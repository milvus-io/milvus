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
#include <cstdint>
#include "common.h"
namespace milvus {
namespace simd {

BitsetBlockType
GetBitsetBlockSSE2(const bool* src);

bool
AllFalseNEON(const bool* src, int64_t size);

bool
AllTrueNEON(const bool* src, int64_t size);

void
InvertBoolNEON(bool* src, int64_t size);

void
AndBoolNEON(bool* left, bool* right, int64_t size);

void
OrBoolNEON(bool* left, bool* right, int64_t size);

}  // namespace simd
}  // namespace milvus