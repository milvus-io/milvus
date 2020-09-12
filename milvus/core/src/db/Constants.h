// Copyright (C) 2019-2020 Zilliz. All rights reserved.
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

#include <stdint.h>

namespace milvus {
namespace engine {

constexpr int64_t KB = 1LL << 10;
constexpr int64_t MB = 1LL << 20;
constexpr int64_t GB = 1LL << 30;
constexpr int64_t TB = 1LL << 40;

constexpr int64_t MAX_TABLE_FILE_MEM = 128 * MB;

constexpr int FLOAT_TYPE_SIZE = sizeof(float);

}  // namespace engine
}  // namespace milvus
