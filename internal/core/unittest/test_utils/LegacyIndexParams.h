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

#include <cstdint>

namespace milvus::index {

struct NgramParams {
    uintptr_t min_gram{0};
    uintptr_t max_gram{0};
};

struct FMIndexParams {
    uint32_t sa_sample_rate = 8;  // suffix-array sampling rate (default 8)
    uint32_t block_bytes = 64;  // rank-block granularity in bytes (default 64)
};

}  // namespace milvus::index
