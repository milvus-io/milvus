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
#include <iterator>
#include "pb/plan.pb.h"
#include "common/EasyAssert.h"

extern "C" {
#include "Murmur3.h"
}

namespace milvus::rescores {

inline float
function_score_merge(const float& a,
                     const float& b,
                     const proto::plan::FunctionMode& mode) {
    switch (mode) {
        case proto::plan::FunctionModeMultiply:
            return a * b;
        case proto::plan::FunctionModeSum:
            return a + b;
        default:
            ThrowInfo(ErrorCode::UnexpectedError,
                      fmt::format("unknwon boost function mode: {}:{}",
                                  proto::plan::FunctionMode_Name(mode),
                                  mode));
    }
}

#define MAGIC_BITS (0x3FFL << 52)

double
hash_to_double(const uint64_t& h) {
    auto double_bytes = (MAGIC_BITS | (h & 0x1FFFFFFFFFFFFF));
    double result;
    memcpy(&result, &double_bytes, sizeof(result));
    return result - 1.0;
}
}  // namespace milvus::rescores