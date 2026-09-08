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

#include <cmath>
#include "common/EasyAssert.h"
#include "knowhere/config.h"

namespace milvus {

inline constexpr size_t kStrictGroupProbeCandidates = 100;
inline constexpr double kDefaultStrictGroupAcceptanceThreshold = 0.1;
inline constexpr char kStrictGroupAcceptanceThreshold[] =
    "strict_group_acceptance_threshold";

// Consume this Milvus-only parameter before forwarding search params to an index.
inline double
ParseStrictGroupAcceptanceThreshold(knowhere::Json& params) {
    const auto it = params.find(kStrictGroupAcceptanceThreshold);
    if (it == params.end()) {
        return kDefaultStrictGroupAcceptanceThreshold;
    }
    if (!it->is_number()) {
        ThrowInfo(InvalidParameter,
                  "{} must be a finite number in [0, 1]",
                  kStrictGroupAcceptanceThreshold);
    }
    const auto threshold = it->get<double>();
    if (!std::isfinite(threshold) || threshold < 0 || threshold > 1) {
        ThrowInfo(InvalidParameter,
                  "{} must be a finite number in [0, 1]",
                  kStrictGroupAcceptanceThreshold);
    }
    params.erase(it);
    return threshold;
}

}  // namespace milvus
