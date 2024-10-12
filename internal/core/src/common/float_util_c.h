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

#include <type_traits>
#include <folly/Hash.h>

namespace milvus {

template <typename FLOAT,
          std::enable_if_t<std::is_floating_point<FLOAT>::value, bool> = true>
struct NaNAwareHash {
    std::size_t
    operator()(const FLOAT& val) const noexcept {
        static const std::size_t kNanHash =
            folly::hasher<FLOAT>{}(std::numeric_limits<FLOAT>::quiet_NaN());
        if (std::isnan(val)) {
            return kNanHash;
        }
        return folly::hasher<FLOAT>{}(val);
    }
};

}  // namespace milvus