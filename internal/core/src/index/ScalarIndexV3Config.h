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

#include <atomic>

namespace milvus::index {

// Runtime-configurable via queryNode.segcore.scalarIndexV3.enableAsyncLoad.
inline std::atomic<bool>&
scalar_index_v3_async_load_enabled_atomic() {
    static std::atomic<bool> instance{false};
    return instance;
}

inline bool
ScalarIndexV3AsyncLoadEnabled() {
    return scalar_index_v3_async_load_enabled_atomic().load(
        std::memory_order_acquire);
}

inline void
SetScalarIndexV3AsyncLoadEnabled(bool enabled) {
    scalar_index_v3_async_load_enabled_atomic().store(
        enabled, std::memory_order_release);
}

}  // namespace milvus::index
